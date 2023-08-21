// This will generate persistency_service object to be cloned 
// and passed to all operators and the state_saver object

// find_snapshot, compute last snap

// generate persistency service obj and state_saver obj

// delete all

use std::time::Duration;
use crate::{network::OperatorCoord, operator::{SnapshotId, ExchangeData}, config::PersistencyConfig};

use super::{PersistencyServices, redis_handler::RedisHandler, persistency_service::PersistencyService};

#[derive(Debug, Clone, Default)]
pub struct PersistencyBuilder {
    handler: RedisHandler,
    active: bool,
    restart_from: Option<SnapshotId>,
    snapshot_frequency_by_item: Option<u64>,
    snapshot_frequency_by_time: Option<Duration>,
}

impl PersistencyBuilder {
    /// Create a new persistencyService from given configuration
    pub (crate) fn new(conf: Option<PersistencyConfig>) -> Self{
        if let Some(config) = conf{
            let handler = RedisHandler::new(config.server_addr.clone());
            return Self { 
                handler, 
                active: true,
                restart_from: None,
                snapshot_frequency_by_item: config.snapshot_frequency_by_item,
                snapshot_frequency_by_time: config.snapshot_frequency_by_time,
            }
        } else {
            return Self { 
                handler: RedisHandler::default(),
                active: false,
                restart_from: None,
                snapshot_frequency_by_item: None,
                snapshot_frequency_by_time: None,
            }
        }        
    }

    pub (crate) fn is_active(&self) -> bool {
        self.active
    }

    /// Call this method to restart from specified snapshot or from the last one.
    /// All partial or complete snapshot with an higher index will be deleted in another step.
    /// You must includes all coordinates of each operator in the graph to have a correct result.
    #[inline(never)]
    pub (crate) fn find_snapshot(&mut self, operators: Vec<OperatorCoord>, snapshot_index: Option<u64>) {
        self.compute_last_complete_snapshot(operators.clone());
        if let Some(restart) = self.restart_from{
            if let Some(snap_idx) = snapshot_index {
                if snap_idx == 0 {
                    self.restart_from = None;
                    self.clean_persisted_state(operators.clone());  //FIX
                } else if snap_idx < restart.id() {
                    // TODO: Check that the snapshot with this id has not been deleted
                    self.restart_from = Some(SnapshotId::new(snap_idx));
                }
            }
        } 
    }

    /// Method to compute the last complete snapshot.
    pub (super) fn compute_last_complete_snapshot(&mut self, operators: Vec<OperatorCoord>) {
        if !self.is_active() {
            panic!("Persistency is not active");
        }
        
        let mut op_iter = operators.into_iter();
        let last_snap = self.handler.get_last_snapshot(op_iter.next().unwrap_or_else(||
            panic!("No operators provided")
        ));
        if last_snap.is_none() {
            // This operator never received a snapshot marker, so there isn't a complete vaild snapshot
            self.restart_from = None;
            return;
        }
        let mut last_snap = last_snap.unwrap();
        for op in op_iter {
            let opt_snap = self.handler.get_last_snapshot(op);
            match opt_snap {
                Some(snap_id) => {
                    if snap_id.terminate() && last_snap.terminate() {
                        // take the max 
                        if snap_id.id() > last_snap.id() {
                            last_snap = snap_id;
                        }
                    } else if !snap_id.terminate() && last_snap.terminate() {
                        // take snap_id
                        last_snap = snap_id;
                    } else if !snap_id.terminate() && !last_snap.terminate() {
                        // take the min
                        if snap_id.id() < last_snap.id() {
                            last_snap = snap_id;
                        }
                    }
                    // if snap_id.terminate() && !last_snap.terminate() do nothing
                },
                None => {
                    // This operator never received a snapshot marker, so there isn't a complete vaild snapshot
                    self.restart_from = None;
                    return;
                }
            }
        }
        self.restart_from = Some(last_snap);
    }

    /// Method to remove all persisted data.
    /// You must includes all coordinates of each operator in the graph to have a complete cleaning.
    #[inline(never)]
    pub (crate) fn clean_persisted_state(&mut self, operators: Vec<OperatorCoord>){
        for op_coord in operators {
            let mut last_opt_snap = self.handler.get_last_snapshot(op_coord);
            while last_opt_snap.is_some() {
                self.handler.delete_state(op_coord, last_opt_snap.unwrap());
                last_opt_snap = self.handler.get_last_snapshot(op_coord);
            } 
        }
    }

    /// Method to generate persistency service object. 
    /// This object can be cloned and passed to all operators
    pub(crate) fn generate_persistency_service<State: ExchangeData>(&self) -> PersistencyService<State> {
        if !self.is_active() {
            // TODO Fix this
            panic!("Persistency not active");
        } else {
            PersistencyService::new(
                self.handler.clone(), 
                self.restart_from, 
                self.snapshot_frequency_by_item, 
                self.snapshot_frequency_by_time
            )
        }
    }
}
