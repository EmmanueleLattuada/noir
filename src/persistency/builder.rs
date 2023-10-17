// This will generate persistency_service object to be cloned 
// and passed to all operators and the state_saver object

use std::time::Duration;
use hashbrown::HashMap;

use crate::{network::OperatorCoord, operator::{SnapshotId, ExchangeData}, config::PersistencyConfig};

use super::{PersistencyServices, redis_handler::RedisHandler, persistency_service::PersistencyService, state_saver::StateSaverHandler};

#[derive(Debug)]
pub struct PersistencyBuilder {
    handler: RedisHandler,
    state_saver_handler: StateSaverHandler,
    restart_from: Option<SnapshotId>,
    restart_form_stack: HashMap<u64, Vec<u64>>,
    snapshot_frequency_by_item: Option<u64>,
    snapshot_frequency_by_time: Option<Duration>,
}

impl PersistencyBuilder {
    /// Create a new persistencyService from given configuration
    pub (crate) fn new(conf: Option<PersistencyConfig>) -> Self{
        if let Some(config) = conf{
            let handler = RedisHandler::new(config.server_addr.clone());
            let state_saver_handler = StateSaverHandler::new(handler.clone());
            return Self { 
                handler, 
                state_saver_handler,
                restart_from: None,
                restart_form_stack: HashMap::default(),
                snapshot_frequency_by_item: config.snapshot_frequency_by_item,
                snapshot_frequency_by_time: config.snapshot_frequency_by_time,
            }
        } else {
            panic!("Please provide persistency configuration");
        }        
    }

    /// Call this method to restart from specified snapshot or from the last one.
    /// All partial or complete snapshot with an higher index will be deleted in another step.
    /// You must includes all coordinates of each operator in the graph to have a correct result.
    #[inline(never)]
    pub (crate) fn find_snapshot(&mut self, operators: Vec<OperatorCoord>, snapshot_index: Option<u64>) {
        let op_iter_indexes = self.compute_last_complete_snapshot(operators.clone());
        if let Some(restart) = self.restart_from.clone(){
            if let Some(snap_idx) = snapshot_index {
                if snap_idx == 0 {
                    self.restart_from = None;
                    self.clean_persisted_state(operators.clone());  //FIX
                } else if snap_idx < restart.id() {
                    // TODO: Check that the snapshot with this id has not been deleted
                    let snap = SnapshotId::new(snap_idx);
                    self.compute_last_iter_stack(snap_idx, op_iter_indexes);
                    self.restart_from = Some(snap);
                }
            }
        } 
    }

    /// Method to compute the last complete snapshot.
    pub (super) fn compute_last_complete_snapshot(&mut self, operators: Vec<OperatorCoord>) -> HashMap<u64, Vec<OperatorCoord>>{
        let mut op_iter = operators.clone().into_iter();
        let last_snap = self.handler.get_last_snapshot(op_iter.next().unwrap_or_else(||
            panic!("No operators provided")
        ));
        if last_snap.is_none() {
            // This operator never received a snapshot marker, so there isn't a complete vaild snapshot
            self.restart_from = None;
            return HashMap::default();
        }
        let mut last_snap = last_snap.unwrap();
        let mut op_iter_index = HashMap::new();
        for op in op_iter {
            let opt_snap = self.handler.get_last_snapshot(op);
            match opt_snap {
                Some(snap_id) => {
                    if let Some(iter_index) = snap_id.iteration_index {
                        op_iter_index.entry(iter_index).or_insert(vec![op]).push(op);
                    }
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
                    return HashMap::default();
                }
            }
        }

        let snap_index = last_snap.id();
        // calculate iteration stack for each iteration index
        self.compute_last_iter_stack(snap_index, op_iter_index.clone());      
        
        self.restart_from = Some(last_snap);
        op_iter_index
    }

    /// For each operator get the iter stack, take max common for each level
    fn compute_last_iter_stack(&mut self, snap_index: u64, op_iter_indexes: HashMap<u64, Vec<OperatorCoord>>) {
        // each iter index
        for (iter_index, operators) in op_iter_indexes.into_iter(){
            // for each operator get the iter stack, take max common for each level
            let mut last_iter_stack: Vec<u64> = Vec::default();    
            for op in operators {
                let opt_iter_stack = self.handler.get_last_iter_stack(op, snap_index);
                if let Some(iter_stack) = opt_iter_stack {
                    let mut i = 0;
                    loop {
                        if last_iter_stack.len() < i + 1 {
                            // iter_stack has more levels
                            last_iter_stack = iter_stack;
                            break;
                        } else if iter_stack.len() < i + 1{
                            // iter_stack has less levels
                            break;
                        } else {
                            if last_iter_stack[i] > iter_stack[i] {
                                last_iter_stack = iter_stack;
                                break;
                            } else if last_iter_stack[i] < iter_stack[i] {
                                break;
                            }
                        }
                        i += 1;
                    }
                }
            }
            self.restart_form_stack.insert(iter_index, last_iter_stack);
        }
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
        PersistencyService::new(
            self.handler.clone(), 
            self.state_saver_handler.get_state_saver(),
            self.restart_from.clone(), 
            self.restart_form_stack.clone(),
            self.snapshot_frequency_by_item, 
            self.snapshot_frequency_by_time,
        )
    }

    /// Method to stop actual sender. Send a special termination message then wait 
    /// that all states have been persisted
    pub (crate) fn stop_actual_sender(&mut self){
        self.state_saver_handler.stop_actual_sender();
    }

    /// Blocking function that stops until all previously saved states are actually persisted.
    /// This will close the old state saver and create a new one.
    /// It should be used only for tests
    #[allow(dead_code)]
    pub (crate) fn flush_state_saver(&mut self) {
        self.state_saver_handler.stop_actual_sender();
        self.state_saver_handler = StateSaverHandler::new(self.handler.clone());
    }
}