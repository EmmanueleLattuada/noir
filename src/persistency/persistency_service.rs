use std::{time::Duration, marker::PhantomData};

use crate::{network::OperatorCoord, operator::{SnapshotId, ExchangeData}};

use super::{PersistencyServices, redis_handler::RedisHandler, state_saver::StateSaver};

/// This provide services for saving the state.
/// This is a wrapper for decoupling the persistency services from the used database
#[derive(Debug)]
pub struct PersistencyService<State: ExchangeData> {
    handler: RedisHandler,
    state_saver: StateSaver<State>, 
    restart_from: Option<SnapshotId>,
    pub(crate) snapshot_frequency_by_item: Option<u64>,
    pub(crate) snapshot_frequency_by_time: Option<Duration>,
    _state : PhantomData<State>,
}

impl <State:ExchangeData> Clone for PersistencyService<State> {
    /// Generate a new actual saver for this clone
    fn clone(&self) -> Self {
        Self {
            handler: self.handler.clone(),
            state_saver: self.state_saver.clone(),
            restart_from: self.restart_from.clone(),
            snapshot_frequency_by_item: self.snapshot_frequency_by_item.clone(),
            snapshot_frequency_by_time: self.snapshot_frequency_by_time.clone(),
            _state: PhantomData::clone(&self._state),
        }
    }
}

impl<State:ExchangeData> PersistencyService<State> {
    /// Create a new persistencyService from given configuration
    pub (crate) fn new(handler: RedisHandler, state_saver: StateSaver<State>, restart_from: Option<SnapshotId>, snapshot_frequency_by_item: Option<u64>, snapshot_frequency_by_time: Option<Duration>) -> Self{       
        return Self { 
            handler: handler.clone(), 
            state_saver,
            restart_from,
            snapshot_frequency_by_item,
            snapshot_frequency_by_time,
            _state: PhantomData::default(),
        }              
    }
    
    /// Return last complete snapshot. Use find_snapshot() first to compute it.
    /// Remove all partial snapshotd with id > self.restart_from
    #[inline(never)]
    pub (crate) fn restart_from_snapshot(&self, op_coord: OperatorCoord) -> Option<SnapshotId> {
        if let Some(snap_id) = self.restart_from.clone() {
            let mut last_snap = self.get_last_snapshot(op_coord).unwrap();
            if !(last_snap <= snap_id && last_snap.terminate()) {
                while last_snap > snap_id {
                    self.delete_state(op_coord, last_snap);
                    last_snap = self.get_last_snapshot(op_coord).unwrap();                
                }
            }
            return Some(last_snap)      
        }
        None
    }

    /// This will get the last saved snapshot id, then save state with 
    /// a terminated snapshot id with id = last snapshot id + 1.
    /// In case there are no saved snapshots the id is set to one.
    /// If the operator has already saved a terminated state this function does nothing.
    /// Call this before forward StreamElement::Terminate.
    #[inline(never)]
    pub (crate) fn save_terminated_state(&self, op_coord: OperatorCoord, state: State) {
        self.state_saver.save_terminated_state(op_coord, state);
        // Old version 
        /*
        // Get snapshot id for terminated state
        let opt_last_snapshot_id = saver.handler.get_last_snapshot(op_coord);
        if let Some(last_snapshot_id) = opt_last_snapshot_id {
            if !last_snapshot_id.terminate() {
                let terminal_snap_id = SnapshotId::new_terminate(last_snapshot_id.id() + 1);
                self.handler.save_state(op_coord, terminal_snap_id, state)
            }
        } else {
            // Save with id = 1
            let terminal_snap_id = SnapshotId::new_terminate(1);
            self.handler.save_state(op_coord, terminal_snap_id, state)
        }*/
    }


    #[inline(never)]
    pub (crate) fn save_state(&self, op_coord: OperatorCoord, snapshot_id: SnapshotId, state: State) {
        if snapshot_id.id() == 0 {
            panic!("Passed snap_id: {snapshot_id:?}.\nSnapshot id must start from 1");
        }
        /*
        // Checks on snapshot id
        let last_snapshot = saver.handler.get_last_snapshot(op_coord);
        if !((snap_id.id() == 1 && last_snapshot.is_none()) || last_snapshot.unwrap_or(SnapshotId::new(0)) == snap_id - 1) {
            panic!("Passed snap_id: {snap_id:?}.\n Last saved snap_id: {last_snapshot:?}.\n  Op_coord: {op_coord:?}.\n Snapshot id must be a sequence with step 1 starting from 1");
        }
         */
        self.state_saver.save(op_coord, snapshot_id, state)
    }

    #[inline(never)]
    pub (crate) fn get_last_snapshot(&self, op_coord: OperatorCoord) -> Option<SnapshotId> {
        self.handler.get_last_snapshot(op_coord)
    }
    #[inline(never)]
    pub (crate) fn get_state(&self, op_coord: OperatorCoord, snapshot_id: SnapshotId) -> Option<State> {
        self.handler.get_state(op_coord, snapshot_id)
    }
    #[inline(never)]
    pub (crate) fn delete_state(&self, op_coord: OperatorCoord, snapshot_id: SnapshotId) {
        self.handler.delete_state(op_coord, snapshot_id);
    }
}