extern crate r2d2_redis;

use r2d2_redis::{RedisConnectionManager, r2d2::Pool, redis::Commands};


use bincode::{DefaultOptions, Options, config::{WithOtherTrailing, WithOtherIntEncoding, FixintEncoding, RejectTrailing}};
use once_cell::sync::Lazy;

use crate::{network::OperatorCoord, operator::{SnapshotId, ExchangeData}, config::PersistencyConfig};

/// Serializer
static SERIALIZER: Lazy<DefaultOptions> = Lazy::new(bincode::DefaultOptions::new);

/// Configuration of the key serializer: the integers must have a fixed length encoding.
static KEY_SERIALIZER: Lazy<
    WithOtherTrailing<WithOtherIntEncoding<DefaultOptions, FixintEncoding>, RejectTrailing>,
> = Lazy::new(|| {
    bincode::DefaultOptions::new()
        .with_fixint_encoding()
        .reject_trailing_bytes()
});

fn serialize_op_coord(op_coord: OperatorCoord) -> Vec<u8> {
    let op_coord_key_len = KEY_SERIALIZER
        .serialized_size(&op_coord)
        .unwrap_or_else(|e| {
            panic!(
                "Failed to compute serialized length of operator coordinate: {op_coord}. Error: {e:?}",
            )
        });        
    let mut op_coord_key_buf = Vec::with_capacity(op_coord_key_len as usize);
    KEY_SERIALIZER
        .serialize_into(&mut op_coord_key_buf, &op_coord)
        .unwrap_or_else(|e| {
            panic!(
                "Failed to serialize operator coordinate (was {op_coord_key_len} bytes) of: {op_coord}. Error: {e:?}",
            )
        });
    assert_eq!(op_coord_key_buf.len(), op_coord_key_len as usize);
    op_coord_key_buf
}

fn serialize_snapshot_id(snapshot_id: SnapshotId) -> Vec<u8> {
    let snap_id_len = KEY_SERIALIZER
        .serialized_size(&snapshot_id)
        .unwrap_or_else(|e| {
            panic!(
                "Failed to compute serialized length of snapshot id: {snapshot_id}. Error: {e:?}",
            )
        });        
    let mut snap_id_buf = Vec::with_capacity(snap_id_len as usize);
    KEY_SERIALIZER
        .serialize_into(&mut snap_id_buf, &snapshot_id)
        .unwrap_or_else(|e| {
            panic!(
                "Failed to serialize snapshot id (was {snap_id_len} bytes): {snapshot_id}. Error: {e:?}",
            )
        });
    assert_eq!(snap_id_buf.len(), snap_id_len as usize);
    snap_id_buf
} 

pub(crate) trait PersistencyServices {  
    /// Method to save the state of the operator
    fn save_state<State: ExchangeData>(&self, op_coord: OperatorCoord, snapshot_id: SnapshotId, state: State);
    /// Method to save a void state, used for operators with no state, is necessary to be able to get last valid snapshot a posteriori
    fn save_void_state(&self, op_coord: OperatorCoord, snapshot_id: SnapshotId);
    /// Method to get the state of the operator
    fn get_state<State: ExchangeData>(&self, op_coord: OperatorCoord, snapshot_id: SnapshotId) -> Option<State>;
    /// Method to get the id of the last snapshot done by specified operator
    fn get_last_snapshot(&self, op_coord: OperatorCoord) -> Option<SnapshotId>;
    /// Method to remove the state associated to specified operator and snapshot id
    fn delete_state(&self, op_coord: OperatorCoord, snapshot_id: SnapshotId);
}

/// This provide services for saving the state.
/// This is a wrapper for decoupling the persistency services from the used database
#[derive(Debug, Clone, Default)]
pub struct PersistencyService {
    handler: RedisHandler,
    active: bool,
    restart_from: Option<SnapshotId>,
}

impl PersistencyService {
    /// Create a new persistencyService from given configuration
    pub (crate) fn new(conf: Option<PersistencyConfig>) -> Self{
        if let Some(config) = conf{
            let handler = RedisHandler::new(config.server_addr);
            return Self { 
                handler: handler, 
                active: true,
                restart_from: None,
            }
        } else {
            return Self { 
                handler: RedisHandler::default(), 
                active: false,
                restart_from: None,
            }
        }        
    }

    pub (crate) fn is_active(&self) -> bool {
        self.active
    }

    /// Call this method to restart from specified snapshot or from the last one.
    /// All partial or complete snapshot with an higher index will be deleted in another step.
    /// You must includes all coordinates of each operator in the graph to have a correct result.
    /// To retrive the result use restart_from_snapshot() method
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
    fn compute_last_complete_snapshot(&mut self, operators: Vec<OperatorCoord>) {
        if !self.is_active() {
            panic!("Persistency serviced aren't active");
        }
        
        let mut op_iter = operators.into_iter();
        let last_snap = self.get_last_snapshot(op_iter.next().unwrap_or_else(||
            panic!("No operators provided")
        ));
        if last_snap.is_none() {
            // This operator never received a snapshot marker, so there isn't a complete vaild snapshot
            self.restart_from = None;
            return;
        }
        let mut last_snap = last_snap.unwrap();
        for op in op_iter {
            let opt_snap = self.get_last_snapshot(op);
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
    /// Return last complete snapshot. Use find_snapshot() first to compute it.
    /// Remove all partial snapshotd with id > self.restart_from
    #[inline(never)]
    pub (crate) fn restart_from_snapshot(&self, op_coord: OperatorCoord) -> Option<SnapshotId> {
        if let Some(snap_id) = self.restart_from {
            let mut last_snap = self.get_last_snapshot(op_coord).unwrap();
            if last_snap.id() <= snap_id.id() && last_snap.terminate() {
                return Some(last_snap)
            }
            // Remove all partial snapshots with id > self.restart_from           
            while last_snap.id() > snap_id.id() {
                self.delete_state(op_coord, last_snap);
                last_snap = self.get_last_snapshot(op_coord).unwrap();                
            }             
        }
        self.restart_from.clone()
    }

    /// This will get the last saved snapshot id, then save state with 
    /// a terminated snapshot id with id = last snapshot id + 1.
    /// In case there are no saved snapshots the id is set to one.
    /// If the operator has already saved a terminated state this function does nothing.
    /// Call this before forward StreamElement::Terminate.
    #[inline(never)]
    pub (crate) fn save_terminated_state<State: ExchangeData>(&self, op_coord: OperatorCoord, state: State) {
        if !self.active {
            panic!("Persistency services aren't configured");
        }
        let opt_last_snapshot_id = self.handler.get_last_snapshot(op_coord);
        if let Some(last_snapshot_id) = opt_last_snapshot_id {
            if !last_snapshot_id.terminate() {
                let terminal_snap_id = SnapshotId::new_terminate(last_snapshot_id.id() + 1);
                self.handler.save_state(op_coord, terminal_snap_id, state);
            }
        } else {
            // Save with id = 1
            let terminal_snap_id = SnapshotId::new_terminate(1);
            self.handler.save_state(op_coord, terminal_snap_id, state);
        }
    }

    /// Similar to save_terminated_state
    /// This will get the last saved snapshot id, then save state with 
    /// a terminated snapshot id with id = last snapshot id + 1.
    /// In case there are no saved snapshots the id is set to one.
    /// If the operator has already saved a terminated state this function does nothing.
    /// Call this before forward StreamElement::Terminate.
    #[inline(never)]
    pub (crate) fn save_terminated_void_state(&self, op_coord: OperatorCoord) {
        if !self.active {
            panic!("Persistency services aren't configured");
        }
        let opt_last_snapshot_id = self.handler.get_last_snapshot(op_coord);
        if let Some(last_snapshot_id) = opt_last_snapshot_id {
            if !last_snapshot_id.terminate() {
                let terminal_snap_id = SnapshotId::new_terminate(last_snapshot_id.id() + 1);
                self.handler.save_void_state(op_coord, terminal_snap_id);
            }
        } else {
            // Save with id = 1
            let terminal_snap_id = SnapshotId::new_terminate(1);
            self.handler.save_void_state(op_coord, terminal_snap_id);
        }
    }

    /// Method to remove all persisted data.
    /// You must includes all coordinates of each operator in the graph to have a complete cleaning.
    #[inline(never)]
    pub (crate) fn clean_persisted_state(&mut self, operators: Vec<OperatorCoord>){
        for op_coord in operators {
            let mut last_opt_snap = self.get_last_snapshot(op_coord);
            while last_opt_snap.is_some() {
                self.delete_state(op_coord, last_opt_snap.unwrap());
                last_opt_snap = self.get_last_snapshot(op_coord);
            } 
        }
    }
}

// Just wrap methods and check snapshot id constraints
impl PersistencyServices for PersistencyService{
    #[inline(never)]
    fn save_state<State: ExchangeData>(&self, op_coord: OperatorCoord, snapshot_id: SnapshotId, state: State) {
        if !self.active {
            panic!("Persistency services aren't configured");
        }
        if snapshot_id.id() == 0 {
            panic!("Passed snap_id: {snapshot_id:?}.\nSnapshot id must start from 1");
        }
        if !((snapshot_id.id() == 1 && self.get_last_snapshot(op_coord).is_none()) || self.get_last_snapshot(op_coord).unwrap_or(SnapshotId::new(0)) == snapshot_id - 1) {
            let saved = self.get_last_snapshot(op_coord);
            panic!("Passed snap_id: {snapshot_id:?}.\n Last saved snap_id: {saved:?}.\n  Op_coord: {op_coord:?}.\n Snapshot id must be a sequence with step 1 starting from 1");
        }
        self.handler.save_state(op_coord, snapshot_id, state);
    }
    #[inline(never)]
    fn save_void_state(&self, op_coord: OperatorCoord, snapshot_id: SnapshotId) {
        if !self.active {
            panic!("Persistency services aren't configured");
        }
        if snapshot_id.id() == 0 {
            panic!("Passed snap_id: {snapshot_id:?}.\nSnapshot id must start from 1");
        }
        if !((snapshot_id.id() == 1 && self.get_last_snapshot(op_coord).is_none()) || self.get_last_snapshot(op_coord).unwrap_or(SnapshotId::new(0)) == snapshot_id - 1) {
            let saved = self.get_last_snapshot(op_coord);
            panic!("Passed snap_id: {snapshot_id:?}.\n Last saved snap_id: {saved:?}.\n  Op_coord: {op_coord:?}.\n Snapshot id must be a sequence with step 1 starting from 1");
        }
        self.handler.save_void_state(op_coord, snapshot_id);
    }
    #[inline(never)]
    fn get_last_snapshot(&self, op_coord: OperatorCoord) -> Option<SnapshotId> {
        if !self.active {
            panic!("Persistency services aren't configured");
        }
        self.handler.get_last_snapshot(op_coord)
    }
    #[inline(never)]
    fn get_state<State: ExchangeData>(&self, op_coord: OperatorCoord, snapshot_id: SnapshotId) -> Option<State> {
        if !self.active {
            panic!("Persistency services aren't configured");
        }
        self.handler.get_state(op_coord, snapshot_id)
    }
    #[inline(never)]
    fn delete_state(&self, op_coord: OperatorCoord, snapshot_id: SnapshotId) {
        if !self.active {
            panic!("Persistency services aren't configured");
        }
        self.handler.delete_state(op_coord, snapshot_id);
    }
}

/// Redis handler
#[derive(Debug, Clone, Default)]
struct RedisHandler {
    conn_pool: Option<Pool<RedisConnectionManager>>,
}

impl RedisHandler {
    /// Create a new Redis handler from given configuration
    fn new(config: String) -> Self{
        let manager = RedisConnectionManager::new(config).unwrap();
        let conn_pool = Pool::builder()
                .build(manager)
                .unwrap();
        Self {  
            conn_pool: Some(conn_pool),
        }
    }
}


impl PersistencyServices for RedisHandler{    
    fn save_state<State: ExchangeData>(&self, op_coord: OperatorCoord, snapshot_id: SnapshotId, state: State) {
        // Prepare connection
        let mut conn = self.conn_pool
            .as_ref()
            .unwrap()
            .get()
            .unwrap_or_else(|e|
                panic!("Fail to connect to Redis: {e:?}")
            );
        
        // Serialize the state
        let state_len = SERIALIZER
            .serialized_size(&state)
            .unwrap_or_else(|e| {
                panic!(
                    "Failed to compute serialized length of the state of operator: {op_coord}, at snapshot id: {snapshot_id}. Error: {e:?}",
                )
            });        
        let mut state_buf = Vec::with_capacity(state_len as usize);
        SERIALIZER
            .serialize_into(&mut state_buf, &state)
            .unwrap_or_else(|e| {
                panic!(
                    "Failed to serialize state (was {state_len} bytes) of operator: {op_coord}, at snapshot id: {snapshot_id}. Error: {e:?}",
                )
            });
        assert_eq!(state_buf.len(), state_len as usize);

        // Serialize op_coord
        let op_coord_key_buf = serialize_op_coord(op_coord);
                
        // Serialize snap_id
        let snap_id_buf= serialize_snapshot_id(snapshot_id);
        
        // Save op_coord + snap_id -> state
        let mut op_snap_key_buf = Vec::with_capacity(op_coord_key_buf.len() + snap_id_buf.len());
        op_snap_key_buf.append(&mut op_coord_key_buf.clone());
        op_snap_key_buf.append(&mut snap_id_buf.clone());
        assert_eq!(op_snap_key_buf.len(), op_coord_key_buf.len() + snap_id_buf.len());

        conn.set::<Vec<u8>, Vec<u8>, ()>(op_snap_key_buf, state_buf)
            .unwrap_or_else(|e|
                panic!("Fail to save the state: {e:?}")
            );

        // Save op_coord -> snap_id (push on list)
        conn.lpush(op_coord_key_buf, snap_id_buf)
            .unwrap_or_else(|e|
                panic!("Fail to save the state: {e:?}")
            );
        
        // Log the store
        log::debug!("Saved state for operator: {op_coord}, at snapshot id: {snapshot_id}\n",);     
    }

    fn save_void_state(&self, op_coord: OperatorCoord, snapshot_id: SnapshotId) {
        // Prepare connection
        let mut conn = self.conn_pool
            .as_ref()
            .unwrap()
            .get()
            .unwrap_or_else(|e|
                panic!("Fail to connect to Redis: {e:?}")
            );

        // Serialize op_coord
        let op_coord_key_buf = serialize_op_coord(op_coord);

        // Serialize snap_id
        let snap_id_buf= serialize_snapshot_id(snapshot_id);

        // Save op_coord -> snap_id (push on list)
        conn.lpush(op_coord_key_buf, snap_id_buf)
            .unwrap_or_else(|e|
                panic!("Fail to save the state: {e:?}")
            );

        // Log the store
        log::debug!("Saved void state for operator: {op_coord}, at snapshot id: {snapshot_id}\n",);
    }

    fn get_last_snapshot(&self, op_coord: OperatorCoord) -> Option<SnapshotId> {
        // Prepare connection
        let mut conn = self.conn_pool
            .as_ref()
            .unwrap()
            .get()
            .unwrap_or_else(|e|
                panic!("Fail to connect to Redis: {e:?}")
            );

        // Serialize op_coord
        let op_coord_key_buf = serialize_op_coord(op_coord);
        
        // Get the last snapshotid
        let ser_snap_id: Option<Vec<u8>> = conn.lindex(op_coord_key_buf.clone(), 0)
        .unwrap_or_else(|e| {
            panic!("Failed to get last snapshot id of operator: {op_coord}. Error: {e:?}")
        });
        // Check if is Some
        if let Some(snap_id) = ser_snap_id {
            // Deserialize the snapshot_id
            let error_msg = format!("Fail deserialization of snapshot id");
            let snapshot_id: SnapshotId = KEY_SERIALIZER
                .deserialize(snap_id.as_ref())
                .expect(&error_msg);
            
            return Some(snapshot_id)
        }
        None
    }

    fn get_state<State: ExchangeData>(&self, op_coord: OperatorCoord, snapshot_id: SnapshotId) -> Option<State> {
        // Prepare connection
        let mut conn = self.conn_pool
            .as_ref()
            .unwrap()
            .get()
            .unwrap_or_else(|e|
                panic!("Redis connection error: {e:?}")
            );

        // Serialize op_coord
        let op_coord_key_buf = serialize_op_coord(op_coord);
                
        // Serialize snap_id
        let snap_id_buf= serialize_snapshot_id(snapshot_id);

        // Get op_coord + snap_id -> state
        let mut op_snap_key_buf = Vec::with_capacity(op_coord_key_buf.len() + snap_id_buf.len());
        op_snap_key_buf.append(&mut op_coord_key_buf.clone());
        op_snap_key_buf.append(&mut snap_id_buf.clone());
        assert_eq!(op_snap_key_buf.len(), op_coord_key_buf.len() + snap_id_buf.len());
        
        let res = conn.get::<Vec<u8>, Vec<u8>>(op_snap_key_buf)
            .unwrap_or_else(|e|
                panic!("Fail to get the state: {e:?}")
            );
        
        if res.len() == 0 {
            return None;
        }
        
        // Deserialize the state
        let error_msg = format!("Fail deserialization of state for operator: {op_coord}, at snapshot id: {snapshot_id}");
        let msg: State = SERIALIZER
            .deserialize(res.as_ref())
            .expect(&error_msg);
        
        Some(msg)
    }

    fn delete_state(&self, op_coord: OperatorCoord, snapshot_id: SnapshotId) {
        // Prepare connection
        let mut conn = self.conn_pool
            .as_ref()
            .unwrap()
            .get()
            .unwrap_or_else(|e|
                panic!("Redis connection error: {e:?}")
            );

        // Serialize op_coord
        let op_coord_key_buf = serialize_op_coord(op_coord);
                
        // Serialize snap_id
        let snap_id_buf= serialize_snapshot_id(snapshot_id);

        // Compute key op_coord + snap_id
        let mut op_snap_key_buf = Vec::with_capacity(op_coord_key_buf.len() + snap_id_buf.len());
        op_snap_key_buf.append(&mut op_coord_key_buf.clone());
        op_snap_key_buf.append(&mut snap_id_buf.clone());
        assert_eq!(op_snap_key_buf.len(), op_coord_key_buf.len() + snap_id_buf.len());

        // Delete key op_coord + snap_id
        conn.del::<Vec<u8>, u64>(op_snap_key_buf.clone()).unwrap_or_else(|e|
                panic!("Fail to delete the state: {e:?}")
            );
        let exists: u64 = conn.exists(op_snap_key_buf).unwrap_or_else(|e|
            panic!("Fail to delete the state: {e:?}")
        );
        if exists != 0 {
            panic!("Fail to delete the state for operator: {op_coord} and snapshot_id: {snapshot_id}");
        }

        // Remove op_coord from list
        conn.lrem(op_coord_key_buf, 1, snap_id_buf).unwrap_or_else(|e|
            panic!("Fail to delete the state: {e:?}")
        );

        /* FIX 
        let exists: i64 = conn.lpos(op_coord_key_buf, snapshot_id, ).unwrap_or_else(|e|
            panic!("Fail to delete the state: {e:?}")
        );
        if exists >= 0 {
            panic!("Fail to delete the state for operator: {op_coord} and snapshot_id: {snapshot_id}");
        }*/

        // Remove op_cord + snap_id from list
        // TODO

        // Log the delete
        log::debug!("Deleted state for operator: {op_coord}, at snapshot id: {snapshot_id}\n",);
    }
}



#[cfg(test)]
mod tests {
    use std::panic::AssertUnwindSafe;

    use serde::{Serialize, Deserialize};
    use serial_test::serial;

    use crate::{network::OperatorCoord, test::REDIS_TEST_CONFIGURATION, operator::SnapshotId, config::PersistencyConfig};

    use super::{PersistencyService, PersistencyServices};

    #[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
    struct FakeState {
        num: u64,
        flag: bool,
        values: Vec<i32>,
        str: String,
    }

    #[test]
    #[serial]
    fn test_save_get_remove_state(){
        let op_coord1 = OperatorCoord {
            block_id: 1,
            host_id: 1,
            replica_id: 1,
            operator_id: 1,
        };
       
        let mut state1 = FakeState{
            num: 10,
            flag: true,
            values: Vec::new(),
            str: "Test_1".to_string(),
        };
        state1.values.push(1);
        state1.values.push(2);
        state1.values.push(3);

        let pers_handler = PersistencyService::new(Some(
            PersistencyConfig { 
                server_addr: String::from(REDIS_TEST_CONFIGURATION),
                try_restart: false,
                clean_on_exit: false,
                restart_from: None,
            }
        ));
        pers_handler.save_state(op_coord1, SnapshotId::new(1), state1.clone());
        let retrived_state: FakeState = pers_handler.get_state(op_coord1, SnapshotId::new(1)).unwrap();
        assert_eq!(state1, retrived_state);

        let mut state2 = state1.clone();
        state2.values.push(4);

        pers_handler.save_state(op_coord1, SnapshotId::new(2), state2.clone());
        let retrived_state: FakeState = pers_handler.get_state(op_coord1, SnapshotId::new(2)).unwrap();
        assert_ne!(state1, retrived_state);
        assert_eq!(state2, retrived_state);

        let retrived_state: Option<FakeState> = pers_handler.get_state(op_coord1, SnapshotId::new(3));
        assert_eq!(None, retrived_state);

        let last = pers_handler.get_last_snapshot(op_coord1).unwrap();
        assert_eq!(SnapshotId::new(2), last);

        // Clean
        pers_handler.delete_state(op_coord1, SnapshotId::new(1));
        let retrived_state: Option<FakeState> = pers_handler.get_state(op_coord1, SnapshotId::new(1));
        assert_eq!(None, retrived_state);
        let last = pers_handler.get_last_snapshot(op_coord1).unwrap();
        assert_eq!(SnapshotId::new(2), last);

        pers_handler.delete_state(op_coord1, SnapshotId::new(2));
        let retrived_state: Option<FakeState> = pers_handler.get_state(op_coord1, SnapshotId::new(2));
        assert_eq!(None, retrived_state);
        let last = pers_handler.get_last_snapshot(op_coord1);
        assert_eq!(None, last);

        // Clean already cleaned state
        pers_handler.delete_state(op_coord1, SnapshotId::new(2));
        let retrived_state: Option<FakeState> = pers_handler.get_state(op_coord1, SnapshotId::new(2));
        assert_eq!(None, retrived_state);
        let last = pers_handler.get_last_snapshot(op_coord1);
        assert_eq!(None, last);

    }    
 
    
    #[test]
    #[serial]
    fn test_save_void_state() {
        let op_coord1 = OperatorCoord {
            block_id: 1,
            host_id: 1,
            replica_id: 1,
            operator_id: 2,
        };

        let pers_handler = PersistencyService::new(Some(
            PersistencyConfig { 
                server_addr: String::from(REDIS_TEST_CONFIGURATION),
                try_restart: false,
                clean_on_exit: false,
                restart_from: None,
            }
        ));
        pers_handler.save_void_state(op_coord1, SnapshotId::new(1));
        pers_handler.save_void_state(op_coord1, SnapshotId::new(2));
        let retrived_state: Option<FakeState> = pers_handler.get_state(op_coord1, SnapshotId::new(3));
        assert_eq!(None, retrived_state);

        // Clean
        pers_handler.delete_state(op_coord1, SnapshotId::new(1));
        let retrived_state: Option<FakeState> = pers_handler.get_state(op_coord1, SnapshotId::new(1));
        assert_eq!(None, retrived_state);
        let last = pers_handler.get_last_snapshot(op_coord1).unwrap();
        assert_eq!(SnapshotId::new(2), last);

        pers_handler.delete_state(op_coord1, SnapshotId::new(2));
        let retrived_state: Option<FakeState> = pers_handler.get_state(op_coord1, SnapshotId::new(2));
        assert_eq!(None, retrived_state);
        let last = pers_handler.get_last_snapshot(op_coord1);
        assert_eq!(None, last);

        pers_handler.save_void_state(op_coord1, SnapshotId::new(1));
        let last = pers_handler.get_last_snapshot(op_coord1).unwrap();
        assert_eq!(SnapshotId::new(1), last);
        pers_handler.delete_state(op_coord1, SnapshotId::new(1));
        let last = pers_handler.get_last_snapshot(op_coord1);
        assert_eq!(None, last);

    }

    #[ignore]
    #[test]
    #[serial]
    fn test_snapshot_id_consistency() {
        let op_coord1 = OperatorCoord {
            block_id: 1,
            host_id: 1,
            replica_id: 1,
            operator_id: 3,
        };

        let pers_handler = PersistencyService::new(Some(
            PersistencyConfig { 
                server_addr: String::from(REDIS_TEST_CONFIGURATION),
                try_restart: false,
                clean_on_exit: false,
                restart_from: None,
            }
        ));
        // Snap_id = 0
        let result = std::panic::catch_unwind(AssertUnwindSafe(|| pers_handler.save_state(op_coord1, SnapshotId::new(0), 100)));
        assert!(result.is_err());

        // Snap_id = 10
        let result = std::panic::catch_unwind(AssertUnwindSafe(|| pers_handler.save_void_state(op_coord1, SnapshotId::new(10))));
        assert!(result.is_err());

        pers_handler.save_state(op_coord1, SnapshotId::new(1), 101);
        pers_handler.save_state(op_coord1, SnapshotId::new(2), 102);
        pers_handler.save_state(op_coord1, SnapshotId::new(3), 103);

        // Snap_id = 1
        let result = std::panic::catch_unwind(AssertUnwindSafe(|| pers_handler.save_void_state(op_coord1, SnapshotId::new(1))));
        assert!(result.is_err());

        // Snap_id = 2
        let result = std::panic::catch_unwind(AssertUnwindSafe(|| pers_handler.save_void_state(op_coord1, SnapshotId::new(2))));
        assert!(result.is_err());

        // Snap_id = 3
        let result = std::panic::catch_unwind(AssertUnwindSafe(|| pers_handler.save_void_state(op_coord1, SnapshotId::new(3))));
        assert!(result.is_err());

        // Clean
        pers_handler.delete_state(op_coord1, SnapshotId::new(1));
        pers_handler.delete_state(op_coord1, SnapshotId::new(2));
        pers_handler.delete_state(op_coord1, SnapshotId::new(3));



    }

    #[ignore]
    #[test]
    #[serial]
    fn test_no_persistency() {
        let op_coord1 = OperatorCoord {
            block_id: 1,
            host_id: 1,
            replica_id: 1,
            operator_id: 4
        };

        let pers_handler = PersistencyService::new(None);
        let result = std::panic::catch_unwind(AssertUnwindSafe(|| pers_handler.save_state(op_coord1, SnapshotId::new(1), 100)));
        assert!(result.is_err());

        let result = std::panic::catch_unwind(AssertUnwindSafe(|| pers_handler.save_void_state(op_coord1, SnapshotId::new(1))));
        assert!(result.is_err());

        let result: Result<Option<u32>, Box<dyn std::any::Any + Send>> = std::panic::catch_unwind(AssertUnwindSafe(|| pers_handler.get_state(op_coord1, SnapshotId::new(1))));
        assert!(result.is_err());

        let result = std::panic::catch_unwind(AssertUnwindSafe(|| pers_handler.get_last_snapshot(op_coord1)));
        assert!(result.is_err());

        let result = std::panic::catch_unwind(AssertUnwindSafe(|| pers_handler.delete_state(op_coord1, SnapshotId::new(1))));
        assert!(result.is_err());

    }


    #[test]
    #[serial]
    fn test_compute_last_complete_snapshot(){
        let op_coord1 = OperatorCoord {
            block_id: 1,
            host_id: 1,
            replica_id: 1,
            operator_id: 5,
        };
        let op_coord2 = OperatorCoord {
            block_id: 1,
            host_id: 1,
            replica_id: 1,
            operator_id: 6,
        };
        let op_coord3 = OperatorCoord {
            block_id: 1,
            host_id: 1,
            replica_id: 1,
            operator_id: 7,
        };
        let mut pers_handler = PersistencyService::new(Some(
            PersistencyConfig { 
                server_addr: String::from(REDIS_TEST_CONFIGURATION),
                try_restart: false,
                clean_on_exit: false,
                restart_from: None,
            }
        ));
        assert_eq!(pers_handler.restart_from_snapshot(op_coord1), None);
        pers_handler.compute_last_complete_snapshot(vec![op_coord1, op_coord2, op_coord3]);
        assert_eq!(pers_handler.restart_from_snapshot(op_coord1), None);

        let fake_state = true;
        pers_handler.save_state(op_coord1, SnapshotId::new(1), fake_state);
        pers_handler.save_state(op_coord2, SnapshotId::new(1), fake_state);
        pers_handler.compute_last_complete_snapshot(vec![op_coord1, op_coord2, op_coord3]);
        assert_eq!(pers_handler.restart_from_snapshot(op_coord1), None);

        pers_handler.save_state(op_coord3, SnapshotId::new(1), fake_state);
        pers_handler.compute_last_complete_snapshot(vec![op_coord1, op_coord2, op_coord3]);
        assert_eq!(pers_handler.restart_from_snapshot(op_coord1), Some(SnapshotId::new(1)));

        pers_handler.save_state(op_coord1, SnapshotId::new(2), fake_state);
        pers_handler.save_state(op_coord3, SnapshotId::new(2), fake_state);
        pers_handler.compute_last_complete_snapshot(vec![op_coord1, op_coord2, op_coord3]);
        assert_eq!(pers_handler.restart_from_snapshot(op_coord1), Some(SnapshotId::new(1)));

        pers_handler.save_state(op_coord1, SnapshotId::new(2), fake_state);
        pers_handler.save_state(op_coord2, SnapshotId::new(2), fake_state);
        pers_handler.compute_last_complete_snapshot(vec![op_coord1, op_coord2, op_coord3]);
        assert_eq!(pers_handler.restart_from_snapshot(op_coord1), Some(SnapshotId::new(2)));

        // Clean
        pers_handler.delete_state(op_coord1, SnapshotId::new(1));
        pers_handler.delete_state(op_coord2, SnapshotId::new(1));
        pers_handler.delete_state(op_coord3, SnapshotId::new(1));
        // Clean
        pers_handler.delete_state(op_coord1, SnapshotId::new(2));
        pers_handler.delete_state(op_coord2, SnapshotId::new(2));
        pers_handler.delete_state(op_coord3, SnapshotId::new(2));

    }

}