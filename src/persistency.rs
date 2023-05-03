extern crate redis;
use redis::{Commands, Client};

use bincode::{DefaultOptions, Options, config::{WithOtherTrailing, WithOtherIntEncoding, FixintEncoding, RejectTrailing}};
use once_cell::sync::Lazy;

use crate::{network::OperatorCoord, operator::{SnapshotId, ExchangeData}};

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
    /// Setup connection with the database
    fn setup(&mut self);
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
}

impl PersistencyService {
    /// Create a new persistencyService from given configuration
    pub (crate) fn new(conf: Option<String>) -> Self{
        if conf.is_some(){
            let handler = RedisHandler::new(conf.unwrap());
            return Self { 
                handler: handler, 
                active: true,
            }
        } else {
            return Self { 
                handler: RedisHandler::default(), 
                active: false,
            }
        }
        
    }    

}

// Just wrap methods and check snapshot id constraints
impl PersistencyServices for PersistencyService{
    fn setup(&mut self) {
        if self.active {
            self.handler.setup();
        }
    }

    fn save_state<State: ExchangeData>(&self, op_coord: OperatorCoord, snapshot_id: SnapshotId, state: State) {
        if !self.active {
            panic!("Persistency services aren't configured");
        }
        if snapshot_id == 0 {
            panic!("Snapshot id must start from 1");
        }
        if !((snapshot_id == 1 && self.get_last_snapshot(op_coord).is_none()) || self.get_last_snapshot(op_coord).unwrap_or(0) == snapshot_id - 1) {
            panic!("Snapshot id must be a sequence with step 1 starting from 1");
        }
        self.handler.save_state(op_coord, snapshot_id, state);
    }
    fn save_void_state(&self, op_coord: OperatorCoord, snapshot_id: SnapshotId) {
        if !self.active {
            panic!("Persistency services aren't configured");
        }
        if snapshot_id == 0 {
            panic!("Snapshot id must start from 1");
        }
        if !((snapshot_id == 1 && self.get_last_snapshot(op_coord).is_none()) || self.get_last_snapshot(op_coord).unwrap_or(0) == snapshot_id - 1) {
            panic!("Snapshot id must be a sequence with step 1 starting from 1");
        }
        self.handler.save_void_state(op_coord, snapshot_id);
    }
    fn get_last_snapshot(&self, op_coord: OperatorCoord) -> Option<SnapshotId> {
        if !self.active {
            panic!("Persistency services aren't configured");
        }
        self.handler.get_last_snapshot(op_coord)
    }
    fn get_state<State: ExchangeData>(&self, op_coord: OperatorCoord, snapshot_id: SnapshotId) -> Option<State> {
        if !self.active {
            panic!("Persistency services aren't configured");
        }
        self.handler.get_state(op_coord, snapshot_id)
    }
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
    // Connection will be done with setup method 
    client: Option<Client>,
    config: String,
}

impl RedisHandler {
    /// Create a new Redis handler from given configuration
    fn new(config: String) -> Self{
        Self {  
            // This will be set in the setup method
            client: None,
            config
        }
    }
}


impl PersistencyServices for RedisHandler{
    /// This will check the Redis server and set the client field
    /// N.B. this will not connect to the Redis server
    fn setup(&mut self) {
        let client = redis::Client::open(self.config.clone())
            .unwrap_or_else(|e|
                panic!("Fail to connect to Redis client: {e:?}")
            );
        self.client = Some(client);
    }    

    fn save_state<State: ExchangeData>(&self, op_coord: OperatorCoord, snapshot_id: SnapshotId, state: State) {
        // Prepare connection
        let mut conn = self.client
            .as_ref()
            .unwrap()
            .get_connection()
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

        // Save op_coord -> snap_iter_id (push on list)
        // TODO

        // Save op_coord -> snap_id (push on list)
        conn.lpush(op_coord_key_buf, snapshot_id)
            .unwrap_or_else(|e|
                panic!("Fail to save the state: {e:?}")
            );
        
        // Log the store
        log::debug!("Saved state for operator: {op_coord}, at snapshot id: {snapshot_id}\n",);     
    }

    fn save_void_state(&self, op_coord: OperatorCoord, snapshot_id: SnapshotId) {
        // Prepare connection
        let mut conn = self.client
            .as_ref()
            .unwrap()
            .get_connection()
            .unwrap_or_else(|e|
                panic!("Fail to connect to Redis: {e:?}")
            );

        // Serialize op_coord
        let op_coord_key_buf = serialize_op_coord(op_coord);

        // Save op_coord -> snap_iter_id (push on list)
        // TODO

        // Save op_coord -> snap_id (push on list)
        conn.lpush(op_coord_key_buf, snapshot_id)
            .unwrap_or_else(|e|
                panic!("Fail to save the state: {e:?}")
            );

        // Log the store
        log::debug!("Saved void state for operator: {op_coord}, at snapshot id: {snapshot_id}\n",);
    }

    fn get_last_snapshot(&self, op_coord: OperatorCoord) -> Option<SnapshotId> {
        // Prepare connection
        let mut conn = self.client
            .as_ref()
            .unwrap()
            .get_connection()
            .unwrap_or_else(|e|
                panic!("Fail to connect to Redis: {e:?}")
            );

        // Serialize op_coord
        let op_coord_key_buf = serialize_op_coord(op_coord);
        
        // Get the last snapshotid
        let snap_id: Option<SnapshotId> = conn.lpop(op_coord_key_buf.clone(), None)
            .unwrap_or_else(|e| {
                panic!("Failed to get last snapshot id of operator: {op_coord}. Error: {e:?}")
            });
        // Check if is Some
        if snap_id.is_some() {
            // Since POP removes the returned element i need to repush it
            conn.lpush(op_coord_key_buf, snap_id.unwrap())
                .unwrap_or_else(|e|
                    panic!("Failed to get last snapshot id of operator: {op_coord}. Error: {e:?}")
                );
        }
        snap_id
    }

    fn get_state<State: ExchangeData>(&self, op_coord: OperatorCoord, snapshot_id: SnapshotId) -> Option<State> {
        // Prepare connection
        let mut conn = self.client
            .as_ref()
            .unwrap()
            .get_connection()
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
        let mut conn = self.client
            .as_ref()
            .unwrap()
            .get_connection()
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
        conn.lrem(op_coord_key_buf, 1, snapshot_id).unwrap_or_else(|e|
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
    use serde::{Serialize, Deserialize};

    use crate::{network::OperatorCoord};

    use super::{PersistencyService, PersistencyServices};

    #[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
    struct FakeState {
        num: u64,
        flag: bool,
        values: Vec<i32>,
        str: String,
    }

    #[ignore]
    #[test]
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

        let mut pers_handler = PersistencyService::new(Some("redis://127.0.0.1".to_string()));
        pers_handler.setup();

        pers_handler.save_state(op_coord1, 1, state1.clone());
        let retrived_state: FakeState = pers_handler.get_state(op_coord1, 1).unwrap();
        assert_eq!(state1, retrived_state);

        let mut state2 = state1.clone();
        state2.values.push(4);

        pers_handler.save_state(op_coord1, 2, state2.clone());
        let retrived_state: FakeState = pers_handler.get_state(op_coord1, 2).unwrap();
        assert_ne!(state1, retrived_state);
        assert_eq!(state2, retrived_state);

        let retrived_state: Option<FakeState> = pers_handler.get_state(op_coord1, 3);
        assert_eq!(None, retrived_state);

        let last = pers_handler.get_last_snapshot(op_coord1).unwrap();
        assert_eq!(2, last);

        // Clean
        pers_handler.delete_state(op_coord1, 1);
        let retrived_state: Option<FakeState> = pers_handler.get_state(op_coord1, 1);
        assert_eq!(None, retrived_state);
        let last = pers_handler.get_last_snapshot(op_coord1).unwrap();
        assert_eq!(2, last);

        pers_handler.delete_state(op_coord1, 2);
        let retrived_state: Option<FakeState> = pers_handler.get_state(op_coord1, 2);
        assert_eq!(None, retrived_state);
        let last = pers_handler.get_last_snapshot(op_coord1);
        assert_eq!(None, last);

        // Clean already cleaned state
        pers_handler.delete_state(op_coord1, 2);
        let retrived_state: Option<FakeState> = pers_handler.get_state(op_coord1, 2);
        assert_eq!(None, retrived_state);
        let last = pers_handler.get_last_snapshot(op_coord1);
        assert_eq!(None, last);

    }

    #[ignore]    
    #[test]
    fn test_save_void_state() {
        let op_coord1 = OperatorCoord {
            block_id: 1,
            host_id: 1,
            replica_id: 1,
            operator_id: 2,
        };

        let mut pers_handler = PersistencyService::new(Some("redis://127.0.0.1".to_string()));
        pers_handler.setup();

        pers_handler.save_void_state(op_coord1, 1);
        pers_handler.save_void_state(op_coord1, 2);
        let retrived_state: Option<FakeState> = pers_handler.get_state(op_coord1, 3);
        assert_eq!(None, retrived_state);

        // Clean
        pers_handler.delete_state(op_coord1, 1);
        let retrived_state: Option<FakeState> = pers_handler.get_state(op_coord1, 1);
        assert_eq!(None, retrived_state);
        let last = pers_handler.get_last_snapshot(op_coord1).unwrap();
        assert_eq!(2, last);

        pers_handler.delete_state(op_coord1, 2);
        let retrived_state: Option<FakeState> = pers_handler.get_state(op_coord1, 2);
        assert_eq!(None, retrived_state);
        let last = pers_handler.get_last_snapshot(op_coord1);
        assert_eq!(None, last);

        pers_handler.save_void_state(op_coord1, 1);
        let last = pers_handler.get_last_snapshot(op_coord1).unwrap();
        assert_eq!(1, last);
        pers_handler.delete_state(op_coord1, 1);
        let last = pers_handler.get_last_snapshot(op_coord1);
        assert_eq!(None, last);

    }

    #[ignore]
    #[test]
    fn test_snapshot_id_consistency() {
        let op_coord1 = OperatorCoord {
            block_id: 1,
            host_id: 1,
            replica_id: 1,
            operator_id: 1,
        };

        let mut pers_handler = PersistencyService::new(Some("redis://127.0.0.1".to_string()));
        pers_handler.setup();

        // Snap_id = 0
        let result = std::panic::catch_unwind(|| pers_handler.save_state(op_coord1, 0, 100));
        assert!(result.is_err());

        // Snap_id = 10
        let result = std::panic::catch_unwind(|| pers_handler.save_void_state(op_coord1, 10));
        assert!(result.is_err());

        pers_handler.save_state(op_coord1, 1, 101);
        pers_handler.save_state(op_coord1, 2, 102);
        pers_handler.save_state(op_coord1, 3, 103);

        // Snap_id = 1
        let result = std::panic::catch_unwind(|| pers_handler.save_void_state(op_coord1, 1));
        assert!(result.is_err());

        // Snap_id = 2
        let result = std::panic::catch_unwind(|| pers_handler.save_void_state(op_coord1, 2));
        assert!(result.is_err());

        // Snap_id = 3
        let result = std::panic::catch_unwind(|| pers_handler.save_void_state(op_coord1, 3));
        assert!(result.is_err());

        // Clean
        pers_handler.delete_state(op_coord1, 1);
        pers_handler.delete_state(op_coord1, 2);
        pers_handler.delete_state(op_coord1, 3);



    }

    #[test]
    fn test_no_persistency() {
        let op_coord1 = OperatorCoord {
            block_id: 1,
            host_id: 1,
            replica_id: 1,
            operator_id: 1,
        };

        let mut pers_handler = PersistencyService::new(None);
        pers_handler.setup();

        let result = std::panic::catch_unwind(|| pers_handler.save_state(op_coord1, 1, 100));
        assert!(result.is_err());

        let result = std::panic::catch_unwind(|| pers_handler.save_void_state(op_coord1, 1));
        assert!(result.is_err());

        let result: Result<Option<u32>, Box<dyn std::any::Any + Send>> = std::panic::catch_unwind(|| pers_handler.get_state(op_coord1, 1));
        assert!(result.is_err());

        let result = std::panic::catch_unwind(|| pers_handler.get_last_snapshot(op_coord1));
        assert!(result.is_err());

        let result = std::panic::catch_unwind(|| pers_handler.delete_state(op_coord1, 1));
        assert!(result.is_err());

    }
    

}