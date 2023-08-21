extern crate redis;

use bincode::Options;
use r2d2::Pool;
use redis::{Client, Commands};
use crate::{network::OperatorCoord, operator::{SnapshotId, ExchangeData}};
use super::{PersistencyServices, SERIALIZER, KEY_SERIALIZER, serialize_op_coord, serialize_snapshot_id};

/// Redis handler
#[derive(Debug, Clone, Default)]
pub(crate) struct RedisHandler {
    conn_pool: Option<Pool<Client>>,
}

impl RedisHandler {
    /// Create a new Redis handler from given configuration
    pub(super) fn new(config: String) -> Self{
        let client: Client = Client::open(config).unwrap();
        let conn_pool: Pool<Client> = Pool::builder().build(client).unwrap();
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
