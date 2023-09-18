extern crate redis;

use bincode::Options;
use r2d2::Pool;
use redis::{Client, Commands};
use crate::{network::OperatorCoord, operator::{SnapshotId, ExchangeData}, persistency::serialize_data};
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
        let state_buf = serialize_data(state);

        // Serialize op_coord
        let op_coord_key_buf = serialize_op_coord(op_coord);
                
        // Serialize snap_id
        let snap_id_buf= serialize_snapshot_id(snapshot_id.clone());
        
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
        conn.lpush(op_coord_key_buf.clone(), snap_id_buf)
            .unwrap_or_else(|e|
                panic!("Fail to save the state: {e:?}")
            );

        // Another list for the iter_stack
        let iter_stack =  snapshot_id.iteration_stack.clone();
        if iter_stack.len() > 0 {
            // Serialize the iter_stack
            let ser_iter_stack = serialize_data(iter_stack);

            let serial_index = snapshot_id.id().to_be_bytes().to_vec();
            let mut op_snap_id_key_buf = Vec::with_capacity(op_coord_key_buf.len() + serial_index.len());
            op_snap_id_key_buf.append(&mut op_coord_key_buf.clone());
            op_snap_id_key_buf.append(&mut serial_index.clone());
            assert_eq!(op_snap_id_key_buf.len(), op_coord_key_buf.len() + serial_index.len());

            // Save op_coord -> snap_id (push on list)
            conn.lpush(op_snap_id_key_buf, ser_iter_stack)
                .unwrap_or_else(|e|
                    panic!("Fail to save the state: {e:?}")
                );
        }
        
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
        let snap_id_buf= serialize_snapshot_id(snapshot_id.clone());

        // Save op_coord -> snap_id (push on list)
        conn.lpush(op_coord_key_buf.clone(), snap_id_buf)
            .unwrap_or_else(|e|
                panic!("Fail to save the state: {e:?}")
            );

        // Another list for the iter_stack
        let iter_stack =  snapshot_id.iteration_stack.clone();
        if iter_stack.len() > 0 {
            // Serialize the iter_stack
            let ser_iter_stack = serialize_data(iter_stack);

            let serial_index = snapshot_id.id().to_be_bytes().to_vec();
            let mut op_snap_id_key_buf = Vec::with_capacity(op_coord_key_buf.len() + serial_index.len());
            op_snap_id_key_buf.append(&mut op_coord_key_buf.clone());
            op_snap_id_key_buf.append(&mut serial_index.clone());
            assert_eq!(op_snap_id_key_buf.len(), op_coord_key_buf.len() + serial_index.len());

            // Save op_coord -> snap_id (push on list)
            conn.lpush(op_snap_id_key_buf, ser_iter_stack)
                .unwrap_or_else(|e|
                    panic!("Fail to save the state: {e:?}")
                );
        }

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

    
    fn get_last_snapshot_with_index(&self, op_coord: OperatorCoord, snapshot_index: u64) -> Option<Vec<u64>> {
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
        

        let serial_index = snapshot_index.to_be_bytes().to_vec();
        let mut op_snap_id_key_buf = Vec::with_capacity(op_coord_key_buf.len() + serial_index.len());
        op_snap_id_key_buf.append(&mut op_coord_key_buf.clone());
        op_snap_id_key_buf.append(&mut serial_index.clone());
        assert_eq!(op_snap_id_key_buf.len(), op_coord_key_buf.len() + serial_index.len());
        
        // Get the last iter stack
        let opt_iter_stack: Option<Vec<u8>> = conn.lindex(op_snap_id_key_buf.clone(), 0)
        .unwrap_or_else(|e| {
            panic!("Failed to get last snapshot id of operator: {op_coord}. Error: {e:?}")
        });
        // Deserialize iter_stack 
        
        if let Some(iter_stack) = opt_iter_stack {
            // Deserialize the state
            let error_msg = format!("Fail deserialization of iter_stack for operator: {op_coord}");
            let des_iter_stack: Vec<u64> = SERIALIZER
                .deserialize(iter_stack.as_ref())
                .expect(&error_msg);
            return Some(des_iter_stack);
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
        let snap_id_buf= serialize_snapshot_id(snapshot_id.clone());

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
        let snap_id_buf= serialize_snapshot_id(snapshot_id.clone());

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
        conn.lrem(op_coord_key_buf.clone(), 1, snap_id_buf).unwrap_or_else(|e|
            panic!("Fail to delete the state: {e:?}")
        );

        // Remove the iter_stack from iter_stack list 
        let iter_stack =  snapshot_id.iteration_stack.clone();
        if iter_stack.len() > 0 {
            let ser_iter_stack = serialize_data(iter_stack);
            let serial_index = snapshot_id.id().to_be_bytes().to_vec();
            let mut op_snap_id_key_buf = Vec::with_capacity(op_coord_key_buf.len() + serial_index.len());
            op_snap_id_key_buf.append(&mut op_coord_key_buf.clone());
            op_snap_id_key_buf.append(&mut serial_index.clone());
            assert_eq!(op_snap_id_key_buf.len(), op_coord_key_buf.len() + serial_index.len());

            // Remove the iter_stack
            conn.lrem(op_snap_id_key_buf, 1, ser_iter_stack).unwrap_or_else(|e|
                    panic!("Fail to delete the state: {e:?}")
                );
        }
        
        

        // Log the delete
        log::debug!("Deleted state for operator: {op_coord}, at snapshot id: {snapshot_id}\n",);
    }
}
