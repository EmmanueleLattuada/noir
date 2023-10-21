extern crate redis;

use super::{
    serialize_op_coord, serialize_snapshot_id, PersistencyServices, KEY_SERIALIZER, SERIALIZER,
};
use crate::{
    network::OperatorCoord,
    operator::{ExchangeData, SnapshotId},
    persistency::serialize_data,
};
use bincode::Options;
use r2d2::Pool;
use redis::{Client, Commands};

/// Redis handler
#[derive(Debug, Clone)]
pub(crate) struct RedisHandler {
    pool: Pool<Client>,
}

impl RedisHandler {
    /// Create a new Redis handler from given configuration
    pub(super) fn new(config: String) -> Self {
        let client: Client = Client::open(config).unwrap();
        let conn_pool: Pool<Client> = Pool::builder().build(client).unwrap();
        Self { pool: conn_pool }
    }
}

impl PersistencyServices for RedisHandler {
    fn save_state(&self, op_coord: &OperatorCoord, snapshot_id: &SnapshotId, state_buf: Vec<u8>) {
        let mut conn = self.pool.get().expect("Fail to connect to Redis");

        // let state_buf = serialize_data(state);
        let op_coord_key_buf = serialize_op_coord(op_coord);
        let snap_id_buf = serialize_snapshot_id(&snapshot_id);

        // Save op_coord + snap_id -> state
        let mut op_snap_key_buf =
            Vec::<u8>::with_capacity(op_coord_key_buf.len() + snap_id_buf.len());
        op_snap_key_buf.extend_from_slice(op_coord_key_buf.as_slice());
        op_snap_key_buf.extend_from_slice(snap_id_buf.as_slice());

        let _: () = redis::pipe()
            .set(op_snap_key_buf, state_buf)
            .lpush(op_coord_key_buf.clone(), snap_id_buf)
            .query(&mut *conn)
            .expect("Fail to save the state");

        // Another list for the iter_stack
        let iter_stack = snapshot_id.iteration_stack.clone();
        if iter_stack.len() > 0 {
            // Serialize the iter_stack
            let ser_iter_stack = serialize_data(&iter_stack);

            let serial_index = snapshot_id.id().to_be_bytes().to_vec();
            let mut op_snap_id_key_buf =
                Vec::with_capacity(op_coord_key_buf.len() + serial_index.len());
            op_snap_id_key_buf.extend_from_slice(op_coord_key_buf.as_slice());
            op_snap_id_key_buf.extend_from_slice(serial_index.as_slice());

            // Save op_coord -> snap_id (push on list)
            let _: () = conn
                .lpush(op_snap_id_key_buf, ser_iter_stack)
                .expect("Fail to save the state");
        }

        // Log the store
        log::debug!("Saved state for operator: {op_coord}, at snapshot id: {snapshot_id}\n",);
    }

    fn get_last_snapshot(&self, op_coord: &OperatorCoord) -> Option<SnapshotId> {
        let mut conn = self.pool.get().expect("Fail to connect to Redis");
        let op_coord_key_buf = serialize_op_coord(&op_coord);

        // Get the last snapshotid
        let ser_snap_id: Option<Vec<u8>> =
            conn.lindex(op_coord_key_buf.clone(), 0)
                .unwrap_or_else(|e| {
                    panic!("Failed to get last snapshot id of operator: {op_coord}. Error {e:?}")
                });
        // Check if is Some
        if let Some(snap_id) = ser_snap_id {
            // Deserialize the snapshot_id
            let error_msg = format!("Fail deserialization of snapshot id");
            let snapshot_id: SnapshotId = KEY_SERIALIZER
                .deserialize(snap_id.as_ref())
                .expect(&error_msg);

            return Some(snapshot_id);
        }
        None
    }

    fn get_last_iter_stack(
        &self,
        op_coord: &OperatorCoord,
        snapshot_index: u64,
    ) -> Option<Vec<u64>> {
        let mut conn = self.pool.get().expect("Fail to connect to Redis");

        // Serialize op_coord
        let op_coord_key_buf = serialize_op_coord(op_coord);

        let serial_index = snapshot_index.to_be_bytes().to_vec();
        let mut op_snap_id_key_buf =
            Vec::with_capacity(op_coord_key_buf.len() + serial_index.len());
        op_snap_id_key_buf.extend_from_slice(op_coord_key_buf.as_slice());
        op_snap_id_key_buf.extend_from_slice(serial_index.as_slice());

        // Get the last iter stack
        let opt_iter_stack: Option<Vec<u8>> = conn
            .lindex(op_snap_id_key_buf.clone(), 0)
            .unwrap_or_else(|e| {
                panic!("Failed to get last snapshot id of operator: {op_coord}. Error {e:?}")
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

    fn get_state<State: ExchangeData>(
        &self,
        op_coord: &OperatorCoord,
        snapshot_id: &SnapshotId,
    ) -> Option<State> {
        // Prepare connection
        let mut conn = self.pool.get().expect("Redis connection error");

        // Serialize op_coord
        let op_coord_key_buf = serialize_op_coord(op_coord);

        // Serialize snap_id
        let snap_id_buf = serialize_snapshot_id(snapshot_id);

        // Get op_coord + snap_id -> state
        let mut op_snap_key_buf = Vec::with_capacity(op_coord_key_buf.len() + snap_id_buf.len());
        op_snap_key_buf.extend_from_slice(op_coord_key_buf.as_slice());
        op_snap_key_buf.extend_from_slice(snap_id_buf.as_slice());

        let res = conn
            .get::<Vec<u8>, Vec<u8>>(op_snap_key_buf)
            .expect("Fail to get the state");

        if res.len() == 0 {
            return None;
        }

        // Deserialize the state
        let error_msg = format!(
            "Fail deserialization of state for operator: {op_coord}, at snapshot id: {snapshot_id}"
        );
        let msg: State = SERIALIZER.deserialize(res.as_ref()).expect(&error_msg);

        Some(msg)
    }

    fn delete_state(&self, op_coord: &OperatorCoord, snapshot_id: &SnapshotId) {
        // Prepare connection
        let mut conn = self.pool.get().expect("Redis connection error");

        let op_coord_key_buf = serialize_op_coord(op_coord);
        let snap_id_buf = serialize_snapshot_id(&snapshot_id);

        // Compute key op_coord + snap_id
        let mut op_snap_key_buf = Vec::with_capacity(op_coord_key_buf.len() + snap_id_buf.len());
        op_snap_key_buf.extend_from_slice(op_coord_key_buf.as_slice());
        op_snap_key_buf.extend_from_slice(snap_id_buf.as_slice());

        // Delete key op_coord + snap_id
        conn.del::<Vec<u8>, u64>(op_snap_key_buf.clone())
            .expect("Fail to delete the state");
        let exists: u64 = conn
            .exists(op_snap_key_buf)
            .expect("Fail to delete the state");
        if exists != 0 {
            panic!(
                "Fail to delete the state for operator: {op_coord} and snapshot_id: {snapshot_id}"
            );
        }

        // Remove op_coord from list
        let _: () = conn
            .lrem(op_coord_key_buf.clone(), 1, snap_id_buf)
            .expect("Fail to delete the state");

        // Remove the iter_stack from iter_stack list
        let iter_stack = snapshot_id.iteration_stack.clone();
        if iter_stack.len() > 0 {
            let ser_iter_stack = serialize_data(&iter_stack);
            let serial_index = snapshot_id.id().to_be_bytes().to_vec();
            let mut op_snap_id_key_buf =
                Vec::with_capacity(op_coord_key_buf.len() + serial_index.len());
            op_snap_id_key_buf.extend_from_slice(op_coord_key_buf.as_slice());
            op_snap_id_key_buf.extend_from_slice(serial_index.as_slice());

            // Remove the iter_stack
            let _: () = conn
                .lrem(op_snap_id_key_buf, 1, ser_iter_stack)
                .expect("Fail to delete the state");
        }

        // Log the delete
        log::debug!("Deleted state for operator: {op_coord}, at snapshot id: {snapshot_id}\n",);
    }
}

/// Function for tests and benchmarks
/// Try to get the number of persisted snapshots then flush all redis db
/// it does NOT work with iterative computations
pub fn get_max_snapshot_id_and_flushall(server_addr: String) -> u64 {
    let handler = RedisHandler::new(server_addr);
    let mut result = 0;
    let mut op_coord = OperatorCoord {
        block_id: 0,
        host_id: 0,
        replica_id: 0,
        operator_id: 0,
    };
    // Find the last snapshot of the last block
    loop {
        let opt_snap_id = handler.get_last_snapshot(&op_coord);
        if let Some(snap_id) = opt_snap_id {
            if snap_id.id() > result {
                result = snap_id.id();
            }
            op_coord.block_id = op_coord.block_id + 1;
        } else {
            break;
        }
    }
    // Clear all db
    // Prepare connection
    let mut conn = handler.pool.get().expect("Redis connection error");
    redis::cmd("FLUSHALL").query::<String>(&mut *conn).unwrap();
    result
}
