use bincode::{DefaultOptions, Options, config::{WithOtherTrailing, WithOtherIntEncoding, FixintEncoding, RejectTrailing}};
use once_cell::sync::Lazy;

use crate::{network::OperatorCoord, operator::{SnapshotId, ExchangeData}};

pub mod builder;
pub mod persistency_service;
pub mod state_saver;
pub mod redis_handler;

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

fn serialize_data<D: ExchangeData>(data: D) -> Vec<u8> {
    let data_len = SERIALIZER
            .serialized_size(&data)
            .unwrap_or_else(|e| {
                panic!(
                    "Failed to compute serialized length of provided data. Error: {e:?}",
                )
            });        
        let mut data_buf = Vec::with_capacity(data_len as usize);
        SERIALIZER
            .serialize_into(&mut data_buf, &data)
            .unwrap_or_else(|e| {
                panic!(
                    "Failed to serialize data (was {data_len} bytes). Error: {e:?}",
                )
            });
        assert_eq!(data_buf.len(), data_len as usize);
        data_buf
}

pub(crate) trait PersistencyServices {  
    /// Method to save the state of the operator. State must be serialized
    fn save_state(&self, op_coord: OperatorCoord, snapshot_id: SnapshotId, state: Vec<u8>);
    /// Method to save a void state, used for operators with no state, is necessary to be able to get last valid snapshot a posteriori
    fn save_void_state(&self, op_coord: OperatorCoord, snapshot_id: SnapshotId);
    /// Method to get the state of the operator
    fn get_state<State: ExchangeData>(&self, op_coord: OperatorCoord, snapshot_id: SnapshotId) -> Option<State>;
    /// Method to get the id of the last snapshot done by specified operator
    fn get_last_snapshot(&self, op_coord: OperatorCoord) -> Option<SnapshotId>;
    /// Method to get the iter_stack of the last snapshot with specified index done by specified operator
    fn get_last_snapshot_with_index(&self, op_coord: OperatorCoord, snapshot_index: u64) -> Option<Vec<u64>>;
    /// Method to remove the state associated to specified operator and snapshot id
    fn delete_state(&self, op_coord: OperatorCoord, snapshot_id: SnapshotId);
}


#[cfg(test)]
mod tests {
    use std::panic::AssertUnwindSafe;

    use serde::{Serialize, Deserialize};
    use serial_test::serial;

    use crate::{network::OperatorCoord, test::persistency_config_unit_tests, operator::SnapshotId, persistency::builder::PersistencyBuilder};

    use super::persistency_service::PersistencyService;

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

        let mut pers_builder = PersistencyBuilder::new(Some(
            persistency_config_unit_tests()
        ));
        let mut pers_services: PersistencyService<FakeState> = pers_builder.generate_persistency_service();
        pers_services.save_state(op_coord1, SnapshotId::new(1), state1.clone());
        pers_builder.flush_state_saver();  
        pers_services = pers_builder.generate_persistency_service();      
        let retrived_state: FakeState = pers_services.get_state(op_coord1, SnapshotId::new(1)).unwrap();
        assert_eq!(state1, retrived_state);

        let mut state2 = state1.clone();
        state2.values.push(4);

        pers_services.save_state(op_coord1, SnapshotId::new(2), state2.clone());
        pers_builder.flush_state_saver();  
        pers_services = pers_builder.generate_persistency_service(); 
        let retrived_state: FakeState = pers_services.get_state(op_coord1, SnapshotId::new(2)).unwrap();
        assert_ne!(state1, retrived_state);
        assert_eq!(state2, retrived_state);

        let retrived_state: Option<FakeState> = pers_services.get_state(op_coord1, SnapshotId::new(3));
        assert_eq!(None, retrived_state);

        let last = pers_services.get_last_snapshot(op_coord1).unwrap();
        assert_eq!(SnapshotId::new(2), last);

        // Clean
        pers_services.delete_state(op_coord1, SnapshotId::new(1));
        let retrived_state: Option<FakeState> = pers_services.get_state(op_coord1, SnapshotId::new(1));
        assert_eq!(None, retrived_state);
        let last = pers_services.get_last_snapshot(op_coord1).unwrap();
        assert_eq!(SnapshotId::new(2), last);

        pers_services.delete_state(op_coord1, SnapshotId::new(2));
        let retrived_state: Option<FakeState> = pers_services.get_state(op_coord1, SnapshotId::new(2));
        assert_eq!(None, retrived_state);
        let last = pers_services.get_last_snapshot(op_coord1);
        assert_eq!(None, last);

        // Clean already cleaned state
        pers_services.delete_state(op_coord1, SnapshotId::new(2));
        let retrived_state: Option<FakeState> = pers_services.get_state(op_coord1, SnapshotId::new(2));
        assert_eq!(None, retrived_state);
        let last = pers_services.get_last_snapshot(op_coord1);
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

        let pers_builder = PersistencyBuilder::new(Some(
            persistency_config_unit_tests()
        ));
        let pers_services: PersistencyService<FakeState> = pers_builder.generate_persistency_service();
        pers_services.save_void_state(op_coord1, SnapshotId::new(1));
        pers_services.save_void_state(op_coord1, SnapshotId::new(2));
        let retrived_state: Option<FakeState> = pers_services.get_state(op_coord1, SnapshotId::new(3));
        assert_eq!(None, retrived_state);

        // Clean
        pers_services.delete_state(op_coord1, SnapshotId::new(1));
        let retrived_state: Option<FakeState> = pers_services.get_state(op_coord1, SnapshotId::new(1));
        assert_eq!(None, retrived_state);
        let last = pers_services.get_last_snapshot(op_coord1).unwrap();
        assert_eq!(SnapshotId::new(2), last);

        pers_services.delete_state(op_coord1, SnapshotId::new(2));
        let retrived_state: Option<FakeState> = pers_services.get_state(op_coord1, SnapshotId::new(2));
        assert_eq!(None, retrived_state);
        let last = pers_services.get_last_snapshot(op_coord1);
        assert_eq!(None, last);

        pers_services.save_void_state(op_coord1, SnapshotId::new(1));
        let last = pers_services.get_last_snapshot(op_coord1).unwrap();
        assert_eq!(SnapshotId::new(1), last);
        pers_services.delete_state(op_coord1, SnapshotId::new(1));
        let last = pers_services.get_last_snapshot(op_coord1);
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

        let mut pers_builder = PersistencyBuilder::new(Some(
            persistency_config_unit_tests()
        ));
        let mut pers_services: PersistencyService<u32> = pers_builder.generate_persistency_service();
        // Snap_id = 0
        let result = std::panic::catch_unwind(AssertUnwindSafe(|| pers_services.save_state(op_coord1, SnapshotId::new(0), 100)));
        assert!(result.is_err());

        // Snap_id = 10
        let result = std::panic::catch_unwind(AssertUnwindSafe(|| pers_services.save_void_state(op_coord1, SnapshotId::new(10))));
        assert!(result.is_err());

        pers_services.save_state(op_coord1, SnapshotId::new(1), 101);
        pers_services.save_state(op_coord1, SnapshotId::new(2), 102);
        pers_services.save_state(op_coord1, SnapshotId::new(3), 103);
        pers_builder.flush_state_saver();  
        pers_services = pers_builder.generate_persistency_service();

        // Snap_id = 1
        let result = std::panic::catch_unwind(AssertUnwindSafe(|| pers_services.save_void_state(op_coord1, SnapshotId::new(1))));
        assert!(result.is_err());

        // Snap_id = 2
        let result = std::panic::catch_unwind(AssertUnwindSafe(|| pers_services.save_void_state(op_coord1, SnapshotId::new(2))));
        assert!(result.is_err());

        // Snap_id = 3
        let result = std::panic::catch_unwind(AssertUnwindSafe(|| pers_services.save_void_state(op_coord1, SnapshotId::new(3))));
        assert!(result.is_err());

        // Clean
        pers_services.delete_state(op_coord1, SnapshotId::new(1));
        pers_services.delete_state(op_coord1, SnapshotId::new(2));
        pers_services.delete_state(op_coord1, SnapshotId::new(3));

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

        let mut pers_builder = PersistencyBuilder::new(Some(
            persistency_config_unit_tests()
        ));
        let mut pers_services: PersistencyService<bool> = pers_builder.generate_persistency_service();
        assert_eq!(pers_services.restart_from_snapshot(op_coord1), None);
        pers_builder.compute_last_complete_snapshot(vec![op_coord1, op_coord2, op_coord3]);
        pers_services = pers_builder.generate_persistency_service();
        assert_eq!(pers_services.restart_from_snapshot(op_coord1), None);

        let fake_state = true;
        pers_services.save_state(op_coord1, SnapshotId::new(1), fake_state);
        pers_services.save_state(op_coord2, SnapshotId::new(1), fake_state);
        pers_builder.flush_state_saver();  
        pers_builder.compute_last_complete_snapshot(vec![op_coord1, op_coord2, op_coord3]);
        pers_services = pers_builder.generate_persistency_service();
        assert_eq!(pers_services.restart_from_snapshot(op_coord1), None);

        pers_services.save_state(op_coord3, SnapshotId::new(1), fake_state);
        pers_builder.flush_state_saver();  
        pers_builder.compute_last_complete_snapshot(vec![op_coord1, op_coord2, op_coord3]);
        pers_services = pers_builder.generate_persistency_service();
        assert_eq!(pers_services.restart_from_snapshot(op_coord1), Some(SnapshotId::new(1)));

        pers_services.save_state(op_coord1, SnapshotId::new(2), fake_state);
        pers_services.save_state(op_coord3, SnapshotId::new(2), fake_state);
        pers_builder.flush_state_saver();  
        pers_builder.compute_last_complete_snapshot(vec![op_coord1, op_coord2, op_coord3]);
        pers_services = pers_builder.generate_persistency_service();
        assert_eq!(pers_services.restart_from_snapshot(op_coord1), Some(SnapshotId::new(1)));

        pers_services.save_state(op_coord1, SnapshotId::new(2), fake_state);
        pers_services.save_state(op_coord2, SnapshotId::new(2), fake_state);
        pers_builder.flush_state_saver();  
        pers_builder.compute_last_complete_snapshot(vec![op_coord1, op_coord2, op_coord3]);
        pers_services = pers_builder.generate_persistency_service();
        assert_eq!(pers_services.restart_from_snapshot(op_coord1), Some(SnapshotId::new(2)));

        // Clean
        pers_services.delete_state(op_coord1, SnapshotId::new(1));
        pers_services.delete_state(op_coord2, SnapshotId::new(1));
        pers_services.delete_state(op_coord3, SnapshotId::new(1));
        // Clean
        pers_services.delete_state(op_coord1, SnapshotId::new(2));
        pers_services.delete_state(op_coord2, SnapshotId::new(2));
        pers_services.delete_state(op_coord3, SnapshotId::new(2));

    }

}