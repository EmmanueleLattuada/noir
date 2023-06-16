use std::fmt::Display;

use crate::block::{BlockStructure, OperatorKind, OperatorStructure};
use crate::network::OperatorCoord;
use crate::operator::sink::{Sink, StreamOutputRef};
use crate::operator::{ExchangeData, Operator, StreamElement};
use crate::persistency::{PersistencyService, PersistencyServices};
use crate::scheduler::{ExecutionMetadata, OperatorId};

#[derive(Debug)]
pub struct CollectVecSink<Out: ExchangeData, PreviousOperators>
where
    PreviousOperators: Operator<Out>,
{
    prev: PreviousOperators,
    operator_coord: OperatorCoord,
    persistency_service: PersistencyService,
    result: Option<Vec<Out>>,
    output: StreamOutputRef<Vec<Out>>,
}

impl<Out: ExchangeData, PreviousOperators> CollectVecSink<Out, PreviousOperators>
where
    PreviousOperators: Operator<Out>,
{
    pub (crate) fn new(prev: PreviousOperators, output: StreamOutputRef<Vec<Out>>) -> Self {
        let op_id = prev.get_op_id() + 1;
        Self {
            prev,
            // This will be set in setup method
            operator_coord: OperatorCoord::new(0, 0, 0, op_id),
            persistency_service: PersistencyService::default(),
            result: Some(Vec::new()),
            output,
        }
    }
}

impl<Out: ExchangeData, PreviousOperators> Display for CollectVecSink<Out, PreviousOperators>
where
    PreviousOperators: Operator<Out>,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} -> CollectVecSink", self.prev)
    }
}

impl<Out: ExchangeData, PreviousOperators> Operator<()> for CollectVecSink<Out, PreviousOperators>
where
    PreviousOperators: Operator<Out>,
{
    fn setup(&mut self, metadata: &mut ExecutionMetadata) {
        self.prev.setup(metadata);

        self.operator_coord.block_id = metadata.coord.block_id;
        self.operator_coord.host_id = metadata.coord.host_id;
        self.operator_coord.replica_id = metadata.coord.replica_id;

        self.persistency_service = metadata.persistency_service.clone();
        self.persistency_service.setup();
        let snapshot_id = self.persistency_service.restart_from_snapshot(self.operator_coord);
        if snapshot_id.is_some() {
            // Get and resume the persisted state
            let opt_state: Option<Option<Vec<Out>>> = self.persistency_service.get_state(self.operator_coord, snapshot_id.unwrap());
            if let Some(state) = opt_state {
                self.result = state;
            } else {
                panic!("No persisted state founded for op: {0}", self.operator_coord);
            } 
        }
    }

    fn next(&mut self) -> StreamElement<()> {
        match self.prev.next() {
            StreamElement::Item(t) | StreamElement::Timestamped(t, _) => {
                // cloned CollectVecSink or already ended stream
                if let Some(result) = self.result.as_mut() {
                    result.push(t);
                }
                StreamElement::Item(())
            }
            StreamElement::Watermark(w) => StreamElement::Watermark(w),
            StreamElement::Terminate => {
                if self.persistency_service.is_active() {
                    // Save terminated state
                    let state = self.result.clone();
                    self.persistency_service.save_terminated_state(self.operator_coord, state);
                }
                if let Some(result) = self.result.take() {
                    *self.output.lock().unwrap() = Some(result);
                }
                StreamElement::Terminate
            }
            StreamElement::FlushBatch => StreamElement::FlushBatch,
            StreamElement::FlushAndRestart => StreamElement::FlushAndRestart,
            StreamElement::Snapshot(snap_id) => {
                // save state
                let state = self.result.clone();
                self.persistency_service.save_state(self.operator_coord, snap_id, state);
                StreamElement::Snapshot(snap_id)
            }
        }
    }

    fn structure(&self) -> BlockStructure {
        let mut operator = OperatorStructure::new::<Out, _>("CollectVecSink");
        let op_id = self.operator_coord.operator_id;
        operator.subtitle = format!("op id: {op_id}");
        operator.kind = OperatorKind::Sink;
        self.prev.structure().add_operator(operator)
    }

    fn get_op_id(&self) -> OperatorId {
        self.operator_coord.operator_id
    }
}

impl<Out: ExchangeData, PreviousOperators> Sink for CollectVecSink<Out, PreviousOperators> where
    PreviousOperators: Operator<Out>
{
}

impl<Out: ExchangeData, PreviousOperators> Clone for CollectVecSink<Out, PreviousOperators>
where
    PreviousOperators: Operator<Out>,
{
    fn clone(&self) -> Self {
        panic!("CollectVecSink cannot be cloned, replication should be 1");
    }
}

#[cfg(test)]
mod tests {
    use itertools::Itertools;
    use serial_test::serial;

    use crate::config::{EnvironmentConfig, PersistencyConfig};
    use crate::environment::StreamEnvironment;
    use crate::network::OperatorCoord;
    use crate::operator::sink::StreamOutputRef;
    use crate::operator::sink::collect_vec::CollectVecSink;
    use crate::operator::{source, StreamElement, SnapshotId};
    use crate::persistency::PersistencyService;
    use crate::test::{FakeOperator, REDIS_TEST_CONFIGURATION};
    use crate::operator::Operator;
    use crate::persistency::PersistencyServices;

    #[test]
    fn collect_vec() {
        let mut env = StreamEnvironment::new(EnvironmentConfig::local(4));
        let source = source::IteratorSource::new(0..10u8);
        let res = env.stream(source).collect_vec();
        env.execute();
        assert_eq!(res.get().unwrap(), (0..10).collect_vec());
    }

    #[test]
    #[serial]
    fn test_collect_vec_persistency_save_state() {
        let mut fake_operator = FakeOperator::empty();
        fake_operator.push(StreamElement::Item(1));
        fake_operator.push(StreamElement::Item(2));
        fake_operator.push(StreamElement::Snapshot(SnapshotId::new(1)));
        fake_operator.push(StreamElement::Item(3));
        fake_operator.push(StreamElement::Item(4));
        fake_operator.push(StreamElement::Snapshot(SnapshotId::new(2)));

        let output = StreamOutputRef::default();
        let mut collect = CollectVecSink::new(fake_operator, output.clone());
        collect.operator_coord = OperatorCoord{
            block_id: 0,
            host_id: 0,
            replica_id: 2,
            operator_id: 2,
        };
        collect.persistency_service = PersistencyService::new(Some(
            PersistencyConfig { 
                server_addr: String::from(REDIS_TEST_CONFIGURATION),
                try_restart: false,
                clean_on_exit: false,
                restart_from: None,
            }
        ));
        collect.persistency_service.setup();

        collect.next();
        collect.next();
        assert_eq!(collect.next(), StreamElement::Snapshot(SnapshotId::new(1)));
        let state: Option<Option<Vec<i32>>> = collect.persistency_service.get_state(collect.operator_coord, SnapshotId::new(1));
        assert_eq!(state.unwrap().unwrap(), [1, 2]);
        collect.next();
        collect.next();
        assert_eq!(collect.next(), StreamElement::Snapshot(SnapshotId::new(2)));
        let state: Option<Option<Vec<i32>>> = collect.persistency_service.get_state(collect.operator_coord, SnapshotId::new(2));
        assert_eq!(state.unwrap().unwrap(), [1, 2, 3, 4]);

        // Clean redis
        collect.persistency_service.delete_state(collect.operator_coord, SnapshotId::new(1));
        collect.persistency_service.delete_state(collect.operator_coord, SnapshotId::new(2));

    }
}
