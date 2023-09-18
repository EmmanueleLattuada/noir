use std::fmt::Display;

use crate::block::{BlockStructure, OperatorKind, OperatorStructure};
use crate::network::OperatorCoord;
use crate::operator::sink::{Sink, StreamOutputRef};
use crate::operator::{Operator, StreamElement, SnapshotId};
use crate::persistency::persistency_service::PersistencyService;
use crate::scheduler::{ExecutionMetadata, OperatorId};

#[derive(Debug)]
pub struct CollectCountSink<PreviousOperators>
where
    PreviousOperators: Operator<usize>,
{
    prev: PreviousOperators,
    operator_coord: OperatorCoord,
    persistency_service: Option<PersistencyService<u64>>,
    result: usize,
    output: StreamOutputRef<usize>,
}

impl<PreviousOperators> CollectCountSink<PreviousOperators>
where
    PreviousOperators: Operator<usize>,
{
    pub (crate) fn new(prev: PreviousOperators, output:StreamOutputRef<usize>) -> Self {
        let op_id = prev.get_op_id() + 1;
        Self {
            prev,
            // This will be set in setup method
            operator_coord: OperatorCoord::new(0, 0, 0, op_id),
            persistency_service: None,
            result: 0,
            output,
        }
    }

    /// Save state for snapshot
    fn save_snap(&mut self, snapshot_id: SnapshotId){
        let state = self.result as u64;
        self.persistency_service.as_mut().unwrap().save_state(self.operator_coord, snapshot_id, state);
    }
    /// Save terminated state
    fn save_terminate(&mut self){
        let state = self.result as u64;
        self.persistency_service.as_mut().unwrap().save_terminated_state(self.operator_coord, state);
    } 
}

impl<PreviousOperators> Display for CollectCountSink<PreviousOperators>
where
    PreviousOperators: Operator<usize>,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} -> CollectCountSink", self.prev)
    }
}

impl<PreviousOperators> Operator<()> for CollectCountSink<PreviousOperators>
where
    PreviousOperators: Operator<usize>,
{
    fn setup(&mut self, metadata: &mut ExecutionMetadata) {
        self.prev.setup(metadata);

        self.operator_coord.from_coord(metadata.coord);

        if let Some(pb) = &metadata.persistency_builder{
            let p_service = pb.generate_persistency_service::<u64>();
            let snapshot_id = p_service.restart_from_snapshot(self.operator_coord);
            if snapshot_id.is_some() {
                // Get and resume the persisted state
                let opt_state: Option<u64> = p_service.get_state(self.operator_coord, snapshot_id.unwrap());
                if let Some(state) = opt_state {
                    self.result = state as usize;
                } else {
                    panic!("No persisted state founded for op: {0}", self.operator_coord);
                } 
            }
            self.persistency_service = Some(p_service);
        }
    }

    fn next(&mut self) -> StreamElement<()> {
        match self.prev.next() {
            StreamElement::Item(c) | StreamElement::Timestamped(c, _) => {
                self.result += c;
                StreamElement::Item(())
            }
            StreamElement::Watermark(w) => StreamElement::Watermark(w),
            StreamElement::Terminate => {
                *self.output.lock().unwrap() = Some(self.result);
                if self.persistency_service.is_some() {
                    self.save_terminate();
                }
                StreamElement::Terminate
            }
            StreamElement::FlushBatch => StreamElement::FlushBatch,
            StreamElement::FlushAndRestart => StreamElement::FlushAndRestart,
            StreamElement::Snapshot(snap_id) => {
                self.save_snap(snap_id.clone());
                StreamElement::Snapshot(snap_id)
            }
        }
    }

    fn structure(&self) -> BlockStructure {
        let mut operator = OperatorStructure::new::<usize, _>("CollectCountSink");
        let op_id = self.operator_coord.operator_id;
        operator.subtitle = format!("op id: {op_id}");
        operator.kind = OperatorKind::Sink;
        self.prev.structure().add_operator(operator)
    }

    fn get_op_id(&self) -> OperatorId {
        self.operator_coord.operator_id
    }
}

impl<PreviousOperators> Sink for CollectCountSink<PreviousOperators> where
    PreviousOperators: Operator<usize>
{
}

impl<PreviousOperators> Clone for CollectCountSink<PreviousOperators>
where
    PreviousOperators: Operator<usize>,
{
    fn clone(&self) -> Self {
        panic!("CollectVecSink cannot be cloned, replication should be 1");
    }
}

#[cfg(test)]
mod tests {
    use itertools::Itertools;
    use serial_test::serial;

    use crate::config::EnvironmentConfig;
    use crate::environment::StreamEnvironment;
    use crate::network::OperatorCoord;
    use crate::operator::sink::StreamOutputRef;
    use crate::operator::sink::collect_count::CollectCountSink;
    use crate::operator::{source, StreamElement, Operator, SnapshotId};
    use crate::persistency::builder::PersistencyBuilder;
    use crate::test::{FakeOperator, persistency_config_unit_tests};

    #[test]
    fn collect_vec() {
        let mut env = StreamEnvironment::new(EnvironmentConfig::local(4));
        let source = source::IteratorSource::new(0..10u8);
        let res = env.stream(source).collect_vec();
        env.execute_blocking();
        assert_eq!(res.get().unwrap(), (0..10).collect_vec());
    }

    #[test]
    #[serial]
    fn test_collect_count_persistency_save_state() {
        let mut fake_operator = FakeOperator::empty();
        fake_operator.push(StreamElement::Item(1));
        fake_operator.push(StreamElement::Item(2));
        fake_operator.push(StreamElement::Snapshot(SnapshotId::new(1)));
        fake_operator.push(StreamElement::Item(3));
        fake_operator.push(StreamElement::Item(4));
        fake_operator.push(StreamElement::Snapshot(SnapshotId::new(2)));

        let output = StreamOutputRef::default();
        let mut collect = CollectCountSink::new(fake_operator, output.clone());
        collect.operator_coord = OperatorCoord{
            block_id: 0,
            host_id: 0,
            replica_id: 2,
            operator_id: 1,
        };
        
        let pers_builder = PersistencyBuilder::new(Some(persistency_config_unit_tests()));
        collect.persistency_service = Some(pers_builder.generate_persistency_service());

        collect.next();
        collect.next();
        assert_eq!(collect.next(), StreamElement::Snapshot(SnapshotId::new(1)));
        collect.persistency_service.as_mut().unwrap().flush_state_saver();
        let state: Option<u64> = collect.persistency_service.as_mut().unwrap().get_state(collect.operator_coord, SnapshotId::new(1));
        assert_eq!(state.unwrap(), 3);
        collect.next();
        collect.next();
        assert_eq!(collect.next(), StreamElement::Snapshot(SnapshotId::new(2)));
        collect.persistency_service.as_mut().unwrap().flush_state_saver();
        let state: Option<u64> = collect.persistency_service.as_mut().unwrap().get_state(collect.operator_coord, SnapshotId::new(2));
        assert_eq!(state.unwrap(), 10);

        // Clean redis
        collect.persistency_service.as_mut().unwrap().delete_state(collect.operator_coord, SnapshotId::new(1));
        collect.persistency_service.as_mut().unwrap().delete_state(collect.operator_coord, SnapshotId::new(2));

    }

}
