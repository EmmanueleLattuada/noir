use std::fmt::Display;

use crate::block::{BlockStructure, OperatorKind, OperatorStructure};
use crate::network::OperatorCoord;
use crate::operator::fold::Fold;
use crate::operator::sink::{Sink, StreamOutput, StreamOutputRef};
use crate::operator::{Data, Operator, StreamElement};
use crate::persistency::{PersistencyService, PersistencyServices};
use crate::scheduler::{ExecutionMetadata, OperatorId};
use crate::stream::Stream;

#[derive(Debug)]
pub struct CollectCountSink<PreviousOperators>
where
    PreviousOperators: Operator<usize>,
{
    prev: PreviousOperators,
    operator_coord: OperatorCoord,
    persistency_service: PersistencyService,
    result: usize,
    output: StreamOutputRef<usize>,
}

impl<PreviousOperators> CollectCountSink<PreviousOperators>
where
    PreviousOperators: Operator<usize>,
{
    fn new(prev: PreviousOperators, result: usize, output:StreamOutputRef<usize>) -> Self {
        let op_id = prev.get_op_id() + 1;
        Self {
            prev,
            // This will be set in setup method
            operator_coord: OperatorCoord::new(0, 0, 0, op_id),
            persistency_service: PersistencyService::default(),
            result,
            output,
        }
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

        self.operator_coord.block_id = metadata.coord.block_id;
        self.operator_coord.host_id = metadata.coord.host_id;
        self.operator_coord.replica_id = metadata.coord.replica_id;

        self.persistency_service = metadata.persistency_service.clone();
        self.persistency_service.setup();
        let snapshot_id = self.persistency_service.restart_from_snapshot(self.operator_coord);
        if snapshot_id.is_some() {
            // Get and resume the persisted state
            let opt_state: Option<u64> = self.persistency_service.get_state(self.operator_coord, snapshot_id.unwrap());
            if let Some(state) = opt_state {
                self.result = state as usize;
            } else {
                panic!("No persisted state founded for op: {0}", self.operator_coord);
            } 
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
                if self.persistency_service.is_active() {
                    // Save terminated state
                    let state = self.result as u64;
                    self.persistency_service.save_terminated_state(self.operator_coord, state);
                }
                StreamElement::Terminate
            }
            StreamElement::FlushBatch => StreamElement::FlushBatch,
            StreamElement::FlushAndRestart => StreamElement::FlushAndRestart,
            StreamElement::Snapshot(snap_id) => {
                // save state
                let state = self.result as u64;
                self.persistency_service.save_state(self.operator_coord, snap_id, state);
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
        panic!("CollectVecSink cannot be cloned, max_parallelism should be 1");
    }
}

impl<Out: Data, OperatorChain> Stream<Out, OperatorChain>
where
    OperatorChain: Operator<Out> + 'static,
{
    /// Close the stream and store all the resulting items into a [`Vec`] on a single host.
    ///
    /// If the stream is distributed among multiple replicas, a bottleneck is placed where all the
    /// replicas sends the items to.
    ///
    /// **Note**: the order of items and keys is unspecified.
    ///
    /// **Note**: this operator will split the current block.
    ///
    /// ## Example
    ///
    /// ```
    /// # use noir::{StreamEnvironment, EnvironmentConfig};
    /// # use noir::operator::source::IteratorSource;
    /// # let mut env = StreamEnvironment::new(EnvironmentConfig::local(1));
    /// let s = env.stream(IteratorSource::new((0..10)));
    /// let res = s.collect_vec();
    ///
    /// env.execute();
    ///
    /// assert_eq!(res.get().unwrap(), (0..10).collect::<Vec<_>>());
    /// ```
    pub fn collect_count(self) -> StreamOutput<usize> {
        let output = StreamOutputRef::default();
        self.add_operator(|prev| Fold::new(prev, 0, |acc, _| *acc += 1))
            .max_parallelism(1)
            .add_operator(|prev| CollectCountSink::new(
                prev,
                0,
                output.clone(),
            ))
            .finalize_block();
        StreamOutput { result: output }
    }
}

// impl<Key: ExchangeDataKey, Out: ExchangeData, OperatorChain> KeyedStream<Key, Out, OperatorChain>
// where
//     OperatorChain: Operator<KeyValue<Key, Out>> + 'static,
// {
//     /// Close the stream and store all the resulting items into a [`Vec`] on a single host.
//     ///
//     /// If the stream is distributed among multiple replicas, a bottleneck is placed where all the
//     /// replicas sends the items to.
//     ///
//     /// **Note**: the collected items are the pairs `(key, value)`.
//     ///
//     /// **Note**: the order of items and keys is unspecified.
//     ///
//     /// **Note**: this operator will split the current block.
//     ///
//     /// ## Example
//     ///
//     /// ```
//     /// # use noir::{StreamEnvironment, EnvironmentConfig};
//     /// # use noir::operator::source::IteratorSource;
//     /// # let mut env = StreamEnvironment::new(EnvironmentConfig::local(1));
//     /// let s = env.stream(IteratorSource::new((0..3))).group_by(|&n| n % 2);
//     /// let res = s.collect_vec();
//     ///
//     /// env.execute();
//     ///
//     /// let mut res = res.get().unwrap();
//     /// res.sort_unstable(); // the output order is nondeterministic
//     /// assert_eq!(res, vec![(0, 0), (0, 2), (1, 1)]);
//     /// ```
//     pub fn collect_count(self) -> StreamOutput<Vec<(Key, Out)>> {
//         self.unkey().collect_vec()
//     }
// }

#[cfg(test)]
mod tests {
    use itertools::Itertools;

    use crate::config::EnvironmentConfig;
    use crate::environment::StreamEnvironment;
    use crate::network::OperatorCoord;
    use crate::operator::sink::StreamOutputRef;
    use crate::operator::sink::collect_count::CollectCountSink;
    use crate::operator::{source, StreamElement, Operator, SnapshotId};
    use crate::persistency::{PersistencyService, PersistencyServices};
    use crate::test::{FakeOperator, REDIS_TEST_COFIGURATION};

    #[test]
    fn collect_vec() {
        let mut env = StreamEnvironment::new(EnvironmentConfig::local(4));
        let source = source::IteratorSource::new(0..10u8);
        let res = env.stream(source).collect_vec();
        env.execute();
        assert_eq!(res.get().unwrap(), (0..10).collect_vec());
    }

    #[ignore]
    #[test]
    fn test_collect_count_persistency_save_state() {
        let mut fake_operator = FakeOperator::empty();
        fake_operator.push(StreamElement::Item(1));
        fake_operator.push(StreamElement::Item(2));
        fake_operator.push(StreamElement::Snapshot(SnapshotId::new(1)));
        fake_operator.push(StreamElement::Item(3));
        fake_operator.push(StreamElement::Item(4));
        fake_operator.push(StreamElement::Snapshot(SnapshotId::new(2)));

        let output = StreamOutputRef::default();
        let mut collect = CollectCountSink::new(fake_operator, 0, output.clone());
        collect.operator_coord = OperatorCoord{
            block_id: 0,
            host_id: 0,
            replica_id: 2,
            operator_id: 1,
        };
        collect.persistency_service = PersistencyService::new(Some(String::from(REDIS_TEST_COFIGURATION)));
        collect.persistency_service.setup();

        collect.next();
        collect.next();
        assert_eq!(collect.next(), StreamElement::Snapshot(SnapshotId::new(1)));
        let state: Option<u64> = collect.persistency_service.get_state(collect.operator_coord, SnapshotId::new(1));
        assert_eq!(state.unwrap(), 3);
        collect.next();
        collect.next();
        assert_eq!(collect.next(), StreamElement::Snapshot(SnapshotId::new(2)));
        let state: Option<u64> = collect.persistency_service.get_state(collect.operator_coord, SnapshotId::new(2));
        assert_eq!(state.unwrap(), 10);

        // Clean redis
        collect.persistency_service.delete_state(collect.operator_coord, SnapshotId::new(1));
        collect.persistency_service.delete_state(collect.operator_coord, SnapshotId::new(2));

    }

}
