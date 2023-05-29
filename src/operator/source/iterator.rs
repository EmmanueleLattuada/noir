use std::fmt::Display;
use std::time::Duration;

use serde::{Serialize, Deserialize};

use crate::block::{BlockStructure, OperatorKind, OperatorStructure};
use crate::network::OperatorCoord;
use crate::operator::source::Source;
use crate::operator::{Data, Operator, StreamElement};
use crate::persistency::{PersistencyService, PersistencyServices};
use crate::scheduler::{ExecutionMetadata, OperatorId};
use crate::Stream;

use super::SnapshotGenerator;

/// Source that consumes an iterator and emits all its elements into the stream.
///
/// The iterator will be consumed **only from one replica**, therefore this source is not parallel.
#[derive(Derivative)]
#[derivative(Debug)]
pub struct IteratorSource<Out: Data, It>
where
    It: Iterator<Item = Out> + Send + 'static,
{
    #[derivative(Debug = "ignore")]
    inner: It,
    terminated: bool,
    last_index: Option<u64>,
    snapshot_generator: SnapshotGenerator,
    persistency_service: PersistencyService,

    operator_coord: OperatorCoord,
}

impl<Out: Data, It> Display for IteratorSource<Out, It>
where
    It: Iterator<Item = Out> + Send + 'static,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "IteratorSource<{}>", std::any::type_name::<Out>())
    }
}

impl<Out: Data, It> IteratorSource<Out, It>
where
    It: Iterator<Item = Out> + Send + 'static,
{
    /// Create a new source that reads the items from the iterator provided as input.
    ///
    /// **Note**: this source is **not parallel**, the iterator will be consumed only on a single
    /// replica, on all the others no item will be read from the iterator. If you want to achieve
    /// parallelism you need to add an operator that shuffles the data (e.g.
    /// [`Stream::shuffle`](crate::Stream::shuffle)).
    ///
    /// ## Example
    ///
    /// ```
    /// # use noir::{StreamEnvironment, EnvironmentConfig};
    /// # use noir::operator::source::IteratorSource;
    /// # let mut env = StreamEnvironment::new(EnvironmentConfig::local(1));
    /// let source = IteratorSource::new((0..5));
    /// let s = env.stream(source);
    /// ```
    pub fn new(inner: It) -> Self {
        Self {
            inner: inner,
            terminated: false,
            last_index: None,
            snapshot_generator: SnapshotGenerator::new(),
            persistency_service: PersistencyService::default(),
            // This is the first operator in the chain so operator_id = 0
            // Other fields will be set inside setup method
            operator_coord: OperatorCoord::new(0, 0, 0, 0),
        }
    }
}

impl<Out: Data, It> Source<Out> for IteratorSource<Out, It>
where
    It: Iterator<Item = Out> + Send + 'static,
{
    fn get_max_parallelism(&self) -> Option<usize> {
        Some(1)
    }

    fn set_snapshot_frequency_by_item(&mut self, item_interval: u64) {
        self.snapshot_generator.set_item_interval(item_interval);
    }

    fn set_snapshot_frequency_by_time(&mut self, time_interval: Duration) {
        self.snapshot_generator.set_time_interval(time_interval);
    }
}

#[derive(Clone, Serialize, Deserialize)]
struct IteratorSourceState{
    last_index: Option<u64>,
}

impl<Out: Data, It> Operator<Out> for IteratorSource<Out, It>
where
    It: Iterator<Item = Out> + Send + 'static,
{
    fn setup(&mut self, metadata: &mut ExecutionMetadata) {
        self.operator_coord.block_id = metadata.coord.block_id;
        self.operator_coord.host_id = metadata.coord.host_id;
        self.operator_coord.replica_id = metadata.coord.replica_id;

        self.persistency_service = metadata.persistency_service.clone();
        self.persistency_service.setup();
        let snapshot_id = self.persistency_service.restart_from_snapshot(self.operator_coord);
        if let Some(snap_id) = snapshot_id {
            // Get and resume the persisted state
            let opt_state: Option<IteratorSourceState> = self.persistency_service.get_state(self.operator_coord, snap_id);
            if let Some(state) = opt_state {
                self.terminated = snap_id.terminate();
                if state.last_index.is_some() {
                    self.inner.nth(state.last_index.unwrap() as usize);
                }
            } else {
                panic!("No persisted state founded for op: {0}", self.operator_coord);
            } 
            self.snapshot_generator.restart_from(snap_id);
        }
    }

    fn next(&mut self) -> StreamElement<Out> {
        if self.terminated {
            if self.persistency_service.is_active() {
                // Save terminated state
                let state = IteratorSourceState{
                    last_index: self.last_index,
                };
                self.persistency_service.save_terminated_state(self.operator_coord, state);
            }
            return StreamElement::Terminate;   
        }
        // Check snapshot generator
        let snapshot = self.snapshot_generator.get_snapshot_marker();
        if snapshot.is_some() {
            let snapshot_id = snapshot.unwrap();
            // Save state and forward snapshot marker
            let state = IteratorSourceState{
                last_index: self.last_index,
            };
            self.persistency_service.save_state(self.operator_coord, snapshot_id, state);
            return StreamElement::Snapshot(snapshot_id);
        }

        // TODO: with adaptive batching this does not work since it never emits FlushBatch messages
        match self.inner.next() {
            Some(t) => {
                if self.last_index.is_none() {
                    self.last_index = Some(0);
                } else {
                    self.last_index = Some(self.last_index.unwrap() + 1);
                }
                StreamElement::Item(t) 
            }
            None => {
                self.terminated = true;
                StreamElement::FlushAndRestart
            }
        }
    }

    fn structure(&self) -> BlockStructure {
        let mut operator = OperatorStructure::new::<Out, _>("IteratorSource");
        let op_id = self.operator_coord.operator_id;
        operator.subtitle = format!("op id: {op_id}");
        operator.kind = OperatorKind::Source;
        BlockStructure::default().add_operator(operator)
    }

    fn get_op_id(&self) -> OperatorId {
        self.operator_coord.operator_id
    }
}

impl<Out: Data, It> Clone for IteratorSource<Out, It>
where
    It: Iterator<Item = Out> + Send + 'static,
{
    fn clone(&self) -> Self {
        // Since this is a non-parallel source, we don't want the other replicas to emit any value
        panic!("IteratorSource cannot be cloned, max_parallelism should be 1");
    }
}

impl crate::StreamEnvironment {
    /// Convenience method, creates a `IteratorSource` and makes a stream using `StreamEnvironment::stream`
    pub fn stream_iter<Out: Data, It: Iterator<Item = Out> + Send + 'static>(
        &mut self,
        iterator: It,
    ) -> Stream<Out, IteratorSource<Out, It>> {
        let source = IteratorSource::new(iterator);
        self.stream(source)
    }
}
