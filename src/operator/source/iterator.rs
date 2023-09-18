use std::fmt::Display;

use serde::{Serialize, Deserialize};

use crate::block::{BlockStructure, OperatorKind, OperatorStructure, Replication};
use crate::network::OperatorCoord;
use crate::operator::source::Source;
use crate::operator::{Data, Operator, StreamElement};
use crate::persistency::persistency_service::PersistencyService;
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
    persistency_service: Option<PersistencyService<IteratorSourceState>>,

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
            inner,
            terminated: false,
            last_index: None,
            snapshot_generator: SnapshotGenerator::new(),
            persistency_service: None,
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
    fn replication(&self) -> Replication {
        Replication::One
    }
}

#[derive(Clone, Serialize, Deserialize, Debug)]
struct IteratorSourceState{
    last_index: Option<u64>,
}

impl<Out: Data, It> Operator<Out> for IteratorSource<Out, It>
where
    It: Iterator<Item = Out> + Send + 'static,
{
    fn setup(&mut self, metadata: &mut ExecutionMetadata) {
        self.operator_coord.from_coord(metadata.coord);
        if let Some(pb) = &metadata.persistency_builder{
            let p_service = pb.generate_persistency_service::<IteratorSourceState>();
            let snapshot_id =p_service.restart_from_snapshot(self.operator_coord);
            if let Some(snap_id) = snapshot_id {
                // Get and resume the persisted state
                let opt_state: Option<IteratorSourceState> =p_service.get_state(self.operator_coord, snap_id.clone());
                if let Some(state) = opt_state {
                    self.terminated = snap_id.terminate();
                    if let Some(idx) = state.last_index {
                        self.inner.nth(idx as usize);
                        self.last_index = Some(idx);
                    }
                } else {
                    panic!("No persisted state founded for op: {0} and snapshot id: {snap_id:?}", self.operator_coord);
                } 
                self.snapshot_generator.restart_from(snap_id);
            }
            if let Some(snap_freq) = p_service.snapshot_frequency_by_item {
                self.snapshot_generator.set_item_interval(snap_freq);
            }
            if let Some(snap_freq) = p_service.snapshot_frequency_by_time {
                self.snapshot_generator.set_time_interval(snap_freq);
            }
            self.persistency_service = Some(p_service);
        }
    }

    fn next(&mut self) -> StreamElement<Out> {
        if self.terminated {
            if self.persistency_service.is_some() {
                // Save terminated state
                let state = IteratorSourceState{
                    last_index: self.last_index,
                };
                self.persistency_service.as_mut().unwrap().save_terminated_state(self.operator_coord, state);
            }
            return StreamElement::Terminate;   
        }
        if self.persistency_service.is_some(){
            // Check snapshot generator
            let snapshot = self.snapshot_generator.get_snapshot_marker();
            if snapshot.is_some() {
                let snapshot_id = snapshot.unwrap();
                // Save state and forward snapshot marker
                let state = IteratorSourceState{
                    last_index: self.last_index,
                };
                self.persistency_service.as_mut().unwrap().save_state(self.operator_coord, snapshot_id.clone(), state);
                return StreamElement::Snapshot(snapshot_id);
            }
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
        panic!("IteratorSource cannot be cloned, replication should be 1");
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
