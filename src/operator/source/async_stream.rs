use std::fmt::Display;
use std::time::Duration;

use futures::{Stream, StreamExt};

use crate::block::{BlockStructure, OperatorKind, OperatorStructure, Replication};
use crate::operator::source::Source;
use crate::operator::{Data, Operator, StreamElement};
use crate::scheduler::ExecutionMetadata;

/// Source that consumes an iterator and emits all its elements into the stream.
///
/// The iterator will be consumed **only from one replica**, therefore this source is not parallel.

#[derive(Derivative)]
#[derivative(Debug)]
pub struct AsyncStreamSource<Out: Data, S>
where
    S: Stream<Item = Out> + Send + Unpin + 'static,
{
    #[derivative(Debug = "ignore")]
    inner: S,
    terminated: bool,

    operator_coord: OperatorCoord,
    snapshot_generator: SnapshotGenerator,
    persistency_service: PersistencyService,
}

impl<Out: Data, S> Display for AsyncStreamSource<Out, S>
where
    S: Stream<Item = Out> + Send + Unpin + 'static,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "StreamSource<{}>", std::any::type_name::<Out>())
    }
}

impl<Out: Data, S> AsyncStreamSource<Out, S>
where
    S: Stream<Item = Out> + Send + Unpin + 'static,
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
    /// # use noir::operator::source::AsyncStreamSource;
    /// # let mut env = StreamEnvironment::new(EnvironmentConfig::local(1));
    /// let stream = futures::stream::iter(0..10u32);
    /// let source = AsyncStreamSource::new(stream);
    /// let s = env.stream(source);
    /// ```
    pub fn new(inner: S) -> Self {
        Self {
            inner,
            terminated: false,
            // This is the first operator in the chain
            // This will be set in setup method
            operator_coord: OperatorCoord::new(0, 0, 0, 0),
            snapshot_generator: SnapshotGenerator::new(),
            persistency_service: PersistencyService::default(),
        }
    }
}

impl<Out: Data, S> Source<Out> for AsyncStreamSource<Out, S>
where
    S: Stream<Item = Out> + Send + Unpin + 'static,
{
    fn replication(&self) -> Replication {
        Replication::One
    }

    fn set_snapshot_frequency_by_item(&self, item_interval: u64) {
        self.snapshot_generator.set_snapshot_frequency_by_item(item_interval);
    }

    fn set_snapshot_frequency_by_time(&self, time_interval: Duration) {
        self.snapshot_generator.set_snapshot_frequency_by_time(time_interval);
    }
}

impl<Out: Data, S> Operator<Out> for AsyncStreamSource<Out, S>
where
    S: Stream<Item = Out> + Send + Unpin + 'static,
{
    fn setup(&mut self, _metadata: &mut ExecutionMetadata) {
        self.operator_coord.block_id = metadata.coord.block_id;
        self.operator_coord.host_id = metadata.coord.host_id;
        self.operator_coord.replica_id = metadata.coord.replica_id;

        self.persistency_service = metadata.persistency_service.clone();
        self.persistency_service.setup();
        let snapshot_id = self.persistency_service.restart_from_snapshot(self.operator_coord);
        if let Some(snap_id) = snapshot_id {
            self.terminate = snap_id.terminate();
            self.snapshot_generator.restart_from(snap_id);
        }
    }

    fn next(&mut self) -> StreamElement<Out> {
        if self.terminated {
            if self.persistency_service.is_active() {
                // TODO: what is the state ?
                // Save terminated state
                self.persistency_service.save_terminated_void_state(self.operator_coord);
            }
            return StreamElement::Terminate;
        }
        // Check snapshot generator
        let snapshot = self.snapshot_generator.get_snapshot_marker();
        if snapshot.is_some() {
            let snapshot_id = snapshot.unwrap();
            // Save state and forward snapshot marker
            // TODO: what is the state ?
            self.persistency_service.save_void_state(self.operator_coord, snapshot_id);
            return StreamElement::Snapshot(snapshot_id);
        }
        // TODO: with adaptive batching this does not work since S never emits FlushBatch messages
        let rt = tokio::runtime::Handle::current();
        match rt.block_on(self.inner.next()) {
            Some(t) => StreamElement::Item(t),
            None => {
                self.terminated = true;
                StreamElement::FlushAndRestart
            }
        }
    }

    fn structure(&self) -> BlockStructure {
        let mut operator = OperatorStructure::new::<Out, _>("AsyncStreamSource");
        let op_id = self.operator_coord.operator_id;
        operator.subtitle = format!("op id: {op_id}");
        operator.kind = OperatorKind::Source;
        BlockStructure::default().add_operator(operator)
    }

    fn get_op_id(&self) -> OperatorId {
        self.operator_coord.operator_id
    }
}

impl<Out: Data, S> Clone for AsyncStreamSource<Out, S>
where
    S: Stream<Item = Out> + Send + Unpin + 'static,
{
    fn clone(&self) -> Self {
        // Since this is a non-parallel source, we don't want the other replicas to emit any value
        panic!("AsyncStreamSource cannot be cloned, replication should be 1");
    }
}
