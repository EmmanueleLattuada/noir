use std::fmt::Display;

use futures::{Stream, StreamExt};

use crate::block::{BlockStructure, OperatorKind, OperatorStructure};
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
        }
    }
}

impl<Out: Data, S> Source<Out> for AsyncStreamSource<Out, S>
where
    S: Stream<Item = Out> + Send + Unpin + 'static,
{
    fn get_max_parallelism(&self) -> Option<usize> {
        Some(1)
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
    }

    fn next(&mut self) -> StreamElement<Out> {
        if self.terminated {
            return StreamElement::Terminate;
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
        panic!("AsyncStreamSource cannot be cloned, max_parallelism should be 1");
    }
}
