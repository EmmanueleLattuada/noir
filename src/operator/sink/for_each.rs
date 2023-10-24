use std::fmt::Display;
use std::marker::PhantomData;

use crate::block::{BlockStructure, OperatorKind, OperatorStructure};
use crate::network::OperatorCoord;
use crate::operator::sink::Sink;
use crate::operator::{Data, DataKey, Operator, StreamElement};
use crate::scheduler::{ExecutionMetadata, OperatorId};
use crate::stream::{KeyedStream, Stream};

#[derive(Clone, Derivative)]
#[derivative(Debug)]
pub struct ForEach<Out: Data, F, PreviousOperators>
where
    F: FnMut(Out) + Send + Clone,
    PreviousOperators: Operator<Out>,
{
    prev: PreviousOperators,
    operator_coord: OperatorCoord,
    #[derivative(Debug = "ignore")]
    f: F,
    _out: PhantomData<Out>,
}

impl<Out: Data, F, PreviousOperators> ForEach<Out, F, PreviousOperators>
where
    F: FnMut(Out) + Send + Clone,
    PreviousOperators: Operator<Out>,
{
    pub (crate) fn new(prev: PreviousOperators, f: F) -> Self {
        let op_id = prev.get_op_id() + 1;
        Self {
            prev,
            // This will be set in setup method
            operator_coord: OperatorCoord::new(0, 0, 0, op_id),
            f,
            _out: PhantomData,
        }
    }
}

impl<Out: Data, F, PreviousOperators> Display for ForEach<Out, F, PreviousOperators>
where
    F: FnMut(Out) + Send + Clone,
    PreviousOperators: Operator<Out>,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} -> ForEach", self.prev)
    }
}

impl<Out: Data, F, PreviousOperators> Operator<()> for ForEach<Out, F, PreviousOperators>
where
    F: FnMut(Out) + Send + Clone,
    PreviousOperators: Operator<Out>,
{
    fn setup(&mut self, metadata: &mut ExecutionMetadata) {
        self.prev.setup(metadata);
        self.operator_coord.setup_coord(metadata.coord);
    }

    fn next(&mut self) -> StreamElement<()> {
        loop {
            match self.prev.next() {
                StreamElement::Item(t) | StreamElement::Timestamped(t, _) => {
                    (self.f)(t);
                }
                StreamElement::Watermark(w) => return StreamElement::Watermark(w),
                StreamElement::Terminate => return StreamElement::Terminate,
                StreamElement::FlushBatch => return StreamElement::FlushBatch,
                StreamElement::FlushAndRestart => return StreamElement::FlushAndRestart,
                StreamElement::Snapshot(snap_id) => return StreamElement::Snapshot(snap_id),
            }
        }
    }

    fn structure(&self) -> BlockStructure {
        let mut operator = OperatorStructure::new::<Out, _>("ForEachSink");
        let op_id = self.operator_coord.operator_id;
        operator.subtitle = format!("op id: {op_id}");
        operator.kind = OperatorKind::Sink;
        self.prev.structure().add_operator(operator)
    }

    fn get_op_id(&self) -> OperatorId {
        self.operator_coord.operator_id
    }

    fn get_stateful_operators(&self) -> Vec<OperatorId> {
        // This operator is stateless
        self.prev.get_stateful_operators()
    }
}

impl<Out: Data, F, PreviousOperators> Sink for ForEach<Out, F, PreviousOperators>
where
    F: FnMut(Out) + Send + Clone,
    PreviousOperators: Operator<Out>,
{
}

impl<Out: Data, OperatorChain> Stream<Out, OperatorChain> where
    OperatorChain: Operator<Out> + 'static
{
}

impl<Key: DataKey, Out: Data, OperatorChain> KeyedStream<Key, Out, OperatorChain> where
    OperatorChain: Operator<(Key, Out)> + 'static
{
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicU8, Ordering};
    use std::sync::Arc;

    use crate::config::EnvironmentConfig;
    use crate::environment::StreamEnvironment;
    use crate::operator::source;

    #[test]
    fn for_each() {
        let mut env = StreamEnvironment::new(EnvironmentConfig::local(4));
        let source = source::IteratorSource::new(0..10u8);
        let sum = Arc::new(AtomicU8::new(0));
        let sum2 = sum.clone();
        env.stream(source).for_each(move |x| {
            sum.fetch_add(x, Ordering::Release);
        });
        env.execute_blocking();
        assert_eq!(sum2.load(Ordering::Acquire), (0..10).sum::<u8>());
    }

    #[test]
    fn for_each_keyed() {
        let mut env = StreamEnvironment::new(EnvironmentConfig::local(4));
        let source = source::IteratorSource::new(0..10u8);
        let sum = Arc::new(AtomicU8::new(0));
        let sum2 = sum.clone();
        env.stream(source)
            .group_by(|x| x % 2)
            .for_each(move |(p, x)| {
                sum.fetch_add(x * (p + 1), Ordering::Release);
            });
        env.execute_blocking();
        assert_eq!(
            sum2.load(Ordering::Acquire),
            (0..10).map(|x| x * (x % 2 + 1)).sum::<u8>()
        );
    }
}
