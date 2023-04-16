use std::fmt::Display;
use std::marker::PhantomData;

use crate::block::{BlockStructure, OperatorKind, OperatorStructure};
use crate::network::OperatorCoord;
use crate::operator::sink::Sink;
use crate::operator::{Data, DataKey, Operator, StreamElement};
use crate::scheduler::{ExecutionMetadata, OperatorId};
use crate::stream::{KeyValue, KeyedStream, Stream};

#[derive(Clone, Derivative)]
#[derivative(Debug)]
pub struct ForEachSink<Out: Data, F, PreviousOperators>
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

impl<Out: Data, F, PreviousOperators> ForEachSink<Out, F, PreviousOperators>
where
    F: FnMut(Out) + Send + Clone,
    PreviousOperators: Operator<Out>,
{
    fn new(prev: PreviousOperators, f: F) -> Self {
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


impl<Out: Data, F, PreviousOperators> Display for ForEachSink<Out, F, PreviousOperators>
where
    F: FnMut(Out) + Send + Clone,
    PreviousOperators: Operator<Out>,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} -> ForEach", self.prev)
    }
}

impl<Out: Data, F, PreviousOperators> Operator<()> for ForEachSink<Out, F, PreviousOperators>
where
    F: FnMut(Out) + Send + Clone,
    PreviousOperators: Operator<Out>,
{
    fn setup(&mut self, metadata: &mut ExecutionMetadata) {
        self.prev.setup(metadata);

        self.operator_coord.block_id = metadata.coord.block_id;
        self.operator_coord.host_id = metadata.coord.host_id;
        self.operator_coord.replica_id = metadata.coord.replica_id;
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
}

impl<Out: Data, F, PreviousOperators> Sink for ForEachSink<Out, F, PreviousOperators>
where
    F: FnMut(Out) + Send + Clone,
    PreviousOperators: Operator<Out>,
{
}

impl<Out: Data, OperatorChain> Stream<Out, OperatorChain>
where
    OperatorChain: Operator<Out> + 'static,
{
    /// Apply the given function to all the elements of the stream, consuming the stream.
    ///
    /// ## Example
    ///
    /// ```
    /// # use noir::{StreamEnvironment, EnvironmentConfig};
    /// # use noir::operator::source::IteratorSource;
    /// # let mut env = StreamEnvironment::new(EnvironmentConfig::local(1));
    /// let s = env.stream(IteratorSource::new((0..5)));
    /// s.for_each(|n| println!("Item: {}", n));
    ///
    /// env.execute();
    /// ```
    pub fn for_each<F>(self, f: F)
    where
        F: FnMut(Out) + Send + Clone + 'static,
    {
        self.add_operator(|prev| ForEachSink::new(
            prev,
            f,
        ))
        .finalize_block();
    }
}

impl<Key: DataKey, Out: Data, OperatorChain> KeyedStream<Key, Out, OperatorChain>
where
    OperatorChain: Operator<KeyValue<Key, Out>> + 'static,
{
    /// Apply the given function to all the elements of the stream, consuming the stream.
    ///
    /// ## Example
    ///
    /// ```
    /// # use noir::{StreamEnvironment, EnvironmentConfig};
    /// # use noir::operator::source::IteratorSource;
    /// # let mut env = StreamEnvironment::new(EnvironmentConfig::local(1));
    /// let s = env.stream(IteratorSource::new((0..5))).group_by(|&n| n % 2);
    /// s.for_each(|(key, n)| println!("Item: {} has key {}", n, key));
    ///
    /// env.execute();
    /// ```
    pub fn for_each<F>(self, f: F)
    where
        F: FnMut((Key, Out)) + Send + Clone + 'static,
    {
        self.0
            .add_operator(|prev| ForEachSink::new(
                prev,
                f,
            ))
            .finalize_block();
    }
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
        env.execute();
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
        env.execute();
        assert_eq!(
            sum2.load(Ordering::Acquire),
            (0..10).map(|x| x * (x % 2 + 1)).sum::<u8>()
        );
    }
}
