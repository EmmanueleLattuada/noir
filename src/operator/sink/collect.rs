use std::fmt::Display;
use std::marker::PhantomData;

use crate::block::{BlockStructure, OperatorKind, OperatorStructure};
use crate::network::OperatorCoord;
use crate::operator::sink::{Sink, StreamOutput, StreamOutputRef};
use crate::operator::{ExchangeData, ExchangeDataKey, Operator, StreamElement};
use crate::scheduler::{ExecutionMetadata, OperatorId};
use crate::stream::{KeyValue, KeyedStream, Stream};

#[derive(Debug)]
pub struct Collect<Out: ExchangeData, C: FromIterator<Out> + Send, PreviousOperators>
where
    PreviousOperators: Operator<Out>,
{
    prev: PreviousOperators,
    operator_coord: OperatorCoord,
    output: StreamOutputRef<C>,
    _out: PhantomData<Out>,
}

impl<Out: ExchangeData, C: FromIterator<Out> + Send, PreviousOperators> Collect<Out, C, PreviousOperators>
where
    PreviousOperators: Operator<Out>,
{
    fn new(prev: PreviousOperators, output: StreamOutputRef<C>) -> Self {
        let op_id = prev.get_op_id() + 1;
        Self {
            prev,
            // This will be set in setup method
            operator_coord: OperatorCoord::new(0, 0, 0, op_id),
            output,
            _out: PhantomData,
        }
    }
}

impl<Out: ExchangeData, C: FromIterator<Out> + Send, PreviousOperators> Display
    for Collect<Out, C, PreviousOperators>
where
    PreviousOperators: Operator<Out>,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{} -> Collect<{}>",
            self.prev,
            std::any::type_name::<C>()
        )
    }
}

impl<Out: ExchangeData, C: FromIterator<Out> + Send, PreviousOperators> Operator<()>
    for Collect<Out, C, PreviousOperators>
where
    PreviousOperators: Operator<Out>,
{
    fn setup(&mut self, metadata: &mut ExecutionMetadata) {
        self.prev.setup(metadata);

        self.operator_coord.block_id = metadata.coord.block_id;
        self.operator_coord.host_id = metadata.coord.host_id;
        self.operator_coord.replica_id = metadata.coord.replica_id;
    }

    fn next(&mut self) -> StreamElement<()> {
        let iter = std::iter::from_fn(|| loop {
            match self.prev.next() {
                StreamElement::Item(t) | StreamElement::Timestamped(t, _) => return Some(t),
                StreamElement::Terminate => return None,
                // TODO: handle snapshot marker
                StreamElement::Snapshot(_) => {
                    panic!("Snapshot not supported for collect operator")
                }
                _ => continue,
            }
        });
        let c = C::from_iter(iter);
        *self.output.lock().unwrap() = Some(c);

        StreamElement::Terminate
    }

    fn structure(&self) -> BlockStructure {
        let mut operator = OperatorStructure::new::<Out, _>("Collect");
        let op_id = self.operator_coord.operator_id;
        operator.subtitle = format!("op id: {op_id}");
        operator.kind = OperatorKind::Sink;
        self.prev.structure().add_operator(operator)
    }

    fn get_op_id(&self) -> OperatorId {
        self.operator_coord.operator_id
    }
}

impl<Out: ExchangeData, C: FromIterator<Out> + Send, PreviousOperators> Sink
    for Collect<Out, C, PreviousOperators>
where
    PreviousOperators: Operator<Out>,
{
}

impl<Out: ExchangeData, C: FromIterator<Out> + Send, PreviousOperators> Clone
    for Collect<Out, C, PreviousOperators>
where
    PreviousOperators: Operator<Out>,
{
    fn clone(&self) -> Self {
        panic!("Collect cannot be cloned, max_parallelism should be 1");
    }
}

impl<Out: ExchangeData, OperatorChain> Stream<Out, OperatorChain>
where
    OperatorChain: Operator<Out> + 'static,
{
    /// Close the stream and store all the resulting items into a collection on a single host.
    ///
    /// If the stream is distributed among multiple replicas, parallelism will
    /// be set to 1 to gather all results
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
    pub fn collect<C: FromIterator<Out> + Send + 'static>(self) -> StreamOutput<C> {
        let output = StreamOutputRef::default();
        self.max_parallelism(1)
            .add_operator(|prev| Collect::new(
                prev,
                output.clone(),
            ))
            .finalize_block();
        StreamOutput { result: output }
    }
}

impl<Key: ExchangeDataKey, Out: ExchangeData, OperatorChain> KeyedStream<Key, Out, OperatorChain>
where
    OperatorChain: Operator<KeyValue<Key, Out>> + 'static,
{
    /// Close the stream and store all the resulting items into a collection on a single host.
    ///
    /// If the stream is distributed among multiple replicas, parallelism will
    /// be set to 1 to gather all results
    ///
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
    /// let s = env.stream(IteratorSource::new((0..3))).group_by(|&n| n % 2);
    /// let res = s.collect_vec();
    ///
    /// env.execute();
    ///
    /// let mut res = res.get().unwrap();
    /// res.sort_unstable(); // the output order is nondeterministic
    /// assert_eq!(res, vec![(0, 0), (0, 2), (1, 1)]);
    /// ```
    pub fn collect<C: FromIterator<(Key, Out)> + Send + 'static>(self) -> StreamOutput<C> {
        self.unkey().collect()
    }
}

#[cfg(test)]
mod tests {
    use itertools::Itertools;

    use crate::config::EnvironmentConfig;
    use crate::environment::StreamEnvironment;
    use crate::operator::source;

    #[test]
    fn collect_vec() {
        let mut env = StreamEnvironment::new(EnvironmentConfig::local(4));
        let source = source::IteratorSource::new(0..10u8);
        let res = env.stream(source).collect_vec();
        env.execute();
        assert_eq!(res.get().unwrap(), (0..10).collect_vec());
    }
}
