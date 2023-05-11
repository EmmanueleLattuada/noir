use std::fmt::Display;
use std::marker::PhantomData;

use crate::block::{BlockStructure, OperatorStructure};
use crate::network::OperatorCoord;
use crate::operator::{Data, DataKey, Operator, StreamElement};
use crate::persistency::{PersistencyService, PersistencyServices};
use crate::scheduler::{ExecutionMetadata, OperatorId};
use crate::stream::{KeyValue, KeyedStream, Stream};

#[derive(Clone, Derivative)]
#[derivative(Debug)]
pub struct Map<Out: Data, NewOut: Data, F, PreviousOperators>
where
    F: Fn(Out) -> NewOut + Send + Clone,
    PreviousOperators: Operator<Out>,
{
    prev: PreviousOperators,
    operator_coord: OperatorCoord,
    #[derivative(Debug = "ignore")]
    f: F,
    _out: PhantomData<Out>,
    _new_out: PhantomData<NewOut>,

    persistency_service: PersistencyService,
}

impl<Out: Data, NewOut: Data, F, PreviousOperators> Display
    for Map<Out, NewOut, F, PreviousOperators>
where
    F: Fn(Out) -> NewOut + Send + Clone,
    PreviousOperators: Operator<Out>,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{} -> Map<{} -> {}>",
            self.prev,
            std::any::type_name::<Out>(),
            std::any::type_name::<NewOut>()
        )
    }
}

impl<Out: Data, NewOut: Data, F, PreviousOperators> Map<Out, NewOut, F, PreviousOperators>
where
    F: Fn(Out) -> NewOut + Send + Clone,
    PreviousOperators: Operator<Out>,
{
    fn new(prev: PreviousOperators, f: F) -> Self {
        let op_id = prev.get_op_id() + 1;
        Self {
            prev,
            f,
            // This will be set in setup method
            operator_coord: OperatorCoord::new(0, 0, 0, op_id),
            persistency_service: Default::default(),

            _out: Default::default(),
            _new_out: Default::default(),
        }
    }
}

impl<Out: Data, NewOut: Data, F, PreviousOperators> Operator<NewOut>
    for Map<Out, NewOut, F, PreviousOperators>
where
    F: Fn(Out) -> NewOut + Send + Clone,
    PreviousOperators: Operator<Out>,
{
    fn setup(&mut self, metadata: &mut ExecutionMetadata) {
        self.prev.setup(metadata);

        self.operator_coord.block_id = metadata.coord.block_id;
        self.operator_coord.host_id = metadata.coord.host_id;
        self.operator_coord.replica_id = metadata.coord.replica_id;

        self.persistency_service = metadata.persistency_service.clone();
        self.persistency_service.setup();        
    }

    #[inline]
    fn next(&mut self) -> StreamElement<NewOut> {
        let elem = self.prev.next();
        // Save void state and forward the marker
        match elem {
            StreamElement::Snapshot(snap_id) => {
                self.persistency_service.save_void_state(self.operator_coord, snap_id);
            }
            _ => {},
        }
        elem.map(&self.f)
    }

    fn structure(&self) -> BlockStructure {
        let mut operator = OperatorStructure::new::<NewOut, _>("Map");
        let op_id = self.operator_coord.operator_id;
        operator.subtitle = format!("op id: {op_id}");
        self.prev
            .structure()
            .add_operator(operator)
    }

    fn get_op_id(&self) -> OperatorId {
        self.operator_coord.operator_id
    }
}

impl<Out: Data, OperatorChain> Stream<Out, OperatorChain>
where
    OperatorChain: Operator<Out> + 'static,
{
    /// Map the elements of the stream into new elements.
    ///
    /// **Note**: this is very similar to [`Iteartor::map`](std::iter::Iterator::map).
    ///
    /// ## Example
    ///
    /// ```
    /// # use noir::{StreamEnvironment, EnvironmentConfig};
    /// # use noir::operator::source::IteratorSource;
    /// # let mut env = StreamEnvironment::new(EnvironmentConfig::local(1));
    /// let s = env.stream(IteratorSource::new((0..5)));
    /// let res = s.map(|n| n * 10).collect_vec();
    ///
    /// env.execute();
    ///
    /// assert_eq!(res.get().unwrap(), vec![0, 10, 20, 30, 40]);
    /// ```
    pub fn map<NewOut: Data, F>(self, f: F) -> Stream<NewOut, impl Operator<NewOut>>
    where
        F: Fn(Out) -> NewOut + Send + Clone + 'static,
    {
        self.add_operator(|prev| Map::new(prev, f))
    }
}

impl<Key: DataKey, Out: Data, OperatorChain> KeyedStream<Key, Out, OperatorChain>
where
    OperatorChain: Operator<KeyValue<Key, Out>> + 'static,
{
    /// Map the elements of the stream into new elements.
    ///
    /// **Note**: this is very similar to [`Iteartor::map`](std::iter::Iterator::map).
    ///
    /// ## Example
    ///
    /// ```
    /// # use noir::{StreamEnvironment, EnvironmentConfig};
    /// # use noir::operator::source::IteratorSource;
    /// # let mut env = StreamEnvironment::new(EnvironmentConfig::local(1));
    /// let s = env.stream(IteratorSource::new((0..5))).group_by(|&n| n % 2);
    /// let res = s.map(|(_key, n)| 10 * n).collect_vec();
    ///
    /// env.execute();
    ///
    /// let mut res = res.get().unwrap();
    /// res.sort_unstable();
    /// assert_eq!(res, vec![(0, 0), (0, 20), (0, 40), (1, 10), (1, 30)]);
    /// ```
    pub fn map<NewOut: Data, F>(
        self,
        f: F,
    ) -> KeyedStream<Key, NewOut, impl Operator<KeyValue<Key, NewOut>>>
    where
        F: Fn(KeyValue<&Key, Out>) -> NewOut + Send + Clone + 'static,
    {
        self.add_operator(|prev| {
            Map::new(prev, move |(k, v)| {
                let mapped_value = f((&k, v));
                (k, mapped_value)
            })
        })
    }
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use crate::operator::map::Map;
    use crate::operator::{Operator, StreamElement};
    use crate::test::FakeOperator;

    #[test]
    #[cfg(feature = "timestamp")]
    fn map_stream() {
        let mut fake_operator = FakeOperator::new(0..10u8);
        for i in 0..10 {
            fake_operator.push(StreamElement::Timestamped(i, i as i64));
        }
        fake_operator.push(StreamElement::Watermark(100));

        let map = Map::new(fake_operator, |x| x.to_string());
        let map = Map::new(map, |x| x + "000");
        let mut map = Map::new(map, |x| u32::from_str(&x).unwrap());

        for i in 0..10 {
            let elem = map.next();
            assert_eq!(elem, StreamElement::Item(i * 1000));
        }
        for i in 0..10 {
            let elem = map.next();
            assert_eq!(elem, StreamElement::Timestamped(i * 1000, i as i64));
        }
        assert_eq!(map.next(), StreamElement::Watermark(100));
        assert_eq!(map.next(), StreamElement::Terminate);
    }
}
