use std::fmt::Display;
use std::marker::PhantomData;

use crate::block::{BlockStructure, OperatorStructure};
use crate::network::OperatorCoord;
use crate::operator::{Data, DataKey, Operator};
use crate::persistency::{PersistencyService, PersistencyServices};
use crate::scheduler::OperatorId;
use crate::stream::{KeyValue, KeyedStream, Stream};
use crate::ExecutionMetadata;

use super::StreamElement;

#[derive(Clone)]
struct FilterMap<In: Data, Out: Data, PreviousOperator, Predicate>
where
    Predicate: Fn(In) -> Option<Out> + Send + Clone + 'static,
    PreviousOperator: Operator<In> + 'static,
{
    prev: PreviousOperator,
    operator_coord : OperatorCoord,
    predicate: Predicate,
    persistency_service: PersistencyService,
    _in: PhantomData<In>,
    _out: PhantomData<Out>,
}

impl<In: Data, Out: Data, PreviousOperator, Predicate> Display
    for FilterMap<In, Out, PreviousOperator, Predicate>
where
    Predicate: Fn(In) -> Option<Out> + Send + Clone + 'static,
    PreviousOperator: Operator<In> + 'static,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{} -> FilterMap<{}>",
            self.prev,
            std::any::type_name::<Out>()
        )
    }
}

impl<In: Data, Out: Data, PreviousOperator, Predicate>
    FilterMap<In, Out, PreviousOperator, Predicate>
where
    Predicate: Fn(In) -> Option<Out> + Send + Clone + 'static,
    PreviousOperator: Operator<In> + 'static,
{
    fn new(prev: PreviousOperator, predicate: Predicate) -> Self {
        let op_id = prev.get_op_id() + 1;
        Self {
            prev,
            // This will be set in setup method
            operator_coord: OperatorCoord::new(0,0,0,op_id),

            predicate,
            persistency_service: PersistencyService::default(),
            _in: Default::default(),
            _out: Default::default(),
        }
    }
}

impl<In: Data, Out: Data, PreviousOperator, Predicate> Operator<Out>
    for FilterMap<In, Out, PreviousOperator, Predicate>
where
    Predicate: Fn(In) -> Option<Out> + Send + Clone + 'static,
    PreviousOperator: Operator<In> + 'static,
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
    fn next(&mut self) -> StreamElement<Out> {
        loop {
            match self.prev.next() {
                StreamElement::Item(item) => {
                    if let Some(el) = (self.predicate)(item) {
                        return StreamElement::Item(el);
                    }
                }
                StreamElement::Timestamped(item, ts) => {
                    if let Some(el) = (self.predicate)(item) {
                        return StreamElement::Timestamped(el, ts);
                    }
                }
                StreamElement::Watermark(w) => return StreamElement::Watermark(w),
                StreamElement::Terminate => return StreamElement::Terminate,
                StreamElement::FlushAndRestart => return StreamElement::FlushAndRestart,
                StreamElement::FlushBatch => return StreamElement::FlushBatch,
                StreamElement::Snapshot(snap_id) => {
                    // Save void state and forward snapshot marker
                    self.persistency_service.save_void_state(self.operator_coord, snap_id);
                    return StreamElement::Snapshot(snap_id);
                }
            }
        }
    }

    fn structure(&self) -> BlockStructure {
        let mut operator = OperatorStructure::new::<Out, _>("FilterMap");
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
    /// Remove from the stream all the elements for which the provided function returns `None` and
    /// keep the elements that returned `Some(_)`.
    ///
    /// **Note**: this is very similar to [`Iteartor::filter_map`](std::iter::Iterator::filter_map)
    ///
    /// ## Example
    ///
    /// ```
    /// # use noir::{StreamEnvironment, EnvironmentConfig};
    /// # use noir::operator::source::IteratorSource;
    /// # let mut env = StreamEnvironment::new(EnvironmentConfig::local(1));
    /// let s = env.stream(IteratorSource::new((0..10)));
    /// let res = s.filter_map(|n| if n % 2 == 0 { Some(n * 3) } else { None }).collect_vec();
    ///
    /// env.execute();
    ///
    /// assert_eq!(res.get().unwrap(), vec![0, 6, 12, 18, 24])
    /// ```
    pub fn filter_map<NewOut: Data, F>(self, f: F) -> Stream<NewOut, impl Operator<NewOut>>
    where
        F: Fn(Out) -> Option<NewOut> + Send + Clone + 'static,
    {
        self.add_operator(|prev| FilterMap::new(prev, f))
    }
}

impl<Key: DataKey, Out: Data, OperatorChain> KeyedStream<Key, Out, OperatorChain>
where
    OperatorChain: Operator<KeyValue<Key, Out>> + 'static,
{
    /// Remove from the stream all the elements for which the provided function returns `None` and
    /// keep the elements that returned `Some(_)`.
    ///
    /// **Note**: this is very similar to [`Iteartor::filter_map`](std::iter::Iterator::filter_map)
    ///
    /// ## Example
    ///
    /// ```
    /// # use noir::{StreamEnvironment, EnvironmentConfig};
    /// # use noir::operator::source::IteratorSource;
    /// # let mut env = StreamEnvironment::new(EnvironmentConfig::local(1));
    /// let s = env.stream(IteratorSource::new((0..10))).group_by(|&n| n % 2);
    /// let res = s.filter_map(|(_key, n)| if n % 3 == 0 { Some(n * 4) } else { None }).collect_vec();
    ///
    /// env.execute();
    ///
    /// let mut res = res.get().unwrap();
    /// res.sort_unstable();
    /// assert_eq!(res, vec![(0, 0), (0, 24), (1, 12), (1, 36)]);
    /// ```
    pub fn filter_map<NewOut: Data, F>(
        self,
        f: F,
    ) -> KeyedStream<Key, NewOut, impl Operator<KeyValue<Key, NewOut>>>
    where
        F: Fn(KeyValue<&Key, Out>) -> Option<NewOut> + Send + Clone + 'static,
    {
        self.map(f)
            .filter(|(_, x)| x.is_some())
            .map(|(_, x)| x.unwrap())
    }
}
