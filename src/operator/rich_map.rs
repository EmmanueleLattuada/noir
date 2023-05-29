use std::collections::HashMap;
use std::fmt::Display;
use std::marker::PhantomData;

use crate::block::{BlockStructure, OperatorStructure};
use crate::network::OperatorCoord;
use crate::operator::{Data, DataKey, Operator, StreamElement};
use crate::persistency::{PersistencyService, PersistencyServices};
use crate::scheduler::{ExecutionMetadata, OperatorId};
use crate::stream::{KeyValue, KeyedStream, Stream};

#[derive(Debug)]
struct RichMap<Key: DataKey, Out: Data, NewOut: Data, F, OperatorChain>
where
    F: FnMut(KeyValue<&Key, Out>) -> NewOut + Clone + Send,
    OperatorChain: Operator<KeyValue<Key, Out>>,
{
    prev: OperatorChain,
    operator_coord: OperatorCoord,
    persistency_service: PersistencyService,
    maps_fn: HashMap<Key, F, crate::block::GroupHasherBuilder>,
    init_map: F,
    _out: PhantomData<Out>,
    _new_out: PhantomData<NewOut>,
}

impl<Key: DataKey, Out: Data, NewOut: Data, F: Clone, OperatorChain: Clone> Clone
    for RichMap<Key, Out, NewOut, F, OperatorChain>
where
    F: FnMut(KeyValue<&Key, Out>) -> NewOut + Clone + Send,
    OperatorChain: Operator<KeyValue<Key, Out>>,
{
    fn clone(&self) -> Self {
        Self {
            prev: self.prev.clone(),
            operator_coord: self.operator_coord,
            persistency_service: self.persistency_service.clone(),
            maps_fn: self.maps_fn.clone(),
            init_map: self.init_map.clone(),
            _out: self._out,
            _new_out: self._new_out,
        }
    }
}

impl<Key: DataKey, Out: Data, NewOut: Data, F, OperatorChain> Display
    for RichMap<Key, Out, NewOut, F, OperatorChain>
where
    F: FnMut(KeyValue<&Key, Out>) -> NewOut + Clone + Send,
    OperatorChain: Operator<KeyValue<Key, Out>>,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{} -> RichMap<{} -> {}>",
            self.prev,
            std::any::type_name::<Out>(),
            std::any::type_name::<NewOut>()
        )
    }
}

impl<Key: DataKey, Out: Data, NewOut: Data, F, OperatorChain>
    RichMap<Key, Out, NewOut, F, OperatorChain>
where
    F: FnMut(KeyValue<&Key, Out>) -> NewOut + Clone + Send,
    OperatorChain: Operator<KeyValue<Key, Out>>,
{
    fn new(prev: OperatorChain, f: F) -> Self {
        let op_id = prev.get_op_id() + 1;
        Self {
            prev,
            
            // This will be set in setup method
            operator_coord: OperatorCoord::new(0, 0, 0, op_id),
            persistency_service: PersistencyService::default(),

            maps_fn: Default::default(),
            init_map: f,
            _out: Default::default(),
            _new_out: Default::default(),
        }
    }
}

impl<Key: DataKey, Out: Data, NewOut: Data, F, OperatorChain> Operator<KeyValue<Key, NewOut>>
    for RichMap<Key, Out, NewOut, F, OperatorChain>
where
    F: FnMut(KeyValue<&Key, Out>) -> NewOut + Clone + Send,
    OperatorChain: Operator<KeyValue<Key, Out>>,
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
    fn next(&mut self) -> StreamElement<(Key, NewOut)> {
        let element = self.prev.next();
        match element {
            StreamElement::Snapshot(snap_id) => {
                self.persistency_service.save_void_state(self.operator_coord, snap_id);
            }
            StreamElement::FlushAndRestart => {
                // self.maps_fn.clear();
            }
            StreamElement::Terminate => {
                if self.persistency_service.is_active() {
                    // Save void terminated state
                    self.persistency_service.save_terminated_void_state(self.operator_coord);
                }
            }
            _ => {}
        }
        element.map(|(key, value)| {
            let map_fn = if let Some(map_fn) = self.maps_fn.get_mut(&key) {
                map_fn
            } else {
                // the key is not present in the hashmap, so this always inserts a new map function
                let map_fn = self.init_map.clone();
                self.maps_fn.entry(key.clone()).or_insert(map_fn)
            };

            let new_value = (map_fn)((&key, value));
            (key, new_value)
        })
    }

    fn structure(&self) -> BlockStructure {
        let mut operator = OperatorStructure::new::<NewOut, _>("RichMap");
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
    /// Map the elements of the stream into new elements. The mapping function can be stateful.
    ///
    /// This is equivalent to [`Stream::map`] but with a stateful function.
    ///
    /// Since the mapping function can be stateful, it is a `FnMut`. This allows expressing simple
    /// algorithms with very few lines of code (see examples).
    ///
    /// The mapping function is _cloned_ inside each replica, and they will not share state between
    /// each other. If you want that only a single replica handles all the items you may want to
    /// change the parallelism of this operator with [`Stream::max_parallelism`].
    ///
    /// ## Examples
    ///
    /// This is a simple implementation of the prefix-sum using a single replica (i.e. each element
    /// is mapped to the sum of all the elements up to that point). Note that this won't work if
    /// there are more replicas.
    ///
    /// ```
    /// # use noir::{StreamEnvironment, EnvironmentConfig};
    /// # use noir::operator::source::IteratorSource;
    /// # let mut env = StreamEnvironment::new(EnvironmentConfig::local(1));
    /// let s = env.stream(IteratorSource::new((1..=5)));
    /// let res = s.rich_map({
    ///     let mut sum = 0;
    ///     move |x| {
    ///         sum += x;
    ///         sum
    ///     }
    /// }).collect_vec();
    ///
    /// env.execute();
    ///
    /// assert_eq!(res.get().unwrap(), vec![1, 1 + 2, 1 + 2 + 3, 1 + 2 + 3 + 4, 1 + 2 + 3 + 4 + 5]);
    /// ```    
    ///
    /// This will enumerate all the elements that reach a replica. This is basically equivalent to
    /// the `enumerate` function in Python.
    ///
    /// ```
    /// # use noir::{StreamEnvironment, EnvironmentConfig};
    /// # use noir::operator::source::IteratorSource;
    /// # let mut env = StreamEnvironment::new(EnvironmentConfig::local(1));
    /// let s = env.stream(IteratorSource::new((1..=5)));
    /// let res = s.rich_map({
    ///     let mut id = 0;
    ///     move |x| {
    ///         id += 1;
    ///         (id - 1, x)
    ///     }
    /// }).collect_vec();
    ///
    /// env.execute();
    ///
    /// assert_eq!(res.get().unwrap(), vec![(0, 1), (1, 2), (2, 3), (3, 4), (4, 5)]);
    /// ```
    pub fn rich_map<NewOut: Data, F>(self, mut f: F) -> Stream<NewOut, impl Operator<NewOut>>
    where
        F: FnMut(Out) -> NewOut + Send + Clone + 'static,
    {
        self.key_by(|_| ())
            .add_operator(|prev| RichMap::new(prev, move |(_, value)| f(value)))
            .drop_key()
    }
}

impl<Key: DataKey, Out: Data, OperatorChain> KeyedStream<Key, Out, OperatorChain>
where
    OperatorChain: Operator<KeyValue<Key, Out>> + 'static,
{
    /// Map the elements of the stream into new elements. The mapping function can be stateful.
    ///
    /// This is exactly like [`Stream::rich_map`], but the function is cloned for each key. This
    /// means that each key will have a unique mapping function (and therefore a unique state).
    pub fn rich_map<NewOut: Data, F>(
        self,
        f: F,
    ) -> KeyedStream<Key, NewOut, impl Operator<KeyValue<Key, NewOut>>>
    where
        F: FnMut(KeyValue<&Key, Out>) -> NewOut + Clone + Send + 'static,
    {
        self.add_operator(|prev| RichMap::new(prev, f))
    }
}
