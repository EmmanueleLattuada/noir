use std::collections::HashMap;
use std::fmt::Display;
use std::hash::BuildHasherDefault;
use std::marker::PhantomData;

use wyhash::WyHash;

use crate::block::{BlockStructure, OperatorStructure};
use crate::network::OperatorCoord;
use crate::operator::{Data, Operator, StreamElement};
use crate::persistency::persistency_service::PersistencyService;
use crate::scheduler::{ExecutionMetadata, OperatorId};
use crate::stream::{KeyedStream, Stream};

use super::{ExchangeData, ExchangeDataKey};

#[derive(Debug)]
struct RichMapPersistent<Key: ExchangeDataKey, Out: Data, NewOut: Data, F, State: ExchangeData, OperatorChain>
where
    F: Fn((&Key, Out), &mut State) -> NewOut + Send + Clone,
    OperatorChain: Operator<(Key, Out)>,
{
    prev: OperatorChain,
    operator_coord: OperatorCoord,
    persistency_service: Option<PersistencyService<HashMap<Key, State, BuildHasherDefault<WyHash>>>>,
    init_state: State,
    states_by_key: HashMap<Key, State, crate::block::GroupHasherBuilder>,
    f: F,
    _out: PhantomData<Out>,
    _new_out: PhantomData<NewOut>,
}

impl<Key: ExchangeDataKey, Out: Data, NewOut: Data, F: Clone, State: ExchangeData, OperatorChain: Clone> Clone
    for RichMapPersistent<Key, Out, NewOut, F, State, OperatorChain>
where
    F: Fn((&Key, Out), &mut State) -> NewOut + Send + Clone,
    OperatorChain: Operator<(Key, Out)>,
{
    fn clone(&self) -> Self {
        Self {
            prev: self.prev.clone(),
            operator_coord: self.operator_coord,
            persistency_service: self.persistency_service.clone(),
            init_state: self.init_state.clone(),
            states_by_key: self.states_by_key.clone(),
            f: self.f.clone(),
            _out: self._out,
            _new_out: self._new_out,
        }
    }
}

impl<Key: ExchangeDataKey, Out: Data, NewOut: Data, F, State: ExchangeData, OperatorChain> Display
    for RichMapPersistent<Key, Out, NewOut, F, State, OperatorChain>
where
    F: Fn((&Key, Out), &mut State) -> NewOut + Send + Clone,
    OperatorChain: Operator<(Key, Out)>,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{} -> RichMapPersistent<{} -> {}>",
            self.prev,
            std::any::type_name::<Out>(),
            std::any::type_name::<NewOut>()
        )
    }
}

impl<Key: ExchangeDataKey, Out: Data, NewOut: Data, F, State: ExchangeData, OperatorChain>
    RichMapPersistent<Key, Out, NewOut, F, State, OperatorChain>
where
    F: Fn((&Key, Out), &mut State) -> NewOut + Send + Clone,
    OperatorChain: Operator<(Key, Out)>,
{
    fn new(prev: OperatorChain, f: F, state: State) -> Self {
        let op_id = prev.get_op_id() + 1;
        Self {
            prev,
            
            // This will be set in setup method
            operator_coord: OperatorCoord::new(0, 0, 0, op_id),
            persistency_service: None,

            states_by_key: Default::default(),
            f,
            init_state: state,
            _out: Default::default(),
            _new_out: Default::default(),
        }
    }
}

impl<Key: ExchangeDataKey, Out: Data, NewOut: Data, F, State: ExchangeData, OperatorChain> Operator<(Key, NewOut)>
    for RichMapPersistent<Key, Out, NewOut, F, State, OperatorChain>
where
    F: Fn((&Key, Out), &mut State) -> NewOut + Send + Clone,
    OperatorChain: Operator<(Key, Out)>,
{
    fn setup(&mut self, metadata: &mut ExecutionMetadata) {
        self.prev.setup(metadata);

        self.operator_coord.from_coord(metadata.coord);
        if let Some(pb) = metadata.persistency_builder{
            let p_service = pb.generate_persistency_service::<HashMap<Key, State, BuildHasherDefault<WyHash>>>();
            let snapshot_id = p_service.restart_from_snapshot(self.operator_coord);
            if snapshot_id.is_some() {
                // Get and resume the persisted state
                let opt_state: Option<HashMap<Key, State, crate::block::GroupHasherBuilder>> = p_service.get_state(self.operator_coord, snapshot_id.unwrap());
                if let Some(state) = opt_state {
                    self.states_by_key = state;
                } else {
                    panic!("No persisted state founded for op: {0}", self.operator_coord);
                } 
            }
            self.persistency_service = Some(p_service);
        }
    }

    #[inline]
    fn next(&mut self) -> StreamElement<(Key, NewOut)> {
        let element = self.prev.next();
        match element {
            StreamElement::Item((key, value)) => {
                let state = if let Some(state) = self.states_by_key.get_mut(&key) {
                    state
                } else {
                    // the key is not present in the hashmap
                    self.states_by_key.insert(key.clone(), self.init_state.clone());
                    self.states_by_key.get_mut(&key).unwrap()
                };
                let new_value = (self.f)((&key, value), state);
                StreamElement::Item((key, new_value))
            }
            StreamElement::Timestamped((key, value), ts) => {
                let state = if let Some(state) = self.states_by_key.get_mut(&key) {
                    state
                } else {
                    // the key is not present in the hashmap
                    self.states_by_key.insert(key.clone(), self.init_state.clone());
                    self.states_by_key.get_mut(&key).unwrap()
                };
                let new_value = (self.f)((&key, value), state);
                StreamElement::Timestamped((key, new_value), ts)
            }
            StreamElement::Snapshot(snap_id) => {
                let state = self.states_by_key.clone();
                self.persistency_service.as_mut().unwrap().save_state(self.operator_coord, snap_id.clone(), state);
                StreamElement::Snapshot(snap_id)
            }
            StreamElement::Watermark(w) => {
                StreamElement::Watermark(w)
            }
            StreamElement::FlushBatch => {
                StreamElement::FlushBatch
            }
            StreamElement::FlushAndRestart => {
                // self.maps_fn.clear();
                StreamElement::FlushAndRestart
            }
            StreamElement::Terminate => {
                if self.persistency_service.is_some() {
                    // Save terminated state
                    let state = self.states_by_key.clone();
                    self.persistency_service.as_mut().unwrap().save_terminated_state(self.operator_coord, state);
                }
                StreamElement::Terminate
            }
        }
    }

    fn structure(&self) -> BlockStructure {
        let mut operator = OperatorStructure::new::<NewOut, _>("RichMapPersistent");
        let op_id = self.operator_coord.operator_id;
        operator.subtitle = format!("op id: {op_id}");
        self.prev
            .structure()
            .add_operator(operator)
    }

    fn get_op_id(&self) -> OperatorId {
        self.operator_coord.operator_id
    }

    fn get_stateful_operators(&self) -> Vec<OperatorId> {
        let mut res = self.prev.get_stateful_operators();
        // This operator is stateful
        res.push(self.operator_coord.operator_id);
        res
    }
}

impl<Out: Data, OperatorChain> Stream<Out, OperatorChain>
where
    OperatorChain: Operator<Out> + 'static,
{
    /// Map the elements of the stream into new elements.
    ///
    /// This is the persistent version of rich_map, the initial state must be passed as 
    /// parameter and the mapping Fn has access to a mutable reference of the state.
    ///
    /// The initial state is _cloned_ inside each replica, and they will not share state between
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
    /// let sum = 0;
    /// let res = s.rich_map_persistent(sum, {
    ///    |x, sum| {
    ///        *sum += x;
    ///        *sum
    ///    }
    /// }).collect_vec();
    ///
    /// env.execute_blocking();
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
    /// let state = 0;
    /// let res = s.rich_map_persistent(state, {
    ///     |x, state| {
    ///         *state += 1;
    ///         (*state - 1, x)
    ///     }
    /// }).collect_vec();
    ///
    /// env.execute_blocking();
    ///
    /// assert_eq!(res.get().unwrap(), vec![(0, 1), (1, 2), (2, 3), (3, 4), (4, 5)]);
    /// ```
    pub fn rich_map_persistent<NewOut: Data, F, State: ExchangeData>(self, state: State, f: F) -> Stream<NewOut, impl Operator<NewOut>>
    where
        F: Fn(Out, &mut State) -> NewOut + Send + Clone + 'static,
    {
        self.key_by(|_| ())
            .add_operator(|prev| RichMapPersistent::new(prev, move |(_, value), state| f(value, state), state))
            .drop_key()
    }
}

impl<Key: ExchangeDataKey, Out: Data, OperatorChain> KeyedStream<Key, Out, OperatorChain>
where
    OperatorChain: Operator<(Key, Out)> + 'static,
{
    /// Map the elements of the stream into new elements. 
    /// 
    /// This is the persistent version of rich_map, the initial state must be passed as 
    /// parameter and the mapping Fn has access to a mutable reference of the state.
    ///
    /// This is exactly like [`Stream::rich_map`], but the state is cloned for each key.
    /// This means that each key will have a unique state.
    pub fn rich_map_persistent<NewOut: Data, F, State: ExchangeData>(
        self,
        state: State,
        f: F,
    ) -> KeyedStream<Key, NewOut, impl Operator<(Key, NewOut)>>
    where
        F: Fn((&Key, Out), &mut State) -> NewOut + Send + Clone + 'static,
    {
        self.add_operator(|prev| RichMapPersistent::new(prev, f, state))
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use serial_test::serial;

    use crate::{operator::{rich_map_persistent::RichMapPersistent, StreamElement, Operator, SnapshotId}, test::{FakeOperator, persistency_config_unit_tests}, network::OperatorCoord, persistency::builder::PersistencyBuilder};  

    #[test]
    #[serial]
    fn test_rich_map_persistent_persistency() {
        let mut fake_operator = FakeOperator::empty();
        fake_operator.push(StreamElement::Item((1, 1)));
        fake_operator.push(StreamElement::Item((1, 2)));
        fake_operator.push(StreamElement::Snapshot(SnapshotId::new(1)));
        fake_operator.push(StreamElement::Item((1, 3)));
        fake_operator.push(StreamElement::Item((1, 4)));
        fake_operator.push(StreamElement::Snapshot(SnapshotId::new(2)));

        let state = 0;
        let f = {
            |x, sum: &mut i32| {
                *sum += x;
                *sum
            }
        };
        let mut rich_map_persistent = RichMapPersistent::new(fake_operator, move |(_, value), state| f(value, state), state);
 
        rich_map_persistent.operator_coord = OperatorCoord{
            block_id: 0,
            host_id: 1,
            replica_id: 2,
            operator_id: 1,
        };
        let mut pers_builder = PersistencyBuilder::new(Some(persistency_config_unit_tests()));
        rich_map_persistent.persistency_service = Some(pers_builder.generate_persistency_service());

        assert_eq!(rich_map_persistent.next(), StreamElement::Item((1, 1)));
        assert_eq!(rich_map_persistent.next(), StreamElement::Item((1, 3)));
        assert_eq!(rich_map_persistent.next(), StreamElement::Snapshot(SnapshotId::new(1)));
        pers_builder.flush_state_saver();
        rich_map_persistent.persistency_service = Some(pers_builder.generate_persistency_service());
        let state: Option<HashMap<i32, i32, crate::block::GroupHasherBuilder>> = rich_map_persistent.persistency_service.as_mut().unwrap().get_state(rich_map_persistent.operator_coord, SnapshotId::new(1));
        assert_eq!(state.unwrap().get_mut(&1).unwrap().clone(), 3);

        assert_eq!(rich_map_persistent.next(), StreamElement::Item((1, 6)));
        assert_eq!(rich_map_persistent.next(), StreamElement::Item((1, 10)));
        assert_eq!(rich_map_persistent.next(), StreamElement::Snapshot(SnapshotId::new(2)));
        pers_builder.flush_state_saver();
        rich_map_persistent.persistency_service = Some(pers_builder.generate_persistency_service());
        let state: Option<HashMap<i32, i32, crate::block::GroupHasherBuilder>> = rich_map_persistent.persistency_service.as_mut().unwrap().get_state(rich_map_persistent.operator_coord, SnapshotId::new(2));
        assert_eq!(state.unwrap().get_mut(&1).unwrap().clone(), 10);

        // Clean redis
        rich_map_persistent.persistency_service.as_mut().unwrap().delete_state(rich_map_persistent.operator_coord, SnapshotId::new(1));
        rich_map_persistent.persistency_service.as_mut().unwrap().delete_state(rich_map_persistent.operator_coord, SnapshotId::new(2));
    }


    #[test]
    #[serial]
    fn test_rich_map_persistent_diff_key_persistency() {
        let mut fake_operator = FakeOperator::empty();
        fake_operator.push(StreamElement::Item((1, 1)));
        fake_operator.push(StreamElement::Item((2, 2)));
        fake_operator.push(StreamElement::Snapshot(SnapshotId::new(1)));
        fake_operator.push(StreamElement::Item((1, 3)));
        fake_operator.push(StreamElement::Item((2, 4)));
        fake_operator.push(StreamElement::Snapshot(SnapshotId::new(2)));

        let state = 0;
        let f = {
            |x, sum: &mut i32| {
                *sum += x;
                *sum
            }
        };
        let mut rich_map_persistent = RichMapPersistent::new(fake_operator, move |(_key, value), state| f(value, state), state);
 
        rich_map_persistent.operator_coord = OperatorCoord{
            block_id: 0,
            host_id: 1,
            replica_id: 2,
            operator_id: 2,
        };
        let mut pers_builder = PersistencyBuilder::new(Some(persistency_config_unit_tests()));
        rich_map_persistent.persistency_service = Some(pers_builder.generate_persistency_service());

        assert_eq!(rich_map_persistent.next(), StreamElement::Item((1, 1)));
        assert_eq!(rich_map_persistent.next(), StreamElement::Item((2, 2)));
        assert_eq!(rich_map_persistent.next(), StreamElement::Snapshot(SnapshotId::new(1)));
        pers_builder.flush_state_saver();
        rich_map_persistent.persistency_service = Some(pers_builder.generate_persistency_service());
        let state: Option<HashMap<i32, i32, crate::block::GroupHasherBuilder>> = rich_map_persistent.persistency_service.as_mut().unwrap().get_state(rich_map_persistent.operator_coord, SnapshotId::new(1));
        assert_eq!(state.clone().unwrap().get_mut(&1).unwrap().clone(), 1);
        assert_eq!(state.unwrap().get_mut(&2).unwrap().clone(), 2);

        assert_eq!(rich_map_persistent.next(), StreamElement::Item((1, 4)));
        assert_eq!(rich_map_persistent.next(), StreamElement::Item((2, 6)));
        assert_eq!(rich_map_persistent.next(), StreamElement::Snapshot(SnapshotId::new(2)));
        pers_builder.flush_state_saver();
        rich_map_persistent.persistency_service = Some(pers_builder.generate_persistency_service());
        let state: Option<HashMap<i32, i32, crate::block::GroupHasherBuilder>> = rich_map_persistent.persistency_service.as_mut().unwrap().get_state(rich_map_persistent.operator_coord, SnapshotId::new(2));
        assert_eq!(state.clone().unwrap().get_mut(&1).unwrap().clone(), 4);
        assert_eq!(state.unwrap().get_mut(&2).unwrap().clone(), 6);

        // Clean redis
        rich_map_persistent.persistency_service.as_mut().unwrap().delete_state(rich_map_persistent.operator_coord, SnapshotId::new(1));
        rich_map_persistent.persistency_service.as_mut().unwrap().delete_state(rich_map_persistent.operator_coord, SnapshotId::new(2));
    }

}
