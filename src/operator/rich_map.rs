use std::collections::HashMap;
use std::fmt::Display;
use std::marker::PhantomData;

use crate::block::{BlockStructure, OperatorStructure};
use crate::network::OperatorCoord;
use crate::operator::{Data, DataKey, Operator, StreamElement};
use crate::scheduler::{ExecutionMetadata, OperatorId};

#[derive(Debug)]
pub struct RichMap<Key: DataKey, Out: Data, NewOut: Data, F, OperatorChain>
where
    F: FnMut((&Key, Out)) -> NewOut + Clone + Send,
    OperatorChain: Operator<(Key, Out)>,
{
    prev: OperatorChain,
    operator_coord: OperatorCoord,
    maps_fn: HashMap<Key, F, crate::block::GroupHasherBuilder>,
    init_map: F,
    _out: PhantomData<Out>,
    _new_out: PhantomData<NewOut>,
}

impl<Key: DataKey, Out: Data, NewOut: Data, F: Clone, OperatorChain: Clone> Clone
    for RichMap<Key, Out, NewOut, F, OperatorChain>
where
    F: FnMut((&Key, Out)) -> NewOut + Clone + Send,
    OperatorChain: Operator<(Key, Out)>,
{
    fn clone(&self) -> Self {
        Self {
            prev: self.prev.clone(),
            operator_coord: self.operator_coord,
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
    F: FnMut((&Key, Out)) -> NewOut + Clone + Send,
    OperatorChain: Operator<(Key, Out)>,
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
    F: FnMut((&Key, Out)) -> NewOut + Clone + Send,
    OperatorChain: Operator<(Key, Out)>,
{
    pub(super) fn new(prev: OperatorChain, f: F) -> Self {
        let op_id = prev.get_op_id() + 1;
        Self {
            prev,
            
            // This will be set in setup method
            operator_coord: OperatorCoord::new(0, 0, 0, op_id),

            maps_fn: Default::default(),
            init_map: f,
            _out: Default::default(),
            _new_out: Default::default(),
        }
    }
}

impl<Key: DataKey, Out: Data, NewOut: Data, F, OperatorChain> Operator<(Key, NewOut)>
    for RichMap<Key, Out, NewOut, F, OperatorChain>
where
    F: FnMut((&Key, Out)) -> NewOut + Clone + Send,
    OperatorChain: Operator<(Key, Out)>,
{
    fn setup(&mut self, metadata: &mut ExecutionMetadata) {
        self.prev.setup(metadata);
        self.operator_coord.setup_coord(metadata.coord);
    }

    #[inline]
    fn next(&mut self) -> StreamElement<(Key, NewOut)> {
        let element = self.prev.next();
        if matches!(element, StreamElement::FlushAndRestart) {
            // self.maps_fn.clear();
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

    fn get_stateful_operators(&self) -> Vec<OperatorId> {
        // This operator is stateless
        self.prev.get_stateful_operators()
    }
}
