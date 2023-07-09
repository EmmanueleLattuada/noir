use std::{
    collections::{HashMap, HashSet, VecDeque},
    fmt::{Debug, Display},
    marker::PhantomData,
};

use serde::{Serialize, Deserialize};

use crate::{
    block::{NextStrategy, OperatorStructure},
    network::OperatorCoord,
    operator::{
        BinaryElement, BinaryStartOperator, Data, DataKey, ExchangeData, Operator, Start,
        StreamElement, SnapshotId,
    },
    KeyedStream, scheduler::OperatorId, persistency::{PersistencyService, PersistencyServices},
};

use super::{InnerJoinTuple, JoinVariant, OuterJoinTuple};

type BinaryTuple<K, V1, V2> = BinaryElement<(K, V1), (K, V2)>;

/// This type keeps the elements of a side of the join.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct SideHashMap<Key: DataKey, Out> {
    /// The actual items on this side, grouped by key.
    ///
    /// Note that when the other side ends this map is emptied.
    data: HashMap<Key, Vec<Out>, crate::block::GroupHasherBuilder>,
    /// The set of all the keys seen.
    ///
    /// Note that when this side ends this set is emptied since it won't be used again.
    keys: HashSet<Key>,
    /// Whether this side has ended.
    ended: bool,
    /// The number of items received.
    count: usize,
}

impl<Key: DataKey, Out> Default for SideHashMap<Key, Out> {
    fn default() -> Self {
        Self {
            data: Default::default(),
            keys: Default::default(),
            ended: false,
            count: 0,
        }
    }
}

#[derive(Clone)]
struct JoinKeyedOuter<K: DataKey + ExchangeData, V1: ExchangeData, V2: ExchangeData> {
    prev: BinaryStartOperator<(K, V1), (K, V2)>,
    operator_coord: OperatorCoord,
    persistency_service: Option<PersistencyService>,
    variant: JoinVariant,
    _k: PhantomData<K>,
    _v1: PhantomData<V1>,
    _v2: PhantomData<V2>,

    /// The content of the left side.
    left: SideHashMap<K, V1>,
    /// The content of the right side.
    right: SideHashMap<K, V2>,

    buffer: VecDeque<(K, OuterJoinTuple<V1, V2>)>,
}

impl<K: DataKey + ExchangeData, V1: ExchangeData, V2: ExchangeData> JoinKeyedOuter<K, V1, V2> {
    pub(crate) fn new(prev: BinaryStartOperator<(K, V1), (K, V2)>, variant: JoinVariant) -> Self {
        let op_id = prev.get_op_id() + 1;
        JoinKeyedOuter {
            prev,
            // This will be set in setup method
            operator_coord: OperatorCoord::new(0, 0, 0, op_id),
            persistency_service: None,
            variant,
            _k: PhantomData,
            _v1: PhantomData,
            _v2: PhantomData,
            left: Default::default(),
            right: Default::default(),
            buffer: Default::default(),
        }
    }

    fn process_item(&mut self, item: BinaryTuple<K, V1, V2>) {
        let left_outer = self.variant.left_outer();
        let right_outer = self.variant.right_outer();
        match item {
            BinaryElement::Left((key, v1)) => {
                self.left.count += 1;
                if let Some(right) = self.right.data.get(&key) {
                    // the left item has at least one right matching element
                    for v2 in right {
                        self.buffer
                            .push_back((key.clone(), (Some(v1.clone()), Some(v2.clone()))));
                    }
                } else if self.right.ended && left_outer {
                    // if the left item has no right correspondent, but the right has already ended
                    // we might need to generate the outer tuple.
                    self.buffer
                        .push_back((key.clone(), (Some(v1.clone()), None)));
                }
                if right_outer {
                    self.left.keys.insert(key.clone());
                }
                if !self.right.ended {
                    self.left.data.entry(key).or_default().push(v1);
                }
            }
            BinaryElement::Right((key, v2)) => {
                self.right.count += 1;
                if let Some(left) = self.left.data.get(&key) {
                    // the left item has at least one right matching element
                    for v1 in left {
                        self.buffer
                            .push_back((key.clone(), (Some(v1.clone()), Some(v2.clone()))));
                    }
                } else if self.left.ended && right_outer {
                    // if the left item has no right correspondent, but the right has already ended
                    // we might need to generate the outer tuple.
                    self.buffer
                        .push_back((key.clone(), (None, Some(v2.clone()))));
                }
                if left_outer {
                    self.right.keys.insert(key.clone());
                }
                if !self.left.ended {
                    self.right.data.entry(key).or_default().push(v2);
                }
            }
            BinaryElement::LeftEnd => {
                log::debug!(
                    "Left side of join ended with {} elements on the left \
                    and {} elements on the right",
                    self.left.count,
                    self.right.count
                );
                if right_outer {
                    // left ended and this is a right-outer, so we need to generate (None, Some)
                    // tuples. For each value on the right side, before dropping the right hashmap,
                    // search if there was already a match.
                    for (key, right) in self.right.data.drain() {
                        if !self.left.keys.contains(&key) {
                            for v2 in right {
                                self.buffer.push_back((key.clone(), (None, Some(v2))));
                            }
                        }
                    }
                } else {
                    // in any case, we won't need the right hashmap anymore.
                    self.right.data.clear();
                }
                // we will never look at it, and nothing will be inserted, drop it freeing some memory.
                self.left.keys.clear();
                self.left.ended = true;
            }
            BinaryElement::RightEnd => {
                log::debug!(
                    "Right side of join ended with {} elements on the left \
                    and {} elements on the right",
                    self.left.count,
                    self.right.count
                );
                if left_outer {
                    // right ended and this is a left-outer, so we need to generate (None, Some)
                    // tuples. For each value on the left side, before dropping the left hashmap,
                    // search if there was already a match.
                    for (key, left) in self.left.data.drain() {
                        if !self.right.keys.contains(&key) {
                            for v1 in left {
                                self.buffer.push_back((key.clone(), (Some(v1), None)));
                            }
                        }
                    }
                } else {
                    // in any case, we won't need the left hashmap anymore.
                    self.left.data.clear();
                }
                // we will never look at it, and nothing will be inserted, drop it freeing some memory.
                self.right.keys.clear();
                self.right.ended = true;
            }
        }
    }

    /// Save state for snapshot
    fn save_snap(&mut self, snapshot_id: SnapshotId) {
        let state = JoinKeyedOuterState{
            left: self.left.clone(),
            right: self.right.clone(),
            buffer: self.buffer.clone(),
        };
        self.persistency_service.as_mut().unwrap().save_state(self.operator_coord, snapshot_id, state);        
    }
    /// Save terminated state
    fn save_terminate(&mut self) {
        let state = JoinKeyedOuterState{
            left: self.left.clone(),
            right: self.right.clone(),
            buffer: self.buffer.clone(),
        };
        self.persistency_service.as_mut().unwrap().save_terminated_state(self.operator_coord, state);
    }
}

impl<K: DataKey + ExchangeData, V1: ExchangeData, V2: ExchangeData> Display
    for JoinKeyedOuter<K, V1, V2>
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{} -> JoinKeyed<{},{},{}>",
            self.prev,
            std::any::type_name::<K>(),
            std::any::type_name::<V1>(),
            std::any::type_name::<V2>(),
        )
    }
}

#[derive(Clone, Serialize, Deserialize)]
struct JoinKeyedOuterState<K: DataKey, V1, V2> {    
    left: SideHashMap<K, V1>,
    right: SideHashMap<K, V2>,
    buffer: VecDeque<(K, OuterJoinTuple<V1, V2>)>,
}

impl<K: DataKey + ExchangeData, V1: ExchangeData, V2: ExchangeData>
    Operator<(K, OuterJoinTuple<V1, V2>)> for JoinKeyedOuter<K, V1, V2>
{
    fn setup(&mut self, metadata: &mut crate::ExecutionMetadata) {
        self.prev.setup(metadata);
        
        self.operator_coord.from_coord(metadata.coord);
        if metadata.persistency_service.is_some() {
            self.persistency_service = metadata.persistency_service.clone();
        
            let snapshot_id = self.persistency_service.as_mut().unwrap().restart_from_snapshot(self.operator_coord);
            if snapshot_id.is_some() {
                // Get and resume the persisted state
                let opt_state: Option<JoinKeyedOuterState<K, V1, V2>> = self.persistency_service.as_mut().unwrap().get_state(self.operator_coord, snapshot_id.unwrap());
                if let Some(state) = opt_state {
                    self.left = state.left;
                    self.right = state.right;
                    self.buffer = state.buffer;
                } else {
                    panic!("No persisted state founded for op: {0}", self.operator_coord);
                } 
            }
        }
    }

    fn next(&mut self) -> crate::operator::StreamElement<(K, OuterJoinTuple<V1, V2>)> {
        while self.buffer.is_empty() {
            match self.prev.next() {
                StreamElement::Item(el) => self.process_item(el),
                StreamElement::FlushAndRestart => {
                    assert!(self.left.ended);
                    assert!(self.right.ended);
                    assert!(self.left.data.is_empty());
                    assert!(self.right.data.is_empty());
                    assert!(self.left.keys.is_empty());
                    assert!(self.right.keys.is_empty());
                    self.left.ended = false;
                    self.left.count = 0;
                    self.right.ended = false;
                    self.right.count = 0;
                    log::debug!(
                        "JoinLocalHash at {} emitted FlushAndRestart",
                        self.operator_coord.get_coord(),
                    );
                    return StreamElement::FlushAndRestart;
                }
                StreamElement::Terminate => {
                    if self.persistency_service.is_some() {
                        self.save_terminate();
                    }
                    return StreamElement::Terminate;
                }
                StreamElement::FlushBatch => return StreamElement::FlushBatch,
                StreamElement::Snapshot(snapshot_id) => {
                    self.save_snap(snapshot_id);
                    return StreamElement::Snapshot(snapshot_id);
                }
                StreamElement::Watermark(_) | StreamElement::Timestamped(_, _) => {
                    panic!("Cannot yet join timestamped streams")
                }
            }
        }

        let item = self.buffer.pop_front().unwrap();
        StreamElement::Item(item)
    }

    fn structure(&self) -> crate::block::BlockStructure {
        let mut operator = OperatorStructure::new::<(K, InnerJoinTuple<V1, V2>), _>("JoinKeyed");
        let op_id = self.operator_coord.operator_id;
        operator.subtitle = format!("op id: {op_id}");
        self.prev.structure().add_operator(operator)
    }

    fn get_op_id(&self) -> OperatorId {
        self.operator_coord.operator_id
    }
}

#[derive(Clone)]
struct JoinKeyedInner<K: DataKey + ExchangeData, V1: ExchangeData, V2: ExchangeData> {
    prev: BinaryStartOperator<(K, V1), (K, V2)>,
    operator_coord: OperatorCoord,
    persistency_service: Option<PersistencyService>,
    _k: PhantomData<K>,
    _v1: PhantomData<V1>,
    _v2: PhantomData<V2>,

    /// The content of the left side.
    left: HashMap<K, Vec<V1>, crate::block::CoordHasherBuilder>,
    /// The content of the right side.
    right: HashMap<K, Vec<V2>, crate::block::CoordHasherBuilder>,

    left_ended: bool,
    right_ended: bool,

    buffer: VecDeque<(K, InnerJoinTuple<V1, V2>)>,
}

impl<K: DataKey + ExchangeData, V1: ExchangeData, V2: ExchangeData> Display
    for JoinKeyedInner<K, V1, V2>
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{} -> JoinKeyedInner<{},{},{}>",
            self.prev,
            std::any::type_name::<K>(),
            std::any::type_name::<V1>(),
            std::any::type_name::<V2>(),
        )
    }
}

impl<K: DataKey + ExchangeData + Debug, V1: ExchangeData + Debug, V2: ExchangeData + Debug>
    JoinKeyedInner<K, V1, V2>
{
    pub(crate) fn new(prev: BinaryStartOperator<(K, V1), (K, V2)>) -> Self {
        let op_id = prev.get_op_id() + 1;
        JoinKeyedInner {
            prev,
            // This will be set in setup method
            operator_coord: OperatorCoord::new(0, 0, 0, op_id),
            persistency_service: None,
            _k: PhantomData,
            _v1: PhantomData,
            _v2: PhantomData,
            left: Default::default(),
            right: Default::default(),
            buffer: Default::default(),
            left_ended: false,
            right_ended: false,
        }
    }

    fn process_item(&mut self, item: BinaryTuple<K, V1, V2>) {
        match item {
            BinaryElement::Left((key, v1)) => {
                if let Some(right) = self.right.get(&key) {
                    // the left item has at least one right matching element
                    for v2 in right {
                        self.buffer
                            .push_back((key.clone(), (v1.clone(), v2.clone())));
                    }
                }
                self.left.entry(key).or_default().push(v1);
            }
            BinaryElement::Right((key, v2)) => {
                if let Some(left) = self.left.get(&key) {
                    // the left item has at least one right matching element
                    for v1 in left {
                        self.buffer
                            .push_back((key.clone(), (v1.clone(), v2.clone())));
                    }
                }
                self.right.entry(key).or_default().push(v2);
            }
            BinaryElement::LeftEnd => {
                self.left_ended = true;
                self.right.clear();
                if self.right_ended {
                    self.left.clear();
                    self.right.clear();
                }
            }
            BinaryElement::RightEnd => {
                self.right_ended = true;
                self.left.clear();
                if self.left_ended {
                    self.left.clear();
                    self.right.clear();
                }
            }
        }
    }

    /// Save state for snapshot
    fn save_snap(&mut self, snapshot_id: SnapshotId){
        let state = JoinKeyedInnerState{
            left: self.left.clone(),
            right: self.right.clone(),
            left_ended: self.left_ended,
            right_ended: self.right_ended,
            buffer: self.buffer.clone(),
        };
        self.persistency_service.as_mut().unwrap().save_state(self.operator_coord, snapshot_id, state);
    }
    /// Save terminated state
    fn save_terminate(&mut self){
        let state = JoinKeyedInnerState{
            left: self.left.clone(),
            right: self.right.clone(),
            left_ended: self.left_ended,
            right_ended: self.right_ended,
            buffer: self.buffer.clone(),
        };
        self.persistency_service.as_mut().unwrap().save_terminated_state(self.operator_coord, state);
    }
}

#[derive(Clone, Serialize, Deserialize)]
struct JoinKeyedInnerState<K: DataKey, V1, V2> {
    left: HashMap<K, Vec<V1>, crate::block::CoordHasherBuilder>,
    right: HashMap<K, Vec<V2>, crate::block::CoordHasherBuilder>,
    left_ended: bool,
    right_ended: bool,
    buffer: VecDeque<(K, InnerJoinTuple<V1, V2>)>,
}

impl<K: DataKey + ExchangeData + Debug, V1: ExchangeData + Debug, V2: ExchangeData + Debug>
    Operator<(K, InnerJoinTuple<V1, V2>)> for JoinKeyedInner<K, V1, V2>
{
    fn setup(&mut self, metadata: &mut crate::ExecutionMetadata) {
        self.prev.setup(metadata);

        self.operator_coord.from_coord(metadata.coord);

        if metadata.persistency_service.is_some() {
            self.persistency_service = metadata.persistency_service.clone();
            let snapshot_id = self.persistency_service.as_mut().unwrap().restart_from_snapshot(self.operator_coord);
            if snapshot_id.is_some() {
                // Get and resume the persisted state
                let opt_state: Option<JoinKeyedInnerState<K, V1, V2>> = self.persistency_service.as_mut().unwrap().get_state(self.operator_coord, snapshot_id.unwrap());
                if let Some(state) = opt_state {
                    self.left_ended = state.left_ended;
                    self.right_ended = state.right_ended;
                    self.left = state.left;
                    self.right = state.right;
                    self.buffer = state.buffer;
                } else {
                    panic!("No persisted state founded for op: {0}", self.operator_coord);
                } 
            }
        }
    }

    fn next(&mut self) -> crate::operator::StreamElement<(K, InnerJoinTuple<V1, V2>)> {
        while self.buffer.is_empty() {
            match self.prev.next() {
                StreamElement::Item(el) => self.process_item(el),
                StreamElement::FlushAndRestart => {
                    assert!(self.left.is_empty());
                    assert!(self.right.is_empty());
                    log::debug!(
                        "JoinLocalHash at {} emitted FlushAndRestart",
                        self.operator_coord.get_coord(),
                    );
                    self.left_ended = false;
                    self.right_ended = false;
                    return StreamElement::FlushAndRestart;
                }
                StreamElement::Terminate => {
                    if self.persistency_service.is_some() {
                        self.save_terminate();
                    }
                    return StreamElement::Terminate;
                }
                StreamElement::FlushBatch => return StreamElement::FlushBatch,
                StreamElement::Snapshot(snapshot_id) => {
                    self.save_snap(snapshot_id);
                    return StreamElement::Snapshot(snapshot_id);
                }
                StreamElement::Watermark(_) | StreamElement::Timestamped(_, _) => {
                    panic!("Cannot yet join timestamped streams")
                }
            }
        }

        let item = self.buffer.pop_front().unwrap();
        StreamElement::Item(item)
    }

    fn structure(&self) -> crate::block::BlockStructure {
        let mut operator = OperatorStructure::new::<(K, InnerJoinTuple<V1, V2>), _>("JoinKeyed");
        let op_id = self.operator_coord.operator_id;
        operator.subtitle = format!("op id: {op_id}");
        self.prev.structure().add_operator(operator)
    }

    fn get_op_id(&self) -> OperatorId {
        self.operator_coord.operator_id
    }
}

impl<K: DataKey + ExchangeData + Debug, V1: Data + ExchangeData + Debug, O1> KeyedStream<K, V1, O1>
where
    O1: Operator<(K, V1)> + 'static,
{
    pub fn join_outer<V2: Data + ExchangeData + Debug, O2>(
        self,
        rhs: KeyedStream<K, V2, O2>,
    ) -> KeyedStream<K, (Option<V1>, Option<V2>), impl Operator<(K, (Option<V1>, Option<V2>))>>
    where
        O2: Operator<(K, V2)> + 'static,
    {
        let next_strategy1 = NextStrategy::only_one();
        let next_strategy2 = NextStrategy::only_one();

        let inner =
            self.0
                .binary_connection(rhs.0, Start::multiple, next_strategy1, next_strategy2);

        let s = inner.add_operator(move |prev| JoinKeyedOuter::new(prev, JoinVariant::Outer));
        KeyedStream(s)
    }

    pub fn join<V2: Data + ExchangeData + Debug, O2>(
        self,
        rhs: KeyedStream<K, V2, O2>,
    ) -> KeyedStream<K, (V1, V2), impl Operator<(K, (V1, V2))>>
    where
        O2: Operator<(K, V2)> + 'static,
    {
        let next_strategy1 = NextStrategy::only_one();
        let next_strategy2 = NextStrategy::only_one();

        let inner =
            self.0
                .binary_connection(rhs.0, Start::multiple, next_strategy1, next_strategy2);

        let s = inner.add_operator(move |prev| JoinKeyedInner::new(prev));
        KeyedStream(s)
    }
}
