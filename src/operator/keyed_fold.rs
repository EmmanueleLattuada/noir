use core::iter::Iterator;
use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::fmt::Display;
use std::marker::PhantomData;
use std::hash::Hash;

use serde::{Serialize, Deserialize};

use crate::block::{BlockStructure, OperatorStructure};

use crate::network::OperatorCoord;
use crate::operator::{Data, ExchangeData, ExchangeDataKey, Operator, StreamElement, Timestamp};
use crate::persistency::persistency_service::PersistencyService;
use crate::scheduler::{ExecutionMetadata, OperatorId};

use super::SnapshotId;


#[derive(Derivative)]
#[derivative(Debug, Clone)]
pub struct KeyedFold<Key: ExchangeDataKey, Out: Data, NewOut: ExchangeData, F, PreviousOperators>
where
    F: Fn(&mut NewOut, Out) + Send + Clone,
    PreviousOperators: Operator<(Key, Out)>,
{
    prev: PreviousOperators,
    operator_coord: OperatorCoord,
    persistency_service: Option<PersistencyService<KeyedFoldState<Key, NewOut>>>,
    #[derivative(Debug = "ignore")]
    fold: F,
    init: NewOut,
    accumulators: HashMap<Key, NewOut, crate::block::GroupHasherBuilder>,
    timestamps: HashMap<Key, Timestamp, crate::block::GroupHasherBuilder>,
    ready: Vec<StreamElement<(Key, NewOut)>>,
    max_watermark: Option<Timestamp>,
    received_end: bool,
    received_end_iter: bool,
    _out: PhantomData<Out>,
}

impl<Key: ExchangeDataKey, Out: Data, NewOut: ExchangeData, F, PreviousOperators> Display
    for KeyedFold<Key, Out, NewOut, F, PreviousOperators>
where
    F: Fn(&mut NewOut, Out) + Send + Clone,
    PreviousOperators: Operator<(Key, Out)>,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{} -> KeyedFold<{} -> {}>",
            self.prev,
            std::any::type_name::<(Key, Out)>(),
            std::any::type_name::<(Key, NewOut)>()
        )
    }
}

impl<Key: ExchangeDataKey, Out: Data, NewOut: ExchangeData, F, PreviousOperators: Operator<(Key, Out)>>
    KeyedFold<Key, Out, NewOut, F, PreviousOperators>
where
    F: Fn(&mut NewOut, Out) + Send + Clone,
{
    pub(super) fn new(prev: PreviousOperators, init: NewOut, fold: F) -> Self {
        let op_id = prev.get_op_id() + 1;
        KeyedFold {
            prev,
            // This will be set in setup method
            operator_coord: OperatorCoord::new(0,0,0,op_id),
            persistency_service: None,
            fold,
            init,
            accumulators: Default::default(),
            timestamps: Default::default(),
            ready: Default::default(),
            max_watermark: None,
            received_end: false,
            received_end_iter: false,
            _out: Default::default(),
        }
    }

    /// Process a new item, folding it with the accumulator inside the hashmap.
    fn process_item(&mut self, key: Key, value: Out) {
        match self.accumulators.entry(key) {
            Entry::Vacant(entry) => {
                let mut acc = self.init.clone();
                (self.fold)(&mut acc, value);
                entry.insert(acc);
            }
            Entry::Occupied(mut entry) => {
                (self.fold)(entry.get_mut(), value);
            }
        }
    }

    /// Save state for snapshot
    fn save_snap(&mut self, snapshot_id: SnapshotId){
        let state = KeyedFoldState{
            accumulators: self.accumulators.clone(),
            timestamps: self.timestamps.clone(),
            ready: self.ready.clone(),
            max_watermark: self.max_watermark,
            received_end_iter: self.received_end_iter,
        }; 
        self.persistency_service.as_mut().unwrap().save_state(self.operator_coord, snapshot_id, state);
    }
    /// Save terminated state
    fn save_terminate(&mut self){
        let state = KeyedFoldState{
            accumulators: self.accumulators.clone(),
            timestamps: self.timestamps.clone(),
            ready: self.ready.clone(),
            max_watermark: self.max_watermark,
            received_end_iter: self.received_end_iter,
        }; 
        self.persistency_service.as_mut().unwrap().save_terminated_state(self.operator_coord, state);
    }
}


#[derive(Clone, Serialize, Deserialize, Debug)]
struct KeyedFoldState<K: Hash + Eq, O> {
    accumulators: HashMap<K, O, crate::block::GroupHasherBuilder>,
    timestamps: HashMap<K, Timestamp, crate::block::GroupHasherBuilder>,
    ready: Vec<StreamElement<(K, O)>>,
    max_watermark: Option<Timestamp>,
    received_end_iter: bool,
}

impl<Key: ExchangeDataKey, Out: Data, NewOut: ExchangeData, F, PreviousOperators> Operator<(Key, NewOut)>
    for KeyedFold<Key, Out, NewOut, F, PreviousOperators>
where
    F: Fn(&mut NewOut, Out) + Send + Clone,
    PreviousOperators: Operator<(Key, Out)>,
{
    fn setup(&mut self, metadata: &mut ExecutionMetadata) {
        self.prev.setup(metadata);

        self.operator_coord.setup_coord(metadata.coord);
        if let Some(pb) = metadata.persistency_builder{
            let p_service = pb.generate_persistency_service::<KeyedFoldState<Key, NewOut>>();
            let snapshot_id = p_service.restart_from_snapshot(self.operator_coord);
            if let Some(restart_snap) = snapshot_id  {
                // Get and resume the persisted state
                let opt_state: Option<KeyedFoldState<Key, NewOut>> = p_service.get_state(self.operator_coord, restart_snap);
                if let Some(state) = opt_state {
                    self.accumulators = state.accumulators;
                    self.timestamps = state.timestamps;
                    self.ready = state.ready;
                    self.max_watermark = state.max_watermark;
                    //self.received_end = state.received_end;
                    self.received_end_iter = state.received_end_iter;
                } else {
                    panic!("No persisted state founded for op: {0}", self.operator_coord);
                } 
            }
            self.persistency_service = Some(p_service);
        }
    }

    #[inline]
    fn next(&mut self) -> StreamElement<(Key, NewOut)> {
        while !self.received_end {
            match self.prev.next() {
                StreamElement::Terminate => self.received_end = true,
                StreamElement::FlushAndRestart => {
                    self.received_end = true;
                    self.received_end_iter = true;
                }
                StreamElement::Watermark(ts) => {
                    self.max_watermark = Some(self.max_watermark.unwrap_or(ts).max(ts))
                }
                StreamElement::Item((k, v)) => {
                    self.process_item(k, v);
                }
                StreamElement::Timestamped((k, v), ts) => {
                    self.process_item(k.clone(), v);
                    self.timestamps
                        .entry(k)
                        .and_modify(|entry| *entry = (*entry).max(ts))
                        .or_insert(ts);
                }
                // this block won't sent anything until the stream ends
                StreamElement::FlushBatch => {}
                StreamElement::Snapshot(snap_id) => {
                    self.save_snap(snap_id.clone());
                    return StreamElement::Snapshot(snap_id);
                }
            }
        }

        // move all the accumulators into a faster vec
        if !self.accumulators.is_empty() {
            // take a reference to move into the closure, avoiding moving "self"
            let timestamps = &mut self.timestamps;
            self.ready
                .extend(self.accumulators.drain().map(|(key, value)| {
                    if let Some(ts) = timestamps.remove(&key) {
                        StreamElement::Timestamped((key, value), ts)
                    } else {
                        StreamElement::Item((key, value))
                    }
                }));
        }

        // consume the ready items
        if let Some(elem) = self.ready.pop() {
            return elem;
        }

        if let Some(ts) = self.max_watermark.take() {
            return StreamElement::Watermark(ts);
        }

        // the end was not really the end... just the end of one iteration!
        if self.received_end_iter {
            self.received_end_iter = false;
            self.received_end = false;
            return StreamElement::FlushAndRestart;
        }

        // Save terminated state before end
        if self.persistency_service.is_some() {
            self.save_terminate();
        }
        StreamElement::Terminate
    }

    fn structure(&self) -> BlockStructure {
        let mut operator = OperatorStructure::new::<(Key, NewOut), _>(
            "KeyedFold",
        );
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

#[cfg(test)]
mod tests {
    use itertools::Itertools;

    use crate::operator::keyed_fold::KeyedFold;
    use crate::operator::{Operator, StreamElement};
    use crate::test::FakeOperator;

    #[test]
    #[allow(clippy::identity_op)]
    fn test_keyed_fold_no_timestamp() {
        let data = (0..10u8).map(|x| (x % 2, x)).collect_vec();
        let fake_operator = FakeOperator::new(data.into_iter());
        let mut keyed_fold = KeyedFold::new(fake_operator, 0, |a, b| *a += b);

        let mut res = vec![];
        for _ in 0..2 {
            let item = keyed_fold.next();
            match item {
                StreamElement::Item(x) => res.push(x),
                other => panic!("Expecting StreamElement::Item, got {}", other.variant()),
            }
        }

        assert_eq!(keyed_fold.next(), StreamElement::Terminate);

        res.sort_unstable();
        assert_eq!(res[0].1, 0 + 2 + 4 + 6 + 8);
        assert_eq!(res[1].1, 1 + 3 + 5 + 7 + 9);
    }

    #[test]
    #[cfg(feature = "timestamp")]
    #[allow(clippy::identity_op)]
    fn test_keyed_fold_timestamp() {
        let mut fake_operator = FakeOperator::empty();
        fake_operator.push(StreamElement::Timestamped((0, 0), 1));
        fake_operator.push(StreamElement::Timestamped((1, 1), 2));
        fake_operator.push(StreamElement::Timestamped((0, 2), 3));
        fake_operator.push(StreamElement::Watermark(4));

        let mut keyed_fold = KeyedFold::new(fake_operator, 0, |a, b| *a += b);

        let mut res = vec![];
        for _ in 0..2 {
            let item = keyed_fold.next();
            match item {
                StreamElement::Timestamped(x, ts) => res.push((x, ts)),
                other => panic!(
                    "Expecting StreamElement::Timestamped, got {}",
                    other.variant()
                ),
            }
        }

        assert_eq!(keyed_fold.next(), StreamElement::Watermark(4));
        assert_eq!(keyed_fold.next(), StreamElement::Terminate);

        res.sort_unstable();
        assert_eq!(res[0].0 .1, 0 + 2);
        assert_eq!(res[0].1, 3);
        assert_eq!(res[1].0 .1, 1);
        assert_eq!(res[1].1, 2);
    }

    #[test]
    #[allow(clippy::identity_op)]
    fn test_keyed_fold_end_iter() {
        let mut fake_operator = FakeOperator::empty();
        fake_operator.push(StreamElement::Item((0, 0)));
        fake_operator.push(StreamElement::Item((0, 2)));
        fake_operator.push(StreamElement::FlushAndRestart);
        fake_operator.push(StreamElement::Item((1, 1)));
        fake_operator.push(StreamElement::Item((1, 3)));
        fake_operator.push(StreamElement::FlushAndRestart);

        let mut keyed_fold = KeyedFold::new(fake_operator, 0, |a, b| *a += b);

        assert_eq!(keyed_fold.next(), StreamElement::Item((0, 0 + 2)));
        assert_eq!(keyed_fold.next(), StreamElement::FlushAndRestart);
        assert_eq!(keyed_fold.next(), StreamElement::Item((1, 1 + 3)));
        assert_eq!(keyed_fold.next(), StreamElement::FlushAndRestart);
        assert_eq!(keyed_fold.next(), StreamElement::Terminate);
    }
}
