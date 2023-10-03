use core::iter::{IntoIterator, Iterator};
use std::fmt::Display;
use std::marker::PhantomData;

use crate::block::{BlockStructure, OperatorStructure};
use crate::network::OperatorCoord;
use crate::operator::{Data, DataKey, Operator, StreamElement, Timestamp};
use crate::persistency::persistency_service::PersistencyService;
use crate::scheduler::{ExecutionMetadata, OperatorId};

#[derive(Clone, Derivative)]
#[derivative(Debug)]
pub struct Flatten<In, Out, InnerIterator, PreviousOperators>
where
    PreviousOperators: Operator<In>,
    In: Data + IntoIterator<Item = Out>,
    Out: Data,
    InnerIterator: Iterator,
{
    prev: PreviousOperators,
    operator_coord: OperatorCoord,
    persistency_service: Option<PersistencyService<()>>,
    // used to store elements that have not been returned by next() yet
    // buffer: VecDeque<StreamElement<NewOut>>,
    // Make an element of type `Out` iterable
    // This is used to make `Flatten` behave differently when applied to `Stream` or `KeyedStream`
    // Takes `Out` as input, returns an `Iterator` with items of type `NewOut`
    #[derivative(Debug = "ignore")]
    frontiter: Option<InnerIterator>,
    #[cfg(feature = "timestamp")]
    timestamp: Option<Timestamp>,
    _out: PhantomData<In>,
    _iter_out: PhantomData<Out>,
}

impl<In, Out, InnerIterator, PreviousOperators> Display
    for Flatten<In, Out, InnerIterator, PreviousOperators>
where
    PreviousOperators: Operator<In>,
    In: Data + IntoIterator<Item = Out>,
    Out: Data,
    InnerIterator: Iterator,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{} -> Flatten<{} -> {}>",
            self.prev,
            std::any::type_name::<In>(),
            std::any::type_name::<Out>()
        )
    }
}

impl<In, Out, InnerIterator, PreviousOperators> Flatten<In, Out, InnerIterator, PreviousOperators>
where
    PreviousOperators: Operator<In>,
    In: Data + IntoIterator<IntoIter = InnerIterator, Item = InnerIterator::Item>,
    Out: Data + Clone,
    InnerIterator: Iterator<Item = Out> + Clone + Send,
{
    pub(super) fn new(prev: PreviousOperators) -> Self {
        let op_id = prev.get_op_id() + 1;
        Self {
            prev,
            // This will be set in setup method
            operator_coord: OperatorCoord::new(0,0,0,op_id),
            persistency_service: None,
            frontiter: None,
            #[cfg(feature = "timestamp")]
            timestamp: None,
            _out: Default::default(),
            _iter_out: Default::default(),
        }
    }
}

impl<In, Out, InnerIterator, PreviousOperators> Operator<Out>
    for Flatten<In, Out, InnerIterator, PreviousOperators>
where
    PreviousOperators: Operator<In>,
    In: Data + IntoIterator<IntoIter = InnerIterator, Item = InnerIterator::Item>,
    Out: Data + Clone,
    InnerIterator: Iterator<Item = Out> + Clone + Send,
{
    fn setup(&mut self, metadata: &mut ExecutionMetadata) {
        self.prev.setup(metadata);

        self.operator_coord.from_coord(metadata.coord);
        if let Some(pb) = &metadata.persistency_builder{
            let p_service = pb.generate_persistency_service::<()>();
            p_service.restart_from_snapshot(self.operator_coord);
            self.persistency_service = Some(p_service);
        }
    }

    #[inline]
    fn next(&mut self) -> StreamElement<Out> {
        loop {
            if let Some(ref mut inner) = self.frontiter {
                match inner.next() {
                    None => self.frontiter = None,
                    #[cfg(not(feature = "timestamp"))]
                    Some(item) => return StreamElement::Item(item),
                    #[cfg(feature = "timestamp")]
                    Some(item) => match self.timestamp {
                        None => return StreamElement::Item(item),
                        Some(ts) => return StreamElement::Timestamped(item, ts),
                    },
                }
            }
            match self.prev.next() {
                #[cfg(not(feature = "timestamp"))]
                StreamElement::Item(inner) | StreamElement::Timestamped(inner, _) => {
                    self.frontiter = Some(inner.into_iter());
                }

                #[cfg(feature = "timestamp")]
                StreamElement::Item(inner) => {
                    self.frontiter = Some(inner.into_iter());
                    self.timestamp = None;
                }
                #[cfg(feature = "timestamp")]
                StreamElement::Timestamped(inner, ts) => {
                    self.frontiter = Some(inner.into_iter());
                    self.timestamp = Some(ts);
                }
                StreamElement::Watermark(ts) => return StreamElement::Watermark(ts),
                StreamElement::FlushBatch => return StreamElement::FlushBatch,
                StreamElement::Terminate => {
                    if self.persistency_service.is_some() {
                        // Save void terminated state                            
                        self.persistency_service.as_mut().unwrap().save_terminated_void_state(self.operator_coord);
                    }
                    return StreamElement::Terminate
                }
                StreamElement::FlushAndRestart => return StreamElement::FlushAndRestart,
                StreamElement::Snapshot(snap_id) => {
                    // Save void state and forward snapshot marker
                    // No state because when a snapshot marker arrives frontiter and timestamp are None 
                    self.persistency_service.as_mut().unwrap().save_void_state(self.operator_coord, snap_id.clone());
                    return StreamElement::Snapshot(snap_id);
                }
            }
        }
    }

    fn structure(&self) -> BlockStructure {
        self.prev
            .structure()
            .add_operator(OperatorStructure::new::<Out, _>("Flatten"))
    }

    fn get_op_id(&self) -> OperatorId {
        self.operator_coord.operator_id
    }
}

#[derive(Clone, Derivative)]
#[derivative(Debug)]
pub struct KeyedFlatten<Key, In, Out, InnerIterator, PreviousOperators>
where
    Key: DataKey,
    PreviousOperators: Operator<(Key, In)>,
    In: Data + IntoIterator<Item = Out>,
    Out: Data,
    InnerIterator: Iterator,
{
    prev: PreviousOperators,
    operator_coord: OperatorCoord,
    persistency_service: Option<PersistencyService<()>>,
    // used to store elements that have not been returned by next() yet
    // buffer: VecDeque<StreamElement<NewOut>>,
    // Make an element of type `Out` iterable
    // This is used to make `Flatten` behave differently when applied to `Stream` or `KeyedStream`
    // Takes `Out` as input, returns an `Iterator` with items of type `NewOut`
    #[derivative(Debug = "ignore")]
    frontiter: Option<(Key, InnerIterator)>,
    timestamp: Option<Timestamp>,
    _key: PhantomData<Key>,
    _in: PhantomData<In>,
    _iter_out: PhantomData<Out>,
}

impl<Key, In, Out, InnerIterator, PreviousOperators> Display
    for KeyedFlatten<Key, In, Out, InnerIterator, PreviousOperators>
where
    Key: DataKey,
    PreviousOperators: Operator<(Key, In)>,
    In: Data + IntoIterator<Item = Out>,
    Out: Data,
    InnerIterator: Iterator,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{} -> KeyedFlatten<{} -> {}>",
            self.prev,
            std::any::type_name::<In>(),
            std::any::type_name::<Out>()
        )
    }
}

impl<Key, In, Out, InnerIterator, PreviousOperators>
    KeyedFlatten<Key, In, Out, InnerIterator, PreviousOperators>
where
    Key: DataKey,
    PreviousOperators: Operator<(Key, In)>,
    In: Data + IntoIterator<IntoIter = InnerIterator, Item = InnerIterator::Item>,
    Out: Data + Clone,
    InnerIterator: Iterator<Item = Out> + Clone + Send,
{
    pub(super) fn new(prev: PreviousOperators) -> Self {
        let op_id = prev.get_op_id() + 1;
        Self {
            prev,
            // This will be set in setup method
            operator_coord: OperatorCoord::new(0,0,0,op_id),
            persistency_service: None,
            frontiter: None,
            timestamp: None,
            _key: Default::default(),
            _in: Default::default(),
            _iter_out: Default::default(),
        }
    }
}

impl<Key, In, Out, InnerIterator, PreviousOperators> Operator<(Key, Out)>
    for KeyedFlatten<Key, In, Out, InnerIterator, PreviousOperators>
where
    Key: DataKey,
    PreviousOperators: Operator<(Key, In)>,
    In: Data + IntoIterator<IntoIter = InnerIterator, Item = InnerIterator::Item>,
    Out: Data + Clone,
    InnerIterator: Iterator<Item = Out> + Clone + Send,
{
    fn setup(&mut self, metadata: &mut ExecutionMetadata) {
        self.prev.setup(metadata);

        self.operator_coord.from_coord(metadata.coord);
        if let Some(pb) = metadata.persistency_builder{
            let p_service = pb.generate_persistency_service::<()>();
            p_service.restart_from_snapshot(self.operator_coord);
            self.persistency_service = Some(p_service);
        }
    }

    fn next(&mut self) -> StreamElement<(Key, Out)> {
        loop {
            if let Some((ref key, ref mut inner)) = self.frontiter {
                match inner.next() {
                    None => self.frontiter = None,
                    #[cfg(not(feature = "timestamp"))]
                    Some(item) => return StreamElement::Item((key.clone(), item)),
                    #[cfg(feature = "timestamp")]
                    Some(item) => match self.timestamp {
                        None => return StreamElement::Item((key.clone(), item)),
                        Some(ts) => return StreamElement::Timestamped((key.clone(), item), ts),
                    },
                }
            }
            match self.prev.next() {
                #[cfg(not(feature = "timestamp"))]
                StreamElement::Item((key, inner)) | StreamElement::Timestamped((key, inner), _) => {
                    self.frontiter = Some((key, inner.into_iter()));
                }
                #[cfg(feature = "timestamp")]
                StreamElement::Item((key, inner)) => {
                    self.frontiter = Some((key, inner.into_iter()));
                    self.timestamp = None;
                }
                #[cfg(feature = "timestamp")]
                StreamElement::Timestamped((key, inner), ts) => {
                    self.frontiter = Some((key, inner.into_iter()));
                    self.timestamp = Some(ts);
                }
                StreamElement::Watermark(ts) => return StreamElement::Watermark(ts),
                StreamElement::FlushBatch => return StreamElement::FlushBatch,
                StreamElement::Terminate => {
                    if self.persistency_service.is_some() {
                        // Save void terminated state                            
                        self.persistency_service.as_mut().unwrap().save_terminated_void_state(self.operator_coord);
                    }
                    return StreamElement::Terminate
                }
                StreamElement::FlushAndRestart => return StreamElement::FlushAndRestart,
                StreamElement::Snapshot(snap_id) => {
                    // Save void state and forward snapshot marker
                    // No state because when a snapshot marker arrives frontiter and timestamp are None 
                    self.persistency_service.as_mut().unwrap().save_void_state(self.operator_coord, snap_id.clone());
                    return StreamElement::Snapshot(snap_id);
                }
            }
        }
    }

    fn structure(&self) -> BlockStructure {
        self.prev
            .structure()
            .add_operator(OperatorStructure::new::<Out, _>("KeyedFlatten"))
    }

    fn get_op_id(&self) -> OperatorId {
        self.operator_coord.operator_id
    }
}

#[cfg(test)]
mod tests {
    use crate::operator::flatten::Flatten;
    use crate::operator::{Operator, StreamElement};
    use crate::test::FakeOperator;

    #[test]
    fn test_flatten_no_timestamps() {
        let fake_operator = FakeOperator::new(
            vec![
                vec![],
                vec![0, 1, 2, 3],
                vec![],
                vec![4],
                vec![5, 6, 7],
                vec![],
            ]
            .into_iter(),
        );
        let mut flatten = Flatten::new(fake_operator);
        for i in 0..=7 {
            assert_eq!(flatten.next(), StreamElement::Item(i));
        }
        assert_eq!(flatten.next(), StreamElement::Terminate);
    }

    #[test]
    #[cfg(feature = "timestamp")]
    fn test_flatten_timestamped() {
        let mut fake_operator = FakeOperator::empty();
        fake_operator.push(StreamElement::Timestamped(vec![], 0));
        fake_operator.push(StreamElement::Timestamped(vec![1, 2, 3], 1));
        fake_operator.push(StreamElement::Timestamped(vec![4], 2));
        fake_operator.push(StreamElement::Timestamped(vec![], 3));
        fake_operator.push(StreamElement::Watermark(4));

        let mut flatten = Flatten::new(fake_operator);

        assert_eq!(flatten.next(), StreamElement::Timestamped(1, 1));
        assert_eq!(flatten.next(), StreamElement::Timestamped(2, 1));
        assert_eq!(flatten.next(), StreamElement::Timestamped(3, 1));
        assert_eq!(flatten.next(), StreamElement::Timestamped(4, 2));
        assert_eq!(flatten.next(), StreamElement::Watermark(4));
        assert_eq!(flatten.next(), StreamElement::Terminate);
    }
}