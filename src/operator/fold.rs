use std::fmt::Display;
use std::marker::PhantomData;

use serde::{Serialize, Deserialize};

use crate::block::{BlockStructure, OperatorStructure};
use crate::network::OperatorCoord;
use crate::operator::{Data, ExchangeData, Operator, StreamElement, Timestamp};
use crate::persistency::{PersistencyService, PersistencyServices};
use crate::scheduler::{ExecutionMetadata, OperatorId};

use super::SnapshotId;

#[derive(Clone, Derivative)]
#[derivative(Debug)]
pub struct Fold<Out: Data, NewOut: ExchangeData, F, PreviousOperators>
where
    F: Fn(&mut NewOut, Out) + Send + Clone,
    PreviousOperators: Operator<Out>,
{
    prev: PreviousOperators,
    operator_coord: OperatorCoord,
    persistency_service: Option<PersistencyService>,
    #[derivative(Debug = "ignore")]
    fold: F,
    init: NewOut,
    accumulator: Option<NewOut>,
    timestamp: Option<Timestamp>,
    max_watermark: Option<Timestamp>,
    received_end: bool,
    received_end_iter: bool,
    _out: PhantomData<Out>,
}

impl<Out: Data, NewOut: ExchangeData, F, PreviousOperators> Display
    for Fold<Out, NewOut, F, PreviousOperators>
where
    F: Fn(&mut NewOut, Out) + Send + Clone,
    PreviousOperators: Operator<Out>,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{} -> Fold<{} -> {}>",
            self.prev,
            std::any::type_name::<Out>(),
            std::any::type_name::<NewOut>()
        )
    }
}

impl<Out: Data, NewOut: ExchangeData, F, PreviousOperators: Operator<Out>>
    Fold<Out, NewOut, F, PreviousOperators>
where
    F: Fn(&mut NewOut, Out) + Send + Clone,
{
    pub(super) fn new(prev: PreviousOperators, init: NewOut, fold: F) -> Self {
        let op_id = prev.get_op_id() + 1;
        Fold {
            prev,
            // This will be set in setup method
            operator_coord: OperatorCoord::new(0,0,0,op_id),
            persistency_service: None,
            fold,
            init,
            accumulator: None,
            timestamp: None,
            max_watermark: None,
            received_end: false,
            received_end_iter: false,
            _out: Default::default(),
        }
    }

    /// Save state for snapshot
    fn save_snap(&mut self, snapshot_id: SnapshotId){
        let acc = self.accumulator.clone();
        let state = FoldState{
            accumulator: acc,
            timestamp: self.timestamp,
            max_watermark: self.max_watermark,
            received_end_iter: self.received_end_iter,
        }; 
        self.persistency_service.as_mut().unwrap().save_state(self.operator_coord, snapshot_id, state);
    }
    /// Save terminated state
    fn save_terminate(&mut self){
        let acc = self.accumulator.clone();
        let state = FoldState{
            accumulator: acc,
            timestamp: self.timestamp,
            max_watermark: self.max_watermark,
            received_end_iter: self.received_end_iter,
        };                          
        self.persistency_service.as_mut().unwrap().save_terminated_state(self.operator_coord, state);
    }
}

#[derive(Clone, Serialize, Deserialize)]
struct FoldState<T> {
    accumulator: Option<T>,
    timestamp: Option<Timestamp>,
    max_watermark: Option<Timestamp>,
    received_end_iter: bool,
}

impl<Out: Data, NewOut: ExchangeData, F, PreviousOperators> Operator<NewOut>
    for Fold<Out, NewOut, F, PreviousOperators>
where
    F: Fn(&mut NewOut, Out) + Send + Clone,
    PreviousOperators: Operator<Out>,
{
    fn setup(&mut self, metadata: &mut ExecutionMetadata) {
        self.prev.setup(metadata);

        self.operator_coord.from_coord(metadata.coord);
        if metadata.persistency_service.is_some(){
            self.persistency_service = metadata.persistency_service.clone();
            let snapshot_id = self.persistency_service.as_mut().unwrap().restart_from_snapshot(self.operator_coord);
            if snapshot_id.is_some() {
                // Get and resume the persisted state
                let opt_state: Option<FoldState<NewOut>> = self.persistency_service.as_mut().unwrap().get_state(self.operator_coord, snapshot_id.unwrap());
                if let Some(state) = opt_state {
                    self.accumulator = state.accumulator;
                    self.timestamp = state.timestamp;
                    self.max_watermark = state.max_watermark;
                    self.received_end_iter = state.received_end_iter;
                } else {
                    panic!("No persisted state founded for op: {0}", self.operator_coord);
                } 
            }
        }
    }

    #[inline]
    fn next(&mut self) -> StreamElement<NewOut> {
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
                StreamElement::Item(item) => {
                    if self.accumulator.is_none() {
                        self.accumulator = Some(self.init.clone());
                    }
                    (self.fold)(self.accumulator.as_mut().unwrap(), item);
                }
                StreamElement::Timestamped(item, ts) => {
                    self.timestamp = Some(self.timestamp.unwrap_or(ts).max(ts));
                    if self.accumulator.is_none() {
                        self.accumulator = Some(self.init.clone());
                    }
                    (self.fold)(self.accumulator.as_mut().unwrap(), item);
                }
                // this block wont sent anything until the stream ends
                StreamElement::FlushBatch => {}
                StreamElement::Snapshot(snap_id) => {
                    self.save_snap(snap_id);
                    return StreamElement::Snapshot(snap_id);
                }
            }
        }

        // If there is an accumulated value, return it
        if let Some(acc) = self.accumulator.take() {
            if let Some(ts) = self.timestamp.take() {
                return StreamElement::Timestamped(acc, ts);
            } else {
                return StreamElement::Item(acc);
            }
        }

        // If watermark were received, send one downstream
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
        let mut operator = OperatorStructure::new::<NewOut, _>("Fold");
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

#[cfg(test)]
mod tests {
    use serial_test::serial;

    use crate::config::PersistencyConfig;
    use crate::network::OperatorCoord;
    use crate::operator::fold::{Fold, FoldState};
    use crate::operator::{Operator, StreamElement, SnapshotId};
    use crate::persistency::{PersistencyService, PersistencyServices};
    use crate::test::{FakeOperator, REDIS_TEST_CONFIGURATION};

    #[test]
    fn test_fold_without_timestamps() {
        let fake_operator = FakeOperator::new(0..10u8);
        let mut fold = Fold::new(fake_operator, 0, |a, b| *a += b);

        assert_eq!(fold.next(), StreamElement::Item((0..10u8).sum()));
        assert_eq!(fold.next(), StreamElement::Terminate);
    }

    #[test]
    #[allow(clippy::identity_op)]
    #[cfg(feature = "timestamp")]
    fn test_fold_timestamped() {
        let mut fake_operator = FakeOperator::empty();
        fake_operator.push(StreamElement::Timestamped(0, 1));
        fake_operator.push(StreamElement::Timestamped(1, 2));
        fake_operator.push(StreamElement::Timestamped(2, 3));
        fake_operator.push(StreamElement::Watermark(4));

        let mut fold = Fold::new(fake_operator, 0, |a, b| *a += b);

        assert_eq!(fold.next(), StreamElement::Timestamped(0 + 1 + 2, 3));
        assert_eq!(fold.next(), StreamElement::Watermark(4));
        assert_eq!(fold.next(), StreamElement::Terminate);
    }

    #[test]
    #[allow(clippy::identity_op)]
    fn test_fold_iter_end() {
        let mut fake_operator = FakeOperator::empty();
        fake_operator.push(StreamElement::Item(0));
        fake_operator.push(StreamElement::Item(1));
        fake_operator.push(StreamElement::Item(2));
        fake_operator.push(StreamElement::FlushAndRestart);
        fake_operator.push(StreamElement::Item(3));
        fake_operator.push(StreamElement::Item(4));
        fake_operator.push(StreamElement::Item(5));
        fake_operator.push(StreamElement::FlushAndRestart);

        let mut fold = Fold::new(fake_operator, 0, |a, b| *a += b);

        assert_eq!(fold.next(), StreamElement::Item(0 + 1 + 2));
        assert_eq!(fold.next(), StreamElement::FlushAndRestart);
        assert_eq!(fold.next(), StreamElement::Item(3 + 4 + 5));
        assert_eq!(fold.next(), StreamElement::FlushAndRestart);
        assert_eq!(fold.next(), StreamElement::Terminate);
    }

    #[test]
    #[serial]
    fn test_fold_persistency_save_state() {
        let mut fake_operator = FakeOperator::empty();
        fake_operator.push(StreamElement::Item(1));
        fake_operator.push(StreamElement::Item(2));
        fake_operator.push(StreamElement::Snapshot(SnapshotId::new(1)));
        fake_operator.push(StreamElement::Item(3));
        fake_operator.push(StreamElement::Item(4));
        fake_operator.push(StreamElement::Snapshot(SnapshotId::new(2)));

        let mut fold = Fold::new(fake_operator, 0, |a, b| *a += b);
 
        fold.operator_coord = OperatorCoord{
            block_id: 0,
            host_id: 0,
            replica_id: 1,
            operator_id: 1,
        };
        fold.persistency_service = Some(PersistencyService::new(Some(
            PersistencyConfig { 
                server_addr: String::from(REDIS_TEST_CONFIGURATION),
                try_restart: false,
                clean_on_exit: false,
                restart_from: None,
            }
        )));

        assert_eq!(fold.next(), StreamElement::Snapshot(SnapshotId::new(1)));
        let state: Option<FoldState<i32>> = fold.persistency_service.as_mut().unwrap().get_state(fold.operator_coord, SnapshotId::new(1));
        assert_eq!(state.unwrap().accumulator.unwrap(), 3);

        assert_eq!(fold.next(), StreamElement::Snapshot(SnapshotId::new(2)));
        let state: Option<FoldState<i32>> = fold.persistency_service.as_mut().unwrap().get_state(fold.operator_coord, SnapshotId::new(2));
        assert_eq!(state.unwrap().accumulator.unwrap(), 10);

        // Clean redis
        fold.persistency_service.as_mut().unwrap().delete_state(fold.operator_coord, SnapshotId::new(1));
        fold.persistency_service.as_mut().unwrap().delete_state(fold.operator_coord, SnapshotId::new(2));

    }
}
