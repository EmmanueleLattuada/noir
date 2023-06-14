use std::fmt::Display;
use std::marker::PhantomData;

use serde::{Serialize, Deserialize};

use crate::block::{BlockStructure, OperatorStructure};
use crate::network::OperatorCoord;
use crate::operator::{Data, ExchangeData, Operator, StreamElement, Timestamp};
use crate::persistency::{PersistencyService, PersistencyServices};
use crate::scheduler::{ExecutionMetadata, OperatorId};
use crate::stream::Stream;

#[derive(Clone, Derivative)]
#[derivative(Debug)]
pub struct Fold<Out: Data, NewOut: ExchangeData, F, PreviousOperators>
where
    F: Fn(&mut NewOut, Out) + Send + Clone,
    PreviousOperators: Operator<Out>,
{
    prev: PreviousOperators,
    operator_coord: OperatorCoord,
    #[derivative(Debug = "ignore")]
    fold: F,
    init: NewOut,
    accumulator: Option<NewOut>,
    timestamp: Option<Timestamp>,
    max_watermark: Option<Timestamp>,
    received_end: bool,
    received_end_iter: bool,
    persistency_service: PersistencyService,
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
            
            fold,
            init,
            accumulator: None,
            timestamp: None,
            max_watermark: None,
            received_end: false,
            received_end_iter: false,
            persistency_service: PersistencyService::default(),
            _out: Default::default(),
        }
    }
}

#[derive(Clone, Serialize, Deserialize)]
struct FoldState<T> {
    accumulator: Option<T>,
    timestamp: Option<Timestamp>,
    max_watermark: Option<Timestamp>,
    received_end: bool,
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

        self.operator_coord.block_id = metadata.coord.block_id;
        self.operator_coord.host_id = metadata.coord.host_id;
        self.operator_coord.replica_id = metadata.coord.replica_id;

        self.persistency_service = metadata.persistency_service.clone();
        self.persistency_service.setup();
        let snapshot_id = self.persistency_service.restart_from_snapshot(self.operator_coord);
        if snapshot_id.is_some() {
            // Get and resume the persisted state
            let opt_state: Option<FoldState<NewOut>> = self.persistency_service.get_state(self.operator_coord, snapshot_id.unwrap());
            if let Some(state) = opt_state {
                self.accumulator = state.accumulator;
                self.timestamp = state.timestamp;
                self.max_watermark = state.max_watermark;
                self.received_end = state.received_end;
                self.received_end_iter = state.received_end_iter;
            } else {
                panic!("No persisted state founded for op: {0}", self.operator_coord);
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
                    // Save state and forward marker
                    let acc = self.accumulator.clone();
                    let state = FoldState{
                        accumulator: acc,
                        timestamp: self.timestamp,
                        max_watermark: self.max_watermark,
                        received_end: self.received_end,
                        received_end_iter: self.received_end_iter,
                    }; 
                    self.persistency_service.save_state(self.operator_coord, snap_id, state);
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
        if self.persistency_service.is_active() { 
            let acc = self.accumulator.clone();
            let state = FoldState{
                accumulator: acc,
                timestamp: self.timestamp,
                max_watermark: self.max_watermark,
                received_end: self.received_end,
                received_end_iter: self.received_end_iter,
            };                          
            self.persistency_service.save_terminated_state(self.operator_coord, state);
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

impl<Out: Data, OperatorChain> Stream<Out, OperatorChain>
where
    OperatorChain: Operator<Out> + 'static,
{
    /// Fold the stream into a stream that emits a single value.
    ///
    /// The folding operator consists in adding to the current accumulation value (initially the
    /// value provided as `init`) the value of the current item in the stream.
    ///
    /// The folding function is provided with a mutable reference to the current accumulator and the
    /// owned item of the stream. The function should modify the accumulator without returning
    /// anything.
    ///
    /// Note that the output type may be different from the input type. Consider using
    /// [`Stream::reduce`] if the output type is the same as the input type.
    ///
    /// **Note**: this operator will retain all the messages of the stream and emit the values only
    /// when the stream ends. Therefore this is not properly _streaming_.
    ///
    /// **Note**: this operator is not parallelized, it creates a bottleneck where all the stream
    /// elements are sent to and the folding is done using a single thread.
    ///
    /// **Note**: this is very similar to [`Iteartor::fold`](std::iter::Iterator::fold).
    ///
    /// **Note**: this operator will split the current block.
    ///
    /// ## Example
    ///
    /// ```
    /// # use noir::{StreamEnvironment, EnvironmentConfig};
    /// # use noir::operator::source::IteratorSource;
    /// # let mut env = StreamEnvironment::new(EnvironmentConfig::local(1));
    /// let s = env.stream(IteratorSource::new((0..5)));
    /// let res = s.fold(0, |acc, value| *acc += value).collect_vec();
    ///
    /// env.execute();
    ///
    /// assert_eq!(res.get().unwrap(), vec![0 + 1 + 2 + 3 + 4]);
    /// ```
    pub fn fold<NewOut: ExchangeData, F>(self, init: NewOut, f: F) -> Stream<NewOut, impl Operator<NewOut>>
    where
        Out: ExchangeData,
        F: Fn(&mut NewOut, Out) + Send + Clone + 'static,
    {
        self.max_parallelism(1)
            .add_operator(|prev| Fold::new(prev, init, f))
    }

    /// Fold the stream into a stream that emits a single value.
    ///
    /// The folding operator consists in adding to the current accumulation value (initially the
    /// value provided as `init`) the value of the current item in the stream.
    ///
    /// This method is very similary to [`Stream::fold`], but performs the folding distributely. To
    /// do so the folding function must be _associative_, in particular the folding process is
    /// performed in 2 steps:
    ///
    /// - `local`: the local function is used to fold the elements present in each replica of the
    ///   stream independently. All those replicas will start with the same `init` value.
    /// - `global`: all the partial results (the elements produced by the `local` step) have to be
    ///   aggregated into a single result. This is done using the `global` folding function.
    ///
    /// Note that the output type may be different from the input type, therefore requireing
    /// different function for the aggregation. Consider using [`Stream::reduce_assoc`] if the
    /// output type is the same as the input type.
    ///
    /// **Note**: this operator will retain all the messages of the stream and emit the values only
    /// when the stream ends. Therefore this is not properly _streaming_.
    ///
    /// **Note**: this operator will split the current block.
    ///
    /// ## Example
    ///
    /// ```
    /// # use noir::{StreamEnvironment, EnvironmentConfig};
    /// # use noir::operator::source::IteratorSource;
    /// # let mut env = StreamEnvironment::new(EnvironmentConfig::local(1));
    /// let s = env.stream(IteratorSource::new((0..5)));
    /// let res = s.fold_assoc(0, |acc, value| *acc += value, |acc, value| *acc += value).collect_vec();
    ///
    /// env.execute();
    ///
    /// assert_eq!(res.get().unwrap(), vec![0 + 1 + 2 + 3 + 4]);
    /// ```
    pub fn fold_assoc<NewOut: ExchangeData, Local, Global>(
        self,
        init: NewOut,
        local: Local,
        global: Global,
    ) -> Stream<NewOut, impl Operator<NewOut>>
    where
        Local: Fn(&mut NewOut, Out) + Send + Clone + 'static,
        Global: Fn(&mut NewOut, NewOut) + Send + Clone + 'static,
    {
        self.add_operator(|prev| Fold::new(prev, init.clone(), local))
            .max_parallelism(1)
            .add_operator(|prev| Fold::new(prev, init, global))
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
        fold.persistency_service = PersistencyService::new(Some(
            PersistencyConfig { 
                server_addr: String::from(REDIS_TEST_CONFIGURATION),
                try_restart: false,
                clean_on_exit: false,
                restart_from: None,
            }
        ));
        fold.persistency_service.setup();

        assert_eq!(fold.next(), StreamElement::Snapshot(SnapshotId::new(1)));
        let state: Option<FoldState<i32>> = fold.persistency_service.get_state(fold.operator_coord, SnapshotId::new(1));
        assert_eq!(state.unwrap().accumulator.unwrap(), 3);

        assert_eq!(fold.next(), StreamElement::Snapshot(SnapshotId::new(2)));
        let state: Option<FoldState<i32>> = fold.persistency_service.get_state(fold.operator_coord, SnapshotId::new(2));
        assert_eq!(state.unwrap().accumulator.unwrap(), 10);

        // Clean redis
        fold.persistency_service.delete_state(fold.operator_coord, SnapshotId::new(1));
        fold.persistency_service.delete_state(fold.operator_coord, SnapshotId::new(2));

    }
}
