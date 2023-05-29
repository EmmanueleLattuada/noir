//! The types related to the windowed streams.

use std::collections::{HashMap, VecDeque};
use std::fmt::Display;
use std::marker::PhantomData;

pub use descr::*;
use serde::{Serialize, Deserialize};
// pub use aggregator::*;
// pub use description::*;

use crate::block::{GroupHasherBuilder, OperatorStructure};
use crate::network::OperatorCoord;
use crate::operator::{Data, DataKey, ExchangeData, Operator, StreamElement, Timestamp};
use crate::persistency::{PersistencyService, PersistencyServices};
use crate::scheduler::OperatorId;
use crate::stream::{KeyValue, KeyedStream, Stream, WindowedStream};

use super::ExchangeDataKey;

mod aggr;
mod descr;

/// Convention: WindowAccumulator expects output to be called after at least one element has been processed.
/// Violating this convention may result in panics.
pub trait WindowBuilder<T> {
    type Manager<A: WindowAccumulator<In = T>>: WindowManager<In = T, Out = A::Out> + 'static;

    fn build<A: WindowAccumulator<In = T>>(&self, accumulator: A) -> Self::Manager<A>;
}

/// Convention: output will always be called after at least one element has been processed
pub trait WindowAccumulator: Clone + Send + 'static {
    type In: Data;
    type Out: Data;
    type AccumulatorState: ExchangeData;

    fn process(&mut self, el: Self::In);
    fn output(self) -> Self::Out;
    fn get_state(&self) -> Self::AccumulatorState;
    fn set_state(&mut self, state: Self::AccumulatorState);
}

#[derive(Clone)]
pub(crate) struct KeyedWindowManager<Key, In, Out, W: WindowManager> {
    windows: HashMap<Key, W, GroupHasherBuilder>,
    init: W,
    _in: PhantomData<In>,
    _out: PhantomData<Out>,
}

pub trait WindowManager: Clone + Send {
    type In: Data;
    type Out: Data;
    type Output: IntoIterator<Item = WindowResult<Self::Out>>;
    type ManagerState: ExchangeData;
    /// Process an input element updating any interest window.
    /// Output the results that have become ready after processing this element.
    fn process(&mut self, el: StreamElement<Self::In>) -> Self::Output;
    /// Return true if the manager has no active windows and can be dropped
    fn recycle(&self) -> bool {
        false
    }
    /// Get the state
    fn get_state(&self) -> Self::ManagerState;
    /// Set the state
    fn set_state(&mut self, state: Self::ManagerState);
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WindowResult<T> {
    Item(T),
    Timestamped(T, Timestamp),
}

impl<T> WindowResult<T> {
    #[inline]
    pub fn new(item: T, timestamp: Option<Timestamp>) -> Self {
        match timestamp {
            Some(ts) => WindowResult::Timestamped(item, ts),
            None => WindowResult::Item(item),
        }
    }

    #[inline]
    pub fn item(&self) -> &T {
        match self {
            WindowResult::Item(item) => item,
            WindowResult::Timestamped(item, _) => item,
        }
    }

    #[inline]
    pub fn unwrap_item(self) -> T {
        match self {
            WindowResult::Item(item) => item,
            WindowResult::Timestamped(item, _) => item,
        }
    }
}

impl<T> From<WindowResult<T>> for StreamElement<T> {
    #[inline]
    fn from(value: WindowResult<T>) -> Self {
        match value {
            WindowResult::Item(item) => StreamElement::Item(item),
            WindowResult::Timestamped(item, ts) => StreamElement::Timestamped(item, ts),
        }
    }
}

/// This operator abstracts the window logic as an operator and delegates to the
/// `KeyedWindowManager` and a `ProcessFunc` the job of building and processing the windows,
/// respectively.
#[derive(Clone)]
pub(crate) struct WindowOperator<Key, In, Out, Prev, W>
where
    W: WindowManager,
{
    /// The previous operators in the chain.
    prev: Prev,
    /// Operator coordinate
    operator_coord: OperatorCoord,
    /// Persistency service
    persistency_service: PersistencyService,
    /// The name of the actual operator that this one abstracts.
    ///
    /// It is used only for tracing purposes.
    name: String,
    /// The manager that will build the windows.
    manager: KeyedWindowManager<Key, In, Out, W>,
    /// A buffer for storing ready items.
    output_buffer: VecDeque<StreamElement<KeyValue<Key, Out>>>,
}

impl<Key, In, Out, Prev, W> Display for WindowOperator<Key, In, Out, Prev, W>
where
    W: WindowManager,
    Prev: Display,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{} -> {} -> WindowOperator[{}]<{}>",
            self.prev,
            std::any::type_name::<W>(),
            self.name,
            std::any::type_name::<Out>(),
        )
    }
}

#[derive(Clone, Serialize, Deserialize)]
pub (crate) struct WindowOperatorState<Key: DataKey, Out, ManagerState> {
    manager: KeyedWindowManagerState<Key, ManagerState>,
    output_buffer: VecDeque<StreamElement<KeyValue<Key, Out>>>,
}

#[derive(Clone, Serialize, Deserialize)]
pub (crate) struct KeyedWindowManagerState<Key: DataKey, ManagerState> {
    windows: HashMap<Key, ManagerState>,
}



impl<Key, In, Out, Prev, W> Operator<KeyValue<Key, Out>> for WindowOperator<Key, In, Out, Prev, W>
where
    W: WindowManager<In = In, Out = Out> + Send,
    Prev: Operator<KeyValue<Key, In>>,
    Key: ExchangeDataKey,
    In: ExchangeData,
    Out: ExchangeData,
{
    fn setup(&mut self, metadata: &mut crate::ExecutionMetadata) {
        self.prev.setup(metadata);

        self.operator_coord.block_id = metadata.coord.block_id;
        self.operator_coord.host_id = metadata.coord.host_id;
        self.operator_coord.replica_id = metadata.coord.replica_id;

        self.persistency_service = metadata.persistency_service.clone();
        self.persistency_service.setup();
        let snapshot_id = self.persistency_service.restart_from_snapshot(self.operator_coord);
        if snapshot_id.is_some() {
            // Get and resume the persisted state
            let opt_state: Option<WindowOperatorState<Key, Out, W::ManagerState>> = self.persistency_service.get_state(self.operator_coord, snapshot_id.unwrap());
            if let Some(state) = opt_state {
                self.output_buffer = state.output_buffer;                
                state.manager.windows
                    .iter()
                    .for_each(|(key, manager_state)| {
                        let mut new_man = self.manager.init.clone();
                        new_man.set_state(manager_state.clone());
                        self
                            .manager
                            .windows
                            .insert(key.clone(), new_man);

                    });
            } else {
                panic!("No persisted state founded for op: {0}", self.operator_coord);
            } 
        }
    }

    fn next(&mut self) -> StreamElement<KeyValue<Key, Out>> {
        loop {
            if let Some(item) = self.output_buffer.pop_front() {
                match item {
                    StreamElement::Terminate => {
                        if self.persistency_service.is_active() {
                            // Save terminated state
                            let windows = HashMap::from_iter(
                                self.manager.windows
                                    .clone()
                                    .iter()
                                    .map(|(key, manager)| (key.clone(), manager.get_state()))
                            );
                            let manager_state = KeyedWindowManagerState {
                                windows,
                            };
                            let state = WindowOperatorState {
                                manager: manager_state,
                                output_buffer: self.output_buffer.clone(),
                            };
                            self.persistency_service.save_terminated_state(self.operator_coord, state);
                        }
                    }
                    _ => {}
                }
                return item;
            }

            let el = self.prev.next();
            match el {
                el @ (StreamElement::Item(_) | StreamElement::Timestamped(_, _)) => {
                    let (key, el) = el.take_key();
                    let key = key.unwrap();

                    let mgr = self
                        .manager
                        .windows
                        .entry(key.clone())
                        .or_insert_with(|| self.manager.init.clone());

                    let ret = mgr.process(el);
                    self.output_buffer.extend(
                        ret.into_iter()
                            .map(|e| StreamElement::from(e).add_key(key.clone())),
                    );
                }
                StreamElement::FlushBatch => return StreamElement::FlushBatch,
                StreamElement::Snapshot(snap_id) => {
                    let windows = HashMap::from_iter(
                        self.manager.windows
                            .clone()
                            .iter()
                            .map(|(key, manager)| (key.clone(), manager.get_state()))
                    );
                    let manager_state = KeyedWindowManagerState {
                        windows,
                    };
                    let state = WindowOperatorState {
                        manager: manager_state,
                        output_buffer: self.output_buffer.clone(),
                    };
                    self.persistency_service.save_state(self.operator_coord, snap_id, state);
                    self.output_buffer.push_back(StreamElement::Snapshot(snap_id));

                }
                el => {
                    let (_, el) = el.take_key();

                    self.manager.windows.retain(|key, mgr| {
                        let ret = mgr.process(el.clone());                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                               
                        self.output_buffer.extend(
                            ret.into_iter()
                                .map(|e| StreamElement::from(e).add_key(key.clone())),
                        );
                        !mgr.recycle()
                    });

                    // Forward system messages and watermarks
                    let msg = match el {
                        StreamElement::Watermark(w) => StreamElement::Watermark(w),
                        StreamElement::Terminate => StreamElement::Terminate,
                        StreamElement::FlushAndRestart => StreamElement::FlushAndRestart,
                        _ => unreachable!(),
                    };
                    self.output_buffer.push_back(msg);
                }
            }
        }
    }

    fn structure(&self) -> crate::block::BlockStructure {
        let mut operator = OperatorStructure::new::<KeyValue<Key, Out>, _>(&self.name);
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

impl<Key, In, Out, Prev, W> WindowOperator<Key, In, Out, Prev, W>
where
    Prev: Operator<KeyValue<Key, In>>,
    Key: DataKey,
    In: Data,
    Out: Data,
    W: WindowManager,
{
    pub(crate) fn new(
        prev: Prev,
        name: String,
        manager: KeyedWindowManager<Key, In, Out, W>,
    ) -> Self {
        let op_id = prev.get_op_id() + 1;
        Self {
            prev,  
            // This will be set in setup method          
            operator_coord: OperatorCoord::new(0, 0, 0, op_id),
            persistency_service: PersistencyService::default(),
            name,
            manager,
            output_buffer: Default::default(),
        }
    }
}

impl<Key, Out, WindowDescr, OperatorChain> WindowedStream<Key, Out, OperatorChain, Out, WindowDescr>
where
    WindowDescr: WindowBuilder<Out>,
    OperatorChain: Operator<KeyValue<Key, Out>> + 'static,
    Key: ExchangeDataKey,
    Out: ExchangeData,
{
    /// Add a new generic window operator to a `KeyedWindowedStream`,
    /// after adding a Reorder operator.
    /// This should be used by every custom window aggregator.
    pub(crate) fn add_window_operator<A, NewOut>(
        self,
        name: &str,
        accumulator: A,
    ) -> KeyedStream<Key, NewOut, impl Operator<KeyValue<Key, NewOut>>>
    where
        NewOut: ExchangeData,
        A: WindowAccumulator<In = Out, Out = NewOut>,
    {
        let stream = self.inner;
        let init = self.descr.build::<A>(accumulator);

        let manager: KeyedWindowManager<Key, Out, NewOut, WindowDescr::Manager<A>> =
            KeyedWindowManager {
                windows: HashMap::default(),
                init,
                _in: PhantomData,
                _out: PhantomData,
            };

        stream // .add_operator(Reorder::new)
            .add_operator(|prev| WindowOperator::new(prev, name.into(), manager))
    }
}

impl<Key: DataKey, Out: Data, OperatorChain> KeyedStream<Key, Out, OperatorChain>
where
    OperatorChain: Operator<KeyValue<Key, Out>> + 'static,
{
    /// Apply a window to the stream.
    ///
    /// Returns a [`KeyedWindowedStream`], with windows created following the behavior specified
    /// by the passed [`WindowDescription`].
    ///
    /// ## Example
    /// ```
    /// # use noir::{StreamEnvironment, EnvironmentConfig};
    /// # use noir::operator::source::IteratorSource;
    /// # use noir::operator::window::CountWindow;
    /// # let mut env = StreamEnvironment::new(EnvironmentConfig::local(1));
    /// let s = env.stream(IteratorSource::new((0..9)));
    /// let res = s
    ///     .group_by(|&n| n % 2)
    ///     .window(CountWindow::sliding(3, 2))
    ///     .sum()
    ///     .collect_vec();
    ///
    /// env.execute();
    ///
    /// let mut res = res.get().unwrap();
    /// res.sort_unstable();
    /// assert_eq!(res, vec![(0, 0 + 2 + 4), (0, 4 + 6 + 8), (1, 1 + 3 + 5)]);
    /// ```
    pub fn window<WinOut: Data, WinDescr: WindowBuilder<Out>>(
        self,
        descr: WinDescr,
    ) -> WindowedStream<Key, Out, impl Operator<KeyValue<Key, Out>>, WinOut, WinDescr> {
        WindowedStream {
            inner: self,
            descr,
            _win_out: PhantomData,
        }
    }
}

impl<Out: ExchangeData, OperatorChain> Stream<Out, OperatorChain>
where
    OperatorChain: Operator<Out> + 'static,
{
    /// Send all elements to a single node and apply a window to the stream.
    ///
    /// Returns a [`KeyedWindowedStream`], with key `()` with windows created following the behavior specified
    /// by the passed [`WindowDescription`].
    ///
    /// **Note**: this operator cannot be parallelized, so all the stream elements are sent to a
    /// single node where the creation and aggregation of the windows are done.
    ///
    /// ## Example
    /// ```
    /// # use noir::{StreamEnvironment, EnvironmentConfig};
    /// # use noir::operator::source::IteratorSource;
    /// # use noir::operator::window::CountWindow;
    /// # let mut env = StreamEnvironment::new(EnvironmentConfig::local(1));
    /// let s = env.stream(IteratorSource::new((0..5usize)));
    /// let res = s
    ///     .window_all(CountWindow::tumbling(2))
    ///     .sum::<usize>()
    ///     .drop_key()
    ///     .collect_vec();
    ///
    /// env.execute();
    ///
    /// let mut res = res.get().unwrap();
    /// assert_eq!(res, vec![0 + 1, 2 + 3]);
    /// ```
    pub fn window_all<WinOut: Data, WinDescr: WindowBuilder<Out>>(
        self,
        descr: WinDescr,
    ) -> WindowedStream<(), Out, impl Operator<KeyValue<(), Out>>, WinOut, WinDescr> {
        // max_parallelism and key_by are used instead of group_by so that there is exactly one
        // replica, since window_all cannot be parallelized
        self.max_parallelism(1).key_by(|_| ()).window(descr)
    }
}
