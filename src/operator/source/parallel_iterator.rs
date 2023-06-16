use std::fmt::Display;
use std::ops::Range;
use std::time::Duration;

use serde::{Serialize, Deserialize};

use crate::block::{BlockStructure, OperatorKind, OperatorStructure, Replication};
use crate::network::OperatorCoord;
use crate::operator::source::Source;
use crate::operator::{Data, Operator, StreamElement};
use crate::persistency::{PersistencyService, PersistencyServices};
use crate::scheduler::{ExecutionMetadata, OperatorId};
use crate::{CoordUInt, Stream};

use super::SnapshotGenerator;

pub trait IntoParallelSource: Clone + Send {
    type Item;
    type Iter: Iterator<Item = Self::Item>;
    fn generate_iterator(self, index: CoordUInt, peers: CoordUInt) -> Self::Iter;
}

impl<It, GenIt, Out> IntoParallelSource for GenIt
where
    It: Iterator<Item = Out> + Send + 'static,
    GenIt: FnOnce(CoordUInt, CoordUInt) -> It + Send + Clone,
{
    type Item = Out;

    type Iter = It;

    fn generate_iterator(self, index: CoordUInt, peers: CoordUInt) -> Self::Iter {
        self(index, peers)
    }
}

impl IntoParallelSource for Range<u64> {
    type Item = u64;

    type Iter = Range<u64>;

    fn generate_iterator(self, index: CoordUInt, peers: CoordUInt) -> Self::Iter {
        let n = self.end - self.start;
        let chunk_size = (n.saturating_add(peers - 1)) / peers;
        let start = self.start.saturating_add(index * chunk_size);
        let end = (start.saturating_add(chunk_size))
            .min(self.end.saturating_sub(1))
            .max(self.start);

        start..end
    }
}

macro_rules! impl_into_parallel_source {
    ($t:ty) => {
        impl IntoParallelSource for Range<$t> {
            type Item = $t;

            type Iter = Range<$t>;

            fn generate_iterator(self, index: CoordUInt, peers: CoordUInt) -> Self::Iter {
                let index: i64 = index.try_into().unwrap();
                let peers: i64 = peers.try_into().unwrap();
                let n = self.end as i64 - self.start as i64;
                let chunk_size = (n.saturating_add(peers - 1)) / peers;
                let start = (self.start as i64).saturating_add(index * chunk_size);
                let end = (start.saturating_add(chunk_size))
                    .min(self.end as i64 - 1)
                    .max(self.start as i64);

                let (start, end) = (start.try_into().unwrap(), end.try_into().unwrap());
                start..end
            }
        }
    };
}

impl_into_parallel_source!(u8);
impl_into_parallel_source!(u16);
impl_into_parallel_source!(u32);

impl_into_parallel_source!(usize);

impl_into_parallel_source!(i8);
impl_into_parallel_source!(i16);
impl_into_parallel_source!(i32);
impl_into_parallel_source!(i64);
impl_into_parallel_source!(isize);

/// This enum wraps either an `Iterator` that yields the items, or a generator function that
/// produces such iterator.
///
/// This enum is `Clone` only _before_ generating the iterator. The generator function must be
/// `Clone`, but the resulting iterator doesn't have to be so.
enum IteratorGenerator<Source: IntoParallelSource> {
    /// The function that generates the iterator.
    Generator(Source),
    /// The actual iterator that produces the items.
    Iterator(Source::Iter),
    /// An extra variant used when moving the generator out of the enum, and before putting back the
    /// iterator. This makes this enum panic-safe in the `generate` method.
    Generating,
}

impl<Source: IntoParallelSource> IteratorGenerator<Source> {
    /// Consume the generator function and store the produced iterator.
    ///
    /// This method can be called only once.
    fn generate(&mut self, global_id: CoordUInt, instances: CoordUInt) {
        let gen = std::mem::replace(self, IteratorGenerator::Generating);
        let iter = match gen {
            IteratorGenerator::Generator(gen) => gen.generate_iterator(global_id, instances),
            _ => unreachable!("generate on non-Generator variant"),
        };
        *self = IteratorGenerator::Iterator(iter);
    }

    /// If the `generate` method has been called, get the next element from the iterator.
    fn next(&mut self) -> Option<Source::Item> {
        match self {
            IteratorGenerator::Iterator(iter) => iter.next(),
            _ => unreachable!("next on non-Iterator variant"),
        }
    }

    /// If the `generate` method has been called, get the n-th element from the iterator.
    fn nth(&mut self, n: usize) -> Option<Source::Item> {
        match self {
            IteratorGenerator::Iterator(iter) => iter.nth(n) ,
            _ => unreachable!("nth on non-Iterator variant"),
        }
    }
}

impl<Source: IntoParallelSource> Clone for IteratorGenerator<Source> {
    fn clone(&self) -> Self {
        match self {
            Self::Generator(gen) => Self::Generator(gen.clone()),
            _ => panic!("Can clone only before generating the iterator"),
        }
    }
}

/// Source that ingests items into a stream using the maximum parallelism. The items are from the
/// iterators returned by a generating function.
///
/// Each replica (i.e. each core) will have a different iterator. The iterator are produced by a
/// generating function passed to the [`ParallelIteratorSource::new`] method.
#[derive(Derivative)]
#[derivative(Debug)]
pub struct ParallelIteratorSource<Source>
where
    Source: IntoParallelSource,
    Source::Item: Data,
{
    #[derivative(Debug = "ignore")]
    inner: IteratorGenerator<Source>,
    last_index: Option<u64>,
    terminated: bool,
    operator_coord: OperatorCoord,
    snapshot_generator: SnapshotGenerator,
    persistency_service: PersistencyService,
}

impl<Source> Display for ParallelIteratorSource<Source>
where
    Source: IntoParallelSource,
    Source::Item: Data,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "ParallelIteratorSource<{}>",
            std::any::type_name::<Source::Item>()
        )
    }
}

impl<Source> ParallelIteratorSource<Source>
where
    Source: IntoParallelSource,
    Source::Item: Data,
{
    /// Create a new source that ingest items into the stream using the maximum parallelism
    /// available.
    ///
    /// The function passed as argument is cloned in each core, and called to get the iterator for
    /// that replica. The first parameter passed to the function is a 0-based index of the replica,
    /// while the second is the total number of replicas.
    ///
    /// ## Example
    ///
    /// ```
    /// # use noir::{StreamEnvironment, EnvironmentConfig};
    /// # use noir::operator::source::ParallelIteratorSource;
    /// # let mut env = StreamEnvironment::new(EnvironmentConfig::local(1));
    /// // generate the numbers from 0 to 99 using multiple replicas
    /// let n = 100;
    /// let source = ParallelIteratorSource::new(move |id, instances| {
    ///     let chunk_size = (n + instances - 1) / instances;
    ///     let remaining = n - n.min(chunk_size * id);
    ///     let range = remaining.min(chunk_size);
    ///     
    ///     let start = id * chunk_size;
    ///     let stop = id * chunk_size + range;
    ///     start..stop
    /// });
    /// let s = env.stream(source);
    /// ```
    pub fn new(generator: Source) -> Self {
        Self {
            inner: IteratorGenerator::Generator(generator),
            last_index: None,
            terminated: false,
            // This is the first operator in the chain so operator_id = 0
            // Other fields will be set in setup method
            operator_coord: OperatorCoord::new(0, 0, 0, 0),
            snapshot_generator: SnapshotGenerator::new(),
            persistency_service: PersistencyService::default(),
        }
    }
}

impl<S> Source<S::Item> for ParallelIteratorSource<S>
where
    S: IntoParallelSource,
    S::Item: Data,
    S::Iter: Send,
{
    fn replication(&self) -> Replication {
        Replication::Unlimited
    }

    fn set_snapshot_frequency_by_item(&mut self, item_interval: u64) {
        self.snapshot_generator.set_item_interval(item_interval);
    }

    fn set_snapshot_frequency_by_time(&mut self, time_interval: Duration) {
        self.snapshot_generator.set_time_interval(time_interval);
    }
}

#[derive(Clone, Serialize, Deserialize)]
struct ParallelIteratorSourceState {
    last_index: Option<u64>,
}

impl<Source> Operator<Source::Item> for ParallelIteratorSource<Source>
where
    Source: IntoParallelSource,
    Source::Iter: Send,
    Source::Item: Data,
{
    fn setup(&mut self, metadata: &mut ExecutionMetadata) {
        self.inner.generate(
            metadata.global_id,
            metadata
                .replicas
                .len()
                .try_into()
                .expect("Num replicas > max id"),
        );

        self.operator_coord.block_id = metadata.coord.block_id;
        self.operator_coord.host_id = metadata.coord.host_id;
        self.operator_coord.replica_id = metadata.coord.replica_id;

        self.persistency_service = metadata.persistency_service.clone();
        self.persistency_service.setup();
        let snapshot_id = self.persistency_service.restart_from_snapshot(self.operator_coord);
        if let Some(snap_id) = snapshot_id {
            // Get and resume the persisted state
            let opt_state: Option<ParallelIteratorSourceState> = self.persistency_service.get_state(self.operator_coord, snap_id);
            if let Some(state) = opt_state {
                self.terminated = snap_id.terminate();
                if let Some(idx) = state.last_index {
                    self.inner.nth(idx as usize);
                    self.last_index = Some(idx);
                }
            } else {
                panic!("No persisted state founded for op: {0}", self.operator_coord);
            }
            self.snapshot_generator.restart_from(snap_id);
        }
    }

    fn next(&mut self) -> StreamElement<Source::Item> {
        if self.terminated {
            if self.persistency_service.is_active() {
                // Save terminated state
                let state = ParallelIteratorSourceState{
                    last_index: self.last_index,
                };
                self.persistency_service.save_terminated_state(self.operator_coord, state);
            } 
            return StreamElement::Terminate;                       
        }
        // Check snapshot generator
        let snapshot = self.snapshot_generator.get_snapshot_marker();
        if snapshot.is_some() {
            let snapshot_id = snapshot.unwrap();
            // Save state and forward snapshot marker
            let state = ParallelIteratorSourceState{
                last_index: self.last_index,
            };
            self.persistency_service.save_state(self.operator_coord, snapshot_id, state);
            return StreamElement::Snapshot(snapshot_id);
        }
        // TODO: with adaptive batching this does not work since it never emits FlushBatch messages
        match self.inner.next() {
            Some(t) => {
                if self.last_index.is_none() {
                    self.last_index = Some(0);
                } else {
                    self.last_index = Some(self.last_index.unwrap() + 1);
                }
                StreamElement::Item(t) 
            }
            None => {
                self.terminated = true;
                StreamElement::FlushAndRestart
            }
        }
    }

    fn structure(&self) -> BlockStructure {
        let mut operator = OperatorStructure::new::<Source::Item, _>("ParallelIteratorSource");
        let op_id = self.operator_coord.operator_id;
        operator.subtitle = format!("op id: {op_id}");
        operator.kind = OperatorKind::Source;
        BlockStructure::default().add_operator(operator)
    }

    fn get_op_id(&self) -> OperatorId {
        self.operator_coord.operator_id
    }
}

impl<Source> Clone for ParallelIteratorSource<Source>
where
    Source: IntoParallelSource,
    Source::Item: Data,
{
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
            last_index: self.last_index,
            terminated: false,
            operator_coord: self.operator_coord,
            snapshot_generator: self.snapshot_generator.clone(),
            persistency_service: self.persistency_service.clone(),
        }
    }
}

impl crate::StreamEnvironment {
    /// Convenience method, creates a `ParallelIteratorSource` and makes a stream using `StreamEnvironment::stream`
    /// # Example:
    /// ```
    /// use noir::prelude::*;
    ///
    /// let mut env = StreamEnvironment::default();
    ///
    /// env.stream_par_iter(0..10)
    ///     .for_each(|q| println!("a: {q}"));
    ///
    /// let n = 10;
    /// env.stream_par_iter(
    ///     move |id, instances| {
    ///         let chunk_size = (n + instances - 1) / instances;
    ///         let remaining = n - n.min(chunk_size * id);
    ///         let range = remaining.min(chunk_size);
    ///         
    ///         let start = id * chunk_size;
    ///         let stop = id * chunk_size + range;
    ///         start..stop
    ///     })
    ///    .for_each(|q| println!("b: {q}"));
    ///
    /// env.execute();
    /// ```
    pub fn stream_par_iter<Source>(
        &mut self,
        generator: Source,
    ) -> Stream<Source::Item, ParallelIteratorSource<Source>>
    where
        Source: IntoParallelSource + 'static,
        Source::Iter: Send,
        Source::Item: Data,
    {
        let source = ParallelIteratorSource::new(generator);
        self.stream(source)
    }
}
