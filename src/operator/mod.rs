//! Operators that can be applied to a stream.
//!
//! The actual operator list can be found from the implemented methods of [`Stream`](crate::Stream),
//! [`KeyedStream`](crate::KeyedStream), [`WindowedStream`](crate::WindowedStream) and
//! [`KeyedWindowedStream`](crate::KeyedWindowedStream).

use std::cmp::Ordering;
use std::fmt::{Display, self};
use std::hash::Hash;
use std::ops::{Add, Sub};

use serde::{Deserialize, Serialize};

pub(crate) use start::*;

pub use rich_map_custom::ElementGenerator;

use crate::block::BlockStructure;
use crate::scheduler::{ExecutionMetadata, OperatorId};
use crate::stream::KeyValue;

#[cfg(feature = "timestamp")]
pub(crate) mod add_timestamps;
pub(crate) mod aggregators;
pub(crate) mod batch_mode;
pub(crate) mod broadcast;
pub(crate) mod end;
pub(crate) mod filter;
pub(crate) mod filter_map;
pub(crate) mod flat_map;
pub(crate) mod flatten;
pub(crate) mod fold;
pub(crate) mod group_by;
pub(crate) mod inspect;
#[cfg(feature = "timestamp")]
pub(crate) mod interval_join;
pub(crate) mod iteration;
pub mod join;
pub(crate) mod key_by;
pub(crate) mod keyed_fold;
pub(crate) mod keyed_reduce;
pub(crate) mod map;
pub(crate) mod max_parallelism;
pub(crate) mod merge;
pub(crate) mod reduce;
pub(crate) mod reorder;
pub(crate) mod rich_filter_map;
pub(crate) mod rich_flat_map;
pub(crate) mod rich_map;
pub(crate) mod rich_map_persistent;
pub(crate) mod rich_map_custom;
pub(crate) mod route;
pub(crate) mod shuffle;
pub mod sink;
pub mod source;
pub(crate) mod split;
pub(crate) mod start;
pub(crate) mod unkey;
pub mod window;
pub(crate) mod zip;

/// Marker trait that all the types inside a stream should implement.
pub trait Data: Clone + Send + 'static {}
impl<T: Clone + Send + 'static> Data for T {}

/// Marker trait for data types that are used to communicate between different blocks.
pub trait ExchangeData: Data + Serialize + for<'a> Deserialize<'a> {}
impl<T: Data + Serialize + for<'a> Deserialize<'a> + 'static> ExchangeData for T {}

/// Marker trait that all the keys should implement.
pub trait DataKey: Data + Hash + Eq {}
impl<T: Data + Hash + Eq> DataKey for T {}

/// Marker trait for key types that are used when communicating between different blocks.
pub trait ExchangeDataKey: DataKey + ExchangeData {}
impl<T: DataKey + ExchangeData> ExchangeDataKey for T {}

/// Marker trait for the function that extracts the key out of a type.
pub trait KeyerFn<Key, Out>: Fn(&Out) -> Key + Clone + Send + 'static {}
impl<Key, Out, T: Fn(&Out) -> Key + Clone + Send + 'static> KeyerFn<Key, Out> for T {}

/// When using timestamps and watermarks, this type expresses the timestamp of a message or of a
/// watermark.
#[cfg(feature = "timestamp")]
pub type Timestamp = i64;

#[cfg(not(feature = "timestamp"))]
pub type Timestamp = ();

/// Identifier of the snapshot
#[derive(Clone, Copy, Debug, Hash, Eq, PartialEq, PartialOrd, Serialize, Deserialize)]
pub struct SnapshotId {
    snapshot_id: u64,
    terminate: bool,
}
impl SnapshotId {
    pub (crate) fn new(snapshot_id: u64) -> Self {
        Self{
            snapshot_id,
            terminate: false,
        }
    }
    pub (crate) fn new_terminate(snapshot_id: u64) -> Self {
        Self{
            snapshot_id,
            terminate: true,
        }
    }
    pub (crate) fn id(&self) -> u64 {
        self.snapshot_id
    }

    pub (crate) fn terminate(&self) -> bool {
        self.terminate
    }
}
impl fmt::Display for SnapshotId {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        if self.terminate {
            write!(f, "{}+", self.snapshot_id)
        } else {
            write!(f, "{}", self.snapshot_id)
        }
    }
}
impl Ord for SnapshotId {
    fn cmp(&self, other: &Self) -> Ordering {
        self.snapshot_id.cmp(&other.snapshot_id)
    }
}
impl Add<u64> for SnapshotId {
    type Output = Self;
    fn add(self, other: u64) -> Self {
        Self {
            snapshot_id: self.snapshot_id + other,
            terminate: self.terminate,
        }
    }
}
impl Sub<u64> for SnapshotId {
    type Output = Self;
    fn sub(self, other: u64) -> Self {
        Self {
            snapshot_id: self.snapshot_id - other,
            terminate: self.terminate,
        }
    }
}

/// An element of the stream. This is what enters and exits from the operators.
///
/// An operator may need to change the content of a `StreamElement` (e.g. a `Map` may change the
/// value of the `Item`). Usually `Watermark` and `FlushAndRestart` are simply forwarded to the next
/// operator in the chain.
///
/// In general a stream may be composed of a sequence of this kind:
///
/// `((Item | Timestamped | Watermark | FlushBatch)* FlushAndRestart)+ Terminate`
#[derive(Debug, Clone, Serialize, Deserialize, Eq, PartialEq, Ord, PartialOrd)]
pub enum StreamElement<Out> {
    /// A normal element containing just the value of the message.
    Item(Out),
    /// Like `Item`, but it's attached with a timestamp, it's used to ensure the ordering of the
    /// messages.
    Timestamped(Out, Timestamp),
    /// When an operator receives a `Watermark` with timestamp `t`, the operator will never see any
    /// message with timestamp less or equal to `t`.
    Watermark(Timestamp),
    /// Flush the internal batch since there will be too much delay till the next message to come.
    FlushBatch,
    /// The stream has ended, and the operators should exit as soon as possible.
    ///
    /// No messages should be generated by the operator between a `FlushAndRestart` and a
    /// `Terminate`.
    Terminate,
    /// Mark the end of a stream of data.
    ///
    /// Note that this does not mean that the entire stream has ended, for example this is used to
    /// mark the end of an iteration. Therefore an operator may be prepared to received new data
    /// after this message, but should not retain the internal state.
    FlushAndRestart,

    /// Marker used to do the snapshot for saving the state
    Snapshot(SnapshotId),
}

/// An operator represents a unit of computation. It's always included inside a chain of operators,
/// inside a block.
///
/// Each operator implements the `Operator<Out>` trait, it produced a stream of `Out` elements.
///
/// An `Operator` must be Clone since it is part of a single chain when it's built, but it has to
/// be cloned to spawn the replicas of the block.
pub trait Operator<Out: Data>: Clone + Send + Display {
    /// Setup the operator chain. This is called before any call to `next` and it's used to
    /// initialize the operator. When it's called the operator has already been cloned and it will
    /// never be cloned again. Therefore it's safe to store replica-specific metadata inside of it.
    ///
    /// It's important that each operator (except the start of a chain) calls `.setup()` recursively
    /// on the previous operators.
    fn setup(&mut self, metadata: &mut ExecutionMetadata);

    /// Take a value from the previous operator, process it and return it.
    fn next(&mut self) -> StreamElement<Out>;

    /// A more refined representation of the operator and its predecessors.
    fn structure(&self) -> BlockStructure;

    /// Return the operator id: this identifies the operator inside the chain
    fn get_op_id(&self) -> OperatorId;
}

impl<Out> StreamElement<Out> {
    /// Create a new `StreamElement` with an `Item(())` if `self` contains an item, otherwise it
    /// returns the same variant of `self`.
    pub fn take(&self) -> StreamElement<()> {
        match self {
            StreamElement::Item(_) => StreamElement::Item(()),
            StreamElement::Timestamped(_, _) => StreamElement::Item(()),
            StreamElement::Watermark(w) => StreamElement::Watermark(*w),
            StreamElement::Terminate => StreamElement::Terminate,
            StreamElement::FlushAndRestart => StreamElement::FlushAndRestart,
            StreamElement::FlushBatch => StreamElement::FlushBatch,
            StreamElement::Snapshot(s) => StreamElement::Snapshot(*s),
        }
    }

    /// Change the type of the element inside the `StreamElement`.
    pub fn map<NewOut>(self, f: impl FnOnce(Out) -> NewOut) -> StreamElement<NewOut> {
        match self {
            StreamElement::Item(item) => StreamElement::Item(f(item)),
            StreamElement::Timestamped(item, ts) => StreamElement::Timestamped(f(item), ts),
            StreamElement::Watermark(w) => StreamElement::Watermark(w),
            StreamElement::Terminate => StreamElement::Terminate,
            StreamElement::FlushAndRestart => StreamElement::FlushAndRestart,
            StreamElement::FlushBatch => StreamElement::FlushBatch,
            StreamElement::Snapshot(s) => StreamElement::Snapshot(s),
        }
    }

    /// A string representation of the variant of this `StreamElement`.
    pub fn variant(&self) -> &'static str {
        match self {
            StreamElement::Item(_) => "Item",
            StreamElement::Timestamped(_, _) => "Timestamped",
            StreamElement::Watermark(_) => "Watermark",
            StreamElement::FlushBatch => "FlushBatch",
            StreamElement::Terminate => "Terminate",
            StreamElement::FlushAndRestart => "FlushAndRestart",
            StreamElement::Snapshot(_) => "Snapshot",
        }
    }

    /// A string representation of the variant of this `StreamElement`.
    pub fn timestamp(&self) -> Option<&Timestamp> {
        match self {
            StreamElement::Timestamped(_, ts) | StreamElement::Watermark(ts) => Some(ts),
            _ => None,
        }
    }

    pub fn add_key<Key>(self, k: Key) -> StreamElement<KeyValue<Key, Out>> {
        match self {
            StreamElement::Item(v) => StreamElement::Item((k, v)),
            StreamElement::Timestamped(v, ts) => StreamElement::Timestamped((k, v), ts),
            StreamElement::Watermark(w) => StreamElement::Watermark(w),
            StreamElement::Terminate => StreamElement::Terminate,
            StreamElement::FlushAndRestart => StreamElement::FlushAndRestart,
            StreamElement::FlushBatch => StreamElement::FlushBatch,
            StreamElement::Snapshot(s) => StreamElement::Snapshot(s),
        }
    }
}

impl<Key, Out> StreamElement<KeyValue<Key, Out>> {
    /// Map a `StreamElement<KeyValue(Key, Out)>` to a `StreamElement<Out>`,
    /// returning the key if possible
    pub fn take_key(self) -> (Option<Key>, StreamElement<Out>) {
        match self {
            StreamElement::Item((k, v)) => (Some(k), StreamElement::Item(v)),
            StreamElement::Timestamped((k, v), ts) => (Some(k), StreamElement::Timestamped(v, ts)),
            StreamElement::Watermark(w) => (None, StreamElement::Watermark(w)),
            StreamElement::Terminate => (None, StreamElement::Terminate),
            StreamElement::FlushAndRestart => (None, StreamElement::FlushAndRestart),
            StreamElement::FlushBatch => (None, StreamElement::FlushBatch),
            StreamElement::Snapshot(s) => (None, StreamElement::Snapshot(s)),
        }
    }

    pub fn key(self) -> Option<Key> {
        match self {
            StreamElement::Item((k, _)) => Some(k),
            StreamElement::Timestamped((k, _), _) => Some(k),
            StreamElement::Watermark(_) => None,
            StreamElement::Terminate => None,
            StreamElement::FlushAndRestart => None,
            StreamElement::FlushBatch => None,
            StreamElement::Snapshot(_) => None,
        }
    }
}
