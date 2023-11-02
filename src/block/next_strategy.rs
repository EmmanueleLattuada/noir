use std::hash::Hash;
use std::marker::PhantomData;

use nanorand::{tls_rng, Rng};

use crate::operator::{ExchangeData, KeyerFn};

use super::group_by_hash;

/// The next strategy used at the end of a block.
///
/// A block in the job graph may have many next blocks. Each of them will receive the message, which
/// of their replica will receive it depends on the value of the next strategy.
#[derive(Clone, Debug)]
pub(crate) enum NextStrategy<Out: ExchangeData, IndexFn = fn(&Out) -> u64>
where
    IndexFn: KeyerFn<u64, Out>,
{
    /// Only one of the replicas will receive the message:
    ///
    /// - if the block is not replicated, the only replica will receive the message
    /// - if the next block is replicated as much as the current block the corresponding replica
    ///   will receive the message
    /// - otherwise the execution graph is malformed  
    OnlyOne,
    /// A random replica will receive the message.
    Random,
    /// Among the next replica, the one is selected based on the hash of the key of the message.
    GroupBy(IndexFn, PhantomData<Out>),
    /// Like previous one but the key is fixed and is the replica index
    /// Used to allign snapshotId before iterations
    #[cfg(feature = "persist-state")]
    GroupByReplica(usize),
    /// Every following replica will receive every message.
    All,
}

impl<Out: ExchangeData> NextStrategy<Out> {
    /// Build a `NextStrategy` from a keyer function.
    pub(crate) fn group_by<Key: Hash, Keyer>(
        keyer: Keyer,
    ) -> NextStrategy<Out, impl KeyerFn<u64, Out>>
    where
        Keyer: KeyerFn<Key, Out>,
    {
        NextStrategy::GroupBy(
            move |item: &Out| group_by_hash(&keyer(item)),
            Default::default(),
        )
    }

    /// Returns `NextStrategy::All` with default `IndexFn`.
    pub(crate) fn all() -> NextStrategy<Out> {
        NextStrategy::All
    }

    /// Returns `NextStrategy::GroupByReplica`.
    #[cfg(feature = "persist-state")]
    pub(crate) fn group_by_replica(replica: usize) -> NextStrategy<Out> {
        NextStrategy::GroupByReplica(replica)
    }

    /// Returns `NextStrategy::OnlyOne` with default `IndexFn`.
    pub(crate) fn only_one() -> NextStrategy<Out> {
        NextStrategy::OnlyOne
    }

    /// Returns `NextStrategy::Random` with default `IndexFn`.
    pub(crate) fn random() -> NextStrategy<Out> {
        NextStrategy::Random
    }
}

impl<Out: ExchangeData, IndexFn> NextStrategy<Out, IndexFn>
where
    IndexFn: KeyerFn<u64, Out>,
{
    /// Compute the index of the replica which this message should be forwarded to.
    pub fn index(&self, message: &Out) -> usize {
        match self {
            NextStrategy::OnlyOne | NextStrategy::All => 0,
            NextStrategy::Random => tls_rng().generate(),
            NextStrategy::GroupBy(keyer, _) => keyer(message) as usize,
            #[cfg(feature = "persist-state")]
            NextStrategy::GroupByReplica(replica) => *replica,
        }
    }

    #[cfg(feature = "persist-state")]
    pub(crate) fn set_replica(&mut self, new_replica: usize) {
        match self {
            NextStrategy::GroupByReplica(replica) => {
                *replica = new_replica;
            },
            _ => {
                panic!("set_replica can be used only on GroupByReplica strategy")
            }
        }
    }
}
