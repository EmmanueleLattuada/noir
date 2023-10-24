use std::collections::{HashMap, VecDeque};
use std::fmt::Display;

use crate::block::{
    BatchMode, Batcher, BlockStructure, Connection, NextStrategy, OperatorStructure,
};
use crate::network::{ReceiverEndpoint, OperatorCoord};
use crate::operator::{ExchangeData, KeyerFn, Operator, StreamElement};
use crate::scheduler::{BlockId, ExecutionMetadata, OperatorId};

/// The list with the interesting senders of a single block.
#[derive(Debug, Clone)]
pub(crate) struct BlockSenders {
    /// Indexes of the senders for all the replicas of this box
    pub indexes: Vec<usize>,
}

impl BlockSenders {
    pub(crate) fn new(indexes: Vec<usize>) -> Self {
        Self { indexes }
    }
}

#[derive(Derivative)]
#[derivative(Clone, Debug)]
pub struct End<Out: ExchangeData, OperatorChain, IndexFn>
where
    IndexFn: KeyerFn<u64, Out>,
    OperatorChain: Operator<Out>,
{
    prev: OperatorChain,
    operator_coord: OperatorCoord,
    next_strategy: NextStrategy<Out, IndexFn>,
    batch_mode: BatchMode,
    block_senders: Vec<BlockSenders>,
    #[derivative(Debug = "ignore", Clone(clone_with = "clone_default"))]
    senders: Vec<(ReceiverEndpoint, Batcher<Out>)>,
    feedback_id: Option<BlockId>,
    ignore_block_ids: Vec<BlockId>,
    feedback_pending_snapshots: VecDeque<StreamElement<Out>>,
}

impl<Out: ExchangeData, OperatorChain, IndexFn> Display for End<Out, OperatorChain, IndexFn>
where
    IndexFn: KeyerFn<u64, Out>,
    OperatorChain: Operator<Out>,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self.next_strategy {
            NextStrategy::Random => write!(f, "{} -> Shuffle", self.prev),
            NextStrategy::OnlyOne => write!(f, "{} -> OnlyOne", self.prev),
            _ => self.prev.fmt(f),
        }
    }
}

impl<Out: ExchangeData, OperatorChain, IndexFn> End<Out, OperatorChain, IndexFn>
where
    IndexFn: KeyerFn<u64, Out>,
    OperatorChain: Operator<Out>,
{
    pub(crate) fn new(
        prev: OperatorChain,
        next_strategy: NextStrategy<Out, IndexFn>,
        batch_mode: BatchMode,
    ) -> Self {
        let op_id = prev.get_op_id() + 1;
        Self {
            prev,
            // This will be set in setup method
            operator_coord: OperatorCoord::new(0,0,0,op_id),
            next_strategy,
            batch_mode,
            block_senders: Default::default(),
            senders: Default::default(),
            feedback_id: None,
            ignore_block_ids: Default::default(),
            feedback_pending_snapshots: VecDeque::default(),
        }
    }

    // group the senders based on the strategy
    fn setup_senders(&mut self) -> usize {
        glidesort::sort_by_key(&mut self.senders, |s| s.0);

        let mut groupbyreplica_index = 0;

        self.block_senders = match self.next_strategy {
            NextStrategy::All => (0..self.senders.len())
                .map(|i| vec![i])
                .map(BlockSenders::new)
                .collect(),
            _ => self
                .senders
                .iter()
                .enumerate()
                .fold(HashMap::<_, Vec<_>>::new(), |mut map, (i, (coord, _))| {
                    if coord.coord.host_id == self.operator_coord.host_id &&
                        coord.coord.replica_id == self.operator_coord.replica_id {
                            groupbyreplica_index = i;
                        }
                    map.entry(coord.coord.block_id).or_default().push(i);
                    map
                })
                .into_values()
                .map(BlockSenders::new)
                .collect(),
        };

        if matches!(self.next_strategy, NextStrategy::OnlyOne) {
            self.block_senders
                .iter()
                .for_each(|s| assert_eq!(s.indexes.len(), 1));
        }
        groupbyreplica_index
    }

    /// Mark this `End` as the end of a feedback loop.
    ///
    /// This will avoid this block from sending `Terminate` in the feedback loop, the destination
    /// should be already gone.
    pub(crate) fn mark_feedback(&mut self, block_id: BlockId) {
        self.feedback_id = Some(block_id);
    }

    pub(crate) fn ignore_destination(&mut self, block_id: BlockId) {
        self.ignore_block_ids.push(block_id);
    }
}

impl<Out: ExchangeData, OperatorChain, IndexFn> Operator<()> for End<Out, OperatorChain, IndexFn>
where
    IndexFn: KeyerFn<u64, Out>,
    OperatorChain: Operator<Out>,
{
    fn setup(&mut self, metadata: &mut ExecutionMetadata) {
        self.prev.setup(metadata);
        self.operator_coord.setup_coord(metadata.coord);

        // TODO: wrap sender-block assignment logic in a struct
        let senders = metadata.network.get_senders(metadata.coord);
        // remove the ignored destinations
        self.senders = senders
            .into_iter()
            .filter(|(endpoint, _)| !self.ignore_block_ids.contains(&endpoint.coord.block_id))
            .map(|(coord, sender)| (coord, Batcher::new(sender, self.batch_mode, metadata.coord)))
            .collect();

        let groupbyreplica_index = self.setup_senders();

        
        // if strategy is GroupByReplica set the right replica id
        if matches!(self.next_strategy, NextStrategy::GroupByReplica(_)) {
            self.next_strategy.set_replica(groupbyreplica_index);
        }

    }

    fn next(&mut self) -> StreamElement<()> {
        let message = self.prev.next();
        let to_return = message.take();
        match &message {
            // Broadcast messages
            StreamElement::Watermark(_)
            | StreamElement::Snapshot(_)
            | StreamElement::Terminate
            | StreamElement::FlushAndRestart => {
                for block in self.block_senders.iter() {
                    for &sender_idx in block.indexes.iter() {
                        let sender = &mut self.senders[sender_idx];

                        // if this block is the end of the feedback loop it should not forward
                        // `Terminate` since the destination is before us in the termination chain,
                        // and therefore has already left.
                        // same thing may happen with snapshots
                        if Some(sender.0.coord.block_id) == self.feedback_id {
                            match &message {
                                StreamElement::Terminate => continue,
                                StreamElement::Snapshot(_) => {
                                    self.feedback_pending_snapshots.push_front(message.clone());
                                    continue
                                },
                                _ => {
                                    // flush feedback pending snapshots
                                    while !self.feedback_pending_snapshots.is_empty() {
                                        sender.1.enqueue(self.feedback_pending_snapshots.pop_back().unwrap());
                                    }
                                }
                            }
                        }
                        sender.1.enqueue(message.clone());
                    }
                }
            }
            // Direct messages
            StreamElement::Item(item) | StreamElement::Timestamped(item, _) => {
                let index = self.next_strategy.index(item);
                for block in self.block_senders.iter() {
                    let index = index % block.indexes.len();
                    let sender_idx = block.indexes[index];
                    if Some(self.senders[sender_idx].0.coord.block_id) == self.feedback_id {
                        // flush feedback pending snapshots
                        while !self.feedback_pending_snapshots.is_empty() {
                            self.senders[sender_idx].1.enqueue(self.feedback_pending_snapshots.pop_back().unwrap());
                        }
                    }
                    if matches!(self.next_strategy, NextStrategy::GroupByReplica(_)) {
                        assert_eq!(self.operator_coord.host_id, self.senders[sender_idx].0.coord.host_id);
                        assert_eq!(self.operator_coord.replica_id, self.senders[sender_idx].0.coord.replica_id)
                    }
                    self.senders[sender_idx].1.enqueue(message.clone());
                }
            }
            StreamElement::FlushBatch => {}
        };

        // Flushing messages
        match to_return {
            StreamElement::FlushAndRestart | StreamElement::FlushBatch => {
                for (_, batcher) in self.senders.iter_mut() {
                    batcher.flush();
                }
            }
            StreamElement::Terminate => {
                log::debug!(
                    "{} received terminate, closing {} channels",
                    self.operator_coord.get_coord(),
                    self.senders.len()
                );
                for (_, batcher) in self.senders.drain(..) {
                    batcher.end();
                }
            }
            _ => {}
        }

        to_return
    }

    fn structure(&self) -> BlockStructure {
        let mut operator = OperatorStructure::new::<Out, _>("End");
        let op_id = self.operator_coord.operator_id;
        operator.subtitle = format!("op id: {op_id}");
        for sender_group in &self.block_senders {
            if !sender_group.indexes.is_empty() {
                let block_id = self.senders[sender_group.indexes[0]].0.coord.block_id;
                operator
                    .connections
                    .push(Connection::new::<Out, _>(block_id, &self.next_strategy));
            }
        }
        self.prev.structure().add_operator(operator)
    }

    fn get_op_id(&self) -> OperatorId {
        self.operator_coord.operator_id
    }

    fn get_stateful_operators(&self) -> Vec<OperatorId> {
        // This operator is stateless
        self.prev.get_stateful_operators()
    }
}

fn clone_default<T>(_: &T) -> T
where
    T: Default,
{
    T::default()
}
