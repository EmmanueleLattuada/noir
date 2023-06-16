use std::collections::{HashSet, HashMap, VecDeque};
use std::fmt::{Debug, Display};
use std::sync::Arc;
use std::time::Duration;

pub(crate) use binary::*;
pub(crate) use simple::*;
use serde::{Serialize, Deserialize};

use super::SnapshotId;
#[cfg(feature = "timestamp")]
use super::Timestamp;
use crate::block::{BlockStructure, Replication};
use crate::channel::RecvTimeoutError;
use crate::network::{Coord, NetworkDataIterator, NetworkMessage, OperatorCoord};
use crate::operator::iteration::IterationStateLock;
use crate::operator::source::Source;
use crate::operator::start::watermark_frontier::WatermarkFrontier;
use crate::operator::{ExchangeData, Operator, StreamElement};
use crate::persistency::{PersistencyService, PersistencyServices};
use crate::scheduler::{BlockId, ExecutionMetadata, OperatorId};

mod binary;
mod simple;
mod watermark_frontier;

/// Trait that abstract the receiving part of the `Start`.
pub(crate) trait StartReceiver<Out>: Clone {
    /// Setup the internal state of the receiver.
    fn setup(&mut self, metadata: &mut ExecutionMetadata);

    /// Obtain the list of all the previous replicas.
    ///
    /// This list should contain all the replicas this receiver will receive data from.
    fn prev_replicas(&self) -> Vec<Coord>;

    /// The number of those replicas which are behind a cache, and therefore never will emit a
    /// `StreamElement::Terminate` message.
    fn cached_replicas(&self) -> usize;

    /// Try to receive a batch from the previous blocks, or fail with an error if the timeout
    /// expires.
    fn recv_timeout(&mut self, timeout: Duration) -> Result<NetworkMessage<Out>, RecvTimeoutError>;

    /// Receive a batch from the previous blocks waiting indefinitely.
    fn recv(&mut self) -> NetworkMessage<Out>;

    /// Like `Operator::structure`.
    fn structure(&self) -> BlockStructure;

    /// Type of the state of the receiver
    type ReceiverState: ExchangeData + Debug;
    /// Get the state of the receiver
    fn get_state(&self) -> Option<Self::ReceiverState>;
    /// Set the state of the receiver
    fn set_state(&mut self, receiver_state: Option<Self::ReceiverState>);
}

pub(crate) type BinaryStartOperator<OutL, OutR> =
    Start<BinaryElement<OutL, OutR>, BinaryStartReceiver<OutL, OutR>>;

pub(crate) type SimpleStartOperator<Out> = Start<Out, SimpleStartReceiver<Out>>;

/// Each block should start with a `Start` operator, whose task is to read from the network,
/// receive from the previous operators and handle the watermark frontier.
///
/// There are different kinds of `Start`, the main difference is in the number of previous
/// blocks. With a `SimpleStartReceiver` the block is able to receive from the replicas of a
/// single block of the job graph. If the block needs the data from multiple blocks it should use
/// `MultipleStartReceiver` which is able to handle 2 previous blocks.
///
/// Following operators will receive the messages in an unspecified order but the watermark property
/// is followed. Note that the timestamps of the messages are not sorted, it's only guaranteed that
/// when a watermark is emitted, all the previous messages are already been emitted (in some order).
#[derive(Clone, Debug)]
pub(crate) struct Start<Out: ExchangeData, Receiver: StartReceiver<Out> + Send> {
    /// Execution metadata of this block.
    max_delay: Option<Duration>,

    coord: Option<Coord>,
    operator_coord: OperatorCoord,
    persistency_service: PersistencyService,
    // Used to keep the on-going snapshots
    on_going_snapshots: HashMap<SnapshotId, (StartState<Out, Receiver>, HashSet<Coord>)>,
    persisted_message_queue: VecDeque<(Coord, StreamElement<Out>)>,
    terminated_replicas: Vec<Coord>,
    last_snapshots: HashMap<Coord, SnapshotId>,

    /// The actual receiver able to fetch messages from the network.
    receiver: Receiver,

    /// Inner iterator over batch items, contains coordinate of the sender
    batch_iter: Option<(Coord, NetworkDataIterator<StreamElement<Out>>)>,

    /// The number of `StreamElement::Terminate` messages yet to be received. When this value
    /// reaches zero this operator will emit the terminate.
    missing_terminate: usize,
    /// The number of `StreamElement::FlushAndRestart` messages yet to be received.
    missing_flush_and_restart: usize,
    /// The total number of replicas in the previous blocks. This is used for resetting
    /// `missing_flush_and_restart`.
    num_previous_replicas: usize,

    /// Whether the previous blocks timed out and the last batch has been flushed.
    ///
    /// The next time `next()` is called it will not wait the timeout asked by the batch mode.
    already_timed_out: bool,

    /// The current frontier of the watermarks from the previous replicas.
    watermark_frontier: WatermarkFrontier,

    /// Whether the iteration has ended and the current block has to wait for the local iteration
    /// leader to update the iteration state before letting the messages pass.
    wait_for_state: bool,
    state_lock: Option<Arc<IterationStateLock>>,
    state_generation: usize,
}

impl<Out: ExchangeData, Receiver: StartReceiver<Out> + Send> Display for Start<Out, Receiver> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "[{}]", std::any::type_name::<Out>())
    }
}

impl<Out: ExchangeData> Start<Out, SimpleStartReceiver<Out>> {
    /// Create a `Start` able to receive data only from a single previous block.
    pub(crate) fn single(
        previous_block_id: BlockId,
        state_lock: Option<Arc<IterationStateLock>>,
    ) -> SimpleStartOperator<Out> {
        Start::new(SimpleStartReceiver::new(previous_block_id), state_lock)
    }
}

impl<OutL: ExchangeData, OutR: ExchangeData>
    Start<BinaryElement<OutL, OutR>, BinaryStartReceiver<OutL, OutR>>
{
    /// Create a `Start` able to receive data from 2 previous blocks, setting up the cache.
    pub(crate) fn multiple(
        previous_block_id1: BlockId,
        previous_block_id2: BlockId,
        left_cache: bool,
        right_cache: bool,
        state_lock: Option<Arc<IterationStateLock>>,
    ) -> BinaryStartOperator<OutL, OutR> {
        Start::new(
            BinaryStartReceiver::new(
                previous_block_id1,
                previous_block_id2,
                left_cache,
                right_cache,
            ),
            state_lock,
        )
    }
}

impl<Out: ExchangeData, Receiver: StartReceiver<Out> + Send + 'static> Start<Out, Receiver> {
    fn new(receiver: Receiver, state_lock: Option<Arc<IterationStateLock>>) -> Self {
        Self {
            coord: Default::default(),
            max_delay: Default::default(),

            // This is the first operator in the chain so operator_id is 0
            // Other fields will be set in setup method
            operator_coord: OperatorCoord::new(0, 0, 0, 0),
            persistency_service: PersistencyService::default(),
            on_going_snapshots: HashMap::new(),
            persisted_message_queue: VecDeque::new(),
            terminated_replicas: Vec::new(),
            last_snapshots: HashMap::new(),

            receiver,
            batch_iter: None,

            missing_terminate: Default::default(),
            missing_flush_and_restart: Default::default(),
            num_previous_replicas: 0,

            already_timed_out: Default::default(),

            watermark_frontier: Default::default(),

            wait_for_state: Default::default(),
            state_lock,
            state_generation: Default::default(),
        }
    }

    pub(crate) fn receiver(&self) -> &Receiver {
        &self.receiver
    }

    fn process_snapshot(&mut self, snap_id: SnapshotId, sender: Coord) -> Option<SnapshotId> {
        // Update last snapshots map
        self.last_snapshots.insert(sender, snap_id);
        // Check if is already arrived this snapshot id
        if self.on_going_snapshots.contains_key(&snap_id) {
            // Remove the sender from the set of previous replicas for this snapshot
            self.on_going_snapshots.get_mut(&snap_id).unwrap().1.remove(&sender);
            // Check if there are no more replicas
            if self.on_going_snapshots.get(&snap_id).unwrap().1.len() == 0 {
                // This snapshot is complete: now i can save it 
                let state = self.on_going_snapshots.remove(&snap_id).unwrap().0;
                self.persistency_service.save_state(self.operator_coord, snap_id, state);
            }
            // I've already forwarded the snapshot marker
            None
        } else {
            // Save current state, messages will be add then 
            let state: StartState<Out, Receiver>= StartState{
                missing_flush_and_restart: self.missing_flush_and_restart as u64,
                missing_terminate: self.missing_terminate as u64,
                wait_for_state: self.wait_for_state,
                state_generation: self.state_generation as u64,
                watermark_forntier: self.watermark_frontier.clone(),
                receiver_state: self.receiver.get_state(),
                terminated_replicas: self.terminated_replicas.clone(),
                message_queue: VecDeque::default(),
            };

            // Set all previous replicas that have to send this snapshot id
            let mut prev_replicas = HashSet::from_iter(self.receiver().prev_replicas());
            prev_replicas.remove(&sender);
            for replica in self.terminated_replicas.iter() {
                prev_replicas.remove(replica);
            }
            // Check if the snapshot is already complete
            if prev_replicas.len() == 0 {
                // This snapshot is complete: i can save it immediately 
                self.persistency_service.save_state(self.operator_coord, snap_id, state);
            } else {
                // Add a new entry in the map with this snapshot id
                self.on_going_snapshots.insert(snap_id, (state, prev_replicas));
            }
            // Forward the snapshot marker
            Some(snap_id)
        }
    }

    fn add_item_to_snapshot(&mut self, item: StreamElement<Out>, sender: Coord) {
        // Add item to message queue state 
        for entry in self.on_going_snapshots.iter_mut() {
            if entry.1.1.contains(&sender) {
                entry.1.0.message_queue.push_back((sender, item.clone()));
            }
        }
    }

    fn process_terminate(&mut self, sender: Coord) -> Option<SnapshotId> {
        // Add Terminate to on going snapshots as a regular item
        self.add_item_to_snapshot(StreamElement::Terminate, sender);
        // Sender terminated: it did implicitely a terminate snapshot, continue that snapshot
        let mut snapshot_id = self.last_snapshots.get(&sender)
            .unwrap_or(&SnapshotId::new(0))
            .clone();
        snapshot_id = snapshot_id + 1;
        let mut result: Option<SnapshotId> = None;
        // Add sender to terminated replicas
        self.terminated_replicas.push(sender);        
        // Check if is already arrived this snapshot id
        if self.on_going_snapshots.contains_key(&snapshot_id) {
            // Fix on going snapshot by remove sender form sets of partial snapshots with id >= snap_id
            let mut future_snap: Vec<SnapshotId> = self.on_going_snapshots
                .keys()
                .filter(|id| id.id() >= snapshot_id.id())
                .cloned()
                .collect();
            future_snap.sort();
            for partial_snapshot in future_snap {
                // Remove the sender from the set of previous replicas for this snapshot
                self.on_going_snapshots.get_mut(&partial_snapshot).unwrap().1.remove(&sender);
                // Check if there are no more replicas
                if self.on_going_snapshots.get(&partial_snapshot).unwrap().1.len() == 0 {
                    // This snapshot is complete: now i can save it 
                    let state = self.on_going_snapshots.remove(&partial_snapshot).unwrap().0;
                    self.persistency_service.save_state(self.operator_coord, partial_snapshot, state);
                }
            }
        } else {
            // Save current state, messages will be add then 
            let state: StartState<Out, Receiver>= StartState{
                missing_flush_and_restart: self.missing_flush_and_restart as u64,
                missing_terminate: self.missing_terminate as u64,
                wait_for_state: self.wait_for_state,
                state_generation: self.state_generation as u64,
                watermark_forntier: self.watermark_frontier.clone(),
                receiver_state: self.receiver.get_state(),
                terminated_replicas: self.terminated_replicas.clone(),
                message_queue: VecDeque::default(),
            };

            // Set all previous replicas that have to send this snapshot id
            let mut prev_replicas = HashSet::from_iter(self.receiver().prev_replicas());
            prev_replicas.remove(&sender);
            for replica in self.terminated_replicas.iter() {
                prev_replicas.remove(replica);
            }
            // Check if the snapshot is already complete
            if prev_replicas.len() == 0 {
                // This snapshot is complete: i can save it immediately 
                self.persistency_service.save_state(self.operator_coord, snapshot_id, state);
            } else {
                // Add a new entry in the map with this snapshot id
                self.on_going_snapshots.insert(snapshot_id, (state, prev_replicas));
            }
            // Forward the snapshot marker
            result = Some(snapshot_id);
        }
        result
    }

}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct StartState<Out, Receiver: StartReceiver<Out>> {
    missing_terminate: u64,
    missing_flush_and_restart: u64,

    wait_for_state: bool,
    state_generation: u64,

    watermark_forntier: WatermarkFrontier,
    #[serde(bound(
        serialize = "Receiver::ReceiverState: Serialize",
        deserialize = "Receiver::ReceiverState: Deserialize<'de>",
    ))]
    receiver_state: Option<<Receiver as StartReceiver<Out>>::ReceiverState>,

    terminated_replicas: Vec<Coord>,

    message_queue: VecDeque<(Coord, StreamElement<Out>)>,
}

impl<Out: ExchangeData, Receiver: StartReceiver<Out> + Send + 'static> Operator<Out>
    for Start<Out, Receiver>
{
    fn setup(&mut self, metadata: &mut ExecutionMetadata) {
        self.receiver.setup(metadata);

        let prev_replicas = self.receiver.prev_replicas();
        self.num_previous_replicas = prev_replicas.len();
        self.missing_terminate = self.num_previous_replicas;
        self.missing_flush_and_restart = self.num_previous_replicas;
        self.watermark_frontier = WatermarkFrontier::new(prev_replicas);

        log::trace!(
            "{} initialized <{}>",
            metadata.coord,
            std::any::type_name::<Out>()
        );
        self.coord = Some(metadata.coord);
        self.max_delay = metadata.batch_mode.max_delay();

        self.operator_coord.block_id = metadata.coord.block_id;
        self.operator_coord.host_id = metadata.coord.host_id;
        self.operator_coord.replica_id = metadata.coord.replica_id;

        self.persistency_service = metadata.persistency_service.clone();
        self.persistency_service.setup();
        let snapshot_id = self.persistency_service.restart_from_snapshot(self.operator_coord);
        if let Some(snap_id) = snapshot_id {
            // Get and resume the persisted state
            let opt_state: Option<StartState<Out, Receiver>> = self.persistency_service.get_state(self.operator_coord, snap_id);
            if let Some(state) = opt_state {
                self.missing_terminate = state.missing_terminate as usize;
                self.missing_flush_and_restart = state.missing_flush_and_restart as usize;
                self.wait_for_state = state.wait_for_state;
                self.state_generation = state.state_generation as usize;
                self.watermark_frontier = state.watermark_forntier;
                self.receiver.set_state(state.receiver_state);
                self.terminated_replicas = state.terminated_replicas;
                self.persisted_message_queue = state.message_queue;  
                // Set last snapshots off all previous replicas to this snapshot_id
                for prev_rep in self.receiver.prev_replicas() {
                    self.last_snapshots.insert(prev_rep, snap_id);
                }             
                
            } else {
                panic!("No persisted state founded for op: {0}", self.operator_coord);
            } 
        }
    }

    fn next(&mut self) -> StreamElement<Out> {
        let coord = self.coord.unwrap();

        loop {
            // all the previous blocks sent an end: we're done
            if self.missing_terminate == 0 {
                // If persistency is on, save terminate state
                if self.persistency_service.is_active() {
                    let state: StartState<Out, Receiver>= StartState{
                        missing_flush_and_restart: self.missing_flush_and_restart as u64,
                        missing_terminate: self.missing_terminate as u64,
                        wait_for_state: self.wait_for_state,
                        state_generation: self.state_generation as u64,
                        watermark_forntier: self.watermark_frontier.clone(),
                        receiver_state: self.receiver.get_state(),
                        terminated_replicas: self.terminated_replicas.clone(),
                        // This is empty since all previous replicas terminated, so no more messages will arrive
                        message_queue: VecDeque::default(),
                    };
                    self.persistency_service.save_terminated_state(self.operator_coord, state)
                }
                log::trace!("{} ended", coord);
                return StreamElement::Terminate;
            }
            if self.missing_flush_and_restart == 0 {
                log::trace!("{} flush_restart", coord);

                self.missing_flush_and_restart = self.num_previous_replicas;
                self.watermark_frontier.reset();
                // this iteration has ended, before starting the next one wait for the state update
                self.wait_for_state = true;
                self.state_generation += 2;
                return StreamElement::FlushAndRestart;
            }

            // Process persisted message queue, if any
            if let Some((sender, item)) = self.persisted_message_queue.pop_front() {
                let to_return = match item {
                    StreamElement::Watermark(ts) => {
                        // update the frontier and return a watermark if necessary
                        match self.watermark_frontier.update(sender, ts) {
                            Some(ts) => StreamElement::Watermark(ts), // ts is safe
                            None => continue,
                        }
                    }
                    StreamElement::FlushAndRestart => {
                        // mark this replica as ended and let the frontier ignore it from now on
                        #[cfg(feature = "timestamp")]
                        {
                            self.watermark_frontier.update(sender, Timestamp::MAX);
                        }
                        self.missing_flush_and_restart -= 1;
                        continue;
                    }
                    StreamElement::Terminate => {
                        self.missing_terminate -= 1;
                        log::debug!(
                            "{} received a Terminate, {} more to come",
                            coord,
                            self.missing_terminate
                        );
                        continue;
                    }
                    _ => {
                        item
                    },
                };
                return to_return;
            }


            if let Some((sender, ref mut inner)) = self.batch_iter {
                let msg = match inner.next() {
                    None => {
                        // Current batch is finished
                        self.batch_iter = None;
                        continue;
                    }
                    Some(item) => {
                        match item {
                            StreamElement::Watermark(ts) => {
                                self.add_item_to_snapshot(item, sender);
                                // update the frontier and return a watermark if necessary
                                match self.watermark_frontier.update(sender, ts) {
                                    Some(ts) => StreamElement::Watermark(ts), // ts is safe
                                    None => continue,
                                }
                            }
                            StreamElement::FlushAndRestart => {
                                self.add_item_to_snapshot(item, sender);
                                // mark this replica as ended and let the frontier ignore it from now on
                                #[cfg(feature = "timestamp")]
                                {
                                    self.watermark_frontier.update(sender, Timestamp::MAX);
                                }
                                self.missing_flush_and_restart -= 1;
                                continue;
                            }
                            StreamElement::Terminate => {
                                self.missing_terminate -= 1;
                                log::trace!(
                                    "{} received terminate, {} left",
                                    coord,
                                    self.missing_terminate
                                );
                                // If persistency is on, process_terminate. This may return a Snapshot marker 
                                if self.persistency_service.is_active() {
                                    let to_forward = self.process_terminate(sender);
                                    if to_forward.is_none() {
                                        continue;
                                    } else {
                                        StreamElement::Snapshot(to_forward.unwrap())
                                    }
                                } else {
                                    continue;
                                }
                            }
                            StreamElement::Snapshot(snapshot_id) => {
                                let to_forward = self.process_snapshot(snapshot_id, sender);
                                if to_forward.is_none() {
                                    continue;
                                }
                                item
                            }
                            _ => {
                                self.add_item_to_snapshot(item.clone(), sender);
                                item
                            },
                        }
                    }
                };

                // the previous iteration has ended, this message refers to the new iteration: we need to be
                // sure the state is set before we let this message pass
                if self.wait_for_state {
                    if let Some(lock) = self.state_lock.as_ref() {
                        lock.wait_for_update(self.state_generation);
                    }
                    self.wait_for_state = false;
                }
                return msg;
            }

            // Receive next batch
            let net_msg = match (self.already_timed_out, self.max_delay) {
                // check the timeout only if there is one and the last time we didn't timed out
                (false, Some(max_delay)) => {
                    match self.receiver.recv_timeout(max_delay) {
                        Ok(net_msg) => net_msg,
                        Err(_) => {
                            // timed out: tell the block to flush the current batch
                            // next time we wait indefinitely without the timeout since the batch is
                            // currently empty
                            self.already_timed_out = true;
                            // this is a fake batch, and its sender is meaningless and will be
                            // forget immediately
                            NetworkMessage::new_single(
                                StreamElement::FlushBatch,
                                Default::default(),
                            )
                        }
                    }
                }
                _ => {
                    self.already_timed_out = false;
                    self.receiver.recv()
                }
            };

            self.batch_iter = Some((net_msg.sender(), net_msg.into_iter()));
        }
    }

    fn structure(&self) -> BlockStructure {
        self.receiver.structure()
    }

    fn get_op_id(&self) -> OperatorId {
        self.operator_coord.operator_id
    }
}

impl<Out: ExchangeData, Receiver: StartReceiver<Out> + Send + 'static> Source<Out> for Start<Out, Receiver> {
    fn replication(&self) -> Replication {
        Replication::Unlimited
    }

    fn set_snapshot_frequency_by_item(&mut self, _item_interval: u64) {
        // Forbidden action
        panic!("It is not possible to set snapshot frequency for start operator");
    }

    fn set_snapshot_frequency_by_time(&mut self, _time_interval: Duration) {
        // Forbidden action
        panic!("It is not possible to set snapshot frequency for start operator");
    }
}

#[cfg(test)]
mod tests {
    use std::collections::VecDeque;

    use serial_test::serial;

    use crate::config::PersistencyConfig;
    use crate::network::NetworkMessage;
    use crate::operator::start::StartState;
    use crate::operator::start::watermark_frontier::WatermarkFrontier;
    use crate::operator::{BinaryElement, Operator, Start, StreamElement, Timestamp, SimpleStartReceiver, SnapshotId};
    use crate::persistency::{PersistencyService, PersistencyServices};
    use crate::test::{FakeNetworkTopology, REDIS_TEST_CONFIGURATION};

    #[cfg(feature = "timestamp")]
    fn ts(millis: u64) -> Timestamp {
        millis as i64
    }

    #[test]
    fn test_single() {
        let mut t = FakeNetworkTopology::new(1, 2);
        let (from1, sender1) = t.senders_mut()[0].pop().unwrap();
        let (from2, sender2) = t.senders_mut()[0].pop().unwrap();

        let mut start_block =
            Start::<i32, _>::single(sender1.receiver_endpoint.prev_block_id, None);
        start_block.setup(&mut t.metadata());

        sender1
            .send(NetworkMessage::new_batch(
                vec![StreamElement::Item(42), StreamElement::FlushAndRestart],
                from1,
            ))
            .unwrap();

        assert_eq!(StreamElement::Item(42), start_block.next());
        assert_eq!(StreamElement::FlushBatch, start_block.next());

        sender2
            .send(NetworkMessage::new_batch(
                vec![StreamElement::FlushAndRestart],
                from2,
            ))
            .unwrap();

        assert_eq!(StreamElement::FlushAndRestart, start_block.next());

        sender1
            .send(NetworkMessage::new_single(StreamElement::Terminate, from1))
            .unwrap();
        sender2
            .send(NetworkMessage::new_single(StreamElement::Terminate, from2))
            .unwrap();

        assert_eq!(StreamElement::Terminate, start_block.next());
    }

    #[test]
    #[cfg(feature = "timestamp")]
    fn test_single_watermark() {
        let mut t = FakeNetworkTopology::new(1, 2);
        let (from1, sender1) = t.senders_mut()[0].pop().unwrap();
        let (from2, sender2) = t.senders_mut()[0].pop().unwrap();

        let mut start_block =
            Start::<i32, _>::single(sender1.receiver_endpoint.prev_block_id, None);
        start_block.setup(&mut t.metadata());

        sender1
            .send(NetworkMessage::new_batch(
                vec![
                    StreamElement::Timestamped(42, ts(10)),
                    StreamElement::Watermark(ts(20)),
                ],
                from1,
            ))
            .unwrap();

        assert_eq!(StreamElement::Timestamped(42, ts(10)), start_block.next());
        assert_eq!(StreamElement::FlushBatch, start_block.next());

        sender2
            .send(NetworkMessage::new_batch(
                vec![StreamElement::Watermark(ts(100))],
                from2,
            ))
            .unwrap();

        assert_eq!(StreamElement::Watermark(ts(20)), start_block.next());
        assert_eq!(StreamElement::FlushBatch, start_block.next());

        sender1
            .send(NetworkMessage::new_batch(
                vec![StreamElement::FlushAndRestart],
                from1,
            ))
            .unwrap();
        sender2
            .send(NetworkMessage::new_batch(
                vec![StreamElement::Watermark(ts(110))],
                from2,
            ))
            .unwrap();
        assert_eq!(StreamElement::Watermark(ts(110)), start_block.next());
    }

    #[test]
    #[cfg(feature = "timestamp")]
    fn test_multiple_no_cache() {
        let mut t = FakeNetworkTopology::new(2, 1);
        let (from1, sender1) = t.senders_mut()[0].pop().unwrap();
        let (from2, sender2) = t.senders_mut()[1].pop().unwrap();

        let mut start_block = Start::<BinaryElement<i32, i32>, _>::multiple(
            from1.block_id,
            from2.block_id,
            false,
            false,
            None,
        );
        start_block.setup(&mut t.metadata());

        sender1
            .send(NetworkMessage::new_batch(
                vec![
                    StreamElement::Timestamped(42, ts(10)),
                    StreamElement::Watermark(ts(20)),
                ],
                from1,
            ))
            .unwrap();

        assert_eq!(
            StreamElement::Timestamped(BinaryElement::Left(42), ts(10)),
            start_block.next()
        );
        assert_eq!(StreamElement::FlushBatch, start_block.next());

        sender2
            .send(NetworkMessage::new_batch(
                vec![
                    StreamElement::Timestamped(69, ts(10)),
                    StreamElement::Watermark(ts(20)),
                ],
                from2,
            ))
            .unwrap();

        assert_eq!(
            StreamElement::Timestamped(BinaryElement::Right(69), ts(10)),
            start_block.next()
        );
        assert_eq!(StreamElement::Watermark(ts(20)), start_block.next());

        sender1
            .send(NetworkMessage::new_batch(
                vec![StreamElement::FlushAndRestart],
                from1,
            ))
            .unwrap();
        assert_eq!(
            StreamElement::Item(BinaryElement::LeftEnd),
            start_block.next()
        );
        sender2
            .send(NetworkMessage::new_batch(
                vec![StreamElement::FlushAndRestart],
                from2,
            ))
            .unwrap();
        assert_eq!(
            StreamElement::Item(BinaryElement::RightEnd),
            start_block.next()
        );
        assert_eq!(StreamElement::FlushAndRestart, start_block.next());
    }

    #[test]
    fn test_multiple_cache() {
        let mut t = FakeNetworkTopology::new(2, 1);
        let (from1, sender1) = t.senders_mut()[0].pop().unwrap();
        let (from2, sender2) = t.senders_mut()[1].pop().unwrap();

        let mut start_block = Start::<BinaryElement<i32, i32>, _>::multiple(
            from1.block_id,
            from2.block_id,
            true,
            false,
            None,
        );
        start_block.setup(&mut t.metadata());

        sender1
            .send(NetworkMessage::new_single(StreamElement::Item(42), from1))
            .unwrap();
        sender1
            .send(NetworkMessage::new_single(StreamElement::Item(43), from1))
            .unwrap();
        sender2
            .send(NetworkMessage::new_single(StreamElement::Item(69), from2))
            .unwrap();

        let mut recv = [start_block.next(), start_block.next(), start_block.next()];
        recv.sort(); // those messages can arrive unordered
        assert_eq!(StreamElement::Item(BinaryElement::Left(42)), recv[0]);
        assert_eq!(StreamElement::Item(BinaryElement::Left(43)), recv[1]);
        assert_eq!(StreamElement::Item(BinaryElement::Right(69)), recv[2]);

        sender1
            .send(NetworkMessage::new_batch(
                vec![StreamElement::FlushAndRestart, StreamElement::Terminate],
                from1,
            ))
            .unwrap();
        sender2
            .send(NetworkMessage::new_batch(
                vec![StreamElement::FlushAndRestart],
                from2,
            ))
            .unwrap();

        let mut recv = [start_block.next(), start_block.next()];
        recv.sort(); // those messages can arrive unordered
        assert_eq!(StreamElement::Item(BinaryElement::LeftEnd), recv[0]);
        assert_eq!(StreamElement::Item(BinaryElement::RightEnd), recv[1]);

        assert_eq!(StreamElement::FlushAndRestart, start_block.next());

        sender2
            .send(NetworkMessage::new_batch(
                vec![StreamElement::Item(6969), StreamElement::FlushAndRestart],
                from2,
            ))
            .unwrap();

        let mut recv = [
            start_block.next(),
            start_block.next(),
            start_block.next(),
            start_block.next(),
            start_block.next(),
        ];
        recv.sort(); // those messages can arrive unordered
        assert_eq!(StreamElement::Item(BinaryElement::Left(42)), recv[0]);
        assert_eq!(StreamElement::Item(BinaryElement::Left(43)), recv[1]);
        assert_eq!(StreamElement::Item(BinaryElement::Right(6969)), recv[2]);
        assert_eq!(StreamElement::Item(BinaryElement::LeftEnd), recv[3]);
        assert_eq!(StreamElement::Item(BinaryElement::RightEnd), recv[4]);

        assert_eq!(StreamElement::FlushAndRestart, start_block.next());

        sender2
            .send(NetworkMessage::new_single(StreamElement::Terminate, from2))
            .unwrap();

        assert_eq!(StreamElement::Terminate, start_block.next());
    }

    #[test]
    fn test_multiple_cache_other_side() {
        let mut t = FakeNetworkTopology::new(2, 1);
        let (from1, sender1) = t.senders_mut()[0].pop().unwrap();
        let (from2, sender2) = t.senders_mut()[1].pop().unwrap();

        let mut start_block = Start::<BinaryElement<i32, i32>, _>::multiple(
            from1.block_id,
            from2.block_id,
            false,
            true,
            None,
        );
        start_block.setup(&mut t.metadata());

        sender1
            .send(NetworkMessage::new_single(StreamElement::Item(42), from1))
            .unwrap();
        sender1
            .send(NetworkMessage::new_single(StreamElement::Item(43), from1))
            .unwrap();
        sender2
            .send(NetworkMessage::new_single(StreamElement::Item(69), from2))
            .unwrap();

        let mut recv = [start_block.next(), start_block.next(), start_block.next()];
        recv.sort(); // those messages can arrive unordered
        assert_eq!(StreamElement::Item(BinaryElement::Left(42)), recv[0]);
        assert_eq!(StreamElement::Item(BinaryElement::Left(43)), recv[1]);
        assert_eq!(StreamElement::Item(BinaryElement::Right(69)), recv[2]);

        sender1
            .send(NetworkMessage::new_batch(
                vec![StreamElement::FlushAndRestart, StreamElement::Terminate],
                from1,
            ))
            .unwrap();
        sender2
            .send(NetworkMessage::new_batch(
                vec![StreamElement::FlushAndRestart],
                from2,
            ))
            .unwrap();

        let mut recv = [start_block.next(), start_block.next()];
        recv.sort(); // those messages can arrive unordered
        assert_eq!(StreamElement::Item(BinaryElement::LeftEnd), recv[0]);
        assert_eq!(StreamElement::Item(BinaryElement::RightEnd), recv[1]);

        assert_eq!(StreamElement::FlushAndRestart, start_block.next());

        sender2
            .send(NetworkMessage::new_single(StreamElement::Terminate, from2))
            .unwrap();

        assert_eq!(StreamElement::Terminate, start_block.next());
    }

    #[test]
    #[serial]
    fn test_single_start_persistency() {
        let mut t = FakeNetworkTopology::new(1, 2);
        let (from1, sender1) = t.senders_mut()[0].pop().unwrap();
        let (from2, sender2) = t.senders_mut()[0].pop().unwrap();

        let mut start_block =
            Start::<i32, _>::single(sender1.receiver_endpoint.prev_block_id, None);

        let mut metadata = t.metadata();
        metadata.persistency_service = PersistencyService::new(Some(
            PersistencyConfig { 
                server_addr: String::from(REDIS_TEST_CONFIGURATION),
                try_restart: false,
                clean_on_exit: false,
                restart_from: None,
            }
        ));
        start_block.setup(&mut metadata);

        start_block.operator_coord.operator_id = 100;


        sender1
            .send(NetworkMessage::new_batch(
                vec![
                    StreamElement::Item(1), 
                    StreamElement::Snapshot(SnapshotId::new(1)),
                    StreamElement::Item(2), 
                    ],
                from1,
            ))
            .unwrap();

        assert_eq!(StreamElement::Item(1), start_block.next());
        assert_eq!(StreamElement::Snapshot(SnapshotId::new(1)), start_block.next());
        assert_eq!(StreamElement::Item(2), start_block.next());

        sender2
            .send(NetworkMessage::new_batch(
                vec![
                    StreamElement::Item(3), 
                    StreamElement::Item(4), 
                    StreamElement::Snapshot(SnapshotId::new(1)),
                    StreamElement::Item(5),
                    ],
                from2,
            ))
            .unwrap();

        assert_eq!(StreamElement::Item(3), start_block.next());
        assert_eq!(StreamElement::Item(4), start_block.next());
        assert_eq!(StreamElement::Item(5), start_block.next());

        let state: StartState<i32, SimpleStartReceiver<i32>> = StartState {
            missing_flush_and_restart: 2,
            missing_terminate: 2,
            wait_for_state: false,
            state_generation: 0,
            watermark_forntier: WatermarkFrontier::new(vec![from1, from2]),
            receiver_state: None,
            terminated_replicas: vec![],
            message_queue: VecDeque::from([
                (from2, StreamElement::Item(3)), 
                (from2, StreamElement::Item(4)),
            ]),
        };

        let retrived_state: StartState<i32, SimpleStartReceiver<i32>> = start_block.persistency_service.get_state(start_block.operator_coord, SnapshotId::new(1)).unwrap();

        assert_eq!(state.missing_flush_and_restart, retrived_state.missing_flush_and_restart);
        assert_eq!(state.missing_terminate, retrived_state.missing_terminate);
        assert_eq!(state.wait_for_state, retrived_state.wait_for_state);
        assert_eq!(state.state_generation, retrived_state.state_generation);
        assert_eq!(state.watermark_forntier.compute_frontier(), retrived_state.watermark_forntier.compute_frontier());
        assert_eq!(state.receiver_state, retrived_state.receiver_state);
        assert_eq!(state.terminated_replicas, retrived_state.terminated_replicas);
        assert_eq!(state.message_queue, retrived_state.message_queue);
        
        // Clean redis
        start_block.persistency_service.delete_state(start_block.operator_coord, SnapshotId::new(1));

    }

    
    #[test]
    #[serial]
    fn test_single_start_persistency_multiple_snapshot() {
        let mut t = FakeNetworkTopology::new(1, 2);
        let (from1, sender1) = t.senders_mut()[0].pop().unwrap();
        let (from2, sender2) = t.senders_mut()[0].pop().unwrap();

        let mut start_block =
            Start::<i32, _>::single(sender1.receiver_endpoint.prev_block_id, None);

        let mut metadata = t.metadata();
        metadata.persistency_service = PersistencyService::new(Some(
            PersistencyConfig { 
                server_addr: String::from(REDIS_TEST_CONFIGURATION),
                try_restart: false,
                clean_on_exit: false,
                restart_from: None,
            }
        ));
        start_block.setup(&mut metadata);

        start_block.operator_coord.operator_id = 101;


        sender1
            .send(NetworkMessage::new_batch(
                vec![
                    StreamElement::Item(1), 
                    StreamElement::Snapshot(SnapshotId::new(1)),
                    StreamElement::Item(2),
                    StreamElement::Snapshot(SnapshotId::new(2)),
                    StreamElement::Item(3), 
                    ],
                from1,
            ))
            .unwrap();

        assert_eq!(StreamElement::Item(1), start_block.next());
        assert_eq!(StreamElement::Snapshot(SnapshotId::new(1)), start_block.next());
        assert_eq!(StreamElement::Item(2), start_block.next());
        assert_eq!(StreamElement::Snapshot(SnapshotId::new(2)), start_block.next());
        assert_eq!(StreamElement::Item(3), start_block.next());

        sender2
            .send(NetworkMessage::new_batch(
                vec![
                    StreamElement::Item(5), 
                    StreamElement::Item(6), 
                    StreamElement::Snapshot(SnapshotId::new(1)),
                    StreamElement::Item(7),
                    StreamElement::Snapshot(SnapshotId::new(2)),
                    StreamElement::Item(8),
                    ],
                from2,
            ))
            .unwrap();

        assert_eq!(StreamElement::Item(5), start_block.next());
        assert_eq!(StreamElement::Item(6), start_block.next());
        assert_eq!(StreamElement::Item(7), start_block.next());

        let state: StartState<i32, SimpleStartReceiver<i32>> = StartState {
            missing_flush_and_restart: 2,
            missing_terminate: 2,
            wait_for_state: false,
            state_generation: 0,
            watermark_forntier: WatermarkFrontier::new(vec![from1, from2]),
            receiver_state: None,
            terminated_replicas: vec![],
            message_queue: VecDeque::from([
                (from2, StreamElement::Item(5)), 
                (from2, StreamElement::Item(6)),
            ]),
        };

        let retrived_state: StartState<i32, SimpleStartReceiver<i32>> = start_block.persistency_service.get_state(start_block.operator_coord, SnapshotId::new(1)).unwrap();

        assert_eq!(state.missing_flush_and_restart, retrived_state.missing_flush_and_restart);
        assert_eq!(state.missing_terminate, retrived_state.missing_terminate);
        assert_eq!(state.wait_for_state, retrived_state.wait_for_state);
        assert_eq!(state.state_generation, retrived_state.state_generation);
        assert_eq!(state.watermark_forntier.compute_frontier(), retrived_state.watermark_forntier.compute_frontier());
        assert_eq!(state.receiver_state, retrived_state.receiver_state);
        assert_eq!(state.terminated_replicas, retrived_state.terminated_replicas);
        assert_eq!(state.message_queue, retrived_state.message_queue);

        assert_eq!(StreamElement::Item(8), start_block.next());

        let state: StartState<i32, SimpleStartReceiver<i32>> = StartState {
            missing_flush_and_restart: 2,
            missing_terminate: 2,
            wait_for_state: false,
            state_generation: 0,
            watermark_forntier: WatermarkFrontier::new(vec![from1, from2]),
            receiver_state: None,
            terminated_replicas: vec![],
            message_queue: VecDeque::from([
                (from2, StreamElement::Item(5)), 
                (from2, StreamElement::Item(6)),
                (from2, StreamElement::Item(7)),
            ]),
        };

        let retrived_state: StartState<i32, SimpleStartReceiver<i32>> = start_block.persistency_service.get_state(start_block.operator_coord, SnapshotId::new(2)).unwrap();

        assert_eq!(state.missing_flush_and_restart, retrived_state.missing_flush_and_restart);
        assert_eq!(state.missing_terminate, retrived_state.missing_terminate);
        assert_eq!(state.wait_for_state, retrived_state.wait_for_state);
        assert_eq!(state.state_generation, retrived_state.state_generation);
        assert_eq!(state.watermark_forntier.compute_frontier(), retrived_state.watermark_forntier.compute_frontier());
        assert_eq!(state.receiver_state, retrived_state.receiver_state);
        assert_eq!(state.terminated_replicas, retrived_state.terminated_replicas);
        assert_eq!(state.message_queue, retrived_state.message_queue);
        
        // Clean redis
        start_block.persistency_service.delete_state(start_block.operator_coord, SnapshotId::new(1));
        start_block.persistency_service.delete_state(start_block.operator_coord, SnapshotId::new(2));

    }

    #[test]
    #[serial]
    fn test_single_start_persistency_multiple_snapshot_terminate() {
        let mut t = FakeNetworkTopology::new(1, 2);
        let (from1, sender1) = t.senders_mut()[0].pop().unwrap();
        let (from2, sender2) = t.senders_mut()[0].pop().unwrap();

        let mut start_block =
            Start::<i32, _>::single(sender1.receiver_endpoint.prev_block_id, None);

        let mut metadata = t.metadata();
        metadata.persistency_service = PersistencyService::new(Some(
            PersistencyConfig { 
                server_addr: String::from(REDIS_TEST_CONFIGURATION),
                try_restart: false,
                clean_on_exit: false,
                restart_from: None,
            }
        ));
        start_block.setup(&mut metadata);

        start_block.operator_coord.operator_id = 102;


        sender1
            .send(NetworkMessage::new_batch(
                vec![
                    StreamElement::Item(1), 
                    StreamElement::Snapshot(SnapshotId::new(1)),
                    StreamElement::Item(2),
                    StreamElement::Snapshot(SnapshotId::new(2)),
                    StreamElement::Item(3), 
                    StreamElement::Snapshot(SnapshotId::new(3)),
                    StreamElement::Item(4),
                    ],
                from1,
            ))
            .unwrap();

        assert_eq!(StreamElement::Item(1), start_block.next());
        assert_eq!(StreamElement::Snapshot(SnapshotId::new(1)), start_block.next());
        assert_eq!(StreamElement::Item(2), start_block.next());
        assert_eq!(StreamElement::Snapshot(SnapshotId::new(2)), start_block.next());
        assert_eq!(StreamElement::Item(3), start_block.next());
        assert_eq!(StreamElement::Snapshot(SnapshotId::new(3)), start_block.next());
        assert_eq!(StreamElement::Item(4), start_block.next());

        sender2
            .send(NetworkMessage::new_batch(
                vec![
                    StreamElement::Item(5), 
                    StreamElement::Item(6), 
                    StreamElement::Snapshot(SnapshotId::new(1)),
                    StreamElement::Item(7),
                    StreamElement::FlushAndRestart,
                    StreamElement::Snapshot(SnapshotId::new(2)),
                    StreamElement::Terminate,
                    //StreamElement::Item(8), // Just for simplify this test, it cannot happen during normal execution
                    ],
                from2,
            ))
            .unwrap();

        assert_eq!(StreamElement::Item(5), start_block.next());
        assert_eq!(StreamElement::Item(6), start_block.next());
        assert_eq!(StreamElement::Item(7), start_block.next());

        let state: StartState<i32, SimpleStartReceiver<i32>> = StartState {
            missing_flush_and_restart: 2,
            missing_terminate: 2,
            wait_for_state: false,
            state_generation: 0,
            watermark_forntier: WatermarkFrontier::new(vec![from1, from2]),
            receiver_state: None,
            terminated_replicas: vec![],
            message_queue: VecDeque::from([
                (from2, StreamElement::Item(5)), 
                (from2, StreamElement::Item(6)),
            ]),
        };

        let retrived_state: StartState<i32, SimpleStartReceiver<i32>> = start_block.persistency_service.get_state(start_block.operator_coord, SnapshotId::new(1)).unwrap();

        assert_eq!(state.missing_flush_and_restart, retrived_state.missing_flush_and_restart);
        assert_eq!(state.missing_terminate, retrived_state.missing_terminate);
        assert_eq!(state.wait_for_state, retrived_state.wait_for_state);
        assert_eq!(state.state_generation, retrived_state.state_generation);
        assert_eq!(state.watermark_forntier.compute_frontier(), retrived_state.watermark_forntier.compute_frontier());
        assert_eq!(state.receiver_state, retrived_state.receiver_state);
        assert_eq!(state.terminated_replicas, retrived_state.terminated_replicas);
        assert_eq!(state.message_queue, retrived_state.message_queue);
        
        sender1
            .send(NetworkMessage::new_batch(
                vec![
                    StreamElement::Item(10), 
                    StreamElement::Snapshot(SnapshotId::new(4)),
                    StreamElement::Item(11),
                    ],
                from1,
            ))
            .unwrap();
        
        assert_eq!(StreamElement::Item(10), start_block.next());
        assert_eq!(StreamElement::Snapshot(SnapshotId::new(4)), start_block.next());
        assert_eq!(StreamElement::Item(11), start_block.next());

        let state: StartState<i32, SimpleStartReceiver<i32>> = StartState {
            missing_flush_and_restart: 1,
            missing_terminate: 1,
            wait_for_state: false,
            state_generation: 0,
            watermark_forntier: WatermarkFrontier::new(vec![from1, from2]),
            receiver_state: None,
            terminated_replicas: vec![from2],
            message_queue: VecDeque::from([]),
        };

        let retrived_state: StartState<i32, SimpleStartReceiver<i32>> = start_block.persistency_service.get_state(start_block.operator_coord, SnapshotId::new(4)).unwrap();

        assert_eq!(state.missing_flush_and_restart, retrived_state.missing_flush_and_restart);
        assert_eq!(state.missing_terminate, retrived_state.missing_terminate);
        assert_eq!(state.wait_for_state, retrived_state.wait_for_state);
        assert_eq!(state.state_generation, retrived_state.state_generation);
        assert_eq!(state.watermark_forntier.compute_frontier(), retrived_state.watermark_forntier.compute_frontier());
        assert_eq!(state.receiver_state, retrived_state.receiver_state);
        assert_eq!(state.terminated_replicas, retrived_state.terminated_replicas);
        assert_eq!(state.message_queue, retrived_state.message_queue);
        
        
        // Clean redis
        start_block.persistency_service.delete_state(start_block.operator_coord, SnapshotId::new(1));
        start_block.persistency_service.delete_state(start_block.operator_coord, SnapshotId::new(2));
        start_block.persistency_service.delete_state(start_block.operator_coord, SnapshotId::new(3));
        start_block.persistency_service.delete_state(start_block.operator_coord, SnapshotId::new(4));

    }

}
