use std::collections::VecDeque;
use std::time::Duration;

use hashbrown::{HashMap, HashSet};
use serde::{Deserialize, Serialize};

use crate::block::{BlockStructure, OperatorReceiver, OperatorStructure};
use crate::channel::{RecvTimeoutError, SelectResult};
use crate::network::{Coord, NetworkMessage};
use crate::operator::start::{SimpleStartReceiver, StartReceiver};
use crate::operator::{Data, ExchangeData, StreamElement, SnapshotId};
use crate::scheduler::{BlockId, ExecutionMetadata};

/// This enum is an _either_ type, it contain either an element from the left part or an element
/// from the right part.
///
/// Since those two parts are merged the information about which one ends is lost, therefore there
/// are two extra variants to keep track of that.
#[derive(Clone, Debug, Serialize, Deserialize, Ord, PartialOrd, Eq, PartialEq)]
pub(crate) enum BinaryElement<OutL: Data, OutR: Data> {
    /// An element of the stream on the left.
    Left(OutL),
    /// An element of the stream on the right.
    Right(OutR),
    /// The left side has ended.
    LeftEnd,
    /// The right side has ended.
    RightEnd,
}

/// The actual receiver from one of the two sides.
#[derive(Clone, Debug)]
struct SideReceiver<Out: ExchangeData, Item: ExchangeData> {
    /// The internal receiver for this side.
    receiver: SimpleStartReceiver<Out>,
    /// The number of replicas this side has.
    instances: usize,
    /// How many replicas from this side has not yet sent `StreamElement::FlushAndRestart`.
    missing_flush_and_restart: usize,
    /// How many replicas from this side has not yet sent `StreamElement::Terminate`.
    missing_terminate: usize,
    /// Whether this side is cached (i.e. after it is fully read, it's just replayed indefinitely).
    cached: bool,
    /// The content of the cache, if any.
    cache: Vec<NetworkMessage<Item>>,
    /// Whether the cache has been fully populated.
    cache_full: bool,
    /// The index of the first element to return from the cache.
    cache_pointer: usize,
}

impl<Out: ExchangeData, Item: ExchangeData> SideReceiver<Out, Item> {
    fn new(previous_block_id: BlockId, cached: bool) -> Self {
        Self {
            receiver: SimpleStartReceiver::new(previous_block_id),
            instances: 0,
            missing_flush_and_restart: 0,
            missing_terminate: 0,
            cached,
            cache: Default::default(),
            cache_full: false,
            cache_pointer: 0,
        }
    }

    fn setup(&mut self, metadata: &mut ExecutionMetadata) {
        self.receiver.setup(metadata);
        self.instances = self.receiver.prev_replicas().len();
        self.missing_flush_and_restart = self.instances;
        self.missing_terminate = self.instances;
    }

    fn recv(&mut self, timeout: Option<Duration>) -> Result<NetworkMessage<Out>, RecvTimeoutError> {
        if let Some(timeout) = timeout {
            self.receiver.recv_timeout(timeout)
        } else {
            Ok(self.receiver.recv())
        }
    }

    fn reset(&mut self) {
        self.missing_flush_and_restart = self.instances;
        if self.cached {
            self.cache_full = true;
            self.cache_pointer = 0;
        }
    }

    /// There is nothing more to read from this side for this iteration.
    fn is_ended(&self) -> bool {
        if self.cached {
            self.is_terminated()
        } else {
            self.missing_flush_and_restart == 0
        }
    }

    /// No more data can come from this side ever again.
    fn is_terminated(&self) -> bool {
        self.missing_terminate == 0
    }

    /// All the cached items have already been read.
    fn cache_finished(&self) -> bool {
        self.cache_pointer >= self.cache.len()
    }

    /// Get the next batch from the cache. This will panic if the cache has been entirely consumed.
    fn next_cached_item(&mut self) -> NetworkMessage<Item> {
        self.cache_pointer += 1;
        if self.cache_finished() {
            // Items are simply returned, so flush and restarts are not counted properly. Just make
            // sure that when the cache ends the counter is zero.
            self.missing_flush_and_restart = 0;
        }
        self.cache[self.cache_pointer - 1].clone()
    }
}

/// This receiver is able to receive data from two previous blocks.
///
/// To do so it will first select on the two channels, and wrap each element into an enumeration
/// that discriminates the two sides.
#[derive(Clone, Debug)]
pub(crate) struct BinaryStartReceiver<OutL: ExchangeData, OutR: ExchangeData> {
    left: SideReceiver<OutL, BinaryElement<OutL, OutR>>,
    right: SideReceiver<OutR, BinaryElement<OutL, OutR>>,
    first_message: bool,

    persistency_active: bool,
    on_going_snapshots: HashMap<SnapshotId, (BinaryReceiverState<OutL, OutR>, HashSet<Coord>)>,
    persisted_message_queue_left: VecDeque<NetworkMessage<OutL>>,
    persisted_message_queue_right: VecDeque<NetworkMessage<OutR>>,
    terminated_replicas: Vec<Coord>,
    last_snapshots: HashMap<Coord, SnapshotId>,
    iteration_stack_level: usize,
    iteration_index: Option<u64>,
    should_flush_cached_side: bool,
}

impl<OutL: ExchangeData, OutR: ExchangeData> BinaryStartReceiver<OutL, OutR> {
    pub(super) fn new(
        left_block_id: BlockId,
        right_block_id: BlockId,
        left_cache: bool,
        right_cache: bool,
        iteration_stack_level: usize,
        iteration_index: Option<u64>,
    ) -> Self {
        assert!(
            !(left_cache && right_cache),
            "At most one of the two sides can be cached"
        );
        Self {
            left: SideReceiver::new(left_block_id, left_cache),
            right: SideReceiver::new(right_block_id, right_cache),
            first_message: false,
            persistency_active: false,
            on_going_snapshots: HashMap::new(),
            persisted_message_queue_left: VecDeque::new(),
            persisted_message_queue_right: VecDeque::new(),
            terminated_replicas: Vec::new(),
            last_snapshots: HashMap::new(),
            iteration_stack_level,
            iteration_index,
            should_flush_cached_side: false,
        }
    }

    /// Process the incoming batch from left side.
    ///
    /// This will map all the elements of the batch into a new batch whose elements are wrapped in
    /// the variant of the correct side. Additionally this will search for
    /// `StreamElement::FlushAndRestart` messages and eventually emit the `LeftEnd`/`RightEnd`
    /// accordingly.    
    fn process_left_side(
        &mut self,
        message: NetworkMessage<OutL>,
    ) -> NetworkMessage<BinaryElement<OutL, OutR>> {
        let sender = message.sender();
        let mut to_cache = Vec::new();
        let mut to_persist = Vec::new();
        let mut to_return = Vec::new();

        for el in message.into_iter() {
            if self.should_flush_cached_side && self.left.cached{
                //flush this side untill flushAnd Restart
                if matches!(el, StreamElement::FlushAndRestart) {
                    self.should_flush_cached_side = false;
                } 
                continue;
            }  
            match el {
                StreamElement::FlushAndRestart => {
                    self.left.missing_flush_and_restart -= 1;
                    // make sure to add this message before `FlushAndRestart`
                    to_persist.push(el.clone());
                    if self.left.missing_flush_and_restart == 0 {
                        to_cache.push(StreamElement::Item(BinaryElement::LeftEnd));
                    }
                    to_cache.push(el.map(BinaryElement::Left));
                }
                StreamElement::Terminate => {
                    self.left.missing_terminate -= 1;
                    // handle terminate for snapshot
                    to_persist.push(el.clone());
                    // Terminate should not be cached
                    if !self.left.cached {
                        to_cache.push(el.map(BinaryElement::Left));
                    
                        // extend to_return with to_cache
                        to_return.extend(to_cache);
                        to_cache = Vec::new();

                        if self.persistency_active {
                            // put previous msgs in all partial snapshot that have sender in the set
                            let last_msgs = NetworkMessage::new_batch(to_persist, sender);
                            if last_msgs.num_items() > 0 {
                                self.add_left_item_to_snapshot(last_msgs);
                            }
                            to_persist = Vec::new();
                            // Process Terminate
                            self.process_terminate(sender);
                        }
                    }
                }
                StreamElement::Snapshot(mut snap_id) => {
                    // Set the iteration stack accordingly to iteration stack level
                    while snap_id.iteration_stack.len() < self.iteration_stack_level {
                        // put 0 at each missing level
                        snap_id.iteration_stack.push(0);
                    }
                    // Set iteration_index if needed
                    snap_id.iteration_index = self.iteration_index;
                    // if needed cache prev msgs
                    if self.left.cached && !to_cache.is_empty() {
                        let last_msgs = NetworkMessage::new_batch(to_cache.clone(), sender);
                        self.left.cache.push(last_msgs);
                        self.left.cache_pointer = self.left.cache.len();
                    }
                    // extend to_return with to_cache
                    to_return.extend(to_cache);
                    to_return.push(StreamElement::Snapshot(snap_id.clone()).map(BinaryElement::Left));
                    to_cache = Vec::new();

                    // put previous msgs in all partial snapshot that have sender in the set
                    let last_msgs = NetworkMessage::new_batch(to_persist, sender);
                    if last_msgs.num_items() > 0 {
                        self.add_left_item_to_snapshot(last_msgs);
                    }
                    to_persist = Vec::new();
                    // process this snapshot
                    self.process_snapshot(&snap_id, sender);                 
                }
                _ => {
                    to_persist.push(el.clone());
                    to_cache.push(el.map(BinaryElement::Left));
                }
            }
        }
        // batch ended, handle 
        // if needed cache prev msgs
        if self.left.cached && !to_cache.is_empty() {
            let last_msgs = NetworkMessage::new_batch(to_cache.clone(), sender);
            self.left.cache.push(last_msgs);
            self.left.cache_pointer = self.left.cache.len();
        }
        // extend to_return with to_cache
        to_return.extend(to_cache);
        if self.persistency_active {
            // put previous msgs in all partial snapshot that have sender in the set
            let last_msgs = NetworkMessage::new_batch(to_persist, sender);
            if last_msgs.num_items() > 0 {
                self.add_left_item_to_snapshot(last_msgs);
            }
        }
        // return batch
        NetworkMessage::new_batch(to_return, sender)
    }

    /// Process the incoming batch from right side.
    /// Similar to process_left_side()
    fn process_right_side(
        &mut self,
        message: NetworkMessage<OutR>,
    ) -> NetworkMessage<BinaryElement<OutL, OutR>> {
        let sender = message.sender();
        let mut to_cache = Vec::new();
        let mut to_persist = Vec::new();
        let mut to_return = Vec::new();

        for el in message.into_iter() {
            if self.should_flush_cached_side && self.right.cached{
                //flush this side untill flushAnd Restart
                if matches!(el, StreamElement::FlushAndRestart) {
                    self.should_flush_cached_side = false;
                } 
                continue;
            }            
            match el {
                StreamElement::FlushAndRestart => {
                    self.right.missing_flush_and_restart -= 1;
                    // make sure to add this message before `FlushAndRestart`
                    to_persist.push(el.clone());
                    if self.right.missing_flush_and_restart == 0 {
                        to_cache.push(StreamElement::Item(BinaryElement::RightEnd));
                    }
                    to_cache.push(el.map(BinaryElement::Right));
                }
                StreamElement::Terminate => {
                    self.right.missing_terminate -= 1;
                    // handle terminate for snapshot
                    to_persist.push(el.clone());
                    // Terminate should not be cached
                    if !self.right.cached {
                        to_cache.push(el.map(BinaryElement::Right));
                        // extend to_return with to_cache
                        to_return.extend(to_cache);
                        to_cache = Vec::new();

                        if self.persistency_active {
                            // put previous msgs in all partial snapshot that have sender in the set
                            let last_msgs = NetworkMessage::new_batch(to_persist, sender);
                            if last_msgs.num_items() > 0 {
                                self.add_right_item_to_snapshot(last_msgs);
                            }
                            to_persist = Vec::new();
                            // Process Terminate
                            self.process_terminate(sender);
                        }
                    }
                }
                StreamElement::Snapshot(mut snap_id) => {
                    // Set the iteration stack accordingly to iteration stack level
                    while snap_id.iteration_stack.len() < self.iteration_stack_level {
                        // put 0 at each missing level
                        snap_id.iteration_stack.push(0);
                    }
                    // Set iteration_index if needed
                    snap_id.iteration_index = self.iteration_index;
                    // if needed cache prev msgs
                    if self.right.cached && !to_cache.is_empty() {
                        let last_msgs = NetworkMessage::new_batch(to_cache.clone(), sender);
                        self.right.cache.push(last_msgs);
                        self.right.cache_pointer = self.right.cache.len();
                    }
                    // extend to_return with to_cache
                    to_return.extend(to_cache);
                    to_return.push(StreamElement::Snapshot(snap_id.clone()).map(BinaryElement::Right));
                    to_cache = Vec::new();

                    // put previous msgs in all partial snapshot that have sender in the set
                    let last_msgs = NetworkMessage::new_batch(to_persist, sender);
                    if last_msgs.num_items() > 0 {
                        self.add_right_item_to_snapshot(last_msgs);
                    }
                    to_persist = Vec::new();
                    // process this snapshot
                    self.process_snapshot(&snap_id, sender);                 
                }
                _ => {
                    to_persist.push(el.clone());
                    to_cache.push(el.map(BinaryElement::Right));
                }
            }
        }
        // batch ended, handle 
        // if needed cache prev msgs
        if self.right.cached && !to_cache.is_empty() {
            let last_msgs = NetworkMessage::new_batch(to_cache.clone(), sender);
            self.right.cache.push(last_msgs);
            self.right.cache_pointer = self.right.cache.len();
        }
        // extend to_return with to_cache
        to_return.extend(to_cache);
        if self.persistency_active {
            // put previous msgs in all partial snapshot that have sender in the set
            let last_msgs = NetworkMessage::new_batch(to_persist, sender);
            if last_msgs.num_items() > 0 {
                self.add_right_item_to_snapshot(last_msgs);
            }
        }        
        // return batch
        NetworkMessage::new_batch(to_return, sender)
    }

    fn state_copy(&self) -> BinaryReceiverState<OutL, OutR> {
        let left = SideReceiverState {
            missing_flush_and_restart: self.left.missing_flush_and_restart as u64,
            //missing_terminate: self.left.missing_terminate as u64,
            cached: self.left.cached,
            cache: self.left.cache.clone(),
            cache_full: self.left.cache_full, 
            cache_pointer: self.left.cache_pointer as u64,
        };
        let right = SideReceiverState {
            missing_flush_and_restart: self.right.missing_flush_and_restart as u64,
            //missing_terminate: self.right.missing_terminate as u64,
            cached: self.right.cached,
            cache: self.right.cache.clone(),
            cache_full: self.right.cache_full,
            cache_pointer: self.right.cache_pointer as u64,
        };
       BinaryReceiverState {
            left,
            right,
            first_message: self.first_message,
            persisted_message_queue_left: VecDeque::new(),
            persisted_message_queue_right: VecDeque::new(),
            should_flush_cached_side: false,
        }
    }

    fn process_snapshot(&mut self, snap_id: &SnapshotId, sender: Coord) {
        // Update last snapshots map
        self.last_snapshots.insert(sender, snap_id.clone());
        // Check if is already arrived this snapshot id
        if !self.on_going_snapshots.contains_key(snap_id) {
            // Check if a bigger snapshot is previously arrived, if yes ignore this snapshot
            if self.last_snapshots.values().any(|x| x > snap_id) {
                return;
            }             
            // Save current state, messages will be add then 
            let mut state = self.state_copy();
            if self.iteration_stack_level > 0 && snap_id.iteration_stack[self.iteration_stack_level - 1] != 0 {
                state.should_flush_cached_side = true;
            }           
            // Set all previous replicas that have to send this snapshot id
            let mut prev_replicas = HashSet::from_iter(self.prev_replicas_for_snapshot()); 
            prev_replicas.remove(&sender);
            for replica in self.terminated_replicas.iter() {
                prev_replicas.remove(replica);
            }
            // Add a new entry in the map with this snapshot id
            self.on_going_snapshots.insert(snap_id.clone(), (state, prev_replicas));
        }
        // Remove the sender from the set of previous replicas for skipped snapshot and this snapshot
        let mut past_snap: Vec<SnapshotId> = self.on_going_snapshots
            .keys()
            .cloned()
            .filter(|s| s <= &snap_id)
            .collect();
        past_snap.sort();
        for p_snap in past_snap {
            // Remove the sender from the set of previous replicas for this snapshot
            self.on_going_snapshots.get_mut(&p_snap).unwrap().1.remove(&sender);
        }
    }


    fn add_left_item_to_snapshot(&mut self, item: NetworkMessage<OutL>) {
        // Add item to message queue state 
        for entry in self.on_going_snapshots.iter_mut() {
            if entry.1.1.contains(&item.sender()) {
                entry.1.0.persisted_message_queue_left.push_back( item.clone());
            }
        }
    }

    fn add_right_item_to_snapshot(&mut self, item: NetworkMessage<OutR>) {
        // Add item to message queue state 
        for entry in self.on_going_snapshots.iter_mut() {
            if entry.1.1.contains(&item.sender()) {
                entry.1.0.persisted_message_queue_right.push_back( item.clone());
            }
        }
    }

    fn process_terminate(&mut self, sender: Coord) {
        // Be sure that is the first Terminate from this sender
        let mut snapshot_id = self.last_snapshots.get(&sender)
            .unwrap_or(&SnapshotId::new(0))
            .clone();
        if snapshot_id.terminate() {
            // Sender has already terminated, we can just ignore it 
            return;
        }
        snapshot_id = snapshot_id.next();
        let term_snap = SnapshotId::new_terminate(snapshot_id.id());
        self.last_snapshots.insert(sender, term_snap);
        // Add sender to terminated replicas
        self.terminated_replicas.push(sender); 
        let future_snap: Vec<SnapshotId> = self.on_going_snapshots.keys().cloned().collect();
        for partial_snapshot in future_snap {
            // Remove the sender from the set of previous replicas for this snapshot
            self.on_going_snapshots.get_mut(&partial_snapshot).unwrap().1.remove(&sender);
        }
    }

    fn prev_replicas_for_snapshot(&self) -> Vec<Coord> {
        let mut previous = Vec::new();
        if !(self.left.cached && self.left.cache_full) {
            previous.append(&mut self.left.receiver.prev_replicas());
        }
        if !(self.right.cached && self.right.cache_full) {
            previous.append(&mut self.right.receiver.prev_replicas());
        }
        previous
    }

    /// Receive from the previous sides the next batch, or fail with a timeout if provided.
    ///
    /// This will access only the needed side (i.e. if one of the sides ended, only the other is
    /// probed). This will try to use the cache if it's available.
    fn select(
        &mut self,
        timeout: Option<Duration>,
    ) -> Result<NetworkMessage<BinaryElement<OutL, OutR>>, RecvTimeoutError> {
        // both sides received all the `StreamElement::Terminate`, but the cached ones have never
        // been emitted
        if self.left.is_terminated() && self.right.is_terminated() {
            let num_terminates = if self.left.cached {
                for sender in self.left.receiver.prev_replicas() {
                    self.process_terminate(sender);
                }
                self.left.instances
            } else if self.right.cached {
                for sender in self.right.receiver.prev_replicas() {
                    self.process_terminate(sender);
                }
                self.right.instances
            } else {
                0
            };
            if num_terminates > 0 {
                return Ok(NetworkMessage::new_batch(
                    (0..num_terminates)
                        .map(|_| StreamElement::Terminate)
                        .collect(),
                    Default::default(),
                ));
            }
        }

        // both left and right received all the FlushAndRestart, prepare for the next iteration
        if self.left.is_ended()
            && self.right.is_ended()
            && self.left.cache_finished()
            && self.right.cache_finished()
        {
            self.left.reset();
            self.right.reset();
            self.first_message = true;
        }

        enum Side<L, R> {
            Left(L),
            Right(R),
        }

        // First message of this iteration, and there is a side with the cache:
        // we need to ask to the other side FIRST to know if this is the end of the stream or a new
        // iteration is about to start.
        let data = if self.first_message && (self.left.cached || self.right.cached) {
            debug_assert!(!self.left.cached || self.left.cache_full);
            debug_assert!(!self.right.cached || self.right.cache_full);
            self.first_message = false;
            if self.left.cached {
                if let Some(p_data) = self.persisted_message_queue_right.pop_front() {
                    Side::Right(Ok(p_data))
                } else {
                    Side::Right(self.right.recv(timeout))
                }
            } else if let Some(p_data) = self.persisted_message_queue_left.pop_front() {
                Side::Left(Ok(p_data))
            } else {
                Side::Left(self.left.recv(timeout))
            }
        } else if self.left.cached && self.left.cache_full && !self.left.cache_finished() {
            // The left side is cached, therefore we can access it immediately
            return Ok(self.left.next_cached_item());
        } else if self.right.cached && self.right.cache_full && !self.right.cache_finished() {
            // The right side is cached, therefore we can access it immediately
            return Ok(self.right.next_cached_item());
        } else if self.left.is_ended() {
            // There is nothing more to read from the left side (if cached, all the cache has
            // already been read).
            if let Some(p_data) = self.persisted_message_queue_right.pop_front() {
                Side::Right(Ok(p_data))
            } else {
                Side::Right(self.right.recv(timeout))
            }
        } else if self.right.is_ended() {
            // There is nothing more to read from the right side (if cached, all the cache has
            // already been read).
            if let Some(p_data) = self.persisted_message_queue_left.pop_front() {
                Side::Left(Ok(p_data))
            } else {
                Side::Left(self.left.recv(timeout))
            }
        } else {
            // Get msg form persisted queues if any
            if let Some(p_data) = self.persisted_message_queue_left.pop_front() {
                Side::Left(Ok(p_data))
            } else if let Some(p_data) = self.persisted_message_queue_right.pop_front() {
                Side::Right(Ok(p_data))
            } else {
                let left_terminated = self.left.is_terminated();
                let right_terminated = self.right.is_terminated();
                let left = self.left.receiver.receiver.as_mut().unwrap();
                let right = self.right.receiver.receiver.as_mut().unwrap();

                let data = match (left_terminated, right_terminated, timeout) {
                    (false, false, Some(timeout)) => left.select_timeout(right, timeout),
                    (false, false, None) => Ok(left.select(right)),

                    (true, false, Some(timeout)) => {
                        right.recv_timeout(timeout).map(|r| SelectResult::B(Ok(r)))
                    }
                    (false, true, Some(timeout)) => {
                        left.recv_timeout(timeout).map(|r| SelectResult::A(Ok(r)))
                    }

                    (true, false, None) => Ok(SelectResult::B(right.recv())),
                    (false, true, None) => Ok(SelectResult::A(left.recv())),

                    (true, true, _) => Err(RecvTimeoutError::Disconnected),
                };

                match data {
                    Ok(SelectResult::A(left)) => {
                        Side::Left(left.map_err(|_| RecvTimeoutError::Disconnected))
                    }
                    Ok(SelectResult::B(right)) => {
                        Side::Right(right.map_err(|_| RecvTimeoutError::Disconnected))
                    }
                    // timeout
                    Err(e) => Side::Left(Err(e)),
                }
            }
        };

        match data {
            Side::Left(Ok(left)) => Ok(
                self.process_left_side(left)
                ),
            Side::Right(Ok(right)) => Ok(
                self.process_right_side(right)
                ),
            Side::Left(Err(e)) | Side::Right(Err(e)) => Err(e),
        }
    }
}

impl<OutL: ExchangeData, OutR: ExchangeData> StartReceiver<BinaryElement<OutL, OutR>>
    for BinaryStartReceiver<OutL, OutR>
{
    fn setup(&mut self, metadata: &mut ExecutionMetadata) {
        self.left.setup(metadata);
        self.right.setup(metadata);
        self.persistency_active = metadata.persistency_builder.is_some();
    }

    fn prev_replicas(&self) -> Vec<Coord> {
        let mut previous = self.left.receiver.prev_replicas();
        previous.append(&mut self.right.receiver.prev_replicas());
        previous
    }

    fn cached_replicas(&self) -> usize {
        let mut cached = 0;
        if self.left.cached {
            cached += self.left.instances
        }
        if self.right.cached {
            cached += self.right.instances
        }
        cached
    }

    fn recv_timeout(
        &mut self,
        timeout: Duration,
    ) -> Result<NetworkMessage<BinaryElement<OutL, OutR>>, RecvTimeoutError> {
        self.select(Some(timeout))
    }

    fn recv(&mut self) -> NetworkMessage<BinaryElement<OutL, OutR>> {
        self.select(None).expect("receiver failed")
    }

    fn structure(&self) -> BlockStructure {
        let mut operator = OperatorStructure::new::<BinaryElement<OutL, OutR>, _>("Start");
        // op id must be 0
        operator.subtitle = "op id: 0".to_string();
        operator.receivers.push(OperatorReceiver::new::<OutL>(
            self.left.receiver.previous_block_id,
        ));
        operator.receivers.push(OperatorReceiver::new::<OutR>(
            self.right.receiver.previous_block_id,
        ));

        BlockStructure::default().add_operator(operator)
    }

    type ReceiverState = BinaryReceiverState<OutL, OutR>;

    fn get_state(&mut self, snap_id: SnapshotId) -> Option<Self::ReceiverState> {
        let entry = self.on_going_snapshots.get(&snap_id);
        if let Some((_, missing_rep)) = entry {
            if missing_rep.is_empty() {
                Some(self.on_going_snapshots.remove(&snap_id).unwrap().0)
            } else {
                None
            }
        } else {
            panic!("Binary receiver cannot provide state for snapshot: {:?}", snap_id);
        }

    }

    fn keep_msg_queue(&self) -> bool {
        true
    }

    fn set_state(&mut self, receiver_state: Self::ReceiverState) {
        self.first_message = receiver_state.first_message;
        self.should_flush_cached_side = receiver_state.should_flush_cached_side;
        self.left.missing_flush_and_restart = receiver_state.left.missing_flush_and_restart as usize;
        //self.left.missing_terminate = receiver_state.left.missing_terminate as usize;
        self.left.cached = receiver_state.left.cached;
        self.left.cache = receiver_state.left.cache;
        self.left.cache_full = receiver_state.left.cache_full;
        self.left.cache_pointer = receiver_state.left.cache_pointer as usize;
        self.right.missing_flush_and_restart = receiver_state.right.missing_flush_and_restart as usize;
        //self.right.missing_terminate = receiver_state.right.missing_terminate as usize;
        self.right.cached = receiver_state.right.cached;
        self.right.cache = receiver_state.right.cache;
        self.right.cache_full = receiver_state.right.cache_full;
        self.right.cache_pointer = receiver_state.right.cache_pointer as usize;
        // set persisted left msg queue
        self.persisted_message_queue_left = receiver_state.persisted_message_queue_left;
        // set persissted right msg queue
        self.persisted_message_queue_right = receiver_state.persisted_message_queue_right;
    }
}



#[derive(Clone, Serialize, Deserialize)]
pub (crate) struct SideReceiverState<Item> {
    missing_flush_and_restart: u64,
    //missing_terminate: u64,
    cached: bool,
    cache: Vec<NetworkMessage<Item>>,
    cache_full: bool,
    cache_pointer: u64,
}

#[derive(Clone, Serialize, Deserialize)]
pub (crate) struct BinaryReceiverState<OutL: Data, OutR: Data>{
    left: SideReceiverState<BinaryElement<OutL, OutR>>,
    right: SideReceiverState<BinaryElement<OutL, OutR>>,
    first_message: bool,
    persisted_message_queue_left: VecDeque<NetworkMessage<OutL>>,
    persisted_message_queue_right: VecDeque<NetworkMessage<OutR>>,
    should_flush_cached_side: bool,
}


impl<OutL: ExchangeData, OutR: ExchangeData> std::fmt::Debug for BinaryReceiverState<OutL, OutR>{
    fn fmt(&self, _f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        todo!()
    }
}


#[cfg(test)]
mod tests {
    use std::collections::VecDeque;

    use serial_test::serial;

    use crate::{network::NetworkMessage, test::{FakeNetworkTopology, persistency_config_unit_tests}, operator::{Start, BinaryElement, StreamElement, Operator, SideReceiverState, BinaryReceiverState, start::{StartState, watermark_frontier::WatermarkFrontier}, BinaryStartReceiver, SnapshotId}, persistency::builder::PersistencyBuilder};

    #[test]
    #[serial]
    fn test_multiple_start_persistency() {
        let mut t = FakeNetworkTopology::new(2, 1);
        let (from1, sender1) = t.senders_mut()[0].pop().unwrap();
        let (from2, sender2) = t.senders_mut()[1].pop().unwrap();

        let mut start_block = Start::<BinaryElement<i32, i32>, _>::multiple(
            from1.block_id,
            from2.block_id,
            true,
            false,
            None,
            0,
            None,
        );
       
        let mut metadata = t.metadata();
        let mut p_builder = PersistencyBuilder::new(Some(
            persistency_config_unit_tests()
        ));
        metadata.persistency_builder = Some(&p_builder);
        start_block.setup(&mut metadata);

        start_block.operator_coord.operator_id = 110;

        sender1
            .send(NetworkMessage::new_single(
                StreamElement::Item(1),
                from1,
            ))
            .unwrap();

        sender1
            .send(NetworkMessage::new_single(
                StreamElement::Snapshot(SnapshotId::new(1)),
                from1,
            ))
            .unwrap();

        sender1
            .send(NetworkMessage::new_single(
                StreamElement::Item(2),
                from1,
            ))
            .unwrap();

        assert_eq!(StreamElement::Item(BinaryElement::Left(1)), start_block.next());
        assert_eq!(StreamElement::Snapshot(SnapshotId::new(1)), start_block.next());
        assert_eq!(StreamElement::Item(BinaryElement::Left(2)), start_block.next());

        sender2
            .send(NetworkMessage::new_single(
                StreamElement::Item(3),
                from2,
            ))
            .unwrap();

        sender2
            .send(NetworkMessage::new_single(
                StreamElement::Item(4),
                from2,
            ))
            .unwrap(); 

        sender2
            .send(NetworkMessage::new_single(
                StreamElement::Snapshot(SnapshotId::new(1)),
                from2,
            ))
            .unwrap(); 

        sender2
            .send(NetworkMessage::new_single(
                StreamElement::Item(5),
                from2,
            ))
            .unwrap();     

        
        assert_eq!(StreamElement::Item(BinaryElement::Right(3)), start_block.next());
        assert_eq!(StreamElement::Item(BinaryElement::Right(4)), start_block.next());
        assert_eq!(StreamElement::Item(BinaryElement::Right(5)), start_block.next());
        
        let left_side: SideReceiverState<BinaryElement<i32, i32>> = SideReceiverState{
            missing_flush_and_restart: 1,
            cached: true,
            cache: vec![NetworkMessage::new_single(
                            StreamElement::Item(BinaryElement::Left(1)),
                            from1,
                        ),
                    ],
            cache_full: false,
            cache_pointer: 1,
        };

        let right_side: SideReceiverState<BinaryElement<i32, i32>> = SideReceiverState{
            missing_flush_and_restart: 1,
            cached: false,
            cache: vec![],
            cache_full: false,
            cache_pointer: 0,
        };

        let recv_state = BinaryReceiverState {
            left: left_side,
            right: right_side,
            first_message: false,
            persisted_message_queue_left: VecDeque::new(), 
            persisted_message_queue_right: VecDeque::from([
                    NetworkMessage::new_single(StreamElement::Item(3), from2), 
                    NetworkMessage::new_single(StreamElement::Item(4), from2),
                ]),
            should_flush_cached_side: false,
        };

        let state: StartState<BinaryElement<i32, i32>, BinaryStartReceiver<i32, i32>> = StartState {
            missing_flush_and_restart: 2,
            wait_for_state: false,
            watermark_forntier: WatermarkFrontier::new(vec![from1, from2]),
            receiver_state: Some(recv_state),
            message_queue: VecDeque::new(),
        };

        p_builder.flush_state_saver();
        start_block.persistency_service = Some(p_builder.generate_persistency_service());
        let retrived_state: StartState<BinaryElement<i32, i32>, BinaryStartReceiver<i32, i32>> = start_block.persistency_service.as_mut().unwrap().get_state(start_block.operator_coord, SnapshotId::new(1)).unwrap();

        assert_eq!(state.missing_flush_and_restart, retrived_state.missing_flush_and_restart);
        assert_eq!(state.wait_for_state, retrived_state.wait_for_state);
        assert_eq!(state.watermark_forntier.compute_frontier(), retrived_state.watermark_forntier.compute_frontier());
        assert_eq!(state.message_queue, retrived_state.message_queue);
        // Check receiver state
        let receiver = state.receiver_state.unwrap();
        let retrived_receiver = retrived_state.receiver_state.unwrap();
        assert_eq!(receiver.first_message, retrived_receiver.first_message);
        assert_eq!(receiver.persisted_message_queue_left, retrived_receiver.persisted_message_queue_left);
        assert_eq!(receiver.persisted_message_queue_right, retrived_receiver.persisted_message_queue_right);
        assert_eq!(receiver.left.missing_flush_and_restart, retrived_receiver.left.missing_flush_and_restart);
        assert_eq!(receiver.left.cached, retrived_receiver.left.cached);
        assert_eq!(receiver.left.cache_full, retrived_receiver.left.cache_full);
        assert_eq!(receiver.left.cache_pointer, retrived_receiver.left.cache_pointer);
        assert_eq!(receiver.left.cache, retrived_receiver.left.cache);
        assert_eq!(receiver.right.missing_flush_and_restart, retrived_receiver.right.missing_flush_and_restart);
        assert_eq!(receiver.right.cached, retrived_receiver.right.cached);
        assert_eq!(receiver.right.cache_full, retrived_receiver.right.cache_full);
        assert_eq!(receiver.right.cache_pointer, retrived_receiver.right.cache_pointer);
        assert_eq!(receiver.right.cache, retrived_receiver.right.cache);

        
        // Clean redis
        start_block.persistency_service.as_mut().unwrap().delete_state(start_block.operator_coord, SnapshotId::new(1));

    }

    #[test]
    #[serial]
    fn test_multiple_start_persistency_multiple_snapshot() {
        let mut t = FakeNetworkTopology::new(2, 1);
        let (from1, sender1) = t.senders_mut()[0].pop().unwrap();
        let (from2, sender2) = t.senders_mut()[1].pop().unwrap();

        let mut start_block = Start::<BinaryElement<i32, i32>, _>::multiple(
            from1.block_id,
            from2.block_id,
            true,
            false,
            None,
            0,
            None,
        );
       
        let mut metadata = t.metadata();
        let mut p_builder = PersistencyBuilder::new(Some(
            persistency_config_unit_tests()
        ));
        metadata.persistency_builder = Some(&p_builder);
        start_block.setup(&mut metadata);

        start_block.operator_coord.operator_id = 111;

        sender1
            .send(NetworkMessage::new_single(
                StreamElement::Item(1),
                from1,
            ))
            .unwrap();

        sender1
            .send(NetworkMessage::new_single(
                StreamElement::Snapshot(SnapshotId::new(1)),
                from1,
            ))
            .unwrap();

        sender1
            .send(NetworkMessage::new_single(
                StreamElement::Item(2),
                from1,
            ))
            .unwrap();

            sender1
            .send(NetworkMessage::new_single(
                StreamElement::Snapshot(SnapshotId::new(2)),
                from1,
            ))
            .unwrap();

        sender1
            .send(NetworkMessage::new_single(
                StreamElement::Item(3),
                from1,
            ))
            .unwrap();

        assert_eq!(StreamElement::Item(BinaryElement::Left(1)), start_block.next());
        assert_eq!(StreamElement::Snapshot(SnapshotId::new(1)), start_block.next());
        assert_eq!(StreamElement::Item(BinaryElement::Left(2)), start_block.next());
        assert_eq!(StreamElement::Snapshot(SnapshotId::new(2)), start_block.next());
        assert_eq!(StreamElement::Item(BinaryElement::Left(3)), start_block.next());

        sender2
            .send(NetworkMessage::new_single(
                StreamElement::Item(5),
                from2,
            ))
            .unwrap();

        sender2
            .send(NetworkMessage::new_single(
                StreamElement::Item(6),
                from2,
            ))
            .unwrap(); 

        sender2
            .send(NetworkMessage::new_single(
                StreamElement::Snapshot(SnapshotId::new(1)),
                from2,
            ))
            .unwrap(); 

        sender2
            .send(NetworkMessage::new_single(
                StreamElement::Item(7),
                from2,
            ))
            .unwrap(); 

        sender2
            .send(NetworkMessage::new_single(
                StreamElement::Snapshot(SnapshotId::new(2)),
                from2,
            ))
            .unwrap(); 

        sender2
            .send(NetworkMessage::new_single(
                StreamElement::Item(8),
                from2,
            ))
            .unwrap();    

        
        assert_eq!(StreamElement::Item(BinaryElement::Right(5)), start_block.next());
        assert_eq!(StreamElement::Item(BinaryElement::Right(6)), start_block.next());
        assert_eq!(StreamElement::Item(BinaryElement::Right(7)), start_block.next());
        
        
        let left_side: SideReceiverState<BinaryElement<i32, i32>> = SideReceiverState{
            missing_flush_and_restart: 1,
            cached: true,
            cache: vec![NetworkMessage::new_single(
                            StreamElement::Item(BinaryElement::Left(1)),
                            from1,
                        ),
                    ],
            cache_full: false,
            cache_pointer: 1,
        };

        let right_side: SideReceiverState<BinaryElement<i32, i32>> = SideReceiverState{
            missing_flush_and_restart: 1,
            cached: false,
            cache: vec![],
            cache_full: false,
            cache_pointer: 0,
        };

        let recv_state = BinaryReceiverState {
            left: left_side,
            right: right_side,
            first_message: false,
            persisted_message_queue_left: VecDeque::new(),
            persisted_message_queue_right: VecDeque::from([
                NetworkMessage::new_single(StreamElement::Item(5), from2), 
                NetworkMessage::new_single(StreamElement::Item(6), from2),
            ]),
            should_flush_cached_side: false,
        };

        let state: StartState<BinaryElement<i32, i32>, BinaryStartReceiver<i32, i32>> = StartState {
            missing_flush_and_restart: 2,
            wait_for_state: false,
            watermark_forntier: WatermarkFrontier::new(vec![from1, from2]),
            receiver_state: Some(recv_state),
            message_queue: VecDeque::new(),
        };

        p_builder.flush_state_saver();
        start_block.persistency_service = Some(p_builder.generate_persistency_service());
        let retrived_state: StartState<BinaryElement<i32, i32>, BinaryStartReceiver<i32, i32>> = start_block.persistency_service.as_mut().unwrap().get_state(start_block.operator_coord, SnapshotId::new(1)).unwrap();

        assert_eq!(state.message_queue, retrived_state.message_queue);
        // Check receiver state
        let receiver = state.receiver_state.unwrap();
        let retrived_receiver = retrived_state.receiver_state.unwrap();
        assert_eq!(receiver.first_message, retrived_receiver.first_message);
        assert_eq!(receiver.persisted_message_queue_left, retrived_receiver.persisted_message_queue_left);
        assert_eq!(receiver.persisted_message_queue_right, retrived_receiver.persisted_message_queue_right);
        assert_eq!(receiver.left.cache_pointer, retrived_receiver.left.cache_pointer);
        assert_eq!(receiver.left.cache, retrived_receiver.left.cache);
        assert_eq!(receiver.right.cache_pointer, retrived_receiver.right.cache_pointer);
        assert_eq!(receiver.right.cache, retrived_receiver.right.cache);

        assert_eq!(StreamElement::Item(BinaryElement::Right(8)), start_block.next());
        
        
        let left_side: SideReceiverState<BinaryElement<i32, i32>> = SideReceiverState{
            missing_flush_and_restart: 1,
            cached: true,
            cache: vec![NetworkMessage::new_single(
                            StreamElement::Item(BinaryElement::Left(1)),
                            from1,
                        ),
                        NetworkMessage::new_single(
                            StreamElement::Item(BinaryElement::Left(2)),
                            from1,
                        ),
                    ],
            cache_full: false,
            cache_pointer: 2,
        };

        let right_side: SideReceiverState<BinaryElement<i32, i32>> = SideReceiverState{
            missing_flush_and_restart: 1,
            cached: false,
            cache: vec![],
            cache_full: false,
            cache_pointer: 0,
        };

        let recv_state = BinaryReceiverState {
            left: left_side,
            right: right_side,
            first_message: false,
            persisted_message_queue_left: VecDeque::new(),
            persisted_message_queue_right: VecDeque::from([
                NetworkMessage::new_single(StreamElement::Item(5), from2), 
                NetworkMessage::new_single(StreamElement::Item(6), from2),
                NetworkMessage::new_single(StreamElement::Item(7), from2),
            ]),
            should_flush_cached_side: false,
        };

        let state: StartState<BinaryElement<i32, i32>, BinaryStartReceiver<i32, i32>> = StartState {
            missing_flush_and_restart: 2,
            wait_for_state: false,
            watermark_forntier: WatermarkFrontier::new(vec![from1, from2]),
            receiver_state: Some(recv_state),
            message_queue: VecDeque::new(),
        };

        p_builder.flush_state_saver();
        start_block.persistency_service = Some(p_builder.generate_persistency_service());
        let retrived_state: StartState<BinaryElement<i32, i32>, BinaryStartReceiver<i32, i32>> = start_block.persistency_service.as_mut().unwrap().get_state(start_block.operator_coord, SnapshotId::new(2)).unwrap();

        assert_eq!(state.message_queue, retrived_state.message_queue);
        // Check receiver state
        let receiver = state.receiver_state.unwrap();
        let retrived_receiver = retrived_state.receiver_state.unwrap();
        assert_eq!(receiver.first_message, retrived_receiver.first_message);
        assert_eq!(receiver.persisted_message_queue_left, retrived_receiver.persisted_message_queue_left);
        assert_eq!(receiver.persisted_message_queue_right, retrived_receiver.persisted_message_queue_right);
        assert_eq!(receiver.left.cache_pointer, retrived_receiver.left.cache_pointer);
        assert_eq!(receiver.left.cache, retrived_receiver.left.cache);
        assert_eq!(receiver.right.cache_pointer, retrived_receiver.right.cache_pointer);
        assert_eq!(receiver.right.cache, retrived_receiver.right.cache);

        
        // Clean redis
        start_block.persistency_service.as_mut().unwrap().delete_state(start_block.operator_coord, SnapshotId::new(1));
        start_block.persistency_service.as_mut().unwrap().delete_state(start_block.operator_coord, SnapshotId::new(2));

    }

    #[test]
    #[serial]
    fn test_multiple_start_persistency_multiple_snapshot_with_cache() {
        let mut t = FakeNetworkTopology::new(2, 1);
        let (from1, sender1) = t.senders_mut()[0].pop().unwrap();
        let (from2, sender2) = t.senders_mut()[1].pop().unwrap();

        let mut start_block = Start::<BinaryElement<i32, i32>, _>::multiple(
            from1.block_id,
            from2.block_id,
            true,
            false,
            None,
            0,
            None,
        );
       
        let mut metadata = t.metadata();
        let mut p_builder = PersistencyBuilder::new(Some(
            persistency_config_unit_tests()
        ));
        metadata.persistency_builder = Some(&p_builder);
        start_block.setup(&mut metadata);

        start_block.operator_coord.operator_id = 112;

        sender1
            .send(NetworkMessage::new_batch(
                vec![
                    StreamElement::Item(1),
                    StreamElement::Snapshot(SnapshotId::new(1)),
                    StreamElement::Item(2),
                    StreamElement::Snapshot(SnapshotId::new(2)),
                    StreamElement::Item(3),
                    StreamElement::FlushAndRestart,
                    StreamElement::Terminate,
                ],
                from1,
            ))
            .unwrap();

        assert_eq!(StreamElement::Item(BinaryElement::Left(1)), start_block.next());
        assert_eq!(StreamElement::Snapshot(SnapshotId::new(1)), start_block.next());
        assert_eq!(StreamElement::Item(BinaryElement::Left(2)), start_block.next());
        assert_eq!(StreamElement::Snapshot(SnapshotId::new(2)), start_block.next());
        assert_eq!(StreamElement::Item(BinaryElement::Left(3)), start_block.next());
        assert_eq!(StreamElement::Item(BinaryElement::LeftEnd), start_block.next());

        sender2
            .send(NetworkMessage::new_batch(
                vec![
                    StreamElement::Item(5),
                    StreamElement::Snapshot(SnapshotId::new(1)),
                    StreamElement::Item(6),
                    StreamElement::Snapshot(SnapshotId::new(2)),
                    StreamElement::FlushAndRestart,
                ],
                from2,
            ))
            .unwrap();    

        
        assert_eq!(StreamElement::Item(BinaryElement::Right(5)), start_block.next());
        assert_eq!(StreamElement::Item(BinaryElement::Right(6)), start_block.next());
        assert_eq!(StreamElement::Item(BinaryElement::RightEnd), start_block.next());
               
        
        let left_side: SideReceiverState<BinaryElement<i32, i32>> = SideReceiverState{
            missing_flush_and_restart: 0,
            cached: true,
            cache: vec![NetworkMessage::new_batch( vec![
                            StreamElement::Item(BinaryElement::Left(1)),
                            ],
                            from1,
                        ),
                        NetworkMessage::new_batch( vec![
                            StreamElement::Item(BinaryElement::Left(2)),
                            ],
                            from1,
                        ),
                    ],
            cache_full: false,
            cache_pointer: 2,
        };

        let right_side: SideReceiverState<BinaryElement<i32, i32>> = SideReceiverState{
            missing_flush_and_restart: 1,
            cached: false,
            cache: vec![],
            cache_full: false,
            cache_pointer: 0,
        };

        let recv_state = BinaryReceiverState {
            left: left_side,
            right: right_side,
            first_message: false,
            persisted_message_queue_left: VecDeque::new(),
            persisted_message_queue_right: VecDeque::from([
                NetworkMessage::new_single(StreamElement::Item(5), from2),
                NetworkMessage::new_single(StreamElement::Item(6), from2),
            ]),
            should_flush_cached_side: false,
        };

        let state: StartState<BinaryElement<i32, i32>, BinaryStartReceiver<i32, i32>> = StartState {
            missing_flush_and_restart: 1,
            wait_for_state: false,
            watermark_forntier: WatermarkFrontier::new(vec![from1, from2]),
            receiver_state: Some(recv_state),
            message_queue: VecDeque::new(),
        };

        p_builder.flush_state_saver();
        start_block.persistency_service = Some(p_builder.generate_persistency_service());
        let retrived_state: StartState<BinaryElement<i32, i32>, BinaryStartReceiver<i32, i32>> = start_block.persistency_service.as_mut().unwrap().get_state(start_block.operator_coord, SnapshotId::new(2)).unwrap();

        assert_eq!(state.message_queue, retrived_state.message_queue);
        // Check receiver state
        let receiver = state.receiver_state.unwrap();
        let retrived_receiver = retrived_state.receiver_state.unwrap();
        assert_eq!(receiver.first_message, retrived_receiver.first_message);
        assert_eq!(receiver.persisted_message_queue_left, retrived_receiver.persisted_message_queue_left);
        assert_eq!(receiver.persisted_message_queue_right, retrived_receiver.persisted_message_queue_right);
        assert_eq!(receiver.left.cache_pointer, retrived_receiver.left.cache_pointer);
        assert_eq!(receiver.left.cache, retrived_receiver.left.cache);
        assert_eq!(receiver.right.cache_pointer, retrived_receiver.right.cache_pointer);
        assert_eq!(receiver.right.cache, retrived_receiver.right.cache);

        
        // Call next() to reply stream from cache
        assert_eq!(StreamElement::FlushAndRestart, start_block.next());
        sender2
            .send(NetworkMessage::new_single(
                StreamElement::Snapshot(SnapshotId::new(3)),
                from2,
            ))
            .unwrap(); 
        
        assert_eq!(StreamElement::Snapshot(SnapshotId::new(3)), start_block.next());
        assert_eq!(StreamElement::Item(BinaryElement::Left(1)), start_block.next());
        
        
        p_builder.flush_state_saver();
        start_block.persistency_service = Some(p_builder.generate_persistency_service());
        let retrived_state: StartState<BinaryElement<i32, i32>, BinaryStartReceiver<i32, i32>> = start_block.persistency_service.as_mut().unwrap().get_state(start_block.operator_coord, SnapshotId::new(3)).unwrap();
        
        let left_side: SideReceiverState<BinaryElement<i32, i32>> = SideReceiverState{
            missing_flush_and_restart: 1,
            cached: true,
            cache: vec![NetworkMessage::new_batch( vec![
                            StreamElement::Item(BinaryElement::Left(1)),
                            ],
                            from1,
                        ),
                        NetworkMessage::new_batch( vec![
                            StreamElement::Item(BinaryElement::Left(2)),
                            ],
                            from1,
                        ),
                        NetworkMessage::new_batch(vec![
                            StreamElement::Item(BinaryElement::Left(3)),
                            StreamElement::Item(BinaryElement::LeftEnd),
                            StreamElement::FlushAndRestart
                            ],
                            from1,
                        ),
                    ],
            cache_full: true,
            cache_pointer: 0,
        };

        let right_side: SideReceiverState<BinaryElement<i32, i32>> = SideReceiverState{
            missing_flush_and_restart: 1,
            cached: false,
            cache: vec![],
            cache_full: false,
            cache_pointer: 0,
        };

        let recv_state = BinaryReceiverState {
            left: left_side,
            right: right_side,
            first_message: false,
            persisted_message_queue_left: VecDeque::new(), 
            persisted_message_queue_right: VecDeque::new(), 
            /*persisted_message_queue_right: VecDeque::from([
                NetworkMessage::new_single(StreamElement::Item(5), from2),
                NetworkMessage::new_single(StreamElement::Item(6), from2),
                NetworkMessage::new_single(StreamElement::FlushAndRestart, from2),
            ]),*/
            should_flush_cached_side: false,
        };

        let state: StartState<BinaryElement<i32, i32>, BinaryStartReceiver<i32, i32>> = StartState {
            missing_flush_and_restart: 2,
            wait_for_state: true,
            watermark_forntier: WatermarkFrontier::new(vec![from1, from2]),
            receiver_state: Some(recv_state),
            message_queue: VecDeque::new(),
        };

        assert_eq!(state.missing_flush_and_restart, retrived_state.missing_flush_and_restart);
        assert_eq!(state.wait_for_state, retrived_state.wait_for_state);
        assert_eq!(state.watermark_forntier.compute_frontier(), retrived_state.watermark_forntier.compute_frontier());
        assert_eq!(state.message_queue, retrived_state.message_queue);
        // Check receiver state
        let receiver = state.receiver_state.unwrap();
        let retrived_receiver = retrived_state.receiver_state.unwrap();
        assert_eq!(receiver.first_message, retrived_receiver.first_message);
        assert_eq!(receiver.persisted_message_queue_left, retrived_receiver.persisted_message_queue_left);
        assert_eq!(receiver.persisted_message_queue_right, retrived_receiver.persisted_message_queue_right);
        assert_eq!(receiver.left.missing_flush_and_restart, retrived_receiver.left.missing_flush_and_restart);
        assert_eq!(receiver.left.cached, retrived_receiver.left.cached);
        assert_eq!(receiver.left.cache_full, retrived_receiver.left.cache_full);
        assert_eq!(receiver.left.cache_pointer, retrived_receiver.left.cache_pointer);
        assert_eq!(receiver.left.cache, retrived_receiver.left.cache);
        assert_eq!(receiver.right.missing_flush_and_restart, retrived_receiver.right.missing_flush_and_restart);
        assert_eq!(receiver.right.cached, retrived_receiver.right.cached);
        assert_eq!(receiver.right.cache_full, retrived_receiver.right.cache_full);
        assert_eq!(receiver.right.cache_pointer, retrived_receiver.right.cache_pointer);
        assert_eq!(receiver.right.cache, retrived_receiver.right.cache);

        // Clean redis
        start_block.persistency_service.as_mut().unwrap().delete_state(start_block.operator_coord, SnapshotId::new(1));
        start_block.persistency_service.as_mut().unwrap().delete_state(start_block.operator_coord, SnapshotId::new(2));
        start_block.persistency_service.as_mut().unwrap().delete_state(start_block.operator_coord, SnapshotId::new(3));
    }
}
