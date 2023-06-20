use std::time::Duration;

use serde::{Deserialize, Serialize};

use crate::block::{BlockStructure, OperatorReceiver, OperatorStructure};
use crate::channel::{RecvTimeoutError, SelectResult};
use crate::network::{Coord, NetworkMessage};
use crate::operator::start::{SimpleStartReceiver, StartReceiver};
use crate::operator::{Data, ExchangeData, StreamElement};
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
}

impl<OutL: ExchangeData, OutR: ExchangeData> BinaryStartReceiver<OutL, OutR> {
    pub(super) fn new(
        left_block_id: BlockId,
        right_block_id: BlockId,
        left_cache: bool,
        right_cache: bool,
    ) -> Self {
        assert!(
            !(left_cache && right_cache),
            "At most one of the two sides can be cached"
        );
        Self {
            left: SideReceiver::new(left_block_id, left_cache),
            right: SideReceiver::new(right_block_id, right_cache),
            first_message: false,
        }
    }

    /// Process the incoming batch from one of the two sides.
    ///
    /// This will map all the elements of the batch into a new batch whose elements are wrapped in
    /// the variant of the correct side. Additionally this will search for
    /// `StreamElement::FlushAndRestart` messages and eventually emit the `LeftEnd`/`RightEnd`
    /// accordingly.
    fn process_side<Out: ExchangeData>(
        side: &mut SideReceiver<Out, BinaryElement<OutL, OutR>>,
        message: NetworkMessage<Out>,
        wrap: fn(Out) -> BinaryElement<OutL, OutR>,
        end: BinaryElement<OutL, OutR>,
    ) -> NetworkMessage<BinaryElement<OutL, OutR>> {
        let sender = message.sender();
        let data = message
            .into_iter()
            .flat_map(|item| {
                let mut res = Vec::new();
                if matches!(item, StreamElement::FlushAndRestart) {
                    side.missing_flush_and_restart -= 1;
                    // make sure to add this message before `FlushAndRestart`
                    if side.missing_flush_and_restart == 0 {
                        res.push(StreamElement::Item(end.clone()));
                    }
                }
                if matches!(item, StreamElement::Terminate) {
                    side.missing_terminate -= 1;
                }
                // StreamElement::Terminate should not be put in the cache
                if !side.cached || !matches!(item, StreamElement::Terminate) {
                    res.push(item.map(wrap));
                }
                res
            })
            .collect::<Vec<_>>();
        let message = NetworkMessage::new_batch(data, sender);
        if side.cached {
            // Remove StreamElement::Snapshot from messages to cache
            let data = message
                .clone()
                .into_iter()
                .filter(|item| !matches!(item, StreamElement::Snapshot(_)))
                .collect::<Vec<_>>();
            let filtered_message = NetworkMessage::new_batch(data, sender);
            if filtered_message.num_items() > 0 {
                side.cache.push(filtered_message);
            }

            // the elements are already out, ignore the cache for this round
            side.cache_pointer = side.cache.len();
        }
        message
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
                self.left.instances
            } else if self.right.cached {
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
                Side::Right(self.right.recv(timeout))
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
            Side::Right(self.right.recv(timeout))
        } else if self.right.is_ended() {
            // There is nothing more to read from the right side (if cached, all the cache has
            // already been read).
            Side::Left(self.left.recv(timeout))
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
        };

        match data {
            Side::Left(Ok(left)) => Ok(Self::process_side(
                &mut self.left,
                left,
                BinaryElement::Left,
                BinaryElement::LeftEnd,
            )),
            Side::Right(Ok(right)) => Ok(Self::process_side(
                &mut self.right,
                right,
                BinaryElement::Right,
                BinaryElement::RightEnd,
            )),
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
        operator.subtitle = format!("op id: 0");
        operator.receivers.push(OperatorReceiver::new::<OutL>(
            self.left.receiver.previous_block_id,
        ));
        operator.receivers.push(OperatorReceiver::new::<OutR>(
            self.right.receiver.previous_block_id,
        ));

        BlockStructure::default().add_operator(operator)
    }

    type ReceiverState = MultipleReceiverState<BinaryElement<OutL, OutR>>;

    fn get_state(&self) -> Option<Self::ReceiverState> {
        let left = SideReceiverState {
            missing_flush_and_restart: self.left.missing_flush_and_restart as u64,
            cached: self.left.cached,
            cache: self.left.cache.clone(),
            cache_full: self.left.cache_full,
            cache_pointer: self.left.cache_pointer as u64,
        };
        let right = SideReceiverState {
            missing_flush_and_restart: self.right.missing_flush_and_restart as u64,
            cached: self.right.cached,
            cache: self.right.cache.clone(),
            cache_full: self.right.cache_full,
            cache_pointer: self.right.cache_pointer as u64,
        };
        let state = MultipleReceiverState {
            left,
            right,
            first_message: self.first_message,
        };
        Some(state)
    }

    fn set_state(&mut self, receiver_state: Option<Self::ReceiverState>) {
        let state = receiver_state.unwrap();
        self.first_message = state.first_message;
        self.left.missing_flush_and_restart = state.left.missing_flush_and_restart as usize;
        self.left.cached = state.left.cached;
        self.left.cache = state.left.cache;
        self.left.cache_full = state.left.cache_full;
        self.left.cache_pointer = state.left.cache_pointer as usize;
        self.right.missing_flush_and_restart = state.right.missing_flush_and_restart as usize;
        self.right.cached = state.right.cached;
        self.right.cache = state.right.cache;
        self.right.cache_full = state.right.cache_full;
        self.right.cache_pointer = state.right.cache_pointer as usize;
    }
}



#[derive(Clone, Serialize, Deserialize)]
pub (crate) struct SideReceiverState<Item> {
    missing_flush_and_restart: u64,
    cached: bool,
    cache: Vec<NetworkMessage<Item>>,
    cache_full: bool,
    cache_pointer: u64,
}

#[derive(Clone, Serialize, Deserialize)]
pub (crate) struct MultipleReceiverState<Item>{
    left: SideReceiverState<Item>,
    right: SideReceiverState<Item>,
    first_message: bool,
}


impl<Item> std::fmt::Debug for MultipleReceiverState<Item>{
    fn fmt(&self, _f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        todo!()
    }
}


#[cfg(test)]
mod tests {
    use std::collections::VecDeque;

    use serial_test::serial;

    use crate::{network::NetworkMessage, test::{FakeNetworkTopology, REDIS_TEST_CONFIGURATION}, operator::{Start, BinaryElement, StreamElement, Operator, SideReceiverState, MultipleReceiverState, start::{StartState, watermark_frontier::WatermarkFrontier}, BinaryStartReceiver, SnapshotId}, persistency::{PersistencyService, PersistencyServices}, config::PersistencyConfig};

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
        );
       
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

        let recv_state = MultipleReceiverState {
            left: left_side,
            right: right_side,
            first_message: false,
        };

        let state: StartState<BinaryElement<i32, i32>, BinaryStartReceiver<i32, i32>> = StartState {
            missing_flush_and_restart: 2,
            wait_for_state: false,
            state_generation: 0,
            watermark_forntier: WatermarkFrontier::new(vec![from1, from2]),
            receiver_state: Some(recv_state),
            message_queue: VecDeque::from([
                (from2, StreamElement::Item(BinaryElement::Right(3))), 
                (from2, StreamElement::Item(BinaryElement::Right(4))),
            ]),
        };

        let retrived_state: StartState<BinaryElement<i32, i32>, BinaryStartReceiver<i32, i32>> = start_block.persistency_service.get_state(start_block.operator_coord, SnapshotId::new(1)).unwrap();

        assert_eq!(state.missing_flush_and_restart, retrived_state.missing_flush_and_restart);
        assert_eq!(state.wait_for_state, retrived_state.wait_for_state);
        assert_eq!(state.state_generation, retrived_state.state_generation);
        assert_eq!(state.watermark_forntier.compute_frontier(), retrived_state.watermark_forntier.compute_frontier());
        assert_eq!(state.message_queue, retrived_state.message_queue);
        // Check receiver state
        let receiver = state.receiver_state.unwrap();
        let retrived_receiver = retrived_state.receiver_state.unwrap();
        assert_eq!(receiver.first_message, retrived_receiver.first_message);
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
        start_block.persistency_service.delete_state(start_block.operator_coord, SnapshotId::new(1));

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
        );
       
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

        let recv_state = MultipleReceiverState {
            left: left_side,
            right: right_side,
            first_message: false,
        };

        let state: StartState<BinaryElement<i32, i32>, BinaryStartReceiver<i32, i32>> = StartState {
            missing_flush_and_restart: 2,
            wait_for_state: false,
            state_generation: 0,
            watermark_forntier: WatermarkFrontier::new(vec![from1, from2]),
            receiver_state: Some(recv_state),
            message_queue: VecDeque::from([
                (from2, StreamElement::Item(BinaryElement::Right(5))), 
                (from2, StreamElement::Item(BinaryElement::Right(6))),
            ]),
        };

        let retrived_state: StartState<BinaryElement<i32, i32>, BinaryStartReceiver<i32, i32>> = start_block.persistency_service.get_state(start_block.operator_coord, SnapshotId::new(1)).unwrap();

        assert_eq!(state.message_queue, retrived_state.message_queue);
        // Check receiver state
        let receiver = state.receiver_state.unwrap();
        let retrived_receiver = retrived_state.receiver_state.unwrap();
        assert_eq!(receiver.first_message, retrived_receiver.first_message);
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

        let recv_state = MultipleReceiverState {
            left: left_side,
            right: right_side,
            first_message: false,
        };

        let state: StartState<BinaryElement<i32, i32>, BinaryStartReceiver<i32, i32>> = StartState {
            missing_flush_and_restart: 2,
            wait_for_state: false,
            state_generation: 0,
            watermark_forntier: WatermarkFrontier::new(vec![from1, from2]),
            receiver_state: Some(recv_state),
            message_queue: VecDeque::from([
                (from2, StreamElement::Item(BinaryElement::Right(5))),
                (from2, StreamElement::Item(BinaryElement::Right(6))),
                (from2, StreamElement::Item(BinaryElement::Right(7))),
            ]),
        };

        let retrived_state: StartState<BinaryElement<i32, i32>, BinaryStartReceiver<i32, i32>> = start_block.persistency_service.get_state(start_block.operator_coord, SnapshotId::new(2)).unwrap();

        assert_eq!(state.message_queue, retrived_state.message_queue);
        // Check receiver state
        let receiver = state.receiver_state.unwrap();
        let retrived_receiver = retrived_state.receiver_state.unwrap();
        assert_eq!(receiver.first_message, retrived_receiver.first_message);
        assert_eq!(receiver.left.cache_pointer, retrived_receiver.left.cache_pointer);
        assert_eq!(receiver.left.cache, retrived_receiver.left.cache);
        assert_eq!(receiver.right.cache_pointer, retrived_receiver.right.cache_pointer);
        assert_eq!(receiver.right.cache, retrived_receiver.right.cache);

        
        // Clean redis
        start_block.persistency_service.delete_state(start_block.operator_coord, SnapshotId::new(1));
        start_block.persistency_service.delete_state(start_block.operator_coord, SnapshotId::new(2));

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
        );
       
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
                            StreamElement::Item(BinaryElement::Left(2)),
                            StreamElement::Item(BinaryElement::Left(3)),
                            StreamElement::Item(BinaryElement::LeftEnd),
                            StreamElement::FlushAndRestart
                            ],
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

        let recv_state = MultipleReceiverState {
            left: left_side,
            right: right_side,
            first_message: false,
        };

        let state: StartState<BinaryElement<i32, i32>, BinaryStartReceiver<i32, i32>> = StartState {
            missing_flush_and_restart: 1,
            wait_for_state: false,
            state_generation: 0,
            watermark_forntier: WatermarkFrontier::new(vec![from1, from2]),
            receiver_state: Some(recv_state),
            message_queue: VecDeque::from([
                (from2, StreamElement::Item(BinaryElement::Right(5))),
                (from2, StreamElement::Item(BinaryElement::Right(6))),
            ]),
        };

        let retrived_state: StartState<BinaryElement<i32, i32>, BinaryStartReceiver<i32, i32>> = start_block.persistency_service.get_state(start_block.operator_coord, SnapshotId::new(2)).unwrap();

        assert_eq!(state.message_queue, retrived_state.message_queue);
        // Check receiver state
        let receiver = state.receiver_state.unwrap();
        let retrived_receiver = retrived_state.receiver_state.unwrap();
        assert_eq!(receiver.first_message, retrived_receiver.first_message);
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
        

        // Extract manually the state
        let retrived_state = start_block.on_going_snapshots.get(&SnapshotId::new(3)).unwrap().clone().0;
        
        let left_side: SideReceiverState<BinaryElement<i32, i32>> = SideReceiverState{
            missing_flush_and_restart: 1,
            cached: true,
            cache: vec![NetworkMessage::new_batch(vec![
                            StreamElement::Item(BinaryElement::Left(1)),
                            StreamElement::Item(BinaryElement::Left(2)),
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

        let recv_state = MultipleReceiverState {
            left: left_side,
            right: right_side,
            first_message: false,
        };

        let state: StartState<BinaryElement<i32, i32>, BinaryStartReceiver<i32, i32>> = StartState {
            missing_flush_and_restart: 2,
            wait_for_state: true,
            state_generation: 2,
            watermark_forntier: WatermarkFrontier::new(vec![from1, from2]),
            receiver_state: Some(recv_state),
            message_queue: VecDeque::from([
                (from1, StreamElement::Item(BinaryElement::Left(1))),
            ]),
        };

        assert_eq!(state.missing_flush_and_restart, retrived_state.missing_flush_and_restart);
        assert_eq!(state.wait_for_state, retrived_state.wait_for_state);
        assert_eq!(state.state_generation, retrived_state.state_generation);
        assert_eq!(state.watermark_forntier.compute_frontier(), retrived_state.watermark_forntier.compute_frontier());
        assert_eq!(state.message_queue, retrived_state.message_queue);
        // Check receiver state
        let receiver = state.receiver_state.unwrap();
        let retrived_receiver = retrived_state.receiver_state.unwrap();
        assert_eq!(receiver.first_message, retrived_receiver.first_message);
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
        start_block.persistency_service.delete_state(start_block.operator_coord, SnapshotId::new(1));
        start_block.persistency_service.delete_state(start_block.operator_coord, SnapshotId::new(2));

    }
}
