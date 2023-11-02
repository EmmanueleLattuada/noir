use std::any::TypeId;
use std::collections::VecDeque;
use std::fmt::Display;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;


use crate::block::{
    Block, BlockStructure, Connection, NextStrategy, OperatorReceiver, OperatorStructure,
    Replication,
};
use crate::channel::RecvError::Disconnected;
use crate::channel::SelectResult;
use crate::environment::StreamEnvironmentInner;
use crate::network::{Coord, NetworkMessage, NetworkReceiver, NetworkSender, ReceiverEndpoint};
use crate::operator::end::End;
use crate::operator::iteration::iteration_end::IterationEnd;
use crate::operator::iteration::leader::IterationLeader;
use crate::operator::iteration::state_handler::IterationStateHandler;
use crate::operator::iteration::{
    IterationResult, IterationStateHandle, IterationStateLock, StateFeedback,
};
use crate::operator::source::Source;
use crate::operator::start::Start;
use crate::operator::{ExchangeData, Operator, StreamElement};
use crate::scheduler::{BlockId, ExecutionMetadata};
use crate::stream::Stream;

#[cfg(feature = "persist-state")]
use hashbrown::HashMap;
#[cfg(feature = "persist-state")]
use itertools::Itertools;
#[cfg(feature = "persist-state")]
use serde::{Serialize, Deserialize};
use crate::network::OperatorCoord;
#[cfg(feature = "persist-state")]
use crate::operator::SnapshotId;
#[cfg(feature = "persist-state")]
use crate::persistency::persistency_service::PersistencyService;
use crate::scheduler::OperatorId;


fn clone_with_default<T: Default>(_: &T) -> T {
    T::default()
}

/// This is the first operator of the chain of blocks inside an iteration.
///
/// After an iteration what comes out of the loop will come back inside for the next iteration.

#[cfg(not(feature = "persist-state"))]
#[derive(Derivative)]
#[derivative(Debug, Clone)]
pub struct Iterate<Out: ExchangeData, State: ExchangeData> {
    /// The coordinate of this replica.
    operator_coord: OperatorCoord,

    /// Helper structure that manages the iteration's state.
    state: IterationStateHandler<State>,

    /// The receiver of the data coming from the previous iteration of the loop.
    #[derivative(Clone(clone_with = "clone_with_default"))]
    input_receiver: Option<NetworkReceiver<Out>>,

    #[derivative(Clone(clone_with = "clone_with_default"))]
    feedback_receiver: Option<NetworkReceiver<Out>>,

    /// The id of the block that handles the feedback connection.
    feedback_end_block_id: Arc<AtomicUsize>,
    input_block_id: BlockId,
    /// The sender that will feed the data to the output of the iteration.
    output_sender: Option<NetworkSender<Out>>,
    /// The id of the block where the output of the iteration comes out.
    output_block_id: Arc<AtomicUsize>,

    /// The content of the stream to put back in the loop.
    content: VecDeque<StreamElement<Out>>,

    /// Used to store outside input arriving early
    input_stash: VecDeque<StreamElement<Out>>,
    /// The content to feed in the loop in the next iteration.
    feedback_content: VecDeque<StreamElement<Out>>,

    /// Whether the input stream has ended or not.
    input_finished: bool,
}

#[cfg(feature = "persist-state")]
#[derive(Derivative)]
#[derivative(Debug, Clone)]
pub struct Iterate<Out: ExchangeData, State: ExchangeData> {
    /// The coordinate of this replica.
    operator_coord: OperatorCoord,

    /// Helper structure that manages the iteration's state.
    state: IterationStateHandler<State>,

    /// The receiver of the data coming from the previous iteration of the loop.
    #[derivative(Clone(clone_with = "clone_with_default"))]
    input_receiver: Option<NetworkReceiver<Out>>,

    #[derivative(Clone(clone_with = "clone_with_default"))]
    feedback_receiver: Option<NetworkReceiver<Out>>,

    /// The id of the block that handles the feedback connection.
    feedback_end_block_id: Arc<AtomicUsize>,
    input_block_id: BlockId,
    /// The sender that will feed the data to the output of the iteration.
    output_sender: Option<NetworkSender<Out>>,
    /// The id of the block where the output of the iteration comes out.
    output_block_id: Arc<AtomicUsize>,

    /// The content of the stream to put back in the loop.
    content: VecDeque<StreamElement<Out>>,

    /// Used to store outside input arriving early
    input_stash: VecDeque<StreamElement<Out>>,
    /// The content to feed in the loop in the next iteration.
    feedback_content: VecDeque<StreamElement<Out>>,

    /// Whether the input stream has ended or not.
    input_finished: bool,

    /// Persistency service    
    persistency_service: Option<PersistencyService<IterateState<Out, State>>>,
    /// Strutcure to handle partial snapshots
    on_going_snapshots: HashMap<SnapshotId, IterateState<Out, State>>,
    /// Pending snapshot
    pending_snapshot: Option<SnapshotId>,
    /// State recovered from snaphot
    recovered_state: Option<State>,
    /// Level of this iterator
    iter_stack_level: usize,
    /// Index of this iteration loop
    iter_index: u64,
    should_flush: bool,
}

#[cfg(feature = "persist-state")]
#[derive(Clone, Serialize, Deserialize, Debug)]
struct IterateState<Out, State> {
    content: VecDeque<StreamElement<Out>>,
    feedback_content: VecDeque<StreamElement<Out>>,
    input_finished: bool,
    state: State,
}

impl<Out: ExchangeData, State: ExchangeData> Iterate<Out, State> {
    fn new(
        state_ref: IterationStateHandle<State>,
        input_block_id: BlockId,
        leader_block_id: BlockId,
        feedback_end_block_id: Arc<AtomicUsize>,
        output_block_id: Arc<AtomicUsize>,
        state_lock: Arc<IterationStateLock>,
        #[cfg(feature = "persist-state")]
        iter_stack_level: usize,
        #[cfg(feature = "persist-state")]
        iter_index: u64,
    ) -> Self {
        Self {
            // these fields will be set inside the `setup` method
            operator_coord: OperatorCoord::new(0, 0, 0, 0),
            input_receiver: None,
            feedback_receiver: None,
            feedback_end_block_id,
            input_block_id,
            output_sender: None,
            output_block_id,
            
            content: Default::default(),
            input_stash: Default::default(),
            feedback_content: Default::default(),
            input_finished: false,
            state: IterationStateHandler::new(leader_block_id, state_ref, state_lock),
            #[cfg(feature = "persist-state")]
            persistency_service: None,
            #[cfg(feature = "persist-state")]
            on_going_snapshots: HashMap::default(),
            #[cfg(feature = "persist-state")]
            pending_snapshot: None,
            #[cfg(feature = "persist-state")]
            recovered_state: None,
            #[cfg(feature = "persist-state")]
            iter_stack_level,
            #[cfg(feature = "persist-state")]
            iter_index,
            #[cfg(feature = "persist-state")]
            should_flush: false,
        }
    }

    fn next_input(&mut self) -> Option<StreamElement<Out>> {
        let item = self.input_stash.pop_front()?;

        let el = match &item {
            StreamElement::FlushAndRestart => {
                log::debug!("input finished for iterate {}", self.operator_coord.get_coord());
                self.input_finished = true;
                // since this moment accessing the state for the next iteration must wait
                self.state.lock();
                StreamElement::FlushAndRestart
            }
            #[cfg(feature = "persist-state")]
            StreamElement::Snapshot(snapshot_id) => {
                // This snapshot comes from outside
                // input stash doesn't need to be saved, previus operators will send again same data
                // content should be empty
                // feedback should be saved we snapshot token arrive from that channel

                // fix snapshot id
                // iter_stack should be of the same lenght of this iteration level
                let mut snap_id = snapshot_id.clone();
                while snap_id.iteration_stack.len() < self.iter_stack_level {
                    // put 0 at each missing level
                    snap_id.iteration_stack.push(0);
                } 
                // set iteration index
                let external_iter_index = snap_id.iteration_index;
                snap_id.iteration_index = Some(self.iter_index);
                // Check if its already in the map
                if let Some(mut state) = self.on_going_snapshots.remove(&snap_id){
                    // Complete and save this snapshot
                    state.input_finished = self.input_finished;
                    self.persistency_service
                        .as_mut()
                        .unwrap()
                        .save_state(
                            self.operator_coord,
                            snap_id.clone(), 
                            state,
                        );  
                } else {        
                    //save state in the map
                    let state = IterateState {
                        content: self.content.clone(),                      // should be empty
                        feedback_content: VecDeque::default(),              // overwritten later
                        input_finished: self.input_finished,
                        state: self.state.state_ref.get().clone(),
                    };
                    self.on_going_snapshots.insert(snap_id.clone(), state);
                }
                // Send the snapshot token also to output block
                let mut snap_id_out = snap_id.clone();
                // fix iter_stack and iter_index of snapshot id
                snap_id_out.iteration_stack.pop();
                snap_id_out.iteration_index = external_iter_index;
                let message = NetworkMessage::new_single(StreamElement::Snapshot(snap_id_out), self.operator_coord.get_coord());
                self.output_sender.as_ref().unwrap().send(message).unwrap();
                return Some(StreamElement::Snapshot(snap_id)); 
            }

            StreamElement::Item(_)
            | StreamElement::Timestamped(_, _)
            | StreamElement::Watermark(_)
            | StreamElement::FlushBatch => item,
            StreamElement::Terminate => {
                log::debug!("Iterate at {} is terminating", self.operator_coord.get_coord());
                #[cfg(feature = "persist-state")]
                if self.persistency_service.is_some() {
                    // flush all on going snapshots
                    let mut partial_snapshots = self.on_going_snapshots.keys().collect_vec();
                    partial_snapshots.sort();
                    for snap_id in partial_snapshots {
                        let state = self.on_going_snapshots.get(snap_id).unwrap();
                        self.persistency_service
                            .as_mut()
                            .unwrap()
                            .save_state(
                                self.operator_coord,
                                snap_id.clone(), 
                                state.clone(),
                            );
                    }
                    // save terminated state
                    let state = IterateState {
                        state: self.state.state_ref.get().clone(),
                        content: self.content.clone(),
                        feedback_content: self.feedback_content.clone(),
                        input_finished: self.input_finished,
                    };
                    self.persistency_service
                        .as_mut()
                        .unwrap()
                        .save_terminated_state(
                            self.operator_coord,
                            state,
                        );
                }               
                // send Terminate to output block
                let message = NetworkMessage::new_single(StreamElement::Terminate, self.operator_coord.get_coord());
                self.output_sender.as_ref().unwrap().send(message).unwrap();
                item
            }
        };
        Some(el)
    }

    fn next_stored(&mut self) -> Option<StreamElement<Out>> {
        let item = self.content.pop_front()?;
        let el = match &item {
            StreamElement::FlushAndRestart => {
                // since this moment accessing the state for the next iteration must wait
                self.state.lock();
                item
            }
            #[cfg(feature = "persist-state")]
            StreamElement::Snapshot(_) => {
                panic!("Snapshot token must not be saved in content or feedback_content")
            }
            StreamElement::Item(_)
            | StreamElement::Timestamped(_, _)
            | StreamElement::Watermark(_)
            | StreamElement::FlushBatch
            | StreamElement::Terminate => item,
        };

        Some(el)
    }

    fn feedback_finished(&self) -> bool {
        matches!(
            self.feedback_content.back(),
            Some(StreamElement::FlushAndRestart)
        )
    }

    #[cfg(feature = "persist-state")]
    fn handle_feedback_snapshot(&mut self, snap_id: SnapshotId) {
        // Handle partial snapshot
        if snap_id.iteration_stack[self.iter_stack_level - 1] == 0 {
            if let Some(mut state) = self.on_going_snapshots.remove(&snap_id){
                // Complete and save this snapshot
                state.feedback_content = self.feedback_content.clone();
                self.persistency_service
                    .as_mut()
                    .unwrap()
                    .save_state(
                        self.operator_coord,
                        snap_id.clone(), 
                        state,
                    );  
            } else {
                // I recevived this snap from the feedback channel but not yet from input channel
                //save state in the map
                let state = IterateState {
                    content: self.content.clone(),                      // should be empty
                    feedback_content: VecDeque::default(),              // fixed do not overwrite it later
                    input_finished: self.input_finished,                // overwrite it later
                    state: self.state.state_ref.get().clone(), 
                };
                self.on_going_snapshots.insert(snap_id.clone(), state);
            }
        }
        // A snapshot generated by leader is saved immediately by iterate so just ignore it        
    }

    pub(crate) fn input_or_feedback(&mut self) {
        let rx_feedback = self.feedback_receiver.as_ref().unwrap();

        if let Some(rx_input) = self.input_receiver.as_ref() {
            match rx_input.select(rx_feedback) {
                SelectResult::A(Ok(msg)) => {
                    self.input_stash.extend(msg.into_iter());
                }
                SelectResult::B(Ok(msg)) => {
                    // Check if it is a snapshot
                    #[cfg(feature = "persist-state")]
                    for el in msg.into_iter() {
                        match el {
                            StreamElement::Snapshot(snap_id) => {
                                self.handle_feedback_snapshot(snap_id);
                            },
                            _ => {
                                self.feedback_content.push_back(el);
                            }
                        }
                    }
                    #[cfg(not(feature = "persist-state"))]
                    self.feedback_content.extend(msg.into_iter());
                }
                SelectResult::A(Err(Disconnected)) => {
                    self.input_receiver = None;
                    self.input_or_feedback();
                }
                SelectResult::B(Err(Disconnected)) => {
                    log::error!("feedback_receiver disconnected!");
                    panic!("feedback_receiver disconnected!");
                }
            }
        } else {
            let msg = rx_feedback.recv().unwrap();
            // Check if it is a snapshot
            #[cfg(feature = "persist-state")]
            for el in msg.into_iter() {
                match el {
                    StreamElement::Snapshot(snap_id) => {
                        self.handle_feedback_snapshot(snap_id);
                    },
                    _ => {
                        self.feedback_content.push_back(el);
                    }
                }
            }
            #[cfg(not(feature = "persist-state"))]
            self.feedback_content.extend(msg.into_iter());
        }
    }

    pub(crate) fn wait_update(&mut self) -> StateFeedback<State> {
        // We need to stash inputs that arrive early to avoid deadlocks

        let rx_state = self.state.state_receiver().unwrap();
        loop {
            let state_msg = if let Some(rx_input) = self.input_receiver.as_ref() {
                match rx_state.select(rx_input) {
                    SelectResult::A(Ok(state_msg)) => state_msg,
                    SelectResult::A(Err(Disconnected)) => {
                        log::error!("state_receiver disconnected!");
                        panic!("state_receiver disconnected!");
                    }
                    SelectResult::B(Ok(msg)) => {
                        self.input_stash.extend(msg.into_iter());
                        continue;
                    }
                    SelectResult::B(Err(Disconnected)) => {
                        self.input_receiver = None;
                        continue;
                    }
                }
            } else {
                rx_state.recv().unwrap()
            };

            assert!(state_msg.num_items() == 1);

            match state_msg.into_iter().next().unwrap() {
                #[cfg(feature = "persist-state")]
                StreamElement::Item((should_continue, new_state, opt_snap)) => {
                    return (should_continue, new_state, opt_snap);
                }
                #[cfg(not(feature = "persist-state"))]
                StreamElement::Item((should_continue, new_state)) => {
                    return (should_continue, new_state);
                }
                StreamElement::FlushBatch => {}
                StreamElement::FlushAndRestart => {}
                m => unreachable!(
                    "Iterate received invalid message from IterationLeader: {}",
                    m.variant()
                ),
            }
        }
    }
}

impl<Out: ExchangeData, State: ExchangeData + Sync> Operator<Out> for Iterate<Out, State> {
    fn setup(&mut self, metadata: &mut ExecutionMetadata) {
        self.operator_coord.setup_coord(metadata.coord);

        let endpoint = ReceiverEndpoint::new(metadata.coord, self.input_block_id);
        self.input_receiver = Some(metadata.network.get_receiver(endpoint));

        let feedback_end_block_id = self.feedback_end_block_id.load(Ordering::Acquire) as BlockId;
        let feedback_endpoint = ReceiverEndpoint::new(metadata.coord, feedback_end_block_id);
        self.feedback_receiver = Some(metadata.network.get_receiver(feedback_endpoint));

        let output_block_id = self.output_block_id.load(Ordering::Acquire) as BlockId;
        let output_endpoint = ReceiverEndpoint::new(
            Coord::new(
                output_block_id,
                metadata.coord.host_id,
                metadata.coord.replica_id,
            ),
            metadata.coord.block_id,
        );
        self.output_sender = Some(metadata.network.get_sender(output_endpoint));

        self.state.setup(metadata);

        #[cfg(feature = "persist-state")]
        if let Some(pb) = metadata.persistency_builder {
            let p_service = pb.generate_persistency_service::<IterateState<Out, State>>();
            let snapshot_id = p_service.restart_from_snapshot(self.operator_coord);
            if let Some(snap_id) = snapshot_id {
                if snap_id.iteration_stack.last() != Some(&0) && !snap_id.terminate() {
                    self.should_flush = true;
                }
                // Get and resume the persisted state
                let opt_state: Option<IterateState<Out, State>> = p_service.get_state(self.operator_coord, snap_id);
                if let Some(state) = opt_state {
                    self.recovered_state = Some(state.state.clone());
                    self.content = state.content.clone();
                    self.feedback_content = state.feedback_content.clone();
                    self.input_finished = state.input_finished;
                } else {
                    panic!("No persisted state founded for op: {0}", self.operator_coord);
                }
            }
            self.persistency_service = Some(p_service);
        }

    }

    fn next(&mut self) -> StreamElement<Out> {
        // recover state if needed
        #[cfg(feature = "persist-state")]
        if let Some(r_state) = self.recovered_state.take(){
            let recovery_update = (IterationResult::Continue, r_state, None);
            self.state.lock();
            self.state.wait_sync_state(recovery_update);
        }
        // if i restart from an internal snapshot at this level of iter stack i need to flush all input
        #[cfg(feature = "persist-state")]
        while self.should_flush {
            let msg = self.input_receiver.as_ref().unwrap().recv().unwrap();
            for el in msg.into_iter() {
                if let StreamElement::FlushAndRestart = el {
                    self.should_flush = false;
                }
            }
        }
        loop {
            // Snapshot if required
            // The snapshot must be done at the start of the iteration
            #[cfg(feature = "persist-state")]
            if let Some(snap_id) = self.pending_snapshot.take() {
                //save state
                let state = IterateState {
                    state: self.state.state_ref.get().clone(),
                    content: self.content.clone(),
                    feedback_content: self.feedback_content.clone(), // should be empty
                    input_finished: self.input_finished,
                };
                self.persistency_service
                    .as_mut()
                    .unwrap()
                    .save_state(
                        self.operator_coord,
                        snap_id.clone(), 
                        state,
                    );
                return StreamElement::Snapshot(snap_id);
            }
            
            // try to make progress on the feedback
            while let Ok(message) = self.feedback_receiver.as_ref().unwrap().try_recv() {
                // Checks for snapshots
                #[cfg(feature = "persist-state")]
                for el in message.into_iter() {
                    match el {
                        StreamElement::Snapshot(snap_id) => {
                            self.handle_feedback_snapshot(snap_id);
                        },
                        _ => {
                            self.feedback_content.push_back(el);
                        }
                    }
                }
                #[cfg(not(feature = "persist-state"))]
                self.feedback_content.extend(message.into_iter());
            }

            if !self.input_finished {
                while self.input_stash.is_empty() {
                    self.input_or_feedback();
                }

                return self.next_input().unwrap();
            }

            if !self.content.is_empty() {
                return self.next_stored().unwrap();
            }

            while !self.feedback_finished() {
                self.input_or_feedback();
            }

            // All feedback received

            // from now do like replay, i can snapshot immediately since content should 
            // be full and feedback should be empty
            log::debug!("Iterate at {} has finished the iteration", self.operator_coord.get_coord());
            assert!(self.content.is_empty());
            std::mem::swap(&mut self.content, &mut self.feedback_content);

            let state_update = self.wait_update();

            // Get snapshot id to be used at next iteration snapshot
            #[cfg(feature = "persist-state")] {
                self.pending_snapshot = state_update.2.clone();
            }

            if let IterationResult::Finished = self.state.wait_sync_state(state_update) {
                log::debug!("Iterate block at {} finished", self.operator_coord.get_coord());
                // cleanup so that if this is a nested iteration next time we'll be good to start again
                self.input_finished = false;

                let message =
                    NetworkMessage::new_batch(self.content.drain(..).collect(), self.operator_coord.get_coord());
                self.output_sender.as_ref().unwrap().send(message).unwrap();
            }

            // This iteration has ended but FlushAndRestart has already been sent. To avoid sending
            // twice the FlushAndRestart repeat.
        }
    }

    fn structure(&self) -> BlockStructure {
        let mut operator = OperatorStructure::new::<Out, _>("Iterate");
        let op_id = self.operator_coord.operator_id;
        operator.subtitle = format!("op id: {op_id}");
        operator
            .receivers
            .push(OperatorReceiver::new::<StateFeedback<State>>(
                self.state.leader_block_id,
            ));
        operator.receivers.push(OperatorReceiver::new::<Out>(
            self.feedback_end_block_id.load(Ordering::Acquire) as BlockId,
        ));
        operator
            .receivers
            .push(OperatorReceiver::new::<Out>(self.input_block_id));
        let output_block_id = self.output_block_id.load(Ordering::Acquire);
        operator.connections.push(Connection::new::<Out, _>(
            output_block_id as BlockId,
            &NextStrategy::only_one(),
        ));
        BlockStructure::default().add_operator(operator)
    }

    fn get_op_id(&self) -> OperatorId {
        self.operator_coord.operator_id
    }

    #[cfg(feature = "persist-state")]
    fn get_stateful_operators(&self) -> Vec<OperatorId> {
        // This operator is stateful
        vec![self.operator_coord.operator_id]
    }
}

impl<Out: ExchangeData, State: ExchangeData + Sync> Display for Iterate<Out, State> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Iterate<{}>", std::any::type_name::<Out>())
    }
}

impl<Out: ExchangeData, OperatorChain> Stream<Out, OperatorChain>
where
    OperatorChain: Operator<Out> + 'static,
{
    #[cfg(feature = "persist-state")]
    /// Construct an iterative dataflow where the input stream is fed inside a cycle. What comes
    /// out of the loop will be fed back at the next iteration.
    ///
    /// This iteration is stateful, this means that all the replicas have a read-only access to the
    /// _iteration state_. The initial value of the state is given as parameter. When an iteration
    /// ends all the elements are reduced locally at each replica producing a `DeltaUpdate`. Those
    /// delta updates are later reduced on a single node that, using the `global_fold` function will
    /// compute the state for the next iteration. This state is also used in `loop_condition` to
    /// check whether the next iteration should start or not. `loop_condition` is also allowed to
    /// mutate the state.
    ///
    /// The initial value of `DeltaUpdate` is initialized with [`Default::default()`].
    ///
    /// The content of the loop has a new scope: it's defined by the `body` function that takes as
    /// parameter the stream of data coming inside the iteration and a reference to the state. This
    /// function should return the stream of the data that exits from the loop (that will be fed
    /// back).
    ///
    /// This construct produces two stream:
    ///
    /// - the first is a stream with a single item: the final state of the iteration
    /// - the second if the set of elements that exited the loop during the last iteration (i.e. the
    ///   ones that should have been fed back in the next iteration).
    ///
    /// **Note**: due to an internal limitation, it's not currently possible to add an iteration
    /// operator when the stream has limited parallelism. This means, for example, that after a
    /// non-parallel source you have to add a shuffle.
    ///
    /// **Note**: this operator will split the current block.
    ///
    /// ## Example
    /// ```
    /// # use noir::{StreamEnvironment, EnvironmentConfig};
    /// # use noir::operator::source::IteratorSource;
    /// # let mut env = StreamEnvironment::new(EnvironmentConfig::local(1));
    /// let s = env.stream(IteratorSource::new(0..3)).shuffle();
    /// let (state, items) = s.iterate(
    ///     3, // at most 3 iterations
    ///     0, // the initial state is zero
    ///     |s, state| s.map(|n| n + 10),
    ///     |delta: &mut i32, n| *delta += n,
    ///     |state, delta| *state += delta,
    ///     |_state| true,
    /// );
    /// let state = state.collect_vec();
    /// let items = items.collect_vec();
    /// env.execute_blocking();
    ///
    /// assert_eq!(state.get().unwrap(), vec![10 + 11 + 12 + 20 + 21 + 22 + 30 + 31 + 32]);
    /// assert_eq!(items.get().unwrap(), vec![30, 31, 32]);
    /// ```
    pub fn iterate<
        Body,
        StateUpdate: ExchangeData + Default,
        State: ExchangeData + Sync,
        OperatorChain2,
    >(
        self,
        num_iterations: usize,
        initial_state: State,
        body: Body,
        local_fold: impl Fn(&mut StateUpdate, Out) + Send + Clone + 'static,
        global_fold: impl Fn(&mut State, StateUpdate) + Send + Clone + 'static,
        loop_condition: impl Fn(&mut State) -> bool + Send + Clone + 'static,
    ) -> (
        Stream<State, impl Operator<State>>,
        Stream<Out, impl Operator<Out>>,
    )
    where
        Body: FnOnce(
            Stream<Out, Iterate<Out, State>>,
            IterationStateHandle<State>,
        ) -> Stream<Out, OperatorChain2>,
        OperatorChain2: Operator<Out> + 'static,
    {
        // this is required because if the iteration block is not present on all the hosts, the ones
        // without it won't receive the state updates.
        assert!(
            self.block.scheduler_requirements.replication.is_unlimited(),
            "Cannot have an iteration block with limited parallelism"
        );

        let state = IterationStateHandle::new(initial_state.clone());
        let state_clone = state.clone();
        let env = self.env.clone();

        // the id of the block where IterationEnd is. At this moment we cannot know it, so we
        // store a fake value inside this and as soon as we know it we set it to the right value.
        let shared_delta_update_end_block_id = Arc::new(AtomicUsize::new(0));
        let shared_feedback_end_block_id = Arc::new(AtomicUsize::new(0));
        let shared_output_block_id = Arc::new(AtomicUsize::new(0));

        // Compute iteration stack level
        let iter_stack_level = self.block.iteration_ctx().len() + 1;
        let iter_index = if iter_stack_level == 1 {
            // New iteration loop not nested: give it a new index
            let mut env = env.lock();
            env.scheduler_mut().iterative_operators_counter += 1;
            env.scheduler_mut().iterative_operators_counter
        } else {
            // Nested loop: take the index of the external loop
            self.block.iteration_index.unwrap()
        };

        // prepare the stream with the IterationLeader block, this will provide the state output
        let mut leader_stream = StreamEnvironmentInner::stream(
            env.clone(),
            IterationLeader::new(
                initial_state,
                num_iterations,
                global_fold,
                loop_condition,
                shared_delta_update_end_block_id.clone(),
                iter_stack_level,
            ),
        );
        let leader_block_id = leader_stream.block.id;
        // the output stream is outside this loop, so it doesn't have the lock for this state
        leader_stream.block.iteration_ctx = self.block.iteration_ctx.clone();

        let batch_mode = self.block.batch_mode;
        let input_block_id;
        let iter_ctx = self.block.iteration_ctx.clone();

        // This change
        let mut input_block = None;
        let mut allign_block = None;

        // Add block to allign snapshots tokens if required by
        // persistency configuration
        let (persistency, isa) = {
            let mut env = env.lock();
            (env.config.persistency_configuration.is_some(), env.scheduler_mut().iterations_snapshot_alignment)
        };
        if self.block.iteration_ctx().is_empty() && persistency && !isa {
            // Add allignment block
            let input_stream = self.split_block(
                End::new, 
                NextStrategy::group_by_replica(0)   // The right replica id is known only after setup, so this will be overwritten
            );
            let mut input = input_stream.add_operator(|prev| End::new(
                prev, 
                NextStrategy::only_one(), 
                batch_mode
            ));
            input.block.is_only_one_strategy = true;
            input_block_id = input.block.id;
            allign_block = Some(input.block);

        } else {
            let mut input = self.add_operator(|prev| End::new(
                prev, 
                NextStrategy::only_one(), 
                batch_mode
            ));
            input.block.is_only_one_strategy = true;
            input_block_id = input.block.id;
            input_block = Some(input.block);
        }

        // the lock for synchronizing the access to the state of this iteration
        let state_lock = Arc::new(IterationStateLock::default());

        let iterate_block_id = {
            let mut env = env.lock();
            env.new_block_id()
        };
        let iter_source = Iterate::new(
            state,
            input_block_id,
            leader_block_id,
            shared_feedback_end_block_id.clone(),
            shared_output_block_id.clone(),
            state_lock.clone(),
            iter_stack_level,
            iter_index,
        );

        let mut iter_start = Stream {
            block: Block::new(
                iterate_block_id,
                iter_source,
                batch_mode,
                iter_ctx,
                Some(iter_index),
            ),
            env: env.clone(),
        };

        iter_start.block.iteration_ctx.push(state_lock.clone());
        // save the stack of the iteration for checking the stream returned by the body
        let pre_iter_stack = iter_start.block.iteration_ctx();

        // prepare the stream that will output the content of the loop
        let output = StreamEnvironmentInner::stream(
            env.clone(),
            Start::single(
                iterate_block_id,
                iter_start.block.iteration_ctx.last().cloned(),
            ),
        );
        let output_block_id = output.block.id;

        // attach the body of the loop to the Iterate operator
        let body = body(iter_start, state_clone);

        // Split the body of the loop in 2: the end block of the loop must ignore the output stream
        // since it's manually handled by the Iterate operator.
        let mut body = body.split_block(
            move |prev, next_strategy, batch_mode| {
                let mut end = End::new(prev, next_strategy, batch_mode);
                end.ignore_destination(output_block_id);
                end
            },
            NextStrategy::only_one(),
        );
        let body_block_id = body.block.id;

        let post_iter_stack = body.block.iteration_ctx();
        if pre_iter_stack != post_iter_stack {
            panic!("The body of the iteration should return the stream given as parameter");
        }
        body.block.iteration_ctx.pop().unwrap();

        // First split of the body: the data will be reduced into delta updates
        let state_update_end = StreamEnvironmentInner::stream(
            env.clone(),
            Start::single(body.block.id, Some(state_lock)),
        )
        .key_by(|_| ())
        .fold(StateUpdate::default(), local_fold)
        .drop_key()
        .add_operator(|prev| IterationEnd::new(prev, leader_block_id));
        let state_update_end_block_id = state_update_end.block.id;

        // Second split of the body: the data will be fed back to the Iterate block
        let batch_mode = body.block.batch_mode;
        let mut feedback_end = body.add_operator(|prev| {
            let mut end = End::new(prev, NextStrategy::only_one(), batch_mode);
            end.mark_feedback(iterate_block_id);
            end
        });
        feedback_end.block.is_only_one_strategy = true;
        let feedback_end_block_id = feedback_end.block.id;

        let mut env = env.lock();
        let scheduler = env.scheduler_mut();
        scheduler.schedule_block(state_update_end.block);
        scheduler.schedule_block(feedback_end.block);
        if let Some(input_b) = input_block {
            scheduler.schedule_block(input_b);
        } else {
            scheduler.schedule_block(allign_block.unwrap());
        }
        scheduler.connect_blocks(input_block_id, iterate_block_id, TypeId::of::<Out>());
        // connect the end of the loop to the IterationEnd
        scheduler.connect_blocks(
            body_block_id,
            state_update_end_block_id,
            TypeId::of::<Out>(),
        );
        // connect the IterationEnd to the IterationLeader
        scheduler.connect_blocks(
            state_update_end_block_id,
            leader_block_id,
            TypeId::of::<StateUpdate>(),
        );
        // connect the IterationLeader to the Iterate
        scheduler.connect_blocks(
            leader_block_id,
            iterate_block_id,
            TypeId::of::<StateFeedback<State>>(),
        );
        // connect the feedback
        scheduler.connect_blocks(feedback_end_block_id, iterate_block_id, TypeId::of::<Out>());
        // connect the output stream
        scheduler.connect_blocks_fragile(iterate_block_id, output_block_id, TypeId::of::<Out>());
        drop(env);

        // store the id of the blocks we now know
        shared_delta_update_end_block_id
            .store(state_update_end_block_id as usize, Ordering::Release);
        shared_feedback_end_block_id.store(feedback_end_block_id as usize, Ordering::Release);
        shared_output_block_id.store(output_block_id as usize, Ordering::Release);

        // TODO: check parallelism and make sure the blocks are spawned on the same replicas

        // FIXME: this add_block is here just to make sure that the NextStrategy of output_stream
        //        is not changed by the following operators. This because the next strategy affects
        //        the connections made by the scheduler and if accidentally set to OnlyOne will
        //        break the connections.
        (
            leader_stream.split_block(End::new, NextStrategy::random()),
            output,
        )
    }

    #[cfg(not(feature = "persist-state"))]
    /// Construct an iterative dataflow where the input stream is fed inside a cycle. What comes
    /// out of the loop will be fed back at the next iteration.
    ///
    /// This iteration is stateful, this means that all the replicas have a read-only access to the
    /// _iteration state_. The initial value of the state is given as parameter. When an iteration
    /// ends all the elements are reduced locally at each replica producing a `DeltaUpdate`. Those
    /// delta updates are later reduced on a single node that, using the `global_fold` function will
    /// compute the state for the next iteration. This state is also used in `loop_condition` to
    /// check whether the next iteration should start or not. `loop_condition` is also allowed to
    /// mutate the state.
    ///
    /// The initial value of `DeltaUpdate` is initialized with [`Default::default()`].
    ///
    /// The content of the loop has a new scope: it's defined by the `body` function that takes as
    /// parameter the stream of data coming inside the iteration and a reference to the state. This
    /// function should return the stream of the data that exits from the loop (that will be fed
    /// back).
    ///
    /// This construct produces two stream:
    ///
    /// - the first is a stream with a single item: the final state of the iteration
    /// - the second if the set of elements that exited the loop during the last iteration (i.e. the
    ///   ones that should have been fed back in the next iteration).
    ///
    /// **Note**: due to an internal limitation, it's not currently possible to add an iteration
    /// operator when the stream has limited parallelism. This means, for example, that after a
    /// non-parallel source you have to add a shuffle.
    ///
    /// **Note**: this operator will split the current block.
    ///
    /// ## Example
    /// ```
    /// # use noir::{StreamEnvironment, EnvironmentConfig};
    /// # use noir::operator::source::IteratorSource;
    /// # let mut env = StreamEnvironment::new(EnvironmentConfig::local(1));
    /// let s = env.stream(IteratorSource::new(0..3)).shuffle();
    /// let (state, items) = s.iterate(
    ///     3, // at most 3 iterations
    ///     0, // the initial state is zero
    ///     |s, state| s.map(|n| n + 10),
    ///     |delta: &mut i32, n| *delta += n,
    ///     |state, delta| *state += delta,
    ///     |_state| true,
    /// );
    /// let state = state.collect_vec();
    /// let items = items.collect_vec();
    /// env.execute_blocking();
    ///
    /// assert_eq!(state.get().unwrap(), vec![10 + 11 + 12 + 20 + 21 + 22 + 30 + 31 + 32]);
    /// assert_eq!(items.get().unwrap(), vec![30, 31, 32]);
    /// ```
    pub fn iterate<
        Body,
        StateUpdate: ExchangeData + Default,
        State: ExchangeData + Sync,
        OperatorChain2,
    >(
        self,
        num_iterations: usize,
        initial_state: State,
        body: Body,
        local_fold: impl Fn(&mut StateUpdate, Out) + Send + Clone + 'static,
        global_fold: impl Fn(&mut State, StateUpdate) + Send + Clone + 'static,
        loop_condition: impl Fn(&mut State) -> bool + Send + Clone + 'static,
    ) -> (
        Stream<State, impl Operator<State>>,
        Stream<Out, impl Operator<Out>>,
    )
    where
        Body: FnOnce(
            Stream<Out, Iterate<Out, State>>,
            IterationStateHandle<State>,
        ) -> Stream<Out, OperatorChain2>,
        OperatorChain2: Operator<Out> + 'static,
    {
        // this is required because if the iteration block is not present on all the hosts, the ones
        // without it won't receive the state updates.
        assert!(
            self.block.scheduler_requirements.replication.is_unlimited(),
            "Cannot have an iteration block with limited parallelism"
        );

        let state = IterationStateHandle::new(initial_state.clone());
        let state_clone = state.clone();
        let env = self.env.clone();

        // the id of the block where IterationEnd is. At this moment we cannot know it, so we
        // store a fake value inside this and as soon as we know it we set it to the right value.
        let shared_delta_update_end_block_id = Arc::new(AtomicUsize::new(0));
        let shared_feedback_end_block_id = Arc::new(AtomicUsize::new(0));
        let shared_output_block_id = Arc::new(AtomicUsize::new(0));

        // prepare the stream with the IterationLeader block, this will provide the state output
        let mut leader_stream = StreamEnvironmentInner::stream(
            env.clone(),
            IterationLeader::new(
                initial_state,
                num_iterations,
                global_fold,
                loop_condition,
                shared_delta_update_end_block_id.clone(),
            ),
        );
        let leader_block_id = leader_stream.block.id;
        // the output stream is outside this loop, so it doesn't have the lock for this state
        leader_stream.block.iteration_ctx = self.block.iteration_ctx.clone();

        // the lock for synchronizing the access to the state of this iteration
        let state_lock = Arc::new(IterationStateLock::default());

        let input_block_id = self.block.id;
        let batch_mode = self.block.batch_mode;
        let mut input =
            self.add_operator(|prev| End::new(prev, NextStrategy::only_one(), batch_mode));
        input.block.is_only_one_strategy = true;

        let iterate_block_id = {
            let mut env = env.lock();
            env.new_block_id()
        };
        let iter_source = Iterate::new(
            state,
            input_block_id,
            leader_block_id,
            shared_feedback_end_block_id.clone(),
            shared_output_block_id.clone(),
            state_lock.clone(),
        );

        let mut iter_start = Stream {
            block: Block::new(
                iterate_block_id,
                iter_source,
                batch_mode,
                input.block.iteration_ctx.clone(),
            ),
            env: env.clone(),
        };

        iter_start.block.iteration_ctx.push(state_lock.clone());
        // save the stack of the iteration for checking the stream returned by the body
        let pre_iter_stack = iter_start.block.iteration_ctx();

        // prepare the stream that will output the content of the loop
        let output = StreamEnvironmentInner::stream(
            env.clone(),
            Start::single(
                iterate_block_id,
                iter_start.block.iteration_ctx.last().cloned(),
            ),
        );
        let output_block_id = output.block.id;

        // attach the body of the loop to the Iterate operator
        let body = body(iter_start, state_clone);

        // Split the body of the loop in 2: the end block of the loop must ignore the output stream
        // since it's manually handled by the Iterate operator.
        let mut body = body.split_block(
            move |prev, next_strategy, batch_mode| {
                let mut end = End::new(prev, next_strategy, batch_mode);
                end.ignore_destination(output_block_id);
                end
            },
            NextStrategy::only_one(),
        );
        let body_block_id = body.block.id;

        let post_iter_stack = body.block.iteration_ctx();
        if pre_iter_stack != post_iter_stack {
            panic!("The body of the iteration should return the stream given as parameter");
        }
        body.block.iteration_ctx.pop().unwrap();

        // First split of the body: the data will be reduced into delta updates
        let state_update_end = StreamEnvironmentInner::stream(
            env.clone(),
            Start::single(body.block.id, Some(state_lock)),
        )
        .key_by(|_| ())
        .fold(StateUpdate::default(), local_fold)
        .drop_key()
        .add_operator(|prev| IterationEnd::new(prev, leader_block_id));
        let state_update_end_block_id = state_update_end.block.id;

        // Second split of the body: the data will be fed back to the Iterate block
        let batch_mode = body.block.batch_mode;
        let mut feedback_end = body.add_operator(|prev| {
            let mut end = End::new(prev, NextStrategy::only_one(), batch_mode);
            end.mark_feedback(iterate_block_id);
            end
        });
        feedback_end.block.is_only_one_strategy = true;
        let feedback_end_block_id = feedback_end.block.id;

        let mut env = env.lock();
        let scheduler = env.scheduler_mut();
        scheduler.schedule_block(state_update_end.block);
        scheduler.schedule_block(feedback_end.block);
        scheduler.schedule_block(input.block);
        scheduler.connect_blocks(input_block_id, iterate_block_id, TypeId::of::<Out>());
        // connect the end of the loop to the IterationEnd
        scheduler.connect_blocks(
            body_block_id,
            state_update_end_block_id,
            TypeId::of::<Out>(),
        );
        // connect the IterationEnd to the IterationLeader
        scheduler.connect_blocks(
            state_update_end_block_id,
            leader_block_id,
            TypeId::of::<StateUpdate>(),
        );
        // connect the IterationLeader to the Iterate
        scheduler.connect_blocks(
            leader_block_id,
            iterate_block_id,
            TypeId::of::<StateFeedback<State>>(),
        );
        // connect the feedback
        scheduler.connect_blocks(feedback_end_block_id, iterate_block_id, TypeId::of::<Out>());
        // connect the output stream
        scheduler.connect_blocks_fragile(iterate_block_id, output_block_id, TypeId::of::<Out>());
        drop(env);

        // store the id of the blocks we now know
        shared_delta_update_end_block_id
            .store(state_update_end_block_id as usize, Ordering::Release);
        shared_feedback_end_block_id.store(feedback_end_block_id as usize, Ordering::Release);
        shared_output_block_id.store(output_block_id as usize, Ordering::Release);

        // TODO: check parallelism and make sure the blocks are spawned on the same replicas

        // FIXME: this add_block is here just to make sure that the NextStrategy of output_stream
        //        is not changed by the following operators. This because the next strategy affects
        //        the connections made by the scheduler and if accidentally set to OnlyOne will
        //        break the connections.
        (
            leader_stream.split_block(End::new, NextStrategy::random()),
            output,
        )
    }
}

impl<Out: ExchangeData, State: ExchangeData + Sync> Source<Out> for Iterate<Out, State> {
    fn replication(&self) -> Replication {
        Replication::Unlimited
    }
}
