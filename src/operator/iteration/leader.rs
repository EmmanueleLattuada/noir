use std::fmt::Display;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;


use serde::{Serialize, Deserialize};

use crate::block::{BlockStructure, Connection, NextStrategy, OperatorStructure, Replication};
use crate::network::{NetworkMessage, NetworkSender, OperatorCoord};
use crate::operator::iteration::{IterationResult, StateFeedback};
use crate::operator::source::{Source, SnapshotGenerator};
use crate::operator::start::{SimpleStartOperator, Start, StartReceiver};
use crate::operator::{ExchangeData, Operator, StreamElement, SnapshotId};
use crate::persistency::persistency_service::PersistencyService;
use crate::profiler::{get_profiler, Profiler};
use crate::scheduler::{BlockId, ExecutionMetadata, OperatorId};

/// The leader block of an iteration.
///
/// This block is the synchronization point for the distributed iterations. At the end of each
/// iteration the `IterationEnd` will send a `DeltaUpdate` each to this block. When all the
/// `DeltaUpdate`s are received the new state is computed and the loop condition is evaluated.
///
/// After establishing if a new iteration should start this block will send a message to all the
/// "iteration blocks" (i.e. the blocks that manage the body of the iteration). When all the
/// iterations have been completed this block will produce the final state of the iteration,
/// followed by a `StreamElement::FlushAndReset`.
#[derive(Derivative)]
#[derivative(Clone, Debug)]
pub struct IterationLeader<StateUpdate: ExchangeData, State: ExchangeData, Global, LoopCond>
where
    Global: Fn(&mut State, StateUpdate) + Send + Clone,
    LoopCond: Fn(&mut State) -> bool + Send + Clone,
{
    /// The coordinates of this operator.
    operator_coord: OperatorCoord,

    /// The index of the current iteration (0-based).
    iteration_index: usize,
    /// The maximum number of iterations to perform.
    max_iterations: usize,

    /// The current global state of the iteration.
    ///
    /// It's an `Options` because the block needs to take ownership of it for processing it. It will
    /// be `None` only during that time frame.
    state: Option<State>,
    /// The initial value of the global state of the iteration.
    ///
    /// Will be used for resetting the global state after all the iterations complete.
    initial_state: State,

    /// The receiver from the `IterationEnd`s at the end of the loop.
    ///
    /// This will be set inside `setup` when we will know the id of that block.
    state_update_receiver: Option<SimpleStartOperator<StateUpdate>>,
    /// The number of replicas of `IterationEnd`.
    num_receivers: usize,
    /// Missing state updates
    missing_state_updates: usize,    
    /// The id of the block where `IterationEnd` is.
    ///
    /// This is a shared reference because when this block is constructed the tail of the iteration
    /// (i.e. the `IterationEnd`) is not constructed yet. Therefore we cannot predict how many
    /// blocks there will be in between, and therefore we cannot know the block id.
    ///
    /// After constructing the entire iteration this shared variable will be set. This will happen
    /// before the call to `setup`.
    feedback_block_id: Arc<AtomicUsize>,
    /// The senders to the start block of the iteration for the information about the new iteration.
    feedback_senders: Vec<NetworkSender<StateFeedback<State>>>,
    /// Whether `next` should emit a `FlushAndRestart` in the next call.
    flush_and_restart: bool,

    /// The function that combines the global state with a delta update.
    #[derivative(Debug = "ignore")]
    global_fold: Global,
    /// A function that, given the global state, checks whether the iteration should continue.
    #[derivative(Debug = "ignore")]
    loop_condition: LoopCond,

    /// Persistency service
    persistency_service: Option<PersistencyService<IterationLeaderState<State>>>, 
    /// Snapshot generator 
    snapshot_generator: SnapshotGenerator,
    /// Max snap id
    max_snap_id: Option<SnapshotId>,
    /// Iteration level
    iter_stack_level: usize,
    /// Pending snapshot
    pending_snapshot: Option<SnapshotId>,
}

#[derive(Clone, Serialize, Deserialize, Debug)]
struct IterationLeaderState<State> {
    state: Option<State>,
    iteration_index: u64,
    flush_and_restart: bool,
    missing_state_updates: u64,
}

impl<DeltaUpdate: ExchangeData, State: ExchangeData, Global, LoopCond> Display
    for IterationLeader<DeltaUpdate, State, Global, LoopCond>
where
    Global: Fn(&mut State, DeltaUpdate) + Send + Clone,
    LoopCond: Fn(&mut State) -> bool + Send + Clone,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "IterationLeader<{}>", std::any::type_name::<State>())
    }
}

impl<DeltaUpdate: ExchangeData, State: ExchangeData, Global, LoopCond>
    IterationLeader<DeltaUpdate, State, Global, LoopCond>
where
    Global: Fn(&mut State, DeltaUpdate) + Send + Clone,
    LoopCond: Fn(&mut State) -> bool + Send + Clone,
{
    pub fn new(
        initial_state: State,
        num_iterations: usize,
        global_fold: Global,
        loop_condition: LoopCond,
        feedback_block_id: Arc<AtomicUsize>,
        iter_stack_level: usize,
    ) -> Self {
        let mut snap_gen = SnapshotGenerator::new();
        snap_gen.set_iter_stack(iter_stack_level);
        Self {
            // these fields will be set inside the `setup` method
            state_update_receiver: None,
            feedback_senders: Default::default(),
            num_receivers: 0,
            missing_state_updates: 0,
            // This the second block in the chain
            operator_coord: OperatorCoord::new(0, 0, 0, 1),
            persistency_service: None,

            max_iterations: num_iterations,
            iteration_index: 0,
            state: Some(initial_state.clone()),
            initial_state,
            feedback_block_id,
            flush_and_restart: false,
            global_fold,
            loop_condition,
            snapshot_generator: snap_gen,
            iter_stack_level,
            max_snap_id: None,
            pending_snapshot: None,
        }
    }

    fn process_update(&mut self) -> Option<StreamElement<State>> {
        let rx = self.state_update_receiver.as_mut().unwrap();
        match rx.next() {
            StreamElement::Item(state_update) => {
                self.missing_state_updates -= 1;
                log::trace!(
                    "iter_leader delta_update {}, {} left",
                    self.operator_coord.get_coord(),
                    self.missing_state_updates
                );
                (self.global_fold)(self.state.as_mut().unwrap(), state_update);
            }
            StreamElement::Terminate => {
                log::trace!("iter_leader terminate {}", self.operator_coord.get_coord());
                if self.persistency_service.is_some() {
                    let state = IterationLeaderState {
                        state: self.state.clone(),
                        iteration_index: self.iteration_index as u64,
                        flush_and_restart: self.flush_and_restart,
                        missing_state_updates: self.missing_state_updates as u64,
                    };
                    self.persistency_service
                        .as_mut()
                        .unwrap()
                        .save_terminated_state(
                            self.operator_coord,
                            state,
                        );
                }
                return Some(StreamElement::Terminate);
            }
            StreamElement::FlushAndRestart | StreamElement::FlushBatch => {}
            StreamElement::Snapshot(mut snap_id) => {
                if self.iteration_index == 0 && snap_id.iteration_stack.last() == Some(&0) {
                    // take the latest snapshot id generated by an operator before the replay                   
                    self.max_snap_id = Some(snap_id.clone());              
                    // save state 
                    let state = IterationLeaderState {
                        state: self.state.clone(),
                        iteration_index: self.iteration_index as u64,
                        flush_and_restart: self.flush_and_restart,
                        missing_state_updates: self.missing_state_updates as u64,
                    };
                    self.persistency_service
                        .as_mut()
                        .unwrap()
                        .save_state(
                            self.operator_coord,
                            snap_id.clone(), 
                            state,
                        );
                    // remove the last 0 in the ire stack before exit the iterator
                    snap_id.iteration_stack.pop();
                    // return marker
                    return Some(StreamElement::Snapshot(snap_id));
                    
                }
            }
            update => unreachable!(
                "IterationLeader received an invalid message: {}",
                update.variant()
            ),
        }
        None
    }   

    /// Returns Some if it's the last loop and the state should be returned
    fn final_result(&mut self) -> Option<State> {
        let loop_condition = (self.loop_condition)(self.state.as_mut().unwrap());
        let more_iterations = self.iteration_index < self.max_iterations;
        let should_continue = loop_condition && more_iterations;

        if !loop_condition {
            log::trace!("iter_leader finish_condition {}", self.operator_coord.get_coord(),);
        }
        if !more_iterations {
            log::trace!("iter_leader finish_max_iter {}", self.operator_coord.get_coord());
        }

        if should_continue {
            None
        } else {
            // reset the global state at the end of the iteration
            let state = self.state.take();
            self.state = Some(self.initial_state.clone());
            state
        }
    }
}

impl<DeltaUpdate: ExchangeData, State: ExchangeData, Global, LoopCond> Operator<State>
    for IterationLeader<DeltaUpdate, State, Global, LoopCond>
where
    Global: Fn(&mut State, DeltaUpdate) + Send + Clone,
    LoopCond: Fn(&mut State) -> bool + Send + Clone,
{
    fn setup(&mut self, metadata: &mut ExecutionMetadata) {
        self.operator_coord.from_coord(metadata.coord);
        self.feedback_senders = metadata
            .network
            .get_senders(metadata.coord)
            .into_iter()
            .map(|(_, s)| s)
            .collect();

        // at this point the id of the block with IterationEnd must be known
        let feedback_block_id = self.feedback_block_id.load(Ordering::Acquire) as BlockId;
        // get the receiver for the delta updates
        let mut delta_update_receiver = Start::single(feedback_block_id, None, self.iter_stack_level);
        delta_update_receiver.setup(metadata);
        self.num_receivers = delta_update_receiver.receiver().prev_replicas().len();
        self.state_update_receiver = Some(delta_update_receiver);
        self.missing_state_updates = self.num_receivers;

        // Setup persistency
        if let Some(pb) = metadata.persistency_builder {
            let p_service = pb.generate_persistency_service::<IterationLeaderState<State>>(); 
            let snapshot_id = p_service.restart_from_snapshot(self.operator_coord);
            if snapshot_id.is_some() {
                // Get and resume the persisted state
                let opt_state: Option<IterationLeaderState<State>> = p_service.get_state(self.operator_coord, snapshot_id.clone().unwrap());
                if let Some(state) = opt_state {
                    self.state = state.state.clone();
                    self.iteration_index = state.iteration_index as usize;
                    self.flush_and_restart = state.flush_and_restart;
                    self.missing_state_updates = state.missing_state_updates as usize;
                } else {
                    panic!("No persisted state founded for op: {0}", self.operator_coord);
                } 
                self.snapshot_generator.restart_from(snapshot_id.unwrap());
            }
            // Set snapshot generator, this will be used starting from the secodn iterations
            // Frequency by item will count the iterations
            if let Some(i) = p_service.snapshot_frequency_by_item{
                self.snapshot_generator.set_item_interval(i);
            }
            if let Some(t) = p_service.snapshot_frequency_by_time{
                self.snapshot_generator.set_time_interval(t);
            }

            self.persistency_service = Some(p_service);
        }

    }

    fn next(&mut self) -> StreamElement<State> {
        if self.flush_and_restart {
            self.flush_and_restart = false;
            return StreamElement::FlushAndRestart;
        }
        loop {
            log::trace!(
                "iter_leader {} {} delta updates left",
                self.operator_coord.get_coord(),
                self.num_receivers
            );
            // Get all state updates
            if self.missing_state_updates > 0 {
                if let Some(value) = self.process_update() {
                    return value;
                } else {
                    continue;
                }
            }
            self.missing_state_updates = self.num_receivers;

            get_profiler().iteration_boundary(self.operator_coord.block_id);
            self.iteration_index += 1;
            let result = self.final_result();

            if self.persistency_service.is_some() {
                if self.iteration_index == 1 {
                    // Initialize the snapshot generator to the max snapshot id receiver
                    if let Some(last_snap) = self.max_snap_id.take() {
                        self.snapshot_generator.restart_from(last_snap);
                    }
                }
                if result.is_none() {
                    // If this is the last iteration i don't try to do snapshot
                    self.pending_snapshot = self.snapshot_generator.get_snapshot_marker();
                }
            }
            
            let state_feedback = (
                IterationResult::from_condition(result.is_none()),
                self.state.clone().unwrap(),
                self.pending_snapshot.clone(),
            );
            for sender in &self.feedback_senders {
                let message = NetworkMessage::new_single(
                    StreamElement::Item(state_feedback.clone()),
                    self.operator_coord.get_coord(),
                );
                sender.send(message).unwrap();
            }

            if let Some(state) = result {
                self.flush_and_restart = true;
                self.iteration_index = 0;
                return StreamElement::Item(state);
            }

            if let Some(snap_id) = self.pending_snapshot.take() {                    
                // save state
                let state = IterationLeaderState {
                    state: self.state.clone(),
                    iteration_index: self.iteration_index as u64,
                    flush_and_restart: self.flush_and_restart,
                    missing_state_updates: self.missing_state_updates as u64,
                };
                self.persistency_service
                    .as_mut()
                    .unwrap()
                    .save_state(
                        self.operator_coord,
                        snap_id.clone(), 
                        state,
                    );
            }
        }
    }

    fn structure(&self) -> BlockStructure {
        let mut operator = OperatorStructure::new::<State, _>("IterationLeader");
        let op_id = self.operator_coord.operator_id;
        operator.subtitle = format!("op id: {op_id}");
        operator
            .connections
            .push(Connection::new::<StateFeedback<State>, _>(
                self.feedback_senders[0].receiver_endpoint.coord.block_id,
                &NextStrategy::only_one(),
            ));
        self.state_update_receiver
            .as_ref()
            .unwrap()
            .structure()
            .add_operator(operator)
    }

    fn get_op_id(&self) -> OperatorId {
        self.operator_coord.operator_id
    }

    fn get_stateful_operators(&self) -> Vec<OperatorId> {
        // This operator is stateful
        let mut res = Vec::new();
        // It will have a Start with op_id = 0 
        res.push(0);
        res.push(self.operator_coord.operator_id);
        res
    }
}

impl<DeltaUpdate: ExchangeData, State: ExchangeData, Global, LoopCond> Source<State>
    for IterationLeader<DeltaUpdate, State, Global, LoopCond>
where
    Global: Fn(&mut State, DeltaUpdate) + Send + Clone,
    LoopCond: Fn(&mut State) -> bool + Send + Clone,
{
    fn replication(&self) -> Replication {
        Replication::One
    }
}
