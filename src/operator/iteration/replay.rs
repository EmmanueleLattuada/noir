use std::any::TypeId;
use std::fmt::Display;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use serde::{Serialize, Deserialize};

use crate::block::{BlockStructure, NextStrategy, OperatorReceiver, OperatorStructure, Block};
use crate::environment::StreamEnvironmentInner;
use crate::network::OperatorCoord;
use crate::operator::end::End;
use crate::operator::iteration::iteration_end::IterationEnd;
use crate::operator::iteration::leader::IterationLeader;
use crate::operator::iteration::state_handler::IterationStateHandler;
use crate::operator::iteration::{
    IterationResult, IterationStateHandle, IterationStateLock, StateFeedback,
};
use crate::operator::{ExchangeData, Operator, StreamElement, SnapshotId, Start, SimpleStartReceiver};
use crate::persistency::persistency_service::PersistencyService;
use crate::scheduler::{BlockId, ExecutionMetadata, OperatorId};
use crate::stream::Stream;

/// This is the first operator of the chain of blocks inside an iteration.
///
/// If a new iteration should start, the initial dataset is replayed.
#[derive(Debug, Clone)]
pub struct Replay<Out: ExchangeData, State: ExchangeData>
{
    /// The coordinate of this operator.
    operator_coord: OperatorCoord,

    /// Helper structure that manages the iteration's state.
    state: IterationStateHandler<State>,

    /// The chain of previous operators where the dataset to replay is read from.
    prev: Start<Out, SimpleStartReceiver<Out>>,

    /// The content of the stream to replay.
    content: Vec<StreamElement<Out>>,
    /// The index inside `content` of the first message to be sent.
    content_index: usize,

    /// Whether the input stream has ended or not.
    input_finished: bool,

    /// Persistency service
    persistency_service: Option<PersistencyService<ReplayState<Out, State>>>,
    /// Pending snapshot
    pending_snapshot: Option<SnapshotId>,
    /// State recovered from snaphot
    recovered_state: Option<State>,
    /// Level of this iterator
    iter_stack_level: usize,

    should_flush: bool,
}

#[derive(Clone, Serialize, Deserialize, Debug)]
struct ReplayState<Out, State> {
    state: State,
    content: Vec<StreamElement<Out>>,
    content_index: u64,
    // maybe this is unecessary
    input_finished: bool,
}

impl<Out: ExchangeData, State: ExchangeData> Display for Replay<Out, State>
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{} -> Replay<{}>",
            self.prev,
            std::any::type_name::<Out>()
        )
    }
}

impl<Out: ExchangeData, State: ExchangeData> Replay<Out, State>
{
    fn new(
        prev: Start<Out, SimpleStartReceiver<Out>>,
        state_ref: IterationStateHandle<State>,
        leader_block_id: BlockId,
        state_lock: Arc<IterationStateLock>,
        iter_stack_level: usize
    ) -> Self {
        let op_id = prev.get_op_id() + 1;
        Self {
            // these fields will be set inside the `setup` method
            operator_coord: OperatorCoord::new(0 , 0, 0, op_id),
            persistency_service: None,

            prev,
            
            content: Default::default(),
            content_index: 0,
            input_finished: false,
            state: IterationStateHandler::new(leader_block_id, state_ref, state_lock),
            pending_snapshot: None,
            recovered_state: None,
            iter_stack_level,
            should_flush: false,
        }
    }

    fn input_next(&mut self) -> Option<StreamElement<Out>> {
        if self.input_finished {
            return None;
        }

        let item = match self.prev.next() {
            StreamElement::FlushAndRestart => {
                log::debug!(
                    "Replay at {} received all the input: {} elements total",
                    self.operator_coord.get_coord(),
                    self.content.len()
                );
                self.input_finished = true;
                self.content.push(StreamElement::FlushAndRestart);
                // the first iteration has already happened
                self.content_index = self.content.len();
                // since this moment accessing the state for the next iteration must wait
                self.state.lock();
                StreamElement::FlushAndRestart
            }
            // messages to save for the replay
            el @ StreamElement::Item(_)
            | el @ StreamElement::Timestamped(_, _)
            | el @ StreamElement::Watermark(_) => {
                self.content.push(el.clone());
                el
            }

            // forward snapshot marker but do not put in the queue
            StreamElement::Snapshot(mut snap_id) => {
                //fix snapshot id: iter_stack should be of the same lenght of this iteration level
                while snap_id.iteration_stack.len() < self.iter_stack_level {
                    // put 0 at each missing level
                    snap_id.iteration_stack.push(0);
                }
                //save state
                let state = ReplayState {
                    state: self.state.state_ref.get().clone(),
                    content: self.content.clone(),
                    content_index: self.content_index as u64,
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
                StreamElement::Snapshot(snap_id)
            }

            // messages to forward without replaying
            StreamElement::FlushBatch => StreamElement::FlushBatch,
            StreamElement::Terminate => {
                log::debug!("Replay at {} is terminating", self.operator_coord.get_coord());
                if self.persistency_service.is_some() {
                    let state = ReplayState {
                        state: self.state.state_ref.get().clone(),
                        content: self.content.clone(),
                        content_index: self.content_index as u64,
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
                StreamElement::Terminate
            }
        };
        Some(item)
    }

    fn wait_update(&mut self) -> StateFeedback<State> {
        let state_receiver = self.state.state_receiver().unwrap();
        // TODO: check if affected by deadlock like iterate was in commit eb481da525850febe7cfb0963c6f3285252ecfaa
        // If there is the possibility of input staying still in the channel
        // waiting for the state, the iteration may deadlock
        // to solve instead of blocking on the state receiver,
        // a select must be performed allowing inputs to be stashed and
        // be pulled off the channel
        loop {
            let message = state_receiver.recv().unwrap();
            assert!(message.num_items() == 1);

            match message.into_iter().next().unwrap() {
                StreamElement::Item((should_continue, new_state, opt_snap)) => {
                    return (should_continue, new_state, opt_snap);
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

impl<Out: ExchangeData, State: ExchangeData + Sync> Operator<Out>
    for Replay<Out, State>
{
    fn setup(&mut self, metadata: &mut ExecutionMetadata) {
        self.operator_coord.from_coord(metadata.coord);
        self.prev.setup(metadata);
        self.state.setup(metadata);

        if let Some(pb) = metadata.persistency_builder {
            let p_service = pb.generate_persistency_service::<ReplayState<Out, State>>();
            let snapshot_id = p_service.restart_from_snapshot(self.operator_coord);
            if let Some(snap_id) = snapshot_id {
                if snap_id.iteration_stack.last() != Some(&0) && !snap_id.terminate() {
                    self.should_flush = true;
                }
                // Get and resume the persisted state
                let opt_state: Option<ReplayState<Out, State>> = p_service.get_state(self.operator_coord, snap_id.clone());
                if let Some(state) = opt_state {
                    self.recovered_state = Some(state.state.clone());
                    self.content = state.content.clone();
                    self.content_index = state.content_index as usize;
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
        if let Some(r_state) = self.recovered_state.take(){
            let recovery_update = (IterationResult::Continue, r_state, None);
            self.state.lock();
            self.state.wait_sync_state(recovery_update);
        }
        // if i restart from a snap with 0 at this level of iter stack i need to flush all input
        if self.should_flush {
            loop {
                match self.prev.next() {
                    StreamElement::FlushAndRestart => {
                        break;
                    }
                    _ => {}
                }
            }
            self.should_flush = false;
        }
        loop {
            // Snapshot if required
            // The snapshot must be done at the start of the iteration
            if let Some(snap_id) = self.pending_snapshot.take() {
                //save state
                let state = ReplayState {
                    state: self.state.state_ref.get().clone(),
                    content: self.content.clone(),
                    content_index: self.content_index as u64,
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
            if let Some(value) = self.input_next() {
                return value;
            }
            // replay

            // this iteration has not ended yet
            if self.content_index < self.content.len() {
                let item = self.content[self.content_index].clone();
                self.content_index += 1;
                if matches!(item, StreamElement::FlushAndRestart) {
                    // since this moment accessing the state for the next iteration must wait
                    self.state.lock();
                }
                return item;
            }

            log::debug!("Replay at {} has ended the iteration", self.operator_coord.get_coord());

            self.content_index = 0;

            let state_update = self.wait_update();

            // Get snapshot id to be used at next iteration snapshot
            self.pending_snapshot = state_update.2.clone();

            if let IterationResult::Finished = self.state.wait_sync_state(state_update) {
                log::debug!("Replay block at {} ended the iteration", self.operator_coord.get_coord());
                // cleanup so that if this is a nested iteration next time we'll be good to start again
                self.content.clear();
                self.input_finished = false;
            }

            // This iteration has ended but FlushAndRestart has already been sent. To avoid sending
            // twice the FlushAndRestart repeat.
        }
    }

    fn structure(&self) -> BlockStructure {
        let mut operator = OperatorStructure::new::<Out, _>("Replay");
        let op_id = self.operator_coord.operator_id;
        operator.subtitle = format!("op id: {op_id}");
        operator
            .receivers
            .push(OperatorReceiver::new::<StateFeedback<State>>(
                self.state.leader_block_id,
            ));
        self.prev.structure().add_operator(operator)
    }

    fn get_op_id(&self) -> OperatorId {
        self.operator_coord.operator_id
    }

    fn get_stateful_operators(&self) -> Vec<OperatorId> {
        let mut res = self.prev.get_stateful_operators();
        // This operator is stateful
        res.push(self.operator_coord.operator_id);
        res
    }
}

impl<Out: ExchangeData, OperatorChain> Stream<Out, OperatorChain>
where
    OperatorChain: Operator<Out> + 'static,
{
    /// Construct an iterative dataflow where the input stream is repeatedly fed inside a cycle,
    /// i.e. what comes into the cycle is _replayed_ at every iteration.
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
    /// This construct produces a single stream with a single element: the final state of the
    /// iteration.
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
    /// let state = s.replay(
    ///     3, // at most 3 iterations
    ///     0, // the initial state is zero
    ///     |s, state| s.map(|n| n + 10),
    ///     |delta: &mut i32, n| *delta += n,
    ///     |state, delta| *state += delta,
    ///     |_state| true,
    /// );
    /// let state = state.collect_vec();
    /// env.execute_blocking();
    ///
    /// assert_eq!(state.get().unwrap(), vec![3 * (10 + 11 + 12)]);
    /// ```
    pub fn replay<
        Body,
        DeltaUpdate: ExchangeData + Default,
        State: ExchangeData + Sync,
        OperatorChain2,
    >(
        self,
        num_iterations: usize,
        initial_state: State,
        body: Body,
        local_fold: impl Fn(&mut DeltaUpdate, Out) + Send + Clone + 'static,
        global_fold: impl Fn(&mut State, DeltaUpdate) + Send + Clone + 'static,
        loop_condition: impl Fn(&mut State) -> bool + Send + Clone + 'static,
    ) -> Stream<State, impl Operator<State>>
    where
        Body: FnOnce(
            Stream<Out, Replay<Out, State>>,
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
        let feedback_block_id = Arc::new(AtomicUsize::new(0));

        // Compute iteration stack level
        let iter_stack_level = self.block.iteration_ctx().len() + 1;
        
        let leader = IterationLeader::new(
            initial_state,
            num_iterations,
            global_fold,
            loop_condition,
            feedback_block_id.clone(),
            iter_stack_level,
        );
        let mut output_stream = StreamEnvironmentInner::stream(
            env.clone(),
            leader,
        );
        let leader_block_id = output_stream.block.id;
        // the output stream is outside this loop, so it doesn't have the lock for this state
        output_stream.block.iteration_ctx = self.block.iteration_ctx.clone();

        // the lock for synchronizing the access to the state of this iteration
        let state_lock = Arc::new(IterationStateLock::default());

        let batch_mode = self.block.batch_mode;
        let input_block_id;
        let iter_ctx = self.block.iteration_ctx.clone();

        // This change
        let mut input_block = None;
        let mut allign_block = None;

        // Add block to allign snapshots tokens if this is not a nested iteration
        // and if persistency is configured (to avoid unnecessary overhead)
        let persistency = {
            let env = env.lock();
            env.config.persistency_configuration.is_some()
        };
        if self.block.iteration_ctx().len() == 0 && persistency {
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

        let replay_block_id = {
            let mut env = env.lock();
            env.new_block_id()
        };
        let replay_source = Start::single(
            input_block_id,
            iter_ctx.last().cloned(),
            iter_ctx.len(),
        );

        let rep_start = Stream {
            block: Block::new(
                replay_block_id,
                replay_source,
                batch_mode,
                iter_ctx,
            ),
            env: env.clone(),
        };       
        
        let mut iter_start =
            rep_start.add_operator(|prev| Replay::new(prev, state, leader_block_id, state_lock.clone(), iter_stack_level));
        let replay_block_id = iter_start.block.id;

        // save the stack of the iteration for checking the stream returned by the body
        iter_start.block.iteration_ctx.push(state_lock);
        let pre_iter_stack = iter_start.block.iteration_ctx();

        let mut iter_end = body(iter_start, state_clone)
            .key_by(|_| ())
            .fold(DeltaUpdate::default(), local_fold)
            .drop_key();

        let post_iter_stack = iter_end.block.iteration_ctx();
        if pre_iter_stack != post_iter_stack {
            panic!("The body of the iteration should return the stream given as parameter");
        }
        iter_end.block.iteration_ctx.pop().unwrap();

        let iter_end = iter_end.add_operator(|prev| IterationEnd::new(prev, leader_block_id));
        let iteration_end_block_id = iter_end.block.id;

        let mut env = iter_end.env.lock();
        let scheduler = env.scheduler_mut();
        scheduler.schedule_block(iter_end.block);
        if input_block.is_some() {
            scheduler.schedule_block(input_block.unwrap());
        } else {
            scheduler.schedule_block(allign_block.unwrap());
        }
        scheduler.connect_blocks(input_block_id, replay_block_id, TypeId::of::<Out>());
        // connect the IterationEnd to the IterationLeader
        scheduler.connect_blocks(
            iteration_end_block_id,
            leader_block_id,
            TypeId::of::<DeltaUpdate>(),
        );
        scheduler.connect_blocks(
            leader_block_id,
            replay_block_id,
            TypeId::of::<StateFeedback<State>>(),
        );
        drop(env);

        // store the id of the block containing the IterationEnd
        feedback_block_id.store(iteration_end_block_id as usize, Ordering::Release);

        // TODO: check parallelism and make sure the blocks are spawned on the same replicas

        // FIXME: this add_block is here just to make sure that the NextStrategy of output_stream
        //        is not changed by the following operators. This because the next strategy affects
        //        the connections made by the scheduler and if accidentally set to OnlyOne will
        //        break the connections.
        output_stream.split_block(End::new, NextStrategy::random())
    }
}
