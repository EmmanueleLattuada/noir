use std::collections::{HashMap, VecDeque};
use std::fmt::Display;
use std::hash::Hash;

use serde::{Serialize, Deserialize};

use crate::block::{BlockStructure, OperatorStructure};
use crate::network::OperatorCoord;
use crate::operator::merge::MergeElement;

use crate::operator::{ExchangeData, ExchangeDataKey, Operator, StreamElement, Timestamp};
use crate::persistency::{PersistencyService, PersistencyServices};
use crate::scheduler::{ExecutionMetadata, OperatorId};

type OutputElement<Key, Out, Out2> = (Key, (Out, Out2));

/// Operator that performs an interval join.
///
/// Given a `lower_bound` and an `upper_bound` duration, each element of the left side with
/// timestamp `ts` is matched with each element of the right side that has timestamp inside the
/// interval `ts - lower_bound` and `ts + upper_bound` (inclusive).
///
/// This operator assumes elements are received in increasing order of timestamp.
#[derive(Clone, Debug)]
pub struct IntervalJoin<Key, Out, Out2, OperatorChain>
where
    Key: ExchangeDataKey,
    Out: ExchangeData,
    Out2: ExchangeData,
    OperatorChain: Operator<(Key, MergeElement<Out, Out2>)>,
{
    prev: OperatorChain,
    operator_coord: OperatorCoord,
    persistency_service: PersistencyService,
    /// Elements of the left side to be processed.
    left: VecDeque<(Timestamp, (Key, Out))>,
    /// Elements of the right side that might still be matched.
    right: HashMap<Key, VecDeque<(Timestamp, Out2)>, crate::block::GroupHasherBuilder>,
    /// Elements ready to be sent downstream.
    buffer: VecDeque<(Timestamp, OutputElement<Key, Out, Out2>)>,
    /// Timestamp of the last element (item or watermark).
    last_seen: Timestamp,
    /// Upper bound duration of the interval.
    upper_bound: Timestamp,
    /// Lower bound duration of the interval.
    lower_bound: Timestamp,
    /// Whether the operator has received a `FlushAndRestart` message.
    received_restart: bool,
}

impl<Key, Out, Out2, OperatorChain> Display for IntervalJoin<Key, Out, Out2, OperatorChain>
where
    Key: ExchangeDataKey,
    Out: ExchangeData,
    Out2: ExchangeData,
    OperatorChain: Operator<(Key, MergeElement<Out, Out2>)>,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{} -> IntervalJoin<{}, {:?}, {:?}>",
            self.prev,
            std::any::type_name::<(Key, (Out, Out2))>(),
            self.lower_bound,
            self.upper_bound,
        )
    }
}

impl<Key, Out, Out2, OperatorChain> IntervalJoin<Key, Out, Out2, OperatorChain>
where
    Key: ExchangeDataKey,
    Out: ExchangeData,
    Out2: ExchangeData,
    OperatorChain: Operator<(Key, MergeElement<Out, Out2>)>,
{
    pub(super) fn new(prev: OperatorChain, lower_bound: Timestamp, upper_bound: Timestamp) -> Self {
        let op_id = prev.get_op_id() + 1;
        Self {
            prev,
            // This will be set in setup method
            operator_coord: OperatorCoord::new(0, 0, 0, op_id),

            persistency_service: PersistencyService::default(),
            left: Default::default(),
            right: Default::default(),
            buffer: Default::default(),
            last_seen: Default::default(),
            upper_bound,
            lower_bound,
            received_restart: false,
        }
    }

    /// Advance the operator, trying to generate some join tuples.
    fn advance(&mut self) {
        while let Some((left_ts, (lkey, lvalue))) = self.left.front() {
            // find lower and upper limits of the interval
            let lower = left_ts
                .checked_sub(self.lower_bound)
                .unwrap_or(Timestamp::MIN);
            let upper = left_ts
                .checked_add(self.upper_bound)
                .unwrap_or(Timestamp::MAX);

            if upper >= self.last_seen && !self.received_restart {
                // there could be some elements in the interval that have not been received yet
                break;
            }

            if let Some(right) = self.right.get_mut(lkey) {
                // Remove elements of the right side that are not going to be matched anymore.
                // This happens when the timestamp of the right element is less than the lower bound
                // of the current interval, since elements of the left side are ordered by ascending
                // timestamp and the lower bound can only increase.
                while let Some((right_ts, _)) = right.front() {
                    if *right_ts < lower {
                        right.pop_front();
                    } else {
                        break;
                    }
                }

                // generate all the join tuples
                let matches = right
                    .iter()
                    .take_while(|(right_ts, _)| *right_ts <= upper)
                    .map(|(right_ts, rvalue)| {
                        let ts = right_ts.max(left_ts);
                        let item = (lkey.clone(), (lvalue.clone(), rvalue.clone()));
                        (*ts, item)
                    });

                // add the generated tuples to the output buffer
                self.buffer.extend(matches);
            }

            // remove the element of the left side
            self.left.pop_front();
        }

        if self.left.is_empty() && self.received_restart {
            // the operator has received a `FlushAndRestart` message and there are no elements
            // remaining in the left side, so we can clear also the right side
            self.right.clear();
        }
    }
}


#[derive(Debug, Clone, Serialize, Deserialize)]
struct IntervalJoinState<K: Hash + Eq, O, O2> {
    /// Elements of the left side to be processed.
    left: VecDeque<(Timestamp, (K, O))>,
    /// Elements of the right side that might still be matched.
    right: HashMap<K, VecDeque<(Timestamp, O2)>, crate::block::GroupHasherBuilder>,
    /// Elements ready to be sent downstream.
    buffer: VecDeque<(Timestamp, OutputElement<K, O, O2>)>,
    /// Timestamp of the last element (item or watermark).
    last_seen: Timestamp,
    /// Whether the operator has received a `FlushAndRestart` message.
    received_restart: bool,
}


impl<Key, Out, Out2, OperatorChain> Operator<(Key, (Out, Out2))>
    for IntervalJoin<Key, Out, Out2, OperatorChain>
where
    Key: ExchangeDataKey,
    Out: ExchangeData,
    Out2: ExchangeData,
    OperatorChain: Operator<(Key, MergeElement<Out, Out2>)>,
{
    fn setup(&mut self, metadata: &mut ExecutionMetadata) {
        self.prev.setup(metadata);

        self.operator_coord.block_id = metadata.coord.block_id;
        self.operator_coord.host_id = metadata.coord.host_id;
        self.operator_coord.replica_id = metadata.coord.replica_id;

        self.persistency_service = metadata.persistency_service.clone();
        let snapshot_id = self.persistency_service.restart_from_snapshot(self.operator_coord);
        if snapshot_id.is_some() {
            // Get and resume the persisted state
            let opt_state: Option<IntervalJoinState<Key, Out, Out2>> = self.persistency_service.get_state(self.operator_coord, snapshot_id.unwrap());
            if let Some(state) = opt_state {
                self.left = state.left;
                self.right = state.right;
                self.buffer = state.buffer;
                self.last_seen = state.last_seen;
                self.received_restart = state.received_restart;
            } else {
                panic!("No persisted state founded for op: {0}", self.operator_coord);
            } 
        }
    }

    fn next(&mut self) -> StreamElement<(Key, (Out, Out2))> {
        while self.buffer.is_empty() {
            if self.received_restart {
                assert!(self.left.is_empty());
                assert!(self.right.is_empty());

                self.received_restart = false;
                self.last_seen = Default::default();

                return StreamElement::FlushAndRestart;
            }

            match self.prev.next() {
                StreamElement::Timestamped((key, item), ts) => {
                    assert!(ts >= self.last_seen);
                    self.last_seen = ts;
                    match item {
                        MergeElement::Left(item) => self.left.push_back((ts, (key, item))),
                        MergeElement::Right(item) => {
                            self.right.entry(key).or_default().push_back((ts, item))
                        }
                    }
                }
                StreamElement::Watermark(ts) => {
                    assert!(ts >= self.last_seen);
                    self.last_seen = ts;
                }
                StreamElement::FlushAndRestart => {
                    self.received_restart = true;
                }
                StreamElement::Item(_) => panic!("Interval Join only supports timestamped streams"),
                StreamElement::FlushBatch => return StreamElement::FlushBatch,
                StreamElement::Terminate => {
                    if self.persistency_service.is_active() {
                        // Save terminated state
                        let state = IntervalJoinState {
                            left: self.left.clone(),
                            right: self.right.clone(),
                            buffer: self.buffer.clone(),
                            last_seen: self.last_seen,
                            received_restart: self.received_restart,
                        };
                        self.persistency_service.save_terminated_state(self.operator_coord, state);
                    }
                    return StreamElement::Terminate
                }
                StreamElement::Snapshot(snap_id) => {
                    // Save state and forward marker
                    let state = IntervalJoinState {
                        left: self.left.clone(),
                        right: self.right.clone(),
                        buffer: self.buffer.clone(),
                        last_seen: self.last_seen,
                        received_restart: self.received_restart,
                    };
                    self.persistency_service.save_state(self.operator_coord, snap_id, state);
                    return StreamElement::Snapshot(snap_id);
                }
            }

            self.advance();
        }

        let (ts, item) = self.buffer.pop_front().unwrap();
        StreamElement::Timestamped(item, ts)
    }

    fn structure(&self) -> BlockStructure {
        let mut operator = OperatorStructure::new::<(Key, (Out, Out2)), _>(
            "IntervalJoin",
        );
        let op_id = self.operator_coord.operator_id;
        operator.subtitle = format!("op id: {op_id}");
        self.prev
            .structure()
            .add_operator(operator)
    }

    fn get_op_id(&self) -> OperatorId {
        self.operator_coord.operator_id
    }
}
