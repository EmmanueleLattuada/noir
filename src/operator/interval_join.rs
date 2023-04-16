use std::collections::{HashMap, VecDeque};
use std::fmt::Display;

use crate::block::{BlockStructure, OperatorStructure};
use crate::network::OperatorCoord;
use crate::operator::merge::MergeElement;
use crate::operator::reorder::Reorder;
use crate::operator::{ExchangeData, ExchangeDataKey, Operator, StreamElement, Timestamp};
use crate::scheduler::{ExecutionMetadata, OperatorId};
use crate::stream::{KeyValue, KeyedStream, Stream};

type OutputElement<Key, Out, Out2> = KeyValue<Key, (Out, Out2)>;

/// Operator that performs an interval join.
///
/// Given a `lower_bound` and an `upper_bound` duration, each element of the left side with
/// timestamp `ts` is matched with each element of the right side that has timestamp inside the
/// interval `ts - lower_bound` and `ts + upper_bound` (inclusive).
///
/// This operator assumes elements are received in increasing order of timestamp.
#[derive(Clone, Debug)]
struct IntervalJoin<Key, Out, Out2, OperatorChain>
where
    Key: ExchangeDataKey,
    Out: ExchangeData,
    Out2: ExchangeData,
    OperatorChain: Operator<KeyValue<Key, MergeElement<Out, Out2>>>,
{
    prev: OperatorChain,
    operator_coord: OperatorCoord,
    /// Elements of the left side to be processed.
    left: VecDeque<(Timestamp, KeyValue<Key, Out>)>,
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
    OperatorChain: Operator<KeyValue<Key, MergeElement<Out, Out2>>>,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{} -> IntervalJoin<{}, {:?}, {:?}>",
            self.prev,
            std::any::type_name::<KeyValue<Key, (Out, Out2)>>(),
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
    OperatorChain: Operator<KeyValue<Key, MergeElement<Out, Out2>>>,
{
    fn new(prev: OperatorChain, lower_bound: Timestamp, upper_bound: Timestamp) -> Self {
        let op_id = prev.get_op_id() + 1;
        Self {
            prev,
            // This will be set in setup method
            operator_coord: OperatorCoord::new(0, 0, 0, op_id),

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

impl<Key, Out, Out2, OperatorChain> Operator<KeyValue<Key, (Out, Out2)>>
    for IntervalJoin<Key, Out, Out2, OperatorChain>
where
    Key: ExchangeDataKey,
    Out: ExchangeData,
    Out2: ExchangeData,
    OperatorChain: Operator<KeyValue<Key, MergeElement<Out, Out2>>>,
{
    fn setup(&mut self, metadata: &mut ExecutionMetadata) {
        self.prev.setup(metadata);

        self.operator_coord.block_id = metadata.coord.block_id;
        self.operator_coord.host_id = metadata.coord.host_id;
        self.operator_coord.replica_id = metadata.coord.replica_id;
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
                StreamElement::Terminate => return StreamElement::Terminate,
            }

            self.advance();
        }

        let (ts, item) = self.buffer.pop_front().unwrap();
        StreamElement::Timestamped(item, ts)
    }

    fn structure(&self) -> BlockStructure {
        let mut operator = OperatorStructure::new::<KeyValue<Key, (Out, Out2)>, _>(
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

impl<Out: ExchangeData, OperatorChain> Stream<Out, OperatorChain>
where
    OperatorChain: Operator<Out> + 'static,
{
    /// Given two streams **with timestamps** join them according to an interval centered around the
    /// timestamp of the left side.
    ///
    /// This means that an element on the left side with timestamp T will be joined to all the
    /// elements on the right with timestamp Q such that `T - lower_bound <= Q <= T + upper_bound`.
    ///
    /// **Note**: this operator is not parallelized, all the elements are sent to a single node to
    /// perform the join.
    ///
    /// **Note**: this operator will split the current block.
    ///
    /// ## Example
    /// TODO: example
    pub fn interval_join<Out2, OperatorChain2>(
        self,
        right: Stream<Out2, OperatorChain2>,
        lower_bound: Timestamp,
        upper_bound: Timestamp,
    ) -> Stream<(Out, Out2), impl Operator<(Out, Out2)>>
    where
        Out2: ExchangeData,
        OperatorChain2: Operator<Out2> + 'static,
    {
        let left = self.max_parallelism(1);
        let right = right.max_parallelism(1);
        left.merge_distinct(right)
            .key_by(|_| ())
            .add_operator(Reorder::new)
            .add_operator(|prev| IntervalJoin::new(prev, lower_bound, upper_bound))
            .drop_key()
    }
}

impl<Key: ExchangeDataKey, Out: ExchangeData, OperatorChain> KeyedStream<Key, Out, OperatorChain>
where
    OperatorChain: Operator<KeyValue<Key, Out>> + 'static,
{
    /// Given two streams **with timestamps** join them according to an interval centered around the
    /// timestamp of the left side.
    ///
    /// This means that an element on the left side with timestamp T will be joined to all the
    /// elements on the right with timestamp Q such that `T - lower_bound <= Q <= T + upper_bound`.
    /// Only items with the same key can be joined together.
    ///
    /// **Note**: this operator will split the current block.
    ///
    /// ## Example
    /// TODO: example
    pub fn interval_join<Out2, OperatorChain2>(
        self,
        right: KeyedStream<Key, Out2, OperatorChain2>,
        lower_bound: Timestamp,
        upper_bound: Timestamp,
    ) -> KeyedStream<Key, (Out, Out2), impl Operator<KeyValue<Key, (Out, Out2)>>>
    where
        Out2: ExchangeData,
        OperatorChain2: Operator<KeyValue<Key, Out2>> + 'static,
    {
        self.merge_distinct(right)
            .add_operator(Reorder::new)
            .add_operator(|prev| IntervalJoin::new(prev, lower_bound, upper_bound))
    }
}
