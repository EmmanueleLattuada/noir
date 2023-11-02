use std::cmp::Ordering;
use std::collections::VecDeque;
use std::fmt::Display;

use serde::{Serialize, Deserialize};

use crate::block::{BlockStructure, OperatorStructure};
use crate::network::OperatorCoord;
use crate::operator::{ExchangeData, Operator, StreamElement, Timestamp};
#[cfg(feature = "persist-state")]
use crate::persistency::persistency_service::PersistencyService;
use crate::scheduler::ExecutionMetadata;
use crate::scheduler::OperatorId;
#[cfg(feature = "persist-state")]
use super::SnapshotId;


#[derive(Clone, Serialize, Deserialize)]
struct TimestampedItem<Out> {
    item: Out,
    timestamp: Timestamp,
}

impl<Out> Ord for TimestampedItem<Out> {
    fn cmp(&self, other: &Self) -> Ordering {
        self.timestamp.cmp(&other.timestamp)
    }
}

impl<Out> PartialOrd for TimestampedItem<Out> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl<Out> Eq for TimestampedItem<Out> {}

impl<Out> PartialEq for TimestampedItem<Out> {
    fn eq(&self, other: &Self) -> bool {
        self.timestamp == other.timestamp
    }
}

#[derive(Clone)]
pub(crate) struct Reorder<Out: ExchangeData, PreviousOperators>
where
    PreviousOperators: Operator<Out>,
{
    buffer: VecDeque<TimestampedItem<Out>>,
    // Scratch memory used for glidesort
    scratch: Vec<TimestampedItem<Out>>,
    last_watermark: Option<Timestamp>,
    prev: PreviousOperators,
    operator_coord: OperatorCoord,
    #[cfg(feature = "persist-state")]
    persistency_service: Option<PersistencyService<ReorderState<Out>>>,
    received_end: bool,
}

impl<Out: ExchangeData, PreviousOperators> Display for Reorder<Out, PreviousOperators>
where
    PreviousOperators: Operator<Out>,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{} -> Reorder<{}>",
            self.prev,
            std::any::type_name::<Out>(),
        )
    }
}

impl<Out: ExchangeData, PreviousOperators> Reorder<Out, PreviousOperators>
where
    PreviousOperators: Operator<Out>,
{
    pub(crate) fn new(prev: PreviousOperators) -> Self {
        let op_id = prev.get_op_id() + 1;
        Self {
            buffer: Default::default(),
            scratch: Default::default(),
            last_watermark: None,
            prev,
            // This will be set in setup method
            operator_coord: OperatorCoord::new(0, 0, 0, op_id),
            #[cfg(feature = "persist-state")]
            persistency_service: None,
            received_end: false,
        }
    }

    /// Save state for snapshot
    #[cfg(feature = "persist-state")]
    fn save_snap(&mut self, snapshot_id: SnapshotId){
         let state = ReorderState {
            buffer: self.buffer.clone(),
            scratch: self.scratch.clone(),
            last_watermark: self.last_watermark,
            received_end: self.received_end,
        };
        self.persistency_service.as_mut().unwrap().save_state(self.operator_coord, snapshot_id, state);
    }
    /// Save terminated state
    #[cfg(feature = "persist-state")]
    fn save_terminate(&mut self){
         let state = ReorderState {
            buffer: self.buffer.clone(),
            scratch: self.scratch.clone(),
            last_watermark: self.last_watermark,
            received_end: self.received_end,
        };
        self.persistency_service.as_mut().unwrap().save_terminated_state(self.operator_coord, state);
    }
}

#[cfg(feature = "persist-state")]
#[derive(Clone, Serialize, Deserialize)]
struct ReorderState<O> {
    buffer: VecDeque<TimestampedItem<O>>,
    scratch: Vec<TimestampedItem<O>>,
    last_watermark: Option<Timestamp>,
    received_end: bool,
}

impl<Out: ExchangeData, PreviousOperators> Operator<Out> for Reorder<Out, PreviousOperators>
where
    PreviousOperators: Operator<Out>,
{
    fn setup(&mut self, metadata: &mut ExecutionMetadata) {
        self.prev.setup(metadata);

        self.operator_coord.setup_coord(metadata.coord);
        #[cfg(feature = "persist-state")]
        if let Some(pb) = metadata.persistency_builder{
            let p_service = pb.generate_persistency_service::<ReorderState<Out>>();
            let snapshot_id = p_service.restart_from_snapshot(self.operator_coord);
            if let Some(restart_snap) = snapshot_id {
                // Get and resume the persisted state
                let opt_state: Option<ReorderState<Out>> = p_service.get_state(self.operator_coord, restart_snap);
                if let Some(state) = opt_state {
                    self.buffer = state.buffer;
                    self.scratch = state.scratch;
                    self.last_watermark = state.last_watermark;
                    self.received_end = state.received_end;
                } else {
                    panic!("No persisted state founded for op: {0}", self.operator_coord);
                } 
            }
            self.persistency_service = Some(p_service);
        }
    }

    #[inline]
    fn next(&mut self) -> StreamElement<Out> {
        while !self.received_end && self.last_watermark.is_none() {
            match self.prev.next() {
                element @ StreamElement::Item(_) => return element,
                StreamElement::Timestamped(item, timestamp) => {
                    self.buffer.push_back(TimestampedItem { item, timestamp })
                }
                StreamElement::Watermark(ts) => {
                    self.last_watermark = Some(ts);
                    glidesort::sort_with_vec(self.buffer.make_contiguous(), &mut self.scratch);
                }
                StreamElement::FlushBatch => return StreamElement::FlushBatch,
                StreamElement::FlushAndRestart => {
                    self.received_end = true;
                    glidesort::sort_with_vec(self.buffer.make_contiguous(), &mut self.scratch);
                }
                StreamElement::Terminate => {
                    #[cfg(feature = "persist-state")]
                    if self.persistency_service.is_some(){
                        self.save_terminate();
                    }
                    return StreamElement::Terminate;
                }
                #[cfg(feature = "persist-state")]
                StreamElement::Snapshot(snap_id) => {
                    self.save_snap(snap_id.clone());
                    return StreamElement::Snapshot(snap_id);
                }
            }
        }

        if let Some(w) = self.last_watermark {
            match self.buffer.front() {
                Some(front) if front.timestamp <= w => {
                    let element = self.buffer.pop_front().unwrap();
                    StreamElement::Timestamped(element.item, element.timestamp)
                }
                _ => StreamElement::Watermark(self.last_watermark.take().unwrap()),
            }
        } else {
            // here we know that received_end must be true, otherwise we wouldn't have exited the loop
            match self.buffer.pop_front() {
                // pop remaining elements in the heap
                Some(element) => StreamElement::Timestamped(element.item, element.timestamp),
                None => {
                    self.received_end = false;
                    StreamElement::FlushAndRestart
                }
            }
        }
    }

    fn structure(&self) -> BlockStructure {
        let mut operator = OperatorStructure::new::<Out, _>("Reorder");
        let op_id = self.operator_coord.operator_id;
        operator.subtitle = format!("op id: {op_id}");
        self.prev
            .structure()
            .add_operator(operator)
    }

    fn get_op_id(&self) -> OperatorId {
        self.operator_coord.operator_id
    }

    #[cfg(feature = "persist-state")]
    fn get_stateful_operators(&self) -> Vec<OperatorId> {
        let mut res = self.prev.get_stateful_operators();
        // This operator is stateful
        res.push(self.operator_coord.operator_id);
        res
    }
}

#[cfg(test)]
mod tests {
    use crate::operator::reorder::Reorder;
    use crate::operator::{Operator, StreamElement};
    use crate::test::FakeOperator;

    #[cfg(feature = "timestamp")]
    #[test]
    fn reorder() {
        let mut fake = FakeOperator::empty();
        for i in &[1, 3, 2] {
            fake.push(StreamElement::Timestamped(*i, *i));
        }
        fake.push(StreamElement::Watermark(3));
        for i in &[6, 4, 5] {
            fake.push(StreamElement::Timestamped(*i, *i));
        }
        fake.push(StreamElement::FlushAndRestart);
        fake.push(StreamElement::Terminate);

        let mut reorder = Reorder::new(fake);

        for i in 1..=6 {
            assert_eq!(reorder.next(), StreamElement::Timestamped(i, i));
            if i == 3 {
                assert_eq!(reorder.next(), StreamElement::Watermark(3));
            }
        }

        assert_eq!(reorder.next(), StreamElement::FlushAndRestart);
        assert_eq!(reorder.next(), StreamElement::Terminate);
    }
}
