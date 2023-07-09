use std::collections::VecDeque;
use std::fmt::Display;
use std::sync::Arc;
use std::time::Duration;

use serde::{Deserialize, Serialize};

use crate::block::{BlockStructure, OperatorReceiver, OperatorStructure, Replication};
use crate::network::OperatorCoord;
use crate::operator::iteration::IterationStateLock;
use crate::operator::start::{BinaryElement, BinaryStartOperator, Start};
use crate::operator::{ExchangeData, Operator, StreamElement};
use crate::persistency::{PersistencyService, PersistencyServices};
use crate::scheduler::{BlockId, ExecutionMetadata, OperatorId};

use super::SnapshotId;
use super::source::Source;

#[derive(Clone)]
pub struct Zip<Out1: ExchangeData, Out2: ExchangeData> {
    prev: BinaryStartOperator<Out1, Out2>,
    operator_coord: OperatorCoord,
    persistency_service: Option<PersistencyService>,
    stash1: VecDeque<StreamElement<Out1>>,
    stash2: VecDeque<StreamElement<Out2>>,
    prev_block_id1: BlockId,
    prev_block_id2: BlockId,
}

impl<Out1: ExchangeData, Out2: ExchangeData> Display for Zip<Out1, Out2> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Zip[{}, {}]",
            std::any::type_name::<Out1>(),
            std::any::type_name::<Out2>()
        )
    }
}

impl<Out1: ExchangeData, Out2: ExchangeData> Zip<Out1, Out2> {
    pub(super) fn new(
        prev_block_id1: BlockId,
        prev_block_id2: BlockId,
        left_cache: bool,
        right_cache: bool,
        state_lock: Option<Arc<IterationStateLock>>,
    ) -> Self {
        Self {
            prev: Start::multiple(
                prev_block_id1,
                prev_block_id2,
                left_cache,
                right_cache,
                state_lock,
            ),
            // Since previous operator is the first in the chain, this will have op_id 1
            // Other fields will be set in setup method
            operator_coord: OperatorCoord::new(0, 0, 0, 1),
            persistency_service: None,
            stash1: Default::default(),
            stash2: Default::default(),
            prev_block_id1,
            prev_block_id2,
        }
    }

    /// Save state for snapshot
    fn save_snap(&mut self, snapshot_id: SnapshotId){
        let state = ZipState {
            stash1: self.stash1.clone(),
            stash2: self.stash2.clone(),
        };
        self.persistency_service.as_mut().unwrap().save_state(self.operator_coord, snapshot_id, state);
    }
    /// Save terminated state
    fn save_terminate(&mut self){
        let state = ZipState {
            stash1: self.stash1.clone(),
            stash2: self.stash2.clone(),
        };
        self.persistency_service.as_mut().unwrap().save_terminated_state(self.operator_coord, state);
    }
}

#[derive(Clone, Serialize, Deserialize)]
struct ZipState<O1, O2> {
    stash1: VecDeque<StreamElement<O1>>,
    stash2: VecDeque<StreamElement<O2>>,
}


impl<Out1: ExchangeData, Out2: ExchangeData> Operator<(Out1, Out2)> for Zip<Out1, Out2> {
    fn setup(&mut self, metadata: &mut ExecutionMetadata) {
        self.prev.setup(metadata);

        self.operator_coord.from_coord(metadata.coord);
        if metadata.persistency_service.is_some(){
            self.persistency_service = metadata.persistency_service.clone();
            let snapshot_id = self.persistency_service.as_mut().unwrap().restart_from_snapshot(self.operator_coord);
            if snapshot_id.is_some() {
                // Get and resume the persisted state
                let opt_state: Option<ZipState<Out1, Out2>> = self.persistency_service.as_mut().unwrap().get_state(self.operator_coord, snapshot_id.unwrap());
                if let Some(state) = opt_state {
                    self.stash1 = state.stash1;
                    self.stash2 = state.stash2;
                } else {
                    panic!("No persisted state founded for op: {0}", self.operator_coord);
                } 
            }
        }
    }

    #[inline]
    fn next(&mut self) -> StreamElement<(Out1, Out2)> {
        while self.stash1.is_empty() || self.stash2.is_empty() {
            let item = self.prev.next();
            match item {
                StreamElement::Item(BinaryElement::Left(left)) => {
                    self.stash1.push_back(StreamElement::Item(left))
                }
                StreamElement::Timestamped(BinaryElement::Left(left), ts) => {
                    self.stash1.push_back(StreamElement::Timestamped(left, ts))
                }
                StreamElement::Item(BinaryElement::Right(right)) => {
                    self.stash2.push_back(StreamElement::Item(right))
                }
                StreamElement::Timestamped(BinaryElement::Right(right), ts) => {
                    self.stash2.push_back(StreamElement::Timestamped(right, ts))
                }
                // ignore LeftEnd | RightEnd
                StreamElement::Item(_) | StreamElement::Timestamped(_, _) => continue,

                // At this point we can emit the watermark safely since all the stashed items will
                // stall until a message from the "other side" is received, and the resulting pair
                // have the max of the two timestamps as timestamp. This timestamp will be for sure
                // bigger than this watermark since the start block will keep the frontier valid.
                StreamElement::Watermark(_) => return item.map(|_| unreachable!()),

                // Both sides are done, we may still have unmatched items in one of the two side.
                // Forget them since the stream ended.
                StreamElement::FlushAndRestart => {
                    self.stash1.clear();
                    self.stash2.clear();
                    return item.map(|_| unreachable!());
                }

                StreamElement::FlushBatch => {
                    return item.map(|_| unreachable!())
                }

                StreamElement::Terminate => {
                    if self.persistency_service.is_some(){
                        self.save_terminate();
                    }
                    return StreamElement::Terminate
                }

                StreamElement::Snapshot(snap_id) => {
                    self.save_snap(snap_id);
                    return StreamElement::Snapshot(snap_id);
                }
            }
        }
        let item1 = self.stash1.pop_front().unwrap();
        let item2 = self.stash2.pop_front().unwrap();
        match (item1, item2) {
            (StreamElement::Item(item1), StreamElement::Item(item2)) => {
                StreamElement::Item((item1, item2))
            }
            (StreamElement::Timestamped(item1, ts1), StreamElement::Timestamped(item2, ts2)) => {
                StreamElement::Timestamped((item1, item2), ts1.max(ts2))
            }
            _ => panic!("Unsupported mixing of timestamped and non-timestamped items"),
        }
    }

    fn structure(&self) -> BlockStructure {
        let mut operator = OperatorStructure::new::<(Out1, Out2), _>("Zip");
        let op_id = self.operator_coord.operator_id;
        operator.subtitle = format!("op id: {op_id}");
        operator
            .receivers
            .push(OperatorReceiver::new::<Out1>(self.prev_block_id1));
        operator
            .receivers
            .push(OperatorReceiver::new::<Out2>(self.prev_block_id2));
        BlockStructure::default().add_operator(operator)
    }

    fn get_op_id(&self) -> OperatorId {
        self.operator_coord.operator_id
    }
}

impl<Out1: ExchangeData, Out2: ExchangeData> Source<(Out1, Out2)> for Zip<Out1, Out2> {
    fn replication(&self) -> Replication {
        Replication::Unlimited
    }

    fn set_snapshot_frequency_by_item(&mut self, _item_interval: u64) {
        // Forbidden action
        panic!("It is not possible to set snapshot frequency for zip operator");
    }

    fn set_snapshot_frequency_by_time(&mut self, _time_interval: Duration) {
        // Forbidden action
        panic!("It is not possible to set snapshot frequency for zip operator");
    }
}

#[cfg(test)]
mod tests {
    use crate::network::{Coord, NetworkMessage, NetworkSender};
    use crate::operator::zip::Zip;
    use crate::operator::{Operator, StreamElement};
    use crate::test::FakeNetworkTopology;

    #[test]
    fn zip() {
        let mut t = FakeNetworkTopology::new(2, 1);

        let (coord_l, sender_l) = t.senders_mut()[0].pop().unwrap();
        let (coord_r, sender_r) = t.senders_mut()[1].pop().unwrap();

        let mut zip = Zip::<i32, i32>::new(coord_l.block_id, coord_r.block_id, false, false, None);
        zip.setup(&mut t.metadata());

        let send = |sender: &NetworkSender<i32>, from: Coord, data: Vec<StreamElement<i32>>| {
            sender.send(NetworkMessage::new_batch(data, from)).unwrap();
        };

        // Stream content:
        // L:   1    2  FnR  3
        // R:   100  -  FnR  300
        // zip: *       *    *
        //
        // "2" has no counterpart in right, so it is discarded

        send(
            &sender_l,
            coord_l,
            vec![StreamElement::Item(1), StreamElement::Item(2)],
        );
        send(&sender_r, coord_r, vec![StreamElement::Item(100)]);

        assert_eq!(zip.next(), StreamElement::Item((1, 100)));

        send(&sender_l, coord_l, vec![StreamElement::FlushAndRestart]);
        send(&sender_r, coord_r, vec![StreamElement::FlushAndRestart]);

        assert_eq!(zip.next(), StreamElement::FlushAndRestart);

        send(&sender_l, coord_l, vec![StreamElement::Item(3)]);
        send(&sender_r, coord_r, vec![StreamElement::Item(300)]);

        assert_eq!(zip.next(), StreamElement::Item((3, 300)));
    }
}
