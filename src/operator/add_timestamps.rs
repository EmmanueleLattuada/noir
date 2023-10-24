use std::fmt::Display;
use std::marker::PhantomData;

use serde::{Serialize, Deserialize};

use crate::block::{BlockStructure, OperatorStructure};
use crate::network::OperatorCoord;
use crate::operator::{Data, Operator, StreamElement, Timestamp};
use crate::persistency::persistency_service::PersistencyService;
use crate::scheduler::{ExecutionMetadata, OperatorId};

use super::SnapshotId;

#[derive(Clone)]
pub struct AddTimestamp<Out: Data, TimestampGen, WatermarkGen, OperatorChain>
where
    OperatorChain: Operator<Out>,
    TimestampGen: FnMut(&Out) -> Timestamp + Clone + Send + 'static,
    WatermarkGen: FnMut(&Out, &Timestamp) -> Option<Timestamp> + Clone + Send + 'static,
{
    prev: OperatorChain,
    operator_coord : OperatorCoord,
    persistency_service: Option<PersistencyService<AddTimestampState>>,
    timestamp_gen: TimestampGen,
    watermark_gen: WatermarkGen,
    pending_watermark: Option<Timestamp>,
    _out: PhantomData<Out>,
}

#[derive(Clone, Serialize, Deserialize)]
struct AddTimestampState{
    pending_watermark: Option<Timestamp>,
}

impl<Out: Data, TimestampGen, WatermarkGen, OperatorChain> Display
    for AddTimestamp<Out, TimestampGen, WatermarkGen, OperatorChain>
where
    OperatorChain: Operator<Out>,
    TimestampGen: FnMut(&Out) -> Timestamp + Clone + Send + 'static,
    WatermarkGen: FnMut(&Out, &Timestamp) -> Option<Timestamp> + Clone + Send + 'static,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} -> AddTimestamp", self.prev)
    }
}

impl<Out: Data, TimestampGen, WatermarkGen, OperatorChain>
    AddTimestamp<Out, TimestampGen, WatermarkGen, OperatorChain>
where
    OperatorChain: Operator<Out>,
    TimestampGen: FnMut(&Out) -> Timestamp + Clone + Send + 'static,
    WatermarkGen: FnMut(&Out, &Timestamp) -> Option<Timestamp> + Clone + Send + 'static,
{
    pub(super) fn new(
        prev: OperatorChain,
        timestamp_gen: TimestampGen,
        watermark_gen: WatermarkGen,
    ) -> Self {
        let op_id = prev.get_op_id() + 1;
        Self {
            prev,
            // This will be set in setup method
            operator_coord: OperatorCoord::new(0,0,0,op_id),
            persistency_service: None,
            timestamp_gen,
            watermark_gen,
            pending_watermark: None,
            _out: Default::default(),
        }
    }
    
    /// Save state for snapshot
    fn save_snap(&mut self, snapshot_id: SnapshotId){
        let state = AddTimestampState{pending_watermark: self.pending_watermark};
        self.persistency_service.as_mut().unwrap().save_state(self.operator_coord, snapshot_id, state);
    }
    /// Save terminated state
    fn save_terminate(&mut self){
        let state = AddTimestampState{pending_watermark: self.pending_watermark};
        self.persistency_service.as_mut().unwrap().save_terminated_state(self.operator_coord, state);
    }
}

impl<Out: Data, TimestampGen, WatermarkGen, OperatorChain> Operator<Out>
    for AddTimestamp<Out, TimestampGen, WatermarkGen, OperatorChain>
where
    OperatorChain: Operator<Out>,
    TimestampGen: FnMut(&Out) -> Timestamp + Clone + Send + 'static,
    WatermarkGen: FnMut(&Out, &Timestamp) -> Option<Timestamp> + Clone + Send + 'static,
{
    fn setup(&mut self, metadata: &mut ExecutionMetadata) {
        self.prev.setup(metadata);

        self.operator_coord.setup_coord(metadata.coord);
        if let Some(pb) = metadata.persistency_builder {
            let p_service = pb.generate_persistency_service::<AddTimestampState>();
            let snapshot_id = p_service.restart_from_snapshot(self.operator_coord);
            if let Some(restart_snap) = snapshot_id {
                // Get and resume the persisted state
                let opt_state: Option<AddTimestampState> = p_service.get_state(self.operator_coord, restart_snap);
                if let Some(state) = opt_state {
                    self.pending_watermark = state.pending_watermark;
                } else {
                    panic!("No persisted state founded for op: {0}", self.operator_coord);
                } 
            }
            self.persistency_service = Some(p_service);
        }
    }

    #[inline]
    fn next(&mut self) -> StreamElement<Out> {
        if let Some(ts) = self.pending_watermark.take() {
            return StreamElement::Watermark(ts);
        }

        let elem = self.prev.next();
        match elem {
            StreamElement::Item(item) => {
                let ts = (self.timestamp_gen)(&item);
                let watermark = (self.watermark_gen)(&item, &ts);
                self.pending_watermark = watermark;
                StreamElement::Timestamped(item, ts)
            }
            StreamElement::FlushAndRestart
            | StreamElement::FlushBatch => elem,
            StreamElement::Terminate => {
                if self.persistency_service.is_some(){
                    self.save_terminate();
                }
                StreamElement::Terminate
            }
            StreamElement::Snapshot(snap_id) => {
                self.save_snap(snap_id.clone());
                StreamElement::Snapshot(snap_id)
            }
            _ => panic!("AddTimestamp received invalid variant: {}", elem.variant()),
        }
    }

    fn structure(&self) -> BlockStructure {
        let mut operator = OperatorStructure::new::<Out, _>("AddTimestamp");
        let op_id = self.operator_coord.operator_id;
        operator.subtitle = format!("op id: {op_id}");
        self.prev
            .structure()
            .add_operator(operator)
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

#[derive(Clone)]
pub struct DropTimestamp<Out: Data, OperatorChain>
where
    OperatorChain: Operator<Out>,
{
    prev: OperatorChain,
    operator_coord: OperatorCoord,
    //persistency_service: Option<PersistencyService<()>>,
    _out: PhantomData<Out>,
}

impl<Out: Data, OperatorChain> Display for DropTimestamp<Out, OperatorChain>
where
    OperatorChain: Operator<Out>,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} -> DropTimestamp", self.prev)
    }
}

impl<Out: Data, OperatorChain> DropTimestamp<Out, OperatorChain>
where
    OperatorChain: Operator<Out>,
{
    pub (super) fn new(prev: OperatorChain) -> Self {
        let op_id = prev.get_op_id() + 1;
        Self {
            prev,
            //persistency_service: None,
            // This will be set in setup method
            operator_coord: OperatorCoord::new(0, 0, 0, op_id),
            _out: Default::default(),
        }
    }
}

impl<Out: Data, OperatorChain> Operator<Out> for DropTimestamp<Out, OperatorChain>
where
    OperatorChain: Operator<Out>,
{
    fn setup(&mut self, metadata: &mut ExecutionMetadata) {
        self.prev.setup(metadata);

        self.operator_coord.setup_coord(metadata.coord);
        /*if let Some(pb) = &metadata.persistency_builder {
            let p_service = pb.generate_persistency_service::<()>();
            p_service.restart_from_snapshot(self.operator_coord);
            self.persistency_service = Some(p_service);
        }*/
    }

    #[inline]
    fn next(&mut self) -> StreamElement<Out> {
        loop {
            match self.prev.next() {
                StreamElement::Watermark(_) => continue,
                StreamElement::Timestamped(item, _) => return StreamElement::Item(item),
                StreamElement::Snapshot(snapshot_id) => {
                    // Save void state and forward snapshot marker
                    //self.persistency_service.as_mut().unwrap().save_void_state(self.operator_coord, snapshot_id.clone());
                    return StreamElement::Snapshot(snapshot_id)
                }
                StreamElement::Terminate => {
                    /*if self.persistency_service.is_some(){
                        // Save void terminated state
                        self.persistency_service.as_mut().unwrap().save_terminated_void_state(self.operator_coord);
                    }*/
                    return StreamElement::Terminate
                }
                el => return el,
            }
        }
    }

    fn structure(&self) -> BlockStructure {
        let mut operator = OperatorStructure::new::<Out, _>("DropTimestamp");
        let op_id = self.operator_coord.operator_id;
        operator.subtitle = format!("op id: {op_id}");
        self.prev
            .structure()
            .add_operator(operator)
    }

    fn get_op_id(&self) -> OperatorId {
        self.operator_coord.operator_id
    }

    fn get_stateful_operators(&self) -> Vec<OperatorId> {
        // This operator is stateless
        self.prev.get_stateful_operators()
    }
}

#[cfg(test)]
mod tests {
    use crate::operator::add_timestamps::AddTimestamp;
    use crate::operator::{Operator, StreamElement};
    use crate::test::FakeOperator;

    #[test]
    fn add_timestamps() {
        let fake_operator = FakeOperator::new(0..10u64);

        let mut oper = AddTimestamp::new(
            fake_operator,
            |n| *n as i64,
            |n, ts| {
                if n % 2 == 0 {
                    Some(*ts)
                } else {
                    None
                }
            },
        );

        for i in 0..5u64 {
            let t = i * 2;
            assert_eq!(oper.next(), StreamElement::Timestamped(t, t as i64));
            assert_eq!(oper.next(), StreamElement::Watermark(t as i64));
            assert_eq!(oper.next(), StreamElement::Timestamped(t + 1, t as i64 + 1));
        }
        assert_eq!(oper.next(), StreamElement::Terminate);
    }
}
