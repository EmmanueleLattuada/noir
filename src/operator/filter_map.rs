use std::fmt::Display;
use std::marker::PhantomData;

use crate::block::{BlockStructure, OperatorStructure};
use crate::network::OperatorCoord;
use crate::operator::{Data, Operator};
use crate::persistency::persistency_service::PersistencyService;
use crate::scheduler::OperatorId;
use crate::ExecutionMetadata;

use super::StreamElement;

#[derive(Clone)]
pub struct FilterMap<In: Data, Out: Data, PreviousOperator, Predicate>
where
    Predicate: Fn(In) -> Option<Out> + Send + Clone + 'static,
    PreviousOperator: Operator<In> + 'static,
{
    prev: PreviousOperator,
    operator_coord : OperatorCoord,
    predicate: Predicate,
    persistency_service: Option<PersistencyService<()>>,
    _in: PhantomData<In>,
    _out: PhantomData<Out>,
}

impl<In: Data, Out: Data, PreviousOperator, Predicate> Display
    for FilterMap<In, Out, PreviousOperator, Predicate>
where
    Predicate: Fn(In) -> Option<Out> + Send + Clone + 'static,
    PreviousOperator: Operator<In> + 'static,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{} -> FilterMap<{}>",
            self.prev,
            std::any::type_name::<Out>()
        )
    }
}

impl<In: Data, Out: Data, PreviousOperator, Predicate>
    FilterMap<In, Out, PreviousOperator, Predicate>
where
    Predicate: Fn(In) -> Option<Out> + Send + Clone + 'static,
    PreviousOperator: Operator<In> + 'static,
{
    pub (super) fn new(prev: PreviousOperator, predicate: Predicate) -> Self {
        let op_id = prev.get_op_id() + 1;
        Self {
            prev,
            // This will be set in setup method
            operator_coord: OperatorCoord::new(0,0,0,op_id),
            persistency_service: None,
            predicate,
            _in: Default::default(),
            _out: Default::default(),
        }
    }
}

impl<In: Data, Out: Data, PreviousOperator, Predicate> Operator<Out>
    for FilterMap<In, Out, PreviousOperator, Predicate>
where
    Predicate: Fn(In) -> Option<Out> + Send + Clone + 'static,
    PreviousOperator: Operator<In> + 'static,
{
    fn setup(&mut self, metadata: &mut ExecutionMetadata) {
        self.prev.setup(metadata);

        self.operator_coord.from_coord(metadata.coord);
        if let Some(pb) = &metadata.persistency_builder{
            let p_service = pb.generate_persistency_service::<()>();
            p_service.restart_from_snapshot(self.operator_coord);
            self.persistency_service = Some(p_service);
        }
    }

    #[inline]
    fn next(&mut self) -> StreamElement<Out> {
        loop {
            match self.prev.next() {
                StreamElement::Item(item) => {
                    if let Some(el) = (self.predicate)(item) {
                        return StreamElement::Item(el);
                    }
                }
                StreamElement::Timestamped(item, ts) => {
                    if let Some(el) = (self.predicate)(item) {
                        return StreamElement::Timestamped(el, ts);
                    }
                }
                StreamElement::Watermark(w) => return StreamElement::Watermark(w),
                StreamElement::Terminate => {
                    if self.persistency_service.is_some() {
                        // Save void terminated state                            
                        self.persistency_service.as_mut().unwrap().save_terminated_void_state(self.operator_coord);
                    }
                    return StreamElement::Terminate
                }
                StreamElement::FlushAndRestart => return StreamElement::FlushAndRestart,
                StreamElement::FlushBatch => return StreamElement::FlushBatch,
                StreamElement::Snapshot(snap_id) => {
                    // Save void state and forward snapshot marker
                    self.persistency_service.as_mut().unwrap().save_void_state(self.operator_coord, snap_id);
                    return StreamElement::Snapshot(snap_id);
                }
            }
        }
    }

    fn structure(&self) -> BlockStructure {
        let mut operator = OperatorStructure::new::<Out, _>("FilterMap");
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
