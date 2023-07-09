use std::fmt::Display;
use std::marker::PhantomData;

use crate::block::{BlockStructure, OperatorStructure};
use crate::network::OperatorCoord;
use crate::operator::{Data, Operator, StreamElement};
use crate::persistency::{PersistencyService, PersistencyServices};
use crate::scheduler::{ExecutionMetadata, OperatorId};

#[derive(Clone)]
pub struct Filter<Out: Data, PreviousOperator, Predicate>
where
    Predicate: Fn(&Out) -> bool + Send + Clone + 'static,
    PreviousOperator: Operator<Out> + 'static,
{
    prev: PreviousOperator,
    operator_coord : OperatorCoord,
    predicate: Predicate,
    persistency_service: Option<PersistencyService>,
    _out: PhantomData<Out>,
}

impl<Out: Data, PreviousOperator, Predicate> Display for Filter<Out, PreviousOperator, Predicate>
where
    Predicate: Fn(&Out) -> bool + Send + Clone + 'static,
    PreviousOperator: Operator<Out> + 'static,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{} -> Filter<{}>",
            self.prev,
            std::any::type_name::<Out>()
        )
    }
}

impl<Out: Data, PreviousOperator, Predicate> Filter<Out, PreviousOperator, Predicate>
where
    Predicate: Fn(&Out) -> bool + Clone + Send + 'static,
    PreviousOperator: Operator<Out> + 'static,
{
    pub (super) fn new(prev: PreviousOperator, predicate: Predicate) -> Self {
        let op_id = prev.get_op_id() + 1;
        Self {
            prev,
            // This will be set in setup method
            operator_coord: OperatorCoord::new(0,0,0,op_id),
            persistency_service: None,
            predicate,
            _out: Default::default(),
        }
    }
}

impl<Out: Data, PreviousOperator, Predicate> Operator<Out>
    for Filter<Out, PreviousOperator, Predicate>
where
    Predicate: Fn(&Out) -> bool + Clone + Send + 'static,
    PreviousOperator: Operator<Out> + 'static,
{
    fn setup(&mut self, metadata: &mut ExecutionMetadata) {
        self.prev.setup(metadata);

        self.operator_coord.from_coord(metadata.coord);
        if metadata.persistency_service.is_some(){
            self.persistency_service = metadata.persistency_service.clone();
            self.persistency_service.as_mut().unwrap().restart_from_snapshot(self.operator_coord);
        }
    }

    #[inline]
    fn next(&mut self) -> StreamElement<Out> {
        loop {
            match self.prev.next() {
                StreamElement::Item(ref item) | StreamElement::Timestamped(ref item, _)
                    if !(self.predicate)(item) => {}
                StreamElement::Terminate => {
                    if self.persistency_service.is_some() {
                        // Save void terminated state                            
                        self.persistency_service.as_mut().unwrap().save_terminated_void_state(self.operator_coord);
                    }
                    return StreamElement::Terminate
                }
                StreamElement::Snapshot(snap_id) => {
                    // Save void state and forward snapshot marker
                    self.persistency_service.as_mut().unwrap().save_void_state(self.operator_coord, snap_id);
                    return StreamElement::Snapshot(snap_id);
                }
                element => return element,
            }
        }
    }

    fn structure(&self) -> BlockStructure {
        let mut operator = OperatorStructure::new::<Out, _>("Filter");
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

#[cfg(test)]
mod tests {
    use crate::operator::filter::Filter;
    use crate::operator::{Operator, StreamElement};
    use crate::test::FakeOperator;

    #[test]
    fn test_filter() {
        let fake_operator = FakeOperator::new(0..10u8);
        let mut filter = Filter::new(fake_operator, |n| n % 2 == 0);

        assert_eq!(filter.next(), StreamElement::Item(0));
        assert_eq!(filter.next(), StreamElement::Item(2));
        assert_eq!(filter.next(), StreamElement::Item(4));
        assert_eq!(filter.next(), StreamElement::Item(6));
        assert_eq!(filter.next(), StreamElement::Item(8));
        assert_eq!(filter.next(), StreamElement::Terminate);
    }
}
