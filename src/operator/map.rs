use std::fmt::Display;
use std::marker::PhantomData;

use crate::block::{BlockStructure, OperatorStructure};
use crate::network::OperatorCoord;
use crate::operator::{Data, Operator, StreamElement};
use crate::persistency::{PersistencyService, PersistencyServices};
use crate::scheduler::{ExecutionMetadata, OperatorId};

#[derive(Clone, Derivative)]
#[derivative(Debug)]
pub struct Map<Out: Data, NewOut: Data, F, PreviousOperators>
where
    F: Fn(Out) -> NewOut + Send + Clone,
    PreviousOperators: Operator<Out>,
{
    prev: PreviousOperators,
    operator_coord: OperatorCoord,
    persistency_service: Option<PersistencyService>,
    #[derivative(Debug = "ignore")]
    f: F,
    _out: PhantomData<Out>,
    _new_out: PhantomData<NewOut>,
}

impl<Out: Data, NewOut: Data, F, PreviousOperators> Display
    for Map<Out, NewOut, F, PreviousOperators>
where
    F: Fn(Out) -> NewOut + Send + Clone,
    PreviousOperators: Operator<Out>,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{} -> Map<{} -> {}>",
            self.prev,
            std::any::type_name::<Out>(),
            std::any::type_name::<NewOut>()
        )
    }
}

impl<Out: Data, NewOut: Data, F, PreviousOperators> Map<Out, NewOut, F, PreviousOperators>
where
    F: Fn(Out) -> NewOut + Send + Clone,
    PreviousOperators: Operator<Out>,
{
    pub(super) fn new(prev: PreviousOperators, f: F) -> Self {
        let op_id = prev.get_op_id() + 1;
        Self {
            prev,
            f,
            // This will be set in setup method
            operator_coord: OperatorCoord::new(0, 0, 0, op_id),
            persistency_service: None,

            _out: Default::default(),
            _new_out: Default::default(),
        }
    }
}

impl<Out: Data, NewOut: Data, F, PreviousOperators> Operator<NewOut>
    for Map<Out, NewOut, F, PreviousOperators>
where
    F: Fn(Out) -> NewOut + Send + Clone,
    PreviousOperators: Operator<Out>,
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
    fn next(&mut self) -> StreamElement<NewOut> {
        let elem = self.prev.next();
        match elem {
            StreamElement::Item(item) => StreamElement::Item((self.f)(item)),
            StreamElement::Timestamped(item, ts) => StreamElement::Timestamped((self.f)(item), ts),
            StreamElement::Watermark(w) => StreamElement::Watermark(w),
            StreamElement::Terminate => {
                if self.persistency_service.is_some() {
                    // Save void terminated state
                    self.persistency_service.as_mut().unwrap().save_terminated_void_state(self.operator_coord);
                }
                StreamElement::Terminate
            }
            StreamElement::FlushAndRestart => StreamElement::FlushAndRestart,
            StreamElement::FlushBatch => StreamElement::FlushBatch,
            StreamElement::Snapshot(snap_id) => {
                // Save void state and forward the marker
                self.persistency_service.as_mut().unwrap().save_void_state(self.operator_coord, snap_id);
                StreamElement::Snapshot(snap_id)
            }
        }
    }

    fn structure(&self) -> BlockStructure {
        let mut operator = OperatorStructure::new::<NewOut, _>("Map");
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
    use std::str::FromStr;

    use crate::operator::map::Map;
    use crate::operator::{Operator, StreamElement};
    use crate::test::FakeOperator;

    #[test]
    #[cfg(feature = "timestamp")]
    fn map_stream() {
        let mut fake_operator = FakeOperator::new(0..10u8);
        for i in 0..10 {
            fake_operator.push(StreamElement::Timestamped(i, i as i64));
        }
        fake_operator.push(StreamElement::Watermark(100));

        let map = Map::new(fake_operator, |x| x.to_string());
        let map = Map::new(map, |x| x + "000");
        let mut map = Map::new(map, |x| u32::from_str(&x).unwrap());

        for i in 0..10 {
            let elem = map.next();
            assert_eq!(elem, StreamElement::Item(i * 1000));
        }
        for i in 0..10 {
            let elem = map.next();
            assert_eq!(elem, StreamElement::Timestamped(i * 1000, i as i64));
        }
        assert_eq!(map.next(), StreamElement::Watermark(100));
        assert_eq!(map.next(), StreamElement::Terminate);
    }
}
