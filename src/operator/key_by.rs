use std::fmt::Display;
use std::marker::PhantomData;

use crate::block::{BlockStructure, OperatorStructure};
use crate::network::OperatorCoord;
use crate::operator::{Data, DataKey};
use crate::operator::{Operator, StreamElement};
use crate::persistency::persistency_service::PersistencyService;
use crate::scheduler::{ExecutionMetadata, OperatorId};

#[derive(Clone, Derivative)]
#[derivative(Debug)]
pub struct KeyBy<Key: DataKey, Out: Data, Keyer, OperatorChain>
where
    Keyer: Fn(&Out) -> Key + Send + Clone,
    OperatorChain: Operator<Out>,
{
    prev: OperatorChain,
    operator_coord: OperatorCoord,
    persistency_service: Option<PersistencyService<()>>,
    #[derivative(Debug = "ignore")]
    keyer: Keyer,
    _key: PhantomData<Key>,
    _out: PhantomData<Out>,
}

impl<Key: DataKey, Out: Data, Keyer, OperatorChain> Display
    for KeyBy<Key, Out, Keyer, OperatorChain>
where
    Keyer: Fn(&Out) -> Key + Send + Clone,
    OperatorChain: Operator<Out>,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{} -> KeyBy<{}>",
            self.prev,
            std::any::type_name::<Key>(),
        )
    }
}

impl<Key: DataKey, Out: Data, Keyer, OperatorChain> KeyBy<Key, Out, Keyer, OperatorChain>
where
    Keyer: Fn(&Out) -> Key + Send + Clone,
    OperatorChain: Operator<Out>,
{
    pub fn new(prev: OperatorChain, keyer: Keyer) -> Self {
        let op_id = prev.get_op_id() + 1;
        Self {
            prev,
            // This will be set in setup method
            operator_coord: OperatorCoord::new(0,0,0,op_id),
            persistency_service: None,
            keyer,
            _key: Default::default(),
            _out: Default::default(),
        }
    }
}

impl<Key: DataKey, Out: Data, Keyer, OperatorChain> Operator<(Key, Out)>
    for KeyBy<Key, Out, Keyer, OperatorChain>
where
    Keyer: Fn(&Out) -> Key + Send + Clone,
    OperatorChain: Operator<Out>,
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
    fn next(&mut self) -> StreamElement<(Key, Out)> {
        match self.prev.next() {
            StreamElement::Item(t) => StreamElement::Item(((self.keyer)(&t), t)),
            StreamElement::Timestamped(t, ts) => {
                StreamElement::Timestamped(((self.keyer)(&t), t), ts)
            }
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
                // Save void state and forward snapshot marker
                self.persistency_service.as_mut().unwrap().save_void_state(self.operator_coord, snap_id);
                StreamElement::Snapshot(snap_id)
            }
        }
    }

    fn structure(&self) -> BlockStructure {
        let mut operator = OperatorStructure::new::<(Key, Out), _>("KeyBy");
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
    use crate::operator::key_by::KeyBy;
    use crate::operator::{Operator, StreamElement};
    use crate::test::FakeOperator;

    #[test]
    fn test_key_by() {
        let fake_operator = FakeOperator::new(0..10u8);
        let mut key_by = KeyBy::new(fake_operator, |&n| n);

        for i in 0..10u8 {
            match key_by.next() {
                StreamElement::Item((a, b)) => {
                    assert_eq!(a, i);
                    assert_eq!(b, i);
                }
                item => panic!("Expected StreamElement::Item, got {}", item.variant()),
            }
        }
        assert_eq!(key_by.next(), StreamElement::Terminate);
    }
}
