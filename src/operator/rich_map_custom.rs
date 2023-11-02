use std::fmt::Display;
use std::marker::PhantomData;

use crate::block::{BlockStructure, OperatorStructure};
use crate::network::OperatorCoord;
use crate::operator::{Data, Operator, StreamElement};
use crate::scheduler::ExecutionMetadata;
use crate::scheduler::OperatorId;

pub struct ElementGenerator<'a, Out, Op> {
    inner: &'a mut Op,
    _out: PhantomData<Out>,
}

impl<'a, Out: Data, Op: Operator<Out>> ElementGenerator<'a, Out, Op> {
    pub fn new(inner: &'a mut Op) -> Self {
        Self {
            inner,
            _out: PhantomData,
        }
    }

    #[allow(clippy::should_implement_trait)]
    pub fn next(&mut self) -> StreamElement<Out> {
        self.inner.next()
    }
}

#[derive(Clone, Debug)]
pub struct RichMapCustom<Out: Data, NewOut: Data, F, OperatorChain>
where
    F: FnMut(ElementGenerator<Out, OperatorChain>) -> StreamElement<NewOut> + Clone + Send,
    OperatorChain: Operator<Out>,
{
    prev: OperatorChain,
    operator_coord: OperatorCoord,
    map_fn: F,
    _out: PhantomData<Out>,
    _new_out: PhantomData<NewOut>,
}

impl<Out: Data, NewOut: Data, F, OperatorChain> Display
    for RichMapCustom<Out, NewOut, F, OperatorChain>
where
    F: FnMut(ElementGenerator<Out, OperatorChain>) -> StreamElement<NewOut> + Clone + Send,
    OperatorChain: Operator<Out>,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{} -> RichMapCustom<{} -> {}>",
            self.prev,
            std::any::type_name::<Out>(),
            std::any::type_name::<NewOut>()
        )
    }
}

impl<Out: Data, NewOut: Data, F, OperatorChain> RichMapCustom<Out, NewOut, F, OperatorChain>
where
    F: FnMut(ElementGenerator<Out, OperatorChain>) -> StreamElement<NewOut> + Clone + Send,
    OperatorChain: Operator<Out>,
{
    pub(super) fn new(prev: OperatorChain, f: F) -> Self {
        let op_id = prev.get_op_id() + 1; 
        Self {
            prev,
            operator_coord: OperatorCoord::new(0, 0, 0, op_id),

            map_fn: f,
            _out: Default::default(),
            _new_out: Default::default(),
        }
    }
}

impl<Out: Data, NewOut: Data, F, OperatorChain> Operator<NewOut>
    for RichMapCustom<Out, NewOut, F, OperatorChain>
where
    F: FnMut(ElementGenerator<Out, OperatorChain>) -> StreamElement<NewOut> + Clone + Send,
    OperatorChain: Operator<Out>,
{
    fn setup(&mut self, metadata: &mut ExecutionMetadata) {
        self.prev.setup(metadata);
        self.operator_coord.setup_coord(metadata.coord);
    }

    fn next(&mut self) -> StreamElement<NewOut> {
        let eg = ElementGenerator::new(&mut self.prev);
        (self.map_fn)(eg)
    }

    fn structure(&self) -> BlockStructure {
        let mut operator = OperatorStructure::new::<NewOut, _>("RichMapCustom");
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
        // This operator is stateless
        self.prev.get_stateful_operators()
    }
}
