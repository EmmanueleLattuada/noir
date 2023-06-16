use std::fmt::Display;
use std::marker::PhantomData;

use crate::block::{BlockStructure, OperatorStructure};
use crate::network::OperatorCoord;
use crate::operator::{Data, Operator, StreamElement};
use crate::persistency::{PersistencyService, PersistencyServices};
use crate::scheduler::{ExecutionMetadata, OperatorId};

#[derive(Clone, Derivative)]
#[derivative(Debug)]
pub struct Inspect<Out: Data, F, PreviousOperators>
where
    F: FnMut(&Out) + Send + Clone,
    PreviousOperators: Operator<Out>,
{
    prev: PreviousOperators,
    operator_coord: OperatorCoord,
    #[derivative(Debug = "ignore")]
    f: F,
    persistency_service: PersistencyService,
    _out: PhantomData<Out>,
}

impl<Out: Data, F, PreviousOperators> Inspect<Out, F, PreviousOperators>
where
    F: FnMut(&Out) + Send + Clone,
    PreviousOperators: Operator<Out>,
{
    pub fn new(prev: PreviousOperators, f: F) -> Self {
        let op_id = prev.get_op_id() + 1;
        Self {
            prev,
            // This will be set in setup method
            operator_coord: OperatorCoord::new(0,0,0,op_id),

            f,
            persistency_service: PersistencyService::default(),
            _out: PhantomData,
        }
    }
}


impl<Out: Data, F, PreviousOperators> Display for Inspect<Out, F, PreviousOperators>
where
    F: FnMut(&Out) + Send + Clone,
    PreviousOperators: Operator<Out>,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} -> Inspect", self.prev)
    }
}

impl<Out: Data, F, PreviousOperators> Operator<Out> for Inspect<Out, F, PreviousOperators>
where
    F: FnMut(&Out) + Send + Clone,
    PreviousOperators: Operator<Out>,
{
    fn setup(&mut self, metadata: &mut ExecutionMetadata) {
        self.prev.setup(metadata);

        self.operator_coord.block_id = metadata.coord.block_id;
        self.operator_coord.host_id = metadata.coord.host_id;
        self.operator_coord.replica_id = metadata.coord.replica_id;

        self.persistency_service = metadata.persistency_service.clone();
        self.persistency_service.setup();
        self.persistency_service.restart_from_snapshot(self.operator_coord);
    }

    #[inline]
    fn next(&mut self) -> StreamElement<Out> {
        let el = self.prev.next();
        match &el {
            StreamElement::Item(t) | StreamElement::Timestamped(t, _) => {
                (self.f)(t);
            }
            StreamElement::Snapshot(snap_id) => {
                // Save void state and forward snapshot marker
                self.persistency_service.save_void_state(self.operator_coord, *snap_id);
            }
            StreamElement::Terminate => {
                if self.persistency_service.is_active() {
                    // Save void terminated state
                    self.persistency_service.save_terminated_void_state(self.operator_coord);
                }
            }
            _ => {}
        }
        el
    }

    fn structure(&self) -> BlockStructure {
        let mut operator = OperatorStructure::new::<Out, _>("Inspect");
        let op_id = self.operator_coord.operator_id;
        operator.subtitle = format!("op id: {op_id}");
        self.prev.structure().add_operator(operator)
    }

    fn get_op_id(&self) -> OperatorId {
        self.operator_coord.operator_id
    }
}
