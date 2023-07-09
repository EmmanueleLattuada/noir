use std::fmt::Display;
use std::marker::PhantomData;

use crate::block::{BlockStructure, OperatorKind, OperatorStructure};
use crate::network::OperatorCoord;
use crate::operator::sink::{Sink, StreamOutputRef};
use crate::operator::{ExchangeData, Operator, StreamElement};
use crate::persistency::{PersistencyService, PersistencyServices};
use crate::scheduler::{ExecutionMetadata, OperatorId};

#[derive(Debug)]
pub struct Collect<Out: ExchangeData, C: FromIterator<Out> + Send, PreviousOperators>
where
    PreviousOperators: Operator<Out>,
{
    prev: PreviousOperators,
    operator_coord: OperatorCoord,
    persistency_service: Option<PersistencyService>,
    output: StreamOutputRef<C>,
    _out: PhantomData<Out>,
}

impl<Out: ExchangeData, C: FromIterator<Out> + Send, PreviousOperators> Collect<Out, C, PreviousOperators>
where
    PreviousOperators: Operator<Out>,
{
    pub fn new(prev: PreviousOperators, output: StreamOutputRef<C>) -> Self {
        let op_id = prev.get_op_id() + 1;
        Self {
            prev,
            // This will be set in setup method
            operator_coord: OperatorCoord::new(0, 0, 0, op_id),
            persistency_service: None,
            output,
            _out: PhantomData,
        }
    }
}

impl<Out: ExchangeData, C: FromIterator<Out> + Send, PreviousOperators> Display
    for Collect<Out, C, PreviousOperators>
where
    PreviousOperators: Operator<Out>,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{} -> Collect<{}>",
            self.prev,
            std::any::type_name::<C>()
        )
    }
}

impl<Out: ExchangeData, C: FromIterator<Out> + Send, PreviousOperators> Operator<()>
    for Collect<Out, C, PreviousOperators>
where
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

    fn next(&mut self) -> StreamElement<()> {
        let iter = std::iter::from_fn(|| loop {
            match self.prev.next() {
                StreamElement::Item(t) | StreamElement::Timestamped(t, _) => return Some(t),
                StreamElement::Terminate => {
                    if self.persistency_service.is_some() {
                        // Save void terminated state
                        self.persistency_service.as_mut().unwrap().save_terminated_void_state(self.operator_coord);
                    }
                    return None;
                }
                StreamElement::Snapshot(snapshot_id) => {
                    // State is not persisted 
                    self.persistency_service.as_mut().unwrap().save_void_state(self.operator_coord, snapshot_id);
                    continue
                }
                _ => continue,
            }
        });
        let c = C::from_iter(iter);
        *self.output.lock().unwrap() = Some(c);

        StreamElement::Terminate
    }

    fn structure(&self) -> BlockStructure {
        let mut operator = OperatorStructure::new::<Out, _>("Collect");
        let op_id = self.operator_coord.operator_id;
        operator.subtitle = format!("op id: {op_id}");
        operator.kind = OperatorKind::Sink;
        self.prev.structure().add_operator(operator)
    }

    fn get_op_id(&self) -> OperatorId {
        self.operator_coord.operator_id
    }
}

impl<Out: ExchangeData, C: FromIterator<Out> + Send, PreviousOperators> Sink
    for Collect<Out, C, PreviousOperators>
where
    PreviousOperators: Operator<Out>,
{
}

impl<Out: ExchangeData, C: FromIterator<Out> + Send, PreviousOperators> Clone
    for Collect<Out, C, PreviousOperators>
where
    PreviousOperators: Operator<Out>,
{
    fn clone(&self) -> Self {
        panic!("Collect cannot be cloned, replication should be 1");
    }
}

#[cfg(test)]
mod qtests {
    use std::collections::HashSet;

    use crate::config::EnvironmentConfig;
    use crate::environment::StreamEnvironment;
    use crate::operator::source;

    #[test]
    fn collect_vec() {
        let mut env = StreamEnvironment::new(EnvironmentConfig::local(4));
        let source = source::IteratorSource::new(0..10u8);
        let res = env.stream(source).collect::<Vec<_>>();
        env.execute_blocking();
        assert_eq!(res.get().unwrap(), (0..10).collect::<Vec<_>>());
    }

    #[test]
    fn collect_set() {
        let mut env = StreamEnvironment::new(EnvironmentConfig::local(4));
        let source = source::IteratorSource::new(0..10u8);
        let res = env.stream(source).collect::<HashSet<_>>();
        env.execute_blocking();
        assert_eq!(res.get().unwrap(), (0..10).collect::<HashSet<_>>());
    }
}
