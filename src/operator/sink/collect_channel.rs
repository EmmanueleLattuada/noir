use std::fmt::Display;

use crate::block::{BlockStructure, OperatorKind, OperatorStructure};
use crate::network::OperatorCoord;
use crate::operator::sink::Sink;
use crate::operator::{ExchangeData, Operator, StreamElement};
use crate::persistency::{PersistencyService, PersistencyServices};
use crate::scheduler::{ExecutionMetadata, OperatorId};

#[cfg(feature = "crossbeam")]
use crossbeam_channel::Sender;
#[cfg(not(feature = "crossbeam"))]
use flume::Sender;

#[derive(Debug, Clone)]
pub struct CollectChannelSink<Out: ExchangeData, PreviousOperators>
where
    PreviousOperators: Operator<Out>,
{
    prev: PreviousOperators,
    operator_coord: OperatorCoord,
    persistency_service: Option<PersistencyService>,
    tx: Option<Sender<Out>>,
}

impl<Out: ExchangeData, PreviousOperators> CollectChannelSink<Out, PreviousOperators>
where
    PreviousOperators: Operator<Out>,
{
    pub(crate) fn new(prev: PreviousOperators, tx:Sender<Out>) -> Self {
        let op_id = prev.get_op_id() + 1;
        Self {
            prev,
            // This will be set in setup method
            operator_coord: OperatorCoord::new(0, 0, 0, op_id),
            persistency_service: None,
            tx: Some(tx)
        }
    }
}

impl<Out: ExchangeData, PreviousOperators> Display for CollectChannelSink<Out, PreviousOperators>
where
    PreviousOperators: Operator<Out>,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} -> CollectChannelSink", self.prev)
    }
}

impl<Out: ExchangeData, PreviousOperators> Operator<()>
    for CollectChannelSink<Out, PreviousOperators>
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
        match self.prev.next() {
            StreamElement::Item(t) | StreamElement::Timestamped(t, _) => {
                let _ = self.tx.as_ref().map(|tx| tx.send(t));
                StreamElement::Item(())
            }
            StreamElement::Watermark(w) => StreamElement::Watermark(w),
            StreamElement::Terminate => {
                self.tx = None;
                if self.persistency_service.is_some() {
                    // Save terminated state
                    self.persistency_service.as_mut().unwrap().save_terminated_void_state(self.operator_coord);
                }
                StreamElement::Terminate
            }
            StreamElement::FlushBatch => StreamElement::FlushBatch,
            StreamElement::FlushAndRestart => StreamElement::FlushAndRestart,
            StreamElement::Snapshot(snap_id) => {
                self.persistency_service.as_mut().unwrap().save_void_state(self.operator_coord, snap_id);
                StreamElement::Snapshot(snap_id)
            }
        }
    }

    fn structure(&self) -> BlockStructure {
        let mut operator = OperatorStructure::new::<Out, _>("CollectChannelSink");
        let op_id = self.operator_coord.operator_id;
        operator.subtitle = format!("op id: {op_id}");
        operator.kind = OperatorKind::Sink;
        self.prev.structure().add_operator(operator)
    }

    fn get_op_id(&self) -> OperatorId {
        self.operator_coord.operator_id
    }
}

impl<Out: ExchangeData, PreviousOperators> Sink for CollectChannelSink<Out, PreviousOperators> where
    PreviousOperators: Operator<Out>
{
}

#[cfg(test)]
mod tests {
    use itertools::Itertools;

    use crate::config::EnvironmentConfig;
    use crate::environment::StreamEnvironment;
    use crate::operator::source;

    #[test]
    fn collect_channel() {
        let mut env = StreamEnvironment::new(EnvironmentConfig::local(4));
        let source = source::IteratorSource::new(0..10u8);
        let rx = env.stream(source).collect_channel();
        env.execute_blocking();
        let mut v = Vec::new();
        while let Ok(x) = rx.recv() {
            v.push(x)
        }
        assert_eq!(v, (0..10).collect_vec());
    }
}
