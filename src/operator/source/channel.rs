use std::fmt::Display;

use crate::channel::{bounded, Receiver, RecvError, Sender, TryRecvError};

use crate::block::{BlockStructure, OperatorKind, OperatorStructure, Replication};
use crate::network::OperatorCoord;
use crate::operator::source::Source;
use crate::operator::{Data, Operator, StreamElement};
use crate::persistency::{PersistencyService, PersistencyServices};
use crate::scheduler::{ExecutionMetadata, OperatorId};

use super::SnapshotGenerator;

const MAX_RETRY: u8 = 8;

/// Source that consumes an iterator and emits all its elements into the stream.
///
/// The iterator will be consumed **only from one replica**, therefore this source is not parallel.

#[derive(Derivative)]
#[derivative(Debug)]
pub struct ChannelSource<Out: Data> {
    #[derivative(Debug = "ignore")]
    rx: Receiver<Out>,
    terminated: bool,
    retry_count: u8,
    operator_coord: OperatorCoord,
    snapshot_generator: SnapshotGenerator,
    persistency_service: Option<PersistencyService>,
}

impl<Out: Data> Display for ChannelSource<Out> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "ChannelSource<{}>", std::any::type_name::<Out>())
    }
}

impl<Out: Data> ChannelSource<Out> {
    /// Create a new source that reads the items from the iterator provided as input.
    ///
    /// **Note**: this source is **not parallel**, the iterator will be consumed only on a single
    /// replica, on all the others no item will be read from the iterator. If you want to achieve
    /// parallelism you need to add an operator that shuffles the data (e.g.
    /// [`Stream::shuffle`](crate::Stream::shuffle)).
    ///
    /// ## Example
    ///
    /// ```
    /// # use noir::{StreamEnvironment, EnvironmentConfig};
    /// # use noir::operator::source::ChannelSource;
    /// # let mut env = StreamEnvironment::new(EnvironmentConfig::local(1));
    /// let (tx_channel, source) = ChannelSource::new(4);
    /// let R = env.stream(source);
    /// tx_channel.send(1);
    /// tx_channel.send(2);
    /// ```
    pub fn new(channel_size: usize) -> (Sender<Out>, Self) {
        let (tx, rx) = bounded(channel_size);
        let s = Self {
            rx,
            terminated: false,
            retry_count: 0,
            // This is the first operator in the chain so operator_id = 0
            // Other fields will be set in setup method
            operator_coord: OperatorCoord::new(0, 0, 0, 0),
            snapshot_generator: SnapshotGenerator::new(),
            persistency_service: None,
        };

        (tx, s)
    }
}
// TODO: remove Debug requirement
impl<Out: Data + core::fmt::Debug> Source<Out> for ChannelSource<Out> {
    fn replication(&self) -> Replication {
        Replication::One
    }
}

impl<Out: Data + core::fmt::Debug> Operator<Out> for ChannelSource<Out> {
    fn setup(&mut self, metadata: &mut ExecutionMetadata) {
        self.operator_coord.from_coord(metadata.coord);
        if metadata.persistency_service.is_some() {
            self.persistency_service = metadata.persistency_service.clone();
            let persist_s = self.persistency_service.as_mut().unwrap();
            let snapshot_id = persist_s.restart_from_snapshot(self.operator_coord);
            if let Some(snap_id) = snapshot_id {
                self.terminated = snap_id.terminate();
                self.snapshot_generator.restart_from(snap_id);
            }
            if let Some(snap_freq) = persist_s.snapshot_frequency_by_item {
                self.snapshot_generator.set_item_interval(snap_freq);
            }
            if let Some(snap_freq) = persist_s.snapshot_frequency_by_time {
                self.snapshot_generator.set_time_interval(snap_freq);
            }
        }
    }

    fn next(&mut self) -> StreamElement<Out> {
        loop {
            if self.terminated {
                if self.persistency_service.is_some(){
                    // Save terminated state
                    self.persistency_service.as_mut().unwrap().save_terminated_void_state(self.operator_coord);
                } 
                return StreamElement::Terminate;
            }
            if self.persistency_service.is_some() {
                // Check snapshot generator
                let snapshot = self.snapshot_generator.get_snapshot_marker();
                if snapshot.is_some() {
                    let snapshot_id = snapshot.unwrap();
                    // Save void state (this operator is stateless) and forward snapshot marker
                    self.persistency_service.as_mut().unwrap().save_void_state(self.operator_coord, snapshot_id);
                    return StreamElement::Snapshot(snapshot_id);
                }
            }

            let result = self.rx.try_recv();

            log::debug!("Channel received stuff");
            match result {
                Ok(t) => {
                    self.retry_count = 0;
                    return StreamElement::Item(t);
                }
                Err(TryRecvError::Empty) if self.retry_count < MAX_RETRY => {
                    // Spin before blocking
                    self.retry_count += 1;
                    continue;
                }
                Err(TryRecvError::Empty) if self.retry_count == MAX_RETRY => {
                    log::debug!("no values ready after {MAX_RETRY} tries, sending flush");
                    self.retry_count += 1;
                    return StreamElement::FlushBatch;
                }
                Err(TryRecvError::Empty) => {
                    log::debug!("flushed and no values ready, blocking");
                    self.retry_count = 0;
                    match self.rx.recv() {
                        Ok(t) => return StreamElement::Item(t),
                        Err(RecvError::Disconnected) => {
                            self.terminated = true;
                            log::info!("Stream disconnected");
                            return StreamElement::FlushAndRestart;
                        }
                    }
                }
                Err(TryRecvError::Disconnected) => {
                    self.terminated = true;
                    log::info!("Stream disconnected");
                    return StreamElement::FlushAndRestart;
                }
            }
        }
    }

    fn structure(&self) -> BlockStructure {
        let mut operator = OperatorStructure::new::<Out, _>("ChannelSource");
        let op_id = self.operator_coord.operator_id;
        operator.subtitle = format!("op id: {op_id}");
        operator.kind = OperatorKind::Source;
        BlockStructure::default().add_operator(operator)
    }

    fn get_op_id(&self) -> OperatorId {
        self.operator_coord.operator_id
    }
}

impl<Out: Data> Clone for ChannelSource<Out> {
    fn clone(&self) -> Self {
        // Since this is a non-parallel source, we don't want the other replicas to emit any value
        panic!("ChannelSource cannot be cloned, replication should be 1");
    }
}
