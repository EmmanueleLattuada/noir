use std::{marker::PhantomData, thread::JoinHandle};

use hashbrown::HashMap;

use crate::{
    block::CoordHasherBuilder,
    channel::Receiver,
    channel::{bounded, Sender},
    network::OperatorCoord,
    operator::{ExchangeData, SnapshotId},
};

use super::{redis_handler::RedisHandler, serialize_data, PersistencyServices};

const CHANNEL_SIZE: usize = 30;

#[derive(Clone, Debug)]
pub(crate) enum PersistencyMessage<State> {
    State(OperatorCoord, SnapshotId, State),
    TerminatedState(OperatorCoord, State),
    Terminate,
}

#[derive(Clone)]
pub(crate) struct StateSaver<State> {
    sender: Sender<PersistencyMessage<Vec<u8>>>,
    _state: PhantomData<State>,
}

impl<S: ExchangeData> std::fmt::Debug for StateSaver<S> {
    fn fmt(&self, _f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        todo!()
    }
}

impl<State: ExchangeData> StateSaver<State> {
    /// Send state to actual sender
    pub(crate) fn save(&self, op_coord: OperatorCoord, snap_id: SnapshotId, state: State) {
        // Serialize the state
        let state_buf = serialize_data(&state);
        // Send to actual state saver
        self.sender
            .send(PersistencyMessage::State(op_coord, snap_id, state_buf))
            .unwrap();
        // TODO: handle errors
    }

    /// Send terminated state to actual sender
    pub(crate) fn save_terminated_state(&self, op_coord: OperatorCoord, state: State) {
        // Serialize the state
        let state_buf = serialize_data(&state);
        // Send to actual state saver
        self.sender
            .send(PersistencyMessage::TerminatedState(op_coord, state_buf))
            .unwrap();
        // TODO: handle errors
    }
}

#[derive(Derivative)]
#[derivative(Debug)]
pub(crate) struct StateSaverHandler {
    sender: Sender<PersistencyMessage<Vec<u8>>>,
    actual_saver: Option<JoinHandle<()>>,
}

impl StateSaverHandler {
    pub(super) fn new(handler: RedisHandler) -> Self {
        // generate channel
        let (tx, rx) = bounded(CHANNEL_SIZE);
        let saver = ActualSaver {
            rx,
            redis: handler,
            last_snapshots: Default::default(),
        };
        // generate and deploy the actual saver
        let handle = std::thread::Builder::new()
            .name(format!("noir-persist"))
            .spawn(move || saver.run())
            .unwrap();
        Self {
            sender: tx,
            actual_saver: Some(handle),
        }
    }

    /// Get state saver
    pub(crate) fn get_state_saver<State>(&self) -> StateSaver<State> {
        StateSaver {
            sender: self.sender.clone(),
            _state: PhantomData::default(),
        }
    }

    ///Close channel then wait for state saver thread to terminate
    pub(crate) fn stop_actual_sender(&mut self) {
        self.sender.send(PersistencyMessage::Terminate).unwrap();
        if let Some(handle) = self.actual_saver.take() {
            handle.join().unwrap();
        }
    }
}

struct ActualSaver {
    rx: Receiver<PersistencyMessage<Vec<u8>>>,
    redis: RedisHandler,
    last_snapshots: HashMap<OperatorCoord, SnapshotId, CoordHasherBuilder>,
}
impl ActualSaver {
    fn save_state(&mut self, coord: OperatorCoord, snap_id: SnapshotId, state: Vec<u8>) {
        self.redis.save_state(&coord, &snap_id, state);
        self.last_snapshots.insert(coord, snap_id);
    }

    // Read from the channel and process messages
    fn run(mut self) {
        while let Ok(msg) = self.rx.recv() {
            micrometer::span!(_g, "redis::saver_rx_save");
            if let PersistencyMessage::State(op_coord, snap_id, state) = msg {
                let last_snap = self.last_snapshots.get(&op_coord);
                if !((snap_id.id() == 1 && last_snap.is_none())
                    || last_snap
                        .unwrap_or(&SnapshotId::new(0))
                        .check_next(&snap_id))
                {
                    panic!("Passed snap_id: {snap_id:?}.\n Last saved snap_id: {last_snap:?}.\n  Op_coord: {op_coord:?}.\n Snapshot id must be a sequence with step 1 starting from 1");
                }
                self.save_state(op_coord, snap_id, state);
            } else if let PersistencyMessage::TerminatedState(op_coord, state) = msg {
                match self.last_snapshots.get(&op_coord) {
                    Some(last_snap) if last_snap.terminate() => {} // Already terminated 
                    Some(last_snap) => {
                        let terminal_snap_id = SnapshotId::new_terminate(last_snap.id() + 1);
                        self.save_state(op_coord, terminal_snap_id, state)
                    }
                    _ => {
                        let terminal_snap_id = SnapshotId::new_terminate(1);
                        self.save_state(op_coord, terminal_snap_id, state)
                    }
                }
            } else {
                // Terminate
                // Stop and exit
                break;
            }
        }
    }
}