use std::{thread::JoinHandle, time::Duration};


use crate::{channel::{Sender, bounded}, channel::Receiver, operator::{ExchangeData, SnapshotId}, network::OperatorCoord};

use super::{redis_handler::RedisHandler, PersistencyServices};

const CHANNEL_SIZE: usize = 30;
const SLEEPING_TIME: Duration = Duration::from_millis(5);

pub(crate) enum PersistencyMessage<State> {
    State(OperatorCoord, SnapshotId, State),
    TerminatedState(OperatorCoord, State),
    Terminate,
}


pub(crate) struct StateSaver<State: ExchangeData>{
    sender: Sender<PersistencyMessage<State>>,
    actual_saver: Option<JoinHandle<()>>,
}


impl <S: ExchangeData> std::fmt::Debug for StateSaver<S> {
    fn fmt(&self, _f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
         todo!()
    }
}

impl<State:ExchangeData> StateSaver<State> {
    pub(super) fn new(handler: RedisHandler, snap_freq: Option<Duration>) -> Self {
        // generate channel
        let (tx, rx) = bounded(CHANNEL_SIZE);
        let saver = ActualSaver {
            recv: rx,
            handler,
            snap_freq,
        };
        // generate and deploy the actual saver
        let handle = std::thread::Builder::new()
            .name(format!("persistency-service"))
            .spawn(move || {
                do_work(saver)
            })
            .unwrap();
        Self {
            sender: tx,
            actual_saver: Some(handle),
        }

    }

    /// Send state to actual sender
    pub(crate) fn save(&self, op_coord: OperatorCoord, snap_id: SnapshotId, state: State) {
        self.sender.send(PersistencyMessage::State(op_coord, snap_id, state)).unwrap();
        // TODO: handle errors
    }

    /// Send terminated state to actual sender
    pub(crate) fn save_terminated_state(&self, op_coord: OperatorCoord, state: State) {
        self.sender.send(PersistencyMessage::TerminatedState(op_coord, state)).unwrap();
        // TODO: handle errors
    }

    ///Close channel then wait for state saver thread to terminate
    pub(crate) fn stop_actual_sender(&mut self){
        self.sender.send(PersistencyMessage::Terminate).unwrap();
        if let Some(handle) = self.actual_saver.take() {
            handle.join().unwrap();
        }
    }
}

struct ActualSaver<State:ExchangeData>{
    recv: Receiver<PersistencyMessage<State>>,
    handler: RedisHandler,
    #[allow(dead_code)]
    snap_freq: Option<Duration>,
}

// Sleep and read from the channel
fn do_work<State: ExchangeData>(saver: ActualSaver<State>) {
    loop {
        // Try to recv
        let recv = saver.recv.recv();
        if let Ok(msg) = recv {
            if let PersistencyMessage::State(op_coord, snap_id, state) = msg {
                // Checks on snapshot id
                let last_snapshot = saver.handler.get_last_snapshot(op_coord);
                if !((snap_id.id() == 1 && last_snapshot.is_none()) || last_snapshot.clone().unwrap_or(SnapshotId::new(0)).check_next(snap_id.clone())) {
                    panic!("Passed snap_id: {snap_id:?}.\n Last saved snap_id: {last_snapshot:?}.\n  Op_coord: {op_coord:?}.\n Snapshot id must be a sequence with step 1 starting from 1");
                }
                saver.handler.save_state(op_coord, snap_id, state);
                continue
            } else if let PersistencyMessage::TerminatedState(op_coord, state) = msg {
                // Get snapshot id for terminated state
                let opt_last_snapshot_id = saver.handler.get_last_snapshot(op_coord);
                if let Some(last_snapshot_id) = opt_last_snapshot_id {
                    if !last_snapshot_id.terminate() {
                        let terminal_snap_id = SnapshotId::new_terminate(last_snapshot_id.id() + 1);
                        saver.handler.save_state(op_coord, terminal_snap_id, state)
                    }
                } else {
                    // Save with id = 1
                    let terminal_snap_id = SnapshotId::new_terminate(1);
                    saver.handler.save_state(op_coord, terminal_snap_id, state)
                }
                continue
            } else {
                // Stop and exit
                break
            }
        }
        // Sleep
        std::thread::sleep(SLEEPING_TIME);
        // TODO: better sleeping timing
    }
}
