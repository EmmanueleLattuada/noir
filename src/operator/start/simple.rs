use std::any::TypeId;
use std::time::Duration;

use crate::block::{BlockStructure, OperatorReceiver, OperatorStructure};
use crate::channel::RecvTimeoutError;
use crate::network::{Coord, NetworkMessage, NetworkReceiver, ReceiverEndpoint};
use crate::operator::start::StartReceiver;
use crate::operator::{ExchangeData, SnapshotId};
use crate::scheduler::{BlockId, ExecutionMetadata};

/// This will receive the data from a single previous block.
#[derive(Debug)]
pub(crate) struct SimpleStartReceiver<Out: ExchangeData> {
    pub(super) receiver: Option<NetworkReceiver<Out>>,
    previous_replicas: Vec<Coord>,
    pub(super) previous_block_id: BlockId,
}

impl<Out: ExchangeData> SimpleStartReceiver<Out> {
    pub(super) fn new(previous_block_id: BlockId) -> Self {
        Self {
            receiver: None,
            previous_replicas: Default::default(),
            previous_block_id,
        }
    }
}

impl<Out: ExchangeData> StartReceiver<Out> for SimpleStartReceiver<Out> {
    fn setup(&mut self, metadata: &mut ExecutionMetadata) {
        let in_type = TypeId::of::<Out>();

        let endpoint = ReceiverEndpoint::new(metadata.coord, self.previous_block_id);
        self.receiver = Some(metadata.network.get_receiver(endpoint));

        for &(prev, typ) in metadata.prev.iter() {
            // ignore this connection because it refers to a different type, another Start
            // in this block will handle it
            if in_type != typ {
                continue;
            }
            if prev.block_id == self.previous_block_id {
                self.previous_replicas.push(prev);
            }
        }
    }

    fn prev_replicas(&self) -> Vec<Coord> {
        self.previous_replicas.clone()
    }

    fn cached_replicas(&self) -> usize {
        0
    }

    fn recv_timeout(&mut self, timeout: Duration) -> Result<NetworkMessage<Out>, RecvTimeoutError> {
        let receiver = self.receiver.as_mut().unwrap();
        receiver.recv_timeout(timeout)
    }

    fn recv(&mut self) -> NetworkMessage<Out> {
        let receiver = self.receiver.as_mut().unwrap();
        receiver.recv().expect("Network receiver failed")
    }

    fn structure(&self) -> BlockStructure {
        let mut operator = OperatorStructure::new::<Out, _>("Start");
        // op id must be 0
        operator.subtitle = format!("op id: 0");
        operator
            .receivers
            .push(OperatorReceiver::new::<Out>(self.previous_block_id));
        BlockStructure::default().add_operator(operator)
    }

    // Void type: get_state return always None
    type ReceiverState = ();

    fn get_state(&mut self, _snap_id: SnapshotId) -> Option<Self::ReceiverState> {
        // No state to be persisted
        None
    }
    fn keep_msg_queue(&self) -> bool {
        false
    }
    fn set_state(&mut self, _receiver_state: Self::ReceiverState) {}
}

impl<Out: ExchangeData> Clone for SimpleStartReceiver<Out> {
    fn clone(&self) -> Self {
        Self {
            receiver: None,
            previous_block_id: self.previous_block_id,
            previous_replicas: self.previous_replicas.clone(),
        }
    }
}
