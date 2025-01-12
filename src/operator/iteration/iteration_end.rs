use crate::block::{BlockStructure, Connection, NextStrategy, OperatorStructure};
use crate::network::{NetworkMessage, NetworkSender, ReceiverEndpoint, OperatorCoord};
use crate::operator::{ExchangeData, Operator, StreamElement};
use crate::persistency::persistency_service::PersistencyService;
use crate::scheduler::{BlockId, ExecutionMetadata, OperatorId};

/// Similar to `End`, but tied specifically for the iterations.
///
/// This block will receive the data (i.e. the `DeltaUpdate` already reduced) and send back to the
/// leader.
///
/// `End` cannot be used here since special care should be taken when the input stream is
/// empty.
#[derive(Debug, Clone)]
pub struct IterationEnd<DeltaUpdate: ExchangeData, OperatorChain>
where
    OperatorChain: Operator<DeltaUpdate>,
{
    /// The chain of previous operators.
    ///
    /// At the end of this chain there should be the local reduction.
    prev: OperatorChain,
    /// Coordinate of the operator in the network
    operator_coord: OperatorCoord,
    /// Whether, since the last `IterEnd`, an element has been received.
    ///
    /// If two `IterEnd` are received in a row it means that the local reduction didn't happen since
    /// no item was present in the stream. A delta update should be sent to the leader nevertheless.
    has_received_item: bool,
    /// The block id of the block containing the `IterationLeader` operator.
    leader_block_id: BlockId,
    /// The sender that points to the `IterationLeader` for sending the `DeltaUpdate` messages.
    leader_sender: Option<NetworkSender<DeltaUpdate>>,
    /// PersistencyService
    persistency_service: Option<PersistencyService<bool>>,
}

impl<DeltaUpdate: ExchangeData, OperatorChain> std::fmt::Display
    for IterationEnd<DeltaUpdate, OperatorChain>
where
    OperatorChain: Operator<DeltaUpdate>,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{} -> IterationEnd<{}>",
            self.prev,
            std::any::type_name::<DeltaUpdate>()
        )
    }
}

impl<DeltaUpdate: ExchangeData, OperatorChain> IterationEnd<DeltaUpdate, OperatorChain>
where
    OperatorChain: Operator<DeltaUpdate>,
{
    pub fn new(prev: OperatorChain, leader_block_id: BlockId) -> Self {
        let op_id = prev.get_op_id() + 1;
        Self {
            prev,
            // This will be set in setup method
            operator_coord: OperatorCoord::new(0, 0, 0, op_id),
            has_received_item: false,
            leader_block_id,
            leader_sender: None,
            persistency_service: None,
        }
    }
}

impl<DeltaUpdate: ExchangeData, OperatorChain> Operator<()>
    for IterationEnd<DeltaUpdate, OperatorChain>
where
    DeltaUpdate: Default,
    OperatorChain: Operator<DeltaUpdate>,
{
    fn setup(&mut self, metadata: &mut ExecutionMetadata) {
        let replicas = metadata.network.replicas(self.leader_block_id);
        assert_eq!(
            replicas.len(),
            1,
            "The IterationEnd block should not be replicated"
        );
        let leader = replicas.into_iter().next().unwrap();
        log::debug!("IterationEnd {} has {} as leader", metadata.coord, leader);

        let sender = metadata
            .network
            .get_sender(ReceiverEndpoint::new(leader, metadata.coord.block_id));
        self.leader_sender = Some(sender);

        self.operator_coord.setup_coord(metadata.coord);
        self.prev.setup(metadata);

        // Setup persistency
        if let Some(pb) = metadata.persistency_builder {
            let p_service = pb.generate_persistency_service::<bool>(); 
            let snapshot_id = p_service.restart_from_snapshot(self.operator_coord);
            if let Some(restart_snap) = snapshot_id {
                // Get and resume the persisted state
                let opt_state: Option<bool> = p_service.get_state(self.operator_coord, restart_snap);
                if let Some(state) = opt_state {
                    self.has_received_item = state;
                } else {
                    panic!("No persisted state founded for op: {0}", self.operator_coord);
                } 
            }
            self.persistency_service = Some(p_service);
        }
    }

    fn next(&mut self) -> StreamElement<()> {
        let elem = self.prev.next();
        match &elem {
            StreamElement::Item(_) => {
                let message = NetworkMessage::new_single(elem, self.operator_coord.get_coord());
                self.leader_sender.as_ref().unwrap().send(message).unwrap();
                self.has_received_item = true;
                StreamElement::Item(())
            }
            StreamElement::FlushAndRestart => {
                // If two FlushAndRestart have been received in a row it means that no message went
                // through the iteration inside this replica. Nevertheless the DeltaUpdate must be
                // sent to the leader.
                if !self.has_received_item {
                    let update = Default::default();
                    let message =
                        NetworkMessage::new_single(StreamElement::Item(update), self.operator_coord.get_coord());
                    let sender = self.leader_sender.as_ref().unwrap();
                    sender.send(message).unwrap();
                }
                self.has_received_item = false;
                StreamElement::FlushAndRestart
            }
            StreamElement::Terminate => {
                let message = NetworkMessage::new_single(StreamElement::Terminate, self.operator_coord.get_coord());
                self.leader_sender.as_ref().unwrap().send(message).unwrap();
                if self.persistency_service.is_some() {
                    let state = self.has_received_item;
                    self.persistency_service
                        .as_mut()
                        .unwrap()
                        .save_terminated_state(
                            self.operator_coord,
                            state,
                        );
                }
                StreamElement::Terminate
            }
            StreamElement::FlushBatch => elem.map(|_| unreachable!()),
            StreamElement::Snapshot(snap_id) => {
                //save state
                let state = self.has_received_item;
                self.persistency_service
                    .as_mut()
                    .unwrap()
                    .save_state(
                        self.operator_coord,
                        snap_id.clone(), 
                        state,
                    );
                let message = NetworkMessage::new_single(StreamElement::Snapshot(snap_id.clone()), self.operator_coord.get_coord());
                self.leader_sender.as_ref().unwrap().send(message).unwrap();
                StreamElement::Snapshot(snap_id.clone())
            }
            _ => unreachable!(),
        }
    }

    fn structure(&self) -> BlockStructure {
        let mut operator = OperatorStructure::new::<DeltaUpdate, _>("IterationEnd");
        let op_id = self.operator_coord.operator_id;
        operator.subtitle = format!("op id: {op_id}");
        operator.connections.push(Connection::new::<DeltaUpdate, _>(
            self.leader_block_id,
            &NextStrategy::only_one(),
        ));
        self.prev.structure().add_operator(operator)
    }

    fn get_op_id(&self) -> OperatorId {
        self.operator_coord.operator_id
    }
    
    fn get_stateful_operators(&self) -> Vec<OperatorId> {
        let mut res = self.prev.get_stateful_operators();
        // This operator is stateful
        res.push(self.operator_coord.operator_id);
        res
    }
}
