use crate::protocol::VescCommandId;
use crate::vesc_trampa_proto;
use bytes::{Bytes, BytesMut};
use normfs::NormFS;
use normfs::UintN;
use parking_lot::RwLock;
use prost::Message;
use station_iface::{Backpressure, QueueWriter, WRITE_TIMEOUT};
use std::sync::Arc;

#[derive(Default)]
struct VescInferenceState {
    state: vesc_trampa_proto::InferenceState,
}

pub struct VescTrampaCommunicator {
    pub normfs: Arc<NormFS>,
    pub tx_queue_id: normfs::QueueId,
    rx_queue_id: normfs::QueueId,
    /// Written from a subscriber callback, which cannot wait for a page.
    tx_writer: QueueWriter,
    /// A snapshot follows every rx record and is kept or skipped with it.
    inference_writer: QueueWriter,
    inference_states_queue_id: normfs::QueueId,
    state: Arc<RwLock<VescInferenceState>>,
}

impl VescTrampaCommunicator {
    pub async fn new(
        normfs: Arc<NormFS>,
        rx_queue_id: normfs::QueueId,
        tx_queue_id: normfs::QueueId,
        inference_queue_id: normfs::QueueId,
    ) -> Result<Self, normfs::Error> {
        let inference_states_queue_id = normfs.resolve("inference-states");
        normfs.ensure_queue_exists_for_write(&rx_queue_id).await?;
        let tx_writer = QueueWriter::open(normfs.clone(), tx_queue_id.clone()).await?;
        let inference_writer = QueueWriter::open(normfs.clone(), inference_queue_id).await?;
        Ok(Self {
            normfs,
            tx_queue_id,
            rx_queue_id,
            tx_writer,
            inference_writer,
            inference_states_queue_id,
            state: Arc::new(RwLock::new(VescInferenceState::default())),
        })
    }

    fn encode<M: Message>(envelope: &M) -> Bytes {
        let mut envelope_buf = Vec::new();
        envelope.encode(&mut envelope_buf).unwrap();
        Bytes::from(envelope_buf)
    }

    /// The caller says whether the record may be skipped: a values packet
    /// the next 20 ms tick replaces may, while a board appearing or going
    /// away, a command's result and the packet answering a command may not.
    ///
    /// A skipped record leaves the inference state untouched, so its pointer
    /// keeps naming the last record that was placed.
    pub async fn send_rx(
        &self,
        envelope: &crate::vesc_trampa_proto::RxEnvelope,
        policy: Backpressure,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let data = Self::encode(envelope);
        let ptr = match policy {
            Backpressure::Skip => match self.normfs.try_enqueue(&self.rx_queue_id, data) {
                Ok(id) => id,
                Err(normfs::Error::WouldBlock) => return Ok(()),
                Err(e) => return Err(Box::new(e)),
            },
            Backpressure::Keep => {
                tokio::time::timeout(WRITE_TIMEOUT, self.normfs.enqueue(&self.rx_queue_id, data))
                    .await
                    .map_err(|_| "no page became free in time")??
            }
        };
        self.update_state(envelope, ptr, policy);
        Ok(())
    }

    /// Never fails and never waits: the writer task reports what it could not
    /// place.
    pub fn send_tx(&self, envelope: &crate::vesc_trampa_proto::TxEnvelope) {
        self.tx_writer
            .write(Self::encode(envelope), Backpressure::Keep);
    }

    fn update_state(
        &self,
        envelope: &vesc_trampa_proto::RxEnvelope,
        ptr: UintN,
        policy: Backpressure,
    ) {
        let Some(board) = envelope.board.as_ref() else {
            return;
        };

        match vesc_trampa_proto::VescTrampaSignalType::try_from(envelope.signal_type) {
            Ok(vesc_trampa_proto::VescTrampaSignalType::VescTrampaBoardConnect) => {
                self.add_board(board, envelope);
            }
            Ok(vesc_trampa_proto::VescTrampaSignalType::VescTrampaBoardDisconnect) => {
                self.remove_board(board);
            }
            Ok(vesc_trampa_proto::VescTrampaSignalType::VescTrampaBoardPacket) => {
                self.update_values(board, envelope, ptr);
            }
            Ok(vesc_trampa_proto::VescTrampaSignalType::VescTrampaCommand) => {}
            Ok(vesc_trampa_proto::VescTrampaSignalType::VescTrampaCommandSuccess) => {
                self.update_mode_for_command_success(board, envelope);
            }
            _ => {}
        }

        {
            let mut state = self.state.write();
            state.state.last_inference_queue_ptr = self.get_last_inference_id_bytes();
        }
        self.publish_inference_state(policy);
    }

    fn add_board(
        &self,
        board: &vesc_trampa_proto::VescTrampaBoard,
        envelope: &vesc_trampa_proto::RxEnvelope,
    ) {
        let mut state = self.state.write();
        if state
            .state
            .boards
            .iter()
            .any(|board_state| Self::same_board(board_state.board.as_ref(), board))
        {
            return;
        }

        state
            .state
            .boards
            .push(vesc_trampa_proto::inference_state::BoardState {
                board: Some(board.clone()),
                motor_mode: vesc_trampa_proto::VescTrampaMotorMode::Unspecified as i32,
                monotonic_stamp_ns: envelope.monotonic_stamp_ns,
                local_stamp_ns: envelope.local_stamp_ns,
                app_start_id: envelope.app_start_id,
                values_payload: Bytes::new(),
                values_rx_pointer: Bytes::new(),
                values_monotonic_stamp_ns: 0,
                values_local_stamp_ns: 0,
                values_app_start_id: 0,
            });
    }

    fn remove_board(&self, board: &vesc_trampa_proto::VescTrampaBoard) {
        let mut state = self.state.write();
        state
            .state
            .boards
            .retain(|board_state| !Self::same_board(board_state.board.as_ref(), board));
    }

    fn update_values(
        &self,
        board: &vesc_trampa_proto::VescTrampaBoard,
        envelope: &vesc_trampa_proto::RxEnvelope,
        ptr: UintN,
    ) {
        let Some(packet) = envelope.board_packet.as_ref() else {
            return;
        };
        if packet.command_id != VescCommandId::GetValues.as_u32() {
            return;
        }

        let mut ptr_buf = BytesMut::with_capacity(8);
        ptr.write_value_to_buffer(&mut ptr_buf);
        let ptr_buf = ptr_buf.freeze();

        let mut state = self.state.write();
        if let Some(board_state) = state
            .state
            .boards
            .iter_mut()
            .find(|board_state| Self::same_board(board_state.board.as_ref(), board))
        {
            board_state.monotonic_stamp_ns = envelope.monotonic_stamp_ns;
            board_state.local_stamp_ns = envelope.local_stamp_ns;
            board_state.app_start_id = envelope.app_start_id;
            board_state.values_payload = packet.payload.clone();
            board_state.values_rx_pointer = ptr_buf;
            board_state.values_monotonic_stamp_ns = envelope.monotonic_stamp_ns;
            board_state.values_local_stamp_ns = envelope.local_stamp_ns;
            board_state.values_app_start_id = envelope.app_start_id;
        }
    }

    fn update_mode_for_command_success(
        &self,
        board: &vesc_trampa_proto::VescTrampaBoard,
        envelope: &vesc_trampa_proto::RxEnvelope,
    ) {
        let Some(command) = envelope.command.as_ref() else {
            return;
        };
        if !command.board_commands.is_empty() {
            self.set_motor_mode(board, vesc_trampa_proto::VescTrampaMotorMode::Unspecified);
            return;
        }

        let Some(motor_mode) = command.motor_mode.as_ref() else {
            return;
        };
        let mode = vesc_trampa_proto::VescTrampaMotorMode::try_from(motor_mode.mode)
            .unwrap_or(vesc_trampa_proto::VescTrampaMotorMode::Unspecified);
        if mode == vesc_trampa_proto::VescTrampaMotorMode::Hold {
            self.set_motor_mode(board, mode);
        }
    }

    fn set_motor_mode(
        &self,
        board: &vesc_trampa_proto::VescTrampaBoard,
        mode: vesc_trampa_proto::VescTrampaMotorMode,
    ) {
        let mut state = self.state.write();
        if let Some(board_state) = state
            .state
            .boards
            .iter_mut()
            .find(|board_state| Self::same_board(board_state.board.as_ref(), board))
        {
            board_state.motor_mode = mode as i32;
        }
    }

    fn same_board(
        state_board: Option<&vesc_trampa_proto::VescTrampaBoard>,
        board: &vesc_trampa_proto::VescTrampaBoard,
    ) -> bool {
        let Some(state_board) = state_board else {
            return false;
        };
        if !board.uuid.is_empty() || !state_board.uuid.is_empty() {
            return state_board.uuid == board.uuid;
        }
        state_board.port_name == board.port_name
    }

    fn get_last_inference_id_bytes(&self) -> Bytes {
        match self.normfs.get_last_id(&self.inference_states_queue_id) {
            Ok(id) => {
                let mut ptr_data = BytesMut::new();
                id.write_value_to_buffer(&mut ptr_data);
                ptr_data.freeze()
            }
            Err(error) => {
                log::warn!(
                    "Failed to get last inference ID from queue inference-states: {}",
                    error
                );
                Bytes::new()
            }
        }
    }

    /// A snapshot that follows a skippable record is replaced by the next
    /// one; a snapshot that follows a connect, disconnect or command result
    /// is the only one that says so, and is kept with it.
    fn publish_inference_state(&self, policy: Backpressure) {
        let mut buf = Vec::new();
        {
            let state = self.state.read();
            state.state.encode(&mut buf).unwrap();
        }
        self.inference_writer.write(Bytes::from(buf), policy);
    }
}
