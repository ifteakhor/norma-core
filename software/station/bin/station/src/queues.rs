use anyhow::Result;
use bytes::Bytes;
use normfs::NormFS;
use prost::Message;
use station_iface::iface_proto::{
    drivers::QueueDataType,
    envelope::{QueueData, QueueOpt, RootQueueEnvelope, RootQueueEnvelopeType},
};
use station_iface::{Backpressure, QueueWriter, STARTUP_WRITE_TIMEOUT};
use std::sync::Arc;

pub const MAIN_QUEUE_ID: &str = "main";

/// Registrations come through a synchronous trait method that cannot wait, so
/// the channel is sized to never fill.
const MAIN_QUEUE_CAPACITY: usize = 256;

pub struct MainQueue {
    writer: QueueWriter,
    station_uuid: Bytes,
}

impl MainQueue {
    pub async fn new(normfs: Arc<NormFS>, station_uuid: Bytes) -> Result<Self> {
        let queue_id = normfs.resolve(MAIN_QUEUE_ID);
        let writer = QueueWriter::spawn(normfs, queue_id, MAIN_QUEUE_CAPACITY).await?;

        Ok(Self {
            writer,
            station_uuid,
        })
    }

    /// Says which run every later record belongs to.
    pub async fn send_app_start(&self) -> Result<()> {
        let envelope = self.create_envelope(RootQueueEnvelopeType::RqetAppStart, None);
        let mut buf = Vec::new();
        envelope.encode(&mut buf)?;
        self.writer
            .write_awaiting(Bytes::from(buf), STARTUP_WRITE_TIMEOUT)
            .await?;
        Ok(())
    }

    pub fn send_queue_start(
        &self,
        queue_id: &normfs::QueueId,
        data_type: QueueDataType,
        opts: Vec<QueueOpt>,
    ) -> Result<()> {
        let queue_data = QueueData {
            id: queue_id.as_str().to_string(),
            data_type: data_type as i32,
            opts,
        };

        let envelope =
            self.create_envelope(RootQueueEnvelopeType::RqetQueueStart, Some(queue_data));
        self.send_envelope(envelope)
    }

    fn create_envelope(
        &self,
        envelope_type: RootQueueEnvelopeType,
        queue: Option<QueueData>,
    ) -> RootQueueEnvelope {
        RootQueueEnvelope {
            r#type: envelope_type as i32,
            monotonic_stamp_ns: systime::get_monotonic_stamp_ns(),
            local_stamp_ns: systime::get_local_stamp_ns(),
            app_start_id: systime::get_app_start_id(),
            station_uuid: self.station_uuid.clone(),
            queue,
        }
    }

    fn send_envelope(&self, envelope: RootQueueEnvelope) -> Result<()> {
        let mut buf = Vec::new();
        envelope.encode(&mut buf)?;
        self.writer.write(Bytes::from(buf), Backpressure::Keep);
        Ok(())
    }
}
