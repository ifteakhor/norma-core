//! Writing into NormFS from anywhere in the station.
//!
//! `NormFS::enqueue` waits when every page of a queue still holds records
//! that are not yet on disk. The station writes from places that cannot wait:
//! camera capture loops, a raw `std::thread` reading `/dev/kmsg`,
//! `spawn_blocking` workers, and NormFS subscriber callbacks -- which run
//! while the source queue holds its append gate, so blocking one stalls
//! command ingestion for everybody.
//!
//! Two ways in, and a queue uses one of them, never both:
//!
//! - A producer on its own async task calls [`enqueue_with`], which decides
//!   per record whether to wait (bounded by [`WRITE_TIMEOUT`]) or to refuse
//!   without waiting.
//! - A producer that cannot wait at all gets a [`QueueWriter`]: one writer
//!   task owns the only `enqueue` running for that queue, reached through a
//!   bounded channel and a `try_send` that never blocks. Back-pressure
//!   arrives as a full channel.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use bytes::Bytes;
use normfs::{NormFS, QueueId};
use tokio::sync::{mpsc, oneshot};

/// Slots [`Backpressure::Skip`] will not touch, so a burst of frames cannot
/// leave a "drive disconnected" with nowhere to go.
const RESERVE: usize = 8;

/// Roughly a second of a 30 fps camera. More only adds latency.
pub const DEFAULT_CAPACITY: usize = 32;

pub const WRITE_TIMEOUT: Duration = Duration::from_secs(5);

/// Startup records are worth more patience: nothing reads without them.
pub const STARTUP_WRITE_TIMEOUT: Duration = Duration::from_secs(30);

const REPORT_INTERVAL: Duration = Duration::from_secs(5);

/// Which record is dropped when the queue is behind. Neither variant blocks
/// the caller.
///
/// The test is whether anything later says the same thing: nothing repeats an
/// app start, a registration or a command's result, while a frame is
/// superseded by the next one.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Backpressure {
    Keep,
    Skip,
}

/// Writes one record straight into NormFS from an async task that owns no
/// [`QueueWriter`] for the queue.
///
/// [`Backpressure::Skip`] refuses rather than waits, and a full queue is not
/// an error: the caller's next record says the same thing. [`Backpressure::Keep`]
/// waits for a page, but no longer than [`WRITE_TIMEOUT`], so a stalled disk
/// costs the caller a logged loss rather than a frozen task.
///
/// Errors other than a full queue are returned for the caller to log with
/// whatever context it has.
pub async fn enqueue_with(
    normfs: &NormFS,
    queue_id: &QueueId,
    data: Bytes,
    policy: Backpressure,
) -> Result<(), normfs::Error> {
    match policy {
        Backpressure::Skip => match normfs.try_enqueue(queue_id, data) {
            Ok(_) | Err(normfs::Error::WouldBlock) => Ok(()),
            Err(e) => Err(e),
        },
        Backpressure::Keep => {
            match tokio::time::timeout(WRITE_TIMEOUT, normfs.enqueue(queue_id, data)).await {
                Ok(outcome) => outcome.map(|_| ()),
                Err(_) => Err(timed_out()),
            }
        }
    }
}

fn timed_out() -> normfs::Error {
    normfs::Error::Io(std::io::Error::new(
        std::io::ErrorKind::TimedOut,
        "no page became free in time",
    ))
}

struct Record {
    data: Bytes,
    wait: Duration,
    ack: Option<oneshot::Sender<Result<(), normfs::Error>>>,
}

#[derive(Default)]
struct Counters {
    /// Refused by a full channel, counted by the producer.
    dropped: AtomicU64,
    /// Accepted but not placed, counted by the writer task.
    failed: AtomicU64,
}

/// A queue's write handle.
///
/// Every clone feeds the same writer task, so records reach the queue in the
/// order the producers handed them over. Cloning allocates nothing: hot paths
/// clone one per record.
#[derive(Clone)]
pub struct QueueWriter {
    tx: mpsc::Sender<Record>,
    queue_id: Arc<QueueId>,
    counters: Arc<Counters>,
}

impl QueueWriter {
    /// Starts the queue for writing and spawns its writer task.
    ///
    /// The `ensure_queue_exists_for_write` inside is what guarantees a WAL
    /// writer is attached before anything enqueues.
    pub async fn spawn(
        normfs: Arc<NormFS>,
        queue_id: QueueId,
        capacity: usize,
    ) -> Result<Self, normfs::Error> {
        normfs.ensure_queue_exists_for_write(&queue_id).await?;

        let capacity = capacity.max(RESERVE * 2);
        let (tx, rx) = mpsc::channel(capacity);
        let counters = Arc::new(Counters::default());

        tokio::spawn(run(normfs, queue_id.clone(), rx, Arc::clone(&counters)));

        Ok(Self {
            tx,
            queue_id: Arc::new(queue_id),
            counters,
        })
    }

    pub async fn open(normfs: Arc<NormFS>, queue_id: QueueId) -> Result<Self, normfs::Error> {
        Self::spawn(normfs, queue_id, DEFAULT_CAPACITY).await
    }

    pub fn queue_id(&self) -> &QueueId {
        &self.queue_id
    }

    pub fn write(&self, data: Bytes, policy: Backpressure) {
        self.send(Self::record(data, WRITE_TIMEOUT), policy);
    }

    fn send(&self, record: Record, policy: Backpressure) {
        match policy {
            Backpressure::Keep => self.must_send(record),
            Backpressure::Skip => self.skip_send(record),
        }
    }

    /// Stops at [`RESERVE`] rather than at the end of the channel, so a
    /// stream of these cannot crowd out a record that has to be kept.
    fn skip_send(&self, record: Record) {
        if self.tx.capacity() <= RESERVE || self.tx.try_send(record).is_err() {
            self.counters.dropped.fetch_add(1, Ordering::Relaxed);
        }
    }

    /// Never blocks either -- the caller may be a subscriber callback or a
    /// capture thread. Exhausting the reserve means a long stall, and is
    /// worth an error line rather than a silent drop.
    fn must_send(&self, record: Record) {
        if let Err(mpsc::error::TrySendError::Full(_)) = self.tx.try_send(record) {
            self.counters.dropped.fetch_add(1, Ordering::Relaxed);
            log::error!(
                "queue '{}' is so far behind that a record that cannot be dropped was dropped",
                self.queue_id
            );
        }
    }

    /// For the few records whose arrival the caller has to know about before
    /// it goes on.
    pub async fn write_awaiting(&self, data: Bytes, wait: Duration) -> Result<(), normfs::Error> {
        let (ack_tx, ack_rx) = oneshot::channel();
        let record = Record {
            data,
            wait,
            ack: Some(ack_tx),
        };
        if self.tx.send(record).await.is_err() {
            return Err(Self::gone(&self.queue_id));
        }
        ack_rx
            .await
            .unwrap_or_else(|_| Err(Self::gone(&self.queue_id)))
    }

    fn record(data: Bytes, wait: Duration) -> Record {
        Record {
            data,
            wait,
            ack: None,
        }
    }

    fn gone(queue_id: &QueueId) -> normfs::Error {
        normfs::Error::Io(std::io::Error::other(format!(
            "the writer task for queue '{queue_id}' is gone"
        )))
    }
}

/// What the last report said, so a quiet writer stays quiet and a recovered
/// one does not keep looking like it is still losing records.
#[derive(Default)]
struct Reported {
    dropped: u64,
    failed: u64,
    losing: bool,
}

async fn run(
    normfs: Arc<NormFS>,
    queue_id: QueueId,
    mut rx: mpsc::Receiver<Record>,
    counters: Arc<Counters>,
) {
    let mut report = tokio::time::interval(REPORT_INTERVAL);
    report.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    let mut reported = Reported::default();

    loop {
        tokio::select! {
            _ = report.tick() => report_losses(&queue_id, &counters, &mut reported),
            record = rx.recv() => {
                let Some(record) = record else { break };
                let outcome =
                    write_one(&normfs, &queue_id, record.data, record.wait, &counters).await;
                let closed = matches!(outcome, Err(normfs::Error::QueueClosed));
                if let Some(ack) = record.ack {
                    let _ = ack.send(outcome);
                }
                if closed {
                    log::info!("queue '{queue_id}' is closed; its writer is stopping");
                    break;
                }
            }
        }
    }

    report_losses(&queue_id, &counters, &mut reported);
}

async fn write_one(
    normfs: &Arc<NormFS>,
    queue_id: &QueueId,
    data: Bytes,
    wait: Duration,
    counters: &Counters,
) -> Result<(), normfs::Error> {
    let len = data.len();
    match tokio::time::timeout(wait, normfs.enqueue(queue_id, data)).await {
        Ok(Ok(_)) => Ok(()),
        Ok(Err(e)) => {
            counters.failed.fetch_add(1, Ordering::Relaxed);
            if !matches!(e, normfs::Error::QueueClosed) {
                log::error!("queue '{queue_id}' refused a {len} byte record: {e}");
            }
            Err(e)
        }
        Err(_) => {
            counters.failed.fetch_add(1, Ordering::Relaxed);
            // NormFS logs which page is holding the queue up and whether
            // the durable watermark is still moving.
            log::error!(
                "queue '{queue_id}' had no room for a {len} byte record within {wait:?}; \
                 the record is lost and the station carries on"
            );
            Err(timed_out())
        }
    }
}

fn report_losses(queue_id: &QueueId, counters: &Counters, reported: &mut Reported) {
    let dropped = counters.dropped.load(Ordering::Relaxed);
    let failed = counters.failed.load(Ordering::Relaxed);

    if dropped > reported.dropped || failed > reported.failed {
        log::warn!(
            "queue '{queue_id}' is losing records: {} skipped and {} refused in the last {:?} \
             ({dropped} and {failed} since start)",
            dropped - reported.dropped,
            failed - reported.failed,
            REPORT_INTERVAL,
        );
        reported.losing = true;
    } else if reported.losing {
        log::info!(
            "queue '{queue_id}' is keeping up again after {dropped} skipped and {failed} \
             refused records"
        );
        reported.losing = false;
    }

    reported.dropped = dropped;
    reported.failed = failed;
}

#[cfg(test)]
mod tests {
    use super::*;
    use normfs::{NormFsSettings, PersistenceMode, ReadPosition, UintN};

    async fn writer(dir: &tempfile::TempDir, name: &str) -> (Arc<NormFS>, QueueWriter) {
        let settings = NormFsSettings {
            persistence_mode: PersistenceMode::MemoryOnly,
            max_disk_usage_per_queue: None,
            ..NormFsSettings::all_active()
        };
        let normfs = Arc::new(
            NormFS::new(dir.path().to_path_buf(), settings)
                .await
                .expect("normfs"),
        );
        let queue_id = normfs.resolve(name);
        let writer = QueueWriter::open(Arc::clone(&normfs), queue_id)
            .await
            .expect("writer");
        (normfs, writer)
    }

    async fn drain(normfs: &NormFS, queue_id: &QueueId, limit: u64) -> Vec<Bytes> {
        let (tx, mut rx) = mpsc::channel(limit as usize + 1);
        normfs
            .read(
                queue_id,
                ReadPosition::Absolute(UintN::from(0u64)),
                limit,
                1,
                tx,
            )
            .await
            .expect("read");
        let mut out = Vec::new();
        while let Some(entry) = rx.recv().await {
            out.push(entry.data);
        }
        out
    }

    #[tokio::test]
    async fn records_reach_the_queue_in_the_order_they_were_handed_over() {
        let dir = tempfile::tempdir().unwrap();
        let (normfs, writer) = writer(&dir, "ordering").await;

        for i in 0u8..7 {
            writer.write(Bytes::from(vec![i]), Backpressure::Keep);
        }
        // The last one waits, so everything handed over before it has landed.
        writer
            .write_awaiting(Bytes::from(vec![7u8]), WRITE_TIMEOUT)
            .await
            .expect("write_awaiting");

        assert_eq!(
            drain(&normfs, writer.queue_id(), 8).await,
            (0u8..8).map(|i| Bytes::from(vec![i])).collect::<Vec<_>>()
        );
    }

    /// The receiver is held rather than drained, which is what a stalled disk
    /// looks like from the producer's side.
    #[test]
    fn skippable_records_stop_at_the_reserve() {
        let (tx, _rx) = mpsc::channel::<Record>(RESERVE * 2);
        let writer = QueueWriter {
            tx,
            queue_id: Arc::new(normfs_types::QueueIdResolver::new("test").resolve("full")),
            counters: Arc::new(Counters::default()),
        };

        for _ in 0..RESERVE * 2 {
            writer.write(Bytes::from_static(b"x"), Backpressure::Skip);
        }
        assert_eq!(writer.tx.capacity(), RESERVE);
        assert_eq!(
            writer.counters.dropped.load(Ordering::Relaxed),
            RESERVE as u64
        );

        for _ in 0..RESERVE {
            writer.write(Bytes::from_static(b"x"), Backpressure::Keep);
        }
        assert_eq!(writer.tx.capacity(), 0, "the reserve is for these");
        assert_eq!(
            writer.counters.dropped.load(Ordering::Relaxed),
            RESERVE as u64,
            "none of them was dropped"
        );
    }
}
