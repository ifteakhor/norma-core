//! Writing into NormFS from the station.
//!
//! `NormFS::enqueue` waits for a free page; `try_enqueue` refuses instead.
//! Which one a record gets is a property of the record, not of the queue:
//! a frame, a telemetry sample or a state snapshot is replaced by the next
//! one and may be skipped, while a connect, a disconnect, a registration or
//! a command echo is said once and is kept.
//!
//! A kept record waits only where the caller can. An async task awaits it,
//! bounded by [`WRITE_TIMEOUT`]. A subscriber callback runs while its queue
//! holds the append gate, and a capture thread has the next frame on the
//! way, so those try once and log a refusal.

use std::time::Duration;

use bytes::Bytes;
use normfs::{NormFS, QueueId};

pub const WRITE_TIMEOUT: Duration = Duration::from_secs(5);

/// Nothing can be read without the startup records, so they get more patience.
pub const STARTUP_WRITE_TIMEOUT: Duration = Duration::from_secs(30);

/// What a full queue means for this record.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Backpressure {
    /// Nothing later repeats it: wait if the caller can, report if it cannot.
    Keep,
    /// The next one supersedes it: a full queue is not an error.
    Skip,
}

/// From an async task. `Keep` waits up to [`WRITE_TIMEOUT`].
pub async fn enqueue_with(
    normfs: &NormFS,
    queue_id: &QueueId,
    data: Bytes,
    policy: Backpressure,
) -> Result<(), normfs::Error> {
    match policy {
        Backpressure::Skip => try_enqueue_with(normfs, queue_id, data, policy),
        Backpressure::Keep => enqueue_waiting(normfs, queue_id, data, WRITE_TIMEOUT).await,
    }
}

/// From an async task, waiting up to `wait` for a page.
pub async fn enqueue_waiting(
    normfs: &NormFS,
    queue_id: &QueueId,
    data: Bytes,
    wait: Duration,
) -> Result<(), normfs::Error> {
    match tokio::time::timeout(wait, normfs.enqueue(queue_id, data)).await {
        Ok(outcome) => outcome.map(|_| ()),
        Err(_) => Err(normfs::Error::Io(std::io::Error::new(
            std::io::ErrorKind::TimedOut,
            "no page became free in time",
        ))),
    }
}

/// From a place that cannot wait. A full queue is `Ok` for `Skip` and
/// `Err(WouldBlock)` for `Keep`, so the caller can say what was lost.
pub fn try_enqueue_with(
    normfs: &NormFS,
    queue_id: &QueueId,
    data: Bytes,
    policy: Backpressure,
) -> Result<(), normfs::Error> {
    match normfs.try_enqueue(queue_id, data) {
        Ok(_) => Ok(()),
        Err(normfs::Error::WouldBlock) if policy == Backpressure::Skip => Ok(()),
        Err(e) => Err(e),
    }
}
