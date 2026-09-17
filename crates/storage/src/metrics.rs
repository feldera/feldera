use crate::histogram::ExponentialHistogram;

/// Histogram of read latency, in microseconds.
pub static READ_LATENCY_MICROSECONDS: ExponentialHistogram = ExponentialHistogram::new();

/// Histogram of write latency, in microseconds.
pub static WRITE_LATENCY_MICROSECONDS: ExponentialHistogram = ExponentialHistogram::new();

/// Histogram of latency syncing one file to stable storage, in microseconds.
pub static SYNC_LATENCY_MICROSECONDS: ExponentialHistogram = ExponentialHistogram::new();

/// Histogram of latency making a checkpoint's files durable, in microseconds.
///
/// One observation per `commit_all`, whichever way the backend does it, so the
/// strategies can be compared against each other. Separate from
/// [SYNC_LATENCY_MICROSECONDS], which times a single file: summing that one
/// does not give this one, because the syncs it times overlap.
pub static COMMIT_ALL_LATENCY_MICROSECONDS: ExponentialHistogram = ExponentialHistogram::new();

/// Histogram of read block sizes, in bytes.
pub static READ_BLOCKS_BYTES: ExponentialHistogram = ExponentialHistogram::new();

/// Histogram of write block sizes, in bytes.
pub static WRITE_BLOCKS_BYTES: ExponentialHistogram = ExponentialHistogram::new();
