use crate::histogram::ExponentialHistogram;

/// Histogram of read latency, in microseconds.
pub static READ_LATENCY_MICROSECONDS: ExponentialHistogram = ExponentialHistogram::new();

/// Histogram of write latency, in microseconds.
pub static WRITE_LATENCY_MICROSECONDS: ExponentialHistogram = ExponentialHistogram::new();

/// Histogram of latency syncing to stable storage, in microseconds.
pub static SYNC_LATENCY_MICROSECONDS: ExponentialHistogram = ExponentialHistogram::new();

/// Histogram of read block sizes, in bytes.
pub static READ_BLOCKS_BYTES: ExponentialHistogram = ExponentialHistogram::new();

/// Histogram of write block sizes, in bytes.
pub static WRITE_BLOCKS_BYTES: ExponentialHistogram = ExponentialHistogram::new();
