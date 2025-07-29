use crate::histogram::ExponentialHistogram;

/// Histogram of read latency, in microseconds.
pub static READ_LATENCY: ExponentialHistogram = ExponentialHistogram::new();

/// Histogram of write latency, in microseconds.
pub static WRITE_LATENCY: ExponentialHistogram = ExponentialHistogram::new();

/// Histogram of latency syncing to stable storage, in microseconds.
pub static SYNC_LATENCY: ExponentialHistogram = ExponentialHistogram::new();
