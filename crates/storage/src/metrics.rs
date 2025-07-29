use crate::histogram::ExponentialHistogram;

/// Histogram of read latency.
pub static READ_LATENCY: ExponentialHistogram = ExponentialHistogram::new();

/// Histogram of write latency.
pub static WRITE_LATENCY: ExponentialHistogram = ExponentialHistogram::new();

/// Histogram of latency syncing to stable storage.
pub static SYNC_LATENCY: ExponentialHistogram = ExponentialHistogram::new();
