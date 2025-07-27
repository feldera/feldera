//! Storage metrics.
//!
//! The constants defined in this module are the names of metrics that the
//! backends maintain via [`metrics`] crate interfaces.

use std::sync::atomic::AtomicU64;

/// Total number of files created.
pub static FILES_CREATED: AtomicU64 = AtomicU64::new(0);

/// Total number of files deleted.
pub static FILES_DELETED: AtomicU64 = AtomicU64::new(0);

/// Time in nanoseconds a worker was stalled waiting for more merges to complete.
pub static COMPACTION_STALL_TIME: AtomicU64 = AtomicU64::new(0);

/// Number of records dropped due to LATENESS annotations
pub static TOTAL_LATE_RECORDS: AtomicU64 = AtomicU64::new(0);

/// Total number of DBSP steps executed.
pub static DBSP_STEP: AtomicU64 = AtomicU64::new(0);
