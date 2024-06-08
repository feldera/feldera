//! Storage metrics.
//!
//! The constants defined in this module are the names of metrics that the
//! backends maintain via [`metrics`] crate interfaces.
use ::metrics::{describe_counter, describe_histogram, Unit};

/// Total number of files created.
pub const FILES_CREATED: &str = "disk.total_files_created";

/// Total number of files deleted.
pub const FILES_DELETED: &str = "disk.total_files_deleted";

/// Total number of successful disk writes.
pub const WRITES_SUCCESS: &str = "disk.total_writes_success";

/// Total number of failed disk writes.
pub const WRITES_FAILED: &str = "disk.total_writes_failed";

/// Total number of successful disk reads.
pub const READS_SUCCESS: &str = "disk.total_reads_success";

/// Total number of failed disk reads.
pub const READS_FAILED: &str = "disk.total_reads_failed";

/// Total number of bytes successfully written.
pub const TOTAL_BYTES_WRITTEN: &str = "disk.total_bytes_written";

/// Total number of bytes successfully read.
pub const TOTAL_BYTES_READ: &str = "disk.total_bytes_read";

/// Histogram of read latency.
pub const READ_LATENCY: &str = "disk.read_latency";

/// Histogram of write latency.
pub const WRITE_LATENCY: &str = "disk.write_latency";

/// Total number of buffer cache hits.
pub const BUFFER_CACHE_HIT: &str = "disk.buffer_cache_hit";

/// Total number of buffer cache misses.
pub const BUFFER_CACHE_MISS: &str = "disk.buffer_cache_miss";

/// Total number of compactions done by compaction thread.
pub const TOTAL_COMPACTIONS: &str = "file.compacted";

/// Sizes of batches that get compacted.
pub const COMPACTION_SIZE: &str = "file.compaction_size";

/// Compaction duration for a single batch.
pub const COMPACTION_DURATION: &str = "file.compaction_duration";

/// Adds descriptions for the metrics we expose.
pub(super) fn describe_disk_metrics() {
    // Storage backend metrics.
    describe_counter!(FILES_CREATED, "total number of files created");
    describe_counter!(FILES_DELETED, "total number of files deleted");
    describe_counter!(WRITES_SUCCESS, "total number of disk writes");
    describe_counter!(WRITES_FAILED, "total number of failed writes");
    describe_counter!(READS_SUCCESS, "total number of disk reads");
    describe_counter!(READS_FAILED, "total number of failed reads");

    describe_counter!(
        TOTAL_BYTES_WRITTEN,
        Unit::Bytes,
        "total number of bytes written to disk"
    );
    describe_counter!(
        TOTAL_BYTES_READ,
        Unit::Bytes,
        "total number of bytes read from disk"
    );

    describe_histogram!(READ_LATENCY, Unit::Seconds, "Read request latency");
    describe_histogram!(WRITE_LATENCY, Unit::Seconds, "Write request latency");

    // Buffer cache metrics.
    describe_counter!(BUFFER_CACHE_HIT, "total number of buffer cache hits");
    describe_counter!(BUFFER_CACHE_MISS, "total number of buffer cache misses");

    // Compactor metrics.
    describe_counter!(TOTAL_COMPACTIONS, "total number of compactions");
    describe_histogram!(
        COMPACTION_SIZE,
        Unit::Count,
        "Batch sizes encountered in compaction"
    );
    describe_histogram!(COMPACTION_DURATION, Unit::Seconds, "Time to compact batch");
}
