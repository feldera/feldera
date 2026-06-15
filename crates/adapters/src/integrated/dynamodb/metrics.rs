//! Connector-specific metrics for the DynamoDB output connector.

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use feldera_adapterlib::metrics::{ConnectorHistogram, ConnectorMetrics, ValueType};
use feldera_storage::histogram::ExponentialHistogram;

/// DynamoDB-specific metrics exported to Prometheus alongside the standard
/// output-endpoint statistics.
#[derive(Default)]
pub(crate) struct DynamoDBOutputMetrics {
    /// Write retries performed against DynamoDB.  A rising value signals
    /// throttling or contention on the target table.
    retries: AtomicU64,

    /// Item-writes rejected by DynamoDB for lack of capacity, counted each time
    /// `BatchWriteItem` returns them as unprocessed.  Unlike `retries`, which
    /// counts retry rounds regardless of cause, this isolates throttling and
    /// points at raising table capacity rather than connector concurrency.
    throttled_items: AtomicU64,

    /// `TransactWriteItems` requests that failed and were retried.  This is the
    /// transactional-mode analogue of `throttled_items`: in transactional mode a
    /// failure surfaces as a whole-request error rather than as unprocessed
    /// items.
    transact_write_failures: AtomicU64,

    /// Output records dropped after exhausting the configured write retries.
    /// This can only happen in bounded-retry mode; a non-zero value means data
    /// failed to reach the target table and was discarded.
    failed_items: AtomicU64,

    /// Output records dropped because they exceed DynamoDB's 400 KB item-size
    /// limit.  Such a record can never be written, so the connector drops it
    /// while encoding rather than letting it fail the `BatchWriteItem` chunk or
    /// `TransactWriteItems` transaction it would otherwise share with valid
    /// records.  A non-zero value means data was discarded.
    oversized_items_dropped: AtomicU64,

    /// Distribution of the time that worker threads spent blocked waiting for a
    /// write slot (the per-worker `max_concurrent_requests` semaphore).  A
    /// shift toward longer waits is backpressure: the connector is producing
    /// writes faster than DynamoDB accepts them, so consider raising
    /// `max_concurrent_requests` or table capacity.  Recorded and exported in
    /// microseconds.
    semaphore_wait_ms: ExponentialHistogram,

    /// `BatchWriteItem` / `TransactWriteItems` requests submitted.  This is the
    /// DynamoDB request count that drives both cost and throttling.
    write_chunks: AtomicU64,

    /// Distribution of the time spent awaiting individual DynamoDB write API
    /// calls. Each retry attempt records a separate observation.
    write_call_latency: ExponentialHistogram,

    /// Output records skipped because their key was not unique.  A non-zero
    /// value points to a view that does not satisfy the connector's uniqueness
    /// requirement.
    duplicate_keys_skipped: AtomicU64,
}

impl DynamoDBOutputMetrics {
    /// Add the retries performed while committing a batch.
    pub(super) fn record_retries(&self, retries: u64) {
        self.retries.fetch_add(retries, Ordering::Relaxed);
    }

    /// Add `count` item-writes returned by DynamoDB as unprocessed (throttled).
    pub(super) fn record_throttled_items(&self, count: u64) {
        self.throttled_items.fetch_add(count, Ordering::Relaxed);
    }

    /// Record a failed `TransactWriteItems` request (which is then retried).
    pub(super) fn record_transact_write_failure(&self) {
        self.transact_write_failures.fetch_add(1, Ordering::Relaxed);
    }

    /// Add `count` records dropped after exhausting the configured retries.
    pub(super) fn record_failed_items(&self, count: u64) {
        self.failed_items.fetch_add(count, Ordering::Relaxed);
    }

    /// Record an output record dropped for exceeding the DynamoDB item-size limit.
    pub(super) fn record_oversized_item_dropped(&self) {
        self.oversized_items_dropped.fetch_add(1, Ordering::Relaxed);
    }

    /// Record the time a worker spent blocked acquiring a write slot.
    pub(super) fn record_semaphore_wait(&self, elapsed: Duration) {
        self.semaphore_wait_ms.record_duration(elapsed);
    }

    /// Record submission of a single DynamoDB write chunk.
    pub(super) fn record_write_chunk(&self) {
        self.write_chunks.fetch_add(1, Ordering::Relaxed);
    }

    /// Record the latency of a single DynamoDB write API call attempt.
    pub(super) fn record_write_call_latency(&self, elapsed: Duration) {
        self.write_call_latency.record_duration(elapsed);
    }

    /// Record an output record skipped due to a non-unique key.
    pub(super) fn record_duplicate_key_skip(&self) {
        self.duplicate_keys_skipped.fetch_add(1, Ordering::Relaxed);
    }
}

impl ConnectorMetrics for DynamoDBOutputMetrics {
    fn metrics(&self) -> Vec<(&'static str, &'static str, ValueType, f64)> {
        vec![
            (
                "dynamodb_output_retries_total",
                "Total number of DynamoDB write retries performed by the output connector.",
                ValueType::Counter,
                self.retries.load(Ordering::Relaxed) as f64,
            ),
            (
                "dynamodb_output_throttled_items_total",
                "Total number of item-writes returned by DynamoDB as unprocessed due to \
                 insufficient capacity (throttling).",
                ValueType::Counter,
                self.throttled_items.load(Ordering::Relaxed) as f64,
            ),
            (
                "dynamodb_output_transact_write_failures_total",
                "Total number of TransactWriteItems requests that failed and were retried.",
                ValueType::Counter,
                self.transact_write_failures.load(Ordering::Relaxed) as f64,
            ),
            (
                "dynamodb_output_failed_items_total",
                "Total number of output records dropped after exhausting the configured write \
                 retries.",
                ValueType::Counter,
                self.failed_items.load(Ordering::Relaxed) as f64,
            ),
            (
                "dynamodb_output_oversized_items_dropped_total",
                "Total number of output records dropped because they exceed DynamoDB's 400 KB \
                 item-size limit.",
                ValueType::Counter,
                self.oversized_items_dropped.load(Ordering::Relaxed) as f64,
            ),
            (
                "dynamodb_output_write_chunks_total",
                "Total number of DynamoDB write chunks (BatchWriteItem/TransactWriteItems requests) \
                 submitted by the output connector.",
                ValueType::Counter,
                self.write_chunks.load(Ordering::Relaxed) as f64,
            ),
            (
                "dynamodb_output_duplicate_keys_skipped_total",
                "Total number of output records skipped because their key was not unique.",
                ValueType::Counter,
                self.duplicate_keys_skipped.load(Ordering::Relaxed) as f64,
            ),
        ]
    }

    fn histograms(&self) -> Vec<ConnectorHistogram> {
        vec![
            ConnectorHistogram {
                name: "dynamodb_output_semaphore_wait_microseconds",
                help: "Time worker threads spent blocked waiting for a write slot (the per-worker \
                       max_concurrent_requests semaphore).",
                snapshot: self.semaphore_wait_ms.snapshot(),
            },
            ConnectorHistogram {
                name: "dynamodb_output_write_call_latency_microseconds",
                help: "Latency of individual DynamoDB write API calls \
                       (BatchWriteItem/TransactWriteItems), including retry attempts.",
                snapshot: self.write_call_latency.snapshot(),
            },
        ]
    }
}
