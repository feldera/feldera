//! Controller statistics used for reporting and flow control.
//!
//! # Design
//!
//! We use a lock-free design to avoid blocking the datapath.  All
//! performance counters are implemented as atomics.  However, updates
//! across multiple counters are not atomic.  More importantly, performance
//! counters can be slightly out of sync with the actual state of the
//! pipeline.  Such discrepancies are bounded and do _not_ accumulate
//! over time.
//!
//! # Example
//!
//! Consider the following interleaving of three threads: transport
//! endpoint 1, transport endpoint 2, and circuit thread:
// The quotes below are needed because of
// https://github.com/rust-lang/rustfmt/issues/4210
//! ```text
//! | Thread        | Action                              | `buffered_input_records` | actual # of buffered records |
//! | ------------- | ----------------------------------- | :----------------------: | :--------------------------: |
//! | 1. endpoint 1 | enqueues 10 records                 |            10            |              10              |
//! | 2. circuit:   | `ControllerStats::consume_counters` |             0            |              10              |
//! | 3. endpoint 2 | enqueue 10 records                  |            10            |              20              |
//! | 4. circuit    | circuit.step()                      |            10            |              0               |
//! ```
//!
//! Here endpoint 2 buffers additional records after the
//! `ControllerStats::buffered_input_records` counter is reset to zero.  As a
//! result, all 20 records enqueued by both endpoints are processed
//! by the circuit, but the counter shows that 10 records are still
//! pending.

use super::{ControllerInnerConfig, EndpointId};
use crossbeam::sync::{ShardedLock, ShardedLockReadGuard, Unparker};
use std::{
    collections::BTreeMap,
    sync::atomic::{AtomicU64, Ordering},
};

/// Controller statistics.
#[derive(Default)]
pub struct ControllerStats {
    /// Total number of records buffered by all endpoints.
    pub buffered_input_records: AtomicU64,

    /// Input endpoint stats.
    // `ShardedLock` is a read/write lock optimized for fast reads.
    // Write access is only required when adding or removing an endpoint.
    // Regular stats updates only require a read lock thanks to the use of
    // atomics.
    pub input_stats: ShardedLock<BTreeMap<EndpointId, InputEndpointStats>>,

    /// Output endpoint stats.
    pub output_stats: ShardedLock<BTreeMap<EndpointId, OutputEndpointStats>>,
}

impl ControllerStats {
    pub fn new() -> Self {
        Self::default()
    }

    /// Initialize stats for a new input endpoint.
    pub fn add_input(&self, endpoint_id: &EndpointId) {
        self.input_stats
            .write()
            .unwrap()
            .insert(*endpoint_id, InputEndpointStats::new());
    }

    /// Initialize stats for a new output endpoint.
    pub fn add_output(&self, endpoint_id: &EndpointId) {
        self.output_stats
            .write()
            .unwrap()
            .insert(*endpoint_id, OutputEndpointStats::new());
    }

    /// Total number of records buffered by all input endpoints.
    pub fn num_buffered_input_records(&self) -> u64 {
        self.buffered_input_records.load(Ordering::Acquire)
    }

    /// Output endpoint stats.
    pub fn output_stats(&self) -> ShardedLockReadGuard<BTreeMap<EndpointId, OutputEndpointStats>> {
        self.output_stats.read().unwrap()
    }

    /// Number of records buffered by the endpoint or 0 if the endpoint
    /// doesn't exist (the latter is possible if the endpoint is being
    /// destroyed).
    pub fn num_input_endpoint_buffered_records(&self, endpoint_id: &EndpointId) -> u64 {
        match &self.input_stats.read().unwrap().get(endpoint_id) {
            None => 0,
            Some(endpoint_stats) => endpoint_stats.buffered_records.load(Ordering::Acquire),
        }
    }

    /// Reset all buffered record and byte counters to zero.
    ///
    /// This method is invoked before `DBSPHandle::step` to indicate that all
    /// buffered data is about to be consumed.  See module-level documentation
    /// for details.
    pub fn consume_buffered_inputs(&self) {
        self.buffered_input_records.store(0, Ordering::Release);
        for endpoint_stats in self.input_stats.read().unwrap().values() {
            endpoint_stats.consume_buffered();
        }
    }

    /// True if the number of records buffered by the endpoint exceeds
    /// its `max_buffered_records` config parameter.
    pub fn input_endpoint_full(
        &self,
        endpoint_id: &EndpointId,
        config: &ControllerInnerConfig,
    ) -> bool {
        let buffered_records = self.num_input_endpoint_buffered_records(endpoint_id);

        let max_buffered_records = match config.inputs.get(endpoint_id) {
            None => return false,
            Some(endpoint_config) => endpoint_config.max_buffered_records,
        };

        buffered_records >= max_buffered_records
    }

    /// Update counters after receiving a new input batch.
    ///
    /// # Arguments
    ///
    /// * `endpoint_id` - id of the input endpoint.
    /// * `num_bytes` - number of bytes received.
    /// * `num_records` - number of records in the deserialized batch.
    /// * `config` - controller config.
    /// * `circuit_thread_unparker` - unparker used to wake up the circuit
    ///   thread if the total number of buffered records exceeds
    ///   `min_batch_size_records`.
    /// * `backpressure_thread_unparker` - unparker used to wake up the the
    ///   backpressure thread if the endpoint is full.
    pub fn input_batch(
        &self,
        endpoint_id: EndpointId,
        num_bytes: usize,
        num_records: usize,
        config: &ControllerInnerConfig,
        circuit_thread_unparker: &Unparker,
        backpressure_thread_unparker: &Unparker,
    ) {
        let num_records = num_records as u64;
        let num_bytes = num_bytes as u64;

        // Increment buffered_records; unpark circuit thread once
        // `min_batch_size_records` is exceeded.
        let old = self
            .buffered_input_records
            .fetch_add(num_records, Ordering::AcqRel);

        if old == 0
            || (old <= config.global.min_batch_size_records
                && old + num_records > config.global.min_batch_size_records)
        {
            circuit_thread_unparker.unpark();
        }

        // There is a potential race condition if the endpoint is currently being
        // removed. In this case, it's safe to ignore this operation.
        let endpoint_config = match config.inputs.get(&endpoint_id) {
            None => return,
            Some(endpoint_config) => endpoint_config,
        };

        // Update endpoint counters; unpark backpressure thread if endpoint's
        // `max_buffered_records` exceeded.
        let input_stats = self.input_stats.read().unwrap();
        if let Some(endpoint_stats) = input_stats.get(&endpoint_id) {
            let old = endpoint_stats.add_buffered(num_bytes, num_records);

            if old < endpoint_config.max_buffered_records
                && old + num_records >= endpoint_config.max_buffered_records
            {
                backpressure_thread_unparker.unpark();
            }
        };
    }

    /// Update counters after receiving an end-of-file event on an input
    /// endpoint.
    ///
    /// # Arguments
    ///
    /// * `endpoint_id` - id of the input endpoint.
    /// * `num_records` - number of records returned by `Parser::eoi`.
    /// * `config` - controller config.
    /// * `circuit_thread_unparker` - unparker used to wake up the circuit
    ///   thread if the total number of buffered records exceeds
    ///   `min_batch_size_records`.
    pub fn eoi(
        &self,
        endpoint_id: EndpointId,
        num_records: usize,
        config: &ControllerInnerConfig,
        circuit_thread_unparker: &Unparker,
    ) {
        let num_records = num_records as u64;

        // Increment buffered_input_records; unpark circuit thread if
        // `min_batch_size_records` exceeded.
        let old = self
            .buffered_input_records
            .fetch_add(num_records, Ordering::AcqRel);
        if old < config.global.min_batch_size_records
            && old + num_records >= config.global.min_batch_size_records
        {
            circuit_thread_unparker.unpark();
        }

        // Update endpoint counters, no need to wake up the backpressure thread since we
        // won't see any more inputs from this endpoint.
        let input_stats = self.input_stats.read().unwrap();
        if let Some(endpoint_stats) = input_stats.get(&endpoint_id) {
            endpoint_stats.add_buffered(0, num_records);
        };
    }

    pub fn enqueue_batch(&self, endpoint_id: EndpointId, num_records: usize) {
        if let Some(endpoint_stats) = self.output_stats().get(&endpoint_id) {
            endpoint_stats.enqueue_batch(num_records);
        }
    }

    pub fn output_batch(
        &self,
        endpoint_id: EndpointId,
        num_records: usize,
        config: &ControllerInnerConfig,
        circuit_thread_unparker: &Unparker,
    ) {
        let endpoint_config = match config.outputs.get(&endpoint_id) {
            None => return,
            Some(endpoint_config) => endpoint_config,
        };

        if let Some(endpoint_stats) = self.output_stats().get(&endpoint_id) {
            let old = endpoint_stats.output_batch(num_records);
            if old < endpoint_config.max_buffered_records
                && old + num_records as u64 >= endpoint_config.max_buffered_records
            {
                circuit_thread_unparker.unpark();
            }
        };
    }

    pub fn output_buffer(&self, endpoint_id: EndpointId, num_bytes: usize) {
        if let Some(endpoint_stats) = self.output_stats().get(&endpoint_id) {
            endpoint_stats.output_buffer(num_bytes);
        };
    }

    pub fn output_buffers_full(&self, config: &ControllerInnerConfig) -> bool {
        self.output_stats()
            .iter()
            .any(|(endpoint_id, endpoint_stats)| {
                if let Some(endpoint_config) = config.outputs.get(endpoint_id) {
                    endpoint_stats.buffered_records.load(Ordering::Acquire)
                        >= endpoint_config.max_buffered_records
                } else {
                    false
                }
            })
    }
}

/// Input endpoint statistics.
pub struct InputEndpointStats {
    /// Total bytes pushed to the endpoint since it was created.
    pub total_bytes: AtomicU64,

    /// Total records pushed to the endpoint since it was created.
    pub total_records: AtomicU64,

    /// Number of bytes currently buffered by the endpoint
    /// (not yet consumed by the circuit).
    pub buffered_bytes: AtomicU64,

    /// Number of records currently buffered by the endpoint
    /// (not yet consumed by the circuit).
    pub buffered_records: AtomicU64,
}

impl InputEndpointStats {
    fn new() -> Self {
        Self {
            total_bytes: AtomicU64::new(0),
            total_records: AtomicU64::new(0),
            buffered_bytes: AtomicU64::new(0),
            buffered_records: AtomicU64::new(0),
        }
    }

    fn consume_buffered(&self) {
        self.buffered_bytes.store(0, Ordering::Release);
        self.buffered_records.store(0, Ordering::Release);
    }

    /// Increment the number of buffered bytes and records; return
    /// the previous number of buffered records.
    fn add_buffered(&self, num_bytes: u64, num_records: u64) -> u64 {
        if num_bytes > 0 {
            // We are only updating statistics here, so no need to pay for
            // strong consistency.
            self.total_bytes.fetch_add(num_bytes, Ordering::Relaxed);
            self.buffered_bytes.fetch_add(num_bytes, Ordering::AcqRel);
        }

        self.total_records.fetch_add(num_records, Ordering::Relaxed);
        self.buffered_records
            .fetch_add(num_records, Ordering::AcqRel)
    }
}

/// Output endpoint statistics.
#[derive(Default)]
pub struct OutputEndpointStats {
    pub transmitted_records: AtomicU64,
    pub transmitted_bytes: AtomicU64,

    pub buffered_records: AtomicU64,
    pub buffered_batches: AtomicU64,
}

/// Public read API.
impl OutputEndpointStats {
    pub fn transmitted_records(&self) -> u64 {
        self.transmitted_records.load(Ordering::Acquire)
    }
}

impl OutputEndpointStats {
    fn new() -> Self {
        Self::default()
    }

    fn enqueue_batch(&self, num_records: usize) {
        self.buffered_records
            .fetch_add(num_records as u64, Ordering::AcqRel);
        self.buffered_batches.fetch_add(1, Ordering::AcqRel);
    }

    fn output_batch(&self, num_records: usize) -> u64 {
        self.transmitted_records
            .fetch_add(num_records as u64, Ordering::Relaxed);

        let old = self
            .buffered_records
            .fetch_sub(num_records as u64, Ordering::AcqRel);
        self.buffered_batches.fetch_sub(1, Ordering::AcqRel);
        old
    }

    fn output_buffer(&self, num_bytes: usize) {
        self.transmitted_bytes
            .fetch_add(num_bytes as u64, Ordering::Relaxed);
    }
}
