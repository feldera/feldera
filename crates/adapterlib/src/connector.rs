//! Connector build-fn types and serializable manifest entry.
//!
//! Connector metadata is registered in
//! [`feldera_adapterlib_meta::CONNECTOR_METADATA_REGISTRY`] via
//! `#[linkme::distributed_slice]`; this module only houses the build-fn type
//! aliases used by the per-pipeline dispatch table and a thin
//! [`ConnectorManifestEntry`] wrapper that re-uses the meta crate's type for
//! ABI stability.

use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::AtomicU64;

use anyhow::{Error as AnyError, Result as AnyResult};
use feldera_types::adapter_stats::ConnectorHealth;
use feldera_types::program_schema::Relation;
use serde_json::Value as JsonValue;

use crate::transport::{
    InputConsumer, IntegratedInputEndpoint, IntegratedOutputEndpoint, OutputEndpoint,
    TransportInputEndpoint,
};

// ── Re-export the data-only types from the meta crate ────────────────────────
//
// Most callers reach these through `feldera_adapterlib::meta::*` already; the
// re-exports here keep `feldera_adapterlib::connector::ConnectorManifestEntry`
// (and the enums it carries) working for existing code that imports them from
// this module.
pub use feldera_adapterlib_meta::{
    ConnectorFlags, ConnectorKind, ConnectorManifestEntry, Direction,
};

// ── OutputControllerRef ───────────────────────────────────────────────────────

/// Callbacks from an integrated output connector back to the pipeline
/// controller.
///
/// [`BuildIntegratedOutputFn`] receives an `Arc<dyn OutputControllerRef>` so
/// that an integrated output connector can report transport errors and health
/// status without depending on the concrete `ControllerInner` type from
/// `dbsp_adapters`.
///
/// In `dbsp_adapters`, `ControllerInner` implements this trait.
pub trait OutputControllerRef: Send + Sync {
    /// Report a transport-level error to the controller.
    ///
    /// If `fatal` is `true`, the controller will shut down the pipeline.
    fn output_transport_error(
        &self,
        endpoint_id: u64,
        endpoint_name: &str,
        fatal: bool,
        error: AnyError,
        tag: Option<&str>,
    );

    /// Update the health status for this output endpoint.
    fn update_output_connector_health(&self, endpoint_id: u64, health: ConnectorHealth);

    /// Register an `AtomicU64` counter that the connector increments with
    /// the number of records written in each batch.  The controller reads
    /// this counter to populate per-endpoint throughput metrics.
    ///
    /// Called once during endpoint initialisation (e.g. in `new()`).
    fn register_batch_progress_counter(&self, endpoint_id: &u64, counter: Arc<AtomicU64>);

    /// Record that `num_bytes` / `num_records` were written to the output
    /// buffer for the given endpoint in the current batch.
    fn output_buffer(&self, endpoint_id: u64, num_bytes: usize, num_records: usize);
}

// ── Build function types ──────────────────────────────────────────────────────

/// Factory function for regular input connectors.
///
/// - `config` — secrets-resolved transport config serialised as JSON.
/// - `endpoint_name` — human-readable name used in error messages.
/// - `secrets_dir` — directory containing secret files.
pub type BuildInputFn = fn(
    config: &JsonValue,
    endpoint_name: &str,
    secrets_dir: &Path,
) -> AnyResult<Box<dyn TransportInputEndpoint>>;

/// Factory function for regular output connectors.
///
/// - `fault_tolerant` — if `true` the caller prefers an exactly-once endpoint;
///   the factory may return a lower-guarantee endpoint if that is all it
///   supports.
pub type BuildOutputFn = fn(
    config: &JsonValue,
    endpoint_name: &str,
    fault_tolerant: bool,
    secrets_dir: &Path,
) -> AnyResult<Box<dyn OutputEndpoint>>;

/// Factory function for integrated input connectors.
pub type BuildIntegratedInputFn = fn(
    config: &JsonValue,
    endpoint_name: &str,
    consumer: Box<dyn InputConsumer>,
) -> AnyResult<Box<dyn IntegratedInputEndpoint>>;

/// Factory function for integrated output connectors.
///
/// - `endpoint_id` — stable numeric ID used for metrics and error reporting.
/// - `controller` — handle for reporting transport errors back to the
///   controller without taking a dependency on `ControllerInner`.
/// - `is_restart` — `true` when the pipeline is resuming from a checkpoint.
pub type BuildIntegratedOutputFn = fn(
    endpoint_id: u64,
    endpoint_name: &str,
    config: &JsonValue,
    key_schema: &Option<Relation>,
    schema: &Relation,
    controller: Arc<dyn OutputControllerRef>,
    is_restart: bool,
) -> AnyResult<Box<dyn IntegratedOutputEndpoint>>;
