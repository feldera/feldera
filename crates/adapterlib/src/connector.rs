//! Connector descriptor and global registry.
//!
//! This module defines the [`ConnectorDescriptor`] type that connector crates
//! submit via the [`register_connector!`] macro.  The controller and
//! pipeline-manager discover all registered connectors at runtime through the
//! [`registered_connectors`] and [`connector_by_name`] discovery functions.
//!
//! # Quick start
//!
//! ```rust,ignore
//! use feldera_adapterlib::connector::{
//!     ConnectorDescriptor, ConnectorFlags, ConnectorKind, Direction,
//! };
//! use feldera_types::config::FtModel;
//!
//! static MY_CONNECTOR: ConnectorDescriptor = ConnectorDescriptor {
//!     name: "my_connector",
//!     direction: Direction::Input,
//!     kind: ConnectorKind::Regular,
//!     fault_tolerance: Some(FtModel::AtLeastOnce),
//!     config_schema: || serde_json::json!({}),
//!     default_format: None,
//!     flags: ConnectorFlags::EMPTY,
//!     build_input: Some(my_build_input),
//!     build_output: None,
//!     build_integrated_input: None,
//!     build_integrated_output: None,
//! };
//!
//! feldera_adapterlib::register_connector!(&MY_CONNECTOR);
//! ```
//!
//! # Naming
//!
//! This type is intentionally named `ConnectorDescriptor` rather than
//! `ConnectorMetadata` to avoid collision with [`crate::ConnectorMetadata`],
//! which describes per-record metadata such as the Kafka topic name or Avro
//! schema ID that accompany individual output rows.

use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::AtomicU64;

use anyhow::{Error as AnyError, Result as AnyResult};
use feldera_types::adapter_stats::ConnectorHealth;
use feldera_types::config::{FormatConfig, FtModel};
use feldera_types::program_schema::Relation;
use serde_json::Value as JsonValue;

use crate::transport::{
    InputConsumer, IntegratedInputEndpoint, IntegratedOutputEndpoint, OutputEndpoint,
    TransportInputEndpoint,
};

// ── Direction ─────────────────────────────────────────────────────────────────

/// Which side(s) of a pipeline a connector can serve.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Direction {
    /// Can only be used as an input (source) connector.
    Input,
    /// Can only be used as an output (sink) connector.
    Output,
    /// Can be used as either an input or an output connector.
    InputOutput,
}

impl Direction {
    /// Returns `true` if this direction allows use as an input.
    pub fn allows_input(self) -> bool {
        matches!(self, Direction::Input | Direction::InputOutput)
    }

    /// Returns `true` if this direction allows use as an output.
    pub fn allows_output(self) -> bool {
        matches!(self, Direction::Output | Direction::InputOutput)
    }
}

// ── ConnectorKind ─────────────────────────────────────────────────────────────

/// Implementation style of a connector.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConnectorKind {
    /// Transport and format are separate layers (the standard case for file,
    /// Kafka, S3, …).  The controller selects a `Parser` / `Encoder` based on
    /// the `format` field of the connector config.
    Regular,
    /// Transport and format are tightly coupled (Postgres, Delta Lake, …).
    /// The connector creates its own record-level deserializer or serializer.
    Integrated,
    /// Connector is ephemeral and not re-created when the pipeline restarts
    /// from a checkpoint (HTTP direct, Clock, AdHoc).
    Transient,
}

// ── ConnectorFlags ────────────────────────────────────────────────────────────

bitflags::bitflags! {
    /// Capability flags for a connector.
    #[derive(Default, Copy, Clone, Debug, PartialEq, Eq)]
    pub struct ConnectorFlags: u32 {
        /// Connector receives data over a direct HTTP connection managed by the
        /// controller's built-in HTTP server (e.g. `http_input`).
        const HTTP_DIRECT = 0x01;

        /// Controller automatically re-creates this connector when resuming
        /// from a checkpoint (e.g. `clock`).
        const AUTO_RECREATED_ON_RESTART = 0x02;
    }
}

impl ConnectorFlags {
    /// No flags set.
    pub const EMPTY: Self = Self::empty();
}

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

// ── ConnectorDescriptor ───────────────────────────────────────────────────────

/// Describes a connector and provides factory functions for constructing it.
///
/// Connector crates submit a `&'static ConnectorDescriptor` via the
/// [`register_connector!`] macro.  The controller discovers all descriptors
/// at runtime via [`registered_connectors`] and [`connector_by_name`].
///
/// Exactly one of the four `build_*` fields should be `Some`; which one
/// depends on the connector's `kind` and `direction`.
pub struct ConnectorDescriptor {
    /// Serde tag name that appears as the `name` field in `TransportConfig`.
    /// Must be globally unique; the describer binary (Phase 8) fails fast on
    /// duplicate names.
    pub name: &'static str,

    /// Direction(s) this connector supports.
    pub direction: Direction,

    /// Implementation style (regular, integrated, or transient).
    pub kind: ConnectorKind,

    /// Best-case fault tolerance this connector can deliver at runtime.
    /// `None` means no fault tolerance.
    pub fault_tolerance: Option<FtModel>,

    /// Returns the JSON Schema for this connector's config type.
    ///
    /// Used by the `GET /v0/connectors` discovery endpoint (Phase 9) and the
    /// web-console config form (Phase 10).
    pub config_schema: fn() -> JsonValue,

    /// Optional default `FormatConfig` injected by the controller when no
    /// `format` is specified in the connector configuration.  Used by the
    /// `datagen` connector which defaults to JSON-Datagen format.
    pub default_format: Option<fn() -> FormatConfig>,

    /// Capability flags (currently empty; see [`ConnectorFlags`] for Phase 5
    /// additions).
    pub flags: ConnectorFlags,

    /// Factory for regular input endpoints.  Set this for `kind == Regular`
    /// and `direction` that allows input.
    pub build_input: Option<BuildInputFn>,

    /// Factory for regular output endpoints.  Set this for `kind == Regular`
    /// and `direction` that allows output.
    pub build_output: Option<BuildOutputFn>,

    /// Factory for integrated input endpoints.  Set this for
    /// `kind == Integrated` and `direction` that allows input.
    pub build_integrated_input: Option<BuildIntegratedInputFn>,

    /// Factory for integrated output endpoints.  Set this for
    /// `kind == Integrated` and `direction` that allows output.
    pub build_integrated_output: Option<BuildIntegratedOutputFn>,
}

// SAFETY: ConnectorDescriptor contains only 'static fn pointers and 'static str
// references; it is safe to share across threads.
unsafe impl Sync for ConnectorDescriptor {}
unsafe impl Send for ConnectorDescriptor {}

inventory::collect!(&'static ConnectorDescriptor);

// ── Discovery API ─────────────────────────────────────────────────────────────

/// Returns an iterator over every registered [`ConnectorDescriptor`].
///
/// The order of iteration is determined by link order and is not guaranteed to
/// be stable across runs.
pub fn registered_connectors() -> impl Iterator<Item = &'static ConnectorDescriptor> {
    inventory::iter::<&'static ConnectorDescriptor>
        .into_iter()
        .copied()
}

/// Look up a registered connector by its serde `name`.
///
/// Returns `None` if no connector with that name has been registered.
pub fn connector_by_name(name: &str) -> Option<&'static ConnectorDescriptor> {
    registered_connectors().find(|d| d.name == name)
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use feldera_types::config::FtModel;

    fn stub_config_schema() -> JsonValue {
        serde_json::json!({ "type": "object" })
    }

    fn stub_build_input(
        _config: &JsonValue,
        _endpoint_name: &str,
        _secrets_dir: &Path,
    ) -> AnyResult<Box<dyn TransportInputEndpoint>> {
        unimplemented!("stub — not called in registry tests")
    }

    static STUB_INPUT_DESCRIPTOR: ConnectorDescriptor = ConnectorDescriptor {
        name: "__test_stub_input__",
        direction: Direction::Input,
        kind: ConnectorKind::Regular,
        fault_tolerance: Some(FtModel::AtLeastOnce),
        config_schema: stub_config_schema,
        default_format: None,
        flags: ConnectorFlags::EMPTY,
        build_input: Some(stub_build_input),
        build_output: None,
        build_integrated_input: None,
        build_integrated_output: None,
    };

    crate::register_connector!(&STUB_INPUT_DESCRIPTOR);

    static STUB_OUTPUT_DESCRIPTOR: ConnectorDescriptor = ConnectorDescriptor {
        name: "__test_stub_output__",
        direction: Direction::Output,
        kind: ConnectorKind::Regular,
        fault_tolerance: None,
        config_schema: stub_config_schema,
        default_format: None,
        flags: ConnectorFlags::EMPTY,
        build_input: None,
        build_output: None,
        build_integrated_input: None,
        build_integrated_output: None,
    };

    crate::register_connector!(&STUB_OUTPUT_DESCRIPTOR);

    #[test]
    fn registered_connectors_are_iterable() {
        let names: Vec<&str> = registered_connectors().map(|d| d.name).collect();
        assert!(
            names.contains(&"__test_stub_input__"),
            "stub input connector not found; got {names:?}"
        );
        assert!(
            names.contains(&"__test_stub_output__"),
            "stub output connector not found; got {names:?}"
        );
    }

    #[test]
    fn connector_by_name_returns_correct_descriptor() {
        let desc = connector_by_name("__test_stub_input__")
            .expect("stub input connector not found by name");
        assert_eq!(desc.name, "__test_stub_input__");
        assert_eq!(desc.direction, Direction::Input);
        assert_eq!(desc.kind, ConnectorKind::Regular);
        assert_eq!(desc.fault_tolerance, Some(FtModel::AtLeastOnce));
        assert!(desc.build_input.is_some());
        assert!(desc.build_output.is_none());
    }

    #[test]
    fn connector_by_name_returns_none_for_unknown() {
        assert!(connector_by_name("__nonexistent_connector__").is_none());
    }

    #[test]
    fn direction_helpers() {
        assert!(Direction::Input.allows_input());
        assert!(!Direction::Input.allows_output());
        assert!(!Direction::Output.allows_input());
        assert!(Direction::Output.allows_output());
        assert!(Direction::InputOutput.allows_input());
        assert!(Direction::InputOutput.allows_output());
    }

    #[test]
    fn connector_flags_contains() {
        let flags = ConnectorFlags::HTTP_DIRECT | ConnectorFlags::AUTO_RECREATED_ON_RESTART;
        assert!(flags.contains(ConnectorFlags::HTTP_DIRECT));
        assert!(flags.contains(ConnectorFlags::AUTO_RECREATED_ON_RESTART));
        assert!(flags.contains(ConnectorFlags::HTTP_DIRECT | ConnectorFlags::AUTO_RECREATED_ON_RESTART));
        assert!(!flags.contains(ConnectorFlags::from_bits_retain(0b0100)));
        assert!(ConnectorFlags::EMPTY.contains(ConnectorFlags::EMPTY));
        assert!(!ConnectorFlags::EMPTY.contains(ConnectorFlags::HTTP_DIRECT));
    }
}
