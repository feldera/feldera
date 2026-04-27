//! # Feldera adapter library — plugin ABI
//!
//! This crate is the stable API contract for Feldera I/O connectors.  A connector crate
//! must depend **only** on `feldera-adapterlib` (plus `feldera-types` for config types);
//! it must never depend on `dbsp` directly.
//!
//! ## Supported plugin-facing types
//!
//! The following types and traits form the supported, versioned plugin ABI.  Types marked
//! `#[doc(hidden)]` are still part of the contract — they just appear in call sites rather
//! than as something a plugin declares.
//!
//! ### Input connectors (regular — transport + separate format)
//! - [`transport::InputEndpoint`] / [`transport::TransportInputEndpoint`] — implement these.
//! - [`transport::InputReader`] — return from `TransportInputEndpoint::open`.
//! - [`format::Parser`] / [`format::InputFormat`] — implement for custom data formats.
//! - [`format::InputBuffer`] / [`StagedBuffers`] — return from parser methods.
//! - [`transport::InputConsumer`] — passed by the controller; call to report progress.
//! - [`transport::InputReaderCommand`] / [`transport::Resume`] — FT state machine.
//!
//! ### Input connectors (integrated — transport + format combined)
//! - [`transport::IntegratedInputEndpoint`] (`#[doc(hidden)]`) — implement for connectors
//!   that cannot separate transport from format (e.g. Postgres, Delta Lake).
//! - [`catalog::InputCollectionHandle`] (`#[doc(hidden)]`) — passed to `open`; access
//!   `.schema` ([`feldera_types::program_schema::Relation`]) and `.handle`
//!   ([`catalog::DeCollectionHandle`]) to configure a deserializer.
//!
//! ### Output connectors
//! - [`transport::OutputEndpoint`] — implement this.
//! - [`format::Encoder`] / [`format::OutputFormat`] — implement for custom data formats.
//! - [`format::OutputConsumer`] (`#[doc(hidden)]`) — passed by the controller.
//! - [`catalog::SerBatchReader`] / [`catalog::SerCursor`] — iterate output batches.
//!
//! ### Shared
//! - [`ConnectorMetrics`](metrics::ConnectorMetrics) — report connector-level metrics.
//! - [`transport::CommandHandler`] — optional; handle connector-specific REST commands.
//! - `FtModel` (re-exported from `feldera-types`) — advertise your FT level.
//!
//! DBSP types that appear in the ABI surface are re-exported here so that connector
//! crates never need to name `dbsp::*` types directly:
//! - [`StagedBuffers`] — used as the return type of [`format::Parser::stage`].
//! - [`reexports`] — advanced DBSP types for key-based output operations.
//!
//! ## Fault-tolerance contract
//!
//! 1. Advertise your FT capability via [`transport::InputEndpoint::fault_tolerance`].
//!    Return `None` for no FT, `Some(FtModel::AtLeastOnce)` or `Some(FtModel::ExactlyOnce)`.
//! 2. After each step, pass [`transport::Resume`] to [`transport::InputConsumer::extended`].
//!    `Resume::Barrier` means no replay is possible; `Resume::Seek` means the endpoint can
//!    seek past already-read data; `Resume::Replay` means it can replay byte-for-byte.
//! 3. The controller drives replay by sending [`transport::InputReaderCommand::Replay`].
//!    The endpoint must flush exactly the buffered data from that step and then call
//!    [`transport::InputConsumer::replayed`].
//!
//! ## SemVer policy
//!
//! `feldera-adapterlib` follows Semantic Versioning.  The minor version is bumped each
//! Feldera release; a major bump is required for any breaking change to the plugin ABI.
//! CI runs [`cargo-semver-checks`](https://github.com/obi1kenobi/cargo-semver-checks)
//! on every merge to catch accidental breaking changes before they are released.
//! Connector crates should depend on `feldera-adapterlib = "0"` (or the current major)
//! with an exact lower bound matching the version they were compiled against.

use bytemuck::NoUninit;
pub use dbsp::DetailedError as DbspDetailedError;
pub use dbsp::operator::StagedBuffers;
use num_derive::FromPrimitive;
use serde::Serialize;

pub mod catalog;
pub mod connector;
mod connector_metadata;
pub mod errors;
pub mod format;
pub mod metrics;
pub mod preprocess;
/// Re-exports of DBSP types that appear in the plugin ABI for advanced use cases.
///
/// Most connectors do not need these.  They are provided so connector crates can avoid
/// naming `dbsp::*` types directly in signatures where these types are returned by
/// [`catalog::SerBatchReader`] or [`catalog::SerCursor`] methods.
pub mod reexports {
    pub use dbsp::dynamic::{DynData, DynVec, Factory};
}
pub mod transport;
pub mod utils;

pub use connector_metadata::ConnectorMetadata;
pub use connector::{connector_by_name, registered_connectors};

/// Register a [`connector::ConnectorDescriptor`] with the global connector
/// registry.
///
/// Connector crates call this macro at module level to make their connector
/// discoverable at runtime by the controller and pipeline-manager.
///
/// # Example
///
/// ```rust,ignore
/// use feldera_adapterlib::connector::{
///     ConnectorDescriptor, ConnectorFlags, ConnectorKind, Direction,
/// };
/// use feldera_types::config::FtModel;
///
/// static MY_CONNECTOR: ConnectorDescriptor = ConnectorDescriptor {
///     name: "my_connector",
///     direction: Direction::Input,
///     kind: ConnectorKind::Regular,
///     fault_tolerance: Some(FtModel::AtLeastOnce),
///     config_schema: || serde_json::json!({}),
///     default_format: None,
///     flags: ConnectorFlags::EMPTY,
///     build_input: Some(my_build_input_fn),
///     build_output: None,
///     build_integrated_input: None,
///     build_integrated_output: None,
/// };
///
/// feldera_adapterlib::register_connector!(&MY_CONNECTOR);
/// ```
#[macro_export]
macro_rules! register_connector {
    ($descriptor:expr $(,)?) => {
        ::inventory::submit! { $descriptor }
    };
}

/// Register a [`format::InputFormat`] implementation with the global input
/// format registry.
///
/// Format modules call this macro at module level.  Pass a reference to a
/// unit-struct factory that implements [`format::InputFormat`].  The reference
/// must be const-evaluable (unit structs and `static` variables are both fine).
///
/// # Example
///
/// ```rust,ignore
/// struct MyInputFormat;
/// impl InputFormat for MyInputFormat { /* … */ }
///
/// feldera_adapterlib::register_input_format!(
///     &MyInputFormat as &dyn feldera_adapterlib::format::InputFormat
/// );
/// ```
#[macro_export]
macro_rules! register_input_format {
    ($factory:expr $(,)?) => {
        ::inventory::submit! { $factory }
    };
}

/// Register a [`format::OutputFormat`] implementation with the global output
/// format registry.
///
/// # Example
///
/// ```rust,ignore
/// struct MyOutputFormat;
/// impl OutputFormat for MyOutputFormat { /* … */ }
///
/// feldera_adapterlib::register_output_format!(
///     &MyOutputFormat as &dyn feldera_adapterlib::format::OutputFormat
/// );
/// ```
#[macro_export]
macro_rules! register_output_format {
    ($factory:expr $(,)?) => {
        ::inventory::submit! { $factory }
    };
}

#[doc(hidden)]
#[derive(Copy, Clone, Debug, Default, PartialEq, Eq, FromPrimitive, Serialize, NoUninit)]
#[repr(u8)]
pub enum PipelineState {
    /// All input endpoints are paused (or are in the process of being paused).
    #[default]
    Paused,

    /// Controller is running.
    Running,

    /// Controller is being terminated.
    Terminated,
}
