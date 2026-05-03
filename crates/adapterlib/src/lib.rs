//! # Feldera adapter library — plugin ABI
//!
//! This crate is the stable API contract for Feldera I/O connectors.  A connector crate
//! must depend **only** on `feldera-adapterlib` (plus `feldera-types` for config types);
//! it must never depend on `dbsp` directly.
//!
//! ## Supported plugin-facing types
//!
//! The following types and traits form the supported, versioned plugin ABI.
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
//! - [`transport::IntegratedInputEndpoint`] — implement for connectors that cannot
//!   separate transport from format (e.g. Postgres, Delta Lake).
//! - [`catalog::InputCollectionHandle`] — passed to `open`; access `.schema`
//!   ([`feldera_types::program_schema::Relation`]) and `.handle`
//!   ([`catalog::DeCollectionHandle`]) to configure a deserializer.
//!
//! ### Output connectors
//! - [`transport::OutputEndpoint`] — implement this.
//! - [`format::Encoder`] / [`format::OutputFormat`] — implement for custom data formats.
//! - [`format::OutputConsumer`] — passed by the controller.
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

/// Re-exports of all connector metadata types from [`feldera_adapterlib_meta`].
///
/// Connector crates that only need to register metadata descriptors (not build
/// fn pointers) can use these re-exports instead of depending on
/// `feldera-adapterlib-meta` directly.
pub mod meta {
    pub use feldera_adapterlib_meta::*;
}
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
