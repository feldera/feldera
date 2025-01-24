//! Synchronous circuits over streams.
//!
//! A circuit consists of [operators](`operator_traits::Operator`) connected by
//! [streams](`circuit_builder::Stream`). At every clock cycle, each operator
//! consumes a single value from each of its input streams and emits a single
//! value to the output stream (except that nested circuits can execute multiple
//! operations for each outer clock tick).
//!
//! Use [`RootCircuit::build`] to create and populate an circuit that executes in
//! the calling thread, or [`Runtime::init_circuit`] to create a multi-circuit,
//! multi-worker threaded runtime.  These functions return a [`CircuitHandle`]
//! or [`DBSPHandle`], respectively, that control the circuits' execution,
//! plus, when used in the recommended way, additional input handles for
//! feeding data into the circuits and output handles for obtaining their
//! output.

mod dbsp_handle;

pub(crate) mod runtime;

#[macro_use]
pub mod metadata;
pub mod cache;
pub mod checkpointer;
pub mod circuit_builder;
mod fingerprinter;
pub mod metrics;
pub mod operator_traits;
pub mod schedule;
pub mod tokio;
pub mod trace;

pub use circuit_builder::{
    ChildCircuit, Circuit, CircuitHandle, ExportId, ExportStream, FeedbackConnector, GlobalNodeId,
    NestedCircuit, NodeId, OwnershipPreference, RootCircuit, Scope, Stream, WithClock,
};
pub use dbsp_handle::{
    CircuitConfig, CircuitStorageConfig, DBSPHandle, Host, Layout, StorageCacheConfig,
    StorageConfig, StorageOptions,
};
pub use runtime::{Error as RuntimeError, LocalStore, LocalStoreMarker, Runtime, RuntimeHandle};

pub use schedule::Error as SchedulerError;

#[cfg(test)]
pub(crate) use dbsp_handle::tests::mkconfig;
