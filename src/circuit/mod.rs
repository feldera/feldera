//! Synchronous circuits over streams.
//!
//! A circuit consists of [operators](`operator_traits::Operator`) connected by
//! [streams](`circuit_builder::Stream`). At every clock cycle, each operator in
//! the circuit is triggered, consuming a single value from each of its input
//! streams and emitting a single value to the output stream.

mod activations;
mod dbsp_handle;

pub(crate) mod runtime;

pub mod cache;
pub mod circuit_builder;
pub mod operator_traits;
pub mod schedule;
pub mod trace;

pub use activations::{Activations, Activator};
pub use circuit_builder::{
    Circuit, CircuitHandle, ExportId, ExportStream, FeedbackConnector, GlobalNodeId, NodeId,
    OwnershipPreference, Scope, Stream,
};
pub use dbsp_handle::DBSPHandle;
pub use runtime::{Error as RuntimeError, LocalStore, LocalStoreMarker, Runtime, RuntimeHandle};

pub use schedule::Error as SchedulerError;
