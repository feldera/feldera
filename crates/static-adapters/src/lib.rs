//! The connector runtime of pipelines that the SQL compiler compiles to Rust, on the DBSP
//! engine.
//!
//! The controller, the server, the transports, and the formats live in `dbsp_adapters`, which
//! reaches the engine only through its engine traits. This crate is the engine's side: it
//! registers a circuit's streams in a [`Catalog`] through the record types that the compiler
//! generates, implements the engine traits over `DBSPHandle` and `Runtime`, and provides the
//! entry points that the generated code calls, [`server::init_circuit`] and
//! [`server::server_main`].

pub(crate) mod bootstrap;
mod catalog;
pub mod deinput;
pub mod engine;
pub mod format;
pub(crate) mod metadata;
#[cfg(feature = "with-nexmark")]
mod nexmark;
pub mod seroutput;
pub mod server;
pub(crate) mod storage;

// The fixture also serves records with `VARIANT` columns of the deprecated enum `Variant`,
// the type that programs with `SET FELDERA_FLAT_VARIANT = 'OFF'` compile to.
#[cfg(any(test, feature = "testing"))]
#[allow(deprecated)]
pub mod test;

pub use catalog::Catalog;
pub use dbsp_adapters::{CircuitCatalog, ControllerError, RecordFormat};
pub use deinput::{DeLazyMapHandle, DeMapHandle, DeScalarHandle, DeScalarHandleImpl, DeZSetHandle};
pub use seroutput::SerCollectionHandleImpl;
