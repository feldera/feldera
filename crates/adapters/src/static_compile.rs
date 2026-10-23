//! Code specific to running pipelines in the statically compiled mode.

pub(crate) mod bootstrap;
pub mod catalog;
pub mod deinput;
pub(crate) mod metadata;
pub mod seroutput;

pub use deinput::{DeLazyMapHandle, DeMapHandle, DeScalarHandle, DeScalarHandleImpl, DeZSetHandle};
pub use seroutput::SerCollectionHandleImpl;
