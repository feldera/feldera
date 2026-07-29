//! Code specific to running pipelines in the statically compiled mode.

pub mod catalog;
pub mod deinput;
pub mod seroutput;

pub use deinput::{DeMapHandle, DeScalarHandle, DeScalarHandleImpl, DeZSetHandle};
pub use seroutput::SerCollectionHandleImpl;
