//! Code specific to running pipelines in the statically compiled mode.

pub mod catalog;
pub mod deinput;
pub mod deserialize_with_context;
pub mod serde_config;
pub mod serialize_with_context;
pub mod seroutput;

pub use deinput::{DeMapHandle, DeScalarHandle, DeScalarHandleImpl, DeSetHandle, DeZSetHandle};
pub use deserialize_with_context::{
    DeserializationContext, DeserializeWithContext, FieldParseError,
};
pub use serde_config::{DateFormat, SqlSerdeConfig, TimeFormat, TimestampFormat};
pub use serialize_with_context::{SerializationContext, SerializeWithContext};
pub use seroutput::SerCollectionHandleImpl;
