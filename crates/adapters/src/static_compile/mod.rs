//! Code specific to running pipelines in the statically compiled mode.

pub mod catalog;
pub mod deinput;
pub mod deserialize_with_context;
pub mod deserializer_config;
pub mod seroutput;

pub use deinput::{DeMapHandle, DeScalarHandle, DeScalarHandleImpl, DeSetHandle, DeZSetHandle};
pub use deserialize_with_context::{
    DeserializationContext, DeserializeWithContext, FieldParseError,
};
pub use deserializer_config::{DateFormat, SqlDeserializerConfig, TimeFormat, TimestampFormat};
pub use seroutput::SerCollectionHandleImpl;
