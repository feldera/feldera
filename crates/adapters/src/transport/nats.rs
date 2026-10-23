//! Transport adapter for NATS

pub mod input;
pub mod output;

#[cfg(any(test, feature = "testing"))]
pub mod input_tests;
#[cfg(any(test, feature = "testing"))]
pub mod pipeline_tests;

pub use input::NatsInputEndpoint;
pub use output::NatsOutputEndpoint;
