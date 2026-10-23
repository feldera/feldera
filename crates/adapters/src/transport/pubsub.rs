//! Transport adapter for Google PubSub

mod input;

#[cfg(any(test, feature = "testing"))]
pub mod gcp_tests;
#[cfg(any(test, feature = "testing"))]
pub mod tests;

pub use input::PubSubInputEndpoint;
