//! Transport adapter for Google PubSub

mod input;

#[cfg(all(any(test, feature = "testing"), feature = "pubsub-gcp-test"))]
pub mod gcp_tests;
#[cfg(any(test, feature = "testing"))]
pub mod tests;

pub use input::PubSubInputEndpoint;
