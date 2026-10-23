//! Transport adapter for Google PubSub

mod input;

#[cfg(any(test, feature = "testing"))]
pub mod test;

pub use input::PubSubInputEndpoint;
