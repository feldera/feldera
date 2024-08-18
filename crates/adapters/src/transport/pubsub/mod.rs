//! Transport adapter for Google PubSub

mod input;

#[cfg(any(feature = "pubsub-emulator-test", feature = "pubsub-gcp-test"))]
#[cfg(test)]
mod test;

pub use input::PubSubInputEndpoint;
