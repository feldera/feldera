//! Transport adapter for Google PubSub

mod input;

pub use input::build_pub_sub_input;

#[cfg(any(feature = "pubsub-emulator-test", feature = "pubsub-gcp-test"))]
#[cfg(test)]
mod test;

