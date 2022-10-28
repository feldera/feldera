//! Test framework for the `adapters` crate.

use crate::{controller::InputEndpointConfig, Catalog, InputEndpoint, InputTransport};
use serde::Deserialize;
use std::{
    sync::{Arc, Mutex},
    thread::sleep,
    time::{Duration, Instant},
};

mod mock_dezset;
mod mock_input_consumer;

pub use mock_dezset::MockDeZSet;
pub use mock_input_consumer::MockInputConsumer;

/// Wait for `predicate` to become `true`.
///
/// Returns the number of milliseconds elapsed or `None` on timeout.
pub fn wait<P>(mut predicate: P, timeout_ms: Option<u128>) -> Option<u128>
where
    P: FnMut() -> bool,
{
    let start = Instant::now();

    while !predicate() {
        if let Some(timeout_ms) = timeout_ms {
            if start.elapsed().as_millis() >= timeout_ms {
                return None;
            }
        }
        sleep(Duration::from_millis(10));
    }

    Some(start.elapsed().as_millis())
}

/// Build an input pipeline that allows testing a transport endpoint and parser
/// standalone, without a DBSP circuit or controller.
///
/// Creates a mock `Catalog` with a single input handle with name `name`
/// and record type `T` backed by `MockDeZSet` and instantiates the following
/// test pipeline:
///
/// ```text
/// ┌────────┐   ┌─────────────────┐   ┌──────┐   ┌──────────┐
/// │endpoint├──►│MockInputConsumer├──►│parser├──►│MockDeZSet│
/// └────────┘   └─────────────────┘   └──────┘   └──────────┘
/// ```
pub fn mock_input_pipeline<T>(
    name: &str,
    config: InputEndpointConfig,
) -> (Box<dyn InputEndpoint>, MockInputConsumer, MockDeZSet<T>)
where
    T: for<'de> Deserialize<'de> + Send + 'static,
{
    let mut catalog = Catalog::new();
    let input_handle = <MockDeZSet<T>>::new();
    catalog.register_input(name, input_handle.clone());

    let consumer = MockInputConsumer::from_config(&config.format, &Arc::new(Mutex::new(catalog)));

    let transport = <dyn InputTransport>::get_transport(&config.transport.name).unwrap();
    let endpoint = transport
        .new_endpoint(&config.transport.config, Box::new(consumer.clone()))
        .unwrap();

    (endpoint, consumer, input_handle)
}
