//! Test framework for the `adapters` crate.

use crate::{controller::InputEndpointConfig, Catalog, InputEndpoint, InputTransport};
use dbsp::{DBSPHandle, Runtime};
use serde::Deserialize;
use std::{
    sync::{Arc, Mutex},
    thread::sleep,
    time::{Duration, Instant},
};

mod data;
mod mock_dezset;
mod mock_input_consumer;

pub use data::{generate_test_batch, generate_test_batches, TestStruct};
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
    catalog.register_input_collection_handle(name, input_handle.clone());

    let consumer = MockInputConsumer::from_config(&config.format, &Arc::new(Mutex::new(catalog)));

    let transport = <dyn InputTransport>::get_transport(&config.transport.name).unwrap();
    let endpoint = transport
        .new_endpoint(&config.transport.config, Box::new(consumer.clone()))
        .unwrap();

    (endpoint, consumer, input_handle)
}

/// Create a simple test circuit that passes the input stream right through to
/// the output.
// TODO: parameterize with the number (and types?) of input and output streams.
pub fn test_circuit(workers: usize) -> (DBSPHandle, Catalog) {
    let (circuit, (input, output)) = Runtime::init_circuit(workers, |circuit| {
        let (input, hinput) = circuit.add_input_zset::<TestStruct, i32>();

        let houtput = input.output();
        (hinput, houtput)
    })
    .unwrap();

    let mut catalog = Catalog::new();
    catalog.register_input_zset_handle("test_input1", input);
    catalog.register_output_batch_handle("test_output1", output);

    (circuit, catalog)
}
