//! Test framework for the `adapters` crate.

use crate::{
    controller::InputEndpointConfig, Catalog, CircuitCatalog, DbspCircuitHandle, FormatConfig,
    InputEndpoint, InputTransport,
};
use anyhow::Result as AnyResult;
use dbsp::Runtime;
use log::{Log, Metadata, Record};
use serde::Deserialize;
use std::{
    thread::sleep,
    time::{Duration, Instant},
};

mod data;

#[cfg(feature = "with-kafka")]
pub mod kafka;

pub mod http;

mod mock_dezset;
mod mock_input_consumer;
mod mock_output_consumer;

pub use data::{
    generate_test_batch, generate_test_batches, generate_test_batches_with_weights, TestStruct,
};
pub use mock_dezset::MockDeZSet;
pub use mock_input_consumer::MockInputConsumer;
pub use mock_output_consumer::MockOutputConsumer;

pub struct TestLogger;
pub static TEST_LOGGER: TestLogger = TestLogger;

impl Log for TestLogger {
    fn enabled(&self, _metadata: &Metadata) -> bool {
        true
    }

    fn log(&self, record: &Record) {
        println!("{} - {}", record.level(), record.args());
    }

    fn flush(&self) {}
}

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

/// Build an input pipeline that allows testing a parser
/// standalone, without a DBSP circuit or controller.
///
/// ```text
/// ┌─────────────────┐   ┌──────┐   ┌──────────┐
/// │MockInputConsumer├──►│parser├──►│MockDeZSet│
/// └─────────────────┘   └──────┘   └──────────┘
/// ```
pub fn mock_parser_pipeline<T>(
    config: &FormatConfig,
) -> AnyResult<(MockInputConsumer, MockDeZSet<T>)>
where
    T: for<'de> Deserialize<'de> + Send + 'static,
{
    let input_handle = <MockDeZSet<T>>::new();
    let consumer = MockInputConsumer::from_handle(&input_handle, config);
    Ok((consumer, input_handle))
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
    config: InputEndpointConfig,
) -> AnyResult<(Box<dyn InputEndpoint>, MockInputConsumer, MockDeZSet<T>)>
where
    T: for<'de> Deserialize<'de> + Send + 'static,
{
    let (consumer, input_handle) = mock_parser_pipeline(&config.connector_config.format)?;

    let transport =
        <dyn InputTransport>::get_transport(&config.connector_config.transport.name).unwrap();
    let mut endpoint = transport.new_endpoint(&config.connector_config.transport.config)?;

    endpoint.connect(Box::new(consumer.clone()))?;

    Ok((endpoint, consumer, input_handle))
}

/// Create a simple test circuit that passes the input stream right through to
/// the output.
// TODO: parameterize with the number (and types?) of input and output streams.
pub fn test_circuit(workers: usize) -> (Box<dyn DbspCircuitHandle>, Box<dyn CircuitCatalog>) {
    let (circuit, catalog) = Runtime::init_circuit(workers, |circuit| {
        let mut catalog = Catalog::new();
        let (input, hinput) = circuit.add_input_zset::<TestStruct, i32>();

        catalog.register_input_zset("test_input1", input.clone(), hinput);
        catalog.register_output_zset("test_output1", input);

        Ok(catalog)
    })
    .unwrap();
    (Box::new(circuit), Box::new(catalog))
}
