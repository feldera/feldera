//! Test framework for the `adapters` crate.

use crate::{
    controller::InputEndpointConfig, transport::InputReader, Catalog, CircuitCatalog,
    DbspCircuitHandle, DeserializeWithContext, FormatConfig, InputTransport, SqlSerdeConfig,
};
use anyhow::Result as AnyResult;
use dbsp::Runtime;
use log::{Log, Metadata, Record};
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

use crate::catalog::InputCollectionHandle;
pub use data::{
    generate_test_batch, generate_test_batches, generate_test_batches_with_weights,
    test_struct_schema, TestStruct,
};
use dbsp::circuit::CircuitConfig;
pub use mock_dezset::{MockDeZSet, MockUpdate};
pub use mock_input_consumer::MockInputConsumer;
pub use mock_output_consumer::MockOutputConsumer;
use pipeline_types::program_schema::Relation;

pub struct TestLogger;

pub static TEST_LOGGER: TestLogger = TestLogger;

pub static DEFAULT_TIMEOUT_MS: u128 = 600_000;

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
pub fn wait<P>(mut predicate: P, timeout_ms: u128) -> Option<u128>
where
    P: FnMut() -> bool,
{
    let start = Instant::now();

    while !predicate() {
        if start.elapsed().as_millis() >= timeout_ms {
            return None;
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
pub fn mock_parser_pipeline<T, U>(
    config: &FormatConfig,
) -> AnyResult<(MockInputConsumer, MockDeZSet<T, U>)>
where
    T: for<'de> DeserializeWithContext<'de, SqlSerdeConfig> + Send + 'static,
    U: for<'de> DeserializeWithContext<'de, SqlSerdeConfig> + Send + 'static,
{
    let input_handle = <MockDeZSet<T, U>>::new();
    // Input parsers don't care about schema yet.
    let schema = Relation::new("mock_schema", false, vec![]);
    let consumer = MockInputConsumer::from_handle(
        &InputCollectionHandle::new(schema, input_handle.clone()),
        config,
    );
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
pub fn mock_input_pipeline<T, U>(
    config: InputEndpointConfig,
) -> AnyResult<(Box<dyn InputReader>, MockInputConsumer, MockDeZSet<T, U>)>
where
    T: for<'de> DeserializeWithContext<'de, SqlSerdeConfig> + Send + 'static,
    U: for<'de> DeserializeWithContext<'de, SqlSerdeConfig> + Send + 'static,
{
    let (consumer, input_handle) = mock_parser_pipeline(&config.connector_config.format)?;

    let transport =
        <dyn InputTransport>::get_transport(&config.connector_config.transport.name).unwrap();
    let endpoint = transport.new_endpoint(&config.connector_config.transport.config)?;

    let reader = endpoint.open(Box::new(consumer.clone()), 0)?;

    Ok((reader, consumer, input_handle))
}

/// Create a simple test circuit that passes the input stream right through to
/// the output.
// TODO: parameterize with the number (and types?) of input and output streams.

pub fn test_circuit(
    config: CircuitConfig,
) -> (Box<dyn DbspCircuitHandle>, Box<dyn CircuitCatalog>) {
    let (circuit, catalog) = Runtime::init_circuit(config, |circuit| {
        let mut catalog = Catalog::new();
        let (input, hinput) = circuit.add_input_zset::<TestStruct>();

        // Use bogus schema until any of the tests care about having a real one.
        let input_schema =
            serde_json::to_string(&Relation::new("test_input1", false, vec![])).unwrap();

        let output_schema =
            serde_json::to_string(&Relation::new("test_output1", false, vec![])).unwrap();

        catalog.register_input_zset(input.clone(), hinput, &input_schema);
        catalog.register_output_zset(input, &output_schema);

        Ok(catalog)
    })
    .unwrap();
    (Box::new(circuit), Box::new(catalog))
}
