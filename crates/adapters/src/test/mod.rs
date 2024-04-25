//! Test framework for the `adapters` crate.

use crate::{
    controller::InputEndpointConfig, transport::InputReader, Catalog, CircuitCatalog,
    DbspCircuitHandle, FormatConfig,
};
use anyhow::Result as AnyResult;
use dbsp::{DBData, Runtime};
use log::{Log, Metadata, Record};
use pipeline_types::serde_with_context::{
    DeserializeWithContext, SerializeWithContext, SqlSerdeConfig,
};
use std::ffi::OsStr;
use std::fs::read_dir;
use std::path::{Path, PathBuf};
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
use crate::transport::input_transport_config_to_endpoint;
pub use data::{
    generate_test_batch, generate_test_batches, generate_test_batches_with_weights, TestStruct,
    TestStruct2,
};
use dbsp::circuit::CircuitConfig;
pub use mock_dezset::{MockDeZSet, MockUpdate};
pub use mock_input_consumer::MockInputConsumer;
pub use mock_output_consumer::MockOutputConsumer;
use pipeline_types::program_schema::{Field, Relation};

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
/// Returns the number of milliseconds elapsed or `Err(())` on timeout.
pub fn wait<P>(mut predicate: P, timeout_ms: u128) -> Result<u128, ()>
where
    P: FnMut() -> bool,
{
    let start = Instant::now();

    while !predicate() {
        if start.elapsed().as_millis() >= timeout_ms {
            return Err(());
        }
        sleep(Duration::from_millis(10));
    }

    Ok(start.elapsed().as_millis())
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
    let (consumer, input_handle) =
        mock_parser_pipeline(config.connector_config.format.as_ref().unwrap())?;

    let endpoint =
        input_transport_config_to_endpoint(config.connector_config.transport.clone())?.unwrap();

    let reader = endpoint.open(Box::new(consumer.clone()), 0)?;

    Ok((reader, consumer, input_handle))
}

/// Create a simple test circuit that passes the input stream right through to
/// the output.
// TODO: parameterize with the number (and types?) of input and output streams.

pub fn test_circuit<T>(
    config: CircuitConfig,
    schema: &[Field],
) -> (Box<dyn DbspCircuitHandle>, Box<dyn CircuitCatalog>)
where
    T: DBData
        + SerializeWithContext<SqlSerdeConfig>
        + for<'de> DeserializeWithContext<'de, SqlSerdeConfig>
        + Sync,
{
    let schema = schema.to_vec();
    let (circuit, catalog) = Runtime::init_circuit(config, move |circuit| {
        let mut catalog = Catalog::new();
        let (input, hinput) = circuit.add_input_zset::<T>();

        let input_schema =
            serde_json::to_string(&Relation::new("test_input1", false, schema.clone())).unwrap();

        let output_schema =
            serde_json::to_string(&Relation::new("test_output1", false, schema)).unwrap();

        catalog.register_input_zset(input.clone(), hinput, &input_schema);
        catalog.register_output_zset(input, &output_schema);

        Ok(catalog)
    })
    .unwrap();
    (Box::new(circuit), Box::new(catalog))
}

pub fn list_files_recursive(dir: &Path, extension: &OsStr) -> Result<Vec<PathBuf>, std::io::Error> {
    let mut result = Vec::new();

    // Iterate over the entries in the directory
    for entry in read_dir(dir)? {
        let entry = entry?;
        let path = entry.path();

        // If the entry is a directory, recursively call list_files_recursive
        if path.is_dir() {
            result.append(&mut list_files_recursive(&path, extension)?);
        } else if path.extension() == Some(extension) {
            result.push(path);
        }
    }
    Ok(result)
}
