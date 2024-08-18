//! Test framework for the `adapters` crate.

use crate::{
    controller::InputEndpointConfig, transport::InputReader, Catalog, CircuitCatalog,
    DbspCircuitHandle, FormatConfig, InputFormat,
};
use anyhow::Result as AnyResult;
use dbsp::{DBData, OrdZSet, Runtime};
use env_logger::Env;
use log::{Log, Metadata, Record};
use pipeline_types::serde_with_context::{
    DeserializeWithContext, SerializeWithContext, SqlSerdeConfig,
};
use std::borrow::Cow;
use std::collections::BTreeMap;
use std::ffi::OsStr;
use std::fs::{read_dir, File};
use std::io::Read;
use std::path::{Path, PathBuf};
use std::{
    io::Write,
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
    generate_test_batch, generate_test_batches, generate_test_batches_with_weights,
    DatabricksPeople, EmbeddedStruct, TestStruct, TestStruct2,
};
use dbsp::circuit::CircuitConfig;
use dbsp::utils::Tup2;
pub use mock_dezset::{wait_for_output_ordered, wait_for_output_unordered, MockDeZSet, MockUpdate};
pub use mock_input_consumer::MockInputConsumer;
pub use mock_output_consumer::MockOutputConsumer;
use pipeline_types::format::json::{JsonFlavor, JsonParserConfig, JsonUpdateFormat};
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
    let schema = Relation::empty();
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
    relation: Relation,
) -> AnyResult<(Box<dyn InputReader>, MockInputConsumer, MockDeZSet<T, U>)>
where
    T: for<'de> DeserializeWithContext<'de, SqlSerdeConfig> + Send + 'static,
    U: for<'de> DeserializeWithContext<'de, SqlSerdeConfig> + Send + 'static,
{
    let default_format = FormatConfig {
        name: Cow::from("json"),
        config: serde_yaml::to_value(JsonParserConfig {
            update_format: JsonUpdateFormat::Raw,
            json_flavor: JsonFlavor::Datagen,
            array: true,
        })
        .unwrap(),
    };

    let (consumer, input_handle) = mock_parser_pipeline(
        config
            .connector_config
            .format
            .as_ref()
            .unwrap_or(&default_format),
    )?;

    let endpoint =
        input_transport_config_to_endpoint(config.connector_config.transport.clone())?.unwrap();

    let reader = endpoint.open(Box::new(consumer.clone()), 0, relation)?;

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

        let input_schema = serde_json::to_string(&Relation::new(
            "test_input1",
            false,
            schema.clone(),
            false,
            BTreeMap::new(),
        ))
        .unwrap();

        let output_schema = serde_json::to_string(&Relation::new(
            "test_output1",
            false,
            schema,
            false,
            BTreeMap::new(),
        ))
        .unwrap();

        catalog.register_materialized_input_zset(input.clone(), hinput, &input_schema);
        catalog.register_materialized_output_zset(input, &output_schema);

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

/// Parse file with data encoded using specified format into a Z-set.
pub fn file_to_zset<T>(file: &mut File, format: &str, format_config_yaml: &str) -> OrdZSet<T>
where
    T: DBData + for<'de> DeserializeWithContext<'de, SqlSerdeConfig>,
{
    let format = <dyn InputFormat>::get_format(format).unwrap();
    let buffer = MockDeZSet::<T, T>::new();

    // Input parsers don't care about schema yet.
    let schema = Relation::new("mock_schema", false, vec![], false, BTreeMap::new());

    let mut parser = format
        .new_parser(
            "BaseConsumer",
            &InputCollectionHandle::new(schema, buffer.clone()),
            &serde_yaml::from_str::<serde_yaml::Value>(format_config_yaml).unwrap(),
        )
        .unwrap();

    let mut bytes = Vec::new();
    file.read_to_end(&mut bytes).unwrap();
    let (_, errors) = parser.input_chunk(&bytes);

    // Use assert_eq, so errors are printed in case of a failure.
    assert_eq!(errors, vec![]);
    let records = buffer.state().flushed.clone();

    OrdZSet::from_tuples(
        (),
        records
            .into_iter()
            .map(|update| match update {
                MockUpdate::Insert(x) => Tup2(Tup2(x, ()), 1),
                MockUpdate::Delete(x) => Tup2(Tup2(x, ()), -1),
                MockUpdate::Update(_) => panic!("Unexpected MockUpdate::Update"),
            })
            .collect::<Vec<_>>(),
    )
}

pub(crate) fn init_test_logger() {
    let _ = env_logger::Builder::from_env(Env::default().default_filter_or("info"))
        .is_test(true)
        .format(move |buf, record| {
            let t = chrono::Utc::now();
            let t = format!("{}", t.format("%Y-%m-%d %H:%M:%S"));
            writeln!(
                buf,
                "{t} {} {}",
                buf.default_styled_level(record.level()),
                record.args()
            )
        })
        .try_init();
}
