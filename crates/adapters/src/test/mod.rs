//! Test framework for the `adapters` crate.

#![allow(clippy::type_complexity)]

use crate::{
    controller::InputEndpointConfig, transport::InputReader, Catalog, CircuitCatalog, FormatConfig,
};
use anyhow::Result as AnyResult;
use dbsp::{DBData, DBSPHandle, OrdZSet, Runtime};
use feldera_adapterlib::format::InputBuffer;
use feldera_types::serde_with_context::{
    DeserializeWithContext, SerializeWithContext, SqlSerdeConfig,
};
use std::borrow::Cow;
use std::collections::BTreeMap;
use std::ffi::OsStr;
use std::fs::{read_dir, File};
use std::hash::Hash;
use std::io::Read;
use std::path::{Path, PathBuf};
use std::{
    fmt::Debug,
    thread::sleep,
    time::{Duration, Instant},
};
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;
use tracing_subscriber::EnvFilter;

mod data;

#[cfg(feature = "with-kafka")]
pub mod kafka;

#[cfg(feature = "with-redis")]
pub mod redis;

pub mod http;

mod mock_dezset;
mod mock_input_consumer;
mod mock_output_consumer;

mod datagen;

#[cfg(all(
    feature = "with-iceberg",
    any(
        feature = "iceberg-tests-fs",
        feature = "iceberg-tests-glue",
        feature = "iceberg-tests-rest"
    )
))]
mod iceberg;

use crate::catalog::InputCollectionHandle;
use crate::format::get_input_format;
use crate::transport::input_transport_config_to_endpoint;
pub use data::{
    generate_test_batch, generate_test_batches, generate_test_batches_with_weights,
    DatabricksPeople, DeltaTestStruct, EmbeddedStruct, IcebergTestStruct, KeyStruct, TestStruct,
    TestStruct2,
};
use dbsp::circuit::CircuitConfig;
use dbsp::utils::Tup2;
use feldera_types::format::json::{JsonFlavor, JsonLines, JsonParserConfig, JsonUpdateFormat};
use feldera_types::program_schema::{Field, Relation};
pub use mock_dezset::{wait_for_output_ordered, wait_for_output_unordered, MockDeZSet, MockUpdate};
pub use mock_input_consumer::{MockInputConsumer, MockInputParser};
pub use mock_output_consumer::MockOutputConsumer;

pub static DEFAULT_TIMEOUT_MS: u128 = 600_000;

/// Wait for `predicate` to become `true`.
///
/// Returns the number of milliseconds elapsed or `Err(())` on timeout.
#[allow(clippy::result_unit_err)]
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
    schema: &Relation,
    config: &FormatConfig,
) -> AnyResult<(MockInputConsumer, MockInputParser, MockDeZSet<T, U>)>
where
    T: for<'de> DeserializeWithContext<'de, SqlSerdeConfig>
        + Hash
        + Send
        + Sync
        + Debug
        + Clone
        + 'static,
    U: for<'de> DeserializeWithContext<'de, SqlSerdeConfig>
        + Hash
        + Send
        + Sync
        + Debug
        + Clone
        + 'static,
{
    let input_handle = <MockDeZSet<T, U>>::new();
    let consumer = MockInputConsumer::new();
    let parser = MockInputParser::from_handle(
        &InputCollectionHandle::new(schema.clone(), input_handle.clone()),
        config,
    );
    Ok((consumer, parser, input_handle))
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
    fault_tolerant: bool,
) -> AnyResult<(
    Box<dyn InputReader>,
    MockInputConsumer,
    MockInputParser,
    MockDeZSet<T, U>,
)>
where
    T: for<'de> DeserializeWithContext<'de, SqlSerdeConfig>
        + Hash
        + Send
        + Sync
        + Debug
        + Clone
        + 'static,
    U: for<'de> DeserializeWithContext<'de, SqlSerdeConfig>
        + Hash
        + Send
        + Sync
        + Debug
        + Clone
        + 'static,
{
    let default_format = FormatConfig {
        name: Cow::from("json"),
        config: serde_yaml::to_value(JsonParserConfig {
            update_format: JsonUpdateFormat::Raw,
            json_flavor: JsonFlavor::Datagen,
            array: true,
            lines: JsonLines::Multiple,
        })
        .unwrap(),
    };

    let (consumer, parser, input_handle) = mock_parser_pipeline(
        &relation,
        config
            .connector_config
            .format
            .as_ref()
            .unwrap_or(&default_format),
    )?;

    let endpoint = input_transport_config_to_endpoint(
        config.connector_config.transport.clone(),
        "",
        fault_tolerant,
    )?
    .unwrap();

    let reader = endpoint.open(
        Box::new(consumer.clone()),
        Box::new(parser.clone()),
        relation,
    )?;

    Ok((reader, consumer, parser, input_handle))
}

/// Create a simple test circuit that passes the input stream right through to
/// the output.
// TODO: parameterize with the number (and types?) of input and output streams.
pub fn test_circuit<T>(
    config: CircuitConfig,
    schema: &[Field],
) -> (DBSPHandle, Box<dyn CircuitCatalog>)
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
            "test_input1".into(),
            schema.clone(),
            false,
            BTreeMap::new(),
        ))
        .unwrap();

        let output_schema = serde_json::to_string(&Relation::new(
            "test_output1".into(),
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
    (circuit, Box::new(catalog))
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
    let format = get_input_format(format).unwrap();
    let buffer = MockDeZSet::<T, T>::new();

    // Input parsers don't care about schema yet.
    let schema = Relation::new("mock_schema".into(), vec![], false, BTreeMap::new());

    let mut parser = format
        .new_parser(
            "BaseConsumer",
            &InputCollectionHandle::new(schema, buffer.clone()),
            &serde_yaml::from_str::<serde_yaml::Value>(format_config_yaml).unwrap(),
        )
        .unwrap();

    let mut bytes = Vec::new();
    file.read_to_end(&mut bytes).unwrap();
    let (mut parsed_buffers, errors) = parser.parse(&bytes);
    parsed_buffers.flush();

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
    let _ = tracing_subscriber::registry()
        .with(tracing_subscriber::fmt::layer().with_test_writer())
        .with(
            EnvFilter::try_from_default_env()
                .or_else(|_| EnvFilter::try_new("info"))
                .unwrap(),
        )
        .try_init();
}
