//! Test support: the connector test suites that every engine runs and the helpers they share.
//!
//! A suite is a function of an engine's fixture, a [`ConnectorTestEngine`], so one copy of it
//! checks every engine. Each case carries [`engine_test`], which registers it, and an engine's
//! crate runs every registered case with [`run_suites`].
//!
//! [`ConnectorTestEngine`]: feldera_adapterlib::test::ConnectorTestEngine

use std::borrow::Cow;
use std::future::Future;
use std::sync::Arc;

use anyhow::Result as AnyResult;
use arrow::datatypes::{Schema, SchemaRef};
pub use feldera_adapterlib::test::{
    Capability, ErrorCallback, MockInputConsumer, MockInputParser, MockOutputConsumer, async_wait,
    default_timeout_ms, wait,
};
use feldera_adapterlib::test::{ConnectorTestEngine, InputOptions, TestCollection};
use feldera_types::config::{FormatConfig, InputEndpointConfig};
use feldera_types::format::json::{JsonFlavor, JsonLines, JsonParserConfig, JsonUpdateFormat};
use feldera_types::program_schema::Relation;
use feldera_types::secret_resolver::default_secrets_directory;

use crate::format::parquet::{ArrowSchemaOptions, relation_to_arrow_fields};
use crate::format::{get_input_format, get_output_format};
use crate::transport::{InputReader, input_transport_config_to_endpoint};
use crate::{ConnectorConfig, Encoder, OutputConsumer};

#[cfg(feature = "with-avro")]
pub mod avro;
#[cfg(feature = "with-deltalake")]
pub mod delta;
mod harness;
#[cfg(feature = "with-kafka")]
pub mod kafka;
#[cfg(feature = "with-nats")]
pub mod nats;
pub mod pipeline;
pub mod read;
pub mod relations;
pub mod render;
pub mod rows;
mod service;
pub mod suites;

pub use feldera_macros::engine_test;
pub use harness::{CaseFn, CaseLock, EngineCase, run_suites};
pub use service::Service;

/// Sends the logs of the tests to the test output, at the level that `RUST_LOG` names, or
/// `info`.
pub fn init_test_logger() {
    use tracing_subscriber::EnvFilter;
    use tracing_subscriber::layer::SubscriberExt;
    use tracing_subscriber::util::SubscriberInitExt;

    let _ = tracing_subscriber::registry()
        .with(tracing_subscriber::fmt::layer().with_test_writer())
        .with(
            EnvFilter::try_from_default_env()
                .or_else(|_| EnvFilter::try_new("info"))
                .unwrap(),
        )
        .try_init();
}

/// Runs `future` to completion on a runtime of its own on the calling thread, as
/// `#[tokio::test]` runs a test.
pub fn run_async<F: Future>(future: F) -> F::Output {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(future)
}

/// Runs `future` to completion on a runtime of its own with `worker_threads` worker threads,
/// as `#[tokio::test(flavor = "multi_thread")]` runs a test.
pub fn run_async_on<F: Future>(worker_threads: usize, future: F) -> F::Output {
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(worker_threads)
        .enable_all()
        .build()
        .unwrap()
        .block_on(future)
}

/// Runs `f` on a thread of its own, outside any runtime, and returns its result.
///
/// A suite that runs on a runtime builds its controllers this way: building a controller
/// blocks on a runtime, which a thread that already runs one cannot do.
pub fn off_runtime<T: Send>(f: impl FnOnce() -> T + Send) -> T {
    std::thread::scope(|scope| scope.spawn(f).join().unwrap())
}

/// Returns the published Arrow schema of `relation`: the form that the controller gives
/// ad-hoc snapshots, and the one that the suites write rows in.
pub fn published_schema(relation: &Relation) -> SchemaRef {
    Arc::new(Schema::new(relation_to_arrow_fields(
        &relation.fields,
        ArrowSchemaOptions::new(false),
    )))
}

/// Returns a parser of the format in `config` that pushes into `collection`.
///
/// Panics if the format is unknown or refuses `config`.
pub fn parser(collection: &dyn TestCollection, config: &FormatConfig) -> MockInputParser {
    let format = get_input_format(&config.name)
        .unwrap_or_else(|| panic!("unknown input format '{}'", config.name));
    let parser = format
        .new_parser("test_input", &collection.input_handle(), &config.config)
        .unwrap_or_else(|error| {
            panic!(
                "the {} format refuses its configuration: {error}",
                config.name
            )
        });
    MockInputParser::new(parser)
}

/// Returns an encoder of the format in `config` that writes `relation`, or the index `key` on
/// it, to `consumer`, as the controller builds it for an output connector.
///
/// Panics if the format is unknown or refuses `config`.
pub fn encoder(
    config: &FormatConfig,
    key: Option<&Relation>,
    relation: &Relation,
    consumer: Box<dyn OutputConsumer>,
) -> Box<dyn Encoder> {
    let format = get_output_format(&config.name)
        .unwrap_or_else(|| panic!("unknown output format '{}'", config.name));
    let connector: ConnectorConfig = serde_json::from_value(serde_json::json!({
        "transport": { "name": "file_output", "config": { "path": "/dev/null" } },
        "format": config,
    }))
    .unwrap();
    format
        .new_encoder(
            "test_output",
            &connector,
            &key.cloned(),
            relation,
            consumer,
            key.is_some(),
        )
        .unwrap_or_else(|error| {
            panic!(
                "the {} format refuses its configuration: {error}",
                config.name
            )
        })
}

/// An input endpoint that feeds a collection, with the mock consumer and parser between them:
///
/// ```text
/// ┌────────┐   ┌─────────────────┐   ┌──────┐   ┌──────────┐
/// │endpoint├──►│MockInputConsumer├──►│parser├──►│collection│
/// └────────┘   └─────────────────┘   └──────┘   └──────────┘
/// ```
pub struct InputPipeline {
    pub reader: Box<dyn InputReader>,
    pub consumer: MockInputConsumer,
    pub parser: MockInputParser,
    pub collection: Box<dyn TestCollection>,
}

/// Opens the input endpoint of `config` into a fresh collection of `relation`, with the
/// format of `config`, or JSON arrays of raw records if `config` has none.
///
/// Fails if the engine does not support `relation` with `options`, or if the transport does
/// not open.
pub fn input_pipeline(
    engine: &dyn ConnectorTestEngine,
    relation: &Relation,
    options: &InputOptions,
    config: InputEndpointConfig,
) -> AnyResult<InputPipeline> {
    input_pipeline_into(
        engine.input_collection(relation, options)?,
        relation,
        config,
    )
}

/// Like [`input_pipeline`], but opens the endpoint into `collection`, which the caller made,
/// so that a test of one engine can read the collection's rows in that engine's own types.
///
/// Fails if the transport does not open.
pub fn input_pipeline_into(
    collection: Box<dyn TestCollection>,
    relation: &Relation,
    config: InputEndpointConfig,
) -> AnyResult<InputPipeline> {
    let default_format = FormatConfig {
        name: Cow::from("json"),
        config: serde_json::to_value(JsonParserConfig {
            update_format: JsonUpdateFormat::Raw,
            json_flavor: JsonFlavor::Datagen,
            array: true,
            lines: JsonLines::Multiple,
        })
        .unwrap(),
    };
    let consumer = MockInputConsumer::new();
    let parser = parser(
        collection.as_ref(),
        config
            .connector_config
            .format
            .as_ref()
            .unwrap_or(&default_format),
    );
    let endpoint = input_transport_config_to_endpoint(
        &config.connector_config.transport,
        "",
        default_secrets_directory(),
        None,
    )?
    .unwrap();
    let reader = endpoint.open(
        Box::new(consumer.clone()),
        Box::new(parser.clone()),
        relation.clone(),
        None,
    )?;
    Ok(InputPipeline {
        reader,
        consumer,
        parser,
        collection,
    })
}

#[cfg(test)]
mod datagen;
