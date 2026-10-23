//! The S3 input connector, over a mock S3 client: one object or many, listing continuations,
//! errors of a listing and of an object, and records of one object split across steps.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use aws_sdk_s3::operation::get_object::{GetObjectError, GetObjectOutput};
use aws_sdk_s3::operation::list_objects_v2::ListObjectsV2Error;
use aws_sdk_s3::primitives::{ByteStream, SdkBody};
use aws_sdk_s3::types::error::builders::{NoSuchBucketBuilder, NoSuchKeyBuilder};
use feldera_adapterlib::format::{MessageOrientedPreprocessedParser, Parser};
use feldera_adapterlib::preprocess::{PreprocessorFactory, PreprocessorWatchdog};
use feldera_adapterlib::test::{
    ConnectorTestEngine, ErrorCallback, InputOptions, MockInputConsumer, MockInputParser,
    TestCollection, Updates, wait_for_rows_within,
};
use feldera_types::config::{InputEndpointConfig, TransportConfig};
use feldera_types::preprocess::PreprocessorConfig;
use feldera_types::program_schema::{ColumnType, Field, Relation};
use mockall::predicate::eq;

use crate::InputReader;
use crate::preprocess::PassthroughPreprocessorFactory;
use crate::test::{parser, published_schema};
use crate::transport::s3::{MockS3Client, S3InputReader};

/// How long a case waits for an error that the connector must report.
const ERROR_TIMEOUT: Duration = Duration::from_secs(60);

/// How long a case waits for the rows of the objects.
const ROWS_TIMEOUT_MS: u64 = 10_000;

/// The table that the cases read into: one BIGINT column, `i`.
pub fn relation() -> Relation {
    Relation::new(
        "s3_input".into(),
        vec![Field::new("i".into(), ColumnType::bigint(false))],
        false,
        BTreeMap::new(),
    )
}

/// An endpoint that lists the objects of the bucket `test-bucket` with an empty prefix.
const MULTI_KEY_CONFIG: &str = r#"
{
  "stream": "test_input",
  "transport": {
    "name": "s3_input",
    "config": {
      "aws_access_key_id": "FAKE_ACCESS_KEY",
      "aws_secret_access_key": "FAKE_SECRET",
      "bucket_name": "test-bucket",
      "region": "us-west-1",
      "prefix": ""
    }
  },
  "format": { "name": "csv" }
}"#;

/// An endpoint that reads the object `obj1`.
const SINGLE_KEY_CONFIG: &str = r#"
{
  "stream": "test_input",
  "transport": {
    "name": "s3_input",
    "config": {
      "aws_access_key_id": "FAKE_ACCESS_KEY",
      "aws_secret_access_key": "FAKE_SECRET",
      "bucket_name": "test-bucket",
      "region": "us-west-1",
      "key": "obj1"
    }
  },
  "format": { "name": "csv" }
}"#;

/// An endpoint that reads newline-delimited JSON from the object `obj1`; `lines: single`
/// selects the line splitter that takes as many records as it can.
const NDJSON_SINGLE_KEY_CONFIG: &str = r#"
{
  "stream": "test_input",
  "transport": {
    "name": "s3_input",
    "config": {
      "aws_access_key_id": "FAKE_ACCESS_KEY",
      "aws_secret_access_key": "FAKE_SECRET",
      "bucket_name": "test-bucket",
      "region": "us-west-1",
      "key": "obj1"
    }
  },
  "format": { "name": "json", "config": { "update_format": "raw", "array": false, "lines": "single" } }
}"#;

/// A reader over a mock S3 client, and the mocks that it feeds.
struct Setup {
    reader: Box<dyn InputReader>,
    consumer: MockInputConsumer,
    parser: MockInputParser,
    collection: Box<dyn TestCollection>,
}

/// Builds a reader of `config` over `mock`, with `on_error` already installed, and with the
/// parser behind a message-oriented preprocessor if `preprocess`.
///
/// The reader lists objects as soon as it starts, so the error callback must be in place
/// first, or the case can miss an error that the listing reports.
fn setup(
    engine: &dyn ConnectorTestEngine,
    config: &str,
    mock: MockS3Client,
    on_error: ErrorCallback,
    preprocess: bool,
) -> Setup {
    let config: InputEndpointConfig = serde_json::from_str(config).unwrap();
    let TransportConfig::S3Input(transport) = config.connector_config.transport.clone() else {
        panic!("the configuration is not of an S3 input");
    };
    let collection = engine
        .input_collection(&relation(), &InputOptions::default())
        .unwrap();
    let parser = parser(
        collection.as_ref(),
        &config.connector_config.format.unwrap(),
    );
    let consumer = MockInputConsumer::new();
    consumer.on_error(Some(on_error));
    let endpoint_parser: Box<dyn Parser> = if preprocess {
        let preprocessor_config = PreprocessorConfig {
            name: "passthrough".to_string(),
            message_oriented: true,
            config: serde_json::Value::Null,
            max_allowed_call_duration: None,
        };
        let preprocessor = PassthroughPreprocessorFactory
            .create(&preprocessor_config)
            .unwrap();
        Box::new(MessageOrientedPreprocessedParser::new(
            preprocessor,
            Box::new(parser.clone()),
            PreprocessorWatchdog::new(&preprocessor_config),
        ))
    } else {
        Box::new(parser.clone())
    };
    let reader = Box::new(S3InputReader::new_inner(
        &Arc::new(transport),
        Box::new(consumer.clone()),
        endpoint_parser,
        Arc::new(mock),
        None,
    ));
    Setup {
        reader,
        consumer,
        parser,
        collection,
    }
}

/// Returns the rows of [`relation`] whose `i` is in `values`, each inserted once.
fn rows(values: impl Iterator<Item = i64>) -> Updates {
    let rows: Vec<String> = values.map(|i| format!(r#"{{"i": {i}}}"#)).collect();
    let rows: Vec<(i64, &str)> = rows.iter().map(|row| (1, row.as_str())).collect();
    Updates::from_json(published_schema(&relation()), &rows).unwrap()
}

/// Returns an object whose body is `body`.
fn object(body: impl Into<SdkBody>) -> GetObjectOutput {
    GetObjectOutput::builder()
        .body(ByteStream::from(body.into()))
        .build()
}

/// Checks that a reader of `config` over `mock` delivers nothing before it starts, and then
/// inserts the rows of `values`, in any order.
fn check(
    engine: &dyn ConnectorTestEngine,
    config: &str,
    mock: MockS3Client,
    values: impl Iterator<Item = i64>,
) {
    let expected = rows(values);
    let setup = setup(engine, config, mock, Box::new(|_, _| ()), false);
    assert!(setup.parser.state().data.is_empty());
    assert!(!setup.consumer.state().eoi);

    setup.reader.extend();
    let updates = wait_for_rows_within(
        setup.collection.as_ref(),
        expected.len(),
        ROWS_TIMEOUT_MS,
        || setup.reader.queue(false),
    );
    assert!(updates.weights().iter().all(|weight| *weight == 1));
    assert_eq!(updates.zset().unwrap(), expected.zset().unwrap());
}

/// An endpoint with a key reads that object.
pub fn single_key_read(engine: &dyn ConnectorTestEngine) {
    let mut mock = MockS3Client::default();
    mock.expect_get_object_keys()
        .with(eq("test-bucket"), eq(""), eq(&None), eq(&None))
        .return_once(|_, _, _, _| Ok((vec!["obj1".to_string()], None)));
    mock.expect_get_object()
        .with(eq("test-bucket"), eq("obj1"), eq(0), eq(&None))
        .return_once(|_, _, _, _| Ok(object("1\n2\n3\n")));
    check(engine, SINGLE_KEY_CONFIG, mock, 1..4);
}

/// A preprocessor makes `parse` return a nested buffer, and the endpoint hands the circuit at
/// most `max_batch_size` records per step, so one parsed buffer must survive a split across
/// steps; records lost at the split would keep the pipeline from completing.
///
/// Newline-delimited JSON splits with a line splitter that takes as many records as it can,
/// so one nested element holds many records and a step boundary falls inside it.
pub fn preprocessed_read_split_across_batches(engine: &dyn ConnectorTestEngine) {
    const RECORDS: i64 = 100;
    // Not a divisor of RECORDS, so the last step is short too.
    const MAX_BATCH_SIZE: usize = 7;
    let body: String = (1..=RECORDS).map(|i| format!("{{\"i\":{i}}}\n")).collect();
    let mut mock = MockS3Client::default();
    mock.expect_get_object_keys()
        .with(eq("test-bucket"), eq(""), eq(&None), eq(&None))
        .return_once(|_, _, _, _| Ok((vec!["obj1".to_string()], None)));
    mock.expect_get_object()
        .with(eq("test-bucket"), eq("obj1"), eq(0), eq(&None))
        .return_once(move |_, _, _, _| Ok(object(body)));

    let setup = setup(
        engine,
        NDJSON_SINGLE_KEY_CONFIG,
        mock,
        Box::new(|_, _| ()),
        true,
    );
    setup.consumer.set_max_batch_size(MAX_BATCH_SIZE);
    setup.reader.extend();
    let updates = wait_for_rows_within(
        setup.collection.as_ref(),
        RECORDS as usize,
        ROWS_TIMEOUT_MS,
        || setup.reader.queue(false),
    );
    assert!(updates.weights().iter().all(|weight| *weight == 1));
    assert_eq!(updates.zset().unwrap(), rows(1..=RECORDS).zset().unwrap());
}

/// An endpoint with a prefix reads the object it lists.
pub fn single_object_with_prefix_read(engine: &dyn ConnectorTestEngine) {
    let mut mock = MockS3Client::default();
    mock.expect_get_object_keys()
        .with(eq("test-bucket"), eq(""), eq(&None), eq(&None))
        .return_once(|_, _, _, _| Ok((vec!["obj1".to_string()], None)));
    mock.expect_get_object()
        .with(eq("test-bucket"), eq("obj1"), eq(0), eq(&None))
        .return_once(|_, _, _, _| Ok(object("1\n2\n3\n")));
    check(engine, MULTI_KEY_CONFIG, mock, 1..4);
}

/// An endpoint follows a listing's continuation token and reads the objects of both pages.
pub fn multi_object_read_with_continuation(engine: &dyn ConnectorTestEngine) {
    let mut mock = MockS3Client::default();
    mock.expect_get_object_keys()
        .with(eq("test-bucket"), eq(""), eq(&None), eq(&None))
        .return_once(|_, _, _, _| Ok((vec!["obj1".to_string()], Some("next_token".to_string()))));
    mock.expect_get_object_keys()
        .with(
            eq("test-bucket"),
            eq(""),
            eq(&None),
            eq(Some("next_token".to_string())),
        )
        .return_once(|_, _, _, _| Ok((vec!["obj2".to_string()], None)));
    mock.expect_get_object()
        .with(eq("test-bucket"), eq("obj1"), eq(0), eq(&None))
        .return_once(|_, _, _, _| Ok(object("1\n2\n3\n")));
    mock.expect_get_object()
        .with(eq("test-bucket"), eq("obj2"), eq(0), eq(&None))
        .return_once(|_, _, _, _| Ok(object("4\n5\n6\n")));
    check(engine, MULTI_KEY_CONFIG, mock, 1..7);
}

/// An endpoint reads a thousand objects of one record each.
pub fn multi_object_read(engine: &dyn ConnectorTestEngine) {
    let mut mock = MockS3Client::default();
    let objects: Vec<String> = (0..1000).map(|i| format!("obj{i}")).collect();
    for (i, key) in objects.iter().enumerate() {
        mock.expect_get_object()
            .with(eq("test-bucket"), eq(key.clone()), eq(0), eq(&None))
            .return_once(move |_, _, _, _| Ok(object(format!("{i}\n"))));
    }
    mock.expect_get_object_keys()
        .with(eq("test-bucket"), eq(""), eq(&None), eq(&None))
        .return_once(|_, _, _, _| Ok((objects, None)));
    let setup = setup(engine, MULTI_KEY_CONFIG, mock, Box::new(|_, _| ()), false);
    setup.reader.extend();
    let updates = wait_for_rows_within(setup.collection.as_ref(), 1000, ROWS_TIMEOUT_MS, || {
        setup.reader.queue(false)
    });
    assert!(updates.weights().iter().all(|weight| *weight == 1));
    assert_eq!(updates.zset().unwrap(), rows(0..1000).zset().unwrap());
}

/// A listing that fails is a fatal error that names the S3 error.
pub fn list_object_read_error(engine: &dyn ConnectorTestEngine) {
    let mut mock = MockS3Client::default();
    mock.expect_get_object_keys()
        .with(eq("test-bucket"), eq(""), eq(&None), eq(&None))
        .return_once(|_, _, _, _| {
            Err(ListObjectsV2Error::NoSuchBucket(NoSuchBucketBuilder::default().build()).into())
        });
    let (sender, receiver) = std::sync::mpsc::channel();
    let setup = setup(
        engine,
        MULTI_KEY_CONFIG,
        mock,
        // The receiver may be gone when the reader reports during teardown.
        Box::new(move |fatal, error| {
            let _ = sender.send((fatal, format!("{error}")));
        }),
        false,
    );
    setup.reader.extend();
    assert_eq!(
        receiver
            .recv_timeout(ERROR_TIMEOUT)
            .expect("the connector reports no error for a missing bucket"),
        (true, "NoSuchBucket".to_string())
    );
}

/// An object that fails to read is skipped, and the next object still arrives.
pub fn get_object_read_error(engine: &dyn ConnectorTestEngine) {
    let mut mock = MockS3Client::default();
    mock.expect_get_object_keys()
        .with(eq("test-bucket"), eq(""), eq(&None), eq(&None))
        .return_once(|_, _, _, _| Ok((vec!["obj1".to_string()], Some("next_token".to_string()))));
    mock.expect_get_object_keys()
        .with(
            eq("test-bucket"),
            eq(""),
            eq(&None),
            eq(Some("next_token".to_string())),
        )
        .return_once(|_, _, _, _| Ok((vec!["obj2".to_string()], None)));
    mock.expect_get_object()
        .with(eq("test-bucket"), eq("obj1"), eq(0), eq(&None))
        .return_once(|_, _, _, _| {
            Err(GetObjectError::NoSuchKey(NoSuchKeyBuilder::default().build()).into())
        });
    mock.expect_get_object()
        .with(eq("test-bucket"), eq("obj2"), eq(0), eq(&None))
        .return_once(|_, _, _, _| Ok(object("4\n5\n6\n")));
    check(engine, MULTI_KEY_CONFIG, mock, 4..7);
}
