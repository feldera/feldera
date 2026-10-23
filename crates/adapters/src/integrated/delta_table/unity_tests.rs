//! The Delta Lake input connector on Unity Catalog tables: the cases of `tests` that need a
//! Databricks workspace, behind the `delta-unity-test` feature.

use crate::test::engine_test;
use std::collections::HashMap;
use std::mem::forget;
use std::sync::{Arc, Mutex};

use feldera_storage::tokio::TOKIO;
use feldera_types::program_schema::SqlIdentifier;
use serde_json::{Value, json};
use tempfile::TempDir;

use super::tests::{
    basic_relation, delta_connector_counter, delta_input_controller_keyed,
    delta_table_snapshot_to_json, file_zset, init_logging, people_relation,
    wait_or_connector_error,
};
use crate::test::pipeline::PipelineTestEngine;

/// Read a Unity Catalog table's change feed over a keyed relation whose rows all
/// move between partitions, and check that each row arrives once, at its new key.
///
/// Each row starts in partition `b<nn>` and the merge moves it to `a<nn>`. The two
/// images of a row therefore land in different change data files.
///
/// NOTE: this test requires a `uc://` table, which exercises the code path that
/// reads files through the object store rather than via a `ListingTable`.
///
/// `python/tests/platform/fixtures/unity_change_feed.py` builds the table; the
/// test is inert until `DELTA_TABLE_TEST_UNITY_CDF_TABLE` names it.
#[engine_test(serial(delta_s3), requires(Storage))]
pub fn unity_change_feed(engine: &dyn PipelineTestEngine) {
    let Ok(table_uri) = std::env::var("DELTA_TABLE_TEST_UNITY_CDF_TABLE") else {
        println!("DELTA_TABLE_TEST_UNITY_CDF_TABLE is unset; skipping");
        return;
    };

    init_logging();

    // Matches `unity_change_feed.py`: v0 creates, v1 inserts ROWS rows in
    // partitions `b<nn>`, v2 merges every one into `a<nn>`.
    const ROWS: u32 = 40;
    const CREATE_VERSION: i64 = 0;
    const MERGE_VERSION: i64 = 2;

    let mut config: HashMap<String, Value> = [
        // feldera-ci has no OAuth secret; CI mints this token per run via
        // Databricks workload identity federation (see test-adapters.yml),
        // so there is nothing here to rotate or leak.
        (
            "databricks_token",
            std::env::var("DELTA_TABLE_TEST_UNITY_ACCESS_TOKEN").unwrap(),
        ),
        (
            "databricks_host",
            std::env::var("DELTA_TABLE_TEST_UNITY_HOST").unwrap(),
        ),
        // Must be set explicitly (delta-rs #1095), and long enough for a cold
        // read of files the catalog vends credentials for.
        ("aws_region", "us-west-1".to_string()),
        ("timeout", "1000 secs".to_string()),
    ]
    .into_iter()
    .map(|(k, v)| (k.to_string(), Value::from(v)))
    .collect();
    config.insert("mode".into(), "snapshot_and_follow".into());
    // `require` fails the connector if the table records no change feed, so a
    // misconfigured fixture cannot pass through the file-action path.
    config.insert("change_feed".into(), "require".into());
    config.insert("version".into(), CREATE_VERSION.into());
    config.insert("end_version".into(), MERGE_VERSION.into());

    let storage_dir = TempDir::new().unwrap();
    let errors: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));
    let errors_clone = errors.clone();
    let pipeline = TOKIO.block_on(async move {
        delta_input_controller_keyed(
            engine,
            &table_uri,
            Value::Object(config.into_iter().collect()),
            &basic_relation(),
            storage_dir.path(),
            &errors_clone,
        )
        .await
    });
    pipeline.start();

    let expected: Vec<Value> = (0..ROWS)
        .map(|id| json!({ "id": id, "b": false, "i": null, "s": format!("a{id:02}") }))
        .collect();
    TOKIO.block_on(async {
        wait_or_connector_error(
            &pipeline,
            &SqlIdentifier::from("test_output1"),
            &basic_relation(),
            &expected,
            &errors,
        )
        .await
    });

    let from_change_data =
        delta_connector_counter(&pipeline, "input_connector_delta_commits_from_change_data");
    pipeline.stop().unwrap();

    // The merge must have been read from the change data it recorded. Reading
    // it from the files the commit rewrote would give the same rows, so only
    // this counter tells the two apart.
    assert_eq!(
        from_change_data, 1,
        "expected the merge at v{MERGE_VERSION} to be read from its change data"
    );
}

// I haven't investigated this in depth but it appears that, when running
// multiple delta connectors in parallel, some of which use unity catalog,
// and others use direct S3 URL to connect to the table, this confuses the
// AWS SDK into using the wrong authentication token in S3-based
// connectors. This causes authentication failures with errors like this:
//
// ```text
// 2025-10-29T17:59:09.906160Z  WARN dbsp_adapters::integrated::delta_table::output: delta_table test_output1: error creating or opening delta table 's3://feldera-delta-table-test/80007986-0e32-466a-ad60-4700013bc56e/' after 1 attempts (retrying in 1000 ms): ObjectStore { source: Generic { store: "S3", source: ListRequest { source: RetryError(RetryErrorImpl { method: GET, uri: Some(https://s3.us-east-2.amazonaws.com/feldera-delta-table-test?list-type=2&prefix=80007986-0e32-466a-ad60-4700013bc56e%2F_delta_log%2F), retries: 0, max_retries: 10, elapsed: 395.553916ms, retry_timeout: 180s, inner: Status { status: 400, body: Some("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<Error><Code>InvalidToken</Code><Message>The provided token is malformed or otherwise invalid.</Message><Token-0>...skipped....</Token-0><RequestId>6A1SW4R71MWQFS8H</RequestId><HostId>XOF0VWtYySOJlF0t2R9roAqWM0qH4x+j6cbdkSIVep1l+0PxsAI3tq55nVIcsCj4brHzV9kzmGY=</HostId></Error>") } }) } } }
// ```
//
// Running the unity test sequentially with S3 tests seems to solve this
// reliably.
/// Read the 2M-record dataset over a Unity Catalog path.
///
/// Behind `delta-unity-test`, not `delta-s3-test`: this one needs a Databricks
/// workspace, so it stays out of CI while the rest of the S3 tests run there.
#[engine_test(serial(delta_s3))]
pub fn unity_people_2m(engine: &dyn PipelineTestEngine) {
    let object_store_config = [
        // feldera-ci has no OAuth secret; CI mints this token per run via
        // Databricks workload identity federation (see test-adapters.yml),
        // so there is nothing here to rotate or leak.
        (
            "databricks_token".to_string(),
            std::env::var("DELTA_TABLE_TEST_UNITY_ACCESS_TOKEN").unwrap(),
        ),
        (
            "databricks_host".to_string(),
            std::env::var("DELTA_TABLE_TEST_UNITY_HOST").unwrap(),
        ),
        // AWS region must be specified (see https://github.com/delta-io/delta-rs/issues/1095).
        ("aws_region".to_string(), "us-west-1".to_string()),
        // Set long timeout for reading large files from S3.
        ("timeout".to_string(), "1000 secs".to_string()),
    ]
    .into_iter()
    .collect::<HashMap<_, _>>();

    let table_uri = "uc://feldera_experimental.default.people_2m";
    let json_file =
        delta_table_snapshot_to_json(engine, table_uri, &people_relation(), &object_store_config);

    println!("reading output file");
    let zset = file_zset(&people_relation(), &json_file);

    assert_eq!(zset.len(), 2_000_000);

    forget(json_file);
}
