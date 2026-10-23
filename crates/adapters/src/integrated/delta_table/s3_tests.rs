//! The Delta Lake connectors on tables in the CI object store: the cases of `tests` that need
//! its credentials, behind the `delta-s3-test` feature.

use std::collections::HashMap;
use std::mem::forget;

use feldera_types::transport::delta_table::DeltaTableTransactionMode;
use proptest::prelude::ProptestConfig;
use proptest::proptest;
use proptest::strategy::{Strategy, ValueTree};
use proptest::test_runner::TestRunner;
use serde_json::Value;

use super::tests::{
    delta_data, delta_table_output_test, delta_table_snapshot_to_json, file_zset, people_relation,
    relation, rows_zset, test_cdc, test_follow,
};
use crate::test::pipeline::PipelineTestEngine;
use crate::test::run_async_on;

/// Value of `name`, or a panic naming the variable that has to be set.
///
/// `env::var(..).unwrap()` reports only `NotPresent`, which does not say what
/// to set. Mirrors `required_env` in `python/tests/utils.py`.
fn required_env(name: &str) -> String {
    std::env::var(name)
        .unwrap_or_else(|_| panic!("{name} must be set to run the delta-s3-test tests"))
}

/// Object-store options for the CI buckets.
///
/// Empty: the buckets are GCS, and both the test process and the pipeline
/// authenticate with Workload Identity in CI and application-default
/// credentials locally, so no key ever reaches a connector config. Same
/// reasoning as `delta_storage_options` in `python/tests/utils.py`.
fn ci_object_store_config() -> Vec<(String, String)> {
    Vec::new()
}

/// Store for per-run test data, as `gs://<bucket>[/<prefix>]`. Everything in it
/// is deleted after 7 days.
fn ci_store_uri() -> String {
    required_env("CI_OBJECT_STORE_URI")
        .trim_end_matches('/')
        .to_string()
}

/// Store for read-only input data, which has no expiry rule. Fixtures that
/// cannot be regenerated from this repo have to live here: the per-run store
/// deletes them a week after upload.
fn ci_inputs_store_uri() -> String {
    required_env("CI_OBJECT_STORE_INPUTS_URI")
        .trim_end_matches('/')
        .to_string()
}

/// Location for a table the test writes. No test removes the table it creates;
/// the bucket's lifecycle rule deletes objects after 7 days instead.
fn ci_scratch_table_uri(uuid: uuid::Uuid) -> String {
    format!("{}/rust-delta-tests/{uuid}/", ci_store_uri())
}

pub fn cdc_s3_test_suspend(engine: &dyn PipelineTestEngine) {
    run_async_on(2, async {
        crate::integrated::delta_table::register_storage_handlers();

        // We cannot use proptest macros in `async` context, so generate
        // some random data manually.
        let mut runner = TestRunner::default();
        let data = delta_data(20_000).new_tree(&mut runner).unwrap().current();

        let relation_schema = relation();

        let input_uuid = uuid::Uuid::new_v4();

        let object_store_config = ci_object_store_config()
            .into_iter()
            .map(|(key, value)| (key, value.into()))
            .collect::<HashMap<String, Value>>();

        let input_table_uri = ci_scratch_table_uri(input_uuid);

        test_cdc(
            engine,
            &relation_schema,
            &input_table_uri,
            &object_store_config,
            data,
            false,
            true,
            DeltaTableTransactionMode::None,
            false,
        )
        .await;
    })
}

async fn follow_s3_test_common(engine: &dyn PipelineTestEngine, snapshot: bool, suspend: bool) {
    crate::integrated::delta_table::register_storage_handlers();

    // We cannot use proptest macros in `async` context, so generate
    // some random data manually.
    let mut runner = TestRunner::default();
    let data = delta_data(20_000).new_tree(&mut runner).unwrap().current();

    let relation_schema = relation();

    let input_uuid = uuid::Uuid::new_v4();
    let output_uuid = uuid::Uuid::new_v4();

    let object_store_config = ci_object_store_config()
        .into_iter()
        .collect::<HashMap<_, _>>();

    let input_table_uri = ci_scratch_table_uri(input_uuid);
    let output_table_uri = ci_scratch_table_uri(output_uuid);

    test_follow(
        engine,
        &relation_schema,
        &input_table_uri,
        &output_table_uri,
        &object_store_config,
        data,
        snapshot,
        DeltaTableTransactionMode::None,
        suspend,
        false,
        1000,
        100,
        None,
        None,
    )
    .await;
}

pub fn follow_s3_test(engine: &dyn PipelineTestEngine) {
    run_async_on(2, async {
        follow_s3_test_common(engine, false, false).await
    })
}

pub fn snapshot_and_follow_s3_test(engine: &dyn PipelineTestEngine) {
    run_async_on(2, async {
        follow_s3_test_common(engine, true, false).await
    })
}

pub fn snapshot_and_follow_s3_test_suspend(engine: &dyn PipelineTestEngine) {
    run_async_on(2, async { follow_s3_test_common(engine, true, true).await })
}

/// Write to a Delta table in S3.
pub fn s3_output_proptest(engine: &dyn PipelineTestEngine) {
    proptest!(ProptestConfig::with_cases(1), |(data in delta_data(20_000))| {
    let uuid = uuid::Uuid::new_v4();
    let object_store_config = ci_object_store_config().into_iter().collect::<HashMap<_,_>>();

    let table_uri = ci_scratch_table_uri(uuid);
    // TODO: enable verification when it's supported for S3.
    delta_table_output_test(engine, data.clone(), &table_uri, &object_store_config, false, None, false);
    //delta_table_output_test(engine, data.clone(), &table_uri, &object_store_config, false, None);

    let json_file = delta_table_snapshot_to_json(engine,
        &table_uri,
        &relation(),
        &object_store_config);

    let expected_zset = rows_zset(&relation(), &data);
    let zset = file_zset(&relation(), &json_file);
    assert_eq!(zset, expected_zset);
    });
}

/// Read a large (2M records) dataset from the CI object store.
///
/// Copied there from the Databricks workspace bucket it was created in, so CI
/// can read it without credentials of its own. It sits in the inputs store
/// rather than the per-run one because nothing in this repo can rebuild it --
/// it came from a Databricks notebook (below) -- and the per-run store deletes
/// everything after 7 days.
///
/// This dataset was derived from
/// `/databricks-datasets/learning-spark-v2/people/people-10m.delta`; however the full dataset
/// is 200MB and can be slow to download, so we cut it down to 2M.
///
/// Here is the full notebook used to create it:
/// ```text
/// # Load the data from its source.
/// df = spark.read.load("/databricks-datasets/learning-spark-v2/people/people-10m.delta")
///
/// # Write the data to a table.
/// table_name = "people_2m"
/// df.limit(2000000).write.saveAsTable(table_name)
///
/// # Find location of the delta table in S3.
/// table_location = spark.sql("DESCRIBE DETAIL people_2m").collect()[0]['location']
/// print(table_location)
/// ```
pub fn s3_people_2m(engine: &dyn PipelineTestEngine) {
    let object_store_config = ci_object_store_config()
        .into_iter()
        // Set long timeout for reading large files.
        .chain([("timeout".to_string(), "1000 secs".to_string())])
        .collect::<HashMap<_, _>>();

    let table_uri = format!("{}/delta-connector-tests/people_2m/", ci_inputs_store_uri());
    let table_uri = table_uri.as_str();
    let json_file =
        delta_table_snapshot_to_json(engine, table_uri, &people_relation(), &object_store_config);

    println!("reading output file");
    let zset = file_zset(&people_relation(), &json_file);

    assert_eq!(zset.len(), 2_000_000);

    forget(json_file);
}
