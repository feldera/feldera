//! The Iceberg input connector reading a table of a REST catalog on S3.
//!
//! The case needs a REST catalog at `http://localhost:8181` that serves
//! `iceberg_test.test_table_v3` from `s3://feldera-ci-iceberg/`, and runs with the
//! `iceberg-tests-rest` feature. See `crates/iceberg/src/test/README.md`.

use serde_json::json;

use super::tests::{env_or, iceberg_snapshot_to_json, lateness_relation, output_zset};
use crate::test::pipeline::PipelineTestEngine;

pub fn rest_s3_input_test(engine: &dyn PipelineTestEngine) {
    // Read delta table unordered.
    let (json_file, _metrics) = iceberg_snapshot_to_json(
        engine,
        &lateness_relation(),
        &[],
        json!({
            "catalog_type": "rest",
            "rest.uri": "http://localhost:8181",
            "rest.warehouse": "s3://feldera-ci-iceberg/",
            "table_name": "iceberg_test.test_table_v3",
            "s3.region": env_or("ICEBERG_TEST_REGION", "us-west-1"),
        }),
    );

    let zset = output_zset(&lateness_relation(), json_file.path());

    assert_eq!(zset.len(), 2000000);
}
