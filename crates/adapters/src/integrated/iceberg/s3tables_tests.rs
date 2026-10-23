//! The Iceberg input connector reading a table of an Amazon S3 Tables catalog.
//!
//! The case needs AWS credentials for an identity authorized for `s3tables:GetTable` and
//! `s3tables:GetTableData` on the test table, and runs with the `iceberg-tests-s3tables`
//! feature. See `crates/iceberg/src/test/README.md`.

use crate::test::engine_test;
use std::collections::BTreeMap;

use feldera_types::program_schema::{ColumnType, Field, Relation};
use serde_json::json;

use super::tests::{iceberg_snapshot_to_json, output_zset};
use crate::test::pipeline::PipelineTestEngine;

/// The columns of the test table `dev.test_table`.
pub fn relation() -> Relation {
    Relation::new(
        "iceberg_s3tables".into(),
        vec![
            Field::new("id".into(), ColumnType::bigint(false)),
            Field::new("name".into(), ColumnType::varchar(true)),
            Field::new("created_at".into(), ColumnType::timestamp(true)),
        ],
        false,
        BTreeMap::new(),
    )
}

#[engine_test(cfg(feature = "iceberg-tests-s3tables"))]
pub fn s3tables_input_test(engine: &dyn PipelineTestEngine) {
    // Reads `dev.test_table` (schema `id BIGINT NOT NULL, name STRING,
    // created_at TIMESTAMP`, 100 rows) from an Amazon S3 Tables bucket.
    //
    // Credentials and region resolve from the ambient AWS provider chain
    // (environment variables, shared config file, or SSO profile), so no keys
    // are embedded in the connector config. The resolved identity must be
    // authorized for `s3tables:GetTable` (to locate the table metadata) and
    // `s3tables:GetTableData` (the FileIO reads the metadata and data files).
    // Run with AWS credentials configured, e.g. `AWS_PROFILE=<profile>` or
    // `AWS_ACCESS_KEY_ID`/`AWS_SECRET_ACCESS_KEY`(/`AWS_SESSION_TOKEN`) exported.
    let (json_file, _metrics) = iceberg_snapshot_to_json(
        engine,
        &relation(),
        &[],
        json!({
            "catalog_type": "s3tables",
            "s3tables.table-bucket-arn": "arn:aws:s3tables:us-west-1:737834633458:bucket/iceberg-test",
            "table_name": "dev.test_table",
            "s3tables.region": "us-west-1",
            "s3.region": "us-west-1",
        }),
    );

    let zset = output_zset(&relation(), json_file.path());

    assert_eq!(zset.len(), 100);
}
