# Iceberg connector test harness

Iceberg connector tests live in `adapters/srs/tests/iceberg.rs`.  Since Iceberg's
Rust ecosystem is not yet self-contained, tests rely on several external components
and are feature gated using:

* `iceberg-tests-fs` - enables tests using local file system
* `iceberg-tests-glue` - enables tests using S3 and AWS Glue catalog
* `iceberg-tests-rest` - enables tests using S3 and Rest catalog.

The `iceberg-rust` crate does not yes support writing Iceberg tables; therefore
we use `pyiceberg` to create test tables for the Iceberg source connector.  The
`create_test_tables_s3.py` script in this directory can be used to create an
Iceberg table.  By default it creates the table in the local FS.  It can also be
used with the `--catalog glue` flag to create a table in S3 using AWS Glue catalog.
We used the latter feature to create a test table in
`s3://feldera-iceberg-test/test_table`.  This table is used to run tests with
Glue and REST catalogs (see below).

## FS-based tests

These tests create an Iceberg table in the local file syste using the
`create_test_tables_s3.py` script and read this table using different configurations.
Before running the tests, make sure that you have Python dependencies listed
in `./requirements.txt` installed:

* `pip install -r crates/iceberg/src/test/requirements.txt`
* Run the following command in the `adapters` crate: `cargo test --features="iceberg-tests-fs" iceberg`

## Glue catalog test

* Set `ICEBERG_TEST_AWS_ACCESS_KEY_ID` and `ICEBERG_TEST_AWS_SECRET_ACCESS_KEY` environment
  variables to AWS credentials of an IAM account that has access to the Glue catalog and
  S3 buckets used by Feldera CI (talk to leonid@feldera.com).
* Run the following command in the `adapters` crate: `cargo test --features="iceberg-tests-glue" iceberg_glue_s3_input_test`

## Rest catalog test

In order to run this test, we need an Iceberg REST catalog implementation. The AWS Glue catalog
provides one, but unfortunately it is not currently usable with the `iceberg-rust` crate, which
does not yet implement the AWS SigV4 protocol (see SigV4-related config options here:
https://py.iceberg.apache.org/configuration/#rest-catalog). Therefore we instead use a standalone
implementation of the REST catalog from Databricks and bind it to the Glue catalog as the backend:

* Create catalog: `docker run -e AWS_REGION=us-east-1 -e AWS_ACCESS_KEY_ID=<aws access key> -e AWS_SECRET_ACCESS_KEY=<aws secret> -e CATALOG_CATALOG__IMPL=org.apache.iceberg.aws.glue.GlueCatalog -p 8181:8181 tabulario/iceberg-rest:0.1.0`

* `cargo test --features="iceberg-tests-rest" iceberg_rest_s3_input_test`

# Running tests in CI

Currently only Glue and FS-based tests run in CI.
