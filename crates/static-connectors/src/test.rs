//! The static engine's test fixture, with which the `connector_suites` test target runs the
//! connector suites of `dbsp_connectors`, and the tests of the static engine itself.
//!
//! The module extends the shared test module of `dbsp_connectors`: it re-exports that module's
//! items, so that the fixtures here name the shared items and their own by the same
//! `crate::test` paths.

pub use dbsp_connectors::test::*;

pub mod data;
mod engine;
mod mock_dezset;

#[cfg(test)]
mod controller_tests;
#[cfg(all(test, feature = "with-deltalake"))]
mod delta_tests;
#[cfg(test)]
mod json_tests;
#[cfg(all(test, feature = "with-kafka"))]
mod kafka_tests;
#[cfg(test)]
mod parquet_tests;

pub use data::{
    DatabricksPeople, DeltaTestStruct, EmbeddedStruct, IcebergSubsetTestStruct, IcebergTestStruct,
    KeyStruct, S3TablesTestStruct, TestStruct, TestStruct2, TestStructSoftDelete,
    generate_test_batch, generate_test_batches, generate_test_batches_with_weights,
};
pub use engine::records::KafkaRawMetadata;
pub use engine::{StaticTestEngine, TypedCollection};
pub use mock_dezset::{MockDeZSet, MockUpdate};
