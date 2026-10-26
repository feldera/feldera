//! The connector test suites by short names, for the engines' fixtures, which serve the
//! relations of the suites. Each suite lives next to the code that it tests, and
//! [`engine_test`](crate::test::engine_test) registers its cases.

pub use crate::controller::test as controller;
pub use crate::format::arrow_ipc::tests as arrow_ipc;
#[cfg(feature = "with-avro")]
pub use crate::format::avro::test as avro_encoder;
#[cfg(feature = "with-avro")]
pub use crate::format::avro::test as avro_parser;
pub use crate::format::csv::test as csv_parser;
pub use crate::format::json::input::test as json_parser;
pub use crate::format::json::output::test as json_encoder;
pub use crate::format::parquet::test as parquet;
pub use crate::format::raw::test as raw_parser;
#[cfg(feature = "with-deltalake")]
pub use crate::integrated::delta_table::input::distributed_snapshot_tests as delta_input;
#[cfg(feature = "with-deltalake")]
pub use crate::integrated::delta_table::merge::model as delta_merge;
#[cfg(feature = "with-deltalake")]
pub use crate::integrated::delta_table::output::parallel as delta_output;
#[cfg(feature = "with-deltalake")]
pub use crate::integrated::delta_table::test as delta_table;
#[cfg(feature = "with-dynamodb")]
pub use crate::integrated::dynamodb::test as dynamodb;
#[cfg(feature = "with-postgres-cdc")]
pub use crate::integrated::postgres::cdc_input::tests as postgres_cdc_destination;
pub use crate::integrated::postgres::output::tests as postgres_output;
pub use crate::integrated::postgres::test as postgres;
#[cfg(feature = "with-postgres-cdc")]
pub use crate::integrated::postgres::test::cdc_tests as postgres_cdc;
pub use crate::test::datagen;
#[cfg(feature = "with-iceberg")]
pub use crate::test::iceberg as iceberg_s3tables;
#[cfg(feature = "with-iceberg")]
pub use crate::test::iceberg;
pub use crate::test::soft_delete;
pub use crate::transport::file::test as file_input;
#[cfg(feature = "with-kafka")]
pub use crate::transport::kafka::ft::test as kafka;
#[cfg(feature = "with-nats")]
pub use crate::transport::nats::input::test::control_tests as nats_pipeline;
#[cfg(feature = "with-nats")]
pub use crate::transport::nats::input::test::mock_tests as nats_input;
#[cfg(feature = "with-pubsub")]
pub use crate::transport::pubsub::test as pubsub;
#[cfg(feature = "with-redis")]
pub use crate::transport::redis::test as redis;
pub use crate::transport::s3::test as s3_input;
pub use crate::transport::url::test as url_input;
