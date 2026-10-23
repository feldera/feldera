//! The connector test suites by short names, for the engines' fixtures, which serve the
//! relations of the suites. Each suite lives next to the code that it tests, and
//! [`engine_test`](crate::test::engine_test) registers its cases.

pub use crate::controller::tests as controller;
pub use crate::format::arrow_ipc::tests as arrow_ipc;
#[cfg(feature = "with-avro")]
pub use crate::format::avro::encoder_tests as avro_encoder;
#[cfg(feature = "with-avro")]
pub use crate::format::avro::parser_tests as avro_parser;
pub use crate::format::csv::tests as csv_parser;
pub use crate::format::json::encoder_tests as json_encoder;
pub use crate::format::json::parser_tests as json_parser;
pub use crate::format::parquet::tests as parquet;
pub use crate::format::raw::tests as raw_parser;
pub use crate::format::soft_delete_tests as soft_delete;
#[cfg(feature = "with-deltalake")]
pub use crate::integrated::delta_table::input::tests as delta_input;
#[cfg(feature = "with-deltalake")]
pub use crate::integrated::delta_table::merge::tests as delta_merge;
#[cfg(feature = "with-deltalake")]
pub use crate::integrated::delta_table::output::tests as delta_output;
#[cfg(feature = "with-deltalake")]
pub use crate::integrated::delta_table::tests as delta_table;
#[cfg(feature = "with-dynamodb")]
pub use crate::integrated::dynamodb::tests as dynamodb;
#[cfg(feature = "with-iceberg")]
pub use crate::integrated::iceberg::s3tables_tests as iceberg_s3tables;
#[cfg(feature = "with-iceberg")]
pub use crate::integrated::iceberg::tests as iceberg;
#[cfg(feature = "with-postgres-cdc")]
pub use crate::integrated::postgres::cdc_input::destination_tests as postgres_cdc_destination;
#[cfg(feature = "with-postgres-cdc")]
pub use crate::integrated::postgres::cdc_tests as postgres_cdc;
pub use crate::integrated::postgres::output::endpoint_tests as postgres_output;
pub use crate::integrated::postgres::tests as postgres;
pub use crate::transport::datagen_tests as datagen;
pub use crate::transport::file::tests as file_input;
#[cfg(feature = "with-kafka")]
pub use crate::transport::kafka::tests as kafka;
#[cfg(feature = "with-nats")]
pub use crate::transport::nats::input_tests as nats_input;
#[cfg(feature = "with-nats")]
pub use crate::transport::nats::pipeline_tests as nats_pipeline;
#[cfg(feature = "with-pubsub")]
pub use crate::transport::pubsub::tests as pubsub;
#[cfg(feature = "with-redis")]
pub use crate::transport::redis::tests as redis;
pub use crate::transport::s3::tests as s3_input;
pub use crate::transport::url::tests as url_input;
