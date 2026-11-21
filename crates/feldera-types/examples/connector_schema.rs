//! Export the runtime schema used by the disposable P0 compiler decoder check.

use feldera_types::{
    format::avro::{AvroEncoderConfig, AvroParserConfig, AvroSchemaRegistryConfig},
    transport::{
        http::HttpOutputConfig,
        iceberg::{
            GlueCatalogConfig, IcebergReaderConfig, RestCatalogConfig, S3TablesCatalogConfig,
        },
        kafka::KafkaInputConfig,
        postgres::{PostgresReaderConfig, PostgresTlsConfig},
    },
};
use serde_json::{Map, Value};
use utoipa::ToSchema;

fn add<'a, T: ToSchema<'a>>(schemas: &mut Map<String, Value>) -> Result<(), serde_json::Error> {
    let (name, schema) = T::schema();
    schemas.insert(name.into(), serde_json::to_value(schema)?);
    Ok(())
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut schemas = Map::new();
    add::<PostgresReaderConfig>(&mut schemas)?;
    add::<PostgresTlsConfig>(&mut schemas)?;
    add::<KafkaInputConfig>(&mut schemas)?;
    add::<IcebergReaderConfig>(&mut schemas)?;
    add::<GlueCatalogConfig>(&mut schemas)?;
    add::<RestCatalogConfig>(&mut schemas)?;
    add::<S3TablesCatalogConfig>(&mut schemas)?;
    add::<AvroParserConfig>(&mut schemas)?;
    add::<AvroEncoderConfig>(&mut schemas)?;
    add::<AvroSchemaRegistryConfig>(&mut schemas)?;
    add::<HttpOutputConfig>(&mut schemas)?;
    serde_json::to_writer_pretty(std::io::stdout().lock(), &schemas)?;
    Ok(())
}
