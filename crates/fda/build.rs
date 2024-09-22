use progenitor::{GenerationSettings, InterfaceStyle};
use std::{env, fs, path::Path};

fn type_replacement() -> Vec<(&'static str, &'static str)> {
    vec![
        ("PipelineConfig", "feldera_types::config::PipelineConfig"),
        ("StorageConfig", "feldera_types::config::StorageConfig"),
        (
            "StorageCacheConfig",
            "feldera_types::config::StorageCacheConfig",
        ),
        ("RuntimeConfig", "feldera_types::config::RuntimeConfig"),
        (
            "InputEndpointConfig",
            "feldera_types::config::InputEndpointConfig",
        ),
        ("ConnectorConfig", "feldera_types::config::ConnectorConfig"),
        (
            "OutputBufferConfig",
            "feldera_types::config::OutputBufferConfig",
        ),
        (
            "OutputEndpointConfig",
            "feldera_types::config::OutputEndpointConfig",
        ),
        ("TransportConfig", "feldera_types::config::TransportConfig"),
        ("FormatConfig", "feldera_types::config::FormatConfig"),
        ("ResourceConfig", "feldera_types::config::ResourceConfig"),
        (
            "FileInputConfig",
            "feldera_types::transport::file::FileInputConfig",
        ),
        (
            "FileOutputConfig",
            "feldera_types::transport::file::FileOutputConfig",
        ),
        (
            "UrlInputConfig",
            "feldera_types::transport::url::UrlInputConfig",
        ),
        (
            "KafkaHeader",
            "feldera_types::transport::kafka::KafkaHeader",
        ),
        (
            "KafkaHeaderValue",
            "feldera_types::transport::kafka::KafkaHeaderValue",
        ),
        (
            "KafkaLogLevel",
            "feldera_types::transport::kafka::KafkaLogLevel",
        ),
        (
            "KafkaInputConfig",
            "feldera_types::transport::kafka::KafkaInputConfig",
        ),
        (
            "KafkaOutputConfig",
            "feldera_types::transport::kafka::KafkaOutputConfig",
        ),
        (
            "KafkaInputFtConfig",
            "feldera_types::transport::kafka::KafkaInputFtConfig",
        ),
        (
            "KafkaOutputFtConfig",
            "feldera_types::transport::kafka::KafkaOutputFtConfig",
        ),
        (
            "ConsumeStrategy",
            "feldera_types::transport::s3::ConsumeStrategy",
        ),
        ("ReadStrategy", "feldera_types::transport::s3::ReadStrategy"),
        (
            "AwsCredentials",
            "feldera_types::transport::s3::AwsCredentials",
        ),
        (
            "S3InputConfig",
            "feldera_types::transport::s3::S3InputConfig",
        ),
        (
            "DatagenStrategy",
            "feldera_types::transport::datagen::DatagenStrategy",
        ),
        (
            "RngFieldSettings",
            "feldera_types::transport::datagen::RngFieldSettings",
        ),
        (
            "GenerationPlan",
            "feldera_types::transport::datagen::GenerationPlan",
        ),
        (
            "DatagenInputConfig",
            "feldera_types::transport::datagen::DatagenInputConfig",
        ),
        (
            "NexmarkInputConfig",
            "feldera_types::transport::nexmark::NexmarkInputConfig",
        ),
        (
            "NexmarkTable",
            "feldera_types::transport::nexmark::NexmarkTable",
        ),
        (
            "NexmarkInputOptions",
            "feldera_types::transport::nexmark::NexmarkInputOptions",
        ),
        (
            "DeltaTableIngestMode",
            "feldera_types::transport::delta_table::DeltaTableIngestMode",
        ),
        (
            "DeltaTableWriteMode",
            "feldera_types::transport::delta_table::DeltaTableWriteMode",
        ),
        (
            "DeltaTableReaderConfig",
            "feldera_types::transport::delta_table::DeltaTableReaderConfig",
        ),
        (
            "DeltaTableWriterConfig",
            "feldera_types::transport::delta_table::DeltaTableWriterConfig",
        ),
        ("Chunk", "feldera_types::transport::http::Chunk"),
        (
            "JsonUpdateFormat",
            "feldera_types::format::json::JsonUpdateFormat",
        ),
        (
            "ProgramSchema",
            "feldera_types::program_schema::ProgramSchema",
        ),
        ("Relation", "feldera_types::program_schema::Relation"),
        ("SqlType", "feldera_types::program_schema::SqlType"),
        ("Field", "feldera_types::program_schema::Field"),
        ("ColumnType", "feldera_types::program_schema::ColumnType"),
        (
            "IntervalUnit",
            "feldera_types::program_schema::IntervalUnit",
        ),
        (
            "SourcePosition",
            "feldera_types::program_schema::SourcePosition",
        ),
        (
            "PropertyValue",
            "feldera_types::program_schema::PropertyValue",
        ),
        ("ErrorResponse", "feldera_types::error::ErrorResponse"),
        (
            "OutputBufferConfig",
            "feldera_types::config::OutputBufferConfig",
        ),
        (
            "OutputEndpointConfig",
            "feldera_types::config::OutputEndpointConfig",
        ),
    ]
}

fn main() {
    let openapi = include_bytes!("openapi.json");
    println!("cargo:rerun-if-changed=../../openapi.json");
    let spec = serde_json::from_reader(&openapi[..]).unwrap();
    let mut settings = GenerationSettings::new();
    settings.with_interface(InterfaceStyle::Builder);
    for (from, to) in type_replacement() {
        let impls = vec![];
        settings.with_replacement(from, to, impls.into_iter());
    }

    let mut generator = progenitor::Generator::new(&settings);

    let tokens = generator.generate_tokens(&spec).unwrap();
    let ast = syn::parse2(tokens).unwrap();
    let content = prettyplease::unparse(&ast);

    let mut out_file = Path::new(&env::var("OUT_DIR").unwrap()).to_path_buf();
    out_file.push("codegen.rs");

    fs::write(out_file, content).unwrap();
}
