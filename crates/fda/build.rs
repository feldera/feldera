use progenitor::{GenerationSettings, InterfaceStyle};
use quote::quote;
use std::{
    env,
    fs::{self, File},
    path::Path,
};

fn type_replacement() -> Vec<(&'static str, &'static str)> {
    vec![
        ("PipelineConfig", "pipeline_types::config::PipelineConfig"),
        ("StorageConfig", "pipeline_types::config::StorageConfig"),
        (
            "StorageCacheConfig",
            "pipeline_types::config::StorageCacheConfig",
        ),
        ("RuntimeConfig", "pipeline_types::config::RuntimeConfig"),
        (
            "InputEndpointConfig",
            "pipeline_types::config::InputEndpointConfig",
        ),
        ("ConnectorConfig", "pipeline_types::config::ConnectorConfig"),
        (
            "OutputBufferConfig",
            "pipeline_types::config::OutputBufferConfig",
        ),
        (
            "OutputEndpointConfig",
            "pipeline_types::config::OutputEndpointConfig",
        ),
        ("TransportConfig", "pipeline_types::config::TransportConfig"),
        ("FormatConfig", "pipeline_types::config::FormatConfig"),
        ("ResourceConfig", "pipeline_types::config::ResourceConfig"),
        (
            "FileInputConfig",
            "pipeline_types::transport::file::FileInputConfig",
        ),
        (
            "FileOutputConfig",
            "pipeline_types::transport::file::FileOutputConfig",
        ),
        (
            "UrlInputConfig",
            "pipeline_types::transport::url::UrlInputConfig",
        ),
        (
            "KafkaHeader",
            "pipeline_types::transport::kafka::KafkaHeader",
        ),
        (
            "KafkaHeaderValue",
            "pipeline_types::transport::kafka::KafkaHeaderValue",
        ),
        (
            "KafkaLogLevel",
            "pipeline_types::transport::kafka::KafkaLogLevel",
        ),
        (
            "KafkaInputConfig",
            "pipeline_types::transport::kafka::KafkaInputConfig",
        ),
        (
            "KafkaOutputConfig",
            "pipeline_types::transport::kafka::KafkaOutputConfig",
        ),
        (
            "KafkaInputFtConfig",
            "pipeline_types::transport::kafka::KafkaInputFtConfig",
        ),
        (
            "KafkaOutputFtConfig",
            "pipeline_types::transport::kafka::KafkaOutputFtConfig",
        ),
        (
            "ConsumeStrategy",
            "pipeline_types::transport::s3::ConsumeStrategy",
        ),
        (
            "ReadStrategy",
            "pipeline_types::transport::s3::ReadStrategy",
        ),
        (
            "AwsCredentials",
            "pipeline_types::transport::s3::AwsCredentials",
        ),
        (
            "S3InputConfig",
            "pipeline_types::transport::s3::S3InputConfig",
        ),
        (
            "DatagenStrategy",
            "pipeline_types::transport::datagen::DatagenStrategy",
        ),
        (
            "RngFieldSettings",
            "pipeline_types::transport::datagen::RngFieldSettings",
        ),
        (
            "GenerationPlan",
            "pipeline_types::transport::datagen::GenerationPlan",
        ),
        (
            "DatagenInputConfig",
            "pipeline_types::transport::datagen::DatagenInputConfig",
        ),
        (
            "NexmarkInputConfig",
            "pipeline_types::transport::nexmark::NexmarkInputConfig",
        ),
        (
            "NexmarkTable",
            "pipeline_types::transport::nexmark::NexmarkTable",
        ),
        (
            "NexmarkInputOptions",
            "pipeline_types::transport::nexmark::NexmarkInputOptions",
        ),
        (
            "DeltaTableIngestMode",
            "pipeline_types::transport::delta_table::DeltaTableIngestMode",
        ),
        (
            "DeltaTableWriteMode",
            "pipeline_types::transport::delta_table::DeltaTableWriteMode",
        ),
        (
            "DeltaTableReaderConfig",
            "pipeline_types::transport::delta_table::DeltaTableReaderConfig",
        ),
        (
            "DeltaTableWriterConfig",
            "pipeline_types::transport::delta_table::DeltaTableWriterConfig",
        ),
        ("Chunk", "pipeline_types::transport::http::Chunk"),
        ("EgressMode", "pipeline_types::transport::http::EgressMode"),
        ("OutputQuery", "pipeline_types::query::OutputQuery"),
        (
            "NeighborhoodQuery",
            "pipeline_types::query::NeighborhoodQuery",
        ),
        (
            "JsonUpdateFormat",
            "pipeline_types::format::json::JsonUpdateFormat",
        ),
        (
            "ProgramSchema",
            "pipeline_types::program_schema::ProgramSchema",
        ),
        ("Relation", "pipeline_types::program_schema::Relation"),
        ("SqlType", "pipeline_types::program_schema::SqlType"),
        ("Field", "pipeline_types::program_schema::Field"),
        ("ColumnType", "pipeline_types::program_schema::ColumnType"),
        (
            "IntervalUnit",
            "pipeline_types::program_schema::IntervalUnit",
        ),
        (
            "SourcePosition",
            "pipeline_types::program_schema::SourcePosition",
        ),
        (
            "PropertyValue",
            "pipeline_types::program_schema::PropertyValue",
        ),
        ("ErrorResponse", "pipeline_types::error::ErrorResponse"),
        (
            "OutputBufferConfig",
            "pipeline_types::config::OutputBufferConfig",
        ),
        (
            "OutputEndpointConfig",
            "pipeline_types::config::OutputEndpointConfig",
        ),
    ]
}

fn main() {
    let src = "../../openapi.json";
    println!("cargo:rerun-if-changed={}", src);
    let file = File::open(src).unwrap();
    let spec = serde_json::from_reader(file).unwrap();
    let mut settings = GenerationSettings::new();
    settings
        .with_interface(InterfaceStyle::Builder)
        .with_inner_type(quote! {Option<String>})
        .with_pre_hook_async(quote! { crate::add_auth_headers });
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
