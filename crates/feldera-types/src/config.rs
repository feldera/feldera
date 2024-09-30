//! Controller configuration.
//!
//! This module defines the controller configuration structure.  The leaves of
//! this structure are individual transport-specific and data-format-specific
//! endpoint configs.  We represent these configs as opaque yaml values, so
//! that the entire configuration tree can be deserialized from a yaml file.

use crate::transport::datagen::DatagenInputConfig;
use crate::transport::delta_table::{DeltaTableReaderConfig, DeltaTableWriterConfig};
use crate::transport::file::{FileInputConfig, FileOutputConfig};
use crate::transport::kafka::{KafkaInputConfig, KafkaOutputConfig};
use crate::transport::nexmark::NexmarkInputConfig;
use crate::transport::pubsub::PubSubInputConfig;
use crate::transport::s3::S3InputConfig;
use crate::transport::url::UrlInputConfig;
use serde::{Deserialize, Serialize};
use serde_yaml::Value as YamlValue;
use std::{borrow::Cow, collections::BTreeMap};
use utoipa::ToSchema;

/// Default value of `ConnectorConfig::max_queued_records`.
pub const fn default_max_queued_records() -> u64 {
    1_000_000
}

/// Default maximum batch size for connectors, in records.
///
/// If you change this then update the comment on
/// [ConnectorConfig::max_batch_size].
pub const fn default_max_batch_size() -> u64 {
    10_000
}

/// Default number of DBSP worker threads.
const fn default_workers() -> u16 {
    8
}

/// Default endpoint to send tracing data to.
fn default_tracing_endpoint() -> String {
    "127.0.0.1:6831".to_string()
}

/// Default value of `RuntimeConfig::tracing`.
// We discovered that the jaeger crate can use up gigabytes of RAM, so it's not harmless
// to keep it on by default.
fn default_tracing() -> bool {
    false
}

/// Pipeline deployment configuration.
/// It represents configuration entries directly provided by the user
/// (e.g., runtime configuration) and entries derived from the schema
/// of the compiled program (e.g., connectors). Storage configuration,
/// if applicable, is set by the runner.
#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize, ToSchema)]
pub struct PipelineConfig {
    /// Global controller configuration.
    #[serde(flatten)]
    #[schema(inline)]
    pub global: RuntimeConfig,

    /// Pipeline name.
    pub name: Option<String>,

    /// Configuration for persistent storage
    ///
    /// If `global.storage` is `true`, this field must be set to some
    /// [`StorageConfig`].  If `global.storage` is `false`, the pipeline ignores
    /// this field.
    #[serde(default)]
    pub storage_config: Option<StorageConfig>,

    /// Input endpoint configuration.
    pub inputs: BTreeMap<Cow<'static, str>, InputEndpointConfig>,

    /// Output endpoint configuration.
    #[serde(default)]
    pub outputs: BTreeMap<Cow<'static, str>, OutputEndpointConfig>,
}

impl PipelineConfig {
    pub fn from_yaml(s: &str) -> Self {
        serde_yaml::from_str(s).unwrap()
    }

    pub fn to_yaml(&self) -> String {
        serde_yaml::to_string(self).unwrap()
    }
}

/// Configuration for persistent storage in a [`PipelineConfig`].
#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize, ToSchema)]
pub struct StorageConfig {
    /// The location where the pipeline state is stored or will be stored.
    ///
    /// It should point to a path on the file-system of the machine/container
    /// where the pipeline will run. If that path doesn't exist yet, or if it
    /// does not contain any checkpoints, then the pipeline creates it and
    /// starts from an initial state in which no data has yet been received. If
    /// it does exist, then the pipeline starts from the most recent checkpoint
    /// that already exists there. In either case, (further) checkpoints will be
    /// written there.
    pub path: String,

    /// How to cache access to storage in this pipeline.
    #[serde(default)]
    pub cache: StorageCacheConfig,
}

/// How to cache access to storage within a Feldera pipeline.
#[derive(Copy, Clone, Default, Deserialize, Serialize, Debug, PartialEq, Eq, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum StorageCacheConfig {
    /// Use the operating system's page cache as the primary storage cache.
    ///
    /// This is the default because it currently performs better than
    /// `FelderaCache`.
    #[default]
    PageCache,

    /// Use Feldera's internal cache implementation.
    ///
    /// This is under development. It will become the default when its
    /// performance exceeds that of `PageCache`.
    FelderaCache,
}

impl StorageCacheConfig {
    #[cfg(unix)]
    pub fn to_custom_open_flags(&self) -> i32 {
        match self {
            StorageCacheConfig::PageCache => (),
            StorageCacheConfig::FelderaCache => {
                #[cfg(target_os = "linux")]
                return libc::O_DIRECT;
            }
        }
        0
    }
}

/// Enable the CPU profile by default.
fn default_cpu_profiler() -> bool {
    true
}

/// Update the clock every 100ms by default.
fn default_clock_resolution_usecs() -> Option<u64> {
    Some(100_000)
}

/// Global pipeline configuration settings. This is the publicly
/// exposed type for users to configure pipelines.
#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize, ToSchema)]
pub struct RuntimeConfig {
    /// Number of DBSP worker threads.
    #[serde(default = "default_workers")]
    pub workers: u16,

    /// Should persistent storage be enabled for this pipeline?
    ///
    /// - If `false` (default), the pipeline's state is kept in in-memory data-structures.
    ///   This is useful if the pipeline is ephemeral and does not need to be recovered
    ///   after a restart. The pipeline will most likely run faster since it does not
    ///   need to read from, or write to disk
    ///
    /// - If `true`, the pipeline state is stored in the specified location,
    ///   is persisted across restarts, and can be checkpointed and recovered.
    ///   This feature is currently experimental.
    #[serde(default)]
    pub storage: bool,

    /// Enable CPU profiler.
    ///
    /// The default value is `true`.
    #[serde(default = "default_cpu_profiler")]
    pub cpu_profiler: bool,

    /// Enable pipeline tracing.
    #[serde(default = "default_tracing")]
    pub tracing: bool,

    /// Jaeger tracing endpoint to send tracing information to.
    #[serde(default = "default_tracing_endpoint")]
    pub tracing_endpoint_jaeger: String,

    /// Minimal input batch size.
    ///
    /// The controller delays pushing input records to the circuit until at
    /// least `min_batch_size_records` records have been received (total
    /// across all endpoints) or `max_buffering_delay_usecs` microseconds
    /// have passed since at least one input records has been buffered.
    /// Defaults to 0.
    #[serde(default)]
    pub min_batch_size_records: u64,

    /// Maximal delay in microseconds to wait for `min_batch_size_records` to
    /// get buffered by the controller, defaults to 0.
    #[serde(default)]
    pub max_buffering_delay_usecs: u64,

    /// Resource reservations and limits. This is enforced
    /// only in Feldera Cloud.
    #[serde(default)]
    pub resources: ResourceConfig,

    /// The minimum estimated number of bytes in a batch of data to write it to
    /// storage.  This is provided for debugging and fine-tuning and should
    /// ordinarily be left unset. It only has an effect when `storage` is set to
    /// true.
    ///
    /// A value of 0 will write even empty batches to storage, and nonzero
    /// values provide a threshold.  `usize::MAX` would effectively disable
    /// storage.
    pub min_storage_bytes: Option<usize>,

    /// Real-time clock resolution in microseconds.
    ///
    /// This parameter controls the execution of queries that use the `NOW()` function.  The output of such
    /// queries depends on the real-time clock and can change over time without any external
    /// inputs.  The pipeline will update the clock value and trigger incremental recomputation
    /// at most each `clock_resolution_usecs` microseconds.
    ///
    /// It is set to 100 milliseconds (100,000 microseconds) by default.
    ///
    /// Set to `null` to disable periodic clock updates.
    #[serde(default = "default_clock_resolution_usecs")]
    pub clock_resolution_usecs: Option<u64>,
}

impl Default for RuntimeConfig {
    fn default() -> Self {
        Self {
            workers: default_workers(),
            storage: false,
            cpu_profiler: default_cpu_profiler(),
            tracing: default_tracing(),
            tracing_endpoint_jaeger: default_tracing_endpoint(),
            min_batch_size_records: 0,
            max_buffering_delay_usecs: 0,
            resources: ResourceConfig::default(),
            min_storage_bytes: None,
            clock_resolution_usecs: default_clock_resolution_usecs(),
        }
    }
}

impl RuntimeConfig {
    pub fn from_yaml(s: &str) -> Self {
        serde_yaml::from_str(s).unwrap()
    }

    pub fn to_yaml(&self) -> String {
        serde_yaml::to_string(self).unwrap()
    }
}

/// Describes an input connector configuration
#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize, ToSchema)]
pub struct InputEndpointConfig {
    /// The name of the input stream of the circuit that this endpoint is
    /// connected to.
    pub stream: Cow<'static, str>,

    /// Connector configuration.
    #[serde(flatten)]
    pub connector_config: ConnectorConfig,
}

/// A data connector's configuration
#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize, ToSchema)]
pub struct ConnectorConfig {
    /// Transport endpoint configuration.
    pub transport: TransportConfig,

    /// Parser configuration.
    pub format: Option<FormatConfig>,

    /// Output buffer configuration.
    #[serde(flatten)]
    pub output_buffer_config: OutputBufferConfig,

    /// Maximum batch size, in records.
    ///
    /// This is the maximum number of records to process in one batch through
    /// the circuit.  The time and space cost of processing a batch is
    /// asymptotically superlinear in the size of the batch, but very small
    /// batches are less efficient due to constant factors.
    ///
    /// This should usually be less than `max_queued_records`, to give the
    /// connector a round-trip time to restart and refill the buffer while
    /// batches are being processed.
    ///
    /// Some input adapters might not honor this setting.
    ///
    /// The default is 10,000.
    #[serde(default = "default_max_batch_size")]
    pub max_batch_size: u64,

    /// Backpressure threshold.
    ///
    /// Maximal number of records queued by the endpoint before the endpoint
    /// is paused by the backpressure mechanism.
    ///
    /// For input endpoints, this setting bounds the number of records that have
    /// been received from the input transport but haven't yet been consumed by
    /// the circuit since the circuit, since the circuit is still busy processing
    /// previous inputs.
    ///
    /// For output endpoints, this setting bounds the number of records that have
    /// been produced by the circuit but not yet sent via the output transport endpoint
    /// nor stored in the output buffer (see `enable_output_buffer`).
    ///
    /// Note that this is not a hard bound: there can be a small delay between
    /// the backpressure mechanism is triggered and the endpoint is paused, during
    /// which more data may be queued.
    ///
    /// The default is 1 million.
    #[serde(default = "default_max_queued_records")]
    pub max_queued_records: u64,

    /// Create connector in paused state.
    ///
    /// The default is `false`.
    #[serde(default)]
    pub paused: bool,
}

fn default_max_buffer_time_millis() -> usize {
    usize::MAX
}

fn default_max_buffer_size_records() -> usize {
    usize::MAX
}

#[derive(Debug, Default, Clone, Eq, PartialEq, Serialize, Deserialize, ToSchema)]
pub struct OutputBufferConfig {
    /// Enable output buffering.
    ///
    /// The output buffering mechanism allows decoupling the rate at which the pipeline
    /// pushes changes to the output transport from the rate of input changes.
    ///
    /// By default, output updates produced by the pipeline are pushed directly to
    /// the output transport. Some destinations may prefer to receive updates in fewer
    /// bigger batches. For instance, when writing Parquet files, producing
    /// one bigger file every few minutes is usually better than creating
    /// small files every few milliseconds.
    ///
    /// To achieve such input/output decoupling, users can enable output buffering by
    /// setting the `enable_output_buffer` flag to `true`.  When buffering is enabled, output
    /// updates produced by the pipeline are consolidated in an internal buffer and are
    /// pushed to the output transport when one of several conditions is satisfied:
    ///
    /// * data has been accumulated in the buffer for more than `max_output_buffer_time_millis`
    ///   milliseconds.
    /// * buffer size exceeds `max_output_buffer_size_records` records.
    ///
    /// This flag is `false` by default.
    // TODO: on-demand output triggered via the API.
    #[serde(default)]
    pub enable_output_buffer: bool,

    /// Maximum time in milliseconds data is kept in the output buffer.
    ///
    /// By default, data is kept in the buffer indefinitely until one of
    /// the other output conditions is satisfied.  When this option is
    /// set the buffer will be flushed at most every
    /// `max_output_buffer_time_millis` milliseconds.
    ///
    /// NOTE: this configuration option requires the `enable_output_buffer` flag
    /// to be set.
    #[serde(default = "default_max_buffer_time_millis")]
    pub max_output_buffer_time_millis: usize,

    /// Maximum number of updates to be kept in the output buffer.
    ///
    /// This parameter bounds the maximal size of the buffer.
    /// Note that the size of the buffer is not always equal to the
    /// total number of updates output by the pipeline. Updates to the
    /// same record can overwrite or cancel previous updates.
    ///
    /// By default, the buffer can grow indefinitely until one of
    /// the other output conditions is satisfied.
    ///
    /// NOTE: this configuration option requires the `enable_output_buffer` flag
    /// to be set.
    #[serde(default = "default_max_buffer_size_records")]
    pub max_output_buffer_size_records: usize,
}

impl OutputBufferConfig {
    pub fn validate(&self) -> Result<(), String> {
        if self.enable_output_buffer
            && self.max_output_buffer_size_records == default_max_buffer_size_records()
            && self.max_output_buffer_time_millis == default_max_buffer_time_millis()
        {
            return Err(
                "when the 'enable_output_buffer' flag is set, one of 'max_output_buffer_size_records' and 'max_output_buffer_time_millis' settings must be specified"
                    .to_string(),
            );
        }

        Ok(())
    }
}

/// Describes an output connector configuration
#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize, ToSchema)]
pub struct OutputEndpointConfig {
    /// The name of the output stream of the circuit that this endpoint is
    /// connected to.
    pub stream: Cow<'static, str>,

    /// Connector configuration.
    #[serde(flatten)]
    pub connector_config: ConnectorConfig,
}

/// Transport-specific endpoint configuration passed to
/// `crate::OutputTransport::new_endpoint`
/// and `crate::InputTransport::new_endpoint`.
#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize, ToSchema)]
#[serde(tag = "name", content = "config", rename_all = "snake_case")]
pub enum TransportConfig {
    FileInput(FileInputConfig),
    FileOutput(FileOutputConfig),
    KafkaInput(KafkaInputConfig),
    KafkaOutput(KafkaOutputConfig),
    PubSubInput(PubSubInputConfig),
    UrlInput(UrlInputConfig),
    S3Input(S3InputConfig),
    DeltaTableInput(DeltaTableReaderConfig),
    DeltaTableOutput(DeltaTableWriterConfig),
    Datagen(DatagenInputConfig),
    Nexmark(NexmarkInputConfig),
    /// Direct HTTP input: cannot be instantiated through API
    HttpInput,
    /// Direct HTTP output: cannot be instantiated through API
    HttpOutput,
}

impl TransportConfig {
    pub fn name(&self) -> String {
        match self {
            TransportConfig::FileInput(_) => "file_input".to_string(),
            TransportConfig::FileOutput(_) => "file_output".to_string(),
            TransportConfig::KafkaInput(_) => "kafka_input".to_string(),
            TransportConfig::KafkaOutput(_) => "kafka_output".to_string(),
            TransportConfig::PubSubInput(_) => "pub_sub_input".to_string(),
            TransportConfig::UrlInput(_) => "url_input".to_string(),
            TransportConfig::S3Input(_) => "s3_input".to_string(),
            TransportConfig::DeltaTableInput(_) => "delta_table_input".to_string(),
            TransportConfig::DeltaTableOutput(_) => "delta_table_output".to_string(),
            TransportConfig::Datagen(_) => "datagen".to_string(),
            TransportConfig::Nexmark(_) => "nexmark".to_string(),
            TransportConfig::HttpInput => "http_input".to_string(),
            TransportConfig::HttpOutput => "http_output".to_string(),
        }
    }
}

/// Data format specification used to parse raw data received from the
/// endpoint or to encode data sent to the endpoint.
#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize, ToSchema)]
pub struct FormatConfig {
    /// Format name, e.g., "csv", "json", "bincode", etc.
    pub name: Cow<'static, str>,

    /// Format-specific parser or encoder configuration.
    #[serde(default)]
    #[schema(value_type = Object)]
    pub config: YamlValue,
}

#[derive(Debug, Clone, Eq, PartialEq, Default, Serialize, Deserialize, ToSchema)]
pub struct ResourceConfig {
    /// The minimum number of CPU cores to reserve
    /// for an instance of this pipeline
    #[serde(default)]
    pub cpu_cores_min: Option<u64>,

    /// The maximum number of CPU cores to reserve
    /// for an instance of this pipeline
    #[serde(default)]
    pub cpu_cores_max: Option<u64>,

    /// The minimum memory in Megabytes to reserve
    /// for an instance of this pipeline
    #[serde(default)]
    pub memory_mb_min: Option<u64>,

    /// The maximum memory in Megabytes to reserve
    /// for an instance of this pipeline
    #[serde(default)]
    pub memory_mb_max: Option<u64>,

    /// The total storage in Megabytes to reserve
    /// for an instance of this pipeline
    #[serde(default)]
    pub storage_mb_max: Option<u64>,

    /// Storage class to use for an instance of this pipeline.
    /// The class determines storage performance such as IOPS and throughput.
    #[serde(default)]
    pub storage_class: Option<String>,
}
