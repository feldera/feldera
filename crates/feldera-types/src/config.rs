//! Controller configuration.
//!
//! This module defines the controller configuration structure.  The leaves of
//! this structure are individual transport-specific and data-format-specific
//! endpoint configs.  We represent these configs as opaque JSON values, so
//! that the entire configuration tree can be deserialized from a JSON file.

use crate::transport::adhoc::AdHocInputConfig;
use crate::transport::datagen::DatagenInputConfig;
use crate::transport::delta_table::{DeltaTableReaderConfig, DeltaTableWriterConfig};
use crate::transport::file::{FileInputConfig, FileOutputConfig};
use crate::transport::http::HttpInputConfig;
use crate::transport::iceberg::IcebergReaderConfig;
use crate::transport::kafka::{KafkaInputConfig, KafkaOutputConfig};
use crate::transport::nexmark::NexmarkInputConfig;
use crate::transport::postgres::{PostgresReaderConfig, PostgresWriterConfig};
use crate::transport::pubsub::PubSubInputConfig;
use crate::transport::redis::RedisOutputConfig;
use crate::transport::s3::S3InputConfig;
use crate::transport::url::UrlInputConfig;
use core::fmt;
use serde::de::{self, MapAccess, Visitor};
use serde::{Deserialize, Deserializer, Serialize};
use serde_json::Value as JsonValue;
use serde_yaml::Value as YamlValue;
use std::fmt::Display;
use std::path::Path;
use std::str::FromStr;
use std::time::Duration;
use std::{borrow::Cow, cmp::max, collections::BTreeMap};
use utoipa::openapi::{ObjectBuilder, OneOfBuilder, Ref, RefOr, Schema, SchemaType};
use utoipa::ToSchema;

const DEFAULT_MAX_PARALLEL_CONNECTOR_INIT: u64 = 10;

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
    /// If `global.storage` is `Some(_)`, this field must be set to some
    /// [`StorageConfig`].  If `global.storage` is `None``, the pipeline ignores
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
    pub fn max_parallel_connector_init(&self) -> u64 {
        max(
            self.global
                .max_parallel_connector_init
                .unwrap_or(DEFAULT_MAX_PARALLEL_CONNECTOR_INIT),
            1,
        )
    }

    pub fn with_storage(self, storage: Option<(StorageConfig, StorageOptions)>) -> Self {
        let (storage_config, storage_options) = storage.unzip();
        Self {
            global: RuntimeConfig {
                storage: storage_options,
                ..self.global
            },
            storage_config,
            ..self
        }
    }

    pub fn storage(&self) -> Option<(&StorageConfig, &StorageOptions)> {
        let storage_options = self.global.storage.as_ref();
        let storage_config = self.storage_config.as_ref();
        storage_config.zip(storage_options)
    }
}

/// Configuration for persistent storage in a [`PipelineConfig`].
#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize, ToSchema)]
pub struct StorageConfig {
    /// A directory to keep pipeline state, as a path on the filesystem of the
    /// machine or container where the pipeline will run.
    ///
    /// When storage is enabled, this directory stores the data for
    /// [StorageBackendConfig::Default].
    ///
    /// When fault tolerance is enabled, this directory stores checkpoints and
    /// the log.
    pub path: String,

    /// How to cache access to storage in this pipeline.
    #[serde(default)]
    pub cache: StorageCacheConfig,
}

impl StorageConfig {
    pub fn path(&self) -> &Path {
        Path::new(&self.path)
    }
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

/// Storage configuration for a pipeline.
#[derive(Debug, Clone, Default, Eq, PartialEq, Serialize, Deserialize, ToSchema)]
#[serde(default)]
pub struct StorageOptions {
    /// How to connect to the underlying storage.
    pub backend: StorageBackendConfig,

    /// For a batch of data maintained as part of a persistent index during a
    /// pipeline run, the minimum estimated number of bytes to write it to
    /// storage.
    ///
    /// This is provided for debugging and fine-tuning and should ordinarily be
    /// left unset.
    ///
    /// A value of 0 will write even empty batches to storage, and nonzero
    /// values provide a threshold.  `usize::MAX` would effectively disable
    /// storage for such batches.  The default is 1,048,576 (1 MiB).
    pub min_storage_bytes: Option<usize>,

    /// For a batch of data passed through the pipeline during a single step,
    /// the minimum estimated number of bytes to write it to storage.
    ///
    /// This is provided for debugging and fine-tuning and should ordinarily be
    /// left unset.  If it is set, it should ordinarily be greater than or equal
    /// to `min_storage_bytes`.
    ///
    /// A value of 0 will write even empty batches to storage, and nonzero
    /// values provide a threshold.  `usize::MAX` would effectively disable
    /// storage for such batches.  The default is 10,485,760 (10 MiB).
    pub min_step_storage_bytes: Option<usize>,

    /// The form of compression to use in data batches.
    ///
    /// Compression has a CPU cost but it can take better advantage of limited
    /// NVMe and network bandwidth, which means that it can increase overall
    /// performance.
    pub compression: StorageCompression,

    /// The maximum size of the in-memory storage cache, in MiB.
    ///
    /// If set, the specified cache size is spread across all the foreground and
    /// background threads. If unset, each foreground or background thread cache
    /// is limited to 256 MiB.
    pub cache_mib: Option<usize>,
}

/// Backend storage configuration.
#[derive(Debug, Clone, Default, Eq, PartialEq, Serialize, Deserialize, ToSchema)]
#[serde(tag = "name", content = "config", rename_all = "snake_case")]
pub enum StorageBackendConfig {
    /// Use the default storage configuration.
    ///
    /// This currently uses the local file system.
    #[default]
    Default,

    /// Object storage.
    Object(ObjectStorageConfig),
}

impl Display for StorageBackendConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            StorageBackendConfig::Default => write!(f, "default"),
            StorageBackendConfig::Object(_) => write!(f, "object"),
        }
    }
}

/// Storage compression algorithm.
#[derive(Debug, Copy, Clone, Default, Eq, PartialEq, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum StorageCompression {
    /// Use Feldera's default compression algorithm.
    ///
    /// The default may change as Feldera's performance is tuned and new
    /// algorithms are introduced.
    #[default]
    Default,

    /// Do not compress.
    None,

    /// Use [Snappy](https://en.wikipedia.org/wiki/Snappy_(compression)) compression.
    Snappy,
}

#[derive(Debug, Clone, Default, Eq, PartialEq, Serialize, Deserialize, ToSchema)]
pub struct ObjectStorageConfig {
    /// URL.
    ///
    /// The following URL schemes are supported:
    ///
    /// * S3:
    ///   - `s3://<bucket>/<path>`
    ///   - `s3a://<bucket>/<path>`
    ///   - `https://s3.<region>.amazonaws.com/<bucket>`
    ///   - `https://<bucket>.s3.<region>.amazonaws.com`
    ///   - `https://ACCOUNT_ID.r2.cloudflarestorage.com/bucket`
    /// * Google Cloud Storage:
    ///   - `gs://<bucket>/<path>`
    /// * Microsoft Azure Blob Storage:
    ///   - `abfs[s]://<container>/<path>` (according to [fsspec](https://github.com/fsspec/adlfs))
    ///   - `abfs[s]://<file_system>@<account_name>.dfs.core.windows.net/<path>`
    ///   - `abfs[s]://<file_system>@<account_name>.dfs.fabric.microsoft.com/<path>`
    ///   - `az://<container>/<path>` (according to [fsspec](https://github.com/fsspec/adlfs))
    ///   - `adl://<container>/<path>` (according to [fsspec](https://github.com/fsspec/adlfs))
    ///   - `azure://<container>/<path>` (custom)
    ///   - `https://<account>.dfs.core.windows.net`
    ///   - `https://<account>.blob.core.windows.net`
    ///   - `https://<account>.blob.core.windows.net/<container>`
    ///   - `https://<account>.dfs.fabric.microsoft.com`
    ///   - `https://<account>.dfs.fabric.microsoft.com/<container>`
    ///   - `https://<account>.blob.fabric.microsoft.com`
    ///   - `https://<account>.blob.fabric.microsoft.com/<container>`
    ///
    /// Settings derived from the URL will override other settings.
    pub url: String,

    /// Additional options as key-value pairs.
    ///
    /// The following keys are supported:
    ///
    /// * S3:
    ///   - `access_key_id`: AWS Access Key.
    ///   - `secret_access_key`: AWS Secret Access Key.
    ///   - `region`: Region.
    ///   - `default_region`: Default region.
    ///   - `endpoint`: Custom endpoint for communicating with S3,
    ///     e.g. `https://localhost:4566` for testing against a localstack
    ///     instance.
    ///   - `token`: Token to use for requests (passed to underlying provider).
    ///   - [Other keys](https://docs.rs/object_store/latest/object_store/aws/enum.AmazonS3ConfigKey.html#variants).
    /// * Google Cloud Storage:
    ///   - `service_account`: Path to the service account file.
    ///   - `service_account_key`: The serialized service account key.
    ///   - `google_application_credentials`: Application credentials path.
    ///   - [Other keys](https://docs.rs/object_store/latest/object_store/gcp/enum.GoogleConfigKey.html).
    /// * Microsoft Azure Blob Storage:
    ///   - `access_key`: Azure Access Key.
    ///   - `container_name`: Azure Container Name.
    ///   - `account`: Azure Account.
    ///   - `bearer_token_authorization`: Static bearer token for authorizing requests.
    ///   - `client_id`: Client ID for use in client secret or Kubernetes federated credential flow.
    ///   - `client_secret`: Client secret for use in client secret flow.
    ///   - `tenant_id`: Tenant ID for use in client secret or Kubernetes federated credential flow.
    ///   - `endpoint`: Override the endpoint for communicating with blob storage.
    ///   - [Other keys](https://docs.rs/object_store/latest/object_store/azure/enum.AzureConfigKey.html#variants).
    ///
    /// Options set through the URL take precedence over those set with these
    /// options.
    #[serde(flatten)]
    pub other_options: BTreeMap<String, String>,
}

/// Global pipeline configuration settings. This is the publicly
/// exposed type for users to configure pipelines.
#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize, ToSchema)]
#[serde(default)]
pub struct RuntimeConfig {
    /// Number of DBSP worker threads.
    ///
    /// Each DBSP "foreground" worker thread is paired with a "background"
    /// thread for LSM merging, making the total number of threads twice the
    /// specified number.
    pub workers: u16,

    /// Storage configuration.
    ///
    /// - If this is `None`, the default, the pipeline's state is kept in
    ///   in-memory data-structures.  This is useful if the pipeline's state
    ///   will fit in memory and if the pipeline is ephemeral and does not need
    ///   to be recovered after a restart. The pipeline will most likely run
    ///   faster since it does not need to access storage.
    ///
    /// - If set, the pipeline's state is kept on storage.  This allows the
    ///   pipeline to work with state that will not fit into memory. It also
    ///   allows the state to be checkpointed and recovered across restarts.
    #[serde(deserialize_with = "deserialize_storage_options")]
    pub storage: Option<StorageOptions>,

    /// Fault tolerance configuration.
    #[serde(deserialize_with = "deserialize_fault_tolerance")]
    pub fault_tolerance: FtConfig,

    /// Enable CPU profiler.
    ///
    /// The default value is `true`.
    pub cpu_profiler: bool,

    /// Enable pipeline tracing.
    pub tracing: bool,

    /// Jaeger tracing endpoint to send tracing information to.
    pub tracing_endpoint_jaeger: String,

    /// Minimal input batch size.
    ///
    /// The controller delays pushing input records to the circuit until at
    /// least `min_batch_size_records` records have been received (total
    /// across all endpoints) or `max_buffering_delay_usecs` microseconds
    /// have passed since at least one input records has been buffered.
    /// Defaults to 0.
    pub min_batch_size_records: u64,

    /// Maximal delay in microseconds to wait for `min_batch_size_records` to
    /// get buffered by the controller, defaults to 0.
    pub max_buffering_delay_usecs: u64,

    /// Resource reservations and limits. This is enforced
    /// only in Feldera Cloud.
    pub resources: ResourceConfig,

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
    pub clock_resolution_usecs: Option<u64>,

    /// Optionally, a list of CPU numbers for CPUs to which the pipeline may pin
    /// its worker threads.  Specify at least twice as many CPU numbers as
    /// workers.  CPUs are generally numbered starting from 0.  The pipeline
    /// might not be able to honor CPU pinning requests.
    ///
    /// CPU pinning can make pipelines run faster and perform more consistently,
    /// as long as different pipelines running on the same machine are pinned to
    /// different CPUs.
    pub pin_cpus: Vec<usize>,

    /// Timeout in seconds for the `Provisioning` phase of the pipeline.
    /// Setting this value will override the default of the runner.
    pub provisioning_timeout_secs: Option<u64>,

    /// The maximum number of connectors initialized in parallel during pipeline
    /// startup.
    ///
    /// At startup, the pipeline must initialize all of its input and output connectors.
    /// Depending on the number and types of connectors, this can take a long time.
    /// To accelerate the process, multiple connectors are initialized concurrently.
    /// This option controls the maximum number of connectors that can be intitialized
    /// in parallel.
    ///
    /// The default is 10.
    pub max_parallel_connector_init: Option<u64>,
}

/// Accepts "true" and "false" and converts them to the new format.
fn deserialize_storage_options<'de, D>(deserializer: D) -> Result<Option<StorageOptions>, D::Error>
where
    D: Deserializer<'de>,
{
    struct BoolOrStruct;

    impl<'de> Visitor<'de> for BoolOrStruct {
        type Value = Option<StorageOptions>;

        fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
            formatter.write_str("boolean or StorageOptions")
        }

        fn visit_bool<E>(self, v: bool) -> Result<Self::Value, E>
        where
            E: de::Error,
        {
            match v {
                false => Ok(None),
                true => Ok(Some(StorageOptions::default())),
            }
        }

        fn visit_unit<E>(self) -> Result<Self::Value, E>
        where
            E: de::Error,
        {
            Ok(None)
        }

        fn visit_none<E>(self) -> Result<Self::Value, E>
        where
            E: de::Error,
        {
            Ok(None)
        }

        fn visit_map<M>(self, map: M) -> Result<Option<StorageOptions>, M::Error>
        where
            M: MapAccess<'de>,
        {
            Deserialize::deserialize(de::value::MapAccessDeserializer::new(map)).map(Some)
        }
    }

    deserializer.deserialize_any(BoolOrStruct)
}

/// Accepts very old 'initial_state' and 'latest_checkpoint' as enabling fault
/// tolerance.
///
/// Accepts `null` as disabling fault tolerance.
///
/// Otherwise, deserializes [FtConfig] in the way that one might otherwise
/// expect.
fn deserialize_fault_tolerance<'de, D>(deserializer: D) -> Result<FtConfig, D::Error>
where
    D: Deserializer<'de>,
{
    struct StringOrStruct;

    impl<'de> Visitor<'de> for StringOrStruct {
        type Value = FtConfig;

        fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
            formatter.write_str("none or FtConfig or 'initial_state' or 'latest_checkpoint'")
        }

        fn visit_str<E>(self, v: &str) -> Result<Self::Value, E>
        where
            E: de::Error,
        {
            match v {
                "initial_state" | "latest_checkpoint" => Ok(FtConfig {
                    model: Some(FtModel::default()),
                    ..FtConfig::default()
                }),
                _ => Err(de::Error::invalid_value(de::Unexpected::Str(v), &self)),
            }
        }

        fn visit_unit<E>(self) -> Result<Self::Value, E>
        where
            E: de::Error,
        {
            Ok(FtConfig::default())
        }

        fn visit_none<E>(self) -> Result<Self::Value, E>
        where
            E: de::Error,
        {
            Ok(FtConfig::default())
        }

        fn visit_map<M>(self, map: M) -> Result<FtConfig, M::Error>
        where
            M: MapAccess<'de>,
        {
            Deserialize::deserialize(de::value::MapAccessDeserializer::new(map))
        }
    }

    deserializer.deserialize_any(StringOrStruct)
}

impl Default for RuntimeConfig {
    fn default() -> Self {
        Self {
            workers: 8,
            storage: Some(StorageOptions::default()),
            fault_tolerance: FtConfig::default(),
            cpu_profiler: true,
            tracing: {
                // We discovered that the jaeger crate can use up gigabytes of RAM, so it's not harmless
                // to keep it on by default.
                false
            },
            tracing_endpoint_jaeger: "127.0.0.1:6831".to_string(),
            min_batch_size_records: 0,
            max_buffering_delay_usecs: 0,
            resources: ResourceConfig::default(),
            clock_resolution_usecs: {
                // Every 100 ms.
                Some(100_000)
            },
            pin_cpus: Vec::new(),
            provisioning_timeout_secs: None,
            max_parallel_connector_init: None,
        }
    }
}

/// Fault-tolerance configuration.
///
/// The default [FtConfig] (via [FtConfig::default]) disables fault tolerance,
/// which is the configuration that one gets if [RuntimeConfig] omits fault
/// tolerance configuration.
///
/// The default value for [FtConfig::model] enables fault tolerance, as
/// `Some(FtModel::default())`.  This is the configuration that one gets if
/// [RuntimeConfig] includes a fault tolerance configuration but does not
/// specify a particular model.
#[derive(Debug, Copy, Clone, Eq, PartialEq, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub struct FtConfig {
    /// Fault tolerance model to use.
    #[serde(with = "none_as_string")]
    #[serde(default = "default_model")]
    #[schema(
        schema_with = none_as_string_schema::<FtModel>,
    )]
    pub model: Option<FtModel>,

    /// Interval between automatic checkpoints, in seconds.
    ///
    /// The default is 60 seconds.  Values less than 1 or greater than 3600 will
    /// be forced into that range.
    #[serde(default = "default_checkpoint_interval_secs")]
    pub checkpoint_interval_secs: u64,
}

fn default_model() -> Option<FtModel> {
    Some(FtModel::default())
}

pub fn default_checkpoint_interval_secs() -> u64 {
    60
}

impl Default for FtConfig {
    fn default() -> Self {
        Self {
            model: None,
            checkpoint_interval_secs: default_checkpoint_interval_secs(),
        }
    }
}

#[cfg(test)]
mod test {
    use super::deserialize_fault_tolerance;
    use crate::config::{FtConfig, FtModel};
    use serde::{Deserialize, Serialize};

    #[test]
    fn ft_config() {
        #[derive(Serialize, Deserialize, Default, PartialEq, Eq, Debug)]
        #[serde(default)]
        struct Wrapper {
            #[serde(deserialize_with = "deserialize_fault_tolerance")]
            config: FtConfig,
        }

        // Omitting FtConfig, or specifying null, or specifying model "none", disables fault tolerance.
        for s in [
            "{}",
            r#"{"config": null}"#,
            r#"{"config": {"model": "none"}}"#,
        ] {
            let config: Wrapper = serde_json::from_str(s).unwrap();
            assert_eq!(
                config,
                Wrapper {
                    config: FtConfig {
                        model: None,
                        ..FtConfig::default()
                    }
                }
            );
        }

        // Serializing disabled FT produces explicit "none" form.
        let s = serde_json::to_string(&Wrapper {
            config: FtConfig::default(),
        })
        .unwrap();
        assert!(s.contains("\"none\""));

        // `{}` for FtConfig, or `{...}` with `model` omitted, enables fault
        // tolerance.
        for s in [r#"{"config": {}}"#, r#"{"checkpoint_interval_secs": 60}"#] {
            assert_eq!(
                serde_json::from_str::<FtConfig>(s).unwrap(),
                FtConfig {
                    model: Some(FtModel::default()),
                    ..FtConfig::default()
                }
            );
        }
    }
}

impl FtConfig {
    pub fn is_enabled(&self) -> bool {
        self.model.is_some()
    }

    /// Returns the checkpoint interval, if fault tolerance is enabled, and
    /// otherwise `None`.
    pub fn checkpoint_interval(&self) -> Option<Duration> {
        self.is_enabled()
            .then(|| Duration::from_secs(self.checkpoint_interval_secs.clamp(1, 3600)))
    }
}

/// Serde implementation for de/serializing a string into `Option<T>` where
/// `"none"` indicates `None` and any other string indicates `Some`.
///
/// This could be extended to handle non-strings by adding more forwarding
/// `visit_*` methods to the Visitor implementation.  I don't see a way to write
/// them automatically.
mod none_as_string {
    use std::marker::PhantomData;

    use serde::de::{Deserialize, Deserializer, IntoDeserializer, Visitor};
    use serde::ser::{Serialize, Serializer};

    pub(super) fn serialize<S, T>(value: &Option<T>, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
        T: Serialize,
    {
        match value.as_ref() {
            Some(value) => value.serialize(serializer),
            None => "none".serialize(serializer),
        }
    }

    struct NoneAsString<T>(PhantomData<fn() -> T>);

    impl<'de, T> Visitor<'de> for NoneAsString<T>
    where
        T: Deserialize<'de>,
    {
        type Value = Option<T>;

        fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
            formatter.write_str("string")
        }

        fn visit_none<E>(self) -> Result<Self::Value, E>
        where
            E: serde::de::Error,
        {
            Ok(None)
        }

        fn visit_str<E>(self, value: &str) -> Result<Option<T>, E>
        where
            E: serde::de::Error,
        {
            if &value.to_ascii_lowercase() == "none" {
                Ok(None)
            } else {
                Ok(Some(T::deserialize(value.into_deserializer())?))
            }
        }
    }

    pub(super) fn deserialize<'de, D, T>(deserializer: D) -> Result<Option<T>, D::Error>
    where
        D: Deserializer<'de>,
        T: Deserialize<'de>,
    {
        deserializer.deserialize_str(NoneAsString(PhantomData))
    }
}

/// Generates an OpenAPI schema for an `Option<T>` field serialized with `none_as_string`.
/// The schema is a `oneOf` with a reference to `T`'s schema and a `"none"` string enum.
fn none_as_string_schema<'a, T: ToSchema<'a> + Default + Serialize>() -> Schema {
    Schema::OneOf(
        OneOfBuilder::new()
            .item(RefOr::Ref(Ref::new(format!(
                "#/components/schemas/{}",
                T::schema().0
            ))))
            .item(
                ObjectBuilder::new()
                    .schema_type(SchemaType::String)
                    .enum_values(Some(vec!["none"])),
            )
            .default(Some(
                serde_json::to_value(T::default()).expect("Failed to serialize default value"),
            ))
            .build(),
    )
}

/// Fault tolerance model.
///
/// The ordering is significant: we consider [Self::ExactlyOnce] to be a "higher
/// level" of fault tolerance than [Self::AtLeastOnce].
#[derive(
    Debug, Copy, Clone, Default, Eq, PartialEq, PartialOrd, Ord, Serialize, Deserialize, ToSchema,
)]
#[serde(rename_all = "snake_case")]
pub enum FtModel {
    /// Each record is output at least once.  Crashes may duplicate output, but
    /// no input or output is dropped.
    AtLeastOnce,

    /// Each record is output exactly once.  Crashes do not drop or duplicate
    /// input or output.
    #[default]
    ExactlyOnce,
}

impl FtModel {
    pub fn option_as_str(value: Option<FtModel>) -> &'static str {
        value.map_or("no", |model| model.as_str())
    }

    pub fn as_str(&self) -> &'static str {
        match self {
            FtModel::AtLeastOnce => "at_least_once",
            FtModel::ExactlyOnce => "exactly_once",
        }
    }
}

pub struct FtModelUnknown;

impl FromStr for FtModel {
    type Err = FtModelUnknown;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_ascii_lowercase().as_str() {
            "exactly_once" => Ok(Self::ExactlyOnce),
            "at_least_once" => Ok(Self::AtLeastOnce),
            _ => Err(FtModelUnknown),
        }
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

/// Deserialize the `start_after` property of a connector configuration.
/// It requires a non-standard deserialization because we want to accept
/// either a string or an array of strings.
fn deserialize_start_after<'de, D>(deserializer: D) -> Result<Option<Vec<String>>, D::Error>
where
    D: Deserializer<'de>,
{
    let value = Option::<JsonValue>::deserialize(deserializer)?;
    match value {
        Some(JsonValue::String(s)) => Ok(Some(vec![s])),
        Some(JsonValue::Array(arr)) => {
            let vec = arr
                .into_iter()
                .map(|item| {
                    item.as_str()
                        .map(|s| s.to_string())
                        .ok_or_else(|| serde::de::Error::custom("invalid 'start_after' property: expected a string, an array of strings, or null"))
                })
                .collect::<Result<Vec<String>, _>>()?;
            Ok(Some(vec))
        }
        Some(JsonValue::Null) | None => Ok(None),
        _ => Err(serde::de::Error::custom(
            "invalid 'start_after' property: expected a string, an array of strings, or null",
        )),
    }
}

/// A data connector's configuration
#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize, ToSchema)]
pub struct ConnectorConfig {
    /// Transport endpoint configuration.
    pub transport: TransportConfig,

    /// Parser configuration.
    pub format: Option<FormatConfig>,

    /// Name of the index that the connector is attached to.
    ///
    /// This property is valid for output connectors only.  It is used with data
    /// transports and formats that expect output updates in the form of key/value
    /// pairs, where the key typically represents a unique id associated with the
    /// table or view.
    ///
    /// To support such output formats, an output connector can be attached to an
    /// index created using the SQL CREATE INDEX statement.  An index of a table
    /// or view contains the same updates as the table or view itself, indexed by
    /// one or more key columns.
    ///
    /// See individual connector documentation for details on how they work
    /// with indexes.
    pub index: Option<String>,

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

    /// Arbitrary user-defined text labels associated with the connector.
    ///
    /// These labels can be used in conjunction with the `start_after` property
    /// to control the start order of connectors.
    #[serde(default)]
    pub labels: Vec<String>,

    /// Start the connector after all connectors with specified labels.
    ///
    /// This property is used to control the start order of connectors.
    /// The connector will not start until all connectors with the specified
    /// labels have finished processing all inputs.
    #[serde(deserialize_with = "deserialize_start_after")]
    #[serde(default)]
    pub start_after: Option<Vec<String>>,
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize, ToSchema)]
#[serde(default)]
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
    pub max_output_buffer_size_records: usize,
}

impl Default for OutputBufferConfig {
    fn default() -> Self {
        Self {
            enable_output_buffer: false,
            max_output_buffer_size_records: usize::MAX,
            max_output_buffer_time_millis: usize::MAX,
        }
    }
}

impl OutputBufferConfig {
    pub fn validate(&self) -> Result<(), String> {
        if self.enable_output_buffer
            && self.max_output_buffer_size_records == Self::default().max_output_buffer_size_records
            && self.max_output_buffer_time_millis == Self::default().max_output_buffer_time_millis
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
    RedisOutput(RedisOutputConfig),
    // Prevent rust from complaining about large size difference between enum variants.
    IcebergInput(Box<IcebergReaderConfig>),
    PostgresInput(PostgresReaderConfig),
    PostgresOutput(PostgresWriterConfig),
    Datagen(DatagenInputConfig),
    Nexmark(NexmarkInputConfig),
    /// Direct HTTP input: cannot be instantiated through API
    HttpInput(HttpInputConfig),
    /// Direct HTTP output: cannot be instantiated through API
    HttpOutput,
    /// Ad hoc input: cannot be instantiated through API
    AdHocInput(AdHocInputConfig),
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
            TransportConfig::IcebergInput(_) => "iceberg_input".to_string(),
            TransportConfig::PostgresInput(_) => "postgres_input".to_string(),
            TransportConfig::PostgresOutput(_) => "postgres_output".to_string(),
            TransportConfig::Datagen(_) => "datagen".to_string(),
            TransportConfig::Nexmark(_) => "nexmark".to_string(),
            TransportConfig::HttpInput(_) => "http_input".to_string(),
            TransportConfig::HttpOutput => "http_output".to_string(),
            TransportConfig::AdHocInput(_) => "adhoc_input".to_string(),
            TransportConfig::RedisOutput(_) => "redis_output".to_string(),
        }
    }
}

/// Data format specification used to parse raw data received from the
/// endpoint or to encode data sent to the endpoint.
#[derive(Debug, Clone, Eq, PartialEq, Hash, Serialize, Deserialize, ToSchema)]
pub struct FormatConfig {
    /// Format name, e.g., "csv", "json", "bincode", etc.
    pub name: Cow<'static, str>,

    /// Format-specific parser or encoder configuration.
    #[serde(default)]
    #[schema(value_type = Object)]
    pub config: YamlValue,
}

#[derive(Debug, Clone, Eq, PartialEq, Default, Serialize, Deserialize, ToSchema)]
#[serde(default)]
pub struct ResourceConfig {
    /// The minimum number of CPU cores to reserve
    /// for an instance of this pipeline
    pub cpu_cores_min: Option<u64>,

    /// The maximum number of CPU cores to reserve
    /// for an instance of this pipeline
    pub cpu_cores_max: Option<u64>,

    /// The minimum memory in Megabytes to reserve
    /// for an instance of this pipeline
    pub memory_mb_min: Option<u64>,

    /// The maximum memory in Megabytes to reserve
    /// for an instance of this pipeline
    pub memory_mb_max: Option<u64>,

    /// The total storage in Megabytes to reserve
    /// for an instance of this pipeline
    pub storage_mb_max: Option<u64>,

    /// Storage class to use for an instance of this pipeline.
    /// The class determines storage performance such as IOPS and throughput.
    pub storage_class: Option<String>,
}
