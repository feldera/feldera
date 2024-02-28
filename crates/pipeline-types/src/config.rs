//! Controller configuration.
//!
//! This module defines the controller configuration structure.  The leaves of
//! this structure are individual transport-specific and data-format-specific
//! endpoint configs.  We represent these configs as opaque yaml values, so
//! that the entire configuration tree can be deserialized from a yaml file.

use serde::{Deserialize, Serialize};
use serde_yaml::Value as YamlValue;
use std::{borrow::Cow, collections::BTreeMap};
use utoipa::ToSchema;

use crate::query::OutputQuery;

/// Default value of `InputEndpointConfig::max_buffered_records`.
/// It is declared as a function and not as a constant, so it can
/// be used in `#[serde(default="default_max_buffered_records")]`.
pub const fn default_max_buffered_records() -> u64 {
    1_000_000
}

/// Default number of DBSP worker threads.
const fn default_workers() -> u16 {
    1
}

/// Pipeline configuration specified by the user when creating
/// a new pipeline instance.
///
/// This is the shape of the overall pipeline configuration, but is not
/// the publicly exposed type with which users configure pipelines.
#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize, ToSchema)]
pub struct PipelineConfig {
    /// Global controller configuration.
    #[serde(flatten)]
    #[schema(inline)]
    pub global: RuntimeConfig,

    /// Pipeline name
    pub name: Option<String>,

    /// Storage location.
    ///
    /// An identifier for location where the pipeline's state is stored.
    /// If not set, the pipeline's state is not persisted across
    /// restarts.
    pub storage_location: Option<String>,

    /// Input endpoint configuration.
    pub inputs: BTreeMap<Cow<'static, str>, InputEndpointConfig>,

    /// Output endpoint configuration.
    #[serde(default)]
    pub outputs: BTreeMap<Cow<'static, str>, OutputEndpointConfig>,
}

/// Global pipeline configuration settings. This is the publicly
/// exposed type for users to configure pipelines.
#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize, ToSchema)]
pub struct RuntimeConfig {
    /// Number of DBSP worker threads.
    #[serde(default = "default_workers")]
    pub workers: u16,

    /// Enable CPU profiler.
    #[serde(default)]
    pub cpu_profiler: bool,

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
}

impl RuntimeConfig {
    pub fn from_yaml(s: &str) -> Self {
        serde_yaml::from_str(s).unwrap()
    }

    pub fn to_yaml(config: &Self) -> String {
        serde_yaml::to_string(config).unwrap()
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
    pub connector_config: PipelineConnectorConfig,
}

/// A data connector's configuration
#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize, ToSchema)]
pub struct PipelineConnectorConfig {
    /// Transport endpoint configuration.
    pub transport: TransportConfig,

    /// Parser configuration.
    pub format: PipelineFormatConfig,

    /// Backpressure threshold.
    ///
    /// Maximal amount of records buffered by the endpoint before the endpoint
    /// is paused by the backpressure mechanism.  Note that this is not a
    /// hard bound: there can be a small delay between the backpressure
    /// mechanism is triggered and the endpoint is paused, during which more
    /// data may be received.
    ///
    /// The default is 1 million.
    #[serde(default = "default_max_buffered_records")]
    pub max_buffered_records: u64,
}

impl PipelineConnectorConfig {
    pub fn from_yaml_str(s: &str) -> Self {
        serde_yaml::from_str(s).unwrap()
    }

    pub fn to_yaml(&self) -> String {
        serde_yaml::to_string(&self).unwrap()
    }
}

/// Describes an output connector configuration
#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize, ToSchema)]
pub struct OutputEndpointConfig {
    /// The name of the output stream of the circuit that this endpoint is
    /// connected to.
    pub stream: Cow<'static, str>,

    /// Query over the output stream.  Only used for HTTP API endpoints.
    #[serde(skip)]
    pub query: OutputQuery,

    /// Connector configuration.
    #[serde(flatten)]
    pub connector_config: PipelineConnectorConfig,
}

/// Transport endpoint configuration.
#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize, ToSchema)]
pub struct TransportConfig {
    /// Data transport name, e.g., `file`, `kafka`, `kinesis`
    pub name: Cow<'static, str>,

    /// Transport-specific endpoint configuration passed to
    /// `crate::OutputTransport::new_endpoint`
    /// and `crate::InputTransport::new_endpoint`.
    #[serde(default)]
    #[schema(value_type = Object)]
    pub config: YamlValue,
}

/// Data format specification used to parse raw data received from the
/// endpoint or to encode data sent to the endpoint.
#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize, ToSchema)]
pub struct PipelineFormatConfig {
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
}
