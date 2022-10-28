//! Controller configuration.
//!
//! This module defines the controller configuration structure.  The leaves of
//! this structure are individual transport-specific and data-format-specific
//! endpoint configs.  We represent these configs as opaque yaml values, so
//! that the entire configuration tree can be deserialized from a yaml file.

use super::EndpointId;
use serde::{Deserialize, Serialize};
use serde_yaml::Value as YamlValue;
use std::{borrow::Cow, collections::BTreeMap};

/// Default value of `InputEndpointConfig::max_buffered_records`.
/// It is declared as a function and not as a constant, so it can
/// be used in `#[serde(default="default_max_buffered_records")]`.
const fn default_max_buffered_records() -> u64 {
    1_000_000
}

/// Controller configuration specified by the user when creating
/// a new controller instance.
#[derive(Clone, Serialize, Deserialize)]
pub struct ControllerConfig {
    /// Global controller configuration.
    #[serde(flatten)]
    pub global: GlobalControllerConfig,

    /// Input endpoint configuration.
    pub inputs: BTreeMap<Cow<'static, str>, InputEndpointConfig>,
}

/// Internal representaion of controller configuration stored inside the
/// controller. Similar to `ControllerConfig`, but indexes endpoints by id
/// instead of names.
#[derive(Clone, Serialize, Deserialize)]
pub struct ControllerInnerConfig {
    pub global: GlobalControllerConfig,
    pub inputs: BTreeMap<EndpointId, InputEndpointConfig>,
}

impl ControllerInnerConfig {
    pub fn new(global: GlobalControllerConfig) -> Self {
        Self {
            global,
            inputs: BTreeMap::new(),
        }
    }
}

/// Global controller configuration settings.
#[derive(Clone, Serialize, Deserialize)]
pub struct GlobalControllerConfig {
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
}

#[derive(Clone, Serialize, Deserialize)]
pub struct InputEndpointConfig {
    /// Transport endpoint configuration.
    pub transport: TransportConfig,

    /// Parser configuration.
    pub format: FormatConfig,

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

/// Transport endpoint configuration.
#[derive(Clone, Serialize, Deserialize)]
pub struct TransportConfig {
    /// Data transport name, e.g., "file", "kafka", "kinesis", etc.
    pub name: Cow<'static, str>,

    /// Transport-specific endpoint configuration passed to
    /// [`Transport::new_endpoint`](`crate::Transport::new_endpoint`).
    pub config: YamlValue,
}

/// Data format specification used to parse raw data received from the
/// endpoint.
#[derive(Clone, Serialize, Deserialize)]
pub struct FormatConfig {
    /// Format name, e.g., "csv", "json", "bincode", etc.
    pub name: Cow<'static, str>,

    /// Format-specific parser configuration.
    pub config: YamlValue,
}
