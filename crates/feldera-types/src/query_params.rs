//! Types for the query parameters of the pipeline endpoints.

use serde::{Deserialize, Serialize};
use utoipa::{IntoParams, ToSchema};

use crate::runtime_status::RuntimeDesiredStatus;

/// Circuit metrics output format.
/// - `prometheus`: [format](https://github.com/prometheus/docs/blob/4b1b80f5f660a2f8dc25a54f52a65a502f31879a/docs/instrumenting/exposition_formats.md) expected by Prometheus
/// - `json`: JSON format
#[derive(Copy, Clone, Debug, PartialEq, Eq, Deserialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum MetricsFormat {
    Prometheus,
    Json,
}

/// Returns default metrics format.
fn default_metrics_format() -> MetricsFormat {
    MetricsFormat::Prometheus
}

/// Query parameters to retrieve pipeline circuit metrics.
#[derive(Debug, Deserialize, IntoParams, ToSchema)]
pub struct MetricsParameters {
    #[serde(default = "default_metrics_format")]
    pub format: MetricsFormat,
}

#[derive(Debug, Deserialize, IntoParams, ToSchema)]
#[serde(default)]
pub struct ActivateParams {
    #[serde(with = "crate::runtime_status::snake_case_runtime_desired_status")]
    pub initial: RuntimeDesiredStatus,
}

impl Default for ActivateParams {
    fn default() -> Self {
        Self {
            initial: RuntimeDesiredStatus::Running,
        }
    }
}

#[derive(Debug, Deserialize, Serialize, IntoParams, ToSchema)]
#[serde(default)]
pub struct SamplyProfileParams {
    /// In a multihost pipeline, the ordinal of the pipeline to sample.
    pub ordinal: usize,
    /// The number of seconds to sample for the profile.
    pub duration_secs: u64,
}

/// Default query parameters for POST of a pipeline samply profile.
impl Default for SamplyProfileParams {
    fn default() -> Self {
        Self {
            ordinal: 0,
            duration_secs: 30,
        }
    }
}

/// Query parameters to retrieve samply profile.
#[derive(Debug, Default, Deserialize, Serialize, IntoParams, ToSchema)]
#[into_params(parameter_in = Query)]
#[serde(default)]
pub struct SamplyProfileGetParams {
    /// In a multihost pipeline, the ordinal of the pipeline to sample.
    pub ordinal: usize,
    /// If true, returns 204 redirect with Retry-After header if profile collection is in progress.
    /// If false or not provided, returns the last collected profile.
    pub latest: bool,
}
