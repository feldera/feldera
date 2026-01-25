//! Types for the query parameters of the pipeline endpoints.

use serde::Deserialize;
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

#[derive(Debug, Deserialize, IntoParams, ToSchema)]
pub struct SamplyProfileParams {
    #[serde(default = "default_samply_profile_duration")]
    pub duration_secs: u64,
}

/// Default for the `duration_secs` query parameter when POST a pipeline samply profile.
fn default_samply_profile_duration() -> u64 {
    30
}

/// Query parameters to retrieve samply profile.
#[derive(Debug, Deserialize, IntoParams, ToSchema)]
#[into_params(parameter_in = Query)]
pub struct SamplyProfileGetParams {
    /// If true, returns 204 redirect with Retry-After header if profile collection is in progress.
    /// If false or not provided, returns the last collected profile.
    #[serde(default)]
    pub latest: bool,
}
