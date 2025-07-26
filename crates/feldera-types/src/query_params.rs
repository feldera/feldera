//! Types for the query parameters of the pipeline endpoints.

use serde::Deserialize;
use utoipa::{IntoParams, ToSchema};

/// Circuit metrics output format.
/// - `prometheus`: [format](https://github.com/prometheus/docs/blob/4b1b80f5f660a2f8dc25a54f52a65a502f31879a/docs/instrumenting/exposition_formats.md) expected by Prometheus
/// - `json`: JSON format
#[derive(Debug, Deserialize, ToSchema)]
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
