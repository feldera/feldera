use serde::{Deserialize, Serialize};

// Metrics for an input endpoint.
///
/// Serializes to match the subset of fields needed for error tracking.
#[derive(Default, Serialize, Deserialize, Debug, Clone)]
pub struct InputEndpointErrorMetrics {
    pub endpoint_name: String,
    #[serde(default)]
    pub num_transport_errors: u64,
    #[serde(default)]
    pub num_parse_errors: u64,
}

/// Metrics for an output endpoint.
///
/// Serializes to match the subset of fields needed for error tracking.
#[derive(Default, Serialize, Deserialize, Debug, Clone)]
pub struct OutputEndpointErrorMetrics {
    pub endpoint_name: String,
    #[serde(default)]
    pub num_encode_errors: u64,
    #[serde(default)]
    pub num_transport_errors: u64,
}

/// Endpoint statistics containing metrics.
///
/// Wraps metrics in a structure that matches the full stats API shape.
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct EndpointErrorStats<T> {
    #[serde(default)]
    pub metrics: T,
}

/// Pipeline error statistics response from the runtime.
///
/// Lightweight response containing only error counts from all endpoints.
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct PipelineStatsErrorsResponse {
    #[serde(default)]
    pub inputs: Vec<EndpointErrorStats<InputEndpointErrorMetrics>>,
    #[serde(default)]
    pub outputs: Vec<EndpointErrorStats<OutputEndpointErrorMetrics>>,
}
