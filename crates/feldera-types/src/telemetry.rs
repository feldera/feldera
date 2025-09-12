//! Telemetry events format exchanged between platform and telemetry service.
use serde::{Deserialize, Serialize};

/// Request to register a telemetry event.
/// Shared type between client and server.
#[derive(Serialize, Deserialize, Debug)]
pub struct RegisterTelemetryEventRequest {
    pub account_id: String,
    pub license_key: String,
    pub event: TelemetryEvent,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum TelemetryEvent {
    /// Pipeline provisioning succeeded.
    PipelineProvisioned { id_hash: String },
    /// Pipeline shutdown finished successfully.
    PipelineShutdownFinished { id_hash: String },
    /// Statistics update about a deployed pipeline.
    PipelineStatistics {
        id_hash: String,
        statistics: serde_json::Value,
    },
}
