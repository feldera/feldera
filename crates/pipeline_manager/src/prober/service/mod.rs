mod kafka;
mod probe;

use kafka::probe_kafka_service;
use pipeline_types::service::ServiceConfig;
use std::time::Duration;

// Exported
pub use probe::{
    ServiceProbeError, ServiceProbeRequest, ServiceProbeResponse, ServiceProbeResult,
    ServiceProbeStatus, ServiceProbeType,
};

/// Perform the probe for the service.
/// Returns with a failure if the timeout is exceeded.
pub fn probe_service(
    service_config: &ServiceConfig,
    probe: ServiceProbeRequest,
    timeout: Duration,
) -> ServiceProbeResponse {
    match service_config {
        ServiceConfig::Kafka(kafka_service) => probe_kafka_service(kafka_service, probe, timeout),
    }
}
