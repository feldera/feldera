use crate::metrics::ConnectorMetrics;
use crate::transport::OutputEndpointCallbacks;
use anyhow::Error as AnyError;
use feldera_types::adapter_stats::ConnectorHealth;
use std::sync::Arc;
use tracing::info;

/// Output endpoint callbacks for tests that drive an endpoint without a
/// controller: logs what the endpoint reports.
pub struct MockOutputCallbacks;

impl OutputEndpointCallbacks for MockOutputCallbacks {
    fn error(&self, fatal: bool, error: AnyError, tag: Option<&'static str>) {
        info!("output endpoint error: ({fatal:?}, {error:?}, {tag:?})");
    }

    fn update_connector_health(&self, health: ConnectorHealth) {
        info!("output endpoint health: {health:?}");
    }

    fn set_custom_metrics(&self, _metrics: Arc<dyn ConnectorMetrics>) {}
}
