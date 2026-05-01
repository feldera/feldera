use crate::config::CommonConfig;
use crate::db::storage::Storage;
use crate::db::storage_postgres::StoragePostgres;
use std::{sync::Arc, time::Duration};
use tokio::sync::Mutex;
use tracing::{debug, error, info};

/// Interval at which the cleanup of events takes place.
const EVENTS_CLEANUP_INTERVAL: Duration = Duration::from_secs(30);

/// Indefinitely clean up pipeline monitor events.
pub async fn events_cleaner(db: Arc<Mutex<StoragePostgres>>, common_config: CommonConfig) {
    info!(
        "Pipeline monitor events cleanup: running every {} seconds (retaining {} events per pipeline)",
        EVENTS_CLEANUP_INTERVAL.as_secs(),
        common_config.pipeline_monitor_events_retention
    );
    loop {
        match db
            .lock()
            .await
            .delete_pipeline_monitor_events_exceeding_retention(
                common_config.pipeline_monitor_events_retention,
            )
            .await
        {
            Ok(num_deleted) => {
                if num_deleted > 0 {
                    debug!("Pipeline monitor events cleanup: deleted {num_deleted} events that exceeded retention limits");
                }
            }
            Err(e) => {
                error!("Pipeline monitor events cleanup: unable to delete events due to: {e}");
            }
        }
        tokio::time::sleep(EVENTS_CLEANUP_INTERVAL).await;
    }
}
