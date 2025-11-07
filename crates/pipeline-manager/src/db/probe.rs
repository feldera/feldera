use crate::db::storage::Storage;
use crate::db::storage_postgres::StoragePostgres;
use crate::error::ManagerError;
use actix_web::HttpResponse;
use chrono::{DateTime, Utc};
use serde_json::json;
use std::{sync::Arc, time::Duration};
use tokio::sync::Mutex;
use tracing::{error, info};

/// Interval at which the database is probed in the background.
const DATABASE_PROBE_INTERVAL: Duration = Duration::from_secs(5);

pub struct DbProbe {
    pub last_checked: DateTime<Utc>,
    pub last_error: Option<ManagerError>,
}

impl DbProbe {
    /// Creates a new database probe that runs in the background.
    /// The background thread regularly updates the returned shared mutex.
    pub async fn new(db: Arc<Mutex<StoragePostgres>>) -> Arc<Mutex<DbProbe>> {
        let probe = DbProbe {
            last_checked: Utc::now(),
            last_error: None,
        };
        let probe = Arc::new(Mutex::new(probe));
        tokio::spawn(regular_database_probe(
            probe.clone(),
            db,
            DATABASE_PROBE_INTERVAL,
        ));
        probe
    }

    /// Converts the database probe status to an HTTP response that can be returned by health check
    /// endpoints.
    pub fn as_http_response(&self) -> HttpResponse {
        match &self.last_error {
            Some(_e) => HttpResponse::InternalServerError().json(json!({
                "status": "unhealthy: unable to reach database (see logs for further details)"
            })),
            None => HttpResponse::Ok().json(json!({
                "status": "healthy"
            })),
        }
    }
}

/// Regularly probes the database connection at an interval.
/// Each time the result is used to update the shared mutex.
async fn regular_database_probe(
    probe: Arc<Mutex<DbProbe>>,
    db: Arc<Mutex<StoragePostgres>>,
    interval: Duration,
) {
    loop {
        // Perform probe
        let probe_at = Utc::now();
        let probe_error = match db.lock().await.check_connection().await {
            Ok(()) => None,
            Err(e) => {
                error!(
                    "Regular database connection probe failed (retry in {}s) -- is the database available? Error: {e}",
                    interval.as_secs(),
                );
                Some(e.into())
            }
        };

        // Update the mutex
        let mut probe = probe.lock().await;
        let has_recovered = probe_error.is_none() && probe.last_error.is_some();
        *probe = DbProbe {
            last_checked: probe_at,
            last_error: probe_error,
        };
        drop(probe);

        // Log if the database connection recovered
        if has_recovered {
            info!("Regular database connection probe succeeded (recovered)");
        }

        // Sleep until next attempt
        tokio::time::sleep(interval).await;
    }
}
