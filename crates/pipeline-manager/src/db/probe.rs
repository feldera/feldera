use crate::db::storage::Storage;
use crate::db::storage_postgres::StoragePostgres;
use crate::error::ManagerError;
use actix_web::{HttpResponse, ResponseError};
use chrono::{DateTime, Local};
use log::{error, trace};
use std::{sync::Arc, time::Duration};
use tokio::sync::Mutex;

#[derive(Debug)]
pub struct DbProbe {
    pub last_checked: DateTime<Local>,
    pub last_error: Option<ManagerError>,
}

async fn check_if_db_reachable(
    probe: Arc<Mutex<DbProbe>>,
    db: Arc<Mutex<StoragePostgres>>,
    interval: Duration,
) {
    loop {
        let now = Local::now();
        let res = db.lock().await.check_connection().await;
        let mut probe = probe.lock().await;
        probe.last_checked = now;
        probe.last_error = res.err().map(|e| e.into());
        if probe.last_error.is_some() {
            trace!("DB reachability probe returned an error: {:?}", probe);
        }
        drop(probe);
        tokio::time::sleep(interval).await;
    }
}

impl DbProbe {
    pub async fn new(db: Arc<Mutex<StoragePostgres>>) -> Arc<Mutex<DbProbe>> {
        let probe = DbProbe {
            last_checked: Local::now(),
            last_error: None,
        };
        let probe = Arc::new(Mutex::new(probe));
        tokio::spawn(check_if_db_reachable(
            probe.clone(),
            db,
            Duration::from_secs(5),
        ));
        probe
    }

    pub fn status_as_http_response(&self) -> Result<HttpResponse, ManagerError> {
        match &self.last_error {
            Some(e) => {
                error!(
                    "/healthz probe returning error (last_checked: {:?}, last_error: {})",
                    self.last_checked, e
                );
                Ok(e.error_response())
            }
            None => {
                trace!(
                    "/healthz probe returning ok (last_checked: {:?})",
                    self.last_checked,
                );
                Ok(HttpResponse::Ok().into())
            }
        }
    }
}
