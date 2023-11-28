use std::{sync::Arc, time::Duration};

use actix_web::{HttpResponse, ResponseError};
use chrono::{DateTime, Local};
use log::{error, trace};
use tokio::sync::Mutex;

use crate::{
    api::ManagerError,
    db::{storage::Storage, ProjectDB},
};

#[derive(Debug)]
pub struct Probe {
    pub last_checked: DateTime<Local>,
    pub last_error: Option<ManagerError>,
}

async fn check_if_db_reachable(
    probe: Arc<Mutex<Probe>>,
    db: Arc<Mutex<ProjectDB>>,
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

impl Probe {
    pub async fn new(db: Arc<Mutex<ProjectDB>>) -> Arc<Mutex<Probe>> {
        let probe = Probe {
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
