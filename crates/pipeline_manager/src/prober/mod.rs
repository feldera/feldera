use crate::api::{ManagerError, ServiceProbe};
use crate::config::ProberConfig;
use crate::db::storage::Storage;
use crate::db::{DBError, ProjectDB};
use actix_web::{get, web, HttpServer, Responder};
use log::{debug, error, info};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex;
use tokio::time::sleep;
use tokio::{join, spawn};

/// Constantly checks for new probes being created in the database.
/// It performs the probes sequentially one after the other (sequentially).
/// If there are no probes to perform, it briefly sleeps before checking again.
///
/// Probes which are in `created` or `running` status are fetched one after the
/// other ordered ascending by their `created_at` timestamp (thus, earliest
/// created first). It also fetches `running` status, as currently there is only
/// a single prober service, and it allows recovering if the prober service was
/// unexpectedly terminated in the middle of a probe.
///
/// TODO: concurrent probing with a sliding window rather than sequential
async fn service_probing_task(
    config: ProberConfig,
    db: Arc<Mutex<ProjectDB>>,
) -> Result<(), ManagerError> {
    info!(
        "Probing service started (sleeps {} ms before checking again if there are no probes)",
        config.probe_sleep_inbetween_ms
    );
    loop {
        let result: Result<(), DBError> = {
            loop {
                let probe = db.lock().await.next_service_probe().await?;
                if probe.is_none() {
                    break;
                }
                let (service_probe_id, tenant_id, request, service_config) = probe.unwrap();
                info!("Probe: {}", service_probe_id);

                // Store in database the probe is started
                let started_at = chrono::offset::Utc::now();
                db.lock()
                    .await
                    .update_service_probe_set_running(tenant_id, service_probe_id, &started_at)
                    .await?;

                // Perform the probe with timeout
                debug!("Probe request: {:?}", &request);
                let response =
                    service_config.probe(request, Duration::from_millis(config.probe_timeout_ms));
                let finished_at = chrono::offset::Utc::now();
                debug!("Probe response: {:?}", &response);

                // Store in database the probe is finished
                db.lock()
                    .await
                    .update_service_probe_set_finished(
                        tenant_id,
                        service_probe_id,
                        response,
                        &finished_at,
                    )
                    .await?;
            }
            Ok(())
        };
        match result {
            Ok(_) => {}
            Err(e) => {
                error!("Probing failed unexpectedly: {e}");
            }
        };

        // Wait inbetween probing
        sleep(Duration::from_millis(config.probe_sleep_inbetween_ms)).await;
    }
}

/// Health check endpoint.
/// Checks whether the prober is able to reach the database.
#[get("/healthz")]
async fn healthz(
    probe: web::Data<Arc<Mutex<crate::probe::Probe>>>,
) -> Result<impl Responder, ManagerError> {
    probe.lock().await.status_as_http_response()
}

/// Runs the prober service, which checks for probes being
/// created in the database.
///
/// Upon start, two threads are spawned, one which performs the
/// probing and the other an HTTP server to expose service health
/// and statistics.
///
/// Returns when the two threads are finished and joined.
pub async fn run_prober(
    config: &ProberConfig,
    db: Arc<Mutex<ProjectDB>>,
) -> Result<(), ManagerError> {
    let probing_task = spawn(service_probing_task(config.clone(), db.clone()));
    let db_probe = web::Data::new(crate::probe::Probe::new(db.clone()).await);
    let port = config.prober_http_server_port;
    let http = spawn(
        HttpServer::new(move || {
            actix_web::App::new()
                .app_data(db_probe.clone())
                .service(healthz)
        })
        .bind(("0.0.0.0", port))
        .unwrap()
        .run(),
    );
    join!(probing_task, http).0.unwrap()?;
    Ok(())
}
