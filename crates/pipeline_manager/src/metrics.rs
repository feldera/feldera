use std::sync::Arc;

use actix_web::{
    get,
    http::{header::ContentType, Method},
    web, HttpResponse, HttpServer, Responder,
};
use prometheus_client::{encoding::text::encode, registry::Registry};
use tokio::sync::Mutex;

use crate::api::ManagerError;
use crate::db::storage::Storage;
use crate::db::{PipelineStatus, ProjectDB};
use crate::runner::RunnerApi;

/// Initialize a metrics registry. This registry has to be passed
/// to every sub-system that wants to register metrics.
pub fn init() -> Registry {
    Registry::default()
}

/// Create a scrape endpoint for metrics on http://0.0.0.0:8081/metrics
pub async fn create_endpoint(registry: Registry, db: Arc<Mutex<ProjectDB>>) {
    let registry = web::Data::new(registry);
    let db = web::Data::new(db);

    let _http = tokio::spawn(
        HttpServer::new(move || {
            actix_web::App::new()
                .app_data(db.clone())
                .app_data(registry.clone())
                .service(metrics)
        })
        .bind(("0.0.0.0", 8081))
        .unwrap()
        .run(),
    );
}

/// A prometheus-compatible metrics scrape endpoint.
#[get("/metrics")]
async fn metrics(
    db: web::Data<Arc<Mutex<ProjectDB>>>,
    registry: web::Data<Registry>,
) -> Result<impl Responder, ManagerError> {
    let mut buffer = String::new();

    let db = db.lock().await;
    let pipelines = db.all_pipelines().await?;
    for (tenant_id, pipeline_id) in pipelines {
        let rts = db
            .get_pipeline_runtime_state_by_id(tenant_id, pipeline_id)
            .await?;

        // Get the metrics for all running pipelines, don't write anything
        // if the request fails.
        if rts.current_status == PipelineStatus::Running {
            if let Ok(r) =
                RunnerApi::pipeline_http_request(pipeline_id, Method::GET, "metrics", &rts.location)
                    .await
            {
                if r.status().is_success() {
                    if let Ok(r) = r.text().await {
                        buffer += &r;
                    }
                }
            }
        }
    }

    // Finally, add pipeline-manager metrics
    encode(&mut buffer, &registry).unwrap();

    Ok(HttpResponse::Ok()
        .content_type(ContentType::plaintext())
        .body(buffer))
}
