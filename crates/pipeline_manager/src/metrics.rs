use actix_web::{get, http::header::ContentType, web, HttpResponse, HttpServer, Responder};
use prometheus_client::{encoding::text::encode, registry::Registry};

use crate::api::ManagerError;

/// Initialize a metrics registry. This registry has to be passed
/// to every sub-system that wants to register metrics.
pub fn init() -> Registry {
    Registry::default()
}

/// Create a scrape endpoint for metrics on http://0.0.0.0:9000/metrics
pub async fn create_endpoint(registry: Registry) {
    let registry = web::Data::new(registry);
    let _http = tokio::spawn(
        HttpServer::new(move || {
            actix_web::App::new()
                .app_data(registry.clone())
                .service(metrics)
        })
        .bind(("0.0.0.0", 9000))
        .unwrap()
        .run(),
    );
}

/// A prometheus-compatible metrics scrape endpoint.
#[get("/metrics")]
async fn metrics(registry: web::Data<Registry>) -> Result<impl Responder, ManagerError> {
    let mut buffer = String::new();
    encode(&mut buffer, &registry).unwrap();
    Ok(HttpResponse::Ok()
        .content_type(ContentType::plaintext())
        .body(buffer))
}
