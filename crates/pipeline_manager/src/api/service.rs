/// API to create, modify and delete Services, which represent named external
/// services like Kafka, MySQL etc.
use super::{ManagerError, ServerState};
use crate::{
    api::{examples, parse_uuid_param},
    auth::TenantId,
    db::{storage::Storage, DBError, ServiceId},
};
use actix_web::{
    delete, get,
    http::header::{CacheControl, CacheDirective},
    patch, post,
    web::{self, Data as WebData, ReqData},
    HttpRequest, HttpResponse,
};
use log::info;
use pipeline_types::config::ServiceConfig;
use serde::{Deserialize, Serialize};
use utoipa::{IntoParams, ToSchema};
use uuid::Uuid;

/// Request to create a new service.
#[derive(Deserialize, ToSchema)]
pub(crate) struct NewServiceRequest {
    /// Service name.
    name: String,
    /// Service description.
    description: String,
    /// Service configuration.
    config: ServiceConfig,
}

/// Response to a service creation request.
#[derive(Serialize, ToSchema)]
pub(crate) struct NewServiceResponse {
    /// Unique id assigned to the new service.
    service_id: ServiceId,
}

#[derive(Debug, Deserialize, IntoParams)]
pub struct ServiceIdOrNameQuery {
    /// Unique service identifier.
    id: Option<Uuid>,
    /// Unique service name.
    name: Option<String>,
}

/// Request to update an existing service.
#[derive(Deserialize, ToSchema)]
pub(crate) struct UpdateServiceRequest {
    /// New service description.
    description: String,
    /// New config YAML. If absent, existing YAML will be kept unmodified.
    config: Option<ServiceConfig>,
}

/// Response to a config update request.
#[derive(Serialize, ToSchema)]
pub(crate) struct UpdateServiceResponse {}

/// Fetch services, optionally filtered by name or ID
#[utoipa::path(
    responses(
        (status = OK, description = "List of services retrieved successfully", body = [ServiceDescr]),
        (status = NOT_FOUND
            , description = "Specified service name or ID does not exist"
            , body = ErrorResponse
            , examples(
                ("Unknown service name" = (value = json!(examples::unknown_name()))),
                ("Unknown service ID" = (value = json!(examples::unknown_service())))
            ),
        )
    ),
    params(ServiceIdOrNameQuery),
    context_path = "/v0",
    security(("JSON web token (JWT) or API key" = [])),
    tag = "Services"
)]
#[get("/services")]
async fn list_services(
    state: WebData<ServerState>,
    tenant_id: ReqData<TenantId>,
    req: web::Query<ServiceIdOrNameQuery>,
) -> Result<HttpResponse, DBError> {
    let descr = if let Some(id) = req.id {
        vec![
            state
                .db
                .lock()
                .await
                .get_service_by_id(*tenant_id, ServiceId(id))
                .await?,
        ]
    } else if let Some(name) = req.name.clone() {
        vec![
            state
                .db
                .lock()
                .await
                .get_service_by_name(*tenant_id, name)
                .await?,
        ]
    } else {
        state.db.lock().await.list_services(*tenant_id).await?
    };

    Ok(HttpResponse::Ok()
        .insert_header(CacheControl(vec![CacheDirective::NoCache]))
        .json(&descr))
}

/// Create a new service.
#[utoipa::path(
    request_body = NewServiceRequest,
    responses(
        (status = OK, description = "Service successfully created.", body = NewServiceResponse),
    ),
    context_path = "/v0",
    security(("JSON web token (JWT) or API key" = [])),
    tag = "Services"
)]
#[post("/services")]
async fn new_service(
    state: WebData<ServerState>,
    tenant_id: ReqData<TenantId>,
    request: web::Json<NewServiceRequest>,
) -> Result<HttpResponse, DBError> {
    let service_id = state
        .db
        .lock()
        .await
        .new_service(
            *tenant_id,
            Uuid::now_v7(),
            &request.name,
            &request.description,
            &request.config,
        )
        .await?;

    info!("Created service {service_id} (tenant:{})", *tenant_id);
    Ok(HttpResponse::Ok()
        .insert_header(CacheControl(vec![CacheDirective::NoCache]))
        .json(&NewServiceResponse { service_id }))
}

/// Change a service's description or configuration.
#[utoipa::path(
    request_body = UpdateServiceRequest,
    responses(
        (status = OK, description = "Service successfully updated.", body = UpdateServiceResponse),
        (status = NOT_FOUND
            , description = "Specified service id does not exist."
            , body = ErrorResponse
            , example = json!(examples::unknown_service())),
    ),
    params(
        ("service_id" = Uuid, Path, description = "Unique service identifier")
    ),
    context_path = "/v0",
    security(("JSON web token (JWT) or API key" = [])),
    tag = "Services"
)]
#[patch("/services/{service_id}")]
async fn update_service(
    state: WebData<ServerState>,
    tenant_id: ReqData<TenantId>,
    req: HttpRequest,
    body: web::Json<UpdateServiceRequest>,
) -> Result<HttpResponse, ManagerError> {
    let service_id = ServiceId(parse_uuid_param(&req, "service_id")?);
    state
        .db
        .lock()
        .await
        .update_service(*tenant_id, service_id, &body.description, &body.config)
        .await?;

    info!("Updated service {service_id} (tenant:{})", *tenant_id);
    Ok(HttpResponse::Ok()
        .insert_header(CacheControl(vec![CacheDirective::NoCache]))
        .json(&UpdateServiceResponse {}))
}

/// Delete an existing service.
#[utoipa::path(
    responses(
        (status = OK, description = "Service successfully deleted."),
        (status = BAD_REQUEST
            , description = "Specified service id is not a valid uuid."
            , body = ErrorResponse
            , example = json!(examples::invalid_uuid_param())),
        (status = NOT_FOUND
            , description = "Specified service id does not exist."
            , body = ErrorResponse
            , example = json!(examples::unknown_service())),
    ),
    params(
        ("service_id" = Uuid, Path, description = "Unique service identifier")
    ),
    context_path = "/v0",
    security(("JSON web token (JWT) or API key" = [])),
    tag = "Services"
)]
#[delete("/services/{service_id}")]
async fn delete_service(
    state: WebData<ServerState>,
    tenant_id: ReqData<TenantId>,
    req: HttpRequest,
) -> Result<HttpResponse, ManagerError> {
    let service_id = ServiceId(parse_uuid_param(&req, "service_id")?);

    state
        .db
        .lock()
        .await
        .delete_service(*tenant_id, service_id)
        .await?;

    info!("Deleted service {service_id} (tenant:{})", *tenant_id);
    Ok(HttpResponse::Ok().finish())
}

/// Fetch a service by ID.
#[utoipa::path(
    responses(
        (status = OK, description = "Service retrieved successfully.", body = ServiceDescr),
        (status = BAD_REQUEST
            , description = "Specified service id is not a valid uuid."
            , body = ErrorResponse
            , example = json!(examples::invalid_uuid_param())),
    ),
    params(
        ("service_id" = Uuid, Path, description = "Unique service identifier"),
    ),
    context_path = "/v0",
    security(("JSON web token (JWT) or API key" = [])),
    tag = "Services"
)]
#[get("/services/{service_id}")]
async fn get_service(
    state: WebData<ServerState>,
    tenant_id: ReqData<TenantId>,
    req: HttpRequest,
) -> Result<HttpResponse, ManagerError> {
    let service_id = ServiceId(parse_uuid_param(&req, "service_id")?);
    let descr = state
        .db
        .lock()
        .await
        .get_service_by_id(*tenant_id, service_id)
        .await?;
    Ok(HttpResponse::Ok()
        .insert_header(CacheControl(vec![CacheDirective::NoCache]))
        .json(&descr))
}
