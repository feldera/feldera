/// API to create, modify and delete Services,
/// which represent named external services such as Kafka.
use super::{ManagerError, ServerState};
use crate::api::{parse_string_param, ServiceConfig};
use crate::{
    api::examples,
    auth::TenantId,
    db::{storage::Storage, DBError, ServiceId},
};
use actix_web::{
    delete, get,
    http::header::{CacheControl, CacheDirective},
    patch, post, put,
    web::{self, Data as WebData, ReqData},
    HttpRequest, HttpResponse,
};
use log::info;
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
    /// Service configuration (JSON).
    config: ServiceConfig,
}

/// Response to a service creation request.
#[derive(Serialize, ToSchema)]
pub(crate) struct NewServiceResponse {
    /// Unique id assigned to the new service.
    service_id: ServiceId,
}

/// Request to retrieve a (filtered) list of services.
/// If multiple filters are provided, only a single
/// filter will be applied (first if `id`, then if `name`, and then if
/// `config_type`). If no filter is provided, return the full list of services.
#[derive(Debug, Deserialize, IntoParams)]
pub struct ListServicesRequest {
    /// If provided, will filter based on exact match of the service identifier.
    id: Option<Uuid>,
    /// If provided, will filter based on exact match of the service name.
    name: Option<String>,
    /// If provided, will filter based on exact match of the configuration type.
    config_type: Option<String>,
}

/// Request to update an existing service.
#[derive(Deserialize, ToSchema)]
pub(crate) struct UpdateServiceRequest {
    /// New service name. If absent, existing name will be kept unmodified.
    name: Option<String>,
    /// New service description. If absent, existing name will be kept
    /// unmodified.
    description: Option<String>,
    /// New service configuration (JSON). If absent, existing configuration will
    /// be kept unmodified.
    config: Option<ServiceConfig>,
}

/// Response to a service update request.
#[derive(Serialize, ToSchema)]
pub(crate) struct UpdateServiceResponse {}

/// Request to create or replace a service.
#[derive(Deserialize, ToSchema)]
pub(crate) struct CreateOrReplaceServiceRequest {
    /// Service description.
    description: String,
    /// Service configuration (JSON).
    config: ServiceConfig,
}

/// Response to a create or replace service request.
#[derive(Serialize, ToSchema)]
pub(crate) struct CreateOrReplaceServiceResponse {
    /// Unique id assigned to the service.
    service_id: ServiceId,
}

/// Fetch services, optionally filtered by name or ID.
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
    params(ListServicesRequest),
    context_path = "/v0",
    security(("JSON web token (JWT) or API key" = [])),
    tag = "Services"
)]
#[get("/services")]
async fn list_services(
    state: WebData<ServerState>,
    tenant_id: ReqData<TenantId>,
    req: web::Query<ListServicesRequest>,
) -> Result<HttpResponse, DBError> {
    let descr = if let Some(id) = req.id {
        vec![
            state
                .db
                .lock()
                .await
                .get_service_by_id(*tenant_id, ServiceId(id), None)
                .await?,
        ]
    } else if let Some(name) = req.name.clone() {
        vec![
            state
                .db
                .lock()
                .await
                .get_service_by_name(*tenant_id, &name, None)
                .await?,
        ]
    } else {
        state
            .db
            .lock()
            .await
            .list_services(*tenant_id, &req.config_type.as_deref())
            .await?
    };

    Ok(HttpResponse::Ok()
        .insert_header(CacheControl(vec![CacheDirective::NoCache]))
        .json(&descr))
}

/// Create a new service.
#[utoipa::path(
    request_body = NewServiceRequest,
    responses(
        (status = CREATED, description = "Service successfully created", body = NewServiceResponse),
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
            None,
        )
        .await?;

    info!(
        "Created service with name {} and id {} (tenant: {})",
        &request.name, service_id, *tenant_id
    );
    Ok(HttpResponse::Created()
        .insert_header(CacheControl(vec![CacheDirective::NoCache]))
        .json(&NewServiceResponse { service_id }))
}

/// Update the name, description and/or configuration of a service.
#[utoipa::path(
    request_body = UpdateServiceRequest,
    responses(
        (status = OK, description = "Service successfully updated", body = UpdateServiceResponse),
        (status = NOT_FOUND
            , description = "Specified service name does not exist"
            , body = ErrorResponse
            , example = json!(examples::unknown_name())
        ),
    ),
    params(
        ("service_name" = String, Path, description = "Unique service name")
    ),
    context_path = "/v0",
    security(("JSON web token (JWT) or API key" = [])),
    tag = "Services"
)]
#[patch("/services/{service_name}")]
async fn update_service(
    state: WebData<ServerState>,
    tenant_id: ReqData<TenantId>,
    req: HttpRequest,
    body: web::Json<UpdateServiceRequest>,
) -> Result<HttpResponse, ManagerError> {
    let service_name = parse_string_param(&req, "service_name")?;
    let db = state.db.lock().await;
    db.update_service_by_name(
        *tenant_id,
        &service_name,
        &body.name.as_deref(),
        &body.description.as_deref(),
        &body.config,
    )
    .await?;

    info!("Updated service {service_name} (tenant: {})", *tenant_id);
    Ok(HttpResponse::Ok()
        .insert_header(CacheControl(vec![CacheDirective::NoCache]))
        .json(&UpdateServiceResponse {}))
}

/// Create or replace a service.
#[utoipa::path(
    request_body = CreateOrReplaceServiceRequest,
    responses(
        (status = CREATED, description = "Service created successfully", body = CreateOrReplaceServiceResponse),
        (status = OK, description = "Service updated successfully", body = CreateOrReplaceServiceResponse),
        (status = CONFLICT
            , description = "A service with this name already exists in the database"
            , body = ErrorResponse
            , example = json!(examples::duplicate_name())),
    ),
    params(
        ("service_name" = String, Path, description = "Unique service name")
    ),
    context_path = "/v0",
    security(("JSON web token (JWT) or API key" = [])),
    tag = "Services"
)]
#[put("/services/{service_name}")]
async fn create_or_replace_service(
    state: WebData<ServerState>,
    tenant_id: ReqData<TenantId>,
    request: HttpRequest,
    body: web::Json<CreateOrReplaceServiceRequest>,
) -> Result<HttpResponse, ManagerError> {
    let service_name = parse_string_param(&request, "service_name")?;
    let (created, service_id) = state
        .db
        .lock()
        .await
        .create_or_replace_service(*tenant_id, &service_name, &body.description, &body.config)
        .await?;
    if created {
        Ok(HttpResponse::Created()
            .insert_header(CacheControl(vec![CacheDirective::NoCache]))
            .json(&CreateOrReplaceServiceResponse { service_id }))
    } else {
        Ok(HttpResponse::Ok()
            .insert_header(CacheControl(vec![CacheDirective::NoCache]))
            .json(&CreateOrReplaceServiceResponse { service_id }))
    }
}

/// Delete an existing service.
#[utoipa::path(
    responses(
        (status = OK, description = "Service successfully deleted"),
        (status = NOT_FOUND
            , description = "Specified service name does not exist"
            , body = ErrorResponse
            , example = json!(examples::unknown_name())),
    ),
    params(
        ("service_name" = String, Path, description = "Unique service name")
    ),
    context_path = "/v0",
    security(("JSON web token (JWT) or API key" = [])),
    tag = "Services"
)]
#[delete("/services/{service_name}")]
async fn delete_service(
    state: WebData<ServerState>,
    tenant_id: ReqData<TenantId>,
    req: HttpRequest,
) -> Result<HttpResponse, ManagerError> {
    let service_name = parse_string_param(&req, "service_name")?;
    state
        .db
        .lock()
        .await
        .delete_service(*tenant_id, &service_name)
        .await?;

    info!("Deleted service {service_name} (tenant: {})", *tenant_id);
    Ok(HttpResponse::Ok().finish())
}

/// Fetch a service by name.
#[utoipa::path(
    responses(
        (status = OK, description = "Service retrieved successfully.", body = ServiceDescr),
        (status = NOT_FOUND
        , description = "Specified service name does not exist"
        , body = ErrorResponse
        , example = json!(examples::unknown_name()))
    ),
    params(
        ("service_name" = String, Path, description = "Unique service name"),
    ),
    context_path = "/v0",
    security(("JSON web token (JWT) or API key" = [])),
    tag = "Services"
)]
#[get("/services/{service_name}")]
async fn get_service(
    state: WebData<ServerState>,
    tenant_id: ReqData<TenantId>,
    req: HttpRequest,
) -> Result<HttpResponse, ManagerError> {
    let service_name = parse_string_param(&req, "service_name")?;
    let descr = state
        .db
        .lock()
        .await
        .get_service_by_name(*tenant_id, &service_name, None)
        .await?;

    Ok(HttpResponse::Ok()
        .insert_header(CacheControl(vec![CacheDirective::NoCache]))
        .json(&descr))
}
