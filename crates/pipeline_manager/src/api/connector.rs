/// API to create and manipulate connectors, which are used to get data in and
/// out of Feldera pipelines
use actix_web::{
    delete, get,
    http::header::{CacheControl, CacheDirective},
    patch, post, put,
    web::{self, Data as WebData, ReqData},
    HttpRequest, HttpResponse,
};
use log::info;
use pipeline_types::config::ConnectorConfig;
use serde::{Deserialize, Serialize};
use utoipa::{IntoParams, ToSchema};

use crate::{
    api::{examples, parse_string_param},
    auth::TenantId,
    db::{storage::Storage, ConnectorId, DBError},
};

use super::{ManagerError, ServerState};
use uuid::Uuid;

/// Request to create a new connector.
#[derive(Deserialize, ToSchema)]
pub(crate) struct NewConnectorRequest {
    /// Connector name.
    name: String,
    /// Connector description.
    description: String,
    /// Connector configuration.
    config: ConnectorConfig,
}

/// Response to a connector creation request.
#[derive(Serialize, ToSchema)]
pub(crate) struct NewConnectorResponse {
    /// Unique id assigned to the new connector.
    connector_id: ConnectorId,
}

#[derive(Debug, Deserialize, IntoParams)]
pub struct ConnectorIdOrNameQuery {
    /// Unique connector identifier.
    id: Option<Uuid>,
    /// Unique connector name.
    name: Option<String>,
}

/// Request to update an existing connector.
#[derive(Deserialize, ToSchema)]
pub(crate) struct UpdateConnectorRequest {
    /// New connector name. If absent, existing name will be kept unmodified.
    name: Option<String>,
    /// New connector description. If absent, existing name will be kept
    /// unmodified.
    description: Option<String>,
    /// New connector configuration. If absent, existing configuration will be
    /// kept unmodified.
    config: Option<ConnectorConfig>,
}

/// Response to a connector update request.
#[derive(Serialize, ToSchema)]
pub(crate) struct UpdateConnectorResponse {}

/// Request to create or replace a connector.
#[derive(Deserialize, ToSchema)]
pub(crate) struct CreateOrReplaceConnectorRequest {
    /// New connector description.
    description: String,
    /// New connector configuration.
    config: ConnectorConfig,
}

/// Response to a create or replace connector request.
#[derive(Serialize, ToSchema)]
pub(crate) struct CreateOrReplaceConnectorResponse {
    /// Unique id assigned to the connector.
    connector_id: ConnectorId,
}

/// Fetch connectors, optionally filtered by name or ID
#[utoipa::path(
    responses(
        (status = OK, description = "List of connectors retrieved successfully", body = [ConnectorDescr]),
        (status = NOT_FOUND
            , description = "Specified connector name or ID does not exist"
            , body = ErrorResponse
            , examples(
                ("Unknown connector name" = (value = json!(examples::unknown_name()))),
                ("Unknown connector ID" = (value = json!(examples::unknown_connector())))
            ),
        )
    ),
    params(ConnectorIdOrNameQuery),
    context_path = "/v0",
    security(("JSON web token (JWT) or API key" = [])),
    tag = "Connectors"
)]
#[get("/connectors")]
async fn list_connectors(
    state: WebData<ServerState>,
    tenant_id: ReqData<TenantId>,
    req: web::Query<ConnectorIdOrNameQuery>,
) -> Result<HttpResponse, DBError> {
    let descr = if let Some(id) = req.id {
        vec![
            state
                .db
                .lock()
                .await
                .get_connector_by_id(*tenant_id, ConnectorId(id))
                .await?,
        ]
    } else if let Some(name) = req.name.clone() {
        vec![
            state
                .db
                .lock()
                .await
                .get_connector_by_name(*tenant_id, &name, None)
                .await?,
        ]
    } else {
        state.db.lock().await.list_connectors(*tenant_id).await?
    };

    Ok(HttpResponse::Ok()
        .insert_header(CacheControl(vec![CacheDirective::NoCache]))
        .json(&descr))
}

/// Create a new connector.
#[utoipa::path(
    request_body = NewConnectorRequest,
    responses(
        (status = CREATED, description = "Connector successfully created", body = NewConnectorResponse),
        (status = CONFLICT
        , description = "A connector with this name already exists in the database"
        , body = ErrorResponse
        , example = json!(examples::duplicate_name())),
    ),
    context_path = "/v0",
    security(("JSON web token (JWT) or API key" = [])),
    tag = "Connectors"
)]
#[post("/connectors")]
async fn new_connector(
    state: WebData<ServerState>,
    tenant_id: ReqData<TenantId>,
    request: web::Json<NewConnectorRequest>,
) -> Result<HttpResponse, DBError> {
    let connector_id = state
        .db
        .lock()
        .await
        .new_connector(
            *tenant_id,
            Uuid::now_v7(),
            &request.name,
            &request.description,
            &request.config,
            None,
        )
        .await?;

    info!(
        "Created connector with name {} and id {} (tenant: {})",
        &request.name, connector_id, *tenant_id
    );
    Ok(HttpResponse::Created()
        .insert_header(CacheControl(vec![CacheDirective::NoCache]))
        .json(&NewConnectorResponse { connector_id }))
}

/// Update the name, description and/or configuration of a connector.
#[utoipa::path(
    request_body = UpdateConnectorRequest,
    responses(
        (status = OK, description = "Connector successfully updated", body = UpdateConnectorResponse),
        (status = NOT_FOUND
            , description = "Specified connector name does not exist"
            , body = ErrorResponse
            , example = json!(examples::unknown_name())),
    ),
    params(
        ("connector_name" = String, Path, description = "Unique connector name")
    ),
    context_path = "/v0",
    security(("JSON web token (JWT) or API key" = [])),
    tag = "Connectors"
)]
#[patch("/connectors/{connector_name}")]
async fn update_connector(
    state: WebData<ServerState>,
    tenant_id: ReqData<TenantId>,
    req: HttpRequest,
    body: web::Json<UpdateConnectorRequest>,
) -> Result<HttpResponse, ManagerError> {
    let connector_name = parse_string_param(&req, "connector_name")?;
    let db = state.db.lock().await;
    db.update_connector_by_name(
        *tenant_id,
        &connector_name,
        &body.name.as_deref(),
        &body.description.as_deref(),
        &body.config,
    )
    .await?;

    info!(
        "Updated connector {connector_name} (tenant: {})",
        *tenant_id
    );
    Ok(HttpResponse::Ok()
        .insert_header(CacheControl(vec![CacheDirective::NoCache]))
        .json(&UpdateConnectorResponse {}))
}

/// Create or replace a connector.
#[utoipa::path(
    request_body = CreateOrReplaceConnectorRequest,
    responses(
        (status = CREATED, description = "Connector created successfully", body = CreateOrReplaceConnectorResponse),
        (status = OK, description = "Connector updated successfully", body = CreateOrReplaceConnectorResponse),
        (status = CONFLICT
            , description = "A connector with this name already exists in the database"
            , body = ErrorResponse
            , example = json!(examples::duplicate_name())),
    ),
    params(
        ("connector_name" = String, Path, description = "Unique connector name")
    ),
    context_path = "/v0",
    security(("JSON web token (JWT) or API key" = [])),
    tag = "Connectors"
)]
#[put("/connectors/{connector_name}")]
async fn create_or_replace_connector(
    state: WebData<ServerState>,
    tenant_id: ReqData<TenantId>,
    request: HttpRequest,
    body: web::Json<CreateOrReplaceConnectorRequest>,
) -> Result<HttpResponse, ManagerError> {
    let connector_name = parse_string_param(&request, "connector_name")?;
    let (created, connector_id) = state
        .db
        .lock()
        .await
        .create_or_replace_connector(*tenant_id, &connector_name, &body.description, &body.config)
        .await?;
    if created {
        info!(
            "Created connector with name {} and id {} (tenant: {})",
            connector_name, connector_id, *tenant_id
        );
        Ok(HttpResponse::Created()
            .insert_header(CacheControl(vec![CacheDirective::NoCache]))
            .json(&CreateOrReplaceConnectorResponse { connector_id }))
    } else {
        info!(
            "Updated connector {connector_name} (tenant: {})",
            *tenant_id
        );
        Ok(HttpResponse::Ok()
            .insert_header(CacheControl(vec![CacheDirective::NoCache]))
            .json(&CreateOrReplaceConnectorResponse { connector_id }))
    }
}

/// Delete an existing connector.
#[utoipa::path(
    responses(
        (status = OK, description = "Connector successfully deleted"),
        (status = NOT_FOUND
            , description = "Specified connector name does not exist"
            , body = ErrorResponse
            , example = json!(examples::unknown_name())),
    ),
    params(
        ("connector_name" = String, Path, description = "Unique connector name")
    ),
    context_path = "/v0",
    security(("JSON web token (JWT) or API key" = [])),
    tag = "Connectors"
)]
#[delete("/connectors/{connector_name}")]
async fn delete_connector(
    state: WebData<ServerState>,
    tenant_id: ReqData<TenantId>,
    req: HttpRequest,
) -> Result<HttpResponse, ManagerError> {
    let connector_name = parse_string_param(&req, "connector_name")?;
    state
        .db
        .lock()
        .await
        .delete_connector(*tenant_id, &connector_name)
        .await?;

    info!(
        "Deleted connector {connector_name} (tenant: {})",
        *tenant_id
    );
    Ok(HttpResponse::Ok().finish())
}

/// Fetch a connector by name.
#[utoipa::path(
    responses(
        (status = OK, description = "Connector retrieved successfully", body = ConnectorDescr),
        (status = NOT_FOUND
        , description = "Specified connector name does not exist"
        , body = ErrorResponse
        , example = json!(examples::unknown_name()))
    ),
    params(
        ("connector_name" = String, Path, description = "Unique connector name")
    ),
    context_path = "/v0",
    security(("JSON web token (JWT) or API key" = [])),
    tag = "Connectors"
)]
#[get("/connectors/{connector_name}")]
async fn get_connector(
    state: WebData<ServerState>,
    tenant_id: ReqData<TenantId>,
    req: HttpRequest,
) -> Result<HttpResponse, ManagerError> {
    let connector_name = parse_string_param(&req, "connector_name")?;
    let descr = state
        .db
        .lock()
        .await
        .get_connector_by_name(*tenant_id, &connector_name, None)
        .await?;

    Ok(HttpResponse::Ok()
        .insert_header(CacheControl(vec![CacheDirective::NoCache]))
        .json(&descr))
}
