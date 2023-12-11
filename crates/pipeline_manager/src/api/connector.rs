/// API to create and manipulate connectors, which are used to get data in and
/// out of Feldera pipelines
use actix_web::{
    delete, get,
    http::header::{CacheControl, CacheDirective},
    patch, post,
    web::{self, Data as WebData, ReqData},
    HttpRequest, HttpResponse,
};
use log::info;
use pipeline_types::config::ConnectorConfig;
use serde::{Deserialize, Serialize};
use utoipa::{IntoParams, ToSchema};

use crate::{
    api::{examples, parse_uuid_param},
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
                .get_connector_by_name(*tenant_id, name)
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
        (status = OK, description = "Connector successfully created.", body = NewConnectorResponse),
    ),
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
        )
        .await?;

    info!("Created connector {connector_id} (tenant:{})", *tenant_id);
    Ok(HttpResponse::Ok()
        .insert_header(CacheControl(vec![CacheDirective::NoCache]))
        .json(&NewConnectorResponse { connector_id }))
}

/// Request to update an existing data-connector.
#[derive(Deserialize, ToSchema)]
pub(crate) struct UpdateConnectorRequest {
    /// New connector name.
    name: String,
    /// New connector description.
    description: String,
    /// New config YAML. If absent, existing YAML will be kept unmodified.
    config: Option<ConnectorConfig>,
}

/// Response to a config update request.
#[derive(Serialize, ToSchema)]
pub(crate) struct UpdateConnectorResponse {}

/// Change a connector's name, description or configuration.
#[utoipa::path(
    request_body = UpdateConnectorRequest,
    responses(
        (status = OK, description = "connector successfully updated.", body = UpdateConnectorResponse),
        (status = NOT_FOUND
            , description = "Specified connector id does not exist."
            , body = ErrorResponse
            , example = json!(examples::unknown_connector())),
    ),
    params(
        ("connector_id" = Uuid, Path, description = "Unique connector identifier")
    ),
    security(("JSON web token (JWT) or API key" = [])),
    tag = "Connectors"
)]
#[patch("/connectors/{connector_id}")]
async fn update_connector(
    state: WebData<ServerState>,
    tenant_id: ReqData<TenantId>,
    req: HttpRequest,
    body: web::Json<UpdateConnectorRequest>,
) -> Result<HttpResponse, ManagerError> {
    let connector_id = ConnectorId(parse_uuid_param(&req, "connector_id")?);
    state
        .db
        .lock()
        .await
        .update_connector(
            *tenant_id,
            connector_id,
            &body.name,
            &body.description,
            &body.config,
        )
        .await?;

    info!("Updated connector {connector_id} (tenant:{})", *tenant_id);
    Ok(HttpResponse::Ok()
        .insert_header(CacheControl(vec![CacheDirective::NoCache]))
        .json(&UpdateConnectorResponse {}))
}

/// Delete an existing connector.
#[utoipa::path(
    responses(
        (status = OK, description = "connector successfully deleted."),
        (status = BAD_REQUEST
            , description = "Specified connector id is not a valid uuid."
            , body = ErrorResponse
            , example = json!(examples::invalid_uuid_param())),
        (status = NOT_FOUND
            , description = "Specified connector id does not exist."
            , body = ErrorResponse
            , example = json!(examples::unknown_connector())),
    ),
    params(
        ("connector_id" = Uuid, Path, description = "Unique connector identifier")
    ),
    security(("JSON web token (JWT) or API key" = [])),
    tag = "Connectors"
)]
#[delete("/connectors/{connector_id}")]
async fn delete_connector(
    state: WebData<ServerState>,
    tenant_id: ReqData<TenantId>,
    req: HttpRequest,
) -> Result<HttpResponse, ManagerError> {
    let connector_id = ConnectorId(parse_uuid_param(&req, "connector_id")?);

    state
        .db
        .lock()
        .await
        .delete_connector(*tenant_id, connector_id)
        .await?;

    info!("Deleted connector {connector_id} (tenant:{})", *tenant_id);
    Ok(HttpResponse::Ok().finish())
}

/// Fetch a connector by ID.
#[utoipa::path(
    responses(
        (status = OK, description = "Connector retrieved successfully.", body = ConnectorDescr),
        (status = BAD_REQUEST
            , description = "Specified connector id is not a valid uuid."
            , body = ErrorResponse
            , example = json!(examples::invalid_uuid_param())),
    ),
    params(
        ("connector_id" = Uuid, Path, description = "Unique connector identifier"),
    ),
    security(("JSON web token (JWT) or API key" = [])),
    tag = "Connectors"
)]
#[get("/connectors/{connector_id}")]
async fn get_connector(
    state: WebData<ServerState>,
    tenant_id: ReqData<TenantId>,
    req: HttpRequest,
) -> Result<HttpResponse, ManagerError> {
    let connector_id = ConnectorId(parse_uuid_param(&req, "connector_id")?);
    let descr = state
        .db
        .lock()
        .await
        .get_connector_by_id(*tenant_id, connector_id)
        .await?;
    Ok(HttpResponse::Ok()
        .insert_header(CacheControl(vec![CacheDirective::NoCache]))
        .json(&descr))
}
