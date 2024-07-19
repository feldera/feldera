/// API to create and delete API keys
use super::{ManagerError, ServerState};
use crate::db::types::api_key::{ApiKeyId, ApiPermission};
use crate::db::types::tenant::TenantId;
use crate::{
    api::{examples, parse_string_param},
    db::storage::Storage,
};
use actix_web::{
    delete, get,
    http::header::{CacheControl, CacheDirective},
    post,
    web::{self, Data as WebData, ReqData},
    HttpRequest, HttpResponse,
};
use log::info;
use serde::{Deserialize, Serialize};
use utoipa::{IntoParams, ToSchema};
use uuid::Uuid;

/// Request to create a new API key.
#[derive(Debug, Deserialize, ToSchema)]
pub(crate) struct NewApiKeyRequest {
    /// Key name.
    #[schema(example = "my-api-key")]
    name: String,
}

/// Response to a successful API key creation.
#[derive(Debug, Serialize, ToSchema)]
pub(crate) struct NewApiKeyResponse {
    /// Id of the newly created API key.
    #[schema(example = 42)]
    api_key_id: ApiKeyId,

    /// API key name
    #[schema(example = "my-api-key")]
    name: String,

    /// Generated API key. There is no way to
    /// retrieve this key again from the
    /// pipeline-manager, so store it securely.
    #[schema(
        example = "apikey:v5y5QNtlPNVMwkmNjKwFU8bbIu5lMge3yHbyddxAOdXlEo84SEoNn32DUhQaf1KLeI9aOOfnJjhQ1pYzMrU4wQXON6pm6BS7Zgzj46U2b8pwz1280vYBEtx41hiDBRP"
    )]
    api_key: String,
}

/// Query for an APIkey name
#[derive(Debug, Deserialize, IntoParams, ToSchema)]
pub(crate) struct ApiKeyNameQuery {
    /// API key name
    #[schema(example = "my-api-key")]
    name: Option<String>,
}

/// List all API keys
#[utoipa::path(
    responses(
        (status = OK, description = "API keys retrieved successfully", body = [ApiKeyDescr]),
        (status = NOT_FOUND
            , description = "Specified API key name does not exist."
            , body = ErrorResponse
            , examples(
                ("Unknown API key name" = (value = json!(examples::unknown_api_key()))),
            ),
        )
    ),
    params(ApiKeyNameQuery),
    context_path = "/v0",
    security(("JSON web token (JWT) or API key" = [])),
    tag = "API keys"
)]
#[get("/api_keys")]
pub(crate) async fn list_api_keys(
    state: WebData<ServerState>,
    tenant_id: ReqData<TenantId>,
    req: web::Query<ApiKeyNameQuery>,
) -> Result<HttpResponse, ManagerError> {
    if let Some(ref name) = req.name {
        let api_key = state.db.lock().await.get_api_key(*tenant_id, name).await?;
        Ok(HttpResponse::Ok()
            .insert_header(CacheControl(vec![CacheDirective::NoCache]))
            .json(&vec![api_key]))
    } else {
        let api_keys = state.db.lock().await.list_api_keys(*tenant_id).await?;
        Ok(HttpResponse::Ok()
            .insert_header(CacheControl(vec![CacheDirective::NoCache]))
            .json(&api_keys))
    }
}

/// Get an API key description
#[utoipa::path(
    responses(
        (status = OK, description = "API key retrieved successfully", body = ApiKeyDescr),
        (status = NOT_FOUND
            , description = "Specified API key name does not exist."
            , body = ErrorResponse
            , examples(
                ("Unknown API key name" = (value = json!(examples::unknown_api_key()))),
            ),
        )
    ),
    params(
        ("api_key_name" = String, Path, description = "Unique API key name")
    ),
    context_path = "/v0",
    security(("JSON web token (JWT) or API key" = [])),
    tag = "API keys"
)]
#[get("/api_keys/{api_key_name}")]
pub(crate) async fn get_api_key(
    state: WebData<ServerState>,
    tenant_id: ReqData<TenantId>,
    req: HttpRequest,
) -> Result<HttpResponse, ManagerError> {
    let name = parse_string_param(&req, "api_key_name")?;
    let api_key = state.db.lock().await.get_api_key(*tenant_id, &name).await?;
    Ok(HttpResponse::Ok()
        .insert_header(CacheControl(vec![CacheDirective::NoCache]))
        .json(&[api_key]))
}

/// Delete an API key
#[utoipa::path(
    responses(
        (status = OK, description = "API key deleted successfully"),
        (status = NOT_FOUND
            , description = "Specified API key name does not exist."
            , body = ErrorResponse
            , examples(
                ("Unknown API key name" = (value = json!(examples::unknown_api_key()))),
            ),
        )
    ),
    params(
        ("api_key_name" = String, Path, description = "Unique API key name")
    ),
    context_path = "/v0",
    security(("JSON web token (JWT) or API key" = [])),
    tag = "API keys"
)]
#[delete("/api_keys/{api_key_name}")]
pub(crate) async fn delete_api_key(
    state: WebData<ServerState>,
    tenant_id: ReqData<TenantId>,
    req: HttpRequest,
) -> Result<HttpResponse, ManagerError> {
    let name = parse_string_param(&req, "api_key_name")?;
    let resp = state
        .db
        .lock()
        .await
        .delete_api_key(*tenant_id, &name)
        .await
        .map(|_| HttpResponse::Ok().finish())?;
    info!("Deleted API key {name} (tenant:{})", *tenant_id);
    Ok(resp)
}

/// Create an API key
#[utoipa::path(
    responses(
        (status = OK, description = "API key created successfully.", body = NewApiKeyResponse),
        (status = CONFLICT
            , description = "An api key with this name already exists."
            , body = ErrorResponse
            , example = json!(examples::duplicate_name())),
    ),
    context_path = "/v0",
    security(("JSON web token (JWT) or API key" = [])),
    tag = "API keys"
)]
#[post("/api_keys")]
async fn create_api_key(
    state: WebData<ServerState>,
    tenant_id: ReqData<TenantId>,
    req: web::Json<NewApiKeyRequest>,
) -> Result<HttpResponse, ManagerError> {
    let api_key = crate::auth::generate_api_key();
    let id = Uuid::now_v7();
    let res = state
        .db
        .lock()
        .await
        .store_api_key_hash(
            *tenant_id,
            id,
            &req.name,
            &api_key,
            vec![ApiPermission::Read, ApiPermission::Write],
        )
        .await
        .map(|_| {
            info!("Created new API key {} (tenant:{})", &req.name, *tenant_id);
            HttpResponse::Created()
                .insert_header(CacheControl(vec![CacheDirective::NoCache]))
                .json(&NewApiKeyResponse {
                    api_key_id: ApiKeyId(id),
                    name: req.name.clone(),
                    api_key,
                })
        })?;
    Ok(res)
}
