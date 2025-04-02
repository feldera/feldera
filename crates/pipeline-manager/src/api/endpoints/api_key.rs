// API to create and delete API keys
use crate::api::main::ServerState;
use crate::api::util::parse_url_parameter;
use crate::db::types::api_key::{ApiKeyId, ApiPermission};
use crate::db::types::tenant::TenantId;
use crate::error::ManagerError;
use crate::{api::examples, db::storage::Storage};
use actix_web::{
    delete, get,
    http::header::{CacheControl, CacheDirective},
    post,
    web::{self, Data as WebData, ReqData},
    HttpRequest, HttpResponse,
};
use log::info;
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;
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
    /// Identifier of the newly created API key.
    #[schema(example = "00000000-0000-0000-0000-000000000000")]
    id: ApiKeyId,

    /// API key name provided by the user.
    #[schema(example = "my-api-key")]
    name: String,

    /// Generated secret API key. There is no way to retrieve this
    /// key again through the API, so store it securely.
    #[schema(
        example = "apikey:v5y5QNtlPNVMwkmNjKwFU8bbIu5lMge3yHbyddxAOdXlEo84SEoNn32DUhQaf1KLeI9aOOfnJjhQ1pYzMrU4wQXON6pm6BS7Zgzj46U2b8pwz1280vYBEtx41hiDBRP"
    )]
    api_key: String,
}

/// Retrieve the list of API keys.
#[utoipa::path(
    context_path = "/v0",
    security(("JSON web token (JWT) or API key" = [])),
    responses(
        (status = OK
            , description = "API keys retrieved successfully"
            , body = [ApiKeyDescr]),
        (status = INTERNAL_SERVER_ERROR, body = ErrorResponse)
    ),
    tag = "API keys"
)]
#[get("/api_keys")]
pub(crate) async fn list_api_keys(
    state: WebData<ServerState>,
    tenant_id: ReqData<TenantId>,
) -> Result<HttpResponse, ManagerError> {
    let api_keys = state.db.lock().await.list_api_keys(*tenant_id).await?;
    Ok(HttpResponse::Ok()
        .insert_header(CacheControl(vec![CacheDirective::NoCache]))
        .json(&api_keys))
}

/// Retrieve an API key.
#[utoipa::path(
    context_path = "/v0",
    security(("JSON web token (JWT) or API key" = [])),
    params(
        ("api_key_name" = String, Path, description = "Unique API key name")
    ),
    responses(
        (status = OK,
            description = "API key retrieved successfully",
            body = ApiKeyDescr),
        (status = NOT_FOUND
            , description = "API key with that name does not exist"
            , body = ErrorResponse
            , example = json!(examples::error_unknown_api_key()))
    ),
    tag = "API keys"
)]
#[get("/api_keys/{api_key_name}")]
pub(crate) async fn get_api_key(
    state: WebData<ServerState>,
    tenant_id: ReqData<TenantId>,
    req: HttpRequest,
) -> Result<HttpResponse, ManagerError> {
    let name = parse_url_parameter(&req, "api_key_name")?;
    let api_key = state.db.lock().await.get_api_key(*tenant_id, &name).await?;
    Ok(HttpResponse::Ok()
        .insert_header(CacheControl(vec![CacheDirective::NoCache]))
        .json(&[api_key]))
}

/// Create a new API key.
#[utoipa::path(
    context_path = "/v0",
    security(("JSON web token (JWT) or API key" = [])),
    responses(
        (status = CREATED
            , description = "API key created successfully"
            , body = NewApiKeyResponse),
        (status = CONFLICT
            , description = "API key with that name already exists"
            , body = ErrorResponse
            , example = json!(examples::error_duplicate_name())),
    ),
    tag = "API keys"
)]
#[post("/api_keys")]
pub(crate) async fn post_api_key(
    state: WebData<ServerState>,
    tenant_id: ReqData<TenantId>,
    req: web::Json<NewApiKeyRequest>,
) -> Result<HttpResponse, ManagerError> {
    let new_id = Uuid::now_v7();
    let generated_api_key = crate::auth::generate_api_key();
    let res = state
        .db
        .lock()
        .await
        .store_api_key_hash(
            *tenant_id,
            new_id,
            &req.name,
            &generated_api_key,
            vec![ApiPermission::Read, ApiPermission::Write],
        )
        .await
        .map(|_| {
            info!("Created API key {} (tenant: {})", &req.name, *tenant_id);
            HttpResponse::Created()
                .insert_header(CacheControl(vec![CacheDirective::NoCache]))
                .json(&NewApiKeyResponse {
                    id: ApiKeyId(new_id),
                    name: req.name.clone(),
                    api_key: generated_api_key,
                })
        })?;
    Ok(res)
}

/// Delete an API key.
#[utoipa::path(
    context_path = "/v0",
    security(("JSON web token (JWT) or API key" = [])),
    params(
        ("api_key_name" = String, Path, description = "Unique API key name")
    ),
    responses(
        (status = OK, description = "API key deleted successfully"),
        (status = NOT_FOUND
            , description = "API key with that name does not exist"
            , body = ErrorResponse
            , example = json!(examples::error_unknown_api_key()))
    ),
    tag = "API keys"
)]
#[delete("/api_keys/{api_key_name}")]
pub(crate) async fn delete_api_key(
    state: WebData<ServerState>,
    tenant_id: ReqData<TenantId>,
    req: HttpRequest,
) -> Result<HttpResponse, ManagerError> {
    let name = parse_url_parameter(&req, "api_key_name")?;
    let resp = state
        .db
        .lock()
        .await
        .delete_api_key(*tenant_id, &name)
        .await
        .map(|_| HttpResponse::Ok().finish())?;
    info!("Deleted API key {name} (tenant: {})", *tenant_id);
    Ok(resp)
}
