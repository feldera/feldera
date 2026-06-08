//! Tenant and user management endpoints.
//!
//! `admin` manages the members and roles of the acting tenant; `owner` manages
//! tenants across the installation. An owner acts in a specific tenant by
//! setting the `Feldera-Tenant` header, so the per-tenant user endpoints serve
//! both an admin in its own tenant and an owner in any tenant.

use crate::api::main::ServerState;
use crate::api::util::parse_url_parameter;
use crate::auth::AuthenticatedPrincipal;
use crate::db::error::DBError;
use crate::db::storage::Storage;
use crate::db::types::role::Role;
use crate::db::types::tenant::TenantId;
use crate::db::types::user::UserId;
use crate::error::ManagerError;
use actix_web::{
    delete, get,
    http::header::{CacheControl, CacheDirective},
    post, put,
    web::{self, Data as WebData, ReqData},
    HttpRequest, HttpResponse,
};
use serde::{Deserialize, Serialize};
use tracing::info;
use utoipa::ToSchema;
use uuid::Uuid;

/// Request to assign a role to a user within a tenant.
#[derive(Debug, Deserialize, ToSchema)]
pub(crate) struct SetMemberRoleRequest {
    /// The role to assign. Must be `read`, `write`, or `admin`; capped at the
    /// caller's own role. `owner` is never assignable here.
    pub role: Role,
}

/// Request to create a tenant (owner-only).
#[derive(Debug, Deserialize, ToSchema)]
pub(crate) struct NewTenantRequest {
    #[schema(example = "acme")]
    pub name: String,
    /// Identity provider the tenant is keyed under. Defaults to `manual` for
    /// tenants created out of band by an owner.
    #[serde(default)]
    pub provider: Option<String>,
}

fn parse_user_id(req: &HttpRequest) -> Result<UserId, ManagerError> {
    let raw = parse_url_parameter(req, "user_id")?;
    let uuid = Uuid::parse_str(&raw).map_err(|_| {
        ManagerError::from(DBError::UnknownUser {
            user_id: raw.clone(),
        })
    })?;
    Ok(UserId(uuid))
}

/// List tenant members
///
/// List the users that are members of the acting tenant and their roles.
#[utoipa::path(
    context_path = "/v0",
    security(("JSON web token (JWT) or API key" = [])),
    responses(
        (status = OK, description = "Members retrieved", body = [TenantMember]),
        (status = INTERNAL_SERVER_ERROR, body = ErrorResponse)
    ),
    tag = "Platform"
)]
#[get("/tenant/users")]
pub(crate) async fn list_tenant_users(
    state: WebData<ServerState>,
    tenant_id: ReqData<TenantId>,
) -> Result<HttpResponse, ManagerError> {
    let members = state
        .db
        .lock()
        .await
        .list_tenant_members(*tenant_id)
        .await?;
    Ok(HttpResponse::Ok()
        .insert_header(CacheControl(vec![CacheDirective::NoCache]))
        .json(&members))
}

/// Assign a member role
///
/// Assign or change a user's role in the acting tenant. The role is capped at
/// the caller's own role and may not be `owner`.
#[utoipa::path(
    context_path = "/v0",
    security(("JSON web token (JWT) or API key" = [])),
    params(("user_id" = String, Path, description = "User identifier")),
    request_body = SetMemberRoleRequest,
    responses(
        (status = OK, description = "Role assigned"),
        (status = FORBIDDEN, description = "Requested role exceeds caller's role or is owner", body = ErrorResponse),
    ),
    tag = "Platform"
)]
#[put("/tenant/users/{user_id}")]
pub(crate) async fn put_tenant_user(
    state: WebData<ServerState>,
    tenant_id: ReqData<TenantId>,
    principal: ReqData<AuthenticatedPrincipal>,
    req: HttpRequest,
    body: web::Json<SetMemberRoleRequest>,
) -> Result<HttpResponse, ManagerError> {
    let user_id = parse_user_id(&req)?;
    let requested = body.role;

    // `owner` is platform-wide and never stored as a tenant membership.
    if requested == Role::Owner {
        return Err(DBError::OwnerRoleNotAssignable.into());
    }
    // An admin cannot grant a role above its own.
    if requested > principal.role {
        return Err(DBError::RoleExceedsCreator {
            requested,
            creator: principal.role,
        }
        .into());
    }

    state
        .db
        .lock()
        .await
        .upsert_member_role(*tenant_id, user_id, requested)
        .await?;
    info!(
        "Set role {requested} for user {user_id} (tenant: {})",
        *tenant_id
    );
    Ok(HttpResponse::Ok().finish())
}

/// Remove a tenant member
///
/// Remove a user from the acting tenant.
#[utoipa::path(
    context_path = "/v0",
    security(("JSON web token (JWT) or API key" = [])),
    params(("user_id" = String, Path, description = "User identifier")),
    responses(
        (status = OK, description = "Member removed"),
        (status = NOT_FOUND, description = "User is not a member", body = ErrorResponse),
    ),
    tag = "Platform"
)]
#[delete("/tenant/users/{user_id}")]
pub(crate) async fn delete_tenant_user(
    state: WebData<ServerState>,
    tenant_id: ReqData<TenantId>,
    req: HttpRequest,
) -> Result<HttpResponse, ManagerError> {
    let user_id = parse_user_id(&req)?;
    state
        .db
        .lock()
        .await
        .remove_member(*tenant_id, user_id)
        .await?;
    info!("Removed user {user_id} from tenant {}", *tenant_id);
    Ok(HttpResponse::Ok().finish())
}

/// Response to a successful tenant creation.
#[derive(Debug, Serialize, ToSchema)]
pub(crate) struct NewTenantResponse {
    pub id: TenantId,
    pub name: String,
}

/// List tenants
///
/// List all tenants in the installation. Owner-only platform view.
#[utoipa::path(
    context_path = "/v0",
    security(("JSON web token (JWT) or API key" = [])),
    responses(
        (status = OK, description = "Tenants retrieved", body = [TenantInfo]),
        (status = INTERNAL_SERVER_ERROR, body = ErrorResponse)
    ),
    tag = "Platform"
)]
#[get("/tenants")]
pub(crate) async fn list_tenants(
    state: WebData<ServerState>,
) -> Result<HttpResponse, ManagerError> {
    let tenants = state.db.lock().await.list_tenants().await?;
    Ok(HttpResponse::Ok()
        .insert_header(CacheControl(vec![CacheDirective::NoCache]))
        .json(&tenants))
}

/// Create a tenant
///
/// Explicitly create a tenant (owner-only), rather than relying on first login.
#[utoipa::path(
    context_path = "/v0",
    security(("JSON web token (JWT) or API key" = [])),
    request_body = NewTenantRequest,
    responses(
        (status = CREATED, description = "Tenant created", body = NewTenantResponse),
    ),
    tag = "Platform"
)]
#[post("/tenants")]
pub(crate) async fn create_tenant(
    state: WebData<ServerState>,
    body: web::Json<NewTenantRequest>,
) -> Result<HttpResponse, ManagerError> {
    let body = body.into_inner();
    let provider = body.provider.unwrap_or_else(|| "manual".to_string());
    let id = state
        .db
        .lock()
        .await
        .get_or_create_tenant_id(Uuid::now_v7(), body.name.clone(), provider)
        .await?;
    info!("Created tenant '{}' ({id})", body.name);
    Ok(HttpResponse::Created()
        .insert_header(CacheControl(vec![CacheDirective::NoCache]))
        .json(&NewTenantResponse {
            id,
            name: body.name,
        }))
}
