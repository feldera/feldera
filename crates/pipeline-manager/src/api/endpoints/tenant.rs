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

/// Request to pre-provision a tenant member by identity, before the user's
/// first login.
#[derive(Debug, Deserialize, ToSchema)]
pub(crate) struct AddMemberRequest {
    /// OIDC subject (matches the JWT `sub` claim). The issuer is not settable:
    /// members authenticate through the platform's single configured issuer, so
    /// the grant is keyed to that issuer automatically.
    #[schema(example = "user@acme.com")]
    pub subject: String,
    /// Optional email for display in the member list.
    #[serde(default)]
    pub email: Option<String>,
    /// Role to grant. Must be `read`, `write`, or `admin`; capped at the
    /// caller's own role. `owner` is never assignable here.
    pub role: Role,
}

/// Response to a successful member pre-provisioning.
#[derive(Debug, Serialize, ToSchema)]
pub(crate) struct AddMemberResponse {
    pub user_id: UserId,
}

/// Reject a requested role an admin may not grant: `owner` is platform-wide and
/// never a tenant membership, and no one may grant above their own role.
fn check_grantable_role(requested: Role, caller: Role) -> Result<(), ManagerError> {
    if requested == Role::Owner {
        return Err(DBError::OwnerRoleNotAssignable.into());
    }
    if requested > caller {
        return Err(DBError::RoleExceedsCreator {
            requested,
            creator: caller,
        }
        .into());
    }
    Ok(())
}

/// Request to create a tenant (owner-only).
#[derive(Debug, Deserialize, ToSchema)]
pub(crate) struct NewTenantRequest {
    #[schema(example = "acme")]
    pub name: String,
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
    check_grantable_role(requested, principal.role)?;

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

/// Pre-provision a tenant member
///
/// Add a member to the acting tenant by identity, before the user's first
/// login. The grant is dormant until that identity authenticates into the
/// tenant through the IdP. The role is capped at the caller's own role and may
/// not be `owner`.
#[utoipa::path(
    context_path = "/v0",
    security(("JSON web token (JWT) or API key" = [])),
    request_body = AddMemberRequest,
    responses(
        (status = OK, description = "Member added", body = AddMemberResponse),
        (status = FORBIDDEN, description = "Requested role exceeds caller's role or is owner", body = ErrorResponse),
    ),
    tag = "Platform"
)]
#[post("/tenant/users")]
pub(crate) async fn add_tenant_user(
    state: WebData<ServerState>,
    tenant_id: ReqData<TenantId>,
    principal: ReqData<AuthenticatedPrincipal>,
    body: web::Json<AddMemberRequest>,
    req: HttpRequest,
) -> Result<HttpResponse, ManagerError> {
    let body = body.into_inner();
    check_grantable_role(body.role, principal.role)?;

    // The provider is the platform's configured issuer, not caller-set: a human
    // login's `iss` is always that issuer, so the grant must be keyed to it to
    // attach (mirrors tenant creation).
    let provider = req
        .app_data::<crate::auth::AuthConfiguration>()
        .map(|c| c.provider.issuer().to_string())
        .unwrap_or_else(|| "manual".to_string());
    let user_id = state
        .db
        .lock()
        .await
        .preprovision_member(
            Uuid::now_v7(),
            *tenant_id,
            &provider,
            &body.subject,
            body.email.as_deref(),
            body.role,
        )
        .await?;
    info!(
        "Pre-provisioned user {} ({}) with role {} in tenant {}",
        body.subject, user_id, body.role, *tenant_id
    );
    Ok(HttpResponse::Ok()
        .insert_header(CacheControl(vec![CacheDirective::NoCache]))
        .json(&AddMemberResponse { user_id }))
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
/// The tenant is keyed to the platform's configured OIDC issuer (statically set
/// at deploy time, e.g. via Helm), so that logins from that issuer resolve into
/// it; the issuer is not caller-settable. Fails with a conflict if a tenant with
/// the same name already exists for that issuer.
#[utoipa::path(
    context_path = "/v0",
    security(("JSON web token (JWT) or API key" = [])),
    request_body = NewTenantRequest,
    responses(
        (status = CREATED, description = "Tenant created", body = NewTenantResponse),
        (status = CONFLICT, description = "A tenant with that name already exists", body = ErrorResponse),
    ),
    tag = "Platform"
)]
#[post("/tenants")]
pub(crate) async fn create_tenant(
    state: WebData<ServerState>,
    body: web::Json<NewTenantRequest>,
    req: HttpRequest,
) -> Result<HttpResponse, ManagerError> {
    let body = body.into_inner();
    // The provider keys the tenant to the platform's configured issuer, so a
    // login from that issuer resolves into it. It mirrors the token `iss` used
    // at login (see `OidcClaim::provider`) and is deliberately not caller-set;
    // `manual` only when auth is disabled (owner routes are then unreachable).
    let provider = req
        .app_data::<crate::auth::AuthConfiguration>()
        .map(|c| c.provider.issuer().to_string())
        .unwrap_or_else(|| "manual".to_string());
    let id = state
        .db
        .lock()
        .await
        .create_tenant(Uuid::now_v7(), &body.name, &provider)
        .await?;
    info!(
        "Created tenant '{}' ({id}, provider: {provider})",
        body.name
    );
    Ok(HttpResponse::Created()
        .insert_header(CacheControl(vec![CacheDirective::NoCache]))
        .json(&NewTenantResponse {
            id,
            name: body.name,
        }))
}
