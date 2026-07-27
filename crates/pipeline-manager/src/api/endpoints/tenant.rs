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
use crate::db::types::user::{TenantInfo, UserId};
use crate::error::ManagerError;
use actix_web::{
    delete, get,
    http::header::{CacheControl, CacheDirective},
    patch, post, put,
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
    /// The role to assign: `read`, `write`, or `admin`. `owner` is platform-wide
    /// rather than a tenant membership, so it is configured at deploy time
    /// (Helm `authorization.owners` / `FELDERA_OWNERS`) or granted by an owner
    /// OIDC trust relationship, never assigned through this endpoint.
    #[schema(value_type = MemberRole)]
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
    /// Role to grant: `read`, `write`, or `admin`. `owner` is platform-wide
    /// rather than a tenant membership, so it is configured at deploy time
    /// (Helm `authorization.owners` / `FELDERA_OWNERS`) or granted by an owner
    /// OIDC trust relationship, never assigned through this endpoint.
    #[schema(value_type = MemberRole)]
    pub role: Role,
}

/// Response to a successful member pre-provisioning.
#[derive(Debug, Serialize, ToSchema)]
pub(crate) struct AddMemberResponse {
    pub user_id: UserId,
}

/// Reject a requested role the caller may not grant: `owner` is platform-wide
/// and never a tenant membership. The second check, that no one grants above
/// their own role, cannot fire while these routes require `admin` (the highest
/// grantable role); it is kept so lowering the route's minimum role stays safe.
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

/// Request to rename a tenant.
#[derive(Debug, Deserialize, ToSchema)]
pub(crate) struct RenameTenantRequest {
    /// The tenant's new name.
    #[schema(example = "acme")]
    pub name: String,
    /// Take the name from the tenant that currently holds it, instead of
    /// failing with a conflict. That tenant is renamed to `<name> (<id>)` and
    /// keeps everything it had; nothing is merged or deleted.
    #[serde(default)]
    pub displace_existing: bool,
}

/// Response to a successful tenant rename.
#[derive(Debug, Serialize, ToSchema)]
pub(crate) struct RenameTenantResponse {
    /// The tenant that gave up the name, when `displace_existing` was set and
    /// another tenant held it. `null` when the name was free.
    pub displaced: Option<TenantInfo>,
}

fn parse_tenant_id(req: &HttpRequest) -> Result<TenantId, ManagerError> {
    let raw = parse_url_parameter(req, "tenant_id")?;
    let uuid = Uuid::parse_str(&raw)
        .map_err(|_| ManagerError::from(DBError::UnknownTenantName { name: raw.clone() }))?;
    Ok(TenantId(uuid))
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

/// List Tenant Members
///
/// List the users that are members of the acting tenant and their roles.
#[utoipa::path(
    context_path = "/v0",
    security(("JSON web token (JWT) or API key" = [])),
    responses(
        (status = OK, description = "Members retrieved", body = [TenantMember]),
        (status = FORBIDDEN, description = "Caller's role is below the required role", body = ErrorResponse),
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

/// Assign Member Role
///
/// Assign or change a user's role in the acting tenant. The role is capped at
/// the caller's own role and may not be `owner`.
#[utoipa::path(
    context_path = "/v0",
    security(("JSON web token (JWT) or API key" = [])),
    params(("user_id" = Uuid, Path, description = "User identifier")),
    request_body = SetMemberRoleRequest,
    responses(
        (status = OK, description = "Role assigned"),
        (status = FORBIDDEN, description = "Caller's role is below the required role, or the requested role is `owner`", body = ErrorResponse),
        (status = NOT_FOUND, description = "No user with that identifier", body = ErrorResponse),
        (status = INTERNAL_SERVER_ERROR, body = ErrorResponse)
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

/// Remove Tenant Member
///
/// Remove a user from the acting tenant. This drops their role now, but if the
/// identity provider still grants them access they are re-added at the default
/// role on their next login; revoke access at the provider for a durable block.
#[utoipa::path(
    context_path = "/v0",
    security(("JSON web token (JWT) or API key" = [])),
    params(("user_id" = Uuid, Path, description = "User identifier")),
    responses(
        (status = OK, description = "Member removed"),
        (status = FORBIDDEN, description = "Caller's role is below the required role", body = ErrorResponse),
        (status = NOT_FOUND, description = "User is not a member", body = ErrorResponse),
        (status = INTERNAL_SERVER_ERROR, body = ErrorResponse)
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

/// Provision Tenant Member
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
        (status = FORBIDDEN, description = "Caller's role is below the required role, or the requested role is `owner`", body = ErrorResponse),
        (status = INTERNAL_SERVER_ERROR, body = ErrorResponse)
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

/// Rename Tenant
///
/// Change a tenant's name. Only the name changes: pipelines, API keys, members
/// and OIDC trust relationships all reference the tenant by its identifier and
/// are unaffected.
///
/// A login resolves its tenant by name, so renaming decides which tenant those
/// users reach. Two consequences follow. Renaming a tenant away from a name the
/// identity provider still asserts sends its users to a new, empty tenant on
/// their next request, which re-creates the name. And a tenant that no login
/// reaches, such as `default` after authentication is switched on, is recovered
/// by giving it the name logins do resolve.
///
/// Recovery normally wants a name that is already taken, by the tenant the
/// first login created. Set `displace_existing` to take it: that tenant is
/// renamed to `<name> (<id>)` in the same transaction and keeps everything it
/// had. One step is what makes recovery possible at all, since freeing the name
/// and claiming it as two calls loses to the next request re-creating it.
#[utoipa::path(
    context_path = "/v0",
    security(("JSON web token (JWT) or API key" = [])),
    params(("tenant_id" = Uuid, Path, description = "Tenant identifier")),
    request_body = RenameTenantRequest,
    responses(
        (status = OK, description = "Tenant renamed", body = RenameTenantResponse),
        (status = FORBIDDEN, description = "Caller is not a platform owner", body = ErrorResponse),
        (status = NOT_FOUND, description = "No tenant with that identifier", body = ErrorResponse),
        (status = CONFLICT, description = "A tenant with that name already exists, and `displace_existing` was not set", body = ErrorResponse),
        (status = INTERNAL_SERVER_ERROR, body = ErrorResponse)
    ),
    tag = "Platform"
)]
#[patch("/tenants/{tenant_id}")]
pub(crate) async fn patch_tenant(
    state: WebData<ServerState>,
    req: HttpRequest,
    body: web::Json<RenameTenantRequest>,
) -> Result<HttpResponse, ManagerError> {
    let tenant_id = parse_tenant_id(&req)?;
    let body = body.into_inner();
    let displaced = state
        .db
        .lock()
        .await
        .rename_tenant(tenant_id, &body.name, body.displace_existing)
        .await?;
    match &displaced {
        Some(t) => info!(
            "Renamed tenant {tenant_id} to '{}', displacing tenant {} to '{}'",
            body.name, t.id, t.name
        ),
        None => info!("Renamed tenant {tenant_id} to '{}'", body.name),
    }
    Ok(HttpResponse::Ok()
        .insert_header(CacheControl(vec![CacheDirective::NoCache]))
        .json(&RenameTenantResponse { displaced }))
}

/// Delete Tenant
///
/// Delete a tenant that holds nothing. Its members lose the membership, and a
/// login that still resolves this tenant's name simply re-creates it, empty.
///
/// The tenant must hold no pipelines, API keys or OIDC trust relationships;
/// otherwise the request fails with a conflict. Everything tenant-scoped
/// cascades on this delete, so the emptiness rule is what keeps a mistyped
/// identifier from taking a live tenant's pipelines with it. Delete those
/// resources first if you mean to.
#[utoipa::path(
    context_path = "/v0",
    security(("JSON web token (JWT) or API key" = [])),
    params(("tenant_id" = Uuid, Path, description = "Tenant identifier")),
    responses(
        (status = OK, description = "Tenant deleted"),
        (status = FORBIDDEN, description = "Caller is not a platform owner", body = ErrorResponse),
        (status = NOT_FOUND, description = "No tenant with that identifier", body = ErrorResponse),
        (status = CONFLICT, description = "The tenant still holds pipelines, API keys or OIDC trust relationships", body = ErrorResponse),
        (status = INTERNAL_SERVER_ERROR, body = ErrorResponse)
    ),
    tag = "Platform"
)]
#[delete("/tenants/{tenant_id}")]
pub(crate) async fn delete_tenant(
    state: WebData<ServerState>,
    req: HttpRequest,
) -> Result<HttpResponse, ManagerError> {
    let tenant_id = parse_tenant_id(&req)?;
    state.db.lock().await.delete_tenant(tenant_id).await?;
    info!("Deleted tenant {tenant_id}");
    Ok(HttpResponse::Ok().finish())
}

/// List Tenants
///
/// List all tenants in the installation.
#[utoipa::path(
    context_path = "/v0",
    security(("JSON web token (JWT) or API key" = [])),
    responses(
        (status = OK, description = "Tenants retrieved", body = [TenantInfo]),
        (status = FORBIDDEN, description = "Caller is not a platform owner", body = ErrorResponse),
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

/// Create Tenant
///
/// Explicitly create a tenant, rather than relying on first login.
/// A login resolves its tenant by name, so a user whose identity provider
/// asserts this name lands in the tenant created here. Fails with a conflict if
/// the name is already taken.
#[utoipa::path(
    context_path = "/v0",
    security(("JSON web token (JWT) or API key" = [])),
    request_body = NewTenantRequest,
    responses(
        (status = CREATED, description = "Tenant created", body = NewTenantResponse),
        (status = FORBIDDEN, description = "Caller is not a platform owner", body = ErrorResponse),
        (status = CONFLICT, description = "A tenant with that name already exists", body = ErrorResponse),
        (status = INTERNAL_SERVER_ERROR, body = ErrorResponse)
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
