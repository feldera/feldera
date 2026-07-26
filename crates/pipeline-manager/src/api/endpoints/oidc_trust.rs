// OIDC workload identity trust relationships.
//
// A trust relationship lets a tenant authorize JWT-bearing requests from an
// external OIDC issuer (e.g. GitHub Actions, AWS, GCP, Auth0) without
// provisioning a long-lived Feldera API key. The issuer is verified via OIDC
// discovery + JWKS; the `subject` and (optional) `audience` claims are matched
// against patterns recorded on the trust relationship (`*` is a wildcard).
use crate::api::main::ServerState;
use crate::api::util::parse_url_parameter;
use crate::auth::AuthenticatedPrincipal;
use crate::db::error::DBError;
use crate::db::storage::Storage;
use crate::db::types::oidc_trust::OidcTrustId;
use crate::db::types::role::Role;
use crate::db::types::tenant::TenantId;
use crate::error::ManagerError;
use actix_web::{
    delete, get,
    http::header::{CacheControl, CacheDirective},
    post,
    web::{self, Data as WebData, ReqData},
    HttpRequest, HttpResponse,
};
use serde::{Deserialize, Serialize};
use tracing::info;
use utoipa::{IntoParams, ToSchema};
use uuid::Uuid;

/// Selects which trusts an operation targets.
#[derive(Debug, Default, Deserialize, IntoParams)]
pub(crate) struct TrustScope {
    /// Select the platform-wide owner trusts, which belong to no tenant,
    /// instead of the trusts scoped to the caller's tenant. Owner-only.
    #[serde(default)]
    platform: bool,
}

// The scope a trust operation targets: `None` (platform-wide owner trusts, which
// only an owner may touch) when `platform` is set, else the caller's tenant.
fn trust_scope(
    scope: &TrustScope,
    tenant_id: TenantId,
    role: Role,
) -> Result<Option<TenantId>, ManagerError> {
    if scope.platform {
        if role != Role::Owner {
            return Err(DBError::InsufficientPermissions {
                required: Role::Owner,
                actual: role,
            }
            .into());
        }
        Ok(None)
    } else {
        Ok(Some(tenant_id))
    }
}

/// Request to create a new OIDC trust relationship.
#[derive(Debug, Deserialize, ToSchema)]
pub(crate) struct NewOidcTrustRequest {
    /// Trust relationship name. Unique within the tenant.
    #[schema(example = "github-actions-prod")]
    pub name: String,

    /// Optional human-readable description.
    #[schema(example = "GitHub Actions deploys from main branch")]
    #[serde(default)]
    pub description: Option<String>,

    /// Issuer URL exactly as it appears in the `iss` claim.
    /// JWKS are discovered at `<issuer>/.well-known/openid-configuration`.
    #[schema(example = "https://token.actions.githubusercontent.com")]
    pub issuer: String,

    /// Subject claim pattern. `*` matches any sequence of characters.
    #[schema(example = "repo:my-org/my-repo:ref:refs/heads/main")]
    pub subject: String,

    /// Optional audience claim pattern. `*` matches any sequence of characters.
    /// If omitted, the audience claim is not checked.
    #[schema(example = "https://github.com/my-org")]
    #[serde(default)]
    pub audience: Option<String>,

    /// Role granted to a token that satisfies this trust. Capped at the
    /// caller's own role. `owner` may be set only by an owner. Defaults to
    /// `read`.
    #[serde(default)]
    pub role: Option<Role>,
}

/// Response to a successful create.
#[derive(Debug, Serialize, ToSchema)]
pub(crate) struct NewOidcTrustResponse {
    #[schema(example = "00000000-0000-0000-0000-000000000000")]
    pub id: OidcTrustId,
    pub name: String,
}

/// List OIDC Trust
#[utoipa::path(
    context_path = "/v0",
    security(("JSON web token (JWT) or API key" = [])),
    params(TrustScope),
    responses(
        (status = OK, description = "Trust relationships retrieved", body = [OidcTrustDescr]),
        (status = FORBIDDEN, description = "Caller's role is below the required role, or `platform` was set by a non-owner", body = ErrorResponse),
        (status = INTERNAL_SERVER_ERROR, body = ErrorResponse)
    ),
    tag = "Platform"
)]
#[get("/oidc_trust")]
pub(crate) async fn list_oidc_trust(
    state: WebData<ServerState>,
    tenant_id: ReqData<TenantId>,
    principal: ReqData<AuthenticatedPrincipal>,
    scope: web::Query<TrustScope>,
) -> Result<HttpResponse, ManagerError> {
    let scope = trust_scope(&scope, *tenant_id, principal.role)?;
    let items = state.db.lock().await.list_oidc_trust(scope).await?;
    Ok(HttpResponse::Ok()
        .insert_header(CacheControl(vec![CacheDirective::NoCache]))
        .json(&items))
}

/// Get OIDC Trust
#[utoipa::path(
    context_path = "/v0",
    security(("JSON web token (JWT) or API key" = [])),
    params(("name" = String, Path, description = "Trust relationship name"), TrustScope),
    responses(
        (status = OK, description = "Trust relationship retrieved", body = OidcTrustDescr),
        (status = FORBIDDEN, description = "Caller's role is below the required role, or `platform` was set by a non-owner", body = ErrorResponse),
        (status = NOT_FOUND, description = "No relationship with that name", body = ErrorResponse),
        (status = INTERNAL_SERVER_ERROR, body = ErrorResponse)
    ),
    tag = "Platform"
)]
#[get("/oidc_trust/{name}")]
pub(crate) async fn get_oidc_trust(
    state: WebData<ServerState>,
    tenant_id: ReqData<TenantId>,
    principal: ReqData<AuthenticatedPrincipal>,
    scope: web::Query<TrustScope>,
    req: HttpRequest,
) -> Result<HttpResponse, ManagerError> {
    let name = parse_url_parameter(&req, "name")?;
    let scope = trust_scope(&scope, *tenant_id, principal.role)?;
    let item = state.db.lock().await.get_oidc_trust(scope, &name).await?;
    Ok(HttpResponse::Ok()
        .insert_header(CacheControl(vec![CacheDirective::NoCache]))
        .json(&item))
}

/// Create OIDC Trust
#[utoipa::path(
    context_path = "/v0",
    security(("JSON web token (JWT) or API key" = [])),
    request_body = NewOidcTrustRequest,
    responses(
        (status = CREATED, description = "Trust relationship created", body = NewOidcTrustResponse),
        (status = BAD_REQUEST, description = "A required field is empty", body = ErrorResponse),
        (status = FORBIDDEN, description = "Caller's role is below the required role, or the requested role exceeds the caller's own", body = ErrorResponse),
        (status = CONFLICT, description = "Name already in use", body = ErrorResponse),
        (status = INTERNAL_SERVER_ERROR, body = ErrorResponse)
    ),
    tag = "Platform"
)]
#[post("/oidc_trust")]
pub(crate) async fn post_oidc_trust(
    state: WebData<ServerState>,
    tenant_id: ReqData<TenantId>,
    principal: ReqData<AuthenticatedPrincipal>,
    body: web::Json<NewOidcTrustRequest>,
) -> Result<HttpResponse, ManagerError> {
    let new_id = Uuid::now_v7();
    let body = body.into_inner();

    // Mint cap: the granted role may not exceed the caller's role. An owner
    // trust may be created only by an owner.
    let requested = body.role.unwrap_or(Role::Read);
    if requested > principal.role {
        return Err(DBError::RoleExceedsCreator {
            requested,
            creator: principal.role,
        }
        .into());
    }

    // Scope follows the role: an owner trust is platform-wide (no tenant), any
    // other role is scoped to the acting tenant. Tenant selection at auth time
    // comes from the Feldera-Tenant header.
    let scope = if requested == Role::Owner {
        None
    } else {
        Some(*tenant_id)
    };

    state
        .db
        .lock()
        .await
        .create_oidc_trust(
            scope,
            new_id,
            &body.name,
            body.description.as_deref(),
            &body.issuer,
            &body.subject,
            body.audience.as_deref(),
            requested,
        )
        .await?;
    info!(
        "Created OIDC trust '{}' (scope: {:?}, issuer: {})",
        body.name, scope, body.issuer
    );
    Ok(HttpResponse::Created()
        .insert_header(CacheControl(vec![CacheDirective::NoCache]))
        .json(&NewOidcTrustResponse {
            id: OidcTrustId(new_id),
            name: body.name,
        }))
}

/// Delete OIDC Trust
#[utoipa::path(
    context_path = "/v0",
    security(("JSON web token (JWT) or API key" = [])),
    params(("name" = String, Path, description = "Trust relationship name"), TrustScope),
    responses(
        (status = OK, description = "Trust relationship deleted"),
        (status = FORBIDDEN, description = "Caller's role is below the required role, or `platform` was set by a non-owner", body = ErrorResponse),
        (status = NOT_FOUND, description = "No relationship with that name", body = ErrorResponse),
        (status = INTERNAL_SERVER_ERROR, body = ErrorResponse)
    ),
    tag = "Platform"
)]
#[delete("/oidc_trust/{name}")]
pub(crate) async fn delete_oidc_trust(
    state: WebData<ServerState>,
    tenant_id: ReqData<TenantId>,
    principal: ReqData<AuthenticatedPrincipal>,
    scope: web::Query<TrustScope>,
    req: HttpRequest,
) -> Result<HttpResponse, ManagerError> {
    let name = parse_url_parameter(&req, "name")?;
    let scope = trust_scope(&scope, *tenant_id, principal.role)?;
    state
        .db
        .lock()
        .await
        .delete_oidc_trust(scope, &name)
        .await?;
    info!("Deleted OIDC trust '{name}' (scope: {scope:?})");
    Ok(HttpResponse::Ok().finish())
}
