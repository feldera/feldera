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
use utoipa::ToSchema;
use uuid::Uuid;

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
    #[schema(example = "feldera")]
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

/// List OIDC trust relationships
#[utoipa::path(
    context_path = "/v0",
    security(("JSON web token (JWT) or API key" = [])),
    responses(
        (status = OK, description = "Trust relationships retrieved", body = [OidcTrustDescr]),
        (status = INTERNAL_SERVER_ERROR, body = ErrorResponse)
    ),
    tag = "Platform"
)]
#[get("/oidc_trust")]
pub(crate) async fn list_oidc_trust(
    state: WebData<ServerState>,
    tenant_id: ReqData<TenantId>,
) -> Result<HttpResponse, ManagerError> {
    let items = state.db.lock().await.list_oidc_trust(*tenant_id).await?;
    Ok(HttpResponse::Ok()
        .insert_header(CacheControl(vec![CacheDirective::NoCache]))
        .json(&items))
}

/// Get OIDC trust relationship
#[utoipa::path(
    context_path = "/v0",
    security(("JSON web token (JWT) or API key" = [])),
    params(("name" = String, Path, description = "Trust relationship name")),
    responses(
        (status = OK, description = "Trust relationship retrieved", body = OidcTrustDescr),
        (status = NOT_FOUND, description = "No relationship with that name", body = ErrorResponse)
    ),
    tag = "Platform"
)]
#[get("/oidc_trust/{name}")]
pub(crate) async fn get_oidc_trust(
    state: WebData<ServerState>,
    tenant_id: ReqData<TenantId>,
    req: HttpRequest,
) -> Result<HttpResponse, ManagerError> {
    let name = parse_url_parameter(&req, "name")?;
    let item = state
        .db
        .lock()
        .await
        .get_oidc_trust(*tenant_id, &name)
        .await?;
    Ok(HttpResponse::Ok()
        .insert_header(CacheControl(vec![CacheDirective::NoCache]))
        .json(&item))
}

/// Create OIDC trust relationship
#[utoipa::path(
    context_path = "/v0",
    security(("JSON web token (JWT) or API key" = [])),
    request_body = NewOidcTrustRequest,
    responses(
        (status = CREATED, description = "Trust relationship created", body = NewOidcTrustResponse),
        (status = CONFLICT, description = "Name already in use", body = ErrorResponse),
        (status = BAD_REQUEST, description = "Invalid request", body = ErrorResponse),
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
    // Above `read`, the subject and audience must be concrete. `claim_matches`
    // treats `*` ANYWHERE in the pattern as a glob over any character run, so a
    // pattern like `*@*`, `*-prod`, or `*admin*` would authorize a broad,
    // unintended set of tokens at elevated access. Reject any `*`, not just a
    // bare `*`.
    if requested > Role::Read {
        let subject_wild = body.subject.contains('*');
        let audience_wild = body
            .audience
            .as_deref()
            .map(|a| a.contains('*'))
            .unwrap_or(false);
        if subject_wild || audience_wild {
            return Err(DBError::OidcTrustTooBroad {
                reason:
                    "subject and audience must not contain a '*' wildcard for a role above 'read'"
                        .to_string(),
            }
            .into());
        }
    }

    state
        .db
        .lock()
        .await
        .create_oidc_trust(
            *tenant_id,
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
        "Created OIDC trust '{}' (tenant: {}, issuer: {})",
        body.name, *tenant_id, body.issuer
    );
    Ok(HttpResponse::Created()
        .insert_header(CacheControl(vec![CacheDirective::NoCache]))
        .json(&NewOidcTrustResponse {
            id: OidcTrustId(new_id),
            name: body.name,
        }))
}

/// Delete OIDC trust relationship
#[utoipa::path(
    context_path = "/v0",
    security(("JSON web token (JWT) or API key" = [])),
    params(("name" = String, Path, description = "Trust relationship name")),
    responses(
        (status = OK, description = "Trust relationship deleted"),
        (status = NOT_FOUND, description = "No relationship with that name", body = ErrorResponse)
    ),
    tag = "Platform"
)]
#[delete("/oidc_trust/{name}")]
pub(crate) async fn delete_oidc_trust(
    state: WebData<ServerState>,
    tenant_id: ReqData<TenantId>,
    req: HttpRequest,
) -> Result<HttpResponse, ManagerError> {
    let name = parse_url_parameter(&req, "name")?;
    state
        .db
        .lock()
        .await
        .delete_oidc_trust(*tenant_id, &name)
        .await?;
    info!("Deleted OIDC trust '{name}' (tenant: {})", *tenant_id);
    Ok(HttpResponse::Ok().finish())
}
