// Configuration API to retrieve the current authentication configuration and list of demos
use actix_web::{
    get,
    web::{Data as WebData, ReqData},
    HttpRequest, HttpResponse,
};
use feldera_cloud1_client::license::DisplaySchedule;
use serde::Serialize;
use utoipa::ToSchema;

use crate::api::main::ServerState;
use crate::db::storage::Storage;
use crate::db::types::tenant::TenantId;
use crate::error::ManagerError;
use crate::license::{LicenseCheck, LicenseValidity};
use crate::unstable_features;

#[derive(Serialize, ToSchema)]
pub(crate) struct UpdateInformation {
    /// Latest version corresponding to the edition
    pub latest_version: String,
    /// Whether the current version matches the latest version
    pub is_latest_version: bool,
    /// URL that navigates the user to instructions on how to update their deployment's version
    pub instructions_url: String,
    /// Suggested frequency of reminding the user about updating
    pub remind_schedule: DisplaySchedule,
}

/// Information about the build of the platform.
#[derive(Serialize, ToSchema)]
pub(crate) struct BuildInformation {
    /// Timestamp of the build.
    build_timestamp: &'static str,
    /// CPU of build machine.
    build_cpu: &'static str,
    /// OS of build machine.
    build_os: &'static str,
    /// Dependencies used during the build.
    cargo_dependencies: &'static str,
    /// Features enabled during the build.
    cargo_features: &'static str,
    /// Whether the build is optimized for performance.
    cargo_debug: &'static str,
    /// Optimization level of the build.
    cargo_opt_level: &'static str,
    /// Target triple of the build.
    cargo_target_triple: &'static str,
    /// Rust version of the build used.
    rustc_version: &'static str,
}

impl BuildInformation {
    fn from_env() -> Self {
        Self {
            build_timestamp: env!("VERGEN_BUILD_TIMESTAMP"),
            build_cpu: env!("VERGEN_SYSINFO_CPU_BRAND"),
            build_os: env!("VERGEN_SYSINFO_OS_VERSION"),
            cargo_dependencies: env!("VERGEN_CARGO_DEPENDENCIES"),
            cargo_features: env!("VERGEN_CARGO_FEATURES"),
            cargo_debug: env!("VERGEN_CARGO_DEBUG"),
            cargo_opt_level: env!("VERGEN_CARGO_OPT_LEVEL"),
            cargo_target_triple: env!("VERGEN_CARGO_TARGET_TRIPLE"),
            rustc_version: env!("VERGEN_RUSTC_SEMVER"),
        }
    }
}

#[derive(Serialize, ToSchema)]
pub(crate) struct Configuration {
    /// Telemetry key.
    pub telemetry: String,
    /// Feldera edition: "Open source" or "Enterprise"
    pub edition: String,
    /// The version corresponding to the type of `edition`.
    /// Format is `x.y.z`.
    pub version: String,
    /// Specific revision corresponding to the edition `version` (e.g., git commit hash).
    pub revision: String,
    /// Specific revision corresponding to the default runtime version of the platform (e.g., git commit hash).
    pub runtime_revision: String,
    /// List of unstable features that are enabled.
    pub unstable_features: Option<String>,
    /// URL that navigates to the changelog of the current version
    pub changelog_url: String,
    /// Information about the checked Enterprise license
    pub license_validity: Option<LicenseValidity>,
    /// Information about whether a new version is available for the corresponding edition
    pub update_info: Option<UpdateInformation>,
    /// Information about the build environment
    pub build_info: BuildInformation,
}

impl Configuration {
    pub(crate) async fn gather(state: &ServerState) -> Self {
        let version = env!("CARGO_PKG_VERSION").to_string();
        let mut revision = env!("FELDERA_PLATFORM_VERSION_SUFFIX");
        if revision.is_empty() {
            // For local builds that don't set FELDERA_PLATFORM_VERSION_SUFFIX,
            // we use the git SHA as the revision.
            revision = env!("VERGEN_GIT_SHA");
        }
        let runtime_revision = env!("VERGEN_GIT_SHA");
        let license_check = LicenseCheck::validate(state).await.unwrap_or_default();

        Configuration {
            telemetry: state.config.telemetry.clone(),
            edition: if cfg!(feature = "feldera-enterprise") {
                "Enterprise"
            } else {
                "Open source"
            }
            .to_string(),
            version: version.clone(),
            revision: revision.to_string(),
            runtime_revision: runtime_revision.to_string(),
            unstable_features: unstable_features()
                .map(|features| features.iter().cloned().collect::<Vec<&str>>().join(",")),
            changelog_url: if cfg!(feature = "feldera-enterprise") {
                "https://docs.feldera.com/changelog/".to_string()
            } else {
                format!("https://github.com/feldera/feldera/releases/tag/v{version}")
            },
            license_validity: license_check.map(|v| v.check_outcome),
            update_info: None,
            build_info: BuildInformation::from_env(),
        }
    }
}

/// Retrieve general configuration.
#[utoipa::path(
    context_path = "/v0",
    security(("JSON web token (JWT) or API key" = [])),
    responses(
        (status = OK
            , description = "The response body contains basic configuration information about this host."
            , content_type = "application/json"
            , body = Configuration),
        (status = INTERNAL_SERVER_ERROR
            , description = "Request failed."
            , body = ErrorResponse),
    ),
    tag = "Configuration"
)]
#[get("/config")]
pub(crate) async fn get_config(
    state: WebData<ServerState>,
    _client: WebData<awc::Client>,
    _tenant_id: ReqData<TenantId>,
) -> Result<HttpResponse, ManagerError> {
    let config = Configuration::gather(&state).await;
    Ok(HttpResponse::Ok().json(config))
}

/// Retrieve authentication provider configuration.
#[utoipa::path(
    path="/config/authentication",
    responses(
        (status = OK
            , description = "The response body contains Authentication Provider configuration, \
                             or is empty if no auth is configured."
            , content_type = "application/json"
            , body = AuthProvider),
        (status = INTERNAL_SERVER_ERROR
            , description = "Request failed."
            , body = ErrorResponse),
    ),
    tag = "Configuration"
)]
#[get("/config/authentication")]
pub(crate) async fn get_config_authentication(
    state: WebData<ServerState>,
    req: HttpRequest,
) -> Result<HttpResponse, ManagerError> {
    if state.config.auth_provider == crate::config::AuthProviderType::None {
        return Ok(HttpResponse::Ok().json(EmptyResponse {}));
    }
    let auth_config = req.app_data::<crate::auth::AuthConfiguration>().unwrap();
    Ok(HttpResponse::Ok().json(&auth_config.provider))
}

/// Retrieve the list of demos.
#[utoipa::path(
    context_path = "/v0",
    security(("JSON web token (JWT) or API key" = [])),
    responses(
        (status = OK
            , description = "List of demos"
            , content_type = "application/json"
            , body = Vec<Demo>),
        (status = INTERNAL_SERVER_ERROR
            , description = "Failed to read demos from the demos directories"
            , body = ErrorResponse),
    ),
    tag = "Configuration",
)]
#[get("/config/demos")]
pub(crate) async fn get_config_demos(
    state: WebData<ServerState>,
) -> Result<HttpResponse, ManagerError> {
    Ok(HttpResponse::Ok().json(&state.demos))
}

#[derive(Serialize, ToSchema)]
pub(crate) struct SessionInfo {
    /// Current user's tenant ID
    pub tenant_id: TenantId,
    /// Current user's tenant name
    pub tenant_name: String,
}

impl SessionInfo {
    pub(crate) async fn gather(state: &ServerState, tenant_id: TenantId) -> Self {
        let db = state.db.lock().await;
        let tenant_name = db
            .get_tenant_name(tenant_id)
            .await
            .unwrap_or_else(|_| "unknown".to_string());

        SessionInfo {
            tenant_id,
            tenant_name,
        }
    }
}

/// Retrieve current session information.
#[utoipa::path(
    context_path = "/v0",
    security(("JSON web token (JWT) or API key" = [])),
    responses(
        (status = OK
            , description = "The response body contains current session information including tenant details."
            , content_type = "application/json"
            , body = SessionInfo),
        (status = INTERNAL_SERVER_ERROR
            , description = "Request failed."
            , body = ErrorResponse),
    ),
    tag = "Configuration"
)]
#[get("/config/session")]
pub(crate) async fn get_config_session(
    state: WebData<ServerState>,
    tenant_id: ReqData<TenantId>,
) -> Result<HttpResponse, ManagerError> {
    let session_info = SessionInfo::gather(&state, *tenant_id).await;
    Ok(HttpResponse::Ok().json(session_info))
}

#[derive(Serialize, ToSchema)]
pub(crate) struct EmptyResponse {}
