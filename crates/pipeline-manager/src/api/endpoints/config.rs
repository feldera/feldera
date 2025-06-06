// Configuration API to retrieve the current authentication configuration and list of demos
use actix_web::{get, web::Data as WebData, HttpRequest, HttpResponse};
use serde::Serialize;
use utoipa::ToSchema;

use crate::api::error::ApiError;
use crate::api::main::ServerState;
use crate::error::ManagerError;
use crate::license::{DisplaySchedule, LicenseInformation, LICENSE_INFO_READ_LOCK_TIMEOUT};

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
    /// This is an empty string if it is unspecified.
    pub revision: String,
    /// URL that navigates to the changelog of the current version
    pub changelog_url: String,
    /// Information about the current Enterprise license
    pub license_info: Option<LicenseInformation>,
    /// Information about whether a new version is available for the corresponding edition
    pub update_info: Option<UpdateInformation>,
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
    _req: HttpRequest,
) -> Result<HttpResponse, ManagerError> {
    let version = if cfg!(feature = "feldera-enterprise") {
        env!("FELDERA_ENTERPRISE_VERSION")
    } else {
        env!("CARGO_PKG_VERSION")
    }
    .to_string();
    // Acquire and read the license information. The timeout prevents it from becoming unresponsive
    // if it cannot acquire the lock, which generally should not happen.
    let license_info =
        match tokio::time::timeout(LICENSE_INFO_READ_LOCK_TIMEOUT, state.license_info.read()).await
        {
            Ok(license_info) => license_info.clone(),
            Err(_elapsed) => {
                return Err(ManagerError::from(ApiError::LockTimeout {
                    value: "license information".to_string(),
                    timeout: LICENSE_INFO_READ_LOCK_TIMEOUT,
                }));
            }
        };
    Ok(HttpResponse::Ok().json(Configuration {
        telemetry: state._config.telemetry.clone(),
        edition: if cfg!(feature = "feldera-enterprise") {
            "Enterprise"
        } else {
            "Open source"
        }
        .to_string(),
        version: version.clone(),
        revision: env!("FELDERA_PLATFORM_VERSION_SUFFIX").to_string(),
        changelog_url: if cfg!(feature = "feldera-enterprise") {
            "".to_string()
        } else {
            format!("https://github.com/feldera/feldera/releases/tag/v{version}")
        },
        license_info,
        update_info: None,
    }))
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
    if state._config.auth_provider == crate::config::AuthProviderType::None {
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
pub(crate) struct EmptyResponse {}
