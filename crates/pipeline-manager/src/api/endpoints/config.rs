// Configuration API to retrieve the current authentication configuration and list of demos
use actix_web::{get, web::Data as WebData, HttpRequest, HttpResponse};
use chrono::{DateTime, Utc};
use serde::Serialize;
use utoipa::ToSchema;

use crate::api::main::ServerState;
use crate::error::ManagerError;

#[derive(Serialize, ToSchema)]
pub enum DisplaySchedule {
    Once,
    Session,
    Every { seconds: u64 },
    Always,
}

#[derive(Serialize, ToSchema)]
pub(crate) struct LicenseInformation {
    pub expires_in_seconds: u64,
    pub expires_at: DateTime<Utc>,
    /// Is current license expired
    pub is_expired: bool,
    /// Is current license a trial
    pub is_trial: bool,
    /// Optional description of the advantages of extending the license / upgrading from a trial
    pub description_html: String,
    /// URL that navigates the user to extend/upgrade their license
    pub extension_url: String,
    /// Suggested frequency of reminding the user to extend/upgrade their license
    pub remind_schedule: DisplaySchedule,
    pub remind_starting_at: DateTime<Utc>,
}

#[derive(Serialize, ToSchema)]
pub(crate) struct UpdateInformation {
    pub latest_version: String,
    pub is_latest_version: bool,
    /// URL that navigates the user to instructions on how to upgrade their deployment's version
    pub instructions_url: String,
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
    pub license_info: Option<LicenseInformation>,
    pub update_info: Option<UpdateInformation>,
}

/// Retrieve general configuration.
#[utoipa::path(
    path="/config",
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
async fn get_config(
    state: WebData<ServerState>,
    _req: HttpRequest,
) -> Result<HttpResponse, ManagerError> {
    Ok(HttpResponse::Ok().json(Configuration {
        telemetry: state._config.telemetry.clone(),
        edition: if cfg!(feature = "feldera-enterprise") {
            "Enterprise"
        } else {
            "Open source"
        }
        .to_string(),
        version: if cfg!(feature = "feldera-enterprise") {
            env!("FELDERA_ENTERPRISE_VERSION")
        } else {
            env!("CARGO_PKG_VERSION")
        }
        .to_string(),
        revision: env!("FELDERA_PLATFORM_VERSION_SUFFIX").to_string(),
        changelog_url: "".to_string(),
        license_info: None,
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
async fn get_config_authentication(
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
async fn get_config_demos(state: WebData<ServerState>) -> Result<HttpResponse, ManagerError> {
    Ok(HttpResponse::Ok().json(&state.demos))
}

#[derive(Serialize, ToSchema)]
pub(crate) struct EmptyResponse {}
