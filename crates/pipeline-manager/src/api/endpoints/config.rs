// Configuration API to retrieve the current authentication configuration and list of demos
use actix_web::{get, web::Data as WebData, HttpRequest, HttpResponse};
use serde::Serialize;
use serde_json::json;
use utoipa::ToSchema;

use crate::api::main::ServerState;
use crate::error::ManagerError;

#[derive(Clone, Serialize, ToSchema)]
pub(crate) struct Configuration {
    pub telemetry: String,
    pub version: String,
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
    Ok(HttpResponse::Ok().json(json!({
        "telemetry": state._config.telemetry,
        "edition": if cfg!(feature = "feldera-enterprise") {
            "Enterprise"
        } else {
            "Open source"
        },
        "version": if cfg!(feature = "feldera-enterprise") {
            env!("FELDERA_ENTERPRISE_VERSION")
        } else {
            env!("CARGO_PKG_VERSION")
        },
        "revision": env!("FELDERA_PLATFORM_VERSION_SUFFIX")
    })))
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
