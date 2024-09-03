// Configuration API to retrieve the current authentication configuration and list of demos
use crate::demo::{read_demos_from_directory, Demo};
use actix_web::{get, web::Data as WebData, HttpRequest, HttpResponse};
use serde::Serialize;
use serde_json::json;
use std::path::Path;
use utoipa::ToSchema;

use super::{ManagerError, ServerState};

/// Retrieve general configuration.
#[utoipa::path(
    path="/config",
    responses(
        (status = OK
            , description = "The response body contains basic configuration information about this host."
            , content_type = "application/json"
            , body = AuthProvider),
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
    Ok(HttpResponse::Ok()
        .json(json!({"telemetry": state._config.telemetry, "version": env!("CARGO_PKG_VERSION") })))
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
    path="/config/demos",
    responses(
        (status = OK
            , description = "List of demos."
            , content_type = "application/json"
            , body = Vec<Demo>),
        (status = INTERNAL_SERVER_ERROR
            , description = "Failed to read demos from the demos directory."
            , body = ErrorResponse),
    ),
    context_path = "/v0",
    security(("JSON web token (JWT) or API key" = [])),
    tag = "Configuration",
)]
#[get("/config/demos")]
async fn get_config_demos(state: WebData<ServerState>) -> Result<HttpResponse, ManagerError> {
    match &state._config.demos_dir {
        None => Ok(HttpResponse::Ok().json(Vec::<Demo>::new())),
        Some(demos_dir) => {
            Ok(HttpResponse::Ok().json(read_demos_from_directory(Path::new(&demos_dir))?))
        }
    }
}

#[derive(Serialize, ToSchema)]
pub(crate) struct EmptyResponse {}
