/// Configuration API to retrieve the current authentication configuration
use actix_web::{get, web::Data as WebData, HttpRequest, HttpResponse};
use log::debug;
use serde::Serialize;
use utoipa::ToSchema;

use super::{ManagerError, ServerState};

/// Get authentication provider configuration
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
    tag = "Authentication"
)]
#[get("/config/authentication")]
async fn get_authentication_config(
    state: WebData<ServerState>,
    req: HttpRequest,
) -> Result<HttpResponse, ManagerError> {
    debug!("Received {req:?}");
    if state._config.auth_provider == crate::config::AuthProviderType::None {
        return Ok(HttpResponse::Ok().json(EmptyResponse {}));
    }
    let auth_config = req.app_data::<crate::auth::AuthConfiguration>().unwrap();
    Ok(HttpResponse::Ok().json(&auth_config.provider))
}

/// Get the list of demo URLs.
#[utoipa::path(
    path="/config/demos",
    responses(
        (status = OK
            , description = "URLs to JSON objects that describe a set of demos"
            , content_type = "application/json"
            , body = Vec<String>),
    ),
    context_path = "/v0",
    security(("JSON web token (JWT) or API key" = [])),
    tag = "Configuration",
)]
#[get("/config/demos")]
async fn get_demos(state: WebData<ServerState>) -> Result<HttpResponse, ManagerError> {
    Ok(HttpResponse::Ok().json(&state._config.demos))
}

#[derive(Serialize, ToSchema)]
pub(crate) struct EmptyResponse {}
