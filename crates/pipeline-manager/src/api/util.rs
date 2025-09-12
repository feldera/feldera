use crate::api::error::ApiError;
use crate::error::ManagerError;
use actix_web::HttpRequest;

/// Parses a named parameter from the URL of the request.
/// For example, a request "GET /examples/{example}" would have an "example" parameter.
/// Returns an error if the parameter is not found.
pub(crate) fn parse_url_parameter(
    req: &HttpRequest,
    param_name: &'static str,
) -> Result<String, ManagerError> {
    match req.match_info().get(param_name) {
        None => Err(ManagerError::from(ApiError::MissingUrlEncodedParam {
            param: param_name,
        })),
        Some(value) => Ok(urlencoding::decode(value)
            .map_err(|e| {
                ManagerError::from(ApiError::InvalidNameParam {
                    value: value.to_string(),
                    error: format!("Failed to URL-decode parameter '{}': {}", param_name, e),
                })
            })?
            .to_string()),
    }
}
