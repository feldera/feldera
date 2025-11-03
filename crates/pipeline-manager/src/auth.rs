//! Support HTTP bearer and API-key authorization to the pipeline manager API.
//! This implementation supports multiple OIDC/OAuth2 providers including AWS Cognito,
//! Google Identity, and Okta.

//! This file implements an actix-web middleware to validate JWT bearer tokens
//! and API keys.
//!
//! 1) JWT tokens:
//!
//! The expected workflow is for users to login via a browser to receive a JWT
//! access token. Clients then issue pipeline manager APIs using an HTTP
//! authorization header for bearer tokens (Authorization: Bearer &lt; token
//! &gt;). With
//! a bearer token, users may generate API keys that can be used for
//! programmatic access (see below).
//!
//! Bearer token validation follows standard OIDC/OAuth2 protocols including
//! signing algorithm verification, expiry (exp) validation, client_id and issuer
//! (iss) validation, signature verification, and tamper detection. For signature
//! verification, we fetch the provider's JWK keys from their well-known URL
//! and cache them locally. We don't yet refresh JWK keys automatically, but
//! a restart of the pipeline manager will refresh the cache.
//!
//! To support bearer token workflows, we require environment variables specific
//! to each provider. All providers require FELDERA_AUTH_CLIENT_ID and FELDERA_AUTH_ISSUER.
//! Some providers may require additional configuration (see provider-specific
//! functions below).
//!
//! 2) API-keys:
//!
//! For programmatic access, a user authenticated via a Bearer token may
//! generate API keys. See the `API keys` endpoints in the pipeline manager's
//! OpenAPI spec (or look at the endpoints in `api/api_keys`).
//! These API keys can then be used in the REST API similar to how JWT tokens
//! are used above, but with the bearer token being "apikey:1234..." to
//! authorize access. For now, we simply have two permission types: Read and
//! Write. Later, we will expand to have fine-grained access to specific API
//! resources.
//!
//! API keys are randomly generated 128 character sequences that are never
//! stored in the pipeline manager or in the database. It is the responsibility
//! of the end-user or client to securely save them and consume them in their
//! programs via environment variables or secret stores as appropriate. On the
//! pipeline manager side, we store a hash of the API key in the database along
//! with the permissions.

use std::{collections::HashMap, env};

use actix_web::HttpMessage;
use actix_web::{dev::ServiceRequest, error::ErrorUnauthorized, web::Data};
use actix_web_httpauth::extractors::{
    bearer::{BearerAuth, Config},
    AuthenticationError,
};
use awc::error::JsonPayloadError;
use cached::{Cached, TimedCache};
use jsonwebtoken::{decode, decode_header, Algorithm, DecodingKey, TokenData, Validation};
use log::{debug, error};
use rand::rngs::ThreadRng;
use rand::{distributions::Alphanumeric, Rng};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use static_assertions::assert_impl_any;
use url::Url;
use utoipa::ToSchema;
use uuid::Uuid;

use crate::api::main::ServerState;
use crate::config::ApiServerConfig;
use crate::db::error::DBError;
use crate::db::storage::Storage;
use crate::db::storage_postgres::StoragePostgres;
use crate::db::types::api_key::ApiPermission;
use crate::db::types::tenant::TenantId;

// Used when no auth is configured, so we tag the request with the default user
// and passthrough
pub(crate) fn tag_with_default_tenant_id(req: ServiceRequest) -> ServiceRequest {
    req.extensions_mut().insert(DEFAULT_TENANT_ID);
    req.extensions_mut()
        .insert(vec![ApiPermission::Read, ApiPermission::Write]);
    req
}

/// Creates a JSON error response for authentication failures
fn create_authz_json_error(message: &str) -> actix_web::Error {
    let error_response = serde_json::json!({
        "message": message,
        "error_code": "AuthenticationFailed"
    });
    ErrorUnauthorized(error_response.to_string())
}

/// Authorization using a bearer token. Expects to find either a typical
/// OAuth2/OIDC JWT token or an API key. JWT tokens are expected to be available
/// as is, whereas API keys are prefix with the string "apikey:".
pub(crate) async fn auth_validator(
    req: ServiceRequest,
    credentials: BearerAuth,
) -> Result<ServiceRequest, (actix_web::error::Error, ServiceRequest)> {
    let token = credentials.token();
    // Check if we are using an API key first.
    if token.starts_with(API_KEY_PREFIX) {
        return api_key_auth(req, token).await;
    }
    bearer_auth(req, token).await
}

async fn bearer_auth(
    req: ServiceRequest,
    token: &str,
) -> Result<ServiceRequest, (actix_web::error::Error, ServiceRequest)> {
    // Validate bearer token
    let configuration = req.app_data::<AuthConfiguration>().unwrap();
    let token = match configuration.provider {
        AuthProvider::AwsCognito(_) => decode_aws_cognito_token(token, &req, configuration).await,
        AuthProvider::GenericOidc(_) => decode_generic_oidc_token(token, &req, configuration).await, // TODO: Implement Google Identity flow where Access Token is not a JWT but an opaque token
                                                                                                     // intended to be decoded with `https://www.googleapis.com/oauth2/v3/tokeninfo?access_token=...`
    };
    match token {
        Ok(claim) => {
            // Validate groups authorization (for providers that support groups)
            let state = req.app_data::<Data<ServerState>>().unwrap();
            if let Err(AuthError::InsufficientGroups) =
                validate_groups_authorization(&claim, &state.config)
            {
                return Err((
                    create_authz_json_error("User does not belong to required groups for access."),
                    req,
                ));
            }

            // Get tenant name using resolution logic with headers
            let tenant_name = match claim.tenant_name(&state.config, req.headers()) {
                Ok(name) => name,
                Err(AuthError::NoTenantFound) => {
                    return Err((
                        create_authz_json_error("You are not authorized to access any Feldera tenant. Contact your administrator if you need access to Feldera."),
                        req,
                    ));
                }
                Err(AuthError::MissingTenantHeader) => {
                    return Err((
                        create_authz_json_error("Feldera-Tenant header is required when your access token contains multiple tenants."),
                        req,
                    ));
                }
                Err(AuthError::UnauthorizedTenant(tenant)) => {
                    return Err((
                        create_authz_json_error(&format!("You are not authorized to access tenant '{}'. Check your access token's tenants claim.", tenant)),
                        req,
                    ));
                }
                Err(e) => {
                    error!("Tenant resolution error: {}", e);
                    return Err((
                        create_authz_json_error(&format!("Tenant resolution failed: {}", e)),
                        req,
                    ));
                }
            };

            // TODO: Handle tenant deletions at some point
            let tenant = {
                let db = &state.db.lock().await;
                db.get_or_create_tenant_id(Uuid::now_v7(), tenant_name, claim.provider())
                    .await
            };

            match tenant {
                Ok(tenant_id) => {
                    req.extensions_mut().insert(tenant_id);
                    req.extensions_mut()
                        .insert(vec![ApiPermission::Read, ApiPermission::Write]);
                    Ok(req)
                }
                Err(e) => {
                    error!(
                        "Could not fetch tenant ID for claim {:?}, with error {}",
                        claim, e
                    );
                    Err((
                        create_authz_json_error(&format!(
                            "Database error while fetching tenant: {}",
                            e
                        )),
                        req,
                    ))
                }
            }
        }
        Err(error) => {
            let descr = match error {
                AuthError::JwkFetch(e) => {
                    error!("JwkFetch: {:?}", e);
                    "Authentication failed - unable to fetch JWT keys".to_owned()
                }
                AuthError::JwkShape(e) => {
                    error!("JwkShapeError: {:?}", e);
                    "Authentication failed - JWT key format error".to_owned()
                }
                _ => error.to_string(),
            };
            let config = req.app_data::<Config>().cloned().unwrap_or_default();
            Err((
                AuthenticationError::from(config)
                    .with_error_description(descr)
                    .into(),
                req,
            ))
        }
    }
}

async fn api_key_auth(
    req: ServiceRequest,
    api_key_str: &str,
) -> Result<ServiceRequest, (actix_web::error::Error, ServiceRequest)> {
    // Check for an API-key
    let ad = req.app_data::<Data<ServerState>>();
    let validate = {
        let db = &ad.unwrap().db.lock().await;
        validate_api_keys(db, api_key_str).await
    };
    match validate {
        Ok((tenant_id, permissions)) => {
            req.extensions_mut().insert(tenant_id);
            req.extensions_mut().insert(permissions);
            Ok(req)
        }
        Err(error) => {
            match error {
                DBError::InvalidApiKey => {
                    let ip = req
                        .peer_addr()
                        .map(|addr| addr.ip().to_string())
                        .unwrap_or_else(|| "<unknown IP>".to_owned());
                    log::error!(
                        "authentication attempt using invalid API key from ip: '{}'",
                        ip
                    );
                }
                e => log::error!("failed to validate API key: {}", e),
            };

            Err((create_authz_json_error("Unauthorized API key"), req))
        }
    }
}

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[cfg_attr(test, derive(proptest_derive::Arbitrary))]
/// Represents information about the tenant, extracted
/// from an OAuth claim when using auth or
/// a default user when not using auth
pub(crate) struct TenantRecord {
    /// This ID is for server-side use only. Do not propagate
    /// this to the client.
    pub id: TenantId,

    /// Corresponds to the sub or subscriber from a claim
    pub tenant: String,

    /// Corresponds to the identity provider from a claim
    pub provider: String,
}

const DEFAULT_TENANT_ID: TenantId = TenantId(Uuid::nil());

impl TenantRecord {
    pub fn default() -> Self {
        Self {
            id: DEFAULT_TENANT_ID,
            tenant: "default".to_string(),
            provider: "default".to_string(),
        }
    }
}

#[derive(Debug)]
enum Claim {
    AwsCognito(TokenData<AwsCognitoClaim>),
    GenericOidc(TokenData<OidcClaim>),
}

impl Claim {
    /// Get the list of authorized tenants for this claim.
    /// Returns None if tenants should be resolved via other means (issuer/sub).
    fn authorized_tenants(&self) -> Option<Vec<String>> {
        match self {
            Claim::AwsCognito(t) => {
                // Priority: tenants array > single tenant claim
                if let Some(ref tenants) = t.claims.tenants {
                    Some(tenants.clone())
                } else {
                    t.claims.tenant.as_ref().map(|t| vec![t.clone()])
                }
            }
            Claim::GenericOidc(t) => {
                // Priority: tenants array > single tenant claim
                if let Some(ref tenants) = t.claims.tenants {
                    Some(tenants.clone())
                } else {
                    t.claims.tenant.as_ref().map(|t| vec![t.clone()])
                }
            }
        }
    }

    /// Resolve tenant name based on claim, headers, and configuration.
    ///
    /// For multi-tenant claims (tenants array), requires Feldera-Tenant header to select one.
    /// For single-tenant claims, uses fallback logic: tenant > issuer > sub
    fn tenant_name(
        &self,
        config: &crate::config::ApiServerConfig,
        headers: &actix_web::http::header::HeaderMap,
    ) -> Result<String, AuthError> {
        let (issuer, sub) = match self {
            Claim::AwsCognito(t) => (&t.claims.iss, &t.claims.sub),
            Claim::GenericOidc(t) => (&t.claims.iss, &t.claims.sub),
        };

        // Check if we have explicit tenant authorization in the claim
        if let Some(authorized) = self.authorized_tenants() {
            if authorized.len() > 1 {
                // Multi-tenant: require header selection
                let selected = headers
                    .get(TENANT_HEADER)
                    .and_then(|h| h.to_str().ok())
                    .ok_or(AuthError::MissingTenantHeader)?;

                // Validate selected tenant is authorized
                if authorized.contains(&selected.to_string()) {
                    return Ok(selected.to_string());
                } else {
                    return Err(AuthError::UnauthorizedTenant(selected.to_string()));
                }
            } else if authorized.len() == 1 {
                // Single tenant in array or single tenant claim
                return Ok(authorized[0].clone());
            }
            // Empty array falls through to fallback logic
        }

        // Fallback logic when no explicit tenant/tenants claims
        // Priority: issuer-domain > sub (if enabled)

        if config.issuer_tenant {
            if let Some(issuer_tenant) = extract_tenant_from_issuer(issuer) {
                return Ok(issuer_tenant);
            }
        }

        if config.individual_tenant {
            debug!("Using sub claim for tenant resolution: {}", sub);
            return Ok(sub.clone());
        }

        // No valid tenant found
        Err(AuthError::NoTenantFound)
    }

    fn provider(&self) -> String {
        match self {
            Claim::AwsCognito(t) => t.claims.iss.clone(),
            Claim::GenericOidc(t) => t.claims.iss.clone(),
        }
    }
}

/// Extract tenant identifier from OIDC issuer domain.
///
/// Extracts the full hostname from issuer URLs for tenant identification.
/// This approach avoids tenant name collisions by using the complete domain.
/// Examples:
/// - "<https://acme-corp.okta.com/oauth2/default>" → Some("acme-corp.okta.com")
/// - "<https://company.auth.us-west-2.amazoncognito.com/oauth2>" → Some("company.auth.us-west-2.amazoncognito.com")
/// - "<https://accounts.google.com>" → Some("accounts.google.com")
///
/// Validates that the user belongs to at least one required group (for providers that support groups).
fn validate_groups_authorization(claim: &Claim, config: &ApiServerConfig) -> Result<(), AuthError> {
    // Only validate groups for providers that include groups claim
    let groups = match claim {
        Claim::GenericOidc(token) => &token.claims.groups,
        _ => return Ok(()), // No group validation for other providers (e.g., AWS Cognito)
    };

    // If no groups are configured, allow access
    if config.authorized_groups.is_empty() {
        return Ok(());
    }

    // Check if user has any of the required groups
    if let Some(user_groups) = groups {
        let has_required_group = user_groups
            .iter()
            .any(|user_group| config.authorized_groups.contains(user_group));

        if has_required_group {
            Ok(())
        } else {
            Err(AuthError::InsufficientGroups)
        }
    } else {
        // User has no groups but groups are required
        Err(AuthError::InsufficientGroups)
    }
}

/// Extract tenant identifier from issuer claim in OIDC Access token.
/// The full issuer hostname is used to avoid collisions.
fn extract_tenant_from_issuer(issuer: &str) -> Option<String> {
    Url::parse(issuer)
        .ok()
        .and_then(|url| url.host_str().map(|host| host.to_string()))
        .filter(|host| !host.is_empty())
}

#[derive(Clone, Serialize, ToSchema)]
pub(crate) struct ProviderAwsCognito {
    pub issuer: String,
    pub login_url: String,
    pub logout_url: String,
    #[serde(skip)]
    pub(crate) jwk_uri: String,
}

#[derive(Clone, Serialize, ToSchema)]
pub(crate) struct ProviderGenericOidc {
    pub issuer: String,
    pub client_id: String,
    pub extra_oidc_scopes: Vec<String>,
    #[serde(skip)]
    pub(crate) jwk_uri: String,
}

#[derive(Deserialize)]
struct OidcDiscoveryDocument {
    jwks_uri: String,
}

/// Fetch OIDC discovery document and extract jwks_uri
async fn fetch_jwks_uri_from_discovery(issuer: &str) -> Result<String, reqwest::Error> {
    let discovery_url = format!(
        "{}/.well-known/openid-configuration",
        issuer.trim_end_matches('/')
    );
    let response = reqwest::get(&discovery_url).await?;
    let discovery: OidcDiscoveryDocument = response.json().await?;
    Ok(discovery.jwks_uri)
}

#[derive(Clone, Serialize, ToSchema)]
pub(crate) enum AuthProvider {
    AwsCognito(ProviderAwsCognito), // The argument is the URL to use for fetching JWKs
    GenericOidc(ProviderGenericOidc),
}

pub(crate) fn aws_auth_config() -> AuthConfiguration {
    let mut validation = Validation::new(Algorithm::RS256);
    let client_id = env::var("FELDERA_AUTH_CLIENT_ID")
        .expect("Missing environment variable FELDERA_AUTH_CLIENT_ID");
    let iss =
        env::var("FELDERA_AUTH_ISSUER").expect("Missing environment variable FELDERA_AUTH_ISSUER");
    let jwk_uri = format!("{}/.well-known/jwks.json", iss);
    // We do not validate with set_audience because it is optional,
    // and AWS Cognito doesn't consistently claim it in JWT (e.g. via Hosted UI
    // auth)
    validation.set_issuer(&[&iss]);
    AuthConfiguration {
        provider: AuthProvider::AwsCognito(ProviderAwsCognito {
            issuer: iss.clone(),
            login_url: env::var("AWS_COGNITO_LOGIN_URL")
                .expect("Missing environment variable AWS_COGNITO_LOGIN_URL"),
            logout_url: env::var("AWS_COGNITO_LOGOUT_URL")
                .expect("Missing environment variable AWS_COGNITO_LOGOUT_URL"),
            jwk_uri,
        }),
        validation,
        client_id,
    }
}

pub(crate) async fn generic_oidc_auth_config(
    api_config: &crate::config::ApiServerConfig,
) -> Result<AuthConfiguration, Box<dyn std::error::Error>> {
    let mut validation = Validation::new(Algorithm::RS256);
    let client_id = env::var("FELDERA_AUTH_CLIENT_ID")
        .expect("Missing environment variable FELDERA_AUTH_CLIENT_ID");
    let iss =
        env::var("FELDERA_AUTH_ISSUER").expect("Missing environment variable FELDERA_AUTH_ISSUER");

    // Use OIDC discovery to fetch jwks_uri
    let jwk_uri = fetch_jwks_uri_from_discovery(&iss).await?;

    validation.set_issuer(&[&iss]);
    // Use configurable audience claim from API server configuration
    validation.set_audience(&[&api_config.auth_audience]);

    // Add 'groups' scope if authorized_groups is configured
    let extra_oidc_scopes = if !api_config.authorized_groups.is_empty() {
        vec!["groups".to_string()]
    } else {
        vec![]
    };

    Ok(AuthConfiguration {
        provider: AuthProvider::GenericOidc(ProviderGenericOidc {
            issuer: iss.clone(),
            client_id: client_id.clone(),
            extra_oidc_scopes,
            jwk_uri,
        }),
        validation,
        client_id,
    })
}

#[derive(Clone)]
// Expected issuer and client_id for each authentication request
pub(crate) struct AuthConfiguration {
    pub provider: AuthProvider,
    pub validation: Validation,
    pub client_id: String,
}

/// Generic OIDC claim structure that works with most OIDC providers
#[derive(Clone, Debug, Serialize, Deserialize)]
struct OidcClaim {
    /// The audience for the token (can be string or array of strings)
    /// Using serde_json::Value to handle both cases, let jsonwebtoken lib handle validation
    aud: Option<serde_json::Value>,

    /// The client ID that authenticated (may be in aud or separate field)
    client_id: Option<String>,

    /// The expiration time in Unix time format
    exp: i64,

    /// The issued-at-time in Unix time format
    iat: i64,

    /// The identity provider that issued the token
    iss: String,

    /// A UUID or "subject" for the authenticated user
    sub: String,

    /// Unique identifier for the JWT
    jti: Option<String>,

    /// OAuth 2.0 scopes
    scope: Option<String>,

    /// Purpose of the token (access, id, refresh)
    token_use: Option<String>,

    /// The username (provider-specific)
    username: Option<String>,

    /// Email address (if available)
    email: Option<String>,

    /// Tenant identifier for single-tenant deployments
    /// TODO: Deprecated, remove when no one no longer uses it
    tenant: Option<String>,

    /// Tenant identifiers for multi-tenant access
    /// Can be either an array of strings or a comma-separated string
    /// When present, user can access any of these tenants
    #[serde(deserialize_with = "deserialize_list")]
    tenants: Option<Vec<String>>,

    /// User groups for authorization (Okta only)
    groups: Option<Vec<String>>,

    /// Additional claims for provider-specific data
    #[serde(flatten)]
    additional_claims: serde_json::Map<String, serde_json::Value>,
}

/// Deserialize tenants claim which can be either:
/// - An array of strings: ["tenant1", "tenant2"]
/// - A comma-separated string: "tenant1,tenant2"
fn deserialize_list<'de, D>(deserializer: D) -> Result<Option<Vec<String>>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    use serde::Deserialize;

    #[derive(Deserialize)]
    #[serde(untagged)]
    enum TenantsValue {
        Array(Vec<String>),
        String(String),
    }

    let value = Option::<TenantsValue>::deserialize(deserializer)?;
    Ok(value.map(|v| match v {
        TenantsValue::Array(arr) => arr,
        TenantsValue::String(s) => s.split(',').map(|t| t.trim().to_string()).collect(),
    }))
}

/// The shape of a claim provided to clients by AwsCognito, following
/// the guide below:
///
/// <https://docs.aws.amazon.com/cognito/latest/developerguide/amazon-cognito-user-pools-using-the-access-token.html>
#[derive(Clone, Debug, Serialize, Deserialize)]
struct AwsCognitoClaim {
    /// The user pool app client that authenticated the client
    client_id: String,

    /// The expiration time in Unix time format
    exp: i64,

    /// The issued-at-time in Unix time format
    iat: i64,

    /// The identity provider that issued the token
    iss: String,

    /// A UUID or "subject" for the authenticated user
    sub: String,

    /// Unique identifier for the JWT
    jti: String,

    /// Token revocation identifier associated with the user's refresh token
    origin_jti: Option<String>,

    /// OAuth 2.0 scopes
    scope: String,

    /// Purpose of the token. For the purpose of bearer authentication,
    /// this value should always be "access"
    token_use: String,

    /// The username. Note: this may not be unique within a user pool.
    /// The sub claim is the appropriate identifier for a user.
    username: String,

    /// Tenant identifier for single-tenant deployments
    /// TODO: Deprecated, remove when no one no longer uses it
    tenant: Option<String>,

    /// Tenant identifiers for multi-tenant access
    /// Can be either an array of strings or a comma-separated string
    /// When present, user can access any of these tenants
    #[serde(deserialize_with = "deserialize_list")]
    tenants: Option<Vec<String>>,
}

#[derive(Debug)]
enum AuthError {
    JwtDecoding(jsonwebtoken::errors::Error),
    JwkFetch(awc::error::SendRequestError),
    JwkPayload(awc::error::PayloadError),
    JwkContentType,
    JwkShape(String),
    NoTenantFound,
    InsufficientGroups,
    MissingTenantHeader,
    UnauthorizedTenant(String),
}

impl std::fmt::Display for AuthError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            AuthError::JwtDecoding(err) => err.fmt(f),
            AuthError::JwkFetch(err) => err.fmt(f),
            AuthError::JwkPayload(err) => err.fmt(f),
            AuthError::JwkShape(err) => err.fmt(f),
            AuthError::JwkContentType => f.write_str("Content type error"),
            AuthError::NoTenantFound => f.write_str("You are not authorized to access any Feldera tenant. Contact your administrator if you need access to Feldera."),
            AuthError::InsufficientGroups => f.write_str("User does not belong to required groups for access."),
            AuthError::MissingTenantHeader => f.write_str("Feldera-Tenant header is required when access token contains multiple tenants."),
            AuthError::UnauthorizedTenant(tenant) => write!(f, "You are not authorized to access tenant '{}'. Check your access token's tenants claim.", tenant),
        }
    }
}

impl From<awc::error::SendRequestError> for AuthError {
    fn from(value: awc::error::SendRequestError) -> Self {
        Self::JwkFetch(value)
    }
}

impl From<jsonwebtoken::errors::Error> for AuthError {
    fn from(value: jsonwebtoken::errors::Error) -> Self {
        Self::JwtDecoding(value)
    }
}

impl From<jsonwebtoken::errors::ErrorKind> for AuthError {
    fn from(value: jsonwebtoken::errors::ErrorKind) -> Self {
        Self::JwtDecoding(value.into())
    }
}

/// Generic OIDC token validation that can be used by most providers
/// Follows standard JWT validation practices
/// JWT claims: <https://datatracker.ietf.org/doc/html/rfc7519#section-4>
async fn decode_oidc_token<T>(
    token: &str,
    req: &ServiceRequest,
    configuration: &AuthConfiguration,
    claim_constructor: fn(TokenData<T>) -> Claim,
    _validate_token_use: Option<&str>,
) -> Result<Claim, AuthError>
where
    T: for<'de> serde::Deserialize<'de> + serde::Serialize,
{
    let header = decode_header(token);
    match header {
        Ok(header) => {
            match header.alg {
                // AWS Cognito user pools use RS256
                Algorithm::RS256 => {
                    if header.kid.is_none() {
                        debug!("JWT header missing 'kid' field");
                        return Err(jsonwebtoken::errors::ErrorKind::InvalidToken.into());
                    }

                    let kid = header.kid.unwrap();

                    let state = req.app_data::<Data<ServerState>>().unwrap();
                    let cache = &mut state.jwk_cache.lock().await;
                    let jwk = cache.get(&kid, &configuration.provider).await?;

                    let token_data = decode::<T>(token, &jwk, &configuration.validation);
                    match token_data {
                        Ok(data) => Ok(claim_constructor(data)),
                        Err(jwt_error) => {
                            debug!("JWT token validation failed: {:?}", jwt_error.kind());
                            Err(jwt_error.into())
                        }
                    }
                }
                _ => {
                    debug!("Unsupported JWT algorithm: {:?}", header.alg);
                    Err(jsonwebtoken::errors::ErrorKind::InvalidAlgorithm.into())
                }
            }
        }
        Err(e) => {
            debug!("Failed to decode JWT header: {:?} - {}", e.kind(), e);
            Err(e.into())
        }
    }
}

/// AWS Cognito-specific token decoder that wraps the generic OIDC function
async fn decode_aws_cognito_token(
    token: &str,
    req: &ServiceRequest,
    configuration: &AuthConfiguration,
) -> Result<Claim, AuthError> {
    let claim =
        decode_oidc_token::<AwsCognitoClaim>(token, req, configuration, Claim::AwsCognito, None)
            .await?;

    // AWS Cognito-specific validations
    if let Claim::AwsCognito(ref data) = claim {
        // Validate client_id (AWS Cognito puts it in a separate field)
        if configuration.client_id != data.claims.client_id {
            return Err(jsonwebtoken::errors::ErrorKind::InvalidAudience.into());
        }
        // Validate token_use (AWS Cognito-specific field)
        if data.claims.token_use != "access" {
            return Err(jsonwebtoken::errors::ErrorKind::InvalidToken.into());
        }
    }

    Ok(claim)
}

/// Generic OIDC token decoder for Okta and Google Identity
async fn decode_generic_oidc_token(
    token: &str,
    req: &ServiceRequest,
    configuration: &AuthConfiguration,
) -> Result<Claim, AuthError> {
    let result = decode_oidc_token::<OidcClaim>(
        token,
        req,
        configuration,
        Claim::GenericOidc,
        None, // Generic OIDC providers don't require token_use validation
    )
    .await;

    if let Err(e) = &result {
        debug!("Generic OIDC token decoding error: {}", e);
    }

    result
}

pub struct JwkCache {
    cache: TimedCache<String, DecodingKey>,
}

const DEFAULT_JWK_CACHE_LIFETIME_SECONDS: u64 = 120;
const DEFAULT_JWK_CACHE_CAPACITY: usize = 10;

impl JwkCache {
    pub(crate) fn new() -> JwkCache {
        Self {
            cache: TimedCache::with_lifespan_and_capacity(
                DEFAULT_JWK_CACHE_LIFETIME_SECONDS,
                DEFAULT_JWK_CACHE_CAPACITY,
            ),
        }
    }

    async fn get(
        &mut self,
        key: &String,
        provider: &AuthProvider,
    ) -> Result<DecodingKey, AuthError> {
        let cache = &mut self.cache;
        let val = &cache.cache_get(key);
        match val {
            Some(dk) => Ok((*dk).clone()),
            None => {
                // TODO: Introduce a minimum delay between refreshes
                let fetched = fetch_jwk_keys(provider).await;
                match fetched {
                    Ok(map) => {
                        for (key_id, decoding_key) in map {
                            cache.cache_set(key_id, decoding_key);
                        }
                        let val_retry = cache.cache_get(key);
                        match val_retry {
                            Some(val_retry) => Ok(val_retry.clone()),
                            None => Err(AuthError::JwkShape("Invalid kid".to_owned())),
                        }
                    }
                    Err(e) => Err(e),
                }
            }
        }
    }
}

async fn fetch_jwk_keys(
    provider: &AuthProvider,
) -> Result<HashMap<String, DecodingKey>, AuthError> {
    match &provider {
        AuthProvider::AwsCognito(provider) => fetch_jwk_oidc_keys(&provider.jwk_uri).await,
        AuthProvider::GenericOidc(provider) => fetch_jwk_oidc_keys(&provider.jwk_uri).await,
    }
}

// We don't want to fetch keys on every authentication attempt, so cache the
// results. TODO: implement periodic refresh
async fn fetch_jwk_oidc_keys(url: &String) -> Result<HashMap<String, DecodingKey>, AuthError> {
    let client = awc::Client::new();

    let res = client.get(url).send().await;
    if let Err(e) = &res {
        debug!("JWK fetch request failed: {:?}", e);
    }

    let keys_as_json = res?.json::<Value>().await;

    match keys_as_json {
        Ok(value) => {
            let filtered = value
                .get("keys")
                .ok_or_else(|| {
                    debug!("JWK response missing 'keys' field");
                    AuthError::JwkShape("Missing keys field".to_owned())
                })?
                .as_array()
                .ok_or_else(|| {
                    debug!("JWK 'keys' field is not an array");
                    AuthError::JwkShape("keys field was not an array".to_owned())
                })?
                .iter()
                // Standard OIDC JWK endpoints should return keys for RS256 signature verification.
                // This filter ensures we only use appropriate keys for our validation
                .filter_map(|val| check_key_as_str("alg", "RS256", val))
                .filter_map(|val| check_key_as_str("use", "sig", val));

            let mut ret = HashMap::new();
            for json_value in filtered {
                let kid = validate_field_is_str("kid", json_value).ok_or_else(|| {
                    debug!("JWK entry missing 'kid' field");
                    AuthError::JwkShape("Could not extract 'kid' field".to_owned())
                })?;
                let n = validate_field_is_str("n", json_value).ok_or_else(|| {
                    debug!("JWK entry missing 'n' field");
                    AuthError::JwkShape("Could not extract 'n' field".to_owned())
                })?;
                let e = validate_field_is_str("e", json_value).ok_or_else(|| {
                    debug!("JWK entry missing 'e' field");
                    AuthError::JwkShape("Could not extract 'e' field".to_owned())
                })?;
                let decoding_key = DecodingKey::from_rsa_components(n, e).map_err(|e| {
                    debug!("Failed to create decoding key: {}", e);
                    AuthError::JwkShape(format!("Invalid JWK decoding key: {}", e))
                })?;
                ret.insert(kid.to_owned(), decoding_key);
            }
            Ok(ret)
        }
        Err(JsonPayloadError::Deserialize(json_error)) => {
            debug!("Failed to deserialize JWK response: {}", json_error);
            Err(AuthError::JwkShape(json_error.to_string()))
        }
        Err(JsonPayloadError::Payload(payload)) => {
            debug!("JWK payload error: {:?}", payload);
            Err(AuthError::JwkPayload(payload))
        }
        Err(JsonPayloadError::ContentType) => {
            debug!("JWK response has invalid content type");
            Err(AuthError::JwkContentType)
        }
    }
}

fn check_key_as_str<'a>(key: &str, check: &str, json: &'a Value) -> Option<&'a Value> {
    if let Some(value) = validate_field_is_str(key, json) {
        if value == check {
            return Some(json);
        }
    }
    debug!(
        "Skipping JWK key because it did not match the required shape {} {}",
        key, json
    );
    None
}

fn validate_field_is_str<'a>(key: &str, json: &'a Value) -> Option<&'a str> {
    let value = json.get(key);
    if let Some(value) = value {
        if let Some(value) = value.as_str() {
            return Some(value);
        }
    }
    None
}

// Fetch keys on every authentication attempt, so cache the
// results.
async fn validate_api_keys(
    db: &StoragePostgres,
    api_key: &str,
) -> Result<(TenantId, Vec<ApiPermission>), DBError> {
    db.validate_api_key(api_key).await
}

const API_KEY_LENGTH: usize = 128;
pub const API_KEY_PREFIX: &str = "apikey:";

/// HTTP header name for tenant selection in multi-tenant deployments
pub const TENANT_HEADER: &str = "Feldera-Tenant";

/// Generates a random 128 character API key
pub fn generate_api_key() -> String {
    assert_impl_any!(ThreadRng: rand::CryptoRng);
    let key: String = rand::thread_rng()
        .sample_iter(Alphanumeric)
        .take(API_KEY_LENGTH)
        .map(char::from)
        .collect();
    format!("{API_KEY_PREFIX}{key}") // the prefix is part of the public API.
}

#[cfg(test)]
mod test {
    use std::sync::Arc;

    use actix_http::{HttpMessage, StatusCode};
    use actix_web::{
        body::{BoxBody, EitherBody},
        dev::ServiceResponse,
        http::{self},
        test, web, App, HttpRequest, HttpResponse,
    };
    use actix_web_httpauth::middleware::HttpAuthentication;
    use base64::Engine;
    use cached::Cached;
    use chrono::Utc;
    use jsonwebtoken::{encode, Algorithm, DecodingKey, EncodingKey, Header, Validation};
    use tokio::sync::{Mutex, RwLock};
    use uuid::Uuid;

    use super::AuthError;
    use crate::db::types::api_key::ApiPermission;
    use crate::{
        api::main::ServerState,
        auth::{self, AuthConfiguration, AuthProvider, AwsCognitoClaim},
        config::ApiServerConfig,
        db::storage::Storage,
        ensure_default_crypto_provider,
    };
    use crate::{auth::fetch_jwk_oidc_keys, config::CommonConfig};

    async fn setup(claim: AwsCognitoClaim) -> (String, DecodingKey) {
        let rsa = openssl::rsa::Rsa::generate(2048).unwrap();
        let header = Header {
            typ: Some("JWT".to_owned()),
            alg: Algorithm::RS256,
            cty: None,
            jku: None,
            jwk: None,
            kid: Some("rsa01".to_owned()),
            x5u: None,
            x5c: None,
            x5t: None,
            x5t_s256: None,
        };

        let token_encoded = encode(
            &header,
            &claim,
            &EncodingKey::from_rsa_pem(&rsa.private_key_to_pem().unwrap()).unwrap(),
        )
        .unwrap();
        let decoding_key = DecodingKey::from_rsa_pem(&rsa.public_key_to_pem().unwrap()).unwrap();
        let token = token_encoded.as_str();
        (token.to_owned(), decoding_key)
    }

    fn validation(aud: &str, iss: &str) -> Validation {
        let mut validation = Validation::new(Algorithm::RS256);
        validation.set_audience(&[aud]);
        validation.set_issuer(&[iss]);
        validation
    }

    fn default_claim() -> AwsCognitoClaim {
        AwsCognitoClaim {
            client_id: "some-client".to_owned(),
            exp: Utc::now().timestamp() + 1000,
            iat: Utc::now().timestamp() + 1000,
            iss: "some-iss".to_owned(),
            sub: "some-sub".to_owned(),
            jti: "some-jti".to_owned(),
            origin_jti: Some("some-origin-jti".to_owned()),
            scope: "".to_owned(),
            token_use: "access".to_owned(),
            username: "some-user".to_owned(),
            tenant: None,
            tenants: None,
        }
    }

    async fn run_test(
        req: actix_http::Request,
        decoding_key: Option<DecodingKey>,
        api_key: Option<String>,
        validation: Validation,
    ) -> ServiceResponse<EitherBody<BoxBody>> {
        let client_id = validation
            .aud
            .as_ref()
            .unwrap()
            .iter()
            .next()
            .unwrap()
            .clone();
        let config = AuthConfiguration {
            provider: AuthProvider::AwsCognito(auth::ProviderAwsCognito {
                issuer: "some-iss".to_string(),
                jwk_uri: "some-url".to_string(),
                login_url: "some".to_string(),
                logout_url: "some".to_string(),
            }),
            validation,
            client_id,
        };
        let closure = auth::auth_validator;
        let auth_middleware = HttpAuthentication::with_fn(closure);

        let common_config = CommonConfig {
            bind_address: "127.0.0.1".to_string(),
            api_port: 0,
            compiler_host: "127.0.0.1".to_string(),
            compiler_port: 8085,
            runner_host: "127.0.0.1".to_string(),
            runner_port: 8089,
            platform_version: "v0".to_string(),
            http_workers: 1,
            unstable_features: None,
            enable_https: false,
            https_tls_cert_path: None,
            https_tls_key_path: None,
        };

        let manager_config = ApiServerConfig {
            auth_provider: crate::config::AuthProviderType::AwsCognito,
            dev_mode: false,
            dump_openapi: false,
            allowed_origins: None,
            demos_dir: vec![],
            telemetry: "".to_owned(),
            support_data_collection_frequency: 15,
            support_data_retention: 3,
            authorized_groups: vec![],
            individual_tenant: true,
            issuer_tenant: false,
            auth_audience: "feldera-api".to_string(),
        };

        let (conn, _temp) = crate::db::test::setup_pg().await;
        if api_key.is_some() {
            let tenant_id = conn
                .get_or_create_tenant_id(
                    Uuid::now_v7(),
                    "some-name".to_string(),
                    "some-provider".to_string(),
                )
                .await
                .unwrap();
            conn.store_api_key_hash(
                tenant_id,
                Uuid::now_v7(),
                "foo",
                &api_key.unwrap(),
                vec![ApiPermission::Read, ApiPermission::Write],
            )
            .await
            .unwrap();
        }
        let db = Arc::new(Mutex::new(conn));
        let state = actix_web::web::Data::new(
            ServerState::new(
                common_config,
                manager_config,
                db,
                Arc::new(RwLock::new(None)),
                Arc::new(RwLock::new(None)),
            )
            .await
            .unwrap(),
        );
        if decoding_key.is_some() {
            state
                .jwk_cache
                .lock()
                .await
                .cache
                .cache_set("rsa01".to_owned(), decoding_key.unwrap());
        }
        let app = App::new()
            .app_data(state)
            .app_data(config)
            .wrap(auth_middleware)
            .route(
                "/",
                web::get().to(|req: HttpRequest| async move {
                    {
                        let ext = req.extensions();
                        let permissions = ext.get::<Vec<ApiPermission>>().unwrap();
                        assert_eq!(
                            *permissions,
                            vec![ApiPermission::Read, ApiPermission::Write]
                        );
                    }
                    HttpResponse::build(StatusCode::OK).await
                }),
            );
        let app = test::init_service(app).await;

        test::call_service(&app, req).await
    }

    #[actix_web::test]
    async fn invalid_url() {
        ensure_default_crypto_provider();
        let url = "http://localhost/doesnotexist".to_owned();
        let res = fetch_jwk_oidc_keys(&url).await;
        assert!(matches!(res.err().unwrap(), AuthError::JwkFetch(_)));
    }

    #[tokio::test]
    async fn valid_token() {
        let claim = default_claim();
        let validation = validation("some-client", "some-iss");
        let (token, decoding_key) = setup(claim).await;
        let req = test::TestRequest::get()
            .uri("/")
            .insert_header((http::header::AUTHORIZATION, format!("Bearer {}", token)))
            .to_request();
        let res = run_test(req, Some(decoding_key), None, validation).await;
        assert_eq!(200, res.status());
    }

    #[tokio::test]
    async fn expired_token() {
        let mut claim = default_claim();
        claim.exp = Utc::now().timestamp() - 10000;
        let validation = validation("some-client", "some-iss");
        let (token, decoding_key) = setup(claim).await;
        let req = test::TestRequest::get()
            .uri("/")
            .insert_header((http::header::AUTHORIZATION, format!("Bearer {}", token)))
            .to_request();
        let res = run_test(req, Some(decoding_key), None, validation).await;
        assert_eq!(401, res.status());
        assert_eq!(
            "Bearer error_description=\"ExpiredSignature\"",
            res.headers().get("www-authenticate").unwrap()
        );
    }

    #[tokio::test]
    async fn non_access_use_token() {
        let mut claim = default_claim();
        claim.token_use = "sig".to_owned();
        let validation = validation("some-client", "some-iss");
        let (token, decoding_key) = setup(claim).await;
        let req = test::TestRequest::get()
            .uri("/")
            .insert_header((http::header::AUTHORIZATION, format!("Bearer {}", token)))
            .to_request();
        let res = run_test(req, Some(decoding_key), None, validation).await;
        assert_eq!(401, res.status());
        assert_eq!(
            "Bearer error_description=\"InvalidToken\"",
            res.headers().get("www-authenticate").unwrap()
        );
    }

    #[tokio::test]
    async fn different_key() {
        let claim = default_claim();
        let validation = validation("some-client", "some-iss");
        let (token, _) = setup(claim.clone()).await;
        let (_, decoding_key) = setup(claim).await; // force a key change
        let req = test::TestRequest::get()
            .uri("/")
            .insert_header((http::header::AUTHORIZATION, format!("Bearer {}", token)))
            .to_request();
        let res = run_test(req, Some(decoding_key), None, validation).await;
        assert_eq!(401, res.status());
        assert_eq!(
            "Bearer error_description=\"InvalidSignature\"",
            res.headers().get("www-authenticate").unwrap()
        );
    }

    #[tokio::test]
    async fn different_client() {
        let claim = default_claim();
        let validation = validation("some-other-client", "some-iss");
        let (token, decoding_key) = setup(claim).await;
        let req = test::TestRequest::get()
            .uri("/")
            .insert_header((http::header::AUTHORIZATION, format!("Bearer {}", token)))
            .to_request();
        let res = run_test(req, Some(decoding_key), None, validation).await;
        assert_eq!(401, res.status());
        assert_eq!(
            "Bearer error_description=\"InvalidAudience\"",
            res.headers().get("www-authenticate").unwrap()
        );
    }

    #[tokio::test]
    async fn different_iss() {
        let claim = default_claim();
        let validation = validation("some-client", "some-other-iss");
        let (token, decoding_key) = setup(claim).await;
        let req = test::TestRequest::get()
            .uri("/")
            .insert_header((http::header::AUTHORIZATION, format!("Bearer {}", token)))
            .to_request();
        let res = run_test(req, Some(decoding_key), None, validation).await;
        assert_eq!(401, res.status());
        assert_eq!(
            "Bearer error_description=\"InvalidIssuer\"",
            res.headers().get("www-authenticate").unwrap()
        );
    }

    #[tokio::test]
    async fn modified_token() {
        let claim = default_claim();
        let validation = validation("some-client", "some-other-iss");
        let (token, decoding_key) = setup(claim).await;

        // Modify the claim
        let base64_parts: Vec<&str> = token.split('.').collect();
        let claim_base64 = base64::engine::general_purpose::STANDARD_NO_PAD
            .decode(base64_parts.get(1).unwrap())
            .unwrap();
        let claim_str = std::str::from_utf8(&claim_base64).unwrap();
        let claim_str_modified = claim_str.replace("some-user", "some-other-user");
        let modified_base64_claim =
            base64::engine::general_purpose::STANDARD_NO_PAD.encode(claim_str_modified);
        let new_token = token.replace(base64_parts.get(1).unwrap(), modified_base64_claim.as_str());
        let req = test::TestRequest::get()
            .uri("/")
            .insert_header((http::header::AUTHORIZATION, format!("Bearer {}", new_token)))
            .to_request();
        let res = run_test(req, Some(decoding_key), None, validation).await;
        assert_eq!(401, res.status());
        assert_eq!(
            "Bearer error_description=\"InvalidSignature\"",
            res.headers().get("www-authenticate").unwrap()
        );
    }

    #[tokio::test]
    async fn valid_api_key() {
        let validation = validation("some-client", "some-iss");
        let api_key = auth::generate_api_key();
        let req = test::TestRequest::get()
            .uri("/")
            .insert_header((
                http::header::AUTHORIZATION,
                format!("Bearer {}", api_key.clone()),
            ))
            .to_request();
        let res = run_test(req, None, Some(api_key), validation).await;
        assert_eq!(200, res.status());
    }

    #[tokio::test]
    async fn test_extract_tenant_from_issuer() {
        use super::extract_tenant_from_issuer;

        let test_cases = vec![
            // Standard Okta domains
            (
                "https://acme-corp.okta.com/oauth2/default",
                Some("acme-corp.okta.com"),
            ),
            (
                "https://dev-123456.okta.com/oauth2/default",
                Some("dev-123456.okta.com"),
            ),
            (
                "https://my-company.okta.com/oauth2/custom",
                Some("my-company.okta.com"),
            ),
            // AWS Cognito domains
            (
                "https://cognito-idp.us-west-2.amazonaws.com/us-west-2_ABCDEFGH",
                Some("cognito-idp.us-west-2.amazonaws.com"),
            ),
            (
                "https://my-pool.auth.us-east-1.amazoncognito.com/",
                Some("my-pool.auth.us-east-1.amazoncognito.com"),
            ),
            // Google Identity domains
            ("https://accounts.google.com", Some("accounts.google.com")),
            (
                "https://oauth2.googleapis.com/token",
                Some("oauth2.googleapis.com"),
            ),
            // Custom domains
            (
                "https://auth.company.com/oauth2/v1",
                Some("auth.company.com"),
            ),
            (
                "https://login.enterprise.org/oidc",
                Some("login.enterprise.org"),
            ),
            // Domain with hyphen and numbers
            (
                "https://tenant-123.auth-provider.com/v2/token",
                Some("tenant-123.auth-provider.com"),
            ),
            (
                "https://dev-env-staging.example.com/auth",
                Some("dev-env-staging.example.com"),
            ),
            // Edge cases that should return None
            ("", None),
            ("invalid-url", None),
            ("http://", None),
            ("https://", None),
            // URLs without protocol
            ("domain.com/path", None),
            ("//domain.com/path", None),
            // IP addresses (should extract full IP as tenant)
            ("https://192.168.1.100/auth", Some("192.168.1.100")),
            ("http://127.0.0.1:8080/token", Some("127.0.0.1")),
            // Localhost
            ("http://localhost:3000/auth", Some("localhost")),
            ("https://localhost/oauth2/default", Some("localhost")),
            // Single word domains
            ("https://auth/token", Some("auth")),
            ("http://provider/oauth", Some("provider")),
            // Domains with ports (port should not be included in hostname)
            (
                "https://tenant.example.com:8443/auth",
                Some("tenant.example.com"),
            ),
            (
                "http://dev-server.local:3000/oauth2",
                Some("dev-server.local"),
            ),
            // Very long subdomain
            (
                "https://very-long-tenant-name-with-many-hyphens.auth.example.com/oauth2",
                Some("very-long-tenant-name-with-many-hyphens.auth.example.com"),
            ),
        ];

        for (input, expected) in test_cases {
            let result = extract_tenant_from_issuer(input);
            assert_eq!(
                result,
                expected.map(|s| s.to_string()),
                "Failed for input: {}",
                input
            );
        }
    }
}
