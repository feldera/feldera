//! Support HTTP bearer authentication to the pipeline manager API. The plan is
//! to support different providers down the line, but for now, we've tested
//! against client claims made via AWS Cognito.
//!
//! The expected workflow is for users to login via a browser to receive a JWT
//! access token. Clients then issue pipeline manager APIs using an HTTP
//! authorization header for bearer tokens (Authorization: Bearer <token>).
//!
//! This file implements an actix-web middleware to validate these tokens. Token
//! validation checks for many things, including signing algorithm, expiry
//! (exp), whether the client_id and issuers (iss) line up, whether the
//! signature is valid and whether the token was modified after being
//! signed. For signature verification, we fetch the provider's JWK keys from a
//! well known URL and cache them locally. We don't yet refresh JWK keys (e.g.,
//! when the clients report using a different kid), but for now, a restart of
//! the pipeline manager suffices.
//!
//! To support this workflow, we introduce three environment variables that the
//! pipeline manager needs for the OAuth protocol: the client ID, the issuer ID,
//! and the well known URL for fetching JWK keys.

use std::{collections::HashMap, env};

use actix_web::{dev::ServiceRequest, web::Data};
use actix_web_httpauth::extractors::{
    bearer::{BearerAuth, Config},
    AuthenticationError,
};
use cached::{Cached, TimedCache};
use jsonwebtoken::{decode, decode_header, Algorithm, DecodingKey, TokenData, Validation};
use log::{error, info};
use rand::{distributions::Alphanumeric, Rng};
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::{
    db::{storage::Storage, ProjectDB, Scopes},
    ServerState,
};

/// Authorization using a bearer token. This is strictly used for authorizing
/// users, not machines.
pub(crate) async fn auth_validator(
    configuration: AuthConfiguration,
    req: ServiceRequest,
    credentials: Option<BearerAuth>,
) -> Result<ServiceRequest, (actix_web::error::Error, ServiceRequest)> {
    // First check if we have a bearer token. If not, we expect an API key.
    match credentials {
        Some(credentials) => {
            let token = credentials.token();
            let token = match configuration.provider {
                Provider::AwsCognito(_) => {
                    decode_aws_cognito_token(token, &req, configuration).await
                }
            };
            match token {
                Ok(_) => Ok(req),
                Err(error) => {
                    let descr = match error {
                        // Do not bubble up internal errors to the user
                        AuthError::JwkFetch(e) => {
                            error!("JwkFetch: {:?}", e);
                            "Authentication failed".to_owned()
                        }
                        // Do not bubble up internal errors to the user
                        AuthError::JwkShape(e) => {
                            error!("JwkShapeError: {:?}", e);
                            "Authentication failed".to_owned()
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
        None => {
            let api_key = req.headers().get("x-api-key");
            match api_key {
                Some(api_key) if api_key.to_str().is_ok() => {
                    let api_key_str = api_key.to_str().unwrap();
                    let ad = req.app_data::<Data<ServerState>>();
                    let validate = {
                        let db = &ad.unwrap().db.lock().await;
                        validate_api_keys(db, api_key_str).await
                    };
                    // TODO: set scopes on request
                    match validate {
                        Ok(_) => Ok(req),
                        Err(_) => {
                            let config = req.app_data::<Config>().cloned().unwrap_or_default();
                            Err((
                                AuthenticationError::from(config)
                                    .with_error_description("Unauthorized API key")
                                    .into(),
                                req,
                            ))
                        }
                    }
                }
                _ => {
                    let config = req.app_data::<Config>().cloned().unwrap_or_default();
                    return Err((
                        AuthenticationError::from(config)
                            .with_error_description("Missing bearer token or API key")
                            .into(),
                        req,
                    ));
                }
            }
        }
    }
}

#[derive(Debug)]
enum Claim {
    AwsCognito(TokenData<AwsCognitoClaim>),
}

#[derive(Clone)]
pub(crate) enum Provider {
    AwsCognito(String), // The argument is the URL to use for fetching JWKs
}

pub(crate) fn aws_auth_config() -> AuthConfiguration {
    let mut validation = Validation::new(Algorithm::RS256);
    let audience =
        env::var("OAUTH_CLIENT_ID").expect("Missing environment variable OAUTH_CLIENT_ID");
    let iss = env::var("OAUTH_ISSUER").expect("Missing environment variable OAUTH_ISSUER");
    let jwk_uri = env::var("OAUTH_JWK_URI").expect("Missing environment variable OAUTH_JWK_URI");
    validation.set_audience(&[audience]);
    validation.set_issuer(&[iss]);
    AuthConfiguration {
        provider: Provider::AwsCognito(jwk_uri),
        validation,
    }
}

#[derive(Clone)]
// Expected issuer and client_id for each authentication request
pub(crate) struct AuthConfiguration {
    pub provider: Provider,
    pub validation: Validation,
}

///
/// The shape of a claim provided to clients by AwsCognito, following
/// the guide below:
///
/// https://docs.aws.amazon.com/cognito/latest/developerguide/amazon-cognito-user-pools-using-the-access-token.html
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
    origin_jti: String,

    /// OAuth 2.0 scopes
    scope: String,

    /// Purpose of the token. For the purpose of bearer authentication,
    /// this value should always be "access"
    token_use: String,

    /// The username. Note: this may not be unique within a user pool.
    /// The sub claim is the appropriate identifier for a user.
    username: String,
}

#[derive(Debug)]
enum AuthError {
    JwtDecoding(jsonwebtoken::errors::Error),
    JwkFetch(reqwest::Error),
    JwkShape(String),
}

impl std::fmt::Display for AuthError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            AuthError::JwtDecoding(err) => err.fmt(f),
            AuthError::JwkFetch(err) => err.fmt(f),
            AuthError::JwkShape(err) => err.fmt(f),
        }
    }
}

impl From<reqwest::Error> for AuthError {
    fn from(value: reqwest::Error) -> Self {
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

///
/// Follows the guidelines in the following links, except that JWK refreshes are
/// not yet implemented
///
/// https://docs.aws.amazon.com/cognito/latest/developerguide/amazon-cognito-user-pools-using-the-access-token.html
/// JWT claims: https://datatracker.ietf.org/doc/html/rfc7519#section-4
async fn decode_aws_cognito_token(
    token: &str,
    req: &ServiceRequest,
    configuration: AuthConfiguration,
) -> Result<Claim, AuthError> {
    let header = decode_header(token);
    match header {
        Ok(header) => match header.alg {
            // AWS Cognito user pools use RS256
            Algorithm::RS256 => {
                if header.kid.is_none() {
                    return Err(jsonwebtoken::errors::ErrorKind::InvalidToken.into());
                }
                let state = req.app_data::<Data<ServerState>>().unwrap();
                let cache = &mut state.jwk_cache.lock().await;
                let jwk = cache.get(&header.kid.unwrap(), &configuration).await?;
                let token_data = decode::<AwsCognitoClaim>(token, &jwk, &configuration.validation);
                if let Ok(t) = &token_data {
                    // TODO: aud and client_id may not be the same when using a resource server
                    if !&configuration
                        .validation
                        .aud
                        .unwrap() // We create the validation object, so it's an error to not have aud set
                        .iter()
                        .any(|aud| *aud == t.claims.client_id)
                    {
                        return Err(jsonwebtoken::errors::ErrorKind::InvalidAudience.into());
                    }
                }
                match token_data {
                    Ok(data) => {
                        if data.claims.token_use != "access" {
                            return Err(jsonwebtoken::errors::ErrorKind::InvalidToken.into());
                        }
                        Ok(Claim::AwsCognito(data))
                    }
                    Err(jwt_error) => Err(jwt_error.into()),
                }
            }
            _ => Err(jsonwebtoken::errors::ErrorKind::InvalidAlgorithm.into()),
        },
        Err(e) => Err(e.into()),
    }
}

pub struct JwkCache {
    cache: TimedCache<String, DecodingKey>,
}

impl JwkCache {
    pub(crate) fn new() -> JwkCache {
        Self {
            cache: TimedCache::with_lifespan_and_capacity(120, 10),
        }
    }

    async fn get(
        &mut self,
        key: &String,
        configuration: &AuthConfiguration,
    ) -> Result<DecodingKey, AuthError> {
        let cache = &mut self.cache;
        let val = &cache.cache_get(key);
        match val {
            Some(dk) => Ok((*dk).clone()),
            None => {
                // TODO: Introduce a minimum delay between refreshes
                let fetched = fetch_jwk_keys(configuration).await;
                match fetched {
                    Ok(map) => {
                        for i in map {
                            cache.cache_set(i.0, i.1);
                        }
                        let val_retry = cache.cache_get(key);
                        return match val_retry {
                            Some(val_retry) => Ok(val_retry.clone()),
                            None => Err(AuthError::JwkShape("Invalid kid".to_owned())),
                        };
                    }
                    Err(e) => {
                        return Err(e);
                    }
                }; // refresh cache
            }
        }
    }
}

async fn fetch_jwk_keys(
    configuration: &AuthConfiguration,
) -> Result<HashMap<String, DecodingKey>, AuthError> {
    match &configuration.provider {
        Provider::AwsCognito(url) => fetch_jwk_aws_cognito_keys(url).await,
    }
}

// We don't want to fetch keys on every authentication attempt, so cache the
// results. TODO: implement periodic refresh
async fn fetch_jwk_aws_cognito_keys(
    url: &String,
) -> Result<HashMap<String, DecodingKey>, AuthError> {
    let res = reqwest::get(url).await;
    let res = res?.text().await?;
    let keys_as_json: Result<Value, serde_json::Error> = serde_json::from_str(&res);

    match keys_as_json {
        Ok(value) => {
            let filtered = value
                .get("keys")
                .ok_or(AuthError::JwkShape("Missing keys field".to_owned()))?
                .as_array()
                .ok_or(AuthError::JwkShape(
                    "keys field was not an array".to_owned(),
                ))?
                .iter()
                // While the AWS Cognito JWK endpoint shouldn't return keys
                // that aren't based on RS256 or meant for verifying signatures,
                // this guard should warn us when used with other auth providers later
                .filter_map(|val| check_key_as_str("alg", "RS256", val))
                .filter_map(|val| check_key_as_str("use", "sig", val));

            let mut ret = HashMap::new();
            for json_value in filtered {
                let kid = validate_field_is_str("kid", json_value).ok_or(AuthError::JwkShape(
                    "Could not extract 'kid' field".to_owned(),
                ))?;
                let n = validate_field_is_str("n", json_value).ok_or(AuthError::JwkShape(
                    "Could not extract 'n' field".to_owned(),
                ))?;
                let e = validate_field_is_str("e", json_value).ok_or(AuthError::JwkShape(
                    "Could not extract 'e' field".to_owned(),
                ))?;
                let decoding_key = DecodingKey::from_rsa_components(n, e)
                    .map_err(|e| AuthError::JwkShape(format!("Invalid JWK decoding key: {}", e)))?;
                ret.insert(kid.to_owned(), decoding_key);
            }
            Ok(ret)
        }
        Err(e) => Err(AuthError::JwkShape(e.to_string())),
    }
}

fn check_key_as_str<'a>(key: &str, check: &str, json: &'a Value) -> Option<&'a Value> {
    if let Some(value) = validate_field_is_str(key, json) {
        if value == check {
            return Some(json);
        }
    }
    info!(
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
// results. TODO: implement periodic refresh
async fn validate_api_keys(db: &ProjectDB, api_key: &str) -> Result<Vec<Scopes>, anyhow::Error> {
    db.validate_api_key(api_key.to_owned()).await
}

/// Generates a random 128 character API key
pub(crate) fn generate_api_key() -> String {
    rand::thread_rng()
        .sample_iter(Alphanumeric)
        .take(128)
        .map(char::from)
        .collect()
}

#[cfg(test)]
mod test {
    use std::{str::FromStr, sync::Arc};

    use actix_http::header::HeaderName;
    use actix_web::{
        body::{BoxBody, EitherBody},
        dev::ServiceResponse,
        http::{self},
        test, web, App,
    };
    use actix_web_httpauth::middleware::HttpAuthentication;
    use base64::Engine;
    use cached::Cached;
    use chrono::Utc;
    use jsonwebtoken::{encode, Algorithm, DecodingKey, EncodingKey, Header, Validation};
    use tokio::sync::Mutex;

    use crate::{
        auth::{self, fetch_jwk_aws_cognito_keys, AuthConfiguration, AwsCognitoClaim, Provider},
        config::ManagerConfig,
        db::{storage::Storage, Scopes},
        ServerState,
    };

    use super::AuthError;

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
            origin_jti: "some-origin-jti".to_owned(),
            scope: "".to_owned(),
            token_use: "access".to_owned(),
            username: "some-user".to_owned(),
        }
    }

    async fn run_test(
        req: actix_http::Request,
        decoding_key: Option<DecodingKey>,
        api_key: Option<String>,
        validation: Validation,
    ) -> ServiceResponse<EitherBody<BoxBody>> {
        let config = AuthConfiguration {
            provider: Provider::AwsCognito("some-url".to_string()),
            validation,
        };
        let config_copy = config.clone();
        let closure =
            move |req, bearer_auth| auth::auth_validator(config_copy.clone(), req, bearer_auth);
        let auth_middleware = HttpAuthentication::with_fn(closure);

        let manager_config = ManagerConfig {
            port: 0,
            bind_address: "0.0.0.0".to_owned(),
            logfile: None,
            working_directory: "".to_owned(),
            sql_compiler_home: "".to_owned(),
            dbsp_override_path: None,
            debug: false,
            unix_daemon: false,
            use_auth: true,
            db_connection_string: "postgres-embed".to_owned(),
            dump_openapi: false,
            config_file: None,
            initial_sql: None,
            dev_mode: false,
        };
        let (conn, _temp_dir) = crate::db::test::setup_embedded_pg().await;
        if api_key.is_some() {
            conn.store_api_key_hash(api_key.unwrap(), vec![Scopes::Read, Scopes::Write])
                .await
                .unwrap();
        }
        let db = Arc::new(Mutex::new(conn));
        let state = crate::WebData::new(ServerState::new(manager_config, db, None).await.unwrap());
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
            .wrap(auth_middleware)
            .route("/", web::get().to(|| async { "OK" }));
        let app = test::init_service(app).await;
        let resp = test::call_service(&app, req).await;
        resp
    }

    #[tokio::test]
    async fn invalid_url() {
        let url = "http://localhost/doesnotexist".to_owned();
        let res = fetch_jwk_aws_cognito_keys(&url).await;
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
        let base64_parts: Vec<&str> = token.split(".").collect();
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
            .insert_header((HeaderName::from_str("x-api-key").unwrap(), api_key.clone()))
            .to_request();
        let res = run_test(req, None, Some(api_key), validation).await;
        assert_eq!(200, res.status());
    }
}
