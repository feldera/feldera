//! Keeping a user's profile current from the provider's UserInfo endpoint.
//!
//! An access token is a poor place to read a person's name and email from. The
//! claims a provider puts there vary, and some put none: an AWS Cognito access
//! token carries neither, however many scopes it was granted. The UserInfo
//! endpoint (OpenID Connect Core 1.0 §5.3) is where a provider answers those
//! questions for the identity a token belongs to, so that is where Feldera asks.
//!
//! Feldera authenticates every request on its own and holds no session, so
//! there is no "login" moment to hang the fetch on. The token's `auth_time`
//! claim supplies one: it records when the user actually authenticated and
//! survives token refresh, so a token bearing a newer `auth_time` than the last
//! refresh covered marks a fresh login, which is when a changed email appears.
//! Providers that publish no `auth_time` fall back to
//! [`PROFILE_REFRESH_TTL_SECONDS`].

use crate::db::types::user::UserProfile;
use crate::oidc::destination::{OidcUrlError, validate_tenant_oidc_url};
use crate::oidc::fetch::{OidcDestination, fetch_discovery_document, oidc_http_client};
use cached::{Cached, TimedSizedCache};
use reqwest::{Certificate, StatusCode};
use serde::Deserialize;
use std::fmt;

/// How long a refresh attempt stands before the next one is due. Applies to
/// attempts that found nothing and to those that failed, so an unreachable or
/// UserInfo-less provider costs one request a day per user rather than one per
/// login request.
pub const PROFILE_REFRESH_TTL_SECONDS: u64 = 24 * 60 * 60;

/// How many identities and issuers the refresh bookkeeping tracks. Evicting an
/// entry only costs a redundant refresh, so this bounds memory rather than
/// correctness.
const USER_PROFILE_CACHE_CAPACITY: usize = 4096;

/// A provider's answer for the identity behind an access token.
#[derive(Deserialize)]
struct UserInfoResponse {
    /// Identifies whose profile this is. Verified against the token's own
    /// subject before anything is stored (OpenID Connect Core 1.0 §5.3.2).
    sub: String,
    email: Option<String>,
    #[serde(default, deserialize_with = "deserialize_bool_or_string")]
    email_verified: bool,
    name: Option<String>,
    preferred_username: Option<String>,
}

impl From<UserInfoResponse> for UserProfile {
    fn from(response: UserInfoResponse) -> UserProfile {
        UserProfile {
            email: response.email,
            email_verified: response.email_verified,
            // `name` is the person's full name; `preferred_username` is
            // whatever handle the provider assigned, which for a federated
            // Cognito identity is a machine-generated string. Prefer the name.
            display_name: response.name.or(response.preferred_username),
        }
    }
}

/// `email_verified` is a boolean in OpenID Connect Core 1.0 §5.1, but AWS
/// Cognito answers with the strings `"true"` and `"false"`. Accept both. Any
/// other shape reads as unverified, because one odd field must not discard the
/// name and email alongside it.
pub(crate) fn deserialize_bool_or_string<'de, D>(deserializer: D) -> Result<bool, D::Error>
where
    D: serde::Deserializer<'de>,
{
    Ok(match serde_json::Value::deserialize(deserializer)? {
        serde_json::Value::Bool(verified) => verified,
        serde_json::Value::String(text) => text.parse::<bool>().unwrap_or(false),
        _ => false,
    })
}

#[derive(Debug)]
pub(crate) enum UserInfoError {
    /// The issuer publishes no `userinfo_endpoint`.
    NotOffered,
    Request(reqwest::Error),
    Status(StatusCode),
    /// The endpoint is not a destination this issuer may name.
    Destination(OidcUrlError),
    /// The answer describes a different identity than the token does. A
    /// provider that does this is either broken or being impersonated; either
    /// way the profile must not be stored against this user.
    SubjectMismatch,
}

impl fmt::Display for UserInfoError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            UserInfoError::NotOffered => f.write_str("issuer publishes no userinfo_endpoint"),
            UserInfoError::Request(e) => write!(f, "UserInfo request failed: {e}"),
            UserInfoError::Status(status) => write!(f, "UserInfo answered {status}"),
            UserInfoError::Destination(e) => {
                write!(f, "userinfo_endpoint is not a permitted destination: {e}")
            }
            UserInfoError::SubjectMismatch => {
                f.write_str("UserInfo answered for a different subject than the token names")
            }
        }
    }
}

impl std::error::Error for UserInfoError {}

/// Read the profile the provider holds for the identity behind `access_token`.
///
/// The endpoint comes out of the issuer's discovery document, which the issuer
/// controls, so a tenant-registered issuer is held to the same destination
/// policy here as its `jwks_uri` is in [`crate::oidc::fetch::fetch_issuer_jwks`].
pub(crate) async fn fetch_user_profile(
    userinfo_endpoint: &str,
    subject: &str,
    access_token: &str,
    destination: OidcDestination,
    extra_roots: &[Certificate],
) -> Result<UserProfile, UserInfoError> {
    if let OidcDestination::TenantRegistered(policy) = destination {
        validate_tenant_oidc_url(userinfo_endpoint, policy).map_err(UserInfoError::Destination)?;
    }
    let response = oidc_http_client(destination, extra_roots)
        .map_err(UserInfoError::Request)?
        .get(userinfo_endpoint)
        .bearer_auth(access_token)
        .send()
        .await
        .map_err(UserInfoError::Request)?;
    if !response.status().is_success() {
        return Err(UserInfoError::Status(response.status()));
    }
    let info: UserInfoResponse = response.json().await.map_err(UserInfoError::Request)?;
    if info.sub != subject {
        return Err(UserInfoError::SubjectMismatch);
    }
    Ok(info.into())
}

/// Which identities have had a recent refresh, and where each issuer's UserInfo
/// endpoint lives.
pub(crate) struct UserProfileCache {
    /// `(provider, subject)` to the token `auth_time` the last attempt covered,
    /// `None` when that token carried none. Entries expire after
    /// [`PROFILE_REFRESH_TTL_SECONDS`], which is what makes a refresh due again
    /// for a provider that publishes no `auth_time`.
    attempts: TimedSizedCache<(String, String), Option<i64>>,
    /// Issuer to its `userinfo_endpoint`, `None` when it publishes none. Cached
    /// for the same span, so a provider that gains one is picked up within a
    /// day without a discovery fetch per login.
    endpoints: TimedSizedCache<String, Option<String>>,
}

impl UserProfileCache {
    pub(crate) fn new() -> Self {
        Self {
            attempts: TimedSizedCache::with_size_and_lifespan(
                USER_PROFILE_CACHE_CAPACITY,
                PROFILE_REFRESH_TTL_SECONDS,
            ),
            endpoints: TimedSizedCache::with_size_and_lifespan(
                USER_PROFILE_CACHE_CAPACITY,
                PROFILE_REFRESH_TTL_SECONDS,
            ),
        }
    }

    /// Whether a refresh is due for this identity, recording the attempt when it
    /// is. Claiming and recording in one step under the caller's lock is what
    /// keeps a burst of parallel requests from all fetching the same profile.
    pub(crate) fn claim_refresh(
        &mut self,
        provider: &str,
        subject: &str,
        auth_time: Option<i64>,
    ) -> bool {
        let key = (provider.to_string(), subject.to_string());
        let due = match self.attempts.cache_get(&key) {
            // Within the span of the last attempt: only evidence that the user
            // authenticated again since then justifies asking the provider.
            Some(covered) => match (auth_time, covered) {
                (Some(now), Some(then)) => now > *then,
                (Some(_), None) => true,
                (None, _) => false,
            },
            None => true,
        };
        if due {
            self.attempts.cache_set(key, auth_time);
        }
        due
    }

    fn cached_endpoint(&mut self, issuer: &str) -> Option<Option<String>> {
        self.endpoints.cache_get(&issuer.to_string()).cloned()
    }

    fn remember_endpoint(&mut self, issuer: &str, endpoint: Option<String>) {
        self.endpoints.cache_set(issuer.to_string(), endpoint);
    }
}

impl Default for UserProfileCache {
    fn default() -> Self {
        Self::new()
    }
}

/// The issuer's UserInfo endpoint, from cache or a discovery fetch.
pub(crate) async fn resolve_userinfo_endpoint(
    cache: &tokio::sync::Mutex<UserProfileCache>,
    issuer: &str,
    destination: OidcDestination,
    extra_roots: &[Certificate],
) -> Result<String, UserInfoError> {
    if let Some(cached) = cache.lock().await.cached_endpoint(issuer) {
        return cached.ok_or(UserInfoError::NotOffered);
    }
    // Discovery runs without the cache lock held, so a slow issuer cannot
    // serialize every login.
    let endpoint = fetch_discovery_document(issuer, destination, extra_roots)
        .await
        .map_err(UserInfoError::Request)?
        .userinfo_endpoint;
    cache
        .lock()
        .await
        .remember_endpoint(issuer, endpoint.clone());
    endpoint.ok_or(UserInfoError::NotOffered)
}

#[cfg(test)]
mod test {
    use super::{
        UserInfoError, UserInfoResponse, UserProfileCache, fetch_user_profile,
        resolve_userinfo_endpoint,
    };
    use crate::db::types::user::UserProfile;
    use crate::oidc::fetch::OidcDestination;
    use tokio::sync::Mutex;
    use wiremock::matchers::{method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    fn parse(json: &str) -> UserProfile {
        serde_json::from_str::<UserInfoResponse>(json)
            .unwrap()
            .into()
    }

    /// A provider serving discovery, and UserInfo when `userinfo` is given.
    /// `discovery_calls` bounds how often discovery may be fetched.
    async fn provider(userinfo: Option<serde_json::Value>, discovery_calls: u64) -> MockServer {
        crate::ensure_default_crypto_provider();
        let server = MockServer::start().await;
        let mut document = serde_json::json!({
            "issuer": server.uri(),
            "jwks_uri": format!("{}/jwks", server.uri()),
        });
        if let Some(body) = userinfo {
            document["userinfo_endpoint"] = format!("{}/userinfo", server.uri()).into();
            Mock::given(method("GET"))
                .and(path("/userinfo"))
                .respond_with(ResponseTemplate::new(200).set_body_json(body))
                .mount(&server)
                .await;
        }
        Mock::given(method("GET"))
            .and(path("/.well-known/openid-configuration"))
            .respond_with(ResponseTemplate::new(200).set_body_json(document))
            .expect(discovery_calls)
            .mount(&server)
            .await;
        server
    }

    /// The endpoint is discovered once and reused, and the provider's answer
    /// lands as a profile.
    #[tokio::test]
    async fn a_discovered_endpoint_answers_with_the_profile() {
        let server = provider(
            Some(serde_json::json!({
                "sub": "ada",
                "email": "ada@example.com",
                // The spelling AWS Cognito uses.
                "email_verified": "true",
                "name": "Ada Lovelace",
            })),
            1,
        )
        .await;
        let cache = Mutex::new(UserProfileCache::new());
        let destination = OidcDestination::OperatorConfigured;

        let endpoint = resolve_userinfo_endpoint(&cache, &server.uri(), destination, &[])
            .await
            .unwrap();
        let profile = fetch_user_profile(&endpoint, "ada", "token", destination, &[])
            .await
            .unwrap();
        assert_eq!(profile.email.as_deref(), Some("ada@example.com"));
        assert!(profile.email_verified);
        assert_eq!(profile.display_name.as_deref(), Some("Ada Lovelace"));

        // The second resolution is served from the cache; `expect(1)` on the
        // discovery mock fails the test on drop if it is not.
        assert_eq!(
            resolve_userinfo_endpoint(&cache, &server.uri(), destination, &[])
                .await
                .unwrap(),
            endpoint
        );
    }

    /// An answer about another identity is refused, however well-formed.
    #[tokio::test]
    async fn a_profile_for_another_subject_is_refused() {
        let server = provider(
            Some(serde_json::json!({"sub": "someone-else", "email": "ada@example.com"})),
            1,
        )
        .await;
        let cache = Mutex::new(UserProfileCache::new());
        let destination = OidcDestination::OperatorConfigured;

        let endpoint = resolve_userinfo_endpoint(&cache, &server.uri(), destination, &[])
            .await
            .unwrap();
        assert!(matches!(
            fetch_user_profile(&endpoint, "ada", "token", destination, &[]).await,
            Err(UserInfoError::SubjectMismatch)
        ));
    }

    /// A provider offering no UserInfo endpoint says so once, then from cache,
    /// so it is not rediscovered on every login.
    #[tokio::test]
    async fn a_provider_without_userinfo_is_asked_once() {
        let server = provider(None, 1).await;
        let cache = Mutex::new(UserProfileCache::new());
        let destination = OidcDestination::OperatorConfigured;

        for _ in 0..3 {
            assert!(matches!(
                resolve_userinfo_endpoint(&cache, &server.uri(), destination, &[]).await,
                Err(UserInfoError::NotOffered)
            ));
        }
    }

    #[test]
    fn email_verified_accepts_a_boolean_or_a_string() {
        assert!(parse(r#"{"sub":"s","email":"a@b.c","email_verified":true}"#).email_verified);
        assert!(!parse(r#"{"sub":"s","email":"a@b.c","email_verified":"false"}"#).email_verified);
    }

    #[test]
    fn an_absent_or_odd_email_verified_reads_as_unverified_without_losing_the_rest() {
        for json in [
            r#"{"sub":"s","email":"a@b.c","name":"A B"}"#,
            r#"{"sub":"s","email":"a@b.c","name":"A B","email_verified":7}"#,
            r#"{"sub":"s","email":"a@b.c","name":"A B","email_verified":"yes"}"#,
        ] {
            let profile = parse(json);
            assert!(!profile.email_verified, "{json}");
            assert_eq!(profile.email.as_deref(), Some("a@b.c"), "{json}");
        }
    }

    #[test]
    fn the_full_name_wins_over_the_provider_assigned_handle() {
        let both =
            parse(r#"{"sub":"s","name":"Ada Lovelace","preferred_username":"federated_ada"}"#);
        assert_eq!(both.display_name.as_deref(), Some("Ada Lovelace"));

        let handle_only = parse(r#"{"sub":"s","preferred_username":"federated_ada"}"#);
        assert_eq!(handle_only.display_name.as_deref(), Some("federated_ada"));
    }

    #[test]
    fn a_claimed_refresh_is_not_due_again_until_the_user_authenticates_anew() {
        let mut cache = UserProfileCache::new();
        assert!(cache.claim_refresh("iss", "sub", Some(100)));
        assert!(!cache.claim_refresh("iss", "sub", Some(100)));
        assert!(cache.claim_refresh("iss", "sub", Some(200)));
        assert!(!cache.claim_refresh("iss", "sub", Some(200)));
        // A different identity is tracked on its own.
        assert!(cache.claim_refresh("iss", "other", Some(100)));
    }

    #[test]
    fn without_auth_time_one_refresh_stands_for_the_whole_span() {
        let mut cache = UserProfileCache::new();
        assert!(cache.claim_refresh("iss", "sub", None));
        assert!(!cache.claim_refresh("iss", "sub", None));
        // A token that does name an authentication time is new evidence.
        assert!(cache.claim_refresh("iss", "sub", Some(1)));
    }
}
