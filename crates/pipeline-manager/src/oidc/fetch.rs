//! Fetching an issuer's keys, and where those fetches may go.
//!
//! Federated authentication reaches the issuer named in a token before it can
//! verify the token's signature, so the fetch happens on behalf of whoever
//! presented it.

use crate::auth::{AuthError, parse_rsa_jwks};
use crate::oidc::destination::{TenantIssuerPolicy, is_public_ip, validate_tenant_oidc_url};
use jsonwebtoken::DecodingKey;
use serde::Deserialize;
use serde_json::Value;
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

#[derive(Deserialize)]
pub(crate) struct OidcDiscoveryDocument {
    pub jwks_uri: String,
    /// Absent for providers that publish no UserInfo endpoint. Optional in the
    /// discovery metadata (OpenID Connect Discovery 1.0 §3).
    #[serde(default)]
    pub userinfo_endpoint: Option<String>,
}

/// Timeout for OIDC discovery / JWKS HTTP requests.
const OIDC_FETCH_TIMEOUT_SECONDS: u64 = 10;

/// Who chose the issuer, which decides where its fetches may go.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub(crate) enum OidcDestination {
    /// The operator named this issuer at deploy time, as the login provider or
    /// in an owner trust, so it may sit on a private network.
    OperatorConfigured,
    /// A tenant administrator registered this issuer through the API. It must
    /// be https, and must resolve to a public address unless the operator
    /// permitted internal ones.
    TenantRegistered(TenantIssuerPolicy),
}

/// A [`reqwest`] resolver that drops every address outside [`is_public_ip`].
#[derive(Debug)]
pub(crate) struct PublicAddrsOnly;

impl reqwest::dns::Resolve for PublicAddrsOnly {
    fn resolve(&self, name: reqwest::dns::Name) -> reqwest::dns::Resolving {
        Box::pin(async move {
            // The port is irrelevant to resolution; reqwest substitutes the
            // real one on the addresses this returns.
            let permitted: Vec<SocketAddr> = tokio::net::lookup_host((name.as_str(), 0))
                .await?
                .filter(|addr| is_public_ip(addr.ip()))
                .collect();
            if permitted.is_empty() {
                return Err(format!(
                    "'{}' resolves to no address permitted by the OIDC destination policy",
                    name.as_str()
                )
                .into());
            }
            Ok(Box::new(permitted.into_iter()) as reqwest::dns::Addrs)
        })
    }
}

/// HTTP client for OIDC discovery / JWKS fetches: a short timeout and no
/// redirect following, to bound the blast radius of a slow or redirecting
/// issuer endpoint. A tenant-registered issuer additionally resolves through
/// [`PublicAddrsOnly`], so it cannot name an internal service.
pub(crate) fn oidc_http_client(
    destination: OidcDestination,
    extra_roots: &[reqwest::Certificate],
) -> Result<reqwest::Client, reqwest::Error> {
    let mut builder = reqwest::Client::builder()
        // rustls with the platform's roots, so which certificates an issuer may
        // present is decided the same way on every platform. The default
        // backend is whatever the target ships, and on macOS that store cannot
        // be pointed elsewhere.
        .use_rustls_tls()
        .timeout(Duration::from_secs(OIDC_FETCH_TIMEOUT_SECONDS))
        .redirect(reqwest::redirect::Policy::none());
    // An issuer may sit behind the deployment's own CA, which the web PKI does
    // not know. These add to the platform's roots rather than replacing them,
    // so a public provider still verifies.
    for root in extra_roots {
        builder = builder.add_root_certificate(root.clone());
    }
    match destination {
        OidcDestination::TenantRegistered(TenantIssuerPolicy::PublicHttpsOnly) => {
            builder.dns_resolver(Arc::new(PublicAddrsOnly)).build()
        }
        _ => builder.build(),
    }
}

/// Fetch an issuer's OIDC discovery document.
pub(crate) async fn fetch_discovery_document(
    issuer: &str,
    destination: OidcDestination,
    extra_roots: &[reqwest::Certificate],
) -> Result<OidcDiscoveryDocument, reqwest::Error> {
    let discovery_url = format!(
        "{}/.well-known/openid-configuration",
        issuer.trim_end_matches('/')
    );
    oidc_http_client(destination, extra_roots)?
        .get(&discovery_url)
        .send()
        .await?
        .json()
        .await
}

/// Fetch OIDC discovery document and extract `jwks_uri`.
pub(crate) async fn fetch_jwks_uri_from_discovery(
    issuer: &str,
    destination: OidcDestination,
    extra_roots: &[reqwest::Certificate],
) -> Result<String, reqwest::Error> {
    Ok(fetch_discovery_document(issuer, destination, extra_roots)
        .await?
        .jwks_uri)
}

/// Fetch and parse the RSA JWKS for a federated `issuer` (discovery then keys),
/// using the hardened OIDC client. Called on the auth path only after the
/// issuer is confirmed trusted.
///
/// The discovery document names the second destination, so for a
/// tenant-registered issuer that destination is held to the same policy as the
/// issuer itself. Same-origin would be stricter, but it rejects providers that
/// legitimately split the two hosts, Google among them.
pub(crate) async fn fetch_issuer_jwks(
    issuer: &str,
    destination: OidcDestination,
    extra_roots: &[reqwest::Certificate],
) -> Result<HashMap<String, DecodingKey>, AuthError> {
    let jwks_uri = fetch_jwks_uri_from_discovery(issuer, destination, extra_roots)
        .await
        .map_err(|e| AuthError::JwkShape(format!("OIDC discovery failed: {e}")))?;
    if let OidcDestination::TenantRegistered(policy) = destination {
        validate_tenant_oidc_url(&jwks_uri, policy).map_err(|e| {
            AuthError::JwkShape(format!(
                "issuer's jwks_uri is not a permitted destination: {e}"
            ))
        })?;
    }
    let client = oidc_http_client(destination, extra_roots)
        .map_err(|e| AuthError::JwkShape(format!("OIDC client build: {e}")))?;
    let keys_json: Value = client
        .get(&jwks_uri)
        .send()
        .await
        .map_err(|e| AuthError::JwkShape(format!("JWKS request failed: {e}")))?
        .json()
        .await
        .map_err(|e| AuthError::JwkShape(format!("JWKS parse failed: {e}")))?;
    parse_rsa_jwks(&keys_json)
}
