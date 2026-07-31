//! Destination policy for OIDC discovery and JWKS fetches.
//!
//! A tenant administrator registers an issuer URL that the pipeline manager
//! later fetches from its own network position, before any signature on the
//! presented token is verified. Without a policy, registering a trust is a
//! server-side request primitive aimed at whatever the manager can reach.
//!
//! Issuers the operator names at deploy time, the login provider and the owner
//! trusts, are exempt: the authority that chooses them also chooses the network
//! the manager runs in, so a private IdP stays supported. The policy applies to
//! what a tenant administrator can register, which is the surface an attacker
//! controls.
//!
//! An installation whose tenants federate against an IdP inside that same
//! network lifts the policy with `--allow-internal-tenant-trust-issuers`.

use std::fmt;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};
use url::{Host, Url};

/// Where a tenant-registered issuer may point.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TenantIssuerPolicy {
    /// Require https and an address routable on the public internet.
    PublicHttpsOnly,
    /// Permit private, loopback and link-local addresses, for an installation
    /// whose IdP is not reachable from the public internet. https is still
    /// required, so the JWKS is still fetched over an authenticated channel.
    ///
    /// This gives every tenant administrator a fetch aimed at the manager's own
    /// network, so it is worth it only where that network is already trusted.
    AllowInternal,
}

/// Why an issuer or `jwks_uri` URL is not a permitted fetch destination.
#[derive(Debug, PartialEq, Eq)]
pub enum OidcUrlError {
    Malformed(String),
    NotHttps(String),
    HasCredentials,
    NoHost,
    PrivateAddress(IpAddr),
}

impl fmt::Display for OidcUrlError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Malformed(e) => write!(f, "not a valid URL: {e}"),
            Self::NotHttps(scheme) => {
                write!(f, "scheme must be https, found '{scheme}'")
            }
            Self::HasCredentials => f.write_str("must not embed a username or password"),
            Self::NoHost => f.write_str("must name a host"),
            Self::PrivateAddress(ip) => {
                write!(f, "address {ip} is not reachable on the public internet")
            }
        }
    }
}

/// Whether `ip` is routable on the public internet.
///
/// Rejects the ranges an SSRF probe aims at, including link-local, which is
/// where the cloud metadata service at 169.254.169.254 lives.
pub fn is_public_ip(ip: IpAddr) -> bool {
    match ip {
        IpAddr::V4(v4) => is_public_ipv4(v4),
        IpAddr::V6(v6) => is_public_ipv6(v6),
    }
}

// `IpAddr::is_global` answers this in std, but it is still unstable (rust issue
// 27709), so the ranges are spelled out below.
fn is_public_ipv4(ip: Ipv4Addr) -> bool {
    let [a, b, _, _] = ip.octets();
    !(ip.is_loopback()
        || ip.is_private()
        || ip.is_link_local()
        || ip.is_broadcast()
        || ip.is_documentation()
        || ip.is_unspecified()
        || ip.is_multicast()
        || a == 0                                // 0.0.0.0/8, "this network"
        || (a == 100 && (64..128).contains(&b))  // 100.64.0.0/10, carrier-grade NAT
        || (a == 198 && (18..20).contains(&b))   // 198.18.0.0/15, benchmarking
        || a >= 240) // 240.0.0.0/4, reserved
}

fn is_public_ipv6(ip: Ipv6Addr) -> bool {
    // `::ffff:a.b.c.d` and `::a.b.c.d` reach an IPv4 destination, so they are
    // judged by the IPv4 rules rather than treated as opaque IPv6.
    if let Some(v4) = ip.to_ipv4() {
        return is_public_ipv4(v4);
    }
    let first_segment = ip.segments()[0];
    !(ip.is_loopback()
        || ip.is_unspecified()
        || ip.is_multicast()
        || (first_segment & 0xfe00) == 0xfc00   // fc00::/7, unique local
        || (first_segment & 0xffc0) == 0xfe80) // fe80::/10, link-local unicast
}

/// Validate a URL the platform would fetch on behalf of a tenant-registered
/// trust: the issuer at registration, and the `jwks_uri` its discovery document
/// returns.
///
/// Checks what holds regardless of DNS. A hostname's addresses are checked
/// again when the connection is made, because DNS can change in between.
pub fn validate_tenant_oidc_url(url: &str, policy: TenantIssuerPolicy) -> Result<(), OidcUrlError> {
    let parsed = Url::parse(url).map_err(|e| OidcUrlError::Malformed(e.to_string()))?;
    if parsed.scheme() != "https" {
        return Err(OidcUrlError::NotHttps(parsed.scheme().to_string()));
    }
    if !parsed.username().is_empty() || parsed.password().is_some() {
        return Err(OidcUrlError::HasCredentials);
    }
    let host = parsed.host().ok_or(OidcUrlError::NoHost)?;
    // The policy governs which addresses are permitted. https and the absence
    // of embedded credentials are required either way.
    if policy == TenantIssuerPolicy::AllowInternal {
        return Ok(());
    }
    match host {
        Host::Ipv4(v4) if !is_public_ipv4(v4) => Err(OidcUrlError::PrivateAddress(IpAddr::V4(v4))),
        Host::Ipv6(v6) if !is_public_ipv6(v6) => Err(OidcUrlError::PrivateAddress(IpAddr::V6(v6))),
        _ => Ok(()),
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use std::str::FromStr;

    fn public(ip: &str) -> bool {
        is_public_ip(IpAddr::from_str(ip).unwrap())
    }

    /// Validate under the default policy, which is what almost every case here
    /// is about.
    fn validate(url: &str) -> Result<(), OidcUrlError> {
        validate_tenant_oidc_url(url, TenantIssuerPolicy::PublicHttpsOnly)
    }

    #[test]
    fn private_and_reserved_addresses_are_not_public() {
        for ip in [
            "127.0.0.1",
            "10.1.2.3",
            "172.16.0.1",
            "192.168.1.1",
            "169.254.169.254", // cloud metadata service
            "0.0.0.0",
            "100.64.0.1",
            "198.18.0.1",
            "255.255.255.255",
            "240.0.0.1",
            "224.0.0.1",
            "::1",
            "::",
            "fd00::1",
            "fe80::1",
            "::ffff:127.0.0.1",
            "::ffff:10.0.0.1",
        ] {
            assert!(!public(ip), "{ip} must not count as public");
        }
    }

    #[test]
    fn routable_addresses_are_public() {
        for ip in ["8.8.8.8", "1.1.1.1", "93.184.216.34", "2606:4700::1111"] {
            assert!(public(ip), "{ip} must count as public");
        }
    }

    #[test]
    fn tenant_urls_must_be_https_without_credentials() {
        assert!(validate("https://accounts.google.com").is_ok());
        assert!(validate("https://token.actions.githubusercontent.com").is_ok());

        assert_eq!(
            validate("http://accounts.google.com"),
            Err(OidcUrlError::NotHttps("http".to_string()))
        );
        assert_eq!(
            validate("https://user:pw@idp.example.com"),
            Err(OidcUrlError::HasCredentials)
        );
        assert!(matches!(
            validate("not a url"),
            Err(OidcUrlError::Malformed(_))
        ));
    }

    #[test]
    fn tenant_urls_may_not_name_an_internal_address() {
        for url in INTERNAL_URLS {
            assert!(
                matches!(validate(url), Err(OidcUrlError::PrivateAddress(_))),
                "{url} must be rejected"
            );
        }
    }

    const INTERNAL_URLS: [&str; 5] = [
        "https://127.0.0.1/idp",
        "https://169.254.169.254/latest/meta-data",
        "https://10.0.0.5:8080",
        "https://[::1]:9876",
        "https://[fd00::1]",
    ];

    /// The escape hatch lifts the address rule and nothing else.
    #[test]
    fn allowing_internal_issuers_still_requires_https() {
        let permissive = |url| validate_tenant_oidc_url(url, TenantIssuerPolicy::AllowInternal);
        for url in INTERNAL_URLS {
            assert!(permissive(url).is_ok(), "{url} must be admitted");
        }
        assert_eq!(
            permissive("http://10.0.0.5"),
            Err(OidcUrlError::NotHttps("http".to_string()))
        );
        assert_eq!(
            permissive("https://user:pw@10.0.0.5"),
            Err(OidcUrlError::HasCredentials)
        );
        assert!(matches!(
            permissive("not a url"),
            Err(OidcUrlError::Malformed(_))
        ));
    }

    /// A hostname is admitted here and judged again at connect time, so that a
    /// name resolving to an internal address is not silently trusted.
    #[test]
    fn a_hostname_passes_static_validation() {
        assert!(validate("https://internal.corp.example").is_ok());
    }
}
