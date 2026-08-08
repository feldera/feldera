use anyhow::{Context, Result as AnyResult};
use feldera_types::transport::postgres::PostgresTlsConfig;
use rustls::pki_types::CertificateDer;
use rustls::{ClientConfig, RootCertStore};
use tokio_postgres_rustls::MakeRustlsConnect;

/// Resolves the configured certificate-authority certificate(s) to PEM text.
///
/// `ssl_ca_pem` (inline PEM) takes precedence over `ssl_ca_location` (a path to
/// a PEM file), which is read from disk. Returns `None` when neither is set,
/// meaning TLS should be disabled.
pub(crate) fn resolve_ca_pem(
    config: &PostgresTlsConfig,
    endpoint_name: &str,
) -> AnyResult<Option<String>> {
    match (&config.ssl_ca_pem, &config.ssl_ca_location) {
        (Some(pem), Some(_)) => {
            tracing::warn!(
                "postgres {endpoint_name}: both `ssl_ca_pem` and `ssl_ca_location` are provided; using `ssl_ca_pem`"
            );
            Ok(Some(pem.clone()))
        }
        (Some(pem), None) => Ok(Some(pem.clone())),
        (None, Some(location)) => {
            let pem = std::fs::read_to_string(location).with_context(|| {
                format!(
                    "postgres {endpoint_name}: failed to read CA certificate file at `ssl_ca_location` ('{location}')"
                )
            })?;
            Ok(Some(pem))
        }
        (None, None) => Ok(None),
    }
}

/// Resolves the client certificate chain, preferring inline PEM over a path.
fn resolve_client_cert_pem(config: &PostgresTlsConfig) -> AnyResult<Option<String>> {
    match (&config.ssl_client_pem, &config.ssl_client_location) {
        (Some(pem), Some(_)) => {
            tracing::warn!(
                "postgres: both `ssl_client_pem` and `ssl_client_location` are provided; using `ssl_client_pem`"
            );
            Ok(Some(pem.clone()))
        }
        (Some(pem), None) => Ok(Some(pem.clone())),
        (None, Some(location)) => Ok(Some(
            std::fs::read_to_string(location)
                .with_context(|| format!("failed to read client certificate at '{location}'"))?,
        )),
        (None, None) => Ok(None),
    }
}

/// Resolves the client private key, preferring inline PEM over a path.
fn resolve_client_key_pem(config: &PostgresTlsConfig) -> AnyResult<Option<String>> {
    match (&config.ssl_client_key, &config.ssl_client_key_location) {
        (Some(key), Some(_)) => {
            tracing::warn!(
                "postgres: both `ssl_client_key` and `ssl_client_key_location` are provided; using `ssl_client_key`"
            );
            Ok(Some(key.clone()))
        }
        (Some(key), None) => Ok(Some(key.clone())),
        (None, Some(location)) => Ok(Some(
            std::fs::read_to_string(location)
                .with_context(|| format!("failed to read client private key at '{location}'"))?,
        )),
        (None, None) => Ok(None),
    }
}

/// Parses every certificate in `pem`.
fn parse_certs(pem: &str, what: &str) -> AnyResult<Vec<CertificateDer<'static>>> {
    let certs: Result<Vec<_>, _> = rustls_pemfile::certs(&mut pem.as_bytes()).collect();
    let certs = certs.with_context(|| format!("failed to parse {what} as PEM certificates"))?;
    if certs.is_empty() {
        anyhow::bail!("{what} contains no certificates");
    }
    Ok(certs)
}

/// Builds a [`MakeRustlsConnect`] from the given TLS configuration.
///
/// Returns `None` if no TLS configuration is provided, meaning the caller
/// should use `NoTls` instead.
pub(crate) fn make_tls_connector(
    tls: &PostgresTlsConfig,
    endpoint_name: &str,
) -> AnyResult<Option<MakeRustlsConnect>> {
    if !tls.has_tls()
        && (tls.ssl_client_pem.is_some()
            || tls.ssl_client_location.is_some()
            || tls.ssl_client_key.is_some()
            || tls.ssl_client_key_location.is_some())
    {
        tracing::warn!(
            "postgres: TLS client certificate fields are set but no CA certificate \
             was provided; connecting without TLS. Set `ssl_ca_pem` or \
             `ssl_ca_location` to enable TLS."
        );
    }

    if !tls.has_tls() {
        return Ok(None);
    }

    let Some(ca_pem) = resolve_ca_pem(tls, endpoint_name)? else {
        return Ok(None);
    };

    let mut roots = RootCertStore::empty();
    for cert in parse_certs(&ca_pem, "CA certificate")? {
        roots
            .add(cert)
            .context("failed to add CA certificate to the trust store")?;
    }

    let builder = ClientConfig::builder().with_root_certificates(roots);

    // A client certificate needs its key, and the chain file extends it.
    let config = match (resolve_client_cert_pem(tls)?, resolve_client_key_pem(tls)?) {
        (Some(cert_pem), Some(key_pem)) => {
            let mut chain = parse_certs(&cert_pem, "client certificate")?;
            if let Some(location) = &tls.ssl_certificate_chain_location {
                let chain_pem = std::fs::read_to_string(location)
                    .with_context(|| format!("failed to read certificate chain at '{location}'"))?;
                chain.extend(parse_certs(&chain_pem, "certificate chain")?);
            }
            let key = rustls_pemfile::private_key(&mut key_pem.as_bytes())
                .context("failed to parse client private key as PEM")?
                .context("client private key PEM contains no key")?;
            builder
                .with_client_auth_cert(chain, key)
                .context("failed to configure client certificate authentication")?
        }
        (Some(_), None) => {
            anyhow::bail!("postgres: a client certificate was provided without a private key")
        }
        (None, Some(_)) => {
            anyhow::bail!("postgres: a client private key was provided without a certificate")
        }
        (None, None) => builder.with_no_client_auth(),
    };

    if Some(false) == tls.verify_hostname {
        tracing::warn!(
            "postgres: ssl: `verify_hostname` is not supported by the rustls connector in \
             endpoint '{endpoint_name}'; the server hostname is still verified against its \
             certificate."
        );
    }

    Ok(Some(MakeRustlsConnect::new(config)))
}

/// Extracts the trusted root certificates from [`PostgresTlsConfig`]
/// into [`etl::config::TlsConfig`], used by the Postgres CDC connector.
///
/// The etl crate doesn't support client-certificate TLS options.
#[cfg(feature = "with-postgres-cdc")]
pub(crate) fn make_etl_tls_config(
    tls: &PostgresTlsConfig,
    endpoint_name: &str,
) -> AnyResult<etl::config::TlsConfig> {
    use etl::config::TlsConfig;

    match resolve_ca_pem(tls, endpoint_name)? {
        Some(trusted_root_certs) => Ok(TlsConfig {
            trusted_root_certs,
            enabled: true,
        }),
        None => Ok(TlsConfig::disabled()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_resolve_ca_pem_none_by_default() {
        assert!(
            resolve_ca_pem(&PostgresTlsConfig::default(), "test")
                .unwrap()
                .is_none()
        );
    }

    #[test]
    fn test_resolve_ca_pem_inline() {
        let pem = resolve_ca_pem(
            &PostgresTlsConfig {
                ssl_ca_pem: Some("inline-pem".to_string()),
                ..Default::default()
            },
            "test",
        )
        .unwrap();
        assert_eq!(pem.as_deref(), Some("inline-pem"));
    }

    #[test]
    fn test_resolve_ca_pem_reads_location() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("ca.pem");
        let contents = "-----BEGIN CERTIFICATE-----\nfromfile\n-----END CERTIFICATE-----";
        std::fs::write(&path, contents).unwrap();

        let pem = resolve_ca_pem(
            &PostgresTlsConfig {
                ssl_ca_location: Some(path.to_string_lossy().into_owned()),
                ..Default::default()
            },
            "test",
        )
        .unwrap();
        assert_eq!(pem.as_deref(), Some(contents));
    }

    #[test]
    fn test_resolve_ca_pem_inline_takes_precedence() {
        let pem = resolve_ca_pem(
            &PostgresTlsConfig {
                ssl_ca_pem: Some("inline-pem".to_string()),
                ssl_ca_location: Some("/does/not/exist.pem".to_string()),
                ..Default::default()
            },
            "test",
        )
        .unwrap();
        assert_eq!(pem.as_deref(), Some("inline-pem"));
    }

    #[test]
    fn test_resolve_ca_pem_missing_file_errors() {
        let result = resolve_ca_pem(
            &PostgresTlsConfig {
                ssl_ca_location: Some("/does/not/exist.pem".to_string()),
                ..Default::default()
            },
            "test",
        );
        assert!(result.is_err());
    }

    #[cfg(feature = "with-postgres-cdc")]
    #[test]
    fn test_etl_tls_disabled_by_default() {
        let tls = make_etl_tls_config(&PostgresTlsConfig::default(), "test").unwrap();
        assert!(!tls.enabled);
        assert!(tls.trusted_root_certs.is_empty());
    }

    #[cfg(feature = "with-postgres-cdc")]
    #[test]
    fn test_etl_tls_enabled_with_ca() {
        let tls = make_etl_tls_config(
            &PostgresTlsConfig {
                ssl_ca_pem: Some(
                    "-----BEGIN CERTIFICATE-----\nMIIB\n-----END CERTIFICATE-----".to_string(),
                ),
                ..Default::default()
            },
            "test",
        )
        .unwrap();
        assert!(tls.enabled);
        assert!(tls.trusted_root_certs.contains("BEGIN CERTIFICATE"));
    }
}
