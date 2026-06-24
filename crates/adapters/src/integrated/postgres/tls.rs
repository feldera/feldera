use anyhow::{Context, Result as AnyResult};
use feldera_types::transport::postgres::PostgresTlsConfig;
use openssl::{
    pkey::PKey,
    rsa::Rsa,
    ssl::{SslConnector, SslConnectorBuilder, SslFiletype, SslMethod},
    x509::X509,
};
use postgres_openssl::MakeTlsConnector;
use std::{io::Write, path::PathBuf};
use tempfile::NamedTempFile;

/// Writes the given certificate to file and returns the file path.
fn write_ca_cert_to_file(cert: &str) -> AnyResult<PathBuf> {
    let mut file =
        NamedTempFile::new().context("failed to create tempfile to write CA certificate to")?;

    file.write_all(cert.as_bytes())
        .context("failed to write CA certificate to tempfile")?;
    file.flush()
        .context("failed to flush CA certificate to tempfile")?;

    let (_, path) = file.keep()?;
    Ok(path)
}

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

/// Configures SSL certificates for the PostgreSQL connection if enabled.
///
/// Sets the CA certificate (`ssl_ca_pem`) and optionally the client certificate
/// and private key when provided.
fn set_certs(
    builder: &mut SslConnectorBuilder,
    config: &PostgresTlsConfig,
    endpoint_name: &str,
) -> AnyResult<()> {
    let Some(ca_cert) = resolve_ca_pem(config, endpoint_name)? else {
        return Ok(());
    };
    let ca_cert_path = write_ca_cert_to_file(&ca_cert)?;

    builder
        .set_ca_file(ca_cert_path)
        .context("failed to set CA certificate in SSL connector")?;

    fn builder_set_client_from_pem(builder: &mut SslConnectorBuilder, pem: &str) -> AnyResult<()> {
        let cert =
            X509::from_pem(pem.as_bytes()).context("failed to parse client certificate as X509")?;
        builder
            .set_certificate(&cert)
            .context("failed to set client certificate in SSL connector")?;
        Ok(())
    }

    fn builder_set_client_key_from_pem(
        builder: &mut SslConnectorBuilder,
        pem: &str,
    ) -> AnyResult<()> {
        let rsa = Rsa::private_key_from_pem(pem.as_bytes())
            .context("failed to parse client private key as RSA")?;
        let key = PKey::from_rsa(rsa).context("failed to client private key from RSA")?;
        builder
            .set_private_key(key.as_ref())
            .context("failed to set client private key")?;
        Ok(())
    }

    // Set the client certificate, `ssl_client_pem` takes priority.
    match (&config.ssl_client_pem, &config.ssl_client_location) {
        (Some(pem), Some(_)) => {
            tracing::warn!(
                "postgres: both `ssl_client_pem` and `ssl_client_location` are provided; using `ssl_client_pem`"
            );
            builder_set_client_from_pem(builder, pem)?;
        }
        (Some(pem), None) => {
            builder_set_client_from_pem(builder, pem)?;
        }
        (None, Some(location)) => {
            builder
                .set_certificate_file(location, SslFiletype::PEM)
                .context("failed to set client certificate")?;
        }
        // No client cert — ssl_certificate_chain_location only applies to the
        // client-side cert chain, so nothing more to configure here.
        (None, None) => return Ok(()),
    }

    // Set the client key, `ssl_client_key` takes priority.
    match (&config.ssl_client_key, &config.ssl_client_key_location) {
        (Some(key), Some(_)) => {
            tracing::warn!(
                "postgres: both `ssl_client_key` and `ssl_client_key_location` are provided; using `ssl_client_key`"
            );
            builder_set_client_key_from_pem(builder, key)?;
        }
        (Some(key), None) => {
            builder_set_client_key_from_pem(builder, key)?;
        }
        (None, Some(location)) => {
            builder
                .set_private_key_file(location, SslFiletype::PEM)
                .context("failed to set client private key")?;
        }
        (None, None) => return Ok(()),
    }

    // Set the SSL chain certificate.
    if let Some(chain) = &config.ssl_certificate_chain_location {
        builder
            .set_certificate_chain_file(chain)
            .context("failed to set certificate chain")?;
    }

    Ok(())
}

/// Builds a [`MakeTlsConnector`] from the given TLS configuration.
///
/// Returns `None` if no TLS configuration is provided, meaning the caller
/// should use `NoTls` instead.
pub(crate) fn make_tls_connector(
    tls: &PostgresTlsConfig,
    endpoint_name: &str,
) -> AnyResult<Option<MakeTlsConnector>> {
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

    let mut builder =
        SslConnector::builder(SslMethod::tls()).context("failed to build SSL connection")?;

    set_certs(&mut builder, tls, endpoint_name)?;

    let mut connector = MakeTlsConnector::new(builder.build());

    if Some(false) == tls.verify_hostname {
        let endpoint_name = endpoint_name.to_owned();
        connector.set_callback(move |ctx, _| {
            tracing::warn!("postgres: ssl: disabling hostname verification in connector '{endpoint_name}'. The PostgreSQL server's hostname may not match the one specified in the SSL certificate.");
            ctx.set_verify_hostname(false);
            Ok(())
        });
    }

    Ok(Some(connector))
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
