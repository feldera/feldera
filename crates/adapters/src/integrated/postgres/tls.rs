use anyhow::{Context, Result as AnyResult};
use feldera_types::transport::postgres::PostgresTlsConfig;
use rustls::client::WebPkiServerVerifier;
use rustls::client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier};
use rustls::pki_types::{CertificateDer, ServerName, UnixTime};
use rustls::{
    CertificateError, ClientConfig, DigitallySignedStruct, RootCertStore, SignatureScheme,
};
use std::sync::Arc;
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

    // `verify_hostname: false` keeps its openssl-connector semantics: the
    // chain is verified against the CA, but the server name need not appear
    // in the certificate.
    let builder = if tls.verify_hostname == Some(false) {
        tracing::warn!(
            "postgres: TLS hostname verification is disabled in endpoint '{endpoint_name}'; \
             the server certificate is still verified against the configured CA."
        );
        let verifier = WebPkiServerVerifier::builder(Arc::new(roots))
            .build()
            .context("failed to build the TLS certificate verifier")?;
        ClientConfig::builder()
            .dangerous()
            .with_custom_certificate_verifier(Arc::new(SkipHostnameVerification(verifier)))
    } else {
        ClientConfig::builder().with_root_certificates(roots)
    };

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

    Ok(Some(MakeRustlsConnect::new(config)))
}

/// Verifies the certificate chain but tolerates a name mismatch, preserving
/// the semantics `verify_hostname: false` had with the openssl connector.
///
/// Duplicated from pipeline-manager's `SkipHostnameVerification`; the crates
/// share no common home for it.
#[derive(Debug)]
struct SkipHostnameVerification(Arc<WebPkiServerVerifier>);

impl ServerCertVerifier for SkipHostnameVerification {
    fn verify_server_cert(
        &self,
        end_entity: &CertificateDer<'_>,
        intermediates: &[CertificateDer<'_>],
        server_name: &ServerName<'_>,
        ocsp_response: &[u8],
        now: UnixTime,
    ) -> Result<ServerCertVerified, rustls::Error> {
        match self
            .0
            .verify_server_cert(end_entity, intermediates, server_name, ocsp_response, now)
        {
            Err(rustls::Error::InvalidCertificate(CertificateError::NotValidForName))
            | Err(rustls::Error::InvalidCertificate(CertificateError::NotValidForNameContext {
                ..
            })) => Ok(ServerCertVerified::assertion()),
            other => other,
        }
    }

    fn verify_tls12_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        self.0.verify_tls12_signature(message, cert, dss)
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        self.0.verify_tls13_signature(message, cert, dss)
    }

    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        self.0.supported_verify_schemes()
    }
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

    /// CA plus a leaf certificate whose only SAN is `san`.
    fn ca_and_leaf(san: &str) -> (String, CertificateDer<'static>) {
        let mut ca_params = rcgen::CertificateParams::default();
        ca_params.is_ca = rcgen::IsCa::Ca(rcgen::BasicConstraints::Unconstrained);
        let ca_key = rcgen::KeyPair::generate().unwrap();
        let ca_pem = ca_params.clone().self_signed(&ca_key).unwrap().pem();

        let mut leaf_params = rcgen::CertificateParams::default();
        leaf_params.subject_alt_names = vec![rcgen::SanType::DnsName(san.try_into().unwrap())];
        let leaf_key = rcgen::KeyPair::generate().unwrap();
        let issuer = rcgen::Issuer::new(ca_params, ca_key);
        let leaf = leaf_params.signed_by(&leaf_key, &issuer).unwrap();
        (ca_pem, leaf.der().clone())
    }

    fn webpki_verifier(ca_pem: &str) -> Arc<WebPkiServerVerifier> {
        // The binary installs the process-level provider at startup; tests
        // must do it themselves.
        let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
        let mut roots = RootCertStore::empty();
        for cert in parse_certs(ca_pem, "CA certificate").unwrap() {
            roots.add(cert).unwrap();
        }
        WebPkiServerVerifier::builder(Arc::new(roots))
            .build()
            .unwrap()
    }

    fn verify(
        verifier: &dyn ServerCertVerifier,
        leaf: &CertificateDer<'static>,
        host: &str,
    ) -> Result<ServerCertVerified, rustls::Error> {
        verifier.verify_server_cert(
            leaf,
            &[],
            &ServerName::try_from(host.to_string()).unwrap(),
            &[],
            UnixTime::now(),
        )
    }

    /// `verify_hostname: false` semantics: a name mismatch passes, but only a
    /// name mismatch.
    #[test]
    fn test_skip_hostname_tolerates_name_mismatch_only() {
        let (ca_pem, leaf) = ca_and_leaf("server.example");
        let inner = webpki_verifier(&ca_pem);

        assert!(matches!(
            verify(inner.as_ref(), &leaf, "other.example"),
            Err(rustls::Error::InvalidCertificate(
                CertificateError::NotValidForName | CertificateError::NotValidForNameContext { .. }
            ))
        ));

        let skip = SkipHostnameVerification(inner);
        assert!(verify(&skip, &leaf, "other.example").is_ok());
        assert!(verify(&skip, &leaf, "server.example").is_ok());
    }

    /// The chain is still verified: a certificate from an untrusted CA fails
    /// even with hostname verification disabled.
    #[test]
    fn test_skip_hostname_rejects_untrusted_ca() {
        let (trusted_ca_pem, _) = ca_and_leaf("server.example");
        let (_, other_leaf) = ca_and_leaf("server.example");

        let skip = SkipHostnameVerification(webpki_verifier(&trusted_ca_pem));
        assert!(matches!(
            verify(&skip, &other_leaf, "server.example"),
            Err(rustls::Error::InvalidCertificate(_))
        ));
    }
}
