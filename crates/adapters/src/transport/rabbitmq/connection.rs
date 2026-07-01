//! Shared AMQP 1.0 connection helpers built on `fe2o3-amqp`.

use anyhow::{Context, Result as AnyResult, anyhow};
use fe2o3_amqp::Connection;
use fe2o3_amqp::connection::ConnectionHandle;
use fe2o3_amqp::sasl_profile::SaslProfile;
use rustls::pki_types::ServerName;
use std::sync::Arc;
use tokio::net::TcpStream;

/// Parameters shared by the input and output connectors to open an AMQP 1.0
/// connection to a RabbitMQ broker.
pub(crate) struct ConnectionParams<'a> {
    pub host: &'a str,
    pub port: u16,
    /// Virtual host, mapped to the AMQP 1.0 `hostname` field as `vhost:<name>`
    /// per the RabbitMQ convention.
    pub vhost: &'a str,
    pub username: &'a str,
    pub password: &'a str,
    pub tls: bool,
    /// PEM-encoded CA certificate(s) to trust. When set, only these CAs are
    /// trusted; when `None` and `tls` is set, fe2o3's default (webpki) roots.
    pub tls_ca_pem: Option<&'a str>,
    /// AMQP container id (should be unique per connection).
    pub container_id: String,
}

/// Build a rustls TLS connector trusting exactly the CA certificate(s) in `ca_pem`.
fn ca_tls_connector(ca_pem: &str) -> AnyResult<tokio_rustls::TlsConnector> {
    let mut roots = rustls::RootCertStore::empty();
    let mut reader = ca_pem.as_bytes();
    for cert in rustls_pemfile::certs(&mut reader) {
        let cert = cert.context("parse tls_ca_pem certificate")?;
        roots
            .add(cert)
            .context("add tls_ca_pem certificate to trust store")?;
    }
    if roots.is_empty() {
        return Err(anyhow!("tls_ca_pem contained no certificates"));
    }
    let config = rustls::ClientConfig::builder_with_provider(Arc::new(
        rustls::crypto::aws_lc_rs::default_provider(),
    ))
    .with_safe_default_protocol_versions()
    .context("configure rustls protocol versions")?
    .with_root_certificates(roots)
    .with_no_client_auth();
    Ok(tokio_rustls::TlsConnector::from(Arc::new(config)))
}

impl ConnectionParams<'_> {
    /// The `vhost:<name>` value RabbitMQ expects in the AMQP 1.0 `hostname`
    /// field to select a virtual host.
    pub fn hostname(&self) -> String {
        format!("vhost:{}", self.vhost)
    }

    /// Open an AMQP 1.0 connection with SASL PLAIN authentication. Uses `amqps`
    /// (rustls) when `tls` is set — trusting `tls_ca_pem` if provided, otherwise
    /// the default webpki roots — and plain `amqp` otherwise.
    pub async fn open(&self) -> AnyResult<ConnectionHandle<()>> {
        let hostname = self.hostname();
        let builder = || {
            Connection::builder()
                .container_id(self.container_id.clone())
                .hostname(hostname.as_str())
                .sasl_profile(SaslProfile::Plain {
                    username: self.username.to_string(),
                    password: self.password.to_string(),
                })
        };
        let result = match (self.tls, self.tls_ca_pem) {
            (false, _) => {
                builder()
                    .open(format!("amqp://{}:{}", self.host, self.port).as_str())
                    .await
            }
            // Custom CA: do the (implicit) TLS handshake ourselves with our own
            // connector, then run AMQP over the established TLS stream. fe2o3's
            // `open("amqps://")` ignores a supplied connector and always uses the
            // default webpki roots, so we cannot rely on it for a private CA.
            (true, Some(ca_pem)) => {
                let connector = ca_tls_connector(ca_pem)?;
                let tcp = TcpStream::connect((self.host, self.port))
                    .await
                    .with_context(|| format!("connect {}:{}", self.host, self.port))?;
                let server_name = ServerName::try_from(self.host.to_string())
                    .with_context(|| format!("invalid TLS server name '{}'", self.host))?;
                let tls = connector
                    .connect(server_name, tcp)
                    .await
                    .context("TLS handshake")?;
                builder().scheme("amqp").open_with_stream(tls).await
            }
            // Default TLS: fe2o3's built-in rustls path (webpki roots).
            (true, None) => {
                builder()
                    .open(format!("amqps://{}:{}", self.host, self.port).as_str())
                    .await
            }
        };
        result.with_context(|| {
            format!(
                "open AMQP 1.0 connection to {}:{} (vhost {}, tls {})",
                self.host, self.port, self.vhost, self.tls
            )
        })
    }
}
