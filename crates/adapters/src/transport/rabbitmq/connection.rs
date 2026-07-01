//! Shared AMQP 1.0 connection helpers built on `fe2o3-amqp`.

use anyhow::{Context, Result as AnyResult};
use fe2o3_amqp::Connection;
use fe2o3_amqp::connection::ConnectionHandle;
use fe2o3_amqp::sasl_profile::SaslProfile;

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
    /// AMQP container id (should be unique per connection).
    pub container_id: String,
}

impl ConnectionParams<'_> {
    /// The `vhost:<name>` value RabbitMQ expects in the AMQP 1.0 `hostname`
    /// field to select a virtual host.
    pub fn hostname(&self) -> String {
        format!("vhost:{}", self.vhost)
    }

    /// Open an AMQP 1.0 connection with SASL PLAIN authentication. Uses `amqps`
    /// (rustls with the system/webpki roots) when `tls` is set, otherwise
    /// plain `amqp`.
    pub async fn open(&self) -> AnyResult<ConnectionHandle<()>> {
        let scheme = if self.tls { "amqps" } else { "amqp" };
        let url = format!("{scheme}://{}:{}", self.host, self.port);
        Connection::builder()
            .container_id(self.container_id.clone())
            .hostname(self.hostname().as_str())
            .sasl_profile(SaslProfile::Plain {
                username: self.username.to_string(),
                password: self.password.to_string(),
            })
            .open(url.as_str())
            .await
            .with_context(|| format!("open AMQP 1.0 connection to {url} (vhost {})", self.vhost))
    }
}
