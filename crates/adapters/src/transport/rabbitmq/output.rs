//! RabbitMQ AMQP 1.0 output connector (`fe2o3-amqp` sender to an exchange).

use super::connection::ConnectionParams;
use crate::OutputEndpoint;
use anyhow::{Result as AnyResult, anyhow};
use dbsp::circuit::tokio::TOKIO;
use fe2o3_amqp::connection::ConnectionHandle;
use fe2o3_amqp::session::SessionHandle;
use fe2o3_amqp::types::messaging::{ApplicationProperties, Data, Header, Message, Properties};
use fe2o3_amqp::types::primitives::Binary;
use fe2o3_amqp::{Sender, Session};
use feldera_adapterlib::transport::AsyncErrorCallback;
use feldera_types::transport::rabbitmq::{RabbitmqDeliveryMode, RabbitmqOutputConfig};
use std::process;
use tracing::{info_span, span::EnteredSpan};

pub struct RabbitmqOutputEndpoint {
    config: RabbitmqOutputConfig,
    connection: Option<ConnectionHandle<()>>,
    session: Option<SessionHandle<()>>,
    sender: Option<Sender>,
}

impl RabbitmqOutputEndpoint {
    pub fn new(config: RabbitmqOutputConfig) -> AnyResult<Self> {
        config
            .validate()
            .map_err(|e| anyhow!("Invalid RabbitMQ output configuration: {e}"))?;
        Ok(Self {
            config,
            connection: None,
            session: None,
            sender: None,
        })
    }

    fn span(&self) -> EnteredSpan {
        info_span!(
            "rabbitmq_output",
            exchange = %self.config.exchange,
            routing_key = %self.config.routing_key,
        )
        .entered()
    }

    fn header(&self) -> Header {
        Header::builder()
            .durable(matches!(
                self.config.delivery_mode,
                RabbitmqDeliveryMode::Persistent
            ))
            .build()
    }

    /// Build the AMQP 1.0 message: `Data` body, subject == routing key, and any
    /// per-record headers mapped to application-properties.
    fn message(&self, payload: Vec<u8>, headers: &[(&str, Option<&[u8]>)]) -> Message<Data> {
        let mut builder = Message::builder()
            .header(self.header())
            .properties(
                Properties::builder()
                    .subject(self.config.routing_key.as_str())
                    .build(),
            )
            .data(Binary::from(payload));
        if !headers.is_empty() {
            let mut props = ApplicationProperties::builder();
            for (key, value) in headers {
                let value = value
                    .map(|v| String::from_utf8_lossy(v).into_owned())
                    .unwrap_or_default();
                props = props.insert(key.to_string(), value);
            }
            builder = builder.application_properties(props.build());
        }
        builder.build()
    }

    async fn send(&mut self, message: Message<Data>) -> AnyResult<()> {
        let sender = self
            .sender
            .as_mut()
            .ok_or_else(|| anyhow!("RabbitMQ output sender not connected"))?;
        sender
            .send(message)
            .await
            .map_err(|e| {
                anyhow!(
                    "publish to RabbitMQ exchange '{}': {e}",
                    self.config.exchange
                )
            })?
            .accepted_or_else(|outcome| anyhow!("RabbitMQ rejected publish: {outcome:?}"))?;
        Ok(())
    }
}

impl OutputEndpoint for RabbitmqOutputEndpoint {
    fn connect(&mut self, _async_error_callback: AsyncErrorCallback) -> AnyResult<()> {
        let _guard = self.span();
        let params = ConnectionParams {
            host: &self.config.host,
            port: self.config.port,
            vhost: &self.config.vhost,
            username: &self.config.username,
            password: &self.config.password,
            tls: self.config.tls,
            tls_ca_pem: self.config.tls_ca_pem.as_deref(),
            container_id: format!("feldera-rabbitmq-output-{}", process::id()),
        };
        // Route by routing key via the exchange address `/exchanges/{ex}/{key}`.
        let address = format!(
            "/exchanges/{}/{}",
            self.config.exchange, self.config.routing_key
        );
        let (connection, session, sender) = TOKIO.block_on(async {
            let mut connection = params.open().await?;
            let mut session = Session::begin(&mut connection)
                .await
                .map_err(|e| anyhow!("begin AMQP session: {e}"))?;
            let sender = Sender::attach(&mut session, "feldera-rabbitmq-output", address.as_str())
                .await
                .map_err(|e| {
                    anyhow!("attach sender to exchange '{}': {e}", self.config.exchange)
                })?;
            AnyResult::<_>::Ok((connection, session, sender))
        })?;
        self.connection = Some(connection);
        self.session = Some(session);
        self.sender = Some(sender);
        Ok(())
    }

    fn max_buffer_size_bytes(&self) -> usize {
        usize::MAX
    }

    fn push_buffer(&mut self, buffer: &[u8]) -> AnyResult<()> {
        let _guard = self.span();
        let message = self.message(buffer.to_vec(), &[]);
        TOKIO.block_on(self.send(message))
    }

    fn push_key(
        &mut self,
        _key: Option<&[u8]>,
        val: Option<&[u8]>,
        headers: &[(&str, Option<&[u8]>)],
    ) -> AnyResult<()> {
        let Some(val) = val else { return Ok(()) };
        let _guard = self.span();
        let message = self.message(val.to_vec(), headers);
        TOKIO.block_on(self.send(message))
    }

    fn is_fault_tolerant(&self) -> bool {
        false
    }
}
