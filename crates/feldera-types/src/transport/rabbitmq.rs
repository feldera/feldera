//! Configuration for the RabbitMQ AMQP 1.0 connectors (`rabbitmq_input` and
//! `rabbitmq_output`).
//!
//! Both connectors speak **AMQP 1.0** (RabbitMQ 4.x native protocol on port
//! 5672, or `amqps` on 5671). The input connector attaches a receiver to a
//! queue or stream (`/queues/{name}`); the output connector attaches a sender
//! to an exchange (`/exchanges/{exchange}`) and routes by the message subject.

use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

/// Default AMQP port (both AMQP 0-9-1 and AMQP 1.0 on RabbitMQ 4.x).
pub const fn default_amqp_port() -> u16 {
    5672
}

fn default_vhost() -> String {
    "/".to_string()
}

/// Starting position for a RabbitMQ **stream** source, mapped to the
/// `rabbitmq:stream-offset-spec` filter on the AMQP 1.0 receiver.
///
/// Ignored for non-stream queues.
#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize, ToSchema)]
#[serde(untagged)]
pub enum RabbitmqOffsetConfig {
    /// `{ "policy": "first" }`, `"last"`, or `"next"`.
    Policy { policy: String },
    /// Absolute stream offset.
    Offset { offset: u64 },
    /// RFC 3339 timestamp.
    Timestamp { timestamp: String },
}

/// Configuration for the `rabbitmq_input` connector (AMQP 1.0 receiver).
#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize, ToSchema)]
pub struct RabbitmqInputConfig {
    /// Broker hostname.
    pub host: String,
    /// AMQP port. Defaults to 5672 (or 5671 when `tls` is set and no port is
    /// given by the user).
    #[serde(default = "default_amqp_port")]
    pub port: u16,
    /// Virtual host. Mapped to the AMQP 1.0 `hostname` field as `vhost:<name>`.
    #[serde(default = "default_vhost")]
    pub vhost: String,
    /// SASL PLAIN username.
    pub username: String,
    /// SASL PLAIN password.
    pub password: String,
    /// Queue or stream to consume from. Attaches to the AMQP 1.0 address
    /// `/queues/{queue}`.
    pub queue: String,
    /// Stream offset specification. When set, the queue is treated as a stream
    /// and consumed with the `rabbitmq:stream-offset-spec` filter, enabling
    /// exactly-once replay. Omit for classic/quorum queues (at-least-once).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub offset: Option<RabbitmqOffsetConfig>,
    /// Use TLS (`amqps`).
    #[serde(default)]
    pub tls: bool,
    /// Optional AMQP container/link name suffix (must be unique per consumer to
    /// allow fan-out).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub consumer_name: Option<String>,
}

/// Delivery durability for published messages.
#[derive(Debug, Clone, Copy, Eq, PartialEq, Serialize, Deserialize, ToSchema, Default)]
#[serde(rename_all = "snake_case")]
pub enum RabbitmqDeliveryMode {
    /// Persistent (durable=true header).
    #[default]
    Persistent,
    /// Transient (durable=false header).
    Transient,
}

/// Configuration for the `rabbitmq_output` connector (AMQP 1.0 sender).
#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize, ToSchema)]
pub struct RabbitmqOutputConfig {
    /// Broker hostname.
    pub host: String,
    /// AMQP port. Defaults to 5672.
    #[serde(default = "default_amqp_port")]
    pub port: u16,
    /// Virtual host. Mapped to the AMQP 1.0 `hostname` field as `vhost:<name>`.
    #[serde(default = "default_vhost")]
    pub vhost: String,
    /// SASL PLAIN username.
    pub username: String,
    /// SASL PLAIN password.
    pub password: String,
    /// Target exchange. Attaches to the AMQP 1.0 address `/exchanges/{exchange}`.
    pub exchange: String,
    /// Routing key. Set as the message **subject**, which RabbitMQ uses as the
    /// routing key for exchange bindings.
    pub routing_key: String,
    /// Message durability.
    #[serde(default)]
    pub delivery_mode: RabbitmqDeliveryMode,
    /// Use TLS (`amqps`).
    #[serde(default)]
    pub tls: bool,
}

impl RabbitmqInputConfig {
    pub fn validate(&self) -> Result<(), String> {
        if self.host.is_empty() {
            return Err("host must not be empty".into());
        }
        if self.queue.is_empty() {
            return Err("queue must not be empty".into());
        }
        if self.username.is_empty() {
            return Err("username must not be empty".into());
        }
        Ok(())
    }

    /// Whether this source is consumed with stream semantics (offset filter +
    /// exactly-once replay).
    pub fn is_stream(&self) -> bool {
        self.offset.is_some()
    }
}

impl RabbitmqOutputConfig {
    pub fn validate(&self) -> Result<(), String> {
        if self.host.is_empty() {
            return Err("host must not be empty".into());
        }
        if self.exchange.is_empty() {
            return Err("exchange must not be empty".into());
        }
        if self.routing_key.is_empty() {
            return Err("routing_key must not be empty".into());
        }
        if self.username.is_empty() {
            return Err("username must not be empty".into());
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn input_config_roundtrip() {
        let json = r#"{
            "host": "localhost",
            "username": "guest",
            "password": "guest",
            "queue": "events",
            "offset": { "policy": "next" }
        }"#;
        let cfg: RabbitmqInputConfig = serde_json::from_str(json).unwrap();
        assert_eq!(cfg.queue, "events");
        assert_eq!(cfg.port, 5672);
        assert_eq!(cfg.vhost, "/");
        assert!(cfg.is_stream());
        assert!(cfg.validate().is_ok());
    }

    #[test]
    fn input_config_no_offset_is_not_stream() {
        let json = r#"{
            "host": "localhost",
            "username": "guest",
            "password": "guest",
            "queue": "work_q"
        }"#;
        let cfg: RabbitmqInputConfig = serde_json::from_str(json).unwrap();
        assert!(!cfg.is_stream());
    }

    #[test]
    fn output_config_roundtrip() {
        let json = r#"{
            "host": "localhost",
            "username": "guest",
            "password": "guest",
            "exchange": "analytics",
            "routing_key": "results.v1"
        }"#;
        let cfg: RabbitmqOutputConfig = serde_json::from_str(json).unwrap();
        assert_eq!(cfg.exchange, "analytics");
        assert_eq!(cfg.delivery_mode, RabbitmqDeliveryMode::Persistent);
        assert!(cfg.validate().is_ok());
    }

    #[test]
    fn missing_queue_fails_validation() {
        let cfg = RabbitmqInputConfig {
            host: "h".into(),
            port: 5672,
            vhost: "/".into(),
            username: "u".into(),
            password: "p".into(),
            queue: "".into(),
            offset: None,
            tls: false,
            consumer_name: None,
        };
        assert!(cfg.validate().is_err());
    }
}
