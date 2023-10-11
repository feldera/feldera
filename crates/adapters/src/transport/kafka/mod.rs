use anyhow::Error as AnyError;
use rdkafka::{
    client::{Client as KafkaClient, ClientContext},
    config::RDKafkaLogLevel,
    error::KafkaError,
    types::RDKafkaErrorCode,
};
use serde::Deserialize;
use std::env;
use utoipa::ToSchema;

mod input;
mod output;

#[cfg(test)]
pub mod test;

pub use input::{KafkaInputConfig, KafkaInputTransport};
pub use output::{KafkaOutputConfig, KafkaOutputTransport};

pub(crate) fn default_redpanda_server() -> String {
    env::var("REDPANDA_BROKERS").unwrap_or_else(|_| "localhost".to_string())
}

/// Kafka logging levels.
#[derive(Debug, Deserialize, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, ToSchema)]
pub enum KafkaLogLevel {
    #[serde(rename = "emerg")]
    Emerg,
    #[serde(rename = "alert")]
    Alert,
    #[serde(rename = "critical")]
    Critical,
    #[serde(rename = "error")]
    Error,
    #[serde(rename = "warning")]
    Warning,
    #[serde(rename = "notice")]
    Notice,
    #[serde(rename = "info")]
    Info,
    #[serde(rename = "debug")]
    Debug,
}

impl From<RDKafkaLogLevel> for KafkaLogLevel {
    fn from(level: RDKafkaLogLevel) -> Self {
        match level {
            RDKafkaLogLevel::Emerg => Self::Emerg,
            RDKafkaLogLevel::Alert => Self::Alert,
            RDKafkaLogLevel::Critical => Self::Critical,
            RDKafkaLogLevel::Error => Self::Error,
            RDKafkaLogLevel::Warning => Self::Warning,
            RDKafkaLogLevel::Notice => Self::Notice,
            RDKafkaLogLevel::Info => Self::Info,
            RDKafkaLogLevel::Debug => Self::Debug,
        }
    }
}

impl From<KafkaLogLevel> for RDKafkaLogLevel {
    fn from(level: KafkaLogLevel) -> Self {
        match level {
            KafkaLogLevel::Emerg => RDKafkaLogLevel::Emerg,
            KafkaLogLevel::Alert => RDKafkaLogLevel::Alert,
            KafkaLogLevel::Critical => RDKafkaLogLevel::Critical,
            KafkaLogLevel::Error => RDKafkaLogLevel::Error,
            KafkaLogLevel::Warning => RDKafkaLogLevel::Warning,
            KafkaLogLevel::Notice => RDKafkaLogLevel::Notice,
            KafkaLogLevel::Info => RDKafkaLogLevel::Info,
            KafkaLogLevel::Debug => RDKafkaLogLevel::Debug,
        }
    }
}

/// If `e` is an error of type `RDKafkaErrorCode::Fatal`, replace
/// it with the result of calling `client.fatal_error()` (which
/// should return the actual cause of the failure).  Otherwise,
/// returns `e`.  The first element of the returned tuple is
/// `true` if `e` is a fatal error.
fn refine_kafka_error<C>(client: &KafkaClient<C>, e: KafkaError) -> (bool, AnyError)
where
    C: ClientContext,
{
    match e.rdkafka_error_code() {
        None => (false, AnyError::from(e)),
        Some(RDKafkaErrorCode::Fatal) => {
            if let Some((_errcode, errstr)) = client.fatal_error() {
                (true, AnyError::msg(errstr))
            } else {
                (true, AnyError::from(e))
            }
        }
        _ => (false, AnyError::from(e)),
    }
}
