use rdkafka::config::RDKafkaLogLevel;
use serde::Deserialize;

mod input;
mod output;

#[cfg(test)]
mod test;

pub use input::KafkaInputTransport;
pub use output::KafkaOutputTransport;

/// Kafka logging levels.
#[derive(Debug, Deserialize, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
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
