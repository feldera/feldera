use anyhow::Error as AnyError;
use pipeline_types::transport::KafkaLogLevel;
use rdkafka::{
    client::{Client as KafkaClient, ClientContext},
    config::RDKafkaLogLevel,
    error::KafkaError,
    types::RDKafkaErrorCode,
};

mod input;
mod output;

#[cfg(test)]
pub mod test;

pub use input::KafkaInputTransport;
pub use output::{KafkaOutputConfig, KafkaOutputTransport};

// fn kafka_loglevel_from(level: RDKafkaLogLevel) -> KafkaLogLevel {
//     match level {
//         RDKafkaLogLevel::Emerg => KafkaLogLevel::Emerg,
//         RDKafkaLogLevel::Alert => KafkaLogLevel::Alert,
//         RDKafkaLogLevel::Critical => KafkaLogLevel::Critical,
//         RDKafkaLogLevel::Error => KafkaLogLevel::Error,
//         RDKafkaLogLevel::Warning => KafkaLogLevel::Warning,
//         RDKafkaLogLevel::Notice => KafkaLogLevel::Notice,
//         RDKafkaLogLevel::Info => KafkaLogLevel::Info,
//         RDKafkaLogLevel::Debug => KafkaLogLevel::Debug,
//     }
// }

fn rdkafka_loglevel_from(level: KafkaLogLevel) -> RDKafkaLogLevel {
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
