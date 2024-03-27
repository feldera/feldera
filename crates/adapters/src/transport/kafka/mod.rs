use anyhow::Error as AnyError;
use pipeline_types::transport::kafka::KafkaLogLevel;
use rdkafka::{
    client::{Client as KafkaClient, ClientContext},
    config::RDKafkaLogLevel,
    error::KafkaError,
    types::RDKafkaErrorCode,
};
#[cfg(test)]
use std::sync::Mutex;

pub use ft::{KafkaFtInputEndpoint, KafkaFtOutputEndpoint};
pub use nonft::{KafkaInputEndpoint, KafkaOutputEndpoint};

mod ft;
mod nonft;

pub(crate) fn rdkafka_loglevel_from(level: KafkaLogLevel) -> RDKafkaLogLevel {
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
///
/// The [rd_kafka_fatal_error] documentation says:
///
///    This function is to be used with the Idempotent Producer and `error_cb`
///    to detect fatal errors.
///
///    Generally all errors raised by `error_cb` are to be considered
///    informational and temporary, the client will try to recover from all
///    errors in a graceful fashion (by retrying, etc).
///
///    However, some errors should logically be considered fatal to retain
///    consistency; in particular a set of errors that may occur when using the
///    Idempotent Producer and the in-order or exactly-once producer guarantees
///    can't be satisfied.
///
/// [rd_kafka_fatal_error]: https://docs.confluent.io/platform/current/clients/librdkafka/html/rdkafka_8h.html#a44c976534da6f3877cc514826c71607c
pub(crate) fn refine_kafka_error<C>(client: &KafkaClient<C>, e: KafkaError) -> (bool, AnyError)
where
    C: ClientContext,
{
    match e.rdkafka_error_code() {
        Some(RDKafkaErrorCode::Fatal) => {
            if let Some((_errcode, errstr)) = client.fatal_error() {
                (true, AnyError::msg(errstr))
            } else {
                (true, AnyError::from(e))
            }
        }
        None | Some(_) => (false, AnyError::from(e)),
    }
}

/// Captures and redirect log messages during tests.
///
/// The Rust unit test framework captures log messages during tests and, in some
/// cases, prints them later associated with the particular test.  It does this
/// OK in most cases, but one of the cases it does not handle is output from
/// threads created by anything other than `std::thread` primitives.  This
/// includes the thread created by librdkafka.  If we log anything from a
/// context callback, then it goes directly to stderr, bypassing the unit test
/// framework.  This makes it hard to read the unit test output and hard to
/// attribute messages to particular tests.
///
/// `DeferredLogging` provides a way to capture log messages emitted by the unit
/// tests.  A context can instantiate `DeferredLogging` and use
/// `DeferredLogging::with_deferred_logging` to wrap calls to librdkafka
/// functions that are likely to emit log messages.  This can be specific calls
/// instead of every call, since we know what message the unit tests actually
/// provoke.
///
/// When tests are disabled, this framework compiles to nothing.
#[cfg(test)]
pub(crate) struct DeferredLogging(Mutex<Option<Vec<(RDKafkaLogLevel, String, String)>>>);

#[cfg(test)]
impl DeferredLogging {
    pub fn new() -> Self {
        Self(Mutex::new(None))
    }

    /// Calls `f`, capturing log messages that occur during the call, and then
    /// logging them in the current thread.
    pub fn with_deferred_logging<F, R>(&self, f: F) -> R
    where
        F: Fn() -> R,
    {
        *self.0.lock().unwrap() = Some(Vec::new());
        let r = f();
        for (level, fac, message) in self.0.lock().unwrap().take().unwrap().drain(..) {
            log::info!("{level:?} {fac} {message}");
        }
        r
    }

    /// Logs the message in the usual way, or captures it for later logging if
    /// we're running inside `Self::with_deferred_logging`.
    ///
    /// This is meant to be used to implement `ClientContext::log()`.
    pub fn log(&self, level: RDKafkaLogLevel, fac: &str, log_message: &str) {
        if let Some(ref mut deferred_logging) = self.0.lock().unwrap().as_mut() {
            deferred_logging.push((level, fac.into(), log_message.into()))
        } else {
            Self::default_log(level, fac, log_message)
        }
    }
}

#[cfg(not(test))]
pub(crate) struct DeferredLogging;

#[cfg(not(test))]
impl DeferredLogging {
    pub fn new() -> Self {
        Self
    }

    pub fn with_deferred_logging<F, R>(&self, f: F) -> R
    where
        F: Fn() -> R,
    {
        f()
    }

    pub fn log(&self, level: RDKafkaLogLevel, fac: &str, log_message: &str) {
        Self::default_log(level, fac, log_message)
    }
}

impl DeferredLogging {
    // This is a copy of the default implementation of [`ClientContext::log`].
    fn default_log(level: RDKafkaLogLevel, fac: &str, log_message: &str) {
        match level {
            RDKafkaLogLevel::Emerg
            | RDKafkaLogLevel::Alert
            | RDKafkaLogLevel::Critical
            | RDKafkaLogLevel::Error => {
                log::error!(target: "librdkafka", "librdkafka: {} {}", fac, log_message)
            }
            RDKafkaLogLevel::Warning => {
                log::warn!(target: "librdkafka", "librdkafka: {} {}", fac, log_message)
            }
            RDKafkaLogLevel::Notice => {
                log::info!(target: "librdkafka", "librdkafka: {} {}", fac, log_message)
            }
            RDKafkaLogLevel::Info => {
                log::info!(target: "librdkafka", "librdkafka: {} {}", fac, log_message)
            }
            RDKafkaLogLevel::Debug => {
                log::debug!(target: "librdkafka", "librdkafka: {} {}", fac, log_message)
            }
        }
    }
}
