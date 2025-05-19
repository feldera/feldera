use anyhow::{anyhow, bail, Error as AnyError, Result as AnyResult};
use aws_msk_iam_sasl_signer::generate_auth_token;
use dbsp::circuit::tokio::TOKIO;
use feldera_types::transport::kafka::{KafkaHeader, KafkaLogLevel};
use parquet::data_type::AsBytes;
use rdkafka::client::OAuthToken;
use rdkafka::message::{Header, OwnedHeaders, ToBytes};
use rdkafka::producer::{BaseRecord, ProducerContext, ThreadedProducer};
use rdkafka::{
    client::{Client as KafkaClient, ClientContext},
    config::RDKafkaLogLevel,
    error::KafkaError,
    types::RDKafkaErrorCode,
};
use sha2::Digest;
use std::cmp::min;
use std::collections::{BTreeMap, HashMap};
use std::error::Error;
use std::io::Write;
use std::path::PathBuf;
#[cfg(test)]
use std::sync::Mutex;
use std::thread::sleep;
use std::time::{Duration, Instant};
use tracing::warn;

pub use ft::{KafkaFtInputEndpoint, KafkaFtOutputEndpoint};
pub use nonft::KafkaOutputEndpoint;

mod ft;
mod nonft;

const MAX_POLLING_INTERVAL: Duration = Duration::from_millis(5000);

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

/// Saves the PEM file to `current_dir()/[FILE-HASH-suffix].pem`
fn save_pem_file(pem: &str, suffix: &str) -> AnyResult<PathBuf> {
    let mut path = std::env::current_dir()?;

    // hash the pem key for file name
    let hash = sha2::Sha256::new().chain_update(pem).finalize();
    let s = format!("{hash:x}-{suffix}");

    path.push(&s);
    path.set_extension("pem");

    let file = std::fs::File::create(&path)?;
    let mut buf = std::io::BufWriter::new(file);

    // write the pem keys to the file
    buf.write_all(pem.as_bytes())?;
    buf.flush()?;

    Ok(path)
}

/// A workaround against https://github.com/confluentinc/librdkafka/issues/3225
pub(crate) trait PemToLocation {
    fn pem_to_location(&mut self, endpoint_name: &str) -> AnyResult<()>;
}

impl PemToLocation for rdkafka::ClientConfig {
    fn pem_to_location(&mut self, endpoint_name: &str) -> AnyResult<()> {
        const KEY: &str = "ssl.certificate.pem";

        let Some(ssl_cert_pem) = self.get(KEY) else {
            return Ok(());
        };

        let file = save_pem_file(ssl_cert_pem, endpoint_name)?;
        self.remove(KEY);

        self.set(
            "ssl.certificate.location",
            file.to_str().expect(
                "failed to convert file name to str while saving \
ssl.certificate.pem to file, this should not happen, \
please create an issue at: https://github.com/feldera/feldera/issues",
            ),
        );

        Ok(())
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
            tracing::info!("{level:?} {fac} {message}");
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
                tracing::error!(target: "librdkafka", "librdkafka: {} {}", fac, log_message)
            }
            RDKafkaLogLevel::Warning => {
                tracing::warn!(target: "librdkafka", "librdkafka: {} {}", fac, log_message)
            }
            RDKafkaLogLevel::Notice => {
                tracing::info!(target: "librdkafka", "librdkafka: {} {}", fac, log_message)
            }
            RDKafkaLogLevel::Info => {
                tracing::info!(target: "librdkafka", "librdkafka: {} {}", fac, log_message)
            }
            RDKafkaLogLevel::Debug => {
                tracing::debug!(target: "librdkafka", "librdkafka: {} {}", fac, log_message)
            }
        }
    }
}

pub fn build_headers(headers: &Vec<KafkaHeader>) -> OwnedHeaders {
    let mut result = OwnedHeaders::new_with_capacity(headers.len());

    for header in headers {
        result = result.insert(Header {
            key: &header.key,
            value: header.value.as_ref().map(|val| val.0.as_bytes()),
        });
    }

    result
}

pub fn kafka_send<T1, T2, C>(
    producer: &ThreadedProducer<C>,
    topic: &str,
    mut record: BaseRecord<T1, T2, C::DeliveryOpaque>,
) -> AnyResult<()>
where
    T1: ToBytes + ?Sized,
    T2: ToBytes + ?Sized,
    C: ProducerContext,
{
    let mut polling_interval = Duration::from_micros(10);
    let mut start;

    loop {
        match producer.send(record) {
            Ok(()) => return Ok(()),
            Err((e, r)) => match e {
                KafkaError::MessageProduction(e) if is_retriable_send_error(e) => {
                    // Start timing after the first error.
                    start = Some(Instant::now());
                    record = r;

                    // Start warning after hitting max polling interval
                    if polling_interval >= MAX_POLLING_INTERVAL {
                        warn!(
                                "Attempts to send a message to Kafka topic '{}' have failed for {:?}, will keep retrying (error: {e})",
                                &topic, start.unwrap().elapsed()
                            );
                    }

                    sleep(polling_interval);
                    if polling_interval < MAX_POLLING_INTERVAL {
                        polling_interval = min(polling_interval * 2, MAX_POLLING_INTERVAL);
                    }
                }
                _ => bail!("error sending Kafka message: {e}"),
            },
        }
    }
}

fn is_retriable_send_error(error: RDKafkaErrorCode) -> bool {
    error == RDKafkaErrorCode::QueueFull
}

fn is_oauthbearer(config: &BTreeMap<String, String>) -> bool {
    config
        .get("sasl.mechanism")
        .is_some_and(|s| s.eq_ignore_ascii_case("OAUTHBEARER"))
}

fn validate_aws_msk_region(
    kafka_options: &BTreeMap<String, String>,
    region: Option<String>,
) -> AnyResult<Option<String>> {
    if is_oauthbearer(kafka_options) {
        // Try to load the region from the environment, but if it isn't set,
        // load it from the configuration.
        // If both are none, return an error.
        let region = TOKIO.block_on(async {
                aws_config::load_from_env()
                    .await
                    .region()
                    .and_then(|r| {
                        let s = r.to_string();
                        if s.trim().is_empty() {
                            None
                        } else {
                            Some(s)
                        }

                    })
            })
            .or(region)
            .ok_or(
                anyhow!(
            "sasl.mechanism is set to OAUTHBEARER, which only supports AWS MSK for now, but no region set. Consider setting the environment variable `AWS_REGION` or `region` field in Kafka connector configuration."
        ))?;

        if region.trim().is_empty() {
            bail!("region is empty, region must be set to connect to AWS MSK");
        }

        return Ok(Some(region));
    }

    Ok(None)
}

fn generate_oauthbearer_token(
    config: &HashMap<String, String>,
) -> Result<OAuthToken, Box<dyn Error>> {
    if let Some(region) = config.get("region").cloned() {
        let (token, expiration_time_ms) = {
            TOKIO.block_on(async {
                generate_auth_token(aws_types::region::Region::new(region)).await
            })
        }?;

        return Ok(OAuthToken {
            token,
            principal_name: "".to_string(),
            lifetime_ms: expiration_time_ms,
        });
    }

    Ok(OAuthToken {
        token: "".to_string(),
        principal_name: "".to_string(),
        lifetime_ms: i64::MAX,
    })
}
