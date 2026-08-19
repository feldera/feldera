use anyhow::{Error as AnyError, Result as AnyResult, anyhow, bail};
use aws_msk_iam_sasl_signer::generate_auth_token;
use base64::{Engine as _, prelude::BASE64_URL_SAFE_NO_PAD};
use dbsp::circuit::tokio::TOKIO;
use feldera_types::transport::kafka::{KafkaHeader, KafkaLogLevel, KafkaOauthProvider};
use google_cloud_auth::project::{
    Config as GcpAuthConfig, Project as GcpProject, create_token_source_from_project,
    project as gcp_project,
};
use google_cloud_auth::token_source::TokenSource as GcpTokenSource;
use parquet::data_type::AsBytes;
use rdkafka::Statistics;
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
use size_of::HumanBytes;
use std::cmp::min;
use std::collections::BTreeMap;
use std::error::Error;
use std::io::Write;
use std::path::PathBuf;
use std::sync::Arc;
#[cfg(test)]
use std::sync::Mutex;
use std::thread::sleep;
use std::time::SystemTime;
use std::time::{Duration, Instant};
use tracing::{info, warn};

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
                            &topic,
                            start.unwrap().elapsed()
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

/// Which identity provider mints SASL/OAUTHBEARER tokens for a Kafka client,
/// as determined by the connector configuration alone.
///
/// Kept separate from [`OauthbearerAuth`] so that validating a configuration
/// needs no credentials and no network access.
#[derive(Debug, Eq, PartialEq)]
enum OauthbearerProvider {
    /// No OAUTHBEARER configuration.
    None,
    /// Mint an AWS Signature V4 token for AWS MSK.
    AwsMsk {
        /// Region to mint for.
        region: String,
    },
    /// Mint a Google OAuth2 access token for GCP Managed Service for Apache
    /// Kafka, from Application Default Credentials.
    Gcp,
}

/// An OAUTHBEARER identity provider together with the credentials it needs,
/// resolved once when the connector starts.
enum OauthbearerAuth {
    /// No OAUTHBEARER configuration.
    None,
    /// Mint an AWS Signature V4 token for AWS MSK.
    AwsMsk {
        /// Region to mint for.
        region: String,
    },
    /// Mint a Google OAuth2 access token for GCP Managed Service for Apache
    /// Kafka.
    Gcp {
        /// Identity to advertise in the token's `sub` claim.
        principal: String,

        /// Source of access tokens. It hands back an unexpired token without
        /// contacting Google, so refreshes are usually cheap.
        token_source: Arc<dyn GcpTokenSource>,
    },
}

/// Decides which OAUTHBEARER identity provider a Kafka client should use, and
/// validates that its required configuration is present.
fn resolve_oauthbearer_provider(
    kafka_options: &BTreeMap<String, String>,
    oauth_provider: Option<KafkaOauthProvider>,
    region: Option<String>,
) -> AnyResult<OauthbearerProvider> {
    if !is_oauthbearer(kafka_options) {
        return Ok(OauthbearerProvider::None);
    }

    match oauth_provider.unwrap_or_default() {
        KafkaOauthProvider::Gcp => Ok(OauthbearerProvider::Gcp),
        KafkaOauthProvider::Aws => {
            // Try to load the region from the environment, but if it isn't set,
            // load it from the configuration.
            // If both are none, return an error.
            let region = TOKIO
                .block_on(async {
                    aws_config::load_from_env().await.region().and_then(|r| {
                        let s = r.to_string();
                        if s.trim().is_empty() { None } else { Some(s) }
                    })
                })
                .or(region)
                .ok_or(anyhow!(
                    "sasl.mechanism is set to OAUTHBEARER, but no AWS region is set and no other \
                     `oauth_provider` is configured. Consider setting the environment variable \
                     `AWS_REGION` or the `region` field to authenticate to AWS MSK, or setting \
                     `oauth_provider` to `gcp` to authenticate to GCP Managed Service for Apache \
                     Kafka using Application Default Credentials."
                ))?;

            if region.trim().is_empty() {
                bail!("region is empty, region must be set to connect to AWS MSK");
            }

            Ok(OauthbearerProvider::AwsMsk { region })
        }
    }
}

/// Resolves which OAUTHBEARER identity provider a Kafka client should use,
/// and acquires the credentials it needs.
fn resolve_oauthbearer_auth(
    kafka_options: &BTreeMap<String, String>,
    oauth_provider: Option<KafkaOauthProvider>,
    region: Option<String>,
) -> AnyResult<OauthbearerAuth> {
    match resolve_oauthbearer_provider(kafka_options, oauth_provider, region)? {
        OauthbearerProvider::None => Ok(OauthbearerAuth::None),
        OauthbearerProvider::AwsMsk { region } => Ok(OauthbearerAuth::AwsMsk { region }),
        OauthbearerProvider::Gcp => TOKIO.block_on(resolve_gcp_auth()),
    }
}

/// OAuth2 scope requested when minting a Google OAuth2 access token for GCP
/// Managed Service for Apache Kafka.
const GCP_OAUTH_SCOPE: &str = "https://www.googleapis.com/auth/cloud-platform";

/// Header segment of the token envelope built by [`gcp_kafka_token`].
const GCP_TOKEN_HEADER: &str = r#"{"typ":"JWT","alg":"GOOG_OAUTH2_TOKEN"}"#;

/// `scope` claim in the token envelope.  It names the service being accessed,
/// so it is this fixed string rather than [`GCP_OAUTH_SCOPE`].
const GCP_TOKEN_SCOPE_CLAIM: &str = "kafka";

/// Environment variable that supplies the principal to authenticate as,
/// overriding the one derived from the credentials.  Google's own Kafka client
/// reads the same variable.
const GCP_PRINCIPAL_ENV_VAR: &str = "GOOGLE_MANAGED_KAFKA_AUTH_PRINCIPAL";

/// Resolves Application Default Credentials into a token source and a
/// principal, so that bad credentials fail when the connector starts rather
/// than at the first token refresh.
///
/// Credentials come from a service account key or user credentials file (e.g.
/// as set up by `gcloud auth application-default login`), or, failing that,
/// the GKE metadata server (which supplies credentials automatically under
/// Workload Identity).
async fn resolve_gcp_auth() -> AnyResult<OauthbearerAuth> {
    let project = gcp_project()
        .await
        .map_err(|e| anyhow!("failed to resolve Google Application Default Credentials: {e}"))?;
    // Build the token source before resolving the principal, so that
    // credentials this build cannot use at all report that, rather than the
    // missing-principal error they would also provoke.
    let config = GcpAuthConfig::default().with_scopes(&[GCP_OAUTH_SCOPE]);
    let token_source = create_token_source_from_project(&project, config)
        .await
        .map_err(|e| anyhow!("failed to create Google OAuth2 token source: {e}"))?;
    let principal = resolve_gcp_principal(&project).await?;

    Ok(OauthbearerAuth::Gcp {
        principal,
        token_source: Arc::from(token_source),
    })
}

/// Determines the principal to authenticate as.  GCP Managed Service for
/// Apache Kafka requires one, and rejects a token whose `sub` claim is empty.
///
/// Only service account credentials and the metadata server name their
/// principal; for anything else, including the user credentials written by
/// `gcloud auth application-default login`, the caller must supply it through
/// [`GCP_PRINCIPAL_ENV_VAR`].
async fn resolve_gcp_principal(project: &GcpProject) -> AnyResult<String> {
    if let Ok(principal) = std::env::var(GCP_PRINCIPAL_ENV_VAR)
        && !principal.trim().is_empty()
    {
        return Ok(principal);
    }

    match project {
        GcpProject::FromFile(credentials) => credentials
            .client_email
            .clone()
            .filter(|email| !email.trim().is_empty())
            .ok_or_else(|| {
                anyhow!(
                    "Google Application Default Credentials of type `{}` do not name a principal, \
                     which GCP Managed Service for Apache Kafka requires. Set the \
                     `{GCP_PRINCIPAL_ENV_VAR}` environment variable to the principal to \
                     authenticate as, or use service account credentials.",
                    credentials.tp
                )
            }),
        GcpProject::FromMetadataServer(_) => {
            let email = google_cloud_metadata::email("default").await.map_err(|e| {
                anyhow!(
                    "failed to read the default service account email from the GCE metadata \
                     server, which GCP Managed Service for Apache Kafka requires as a principal: \
                     {e}. Consider setting the `{GCP_PRINCIPAL_ENV_VAR}` environment variable \
                     instead."
                )
            })?;
            if email.trim().is_empty() {
                bail!(
                    "the GCE metadata server reported an empty default service account email, \
                     but GCP Managed Service for Apache Kafka requires a principal. Consider \
                     setting the `{GCP_PRINCIPAL_ENV_VAR}` environment variable instead."
                );
            }
            Ok(email)
        }
    }
}

/// Wraps a Google OAuth2 access token in the envelope that GCP Managed Service
/// for Apache Kafka requires: three dot-joined base64url segments, shaped like
/// a JWT but carrying the access token where a signature would go.  The broker
/// rejects a bare access token.
///
/// `expires_at` and `issued_at` are Unix timestamps in seconds.
///
/// Google's own Kafka client builds the same string; see
/// <https://github.com/googleapis/managedkafka/blob/main/kafka-java-auth/src/main/java/com/google/cloud/hosted/kafka/auth/GcpLoginCallbackHandler.java>.
fn gcp_kafka_token(access_token: &str, principal: &str, expires_at: i64, issued_at: i64) -> String {
    let claims = serde_json::json!({
        "exp": expires_at,
        "iat": issued_at,
        "scope": GCP_TOKEN_SCOPE_CLAIM,
        "sub": principal,
    })
    .to_string();

    format!(
        "{}.{}.{}",
        BASE64_URL_SAFE_NO_PAD.encode(GCP_TOKEN_HEADER),
        BASE64_URL_SAFE_NO_PAD.encode(claims),
        BASE64_URL_SAFE_NO_PAD.encode(access_token)
    )
}

/// Number of seconds since the Unix epoch.
fn unix_now_secs() -> AnyResult<i64> {
    Ok(SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .map_err(|e| anyhow!("system clock is set before the Unix epoch: {e}"))?
        .as_secs() as i64)
}

/// Mints a token for GCP Managed Service for Apache Kafka.
async fn fetch_gcp_oauthbearer_token(
    principal: &str,
    token_source: &Arc<dyn GcpTokenSource>,
) -> AnyResult<OAuthToken> {
    let token = token_source
        .token()
        .await
        .map_err(|e| anyhow!("failed to obtain Google OAuth2 access token: {e}"))?;
    let expires_at = token
        .expiry
        .ok_or_else(|| anyhow!("Google OAuth2 access token response is missing an expiry time"))?
        .unix_timestamp();

    Ok(OAuthToken {
        token: gcp_kafka_token(&token.access_token, principal, expires_at, unix_now_secs()?),
        principal_name: principal.to_string(),
        lifetime_ms: expires_at * 1000,
    })
}

fn generate_oauthbearer_token(auth: &OauthbearerAuth) -> Result<OAuthToken, Box<dyn Error>> {
    match auth {
        OauthbearerAuth::None => Ok(OAuthToken {
            token: "".to_string(),
            principal_name: "".to_string(),
            lifetime_ms: i64::MAX,
        }),
        OauthbearerAuth::AwsMsk { region } => {
            let (token, expiration_time_ms) = TOKIO.block_on(async {
                generate_auth_token(aws_types::region::Region::new(region.clone())).await
            })?;

            Ok(OAuthToken {
                token,
                principal_name: "".to_string(),
                lifetime_ms: expiration_time_ms,
            })
        }
        OauthbearerAuth::Gcp {
            principal,
            token_source,
        } => Ok(TOKIO.block_on(fetch_gcp_oauthbearer_token(principal, token_source))?),
    }
}

/// Tracks and reports memory use for a consumer or producer.
struct MemoryUseReporter {
    /// When we were created.
    ///
    /// We don't want to report on memory use for a while afterward, since it
    /// will take some time to reach what we hope is a steady state.
    start: Instant,

    /// The most recently measured memory use, in bytes.
    current: u64,

    /// The peak memory use we last reported, in bytes, and when we reported it.
    ///
    /// This is a peak value: we only ever report a new value when the usage
    /// increases substantially from the previously reported value.
    peak: Option<(Instant, u64)>,
}

impl MemoryUseReporter {
    fn new() -> Self {
        Self {
            start: Instant::now(),
            current: 0,
            peak: None,
        }
    }
    /// The most recent measured memory use in bytes.
    fn current(&self) -> usize {
        self.current as usize
    }
    fn update(&mut self, statistics: &Statistics) {
        /// Minimum time before first report.
        const REPORT_DELAY: Duration = Duration::from_secs(60);

        /// Minimum amount of memory to report on.
        const MIN_MEMORY: u64 = 1024 * 1024;

        let mut memory = 0;
        for topic in statistics.topics.values() {
            for partition in topic.partitions.values() {
                memory += partition.msgq_bytes + partition.xmit_msgq_bytes + partition.fetchq_size;
            }
        }
        self.current = memory;
        if self.start.elapsed() < REPORT_DELAY {
            return;
        }

        match &self.peak {
            None if memory > MIN_MEMORY => {
                info!(
                    "Buffered {} after {} seconds",
                    HumanBytes::new(memory),
                    self.start.elapsed().as_secs()
                );
            }
            Some((last_time, last_memory)) if memory > *last_memory * 3 / 2 => {
                info!(
                    "Buffers grew {:.0}%, from {} to {}, in last {} seconds",
                    memory as f64 / *last_memory as f64 * 100.0 - 100.0,
                    HumanBytes::new(*last_memory),
                    HumanBytes::new(memory),
                    last_time.elapsed().as_secs()
                );
            }
            _ => return,
        }
        self.peak = Some((Instant::now(), memory));
    }
}

#[cfg(test)]
mod oauthbearer_tests {
    use super::{
        GCP_PRINCIPAL_ENV_VAR, GCP_TOKEN_HEADER, GcpProject, KafkaOauthProvider,
        OauthbearerProvider, TOKIO, gcp_kafka_token, resolve_gcp_principal,
        resolve_oauthbearer_provider,
    };
    use base64::{Engine as _, prelude::BASE64_URL_SAFE_NO_PAD};
    use std::collections::BTreeMap;

    fn oauthbearer_options() -> BTreeMap<String, String> {
        BTreeMap::from([("sasl.mechanism".to_string(), "OAUTHBEARER".to_string())])
    }

    /// Isolates the AWS region lookup from the environment: clears
    /// `AWS_REGION` and points the profile-file variables at paths that
    /// don't exist, so a developer's or CI machine's `~/.aws/config` can't
    /// make these tests flaky.
    fn clear_aws_region_env() {
        unsafe {
            std::env::remove_var("AWS_REGION");
            std::env::remove_var("AWS_DEFAULT_REGION");
            std::env::remove_var("AWS_PROFILE");
            std::env::set_var("AWS_CONFIG_FILE", "/nonexistent-feldera-test-aws-config");
            std::env::set_var(
                "AWS_SHARED_CREDENTIALS_FILE",
                "/nonexistent-feldera-test-aws-credentials",
            );
        }
    }

    #[test]
    fn non_oauthbearer_mechanism_is_ignored() {
        let options = BTreeMap::from([("sasl.mechanism".to_string(), "PLAIN".to_string())]);
        let provider =
            resolve_oauthbearer_provider(&options, Some(KafkaOauthProvider::Aws), None).unwrap();
        assert_eq!(provider, OauthbearerProvider::None);
    }

    #[test]
    fn gcp_provider_does_not_require_a_region() {
        let provider = resolve_oauthbearer_provider(
            &oauthbearer_options(),
            Some(KafkaOauthProvider::Gcp),
            None,
        )
        .unwrap();
        assert_eq!(provider, OauthbearerProvider::Gcp);
    }

    #[test]
    #[serial_test::serial]
    fn aws_provider_uses_the_configured_region_when_no_env_var_is_set() {
        clear_aws_region_env();

        let provider = resolve_oauthbearer_provider(
            &oauthbearer_options(),
            Some(KafkaOauthProvider::Aws),
            Some("us-east-1".to_string()),
        )
        .unwrap();
        assert_eq!(
            provider,
            OauthbearerProvider::AwsMsk {
                region: "us-east-1".to_string()
            }
        );
    }

    #[test]
    #[serial_test::serial]
    fn missing_provider_defaults_to_aws_for_backward_compatibility() {
        clear_aws_region_env();

        let provider = resolve_oauthbearer_provider(
            &oauthbearer_options(),
            None,
            Some("eu-west-1".to_string()),
        )
        .unwrap();
        assert_eq!(
            provider,
            OauthbearerProvider::AwsMsk {
                region: "eu-west-1".to_string()
            }
        );
    }

    #[test]
    #[serial_test::serial]
    fn aws_provider_without_a_region_is_an_error() {
        clear_aws_region_env();

        let result = resolve_oauthbearer_provider(
            &oauthbearer_options(),
            Some(KafkaOauthProvider::Aws),
            None,
        );
        assert!(result.is_err());
    }

    fn decode_segment(segment: &str) -> String {
        String::from_utf8(BASE64_URL_SAFE_NO_PAD.decode(segment).unwrap()).unwrap()
    }

    /// GCP Managed Kafka rejects a bare access token; it must arrive wrapped
    /// in a JWT-shaped envelope whose third segment is the access token.
    /// Reverting `gcp_kafka_token` to return `access_token` fails this test.
    #[test]
    fn gcp_token_wraps_the_access_token_in_an_envelope() {
        let token = gcp_kafka_token(
            "ya29.the-access-token",
            "svc@proj.iam.gserviceaccount.com",
            1_700_000_060,
            1_700_000_000,
        );

        let segments: Vec<&str> = token.split('.').collect();
        assert_eq!(segments.len(), 3, "expected three dot-joined segments");

        assert_eq!(decode_segment(segments[0]), GCP_TOKEN_HEADER);
        assert_eq!(
            serde_json::from_str::<serde_json::Value>(&decode_segment(segments[1])).unwrap(),
            serde_json::json!({
                "exp": 1_700_000_060,
                "iat": 1_700_000_000,
                "scope": "kafka",
                "sub": "svc@proj.iam.gserviceaccount.com",
            })
        );
        assert_eq!(decode_segment(segments[2]), "ya29.the-access-token");
    }

    /// The segments must be base64url without padding: the standard alphabet
    /// and `=` padding both make the broker reject the token.  The UTF-8 bytes
    /// of this access token are `c3bfc3bf`, which standard base64 renders as
    /// `w7/Dvw==` -- differing from the expected value in both respects.
    #[test]
    fn gcp_token_segments_are_unpadded_base64url() {
        let token = gcp_kafka_token("\u{ff}\u{ff}", "svc@proj.iam.gserviceaccount.com", 1, 2);
        let access_token_segment = token.split('.').nth(2).unwrap();

        assert_eq!(access_token_segment, "w7_Dvw");
        assert!(!token.contains('='), "padding must be stripped: {token}");
    }

    /// The `sub` claim carries the principal, which the broker requires.
    #[test]
    fn gcp_token_carries_the_principal_in_the_sub_claim() {
        let token = gcp_kafka_token("access-token", "someone@example.com", 1, 2);
        let claims: serde_json::Value =
            serde_json::from_str(&decode_segment(token.split('.').nth(1).unwrap())).unwrap();
        assert_eq!(claims["sub"], "someone@example.com");
    }

    fn service_account_credentials(client_email: Option<&str>) -> GcpProject {
        let mut json = serde_json::json!({"type": "service_account"});
        if let Some(client_email) = client_email {
            json["client_email"] = client_email.into();
        }
        GcpProject::FromFile(Box::new(serde_json::from_value(json).unwrap()))
    }

    fn clear_gcp_principal_env() {
        unsafe { std::env::remove_var(GCP_PRINCIPAL_ENV_VAR) };
    }

    #[test]
    #[serial_test::serial]
    fn principal_comes_from_the_service_account_email() {
        clear_gcp_principal_env();

        let project = service_account_credentials(Some("svc@proj.iam.gserviceaccount.com"));
        let principal = TOKIO.block_on(resolve_gcp_principal(&project)).unwrap();
        assert_eq!(principal, "svc@proj.iam.gserviceaccount.com");
    }

    /// User credentials, as written by `gcloud auth application-default
    /// login`, name no principal, so the broker's requirement can only be met
    /// through the environment variable.
    #[test]
    #[serial_test::serial]
    fn credentials_without_a_principal_are_an_error_naming_the_env_var() {
        clear_gcp_principal_env();

        let project = GcpProject::FromFile(Box::new(
            serde_json::from_value(serde_json::json!({"type": "authorized_user"})).unwrap(),
        ));
        let error = TOKIO
            .block_on(resolve_gcp_principal(&project))
            .unwrap_err()
            .to_string();
        assert!(
            error.contains(GCP_PRINCIPAL_ENV_VAR),
            "error should name the environment variable: {error}"
        );
    }

    #[test]
    #[serial_test::serial]
    fn env_var_overrides_the_credentials_principal() {
        unsafe { std::env::set_var(GCP_PRINCIPAL_ENV_VAR, "override@example.com") };

        let project = service_account_credentials(Some("svc@proj.iam.gserviceaccount.com"));
        let principal = TOKIO.block_on(resolve_gcp_principal(&project)).unwrap();

        clear_gcp_principal_env();
        assert_eq!(principal, "override@example.com");
    }

    /// An empty variable must not shadow a principal the credentials name.
    #[test]
    #[serial_test::serial]
    fn empty_env_var_falls_back_to_the_credentials_principal() {
        unsafe { std::env::set_var(GCP_PRINCIPAL_ENV_VAR, "") };

        let project = service_account_credentials(Some("svc@proj.iam.gserviceaccount.com"));
        let principal = TOKIO.block_on(resolve_gcp_principal(&project)).unwrap();

        clear_gcp_principal_env();
        assert_eq!(principal, "svc@proj.iam.gserviceaccount.com");
    }
}
