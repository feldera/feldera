use actix_http::header::{HeaderName, HeaderValue};
use awc::ClientRequest;
use reqwest::RequestBuilder;
use sentry::{ClientInitGuard, TransactionContext};
use std::borrow::Cow;
use std::env;

/// Initializes Sentry if `FELDERA_SENTRY_ENABLED` is set, tagging the service and release.
///
/// Use dsn as the default DSN, which can be overridden by the `SENTRY_DSN` environment variable.
pub fn init(dsn: &str, service_name: &str, release: &str) -> Option<ClientInitGuard> {
    env::var("FELDERA_SENTRY_ENABLED")
        .ok()
        .filter(|v| v == "1")?;

    const DEFAULT_SAMPLE_RATE: f32 = 1.0;
    const DEFAULT_TRACE_SAMPLE_RATE: f32 = 0.1;
    const MAX_BREADCRUMBS: usize = 10;

    let dsn = env::var("SENTRY_DSN").ok().unwrap_or(dsn.to_string());
    let sample_rate = env::var("SENTRY_SAMPLE_RATE")
        .ok()
        .and_then(|rate| rate.parse::<f32>().ok())
        .unwrap_or(DEFAULT_SAMPLE_RATE);
    let traces_sample_rate = env::var("SENTRY_TRACES_SAMPLE_RATE")
        .ok()
        .and_then(|rate| rate.parse::<f32>().ok())
        .unwrap_or(DEFAULT_TRACE_SAMPLE_RATE);
    let environment = env::var("SENTRY_ENVIRONMENT")
        .unwrap_or_else(|_| String::from("dev"))
        .into();
    let accept_invalid_certs = environment == "ci" || environment == "dev";
    let max_breadcrumbs = env::var("SENTRY_MAX_BREADCRUMBS")
        .ok()
        .and_then(|rate| rate.parse::<usize>().ok())
        .unwrap_or(MAX_BREADCRUMBS);

    let guard = sentry::init((
        dsn,
        sentry::ClientOptions {
            environment: Some(environment),
            release: Some(Cow::Owned(release.to_string())),
            max_breadcrumbs,
            sample_rate,
            traces_sample_rate,
            enable_logs: true,
            attach_stacktrace: true,
            accept_invalid_certs,
            ..Default::default()
        },
    ));

    sentry::configure_scope(|scope| scope.set_tag("service", service_name));
    Some(guard)
}

fn sentry_enabled() -> bool {
    sentry::Hub::current().client().is_some()
}

/// Returns an Actix middleware that captures errors and traces when Sentry is enabled.
pub fn actix_middleware() -> sentry::integrations::actix::Sentry {
    sentry::integrations::actix::Sentry::builder()
        .emit_header(true)
        .capture_server_errors(true)
        .start_transaction(true)
        .finish()
}

fn trace_header_value() -> Option<String> {
    if !sentry_enabled() {
        return None;
    }

    let mut header = None;
    sentry::configure_scope(|scope| {
        if let Some(span) = scope.get_span() {
            header = span.iter_headers().next().map(|(_, value)| value);
        }
    });
    if header.is_none() {
        let transaction =
            sentry::start_transaction(TransactionContext::new("http.client", "http.client"));
        header = transaction.iter_headers().next().map(|(_, value)| value);
        transaction.finish();
    }
    header
}

/// Adds Sentry trace headers to outgoing awc requests.
pub trait AwcRequestTracingExt {
    fn with_sentry_tracing(self) -> Self;
}

impl AwcRequestTracingExt for ClientRequest {
    fn with_sentry_tracing(mut self) -> Self {
        if let Some(value) = trace_header_value() {
            if let Ok(header_value) = HeaderValue::from_str(&value) {
                self = self.insert_header((HeaderName::from_static("sentry-trace"), header_value));
            }
        }
        self
    }
}

/// Adds Sentry trace headers to outgoing reqwest requests.
pub trait ReqwestTracingExt {
    fn with_sentry_tracing(self) -> Self;
}

impl ReqwestTracingExt for RequestBuilder {
    fn with_sentry_tracing(self) -> Self {
        if let Some(value) = trace_header_value() {
            if let Ok(header_value) = reqwest::header::HeaderValue::from_str(&value) {
                return self.header(
                    reqwest::header::HeaderName::from_static("sentry-trace"),
                    header_value,
                );
            }
        }
        self
    }
}
