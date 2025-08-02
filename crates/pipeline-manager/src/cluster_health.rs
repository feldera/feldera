use chrono::{DateTime, Utc};
use log::{info, warn};
use serde::Serialize;
use std::{sync::Arc, time::Duration};
use tokio::sync::RwLock;
use utoipa::ToSchema;

use crate::config::CommonConfig;

/// Interval between checking health again.
const POLL_INTERVAL: Duration = Duration::from_secs(10);

/// Default HTTP request timeout to use
const DEFAULT_REQUEST_TIMEOUT: Duration = Duration::from_secs(10);

#[derive(Debug, Clone, Serialize, PartialEq, ToSchema, Default)]
pub struct HealthStatus {
    pub runner: ServiceStatus,
    pub compiler: ServiceStatus,
}

impl HealthStatus {
    fn is_equivalent_health(&self, other: &Self) -> bool {
        self.runner.is_equivalent_health(&other.runner)
            && self.compiler.is_equivalent_health(&other.compiler)
    }
}

#[derive(Debug, Clone, Serialize, PartialEq, ToSchema)]
pub struct ServiceStatus {
    pub healthy: bool,
    pub message: String,
    pub unchanged_since: DateTime<Utc>,
    pub checked_at: DateTime<Utc>,
}

impl Default for ServiceStatus {
    fn default() -> Self {
        Self {
            healthy: false,
            message: String::new(),
            unchanged_since: Utc::now(),
            checked_at: Utc::now(),
        }
    }
}

impl ServiceStatus {
    fn is_equivalent_health(&self, other: &Self) -> bool {
        self.healthy == other.healthy && self.message == other.message
    }
}

async fn poll_service(
    service_name: &str,
    url: &str,
    client: &reqwest::Client,
    timeout: std::time::Duration,
) -> (bool, String) {
    match client.get(url).timeout(timeout).send().await {
        Ok(resp) if resp.status().is_success() => (
            true,
            format!("Healthy: The {service_name} service responded successfully to the last health check."),
        ),
        Ok(resp) => {
            let status = resp.status();
            let message = resp.json::<serde_json::Value>().await.map_or_else(
                |_| format!(
                    "Unhealthy: {service_name} at {url} responded with HTTP {status} and an invalid JSON body. \
                     Please check the {service_name} logs for error details."
                ),
                |v| format!(
                    "Unhealthy: {service_name} at {url} responded with HTTP {status} and body: {}. \
                     Please check the {service_name} logs for more information.",
                    v
                ),
            );
            (false, message)
        }
        Err(e) if e.is_connect() => (
            false,
            format!(
                "Unreachable: Unable to connect to the {service_name} at {url}. This likely means the service \
                 is not running, has crashed, or is not listening on the expected port. Underlying connection error: {}. \
                 Please ensure that the {service_name} is running and check its logs for details.",
                error_source(&e)
            ),
        ),
        Err(e) if e.is_timeout() => (
            false,
            format!(
                "Timeout: The health check request to {service_name} at {url} did not respond within {} seconds. \
                 This usually means the service is running, but it is overloaded, unresponsive, or stuck processing. \
                 Please check the {service_name} logs for any errors or performance issues. Timeout error: {}.",
                timeout.as_secs(),
                error_source(&e)
            ),
        ),
        Err(e) => (
            false,
            format!(
                "Error: An unexpected error occurred while checking the health of {service_name} at {url}: {e}, \
                 source: {}. Please check the {service_name} logs for more information.",
                error_source(&e)
            ),

        ),
    }
}

pub async fn regular_health_check(
    status: Arc<RwLock<Option<HealthStatus>>>,
    common_config: CommonConfig,
    request_timeout: Option<Duration>,
) {
    let protocol = match common_config.enable_https {
        true => "https",
        _ => "http",
    };

    let runner_url = format!(
        "{protocol}://{}:{}/healthz",
        common_config.runner_host, common_config.runner_port
    );
    let compiler_url = format!(
        "{protocol}://{}:{}/healthz",
        common_config.compiler_host, common_config.compiler_port
    );

    let client = common_config.reqwest_client().await;

    let mut last_status: HealthStatus = HealthStatus::default();
    let request_timeout = request_timeout.unwrap_or(DEFAULT_REQUEST_TIMEOUT);

    loop {
        let (runner_ok, runner_msg) =
            poll_service("runner", &runner_url, &client, request_timeout).await;
        let (compiler_ok, compiler_msg) =
            poll_service("compiler", &compiler_url, &client, request_timeout).await;

        let runner_status = build_status(runner_ok, runner_msg, &last_status.runner);
        let compiler_status = build_status(compiler_ok, compiler_msg, &last_status.compiler);

        let current_status = HealthStatus {
            runner: runner_status,
            compiler: compiler_status,
        };

        let is_status_changed = !last_status.is_equivalent_health(&current_status);

        let should_log =
            is_status_changed || !current_status.runner.healthy || !current_status.compiler.healthy;

        if should_log {
            match (
                &current_status.runner.healthy,
                &current_status.compiler.healthy,
            ) {
                (true, true) => {
                    info!("Health check OK: runner and compiler are healthy.");
                }
                (runner_ok, compiler_ok) => {
                    if !runner_ok {
                        warn!(
                            "Runner unhealthy. reason: {} (since {})",
                            current_status.runner.message,
                            current_status.runner.unchanged_since.to_rfc3339()
                        );
                    } else if !last_status.runner.healthy {
                        info!(
                            "Runner recovered and is now healthy (as of {})",
                            current_status.runner.unchanged_since.to_rfc3339()
                        );
                    }

                    if !compiler_ok {
                        warn!(
                            "Compiler unhealthy. reason: {} (since {})",
                            current_status.compiler.message,
                            current_status.compiler.unchanged_since.to_rfc3339()
                        );
                    } else if !last_status.compiler.healthy {
                        info!(
                            "Compiler recovered and is now healthy (as of {})",
                            current_status.compiler.unchanged_since.to_rfc3339()
                        );
                    }
                }
            }
        }

        {
            let mut write_guard = status.write().await;
            *write_guard = Some(current_status.clone());
        }

        last_status = current_status;

        tokio::time::sleep(POLL_INTERVAL).await;
    }
}

// Helper function: if the state changed, update timestamp; if not, preserve it.
fn build_status(ok: bool, msg: String, prev: &ServiceStatus) -> ServiceStatus {
    let state_changed = prev.healthy != ok || prev.message != msg;
    let now = Utc::now();
    ServiceStatus {
        healthy: ok,
        message: msg,
        unchanged_since: if state_changed {
            now
        } else {
            prev.unchanged_since
        },
        checked_at: now,
    }
}

// Helper function: get the source of error
fn error_source(mut err: &dyn std::error::Error) -> String {
    while let Some(src) = err.source() {
        err = src;
    }
    format!("{}", err)
}
