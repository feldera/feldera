use crate::config::CommonConfig;
use crate::db::error::DBError;
use crate::db::storage::Storage;
use crate::db::storage_postgres::StoragePostgres;
use crate::db::types::monitor::{MonitorStatus, NewClusterMonitorEvent};
use crate::error::source_error;
use async_trait::async_trait;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex;
use tokio::time::Instant;
use tracing::{error, info};
use uuid::Uuid;

/// Interval at which the monitor occurs to check the current status.
const MONITOR_INTERVAL: Duration = Duration::from_secs(10);

/// Number of `MONITOR_INTERVAL`s within which the monitor writes an event.
const MONITOR_STORE_EVENT_NUM_INTERVALS: u64 = 60;

/// Interval within which the monitor writes an event, healthy or not: the unhealthy backoff
/// is capped at the same duration. Measured in elapsed time rather than in iterations, so
/// slow polling cannot stretch it.
const MONITOR_MAX_WRITE_INTERVAL: Duration =
    Duration::from_secs(MONITOR_INTERVAL.as_secs() * MONITOR_STORE_EVENT_NUM_INTERVALS);

/// An event older than this means the monitor stopped writing. In the enterprise edition it
/// runs within the runner process, the first suspect there. Three write intervals leave room
/// for one slow iteration and a restart, so a slow cluster does not read as a dead monitor.
pub const MONITOR_STALE_AFTER: Duration =
    Duration::from_secs(3 * MONITOR_MAX_WRITE_INTERVAL.as_secs());

// The maximum retention duration and number of events.
// Suppose we want to use at most 200 MiB in the database for storing these,
// then we can allow events up to 200 MiB / 1000 ~= 205 KiB.
pub const MONITOR_RETENTION_HOURS: u16 = 72; // 72 hours / 10 minutes = 432 events
pub const MONITOR_RETENTION_NUM: u16 = 1000;

/// The self-provided information by each service is capped to prevent the database row becoming
/// too large. Unicode characters are at most 4 bytes. As such, this should be at most 32 KiB.
/// We have 6 information entries, as such the row should not exceed the 205 KiB maximum we set
/// above.
const INFO_MAXIMUM_NUM_CHARS: usize = 8192;

/// Window after the monitor starts in which a service that does not answer yet is still
/// considered to be starting up.
const STARTUP_GRACE: Duration = Duration::from_secs(60);

/// Default HTTP request timeout to use
const DEFAULT_REQUEST_TIMEOUT: Duration = Duration::from_secs(5);

/// Message when the resources information is not available.
const RESOURCES_INFO_NOT_AVAILABLE: &str =
    "Resources information not available in Community edition.";

/// Message when the resources information gathering is not enabled.
const RESOURCES_INFO_DISABLED: &str =
    "Cluster monitoring resources information is disabled in the configuration.";

/// Target to poll resources of.
pub enum PollResourcesTarget {
    Api,
    Compiler,
    Runner,
}

#[async_trait]
pub trait ResourcesPoller {
    async fn poll_resources(&mut self, target: PollResourcesTarget) -> (MonitorStatus, String);
}

/// Indefinitely monitor the local cluster by polling the endpoints.
pub async fn cluster_monitor<P: ResourcesPoller>(
    db: Arc<Mutex<StoragePostgres>>,
    common_config: CommonConfig,
    mut resources_poller: P,
) {
    // Cluster monitoring should be enabled if this non-returning function is called
    info!("Cluster monitor is starting");

    // Determine URLs of the services for their self-reporting
    let protocol = if common_config.enable_https {
        "https"
    } else {
        "http"
    };
    let api_url = format!(
        "{protocol}://{}:{}/healthz",
        common_config.api_host, common_config.api_port
    );
    // `check_storage` additionally fails on storage pressure of the compiler
    // working directory, which would make binary uploads fail with ENOSPC.
    let compiler_url = format!(
        "{protocol}://{}:{}/healthz?check_storage=true",
        common_config.compiler_host, common_config.compiler_port
    );
    let runner_url = format!(
        "{protocol}://{}:{}/healthz",
        common_config.runner_host, common_config.runner_port
    );

    // Indefinitely loop checking status
    let client = common_config.reqwest_client().await;
    let started_at = Instant::now();
    let mut last_write: Option<Instant> = None;
    let mut backoff = MONITOR_INTERVAL;
    loop {
        // Retrieve the latest event
        let latest_event = db
            .lock()
            .await
            .get_latest_cluster_monitor_event_extended()
            .await;
        let latest_event = match latest_event {
            Ok(latest_event) => Some(latest_event),
            Err(e) => {
                if matches!(e, DBError::NoClusterMonitorEventsAvailable) {
                    None
                } else {
                    error!(
                        "Cluster monitor cannot perform monitoring because it is unable to retrieve the latest event due to: {e}"
                    );
                    tokio::time::sleep(MONITOR_INTERVAL).await;
                    continue;
                }
            }
        };

        // Perform polling for self-reported info
        let (api_self_ok, api_self_info) =
            poll_service_health_endpoint("api", &api_url, &client).await;
        let (compiler_self_ok, compiler_self_info) =
            poll_service_health_endpoint("compiler", &compiler_url, &client).await;
        let (runner_self_ok, runner_self_info) =
            poll_service_health_endpoint("runner", &runner_url, &client).await;
        let api_self_info = truncate_info(api_self_info);
        let compiler_self_info = truncate_info(compiler_self_info);
        let runner_self_info = truncate_info(runner_self_info);

        // Perform polling of the resources backing the services
        let (
            api_resources_status,
            compiler_resources_status,
            runner_resources_status,
            api_resources_info,
            compiler_resources_info,
            runner_resources_info,
        ) = if common_config.disable_cluster_monitor_resources {
            (
                MonitorStatus::Healthy,
                MonitorStatus::Healthy,
                MonitorStatus::Healthy,
                RESOURCES_INFO_DISABLED.to_string(),
                RESOURCES_INFO_DISABLED.to_string(),
                RESOURCES_INFO_DISABLED.to_string(),
            )
        } else {
            let (api_resources_status, api_resources_info) = resources_poller
                .poll_resources(PollResourcesTarget::Api)
                .await;
            let (compiler_resources_status, compiler_resources_info) = resources_poller
                .poll_resources(PollResourcesTarget::Compiler)
                .await;
            let (runner_resources_status, runner_resources_info) = resources_poller
                .poll_resources(PollResourcesTarget::Runner)
                .await;
            (
                api_resources_status,
                compiler_resources_status,
                runner_resources_status,
                truncate_info(api_resources_info),
                truncate_info(compiler_resources_info),
                truncate_info(runner_resources_info),
            )
        };

        let within_startup_grace = started_at.elapsed() < STARTUP_GRACE;
        let api_status = poll_to_status(api_self_ok, api_resources_status, within_startup_grace);
        let compiler_status = poll_to_status(
            compiler_self_ok,
            compiler_resources_status,
            within_startup_grace,
        );
        let runner_status = poll_to_status(
            runner_self_ok,
            runner_resources_status,
            within_startup_grace,
        );

        // Whether to insert the event into the database
        let insert_into_database = match (&latest_event, last_write) {
            (Some(latest_event), Some(last_write)) => {
                let changed = (
                    latest_event.api_status,
                    latest_event.compiler_status,
                    latest_event.runner_status,
                ) != (api_status, compiler_status, runner_status);
                let healthy = [api_status, compiler_status, runner_status]
                    .into_iter()
                    .all(|status| status == MonitorStatus::Healthy);
                let (insert, next_backoff) =
                    write_decision(last_write.elapsed(), changed, healthy, backoff);
                backoff = next_backoff;
                insert
            }
            // No event yet, or this monitor has not written one since it started
            _ => true,
        };

        // Only insert into the database if required
        if insert_into_database {
            // Count the attempt, not the outcome, so that a failed write waits for the
            // next scheduled one rather than retrying on every iteration against a
            // database that is already struggling. This costs one reading: three failures
            // in a row push the newest event past MONITOR_STALE_AFTER, and
            // `cluster_healthz` then calls the cluster stale while this monitor is still
            // running. Record the time of a successful insert instead if that ever
            // matters more.
            last_write = Some(Instant::now());

            // Insert new event
            let stored = if let Err(e) = db
                .lock()
                .await
                .new_cluster_monitor_event(
                    Uuid::now_v7(),
                    NewClusterMonitorEvent {
                        api_status,
                        api_self_info,
                        api_resources_info,
                        compiler_status,
                        compiler_self_info,
                        compiler_resources_info,
                        runner_status,
                        runner_self_info,
                        runner_resources_info,
                    },
                )
                .await
            {
                error!("Cluster monitor is unable to store event due to: {e}");
                false
            } else {
                true
            };

            // Clean up events that no longer need to be retained
            if stored
                && let Err(e) = db
                    .lock()
                    .await
                    .delete_cluster_monitor_events_beyond_retention(
                        MONITOR_RETENTION_HOURS,
                        MONITOR_RETENTION_NUM,
                    )
                    .await
            {
                error!("Cluster monitor is unable to clean up based on retention due to: {e}");
            }
        }

        // Sleep till next monitor attempt
        tokio::time::sleep(MONITOR_INTERVAL).await;
    }
}

/// Truncate the information message to a maximum number of Unicode characters.
fn truncate_info(mut info: String) -> String {
    if info.chars().count() > INFO_MAXIMUM_NUM_CHARS {
        info = info.chars().take(INFO_MAXIMUM_NUM_CHARS).collect();
        info.push_str(" (truncated due to exceeding maximum number of characters)");
    }
    info
}

/// Polls the service health endpoint, which is used as the service reporting its own status
/// information.
async fn poll_service_health_endpoint(
    service_name: &str,
    url: &str,
    client: &reqwest::Client,
) -> (bool, String) {
    match client
        .get(url)
        .timeout(DEFAULT_REQUEST_TIMEOUT)
        .send()
        .await
    {
        Ok(resp) if resp.status().is_success() => (
            true,
            format!(
                "Healthy: The {service_name} service responded successfully to the last health check."
            ),
        ),
        Ok(resp) => {
            let status = resp.status();
            let message = resp.json::<serde_json::Value>().await.map_or_else(
                |_| format!(
                    "Unhealthy: {service_name} at {url} responded with HTTP {status} and an invalid JSON body. \
                     Please check the {service_name} logs for error details."
                ),
                |v| format!(
                    "Unhealthy: {service_name} at {url} responded with HTTP {status} and body: {v}. \
                     Please check the {service_name} logs for more information.",
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
                source_error(&e)
            ),
        ),
        Err(e) if e.is_timeout() => (
            false,
            format!(
                "Timeout: The health check request to {service_name} at {url} did not respond within {} seconds. \
                 This usually means the service is running, but it is overloaded, unresponsive, or stuck processing. \
                 Please check the {service_name} logs for any errors or performance issues. Timeout error: {}.",
                DEFAULT_REQUEST_TIMEOUT.as_secs(),
                source_error(&e)
            ),
        ),
        Err(e) => (
            false,
            format!(
                "Error: An unexpected error occurred while checking the health of {service_name} at {url}: {e}, \
                 source: {}. Please check the {service_name} logs for more information.",
                source_error(&e)
            ),
        ),
    }
}

/// Whether to write an event now, and the unhealthy backoff to use next. A cluster whose
/// status is unchanged is written within `MONITOR_MAX_WRITE_INTERVAL`, an unhealthy one sooner
/// with exponential backoff, and any change to the status immediately.
fn write_decision(
    since_last_write: Duration,
    changed: bool,
    healthy: bool,
    backoff: Duration,
) -> (bool, Duration) {
    if changed {
        (true, MONITOR_INTERVAL)
    } else if healthy {
        (
            since_last_write >= MONITOR_MAX_WRITE_INTERVAL,
            MONITOR_INTERVAL,
        )
    } else if since_last_write >= backoff {
        (true, std::cmp::min(backoff * 2, MONITOR_MAX_WRITE_INTERVAL))
    } else {
        (false, backoff)
    }
}

fn poll_to_status(
    self_ok: bool,
    resources_status: MonitorStatus,
    within_startup_grace: bool,
) -> MonitorStatus {
    match resources_status {
        MonitorStatus::Healthy if !self_ok && within_startup_grace => MonitorStatus::Transitioning,
        MonitorStatus::Healthy if !self_ok => MonitorStatus::Unhealthy,
        status => status,
    }
}

/// Poller for local resources.
pub struct LocalResourcesPoller {}

#[async_trait]
impl ResourcesPoller for LocalResourcesPoller {
    /// The local resources cannot be polled, as such it returns a default message indicating as such.
    async fn poll_resources(&mut self, _target: PollResourcesTarget) -> (MonitorStatus, String) {
        (
            MonitorStatus::Healthy,
            RESOURCES_INFO_NOT_AVAILABLE.to_string(),
        )
    }
}

#[cfg(test)]
mod test {
    use super::poll_to_status;
    use crate::db::types::monitor::MonitorStatus;

    #[test]
    fn transitioning_resources_are_reported_as_such() {
        assert_eq!(
            poll_to_status(true, MonitorStatus::Transitioning, false),
            MonitorStatus::Transitioning
        );
        assert_eq!(
            poll_to_status(false, MonitorStatus::Transitioning, false),
            MonitorStatus::Transitioning
        );
    }

    #[test]
    fn a_ready_service_that_does_not_answer_is_unhealthy() {
        assert_eq!(
            poll_to_status(false, MonitorStatus::Healthy, false),
            MonitorStatus::Unhealthy
        );
        assert_eq!(
            poll_to_status(false, MonitorStatus::Healthy, true),
            MonitorStatus::Transitioning
        );
    }

    #[test]
    fn unhealthy_resources_stay_unhealthy() {
        assert_eq!(
            poll_to_status(true, MonitorStatus::Unhealthy, true),
            MonitorStatus::Unhealthy
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A cluster whose status is unchanged is written once per write interval, measured in
    /// elapsed time so that slow polling cannot push it past `MONITOR_STALE_AFTER`. A change
    /// in status is written at once.
    #[test]
    fn write_decision_follows_elapsed_time() {
        let unchanged = |since| write_decision(since, false, true, MONITOR_INTERVAL).0;
        assert!(!unchanged(Duration::ZERO));
        assert!(!unchanged(MONITOR_MAX_WRITE_INTERVAL - MONITOR_INTERVAL));
        assert!(unchanged(MONITOR_MAX_WRITE_INTERVAL));

        for healthy in [true, false] {
            assert_eq!(
                write_decision(Duration::ZERO, true, healthy, MONITOR_MAX_WRITE_INTERVAL),
                (true, MONITOR_INTERVAL)
            );
        }
    }

    /// An unhealthy cluster is written with exponential backoff, capped at the write interval.
    #[test]
    fn unhealthy_cluster_backs_off_up_to_the_write_interval() {
        let mut backoff = MONITOR_INTERVAL;
        let mut intervals = vec![];
        for _ in 0..10 {
            let (insert, next_backoff) = write_decision(backoff, false, false, backoff);
            assert!(insert);
            intervals.push(backoff);
            backoff = next_backoff;
        }
        assert_eq!(
            intervals,
            [10, 20, 40, 80, 160, 320, 600, 600, 600, 600].map(Duration::from_secs)
        );

        // Before the backoff elapses there is nothing to write, and the backoff holds.
        assert_eq!(
            write_decision(Duration::from_secs(9), false, false, MONITOR_INTERVAL),
            (false, MONITOR_INTERVAL)
        );
    }
}
