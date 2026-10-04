//! Pipeline list rows built from `GET /v0/pipelines`.

use serde_json::Value;

use super::json;
use super::text::terminal_safe;

/// Traffic-light classification of a status label, used for coloring.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Health {
    Good,
    Busy,
    Idle,
    Bad,
}

/// Classify any deployment status label the server may send.
pub fn deployment_health(status: &str) -> Health {
    match status {
        "Running" => Health::Good,
        "Stopped" | "Suspended" => Health::Idle,
        status if status.contains("Error") || status == "Unavailable" => Health::Bad,
        _ => Health::Busy,
    }
}

/// Classify any program compilation status label.
pub fn program_health(status: &str) -> Health {
    match status {
        "Success" => Health::Good,
        status if status.contains("Error") => Health::Bad,
        _ => Health::Busy,
    }
}

/// One row of the pipelines table.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct PipelineRow {
    pub name: String,
    pub tags: Vec<String>,
    pub deployment_status: String,
    /// Where the deployment is headed; differs from the status mid-transition.
    pub desired_status: Option<String>,
    pub runtime_status: Option<String>,
    pub program_status: String,
    pub storage_status: String,
    /// Platform the pipeline binary was built with, e.g. `0.338.0+enterprise`.
    pub platform_version: String,
    /// Configuration version; bumps whenever the pipeline is edited.
    pub version: i64,
    /// Configured storage quota in bytes, from
    /// `runtime_config.resources.storage_mb_max`; `None` without a quota.
    pub storage_quota_bytes: Option<i64>,
    pub connector_error_count: i64,
    pub deployment_error: Option<String>,
    /// Milliseconds since the Unix epoch when the current status started.
    pub status_since_millis: Option<i64>,
}

impl PipelineRow {
    pub fn from_json(pipeline: &Value) -> Self {
        let deployment_error = pipeline
            .get("deployment_error")
            .filter(|error| !error.is_null())
            .map(|error| {
                json::string(error, "message")
                    .map(|message| terminal_safe(&message))
                    .unwrap_or_else(|| "deployment error".to_string())
            });
        let tags = json::array(pipeline, "tags")
            .iter()
            .filter_map(Value::as_str)
            .map(terminal_safe)
            .collect();
        Self {
            name: json::string(pipeline, "name")
                .map(|name| terminal_safe(&name))
                .unwrap_or_else(|| "<unnamed>".to_string()),
            tags,
            deployment_status: json::string(pipeline, "deployment_status")
                .map(|status| terminal_safe(&status))
                .unwrap_or_else(|| "Unknown".to_string()),
            desired_status: json::string(pipeline, "deployment_desired_status")
                .map(|status| terminal_safe(&status)),
            runtime_status: json::string(pipeline, "deployment_runtime_status")
                .map(|status| terminal_safe(&status)),
            program_status: json::string(pipeline, "program_status")
                .map(|status| terminal_safe(&status))
                .unwrap_or_else(|| "Unknown".to_string()),
            storage_status: json::string(pipeline, "storage_status")
                .map(|status| terminal_safe(&status))
                .unwrap_or_else(|| "Unknown".to_string()),
            platform_version: json::string(pipeline, "platform_version")
                .map(|version| terminal_safe(&version))
                .unwrap_or_default(),
            version: json::integer_or_zero(pipeline, "version"),
            storage_quota_bytes: pipeline
                .get("runtime_config")
                .and_then(|config| config.get("resources"))
                .and_then(|resources| json::integer(resources, "storage_mb_max"))
                .map(|megabytes| megabytes.saturating_mul(1_000_000)),
            connector_error_count: pipeline
                .get("connectors")
                .map(|connectors| json::integer_or_zero(connectors, "num_errors"))
                .unwrap_or(0),
            deployment_error,
            status_since_millis: json::timestamp_millis(pipeline, "deployment_status_since"),
        }
    }

    /// Parse a whole `GET /v0/pipelines` response.
    pub fn list_from_json(pipelines: &Value) -> Vec<Self> {
        pipelines
            .as_array()
            .map(|pipelines| pipelines.iter().map(Self::from_json).collect())
            .unwrap_or_default()
    }

    pub fn health(&self) -> Health {
        if self.deployment_error.is_some() || program_health(&self.program_status) == Health::Bad {
            return Health::Bad;
        }
        deployment_health(&self.deployment_status)
    }

    /// Whether the engine is deployed enough for `/stats` to answer.
    pub fn is_queryable(&self) -> bool {
        matches!(
            self.deployment_status.as_str(),
            "Running" | "Paused" | "Initializing" | "Bootstrapping" | "Replaying" | "Synchronizing"
        )
    }

    /// Age of the current deployment status, as a compact label.
    pub fn status_age(&self, now_millis: i64) -> String {
        let Some(since) = self.status_since_millis else {
            return "-".to_string();
        };
        compact_age(now_millis.saturating_sub(since))
    }

    /// Whether the deployment is between steady states: either the status
    /// itself is transitional, or the desired status is a different steady
    /// state (e.g. a stop was just requested on a running pipeline).
    pub fn is_transitioning(&self) -> bool {
        // Broken deployments are not progressing anywhere.
        if deployment_health(&self.deployment_status) == Health::Bad {
            return false;
        }
        let steady = matches!(
            self.deployment_status.as_str(),
            "Running" | "Paused" | "Stopped" | "Suspended" | "Standby"
        );
        if !steady {
            return true;
        }
        self.desired_status
            .as_deref()
            .is_some_and(|desired| desired != self.deployment_status)
    }

    /// Compact runtime version for the table, e.g. `0.338.0+ent`.
    pub fn runtime_label(&self) -> String {
        if self.platform_version.is_empty() {
            return "-".to_string();
        }
        self.platform_version.replace("+enterprise", "+ent")
    }

    /// Tags joined for the table cell.
    pub fn tags_label(&self) -> String {
        self.tags.join(",")
    }
}

/// Format an age in milliseconds like `12s`, `4m`, `2h`, `3d`.
pub fn compact_age(age_millis: i64) -> String {
    let seconds = age_millis.max(0) / 1_000;
    match seconds {
        0..=59 => format!("{seconds}s"),
        60..=3_599 => format!("{}m", seconds / 60),
        3_600..=86_399 => format!("{}h", seconds / 3_600),
        _ => format!("{}d", seconds / 86_400),
    }
}

#[cfg(test)]
mod tests {
    use super::{Health, PipelineRow, compact_age, deployment_health, program_health};
    use serde_json::json;

    #[test]
    fn rows_read_the_storage_quota_from_the_runtime_config() {
        let with_quota = PipelineRow::from_json(&json!({
            "name": "p",
            "runtime_config": {"resources": {"storage_mb_max": 5}}
        }));
        assert_eq!(with_quota.storage_quota_bytes, Some(5_000_000));
        let unlimited = PipelineRow::from_json(&json!({
            "name": "p",
            "runtime_config": {"resources": {"storage_mb_max": null}}
        }));
        assert_eq!(unlimited.storage_quota_bytes, None);
        let bare = PipelineRow::from_json(&json!({"name": "p"}));
        assert_eq!(bare.storage_quota_bytes, None);
    }

    #[test]
    fn rows_parse_the_status_selector_payload() {
        let rows = PipelineRow::list_from_json(&json!([{
            "name": "orders",
            "tags": ["prod", "billing", 7],
            "deployment_status": "Running",
            "deployment_desired_status": "Running",
            "deployment_runtime_status": "Running",
            "program_status": "Success",
            "storage_status": "InUse",
            "platform_version": "0.338.0+enterprise",
            "connectors": {"num_errors": 3},
            "deployment_status_since": "2026-01-02T03:04:05Z"
        }]));
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].name, "orders");
        assert_eq!(rows[0].tags, vec!["prod", "billing"]);
        assert_eq!(rows[0].tags_label(), "prod,billing");
        assert_eq!(rows[0].runtime_label(), "0.338.0+ent");
        assert_eq!(rows[0].connector_error_count, 3);
        assert_eq!(rows[0].health(), Health::Good);
        assert!(rows[0].is_queryable());
        assert!(!rows[0].is_transitioning());
        assert!(rows[0].status_since_millis.is_some());
    }

    #[test]
    fn transitions_cover_busy_states_and_desired_mismatches() {
        let row = |status: &str, desired: Option<&str>| PipelineRow {
            deployment_status: status.to_string(),
            desired_status: desired.map(str::to_string),
            ..Default::default()
        };
        assert!(row("Provisioning", None).is_transitioning());
        assert!(row("Stopping", Some("Stopped")).is_transitioning());
        assert!(
            !row("Paused", Some("Paused")).is_transitioning(),
            "paused is a settled state, not a perpetual transition"
        );
        assert!(!row("Paused", None).is_transitioning());
        assert!(row("Paused", Some("Running")).is_transitioning());
        assert!(
            row("Running", Some("Stopped")).is_transitioning(),
            "stop requested but not yet picked up"
        );
        assert!(row("Stopped", Some("Running")).is_transitioning());
        assert!(!row("Running", Some("Running")).is_transitioning());
        assert!(!row("Stopped", None).is_transitioning());
        assert!(
            !row("Unavailable", Some("Running")).is_transitioning(),
            "broken deployments are not animated as progress"
        );
    }

    #[test]
    fn a_non_array_response_yields_no_rows() {
        assert!(PipelineRow::list_from_json(&json!({"detail": "boom"})).is_empty());
    }

    #[test]
    fn deployment_errors_dominate_health() {
        let row = PipelineRow::from_json(&json!({
            "name": "bad",
            "deployment_status": "Running",
            "program_status": "Success",
            "deployment_error": {"message": "worker died"}
        }));
        assert_eq!(row.health(), Health::Bad);
        assert_eq!(row.deployment_error.as_deref(), Some("worker died"));
    }

    #[test]
    fn health_classification_covers_the_status_families() {
        assert_eq!(deployment_health("Running"), Health::Good);
        assert_eq!(deployment_health("Stopped"), Health::Idle);
        assert_eq!(deployment_health("Unavailable"), Health::Bad);
        assert_eq!(deployment_health("Provisioning"), Health::Busy);
        assert_eq!(program_health("Success"), Health::Good);
        assert_eq!(program_health("SqlError"), Health::Bad);
        assert_eq!(program_health("CompilingRust"), Health::Busy);
    }

    #[test]
    fn unknown_pipelines_render_placeholders() {
        let row = PipelineRow::from_json(&json!({}));
        assert_eq!(row.name, "<unnamed>");
        assert_eq!(row.deployment_status, "Unknown");
        assert_eq!(row.status_age(1_000), "-");
        assert_eq!(row.runtime_label(), "-");
        assert_eq!(row.tags_label(), "");
        assert!(!row.is_queryable());
    }

    #[test]
    fn ages_use_the_largest_natural_unit() {
        assert_eq!(compact_age(12_000), "12s");
        assert_eq!(compact_age(180_000), "3m");
        assert_eq!(compact_age(7_200_000), "2h");
        assert_eq!(compact_age(200_000_000), "2d");
        assert_eq!(compact_age(-5), "0s");
    }
}
