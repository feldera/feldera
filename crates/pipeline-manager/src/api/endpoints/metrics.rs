use crate::api::main::ServerState;
use crate::db::types::combined_status::CombinedStatus;
use crate::db::types::pipeline::ExtendedPipelineDescrMonitoring;
use crate::db::{storage::Storage as _, types::tenant::TenantId};
use crate::error::ManagerError;
use actix_web::web::Bytes;
use actix_web::{
    HttpResponse, get,
    http::Method,
    web::{Data as WebData, ReqData},
};
use awc::body::MessageBody as _;
use feldera_types::runtime_status::RuntimeStatus;
use serde_json::{Value, json};
use std::fmt::Write as _;
use tracing::debug;

const STATUS_METRIC: &str = "pipeline_status";
const STATUS_HELP: &str =
    "Whether the pipeline is in the status named by the status label: 1 if it is, 0 if it is not.";

/// Returns the `HELP` and `TYPE` lines of the `metric` family.
fn family_header(metric: &str, help: &str) -> String {
    format!("# HELP {metric} {help}\n# TYPE {metric} gauge\n")
}

/// Returns the status that `pipeline` reports.
fn status_of(pipeline: &ExtendedPipelineDescrMonitoring) -> CombinedStatus {
    CombinedStatus::new(
        pipeline.deployment_resources_status,
        pipeline.deployment_runtime_status,
    )
}

/// Appends the status of `pipeline` to the metric family.
fn write_status_metrics(status_family: &mut String, pipeline: &ExtendedPipelineDescrMonitoring) {
    let labels = format!(
        "pipeline=\"pipeline-{}\",pipeline_name=\"{}\"",
        pipeline.id, pipeline.name
    );
    let status = status_of(pipeline);

    for candidate in CombinedStatus::ALL {
        writeln!(
            status_family,
            "{STATUS_METRIC}{{{labels},status=\"{}\"}} {}",
            candidate.as_str(),
            u8::from(candidate == status)
        )
        .unwrap();
    }
}

/// Renders the status metric family for `pipelines` in Prometheus format.
pub(crate) fn status_metrics(pipelines: &[ExtendedPipelineDescrMonitoring]) -> String {
    let mut status_family = family_header(STATUS_METRIC, STATUS_HELP);
    for pipeline in pipelines {
        write_status_metrics(&mut status_family, pipeline);
    }
    status_family
}

/// Renders the status metric family for `pipelines` in JSON.
pub(crate) fn status_metrics_json(pipelines: &[ExtendedPipelineDescrMonitoring]) -> Vec<Value> {
    let mut status_values = Vec::new();
    for pipeline in pipelines {
        let status = status_of(pipeline);
        for candidate in CombinedStatus::ALL {
            status_values.push(json!({
                "labels": {
                    "pipeline": format!("pipeline-{}", pipeline.id),
                    "pipeline_name": pipeline.name,
                    "status": candidate.as_str(),
                },
                "value": u8::from(candidate == status),
            }));
        }
    }

    vec![
        json!({"key": STATUS_METRIC, "type": "gauge", "description": STATUS_HELP, "values": status_values}),
    ]
}

/// Returns the metrics that `pipeline` exports itself, or `None` when it is not
/// running or does not answer.
pub(crate) async fn fetch_pipeline_metrics(
    state: &ServerState,
    client: &awc::Client,
    tenant_id: TenantId,
    pipeline: &ExtendedPipelineDescrMonitoring,
    query_string: &str,
) -> Option<Bytes> {
    if pipeline.deployment_runtime_status != Some(RuntimeStatus::Running)
        && pipeline.deployment_runtime_status != Some(RuntimeStatus::Paused)
    {
        return None;
    }

    let name = &pipeline.name;
    match state
        .runner
        .forward_http_request_to_pipeline_by_name(
            client,
            tenant_id,
            name,
            Method::GET,
            "metrics",
            query_string,
            None,
            None,
        )
        .await
    {
        Ok(response) if response.status().is_success() => {
            response.into_body().try_into_bytes().ok()
        }
        Ok(response) => {
            debug!(
                "Pipeline {name} answered its metrics request with {}, reporting only its status",
                response.status()
            );
            None
        }
        Err(error) => {
            debug!(
                "Pipeline {name} did not answer its metrics request, reporting only its status: {error}"
            );
            None
        }
    }
}

/// List All Metrics
///
/// Retrieve the metrics of all pipelines belonging to this tenant.
#[utoipa::path(
    context_path = "/v0",
    security(("JSON web token (JWT) or API key" = [])),
    responses(
        (status = OK
        , description = "Metrics of all pipelines belonging to this tenant in Prometheus format"
        , content_type = "text/plain"
        , body = Vec<u8>),
    ),
    tag = "Metrics & Debugging"
)]
#[get("/metrics")]
pub(crate) async fn get_metrics(
    state: WebData<ServerState>,
    client: WebData<awc::Client>,
    tenant_id: ReqData<TenantId>,
) -> Result<HttpResponse, ManagerError> {
    let pipelines = state
        .db
        .lock()
        .await
        .list_pipelines_for_monitoring(*tenant_id)
        .await?;

    const NEWLINE: u8 = b'\n';
    let mut result = status_metrics(&pipelines).into_bytes();

    for pipeline in &pipelines {
        if let Some(bytes) =
            fetch_pipeline_metrics(&state, client.as_ref(), *tenant_id, pipeline, "").await
        {
            result.extend(bytes);
            result.push(NEWLINE);
        }
    }

    Ok(HttpResponse::Ok().content_type("text/plain").body(result))
}

#[cfg(test)]
mod test {
    use super::{
        STATUS_HELP, STATUS_METRIC, family_header, status_metrics, status_metrics_json,
        write_status_metrics,
    };
    use crate::db::types::combined_status::CombinedStatus;
    use crate::db::types::pipeline::{ExtendedPipelineDescrMonitoring, PipelineId};
    use crate::db::types::program::ProgramStatus;
    use crate::db::types::resources_status::{ResourcesDesiredStatus, ResourcesStatus};
    use crate::db::types::storage::StorageStatus;
    use crate::db::types::version::Version;
    use feldera_types::runtime_status::RuntimeStatus;
    use uuid::Uuid;

    fn stopped_pipeline(name: &str) -> ExtendedPipelineDescrMonitoring {
        ExtendedPipelineDescrMonitoring {
            id: PipelineId(Uuid::from_u128(1)),
            name: name.to_string(),
            description: Default::default(),
            tags: Default::default(),
            created_at: Default::default(),
            version: Version(1),
            platform_version: String::new(),
            runtime_config: Default::default(),
            program_config: Default::default(),
            program_version: Version(1),
            program_status: ProgramStatus::Pending,
            program_status_since: Default::default(),
            deployment_error: None,
            deployment_location: None,
            refresh_version: Version(1),
            storage_status: StorageStatus::Cleared,
            storage_status_details: None,
            deployment_id: None,
            deployment_initial: None,
            deployment_resources_status: ResourcesStatus::Stopped,
            deployment_resources_status_since: Default::default(),
            deployment_resources_desired_status: ResourcesDesiredStatus::Stopped,
            deployment_resources_desired_status_since: Default::default(),
            deployment_runtime_status: None,
            deployment_runtime_status_details: None,
            deployment_runtime_status_since: None,
            deployment_runtime_desired_status: None,
            bootstrap_policy: None,
            deployment_runtime_desired_status_since: None,
        }
    }

    fn family(pipeline: &ExtendedPipelineDescrMonitoring) -> String {
        let mut status_family = String::new();
        write_status_metrics(&mut status_family, pipeline);
        status_family
    }

    /// Returns the status whose line is the only one valued 1.
    fn reported(family: &str) -> String {
        let ones: Vec<&str> = family.lines().filter(|line| line.ends_with(" 1")).collect();
        assert_eq!(ones.len(), 1, "expected exactly one status in {family}");
        ones[0]
            .split_once("status=\"")
            .unwrap()
            .1
            .split_once('"')
            .unwrap()
            .0
            .to_string()
    }

    #[test]
    fn stopped_pipeline_reports_all_statuses() {
        let status_family = family(&stopped_pipeline("p1"));

        assert_eq!(
            status_family,
            r#"pipeline_status{pipeline="pipeline-00000000-0000-0000-0000-000000000001",pipeline_name="p1",status="Stopped"} 1
pipeline_status{pipeline="pipeline-00000000-0000-0000-0000-000000000001",pipeline_name="p1",status="Provisioning"} 0
pipeline_status{pipeline="pipeline-00000000-0000-0000-0000-000000000001",pipeline_name="p1",status="Unavailable"} 0
pipeline_status{pipeline="pipeline-00000000-0000-0000-0000-000000000001",pipeline_name="p1",status="Coordination"} 0
pipeline_status{pipeline="pipeline-00000000-0000-0000-0000-000000000001",pipeline_name="p1",status="Standby"} 0
pipeline_status{pipeline="pipeline-00000000-0000-0000-0000-000000000001",pipeline_name="p1",status="AwaitingApproval"} 0
pipeline_status{pipeline="pipeline-00000000-0000-0000-0000-000000000001",pipeline_name="p1",status="Initializing"} 0
pipeline_status{pipeline="pipeline-00000000-0000-0000-0000-000000000001",pipeline_name="p1",status="Bootstrapping"} 0
pipeline_status{pipeline="pipeline-00000000-0000-0000-0000-000000000001",pipeline_name="p1",status="ConcurrentBootstrapping"} 0
pipeline_status{pipeline="pipeline-00000000-0000-0000-0000-000000000001",pipeline_name="p1",status="Synchronizing"} 0
pipeline_status{pipeline="pipeline-00000000-0000-0000-0000-000000000001",pipeline_name="p1",status="Replaying"} 0
pipeline_status{pipeline="pipeline-00000000-0000-0000-0000-000000000001",pipeline_name="p1",status="Paused"} 0
pipeline_status{pipeline="pipeline-00000000-0000-0000-0000-000000000001",pipeline_name="p1",status="Running"} 0
pipeline_status{pipeline="pipeline-00000000-0000-0000-0000-000000000001",pipeline_name="p1",status="Suspended"} 0
pipeline_status{pipeline="pipeline-00000000-0000-0000-0000-000000000001",pipeline_name="p1",status="Stopping"} 0
"#
        );
    }

    #[test]
    fn pipelines_share_the_family() {
        let mut status_family = String::new();
        for name in ["p1", "p2"] {
            write_status_metrics(&mut status_family, &stopped_pipeline(name));
        }

        for name in ["p1", "p2"] {
            let label = format!("pipeline_name=\"{name}\"");
            assert_eq!(
                status_family.matches(&label).count(),
                CombinedStatus::ALL.len()
            );
        }
        assert!(
            status_family
                .lines()
                .all(|line| line.starts_with("pipeline_status{"))
        );
    }

    #[test]
    fn headers_name_their_family() {
        assert_eq!(
            family_header(STATUS_METRIC, STATUS_HELP),
            format!("# HELP pipeline_status {STATUS_HELP}\n# TYPE pipeline_status gauge\n")
        );
    }

    /// A tenant without pipelines still gets a well-formed, sample-less family.
    #[test]
    fn no_pipelines_yields_only_the_header() {
        assert_eq!(
            status_metrics(&[]),
            family_header(STATUS_METRIC, STATUS_HELP)
        );
        for family in status_metrics_json(&[]) {
            assert_eq!(family["values"].as_array().unwrap().len(), 0);
        }
    }

    /// Prometheus expects one `HELP`/`TYPE` pair per family, ahead of its samples.
    #[test]
    fn the_family_is_announced_once_before_its_samples() {
        let body = status_metrics(&[stopped_pipeline("p1"), stopped_pipeline("p2")]);
        let lines: Vec<&str> = body.lines().collect();

        assert_eq!(body.matches(&format!("# HELP {STATUS_METRIC} ")).count(), 1);
        assert_eq!(body.matches(&format!("# TYPE {STATUS_METRIC} ")).count(), 1);

        let header = lines
            .iter()
            .position(|line| *line == format!("# TYPE {STATUS_METRIC} gauge"))
            .unwrap();
        let first_sample = lines
            .iter()
            .position(|line| line.starts_with(&format!("{STATUS_METRIC}{{")))
            .unwrap();
        assert!(header < first_sample);
        assert_eq!(lines.len(), 2 + 2 * CombinedStatus::ALL.len());
    }

    /// The two output formats must not disagree about a pipeline's status.
    #[test]
    fn json_reports_what_prometheus_reports() {
        let pipelines = [stopped_pipeline("p1"), stopped_pipeline("p2")];
        let families = status_metrics_json(&pipelines);

        let mut from_json: Vec<String> = Vec::new();
        for family in &families {
            let metric = family["key"].as_str().unwrap();
            for value in family["values"].as_array().unwrap() {
                let labels = &value["labels"];
                from_json.push(format!(
                    "{metric}{{pipeline=\"{}\",pipeline_name=\"{}\",status=\"{}\"}} {}",
                    labels["pipeline"].as_str().unwrap(),
                    labels["pipeline_name"].as_str().unwrap(),
                    labels["status"].as_str().unwrap(),
                    value["value"]
                ));
            }
        }

        let prometheus = status_metrics(&pipelines);
        let from_prometheus: Vec<&str> = prometheus
            .lines()
            .filter(|line| !line.starts_with('#'))
            .collect();
        assert_eq!(from_json, from_prometheus);
    }

    /// The JSON families carry the descriptions the Prometheus headers announce.
    #[test]
    fn json_families_describe_themselves() {
        let families = status_metrics_json(&[stopped_pipeline("p1")]);
        let described: Vec<(&str, &str, &str)> = families
            .iter()
            .map(|family| {
                (
                    family["key"].as_str().unwrap(),
                    family["type"].as_str().unwrap(),
                    family["description"].as_str().unwrap(),
                )
            })
            .collect();

        assert_eq!(described, vec![(STATUS_METRIC, "gauge", STATUS_HELP)]);
    }

    #[test]
    fn each_status_is_reported_in_turn() {
        use ResourcesStatus::*;
        use RuntimeStatus::*;

        let cases = [
            (Stopped, None, "Stopped"),
            (Provisioning, None, "Provisioning"),
            (Stopping, None, "Stopping"),
            (Provisioned, Some(Unavailable), "Unavailable"),
            (Provisioned, Some(Coordination), "Coordination"),
            (Provisioned, Some(Standby), "Standby"),
            (Provisioned, Some(AwaitingApproval), "AwaitingApproval"),
            (Provisioned, Some(Initializing), "Initializing"),
            (Provisioned, Some(Bootstrapping), "Bootstrapping"),
            (
                Provisioned,
                Some(ConcurrentBootstrapping),
                "ConcurrentBootstrapping",
            ),
            (Provisioned, Some(Synchronizing), "Synchronizing"),
            (Provisioned, Some(Replaying), "Replaying"),
            (Provisioned, Some(Paused), "Paused"),
            (Provisioned, Some(Running), "Running"),
            (Provisioned, Some(Suspended), "Suspended"),
        ];
        assert_eq!(cases.len(), CombinedStatus::ALL.len());

        for (resources_status, runtime_status, expected) in cases {
            let mut pipeline = stopped_pipeline("p1");
            pipeline.deployment_resources_status = resources_status;
            pipeline.deployment_runtime_status = runtime_status;

            let status_family = family(&pipeline);
            assert_eq!(reported(&status_family), expected);
            assert_eq!(status_family.lines().count(), CombinedStatus::ALL.len());
        }
    }

    #[test]
    fn a_status_without_a_reportable_value_becomes_unavailable() {
        let mut pipeline = stopped_pipeline("p1");
        pipeline.deployment_resources_status = ResourcesStatus::Provisioned;
        pipeline.deployment_runtime_status = None;

        assert_eq!(reported(&family(&pipeline)), "Unavailable");
    }
}
