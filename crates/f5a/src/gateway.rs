//! The API boundary: everything the console asks of a Feldera instance.
//!
//! Reads go over [`Http`] as raw JSON and are shaped by [`crate::model`], so
//! version skew between console and server can never abort a refresh. Writes
//! go through the generated `feldera-rest-api` client: they carry no response
//! bodies worth parsing, and the typed builders document the parameters.

use std::future::Future;
use std::sync::{Arc, RwLock};

use feldera_rest_api::types::BootstrapPolicy;
use feldera_rest_api::{Client, RetryPolicy};
use serde_json::Value;

use crate::http::{ApiError, Download, Http};
use crate::model::dataflow::{DataflowIndex, HotspotReport};
use crate::model::diagnostics::DiagnosticSection;
use crate::model::header::{InstanceInfo, Session};
use crate::model::pipeline::PipelineRow;
use crate::model::profile::CircuitProfile;
use crate::model::stats::PipelineStats;
use crate::model::text::terminal_safe_multiline;
use crate::model::timeseries::TimeSeries;
use crate::options::ConnectionOptions;

/// Everything the dashboard needs for one refresh.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct Overview {
    pub instance: InstanceInfo,
    pub session: Session,
    pub pipelines: Vec<PipelineRow>,
}

/// Everything the detail panes need for one selected pipeline.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct PipelineDetail {
    pub stats: PipelineStats,
    pub series: TimeSeries,
}

/// A state-changing operation on the instance.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Action {
    Start {
        pipeline: String,
    },
    Pause {
        pipeline: String,
    },
    Resume {
        pipeline: String,
    },
    Stop {
        pipeline: String,
        force: bool,
    },
    /// Force-stop, wait for `Stopped`, then start again (like `fda restart`).
    Restart {
        pipeline: String,
    },
    Clear {
        pipeline: String,
    },
    /// Remove the pipeline definition entirely; the server requires it to be
    /// fully stopped with cleared storage.
    Delete {
        pipeline: String,
    },
    ConnectorPause {
        pipeline: String,
        table: String,
        connector: String,
    },
    ConnectorResume {
        pipeline: String,
        table: String,
        connector: String,
    },
    Samply {
        pipeline: String,
        duration_secs: u64,
    },
}

impl Action {
    /// Human phrasing for confirmations and toasts.
    pub fn describe(&self) -> String {
        match self {
            Self::Start { pipeline } => format!("start `{pipeline}`"),
            Self::Pause { pipeline } => format!("pause `{pipeline}`"),
            Self::Resume { pipeline } => format!("resume `{pipeline}`"),
            Self::Stop {
                pipeline,
                force: false,
            } => format!("stop `{pipeline}` (with checkpoint)"),
            Self::Stop {
                pipeline,
                force: true,
            } => format!("force-stop `{pipeline}` (no checkpoint)"),
            Self::Restart { pipeline } => format!("restart `{pipeline}` (force-stop, then start)"),
            Self::Clear { pipeline } => format!("clear all storage of `{pipeline}`"),
            Self::Delete { pipeline } => format!("DELETE pipeline `{pipeline}` permanently"),
            Self::ConnectorPause { connector, .. } => format!("pause connector `{connector}`"),
            Self::ConnectorResume { connector, .. } => format!("resume connector `{connector}`"),
            Self::Samply {
                pipeline,
                duration_secs,
            } => format!("record a {duration_secs}s Samply profile of `{pipeline}`"),
        }
    }

    /// Actions that lose data or interrupt processing; the `--ask` flag
    /// gates these behind a confirmation dialog.
    pub fn needs_confirmation(&self) -> bool {
        matches!(
            self,
            Self::Stop { .. } | Self::Restart { .. } | Self::Clear { .. } | Self::Delete { .. }
        )
    }

    /// Deleting a pipeline erases its definition; that confirms even
    /// without the `--ask` flag.
    pub fn always_confirms(&self) -> bool {
        matches!(self, Self::Delete { .. })
    }

    /// The pipeline this action targets.
    pub fn pipeline(&self) -> &str {
        match self {
            Self::Start { pipeline }
            | Self::Pause { pipeline }
            | Self::Resume { pipeline }
            | Self::Stop { pipeline, .. }
            | Self::Restart { pipeline }
            | Self::Clear { pipeline }
            | Self::Delete { pipeline }
            | Self::ConnectorPause { pipeline, .. }
            | Self::ConnectorResume { pipeline, .. }
            | Self::Samply { pipeline, .. } => pipeline,
        }
    }

    /// Deployment-lifecycle actions take part in per-pipeline queueing.
    pub fn is_lifecycle(&self) -> bool {
        matches!(
            self,
            Self::Start { .. }
                | Self::Pause { .. }
                | Self::Resume { .. }
                | Self::Stop { .. }
                | Self::Restart { .. }
                | Self::Clear { .. }
                | Self::Delete { .. }
        )
    }

    /// One-word label for queue badges.
    pub fn verb(&self) -> &'static str {
        match self {
            Self::Start { .. } => "start",
            Self::Pause { .. } => "pause",
            Self::Resume { .. } => "resume",
            Self::Stop { force: false, .. } => "stop",
            Self::Stop { force: true, .. } => "force-stop",
            Self::Restart { .. } => "restart",
            Self::Clear { .. } => "clear",
            Self::Delete { .. } => "delete",
            Self::ConnectorPause { .. } => "connector-pause",
            Self::ConnectorResume { .. } => "connector-resume",
            Self::Samply { .. } => "samply",
        }
    }
}

/// A file the server produces on request.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum DownloadKind {
    /// A Samply CPU profile: gzipped Firefox Profiler JSON.
    SamplyProfile,
    /// A support bundle: logs, configs, profiles, and metrics in one zip.
    SupportBundle,
}

impl DownloadKind {
    /// Short tag that prefixes badges and status lines.
    ///
    /// ```
    /// use f5a::gateway::DownloadKind;
    ///
    /// assert_eq!(DownloadKind::SamplyProfile.tag(), "samply");
    /// assert_eq!(DownloadKind::SupportBundle.tag(), "bundle");
    /// ```
    pub const fn tag(self) -> &'static str {
        match self {
            Self::SamplyProfile => "samply",
            Self::SupportBundle => "bundle",
        }
    }

    /// What the file is, for prose.
    ///
    /// ```
    /// use f5a::gateway::DownloadKind;
    ///
    /// assert_eq!(DownloadKind::SupportBundle.noun(), "support bundle");
    /// ```
    pub const fn noun(self) -> &'static str {
        match self {
            Self::SamplyProfile => "profile",
            Self::SupportBundle => "support bundle",
        }
    }

    /// File name infix and extension of a saved download.
    ///
    /// ```
    /// use f5a::gateway::DownloadKind;
    ///
    /// assert_eq!(DownloadKind::SamplyProfile.file_parts(), ("samply", "json.gz"));
    /// assert_eq!(DownloadKind::SupportBundle.file_parts(), ("support-bundle", "zip"));
    /// ```
    pub const fn file_parts(self) -> (&'static str, &'static str) {
        match self {
            Self::SamplyProfile => ("samply", "json.gz"),
            Self::SupportBundle => ("support-bundle", "zip"),
        }
    }

    /// API path below `/v0/pipelines/{name}`. The profile asks for the
    /// latest recording so the server answers 204 while it is still running;
    /// the bundle carries its collection options as a query.
    fn endpoint(self, bundle: BundleOptions) -> String {
        match self {
            Self::SamplyProfile => "samply_profile?latest=true".to_string(),
            Self::SupportBundle => format!("support_bundle{}", bundle.query()),
        }
    }
}

/// What a support bundle request asks the server to gather.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct BundleOptions {
    /// Collect fresh data from the pipeline before answering.
    pub collect: bool,
    /// At most this many collections, newest first; `None` returns every
    /// collection the server retains.
    pub limit: Option<u64>,
    pub circuit_profile: bool,
    pub heap_profile: bool,
    pub metrics: bool,
    pub logs: bool,
    pub stats: bool,
    pub pipeline_config: bool,
    pub system_config: bool,
    pub dataflow_graph: bool,
    pub pipeline_events: bool,
}

impl Default for BundleOptions {
    /// Everything, collected now, and only that fresh collection.
    fn default() -> Self {
        Self {
            collect: true,
            limit: Some(1),
            circuit_profile: true,
            heap_profile: true,
            metrics: true,
            logs: true,
            stats: true,
            pipeline_config: true,
            system_config: true,
            dataflow_graph: true,
            pipeline_events: true,
        }
    }
}

impl BundleOptions {
    /// The query string for `GET /support_bundle`, naming only what differs
    /// from the server's defaults (collect everything, return all retained).
    ///
    /// ```
    /// use f5a::gateway::BundleOptions;
    ///
    /// assert_eq!(BundleOptions::default().query(), "?limit=1");
    /// let sparse = BundleOptions { collect: false, limit: None, logs: false, ..Default::default() };
    /// assert_eq!(sparse.query(), "?collect=false&logs=false");
    /// let all = BundleOptions { limit: None, ..Default::default() };
    /// assert_eq!(all.query(), "");
    /// ```
    pub fn query(&self) -> String {
        let mut parameters = Vec::new();
        if !self.collect {
            parameters.push("collect=false".to_string());
        }
        if let Some(limit) = self.limit {
            parameters.push(format!("limit={limit}"));
        }
        for (name, included) in [
            ("circuit_profile", self.circuit_profile),
            ("heap_profile", self.heap_profile),
            ("metrics", self.metrics),
            ("logs", self.logs),
            ("stats", self.stats),
            ("pipeline_config", self.pipeline_config),
            ("system_config", self.system_config),
            ("dataflow_graph", self.dataflow_graph),
            ("pipeline_events", self.pipeline_events),
        ] {
            if !included {
                parameters.push(format!("{name}=false"));
            }
        }
        if parameters.is_empty() {
            String::new()
        } else {
            format!("?{}", parameters.join("&"))
        }
    }
}

/// The console's view of a Feldera instance. Implementations must be cheap to
/// clone; every clone shares the tenant override.
pub trait Gateway: Clone + Send + Sync + 'static {
    fn host(&self) -> String;
    fn tenant(&self) -> Option<String>;
    fn set_tenant(&self, tenant: Option<String>);
    fn overview(&self) -> impl Future<Output = Result<Overview, ApiError>> + Send;
    fn detail(
        &self,
        pipeline: &str,
    ) -> impl Future<Output = Result<PipelineDetail, ApiError>> + Send;
    /// Stats only, used to enrich list rows cheaply.
    fn quick_stats(
        &self,
        pipeline: &str,
    ) -> impl Future<Output = Result<PipelineStats, ApiError>> + Send;
    fn program_sql(&self, pipeline: &str) -> impl Future<Output = Result<String, ApiError>> + Send;
    fn hotspots(
        &self,
        pipeline: &str,
    ) -> impl Future<Output = Result<HotspotReport, ApiError>> + Send;
    fn execute(&self, action: Action) -> impl Future<Output = Result<(), ApiError>> + Send;
    /// The pipeline's newest file of `kind`, or how long until it exists
    /// while a recording started by [`Action::Samply`] is still running.
    /// `bundle` shapes a support bundle and is ignored for profiles.
    fn download(
        &self,
        kind: DownloadKind,
        pipeline: &str,
        bundle: BundleOptions,
    ) -> impl Future<Output = Result<Download, ApiError>> + Send;
    /// Error diagnostics from the pipeline descriptor (compile and
    /// deployment errors), for the Logs view.
    fn diagnostics(
        &self,
        pipeline: &str,
    ) -> impl Future<Output = Result<Vec<DiagnosticSection>, ApiError>> + Send;
    /// Follow the pipeline's log stream, forwarding each line into `sink`
    /// until the stream ends, errors, or the receiver is dropped.
    fn stream_logs(
        &self,
        pipeline: &str,
        sink: tokio::sync::mpsc::UnboundedSender<String>,
    ) -> impl Future<Output = Result<(), ApiError>> + Send;
}

/// The production [`Gateway`] over HTTP.
#[derive(Clone)]
pub struct RestGateway {
    http: Http,
    /// Generated client for writes. Rebuilt on tenant switches and token
    /// refreshes because auth and tenant live in its default headers.
    typed: Arc<RwLock<Client>>,
    /// The minted token the typed client was last built with.
    typed_token: Arc<RwLock<Option<String>>>,
    options: ConnectionOptions,
}

impl RestGateway {
    pub fn connect(options: &ConnectionOptions) -> anyhow::Result<Self> {
        let http = Http::new(options)?;
        let typed = Arc::new(RwLock::new(build_typed_client(options)?));
        Ok(Self {
            http,
            typed,
            typed_token: Arc::new(RwLock::new(None)),
            options: options.clone(),
        })
    }

    /// The typed client, rebuilt when the auth command minted a new token
    /// since the last build.
    async fn typed(&self) -> Result<Client, ApiError> {
        let Some(minter) = self.http.minter() else {
            return Ok(self.typed.read().expect("typed client lock").clone());
        };
        let token = minter.token().await?;
        let stale = self
            .typed_token
            .read()
            .expect("typed token lock")
            .as_deref()
            != Some(token.as_str());
        if stale {
            let options = ConnectionOptions {
                api_key: Some(token.clone()),
                auth_command: None,
                tenant: self.http.tenant(),
                ..self.options.clone()
            };
            let client = build_typed_client(&options)
                .map_err(|error| ApiError::Transport(error.to_string()))?;
            *self.typed.write().expect("typed client lock") = client;
            *self.typed_token.write().expect("typed token lock") = Some(token);
        }
        Ok(self.typed.read().expect("typed client lock").clone())
    }

    /// Poll the pipeline status until it reports `Stopped`, for restarts.
    async fn wait_until_stopped(&self, pipeline: &str) -> Result<(), ApiError> {
        const POLL_INTERVAL: std::time::Duration = std::time::Duration::from_millis(500);
        const MAX_POLLS: u32 = 240;
        let path = format!(
            "/v0/pipelines/{}?selector=status",
            encode_path_segment(pipeline)
        );
        for _ in 0..MAX_POLLS {
            let descriptor = self.http.get_json(&path).await?;
            let status = descriptor
                .get("deployment_status")
                .and_then(Value::as_str)
                .unwrap_or_default();
            if status == "Stopped" {
                return Ok(());
            }
            tokio::time::sleep(POLL_INTERVAL).await;
        }
        Err(ApiError::Timeout(format!(
            "`{pipeline}` did not reach Stopped within {}s",
            POLL_INTERVAL.as_secs_f64() * MAX_POLLS as f64
        )))
    }
}

fn build_typed_client(options: &ConnectionOptions) -> anyhow::Result<Client> {
    let mut headers = reqwest::header::HeaderMap::new();
    if let Some(tenant) = &options.tenant {
        headers.insert(
            "Feldera-Tenant",
            reqwest::header::HeaderValue::from_str(tenant)
                .map_err(|_| anyhow::anyhow!("tenant name is not a valid header value"))?,
        );
    }
    if let Some(api_key) = &options.api_key {
        let mut value = reqwest::header::HeaderValue::from_str(&format!("Bearer {api_key}"))
            .map_err(|_| anyhow::anyhow!("API key is not a valid header value"))?;
        value.set_sensitive(true);
        headers.insert(reqwest::header::AUTHORIZATION, value);
    }
    let client = reqwest::ClientBuilder::new()
        .danger_accept_invalid_certs(options.insecure_tls)
        .timeout(std::time::Duration::from_secs(options.timeout_secs))
        .default_headers(headers)
        .build()
        .map_err(|error| anyhow::anyhow!("failed to build the HTTP client: {error}"))?;
    Ok(Client::new_with_client(
        &options.host,
        client,
        RetryPolicy::none(),
    ))
}

/// Percent-encode one path segment (RFC 3986 unreserved characters pass).
pub fn encode_path_segment(segment: &str) -> String {
    let mut encoded = String::with_capacity(segment.len());
    for byte in segment.bytes() {
        match byte {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                encoded.push(byte as char);
            }
            byte => {
                encoded.push('%');
                encoded.push_str(&format!("{byte:02X}"));
            }
        }
    }
    encoded
}

impl Gateway for RestGateway {
    fn host(&self) -> String {
        self.http.base_url().to_string()
    }

    fn tenant(&self) -> Option<String> {
        self.http.tenant()
    }

    fn set_tenant(&self, tenant: Option<String>) {
        self.http.set_tenant(tenant.clone());
        let options = ConnectionOptions {
            tenant,
            ..self.options.clone()
        };
        if let Ok(client) = build_typed_client(&options) {
            *self.typed.write().expect("typed client lock") = client;
        }
    }

    async fn overview(&self) -> Result<Overview, ApiError> {
        let pipelines_path = "/v0/pipelines?selector=status_with_connectors";
        let (configuration, session, pipelines) = tokio::join!(
            self.http.get_json("/v0/config"),
            self.http.get_json("/v0/config/session"),
            self.http.get_json(pipelines_path),
        );
        // The pipeline list is the dashboard; config and session only decorate
        // the header, so their failures degrade to defaults.
        let pipelines = pipelines?;
        Ok(Overview {
            instance: InstanceInfo::from_json(&configuration.unwrap_or(Value::Null)),
            session: Session::from_json(&session.unwrap_or(Value::Null)),
            pipelines: PipelineRow::list_from_json(&pipelines),
        })
    }

    async fn detail(&self, pipeline: &str) -> Result<PipelineDetail, ApiError> {
        let name = encode_path_segment(pipeline);
        let stats_path = format!("/v0/pipelines/{name}/stats");
        let series_path = format!("/v0/pipelines/{name}/time_series");
        let (stats, series) = tokio::join!(
            self.http.get_json(&stats_path),
            self.http.get_json(&series_path),
        );
        // Older servers lack /time_series; charts degrade to stats samples.
        Ok(PipelineDetail {
            stats: PipelineStats::from_json(&stats?),
            series: TimeSeries::from_json(&series.unwrap_or(Value::Null)),
        })
    }

    async fn quick_stats(&self, pipeline: &str) -> Result<PipelineStats, ApiError> {
        let name = encode_path_segment(pipeline);
        let stats = self
            .http
            .get_json(&format!("/v0/pipelines/{name}/stats"))
            .await?;
        Ok(PipelineStats::from_json(&stats))
    }

    async fn program_sql(&self, pipeline: &str) -> Result<String, ApiError> {
        let name = encode_path_segment(pipeline);
        let descriptor = self.http.get_json(&format!("/v0/pipelines/{name}")).await?;
        let sql = descriptor
            .get("program_code")
            .and_then(Value::as_str)
            .unwrap_or_default();
        Ok(terminal_safe_multiline(sql))
    }

    async fn hotspots(&self, pipeline: &str) -> Result<HotspotReport, ApiError> {
        let name = encode_path_segment(pipeline);
        let profile_path = format!("/v0/pipelines/{name}/circuit_json_profile");
        let dataflow_path = format!("/v0/pipelines/{name}/dataflow_graph");
        let (profile, dataflow) = tokio::join!(
            self.http.get_json(&profile_path),
            self.http.get_json(&dataflow_path),
        );
        // Without the dataflow graph the report ranks operators but cannot
        // point into the SQL; that is still worth showing.
        let profile = CircuitProfile::from_json(&profile?);
        let dataflow = DataflowIndex::from_json(&dataflow.unwrap_or(Value::Null));
        Ok(HotspotReport::build(&profile, &dataflow))
    }

    async fn execute(&self, action: Action) -> Result<(), ApiError> {
        let typed = self.typed().await?;
        match &action {
            Action::Start { pipeline } => {
                typed
                    .post_pipeline_start()
                    .pipeline_name(pipeline)
                    .initial("running")
                    .bootstrap_policy(BootstrapPolicy::Allow)
                    .dismiss_error(true)
                    .send()
                    .await
                    .map_err(typed_error)?;
            }
            Action::Pause { pipeline } => {
                typed
                    .post_pipeline_pause()
                    .pipeline_name(pipeline)
                    .send()
                    .await
                    .map_err(typed_error)?;
            }
            Action::Resume { pipeline } => {
                typed
                    .post_pipeline_resume()
                    .pipeline_name(pipeline)
                    .send()
                    .await
                    .map_err(typed_error)?;
            }
            Action::Stop { pipeline, force } => {
                typed
                    .post_pipeline_stop()
                    .pipeline_name(pipeline)
                    .force(*force)
                    .send()
                    .await
                    .map_err(typed_error)?;
            }
            Action::Restart { pipeline } => {
                typed
                    .post_pipeline_stop()
                    .pipeline_name(pipeline)
                    .force(true)
                    .send()
                    .await
                    .map_err(typed_error)?;
                self.wait_until_stopped(pipeline).await?;
                typed
                    .post_pipeline_start()
                    .pipeline_name(pipeline)
                    .initial("running")
                    .bootstrap_policy(BootstrapPolicy::Allow)
                    .dismiss_error(true)
                    .send()
                    .await
                    .map_err(typed_error)?;
            }
            Action::Clear { pipeline } => {
                typed
                    .post_pipeline_clear()
                    .pipeline_name(pipeline)
                    .send()
                    .await
                    .map_err(typed_error)?;
            }
            Action::Delete { pipeline } => {
                typed
                    .delete_pipeline()
                    .pipeline_name(pipeline)
                    .send()
                    .await
                    .map_err(typed_error)?;
            }
            Action::ConnectorPause {
                pipeline,
                table,
                connector,
            }
            | Action::ConnectorResume {
                pipeline,
                table,
                connector,
            } => {
                let verb = match &action {
                    Action::ConnectorPause { .. } => "pause",
                    _ => "resume",
                };
                typed
                    .post_pipeline_input_connector_action()
                    .pipeline_name(pipeline)
                    .table_name(table)
                    .connector_name(connector)
                    .action(verb)
                    .send()
                    .await
                    .map_err(typed_error)?;
            }
            Action::Samply {
                pipeline,
                duration_secs,
            } => {
                // Answers 202 once sampling started; the profile comes later
                // through `download`.
                let name = encode_path_segment(pipeline);
                self.http
                    .post(&format!(
                        "/v0/pipelines/{name}/samply_profile?duration_secs={duration_secs}"
                    ))
                    .await?;
            }
        }
        Ok(())
    }

    async fn download(
        &self,
        kind: DownloadKind,
        pipeline: &str,
        bundle: BundleOptions,
    ) -> Result<Download, ApiError> {
        let name = encode_path_segment(pipeline);
        self.http
            .get_download(&format!("/v0/pipelines/{name}/{}", kind.endpoint(bundle)))
            .await
    }

    async fn diagnostics(&self, pipeline: &str) -> Result<Vec<DiagnosticSection>, ApiError> {
        let name = encode_path_segment(pipeline);
        let descriptor = self.http.get_json(&format!("/v0/pipelines/{name}")).await?;
        Ok(crate::model::diagnostics::from_pipeline_json(&descriptor))
    }

    async fn stream_logs(
        &self,
        pipeline: &str,
        sink: tokio::sync::mpsc::UnboundedSender<String>,
    ) -> Result<(), ApiError> {
        let name = encode_path_segment(pipeline);
        self.http
            .stream_lines(&format!("/v0/pipelines/{name}/logs"), &sink)
            .await
    }
}

/// Reduce a generated-client error to the console's [`ApiError`].
fn typed_error(error: feldera_rest_api::Error<feldera_types::error::ErrorResponse>) -> ApiError {
    use feldera_rest_api::Error;
    match error {
        Error::ErrorResponse(response) => ApiError::Status {
            status: response.status().as_u16(),
            message: crate::model::text::terminal_safe(&response.message),
        },
        Error::CommunicationError(error) => ApiError::Transport(error.to_string()),
        Error::UnexpectedResponse(response) => ApiError::Status {
            status: response.status().as_u16(),
            message: "unexpected response".to_string(),
        },
        other => ApiError::Decode(other.to_string()),
    }
}

#[cfg(test)]
mod tests {
    use super::{
        Action, BundleOptions, Download, DownloadKind, Gateway, Overview, RestGateway,
        encode_path_segment,
    };
    use crate::options::ConnectionOptions;
    use serde_json::json;
    use wiremock::matchers::{header, method, path, query_param};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    async fn gateway(server: &MockServer) -> RestGateway {
        RestGateway::connect(&ConnectionOptions {
            host: server.uri(),
            ..Default::default()
        })
        .unwrap()
    }

    #[test]
    fn actions_describe_themselves_and_flag_destructive_ones() {
        let pipeline = "p".to_string();
        let descriptions = [
            (
                Action::Start {
                    pipeline: pipeline.clone(),
                },
                "start",
            ),
            (
                Action::Pause {
                    pipeline: pipeline.clone(),
                },
                "pause",
            ),
            (
                Action::Resume {
                    pipeline: pipeline.clone(),
                },
                "resume",
            ),
            (
                Action::Stop {
                    pipeline: pipeline.clone(),
                    force: false,
                },
                "with checkpoint",
            ),
            (
                Action::Stop {
                    pipeline: pipeline.clone(),
                    force: true,
                },
                "force-stop",
            ),
            (
                Action::Clear {
                    pipeline: pipeline.clone(),
                },
                "clear all storage",
            ),
            (
                Action::ConnectorPause {
                    pipeline: pipeline.clone(),
                    table: "t".to_string(),
                    connector: "c".to_string(),
                },
                "pause connector",
            ),
            (
                Action::ConnectorResume {
                    pipeline: pipeline.clone(),
                    table: "t".to_string(),
                    connector: "c".to_string(),
                },
                "resume connector",
            ),
            (
                Action::Samply {
                    pipeline: pipeline.clone(),
                    duration_secs: 10,
                },
                "10s",
            ),
        ];
        for (action, needle) in descriptions {
            assert!(
                action.describe().contains(needle),
                "`{}` should mention `{needle}`",
                action.describe()
            );
        }
        assert!(
            Action::Stop {
                pipeline: pipeline.clone(),
                force: true
            }
            .needs_confirmation()
        );
        assert!(
            Action::Clear {
                pipeline: pipeline.clone()
            }
            .needs_confirmation()
        );
        assert!(!Action::Start { pipeline }.needs_confirmation());
    }

    #[test]
    fn path_segments_encode_reserved_characters() {
        assert_eq!(encode_path_segment("orders_v2"), "orders_v2");
        assert_eq!(encode_path_segment("a b/c"), "a%20b%2Fc");
    }

    #[tokio::test]
    async fn overview_survives_config_and_session_failures() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/v0/pipelines"))
            .respond_with(
                ResponseTemplate::new(200)
                    .set_body_json(json!([{"name": "p1", "deployment_status": "Running"}])),
            )
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .respond_with(ResponseTemplate::new(500).set_body_string("nope"))
            .mount(&server)
            .await;
        let overview: Overview = gateway(&server).await.overview().await.unwrap();
        assert_eq!(overview.pipelines.len(), 1);
        assert_eq!(overview.instance.edition, "Feldera");
    }

    #[tokio::test]
    async fn overview_fails_when_pipelines_fail() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .respond_with(ResponseTemplate::new(503).set_body_string("down"))
            .mount(&server)
            .await;
        gateway(&server).await.overview().await.unwrap_err();
    }

    #[tokio::test]
    async fn detail_tolerates_a_missing_time_series_endpoint() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/v0/pipelines/p1/stats"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "global_metrics": {"state": "Running", "total_processed_records": 5}
            })))
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/v0/pipelines/p1/time_series"))
            .respond_with(ResponseTemplate::new(404).set_body_string("no"))
            .mount(&server)
            .await;
        let detail = gateway(&server).await.detail("p1").await.unwrap();
        assert_eq!(detail.stats.total_processed_records, 5);
        assert!(detail.series.points.is_empty());
    }

    #[tokio::test]
    async fn program_sql_reads_and_sanitizes_the_program() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/v0/pipelines/p1"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "name": "p1",
                "program_code": "SELECT 1;\u{7}\nSELECT 2;"
            })))
            .mount(&server)
            .await;
        let sql = gateway(&server).await.program_sql("p1").await.unwrap();
        assert_eq!(sql, "SELECT 1;�\nSELECT 2;");
    }

    #[tokio::test]
    async fn hotspots_join_profile_and_dataflow() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/v0/pipelines/p1/circuit_json_profile"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "worker_profiles": [{"metadata": {"n0_1": [
                    {"metric_id": "runtime_seconds",
                     "value": {"type": "duration", "value": {"secs": 2, "nanos": 0}}},
                    {"metric_id": "persistent_id",
                     "value": {"type": "string", "value": "op-1"}}
                ]}}]
            })))
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/v0/pipelines/p1/dataflow_graph"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "mir": {"m1": {
                    "operation": "map",
                    "persistent_id": "op-1",
                    "view": "v",
                    "positions": [{"start_line_number": 1, "start_column": 1,
                                   "end_line_number": 1, "end_column": 9}]
                }}
            })))
            .mount(&server)
            .await;
        let report = gateway(&server).await.hotspots("p1").await.unwrap();
        assert_eq!(report.hotspots.len(), 1);
        assert_eq!(report.mapped_count, 1);
        assert_eq!(report.hotspots[0].relation.as_deref(), Some("v"));
    }

    #[tokio::test]
    async fn restart_stops_waits_for_stopped_then_starts() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/v0/pipelines/p1/stop"))
            .respond_with(ResponseTemplate::new(202))
            .expect(1)
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/v0/pipelines/p1"))
            .respond_with(
                ResponseTemplate::new(200)
                    .set_body_json(json!({"name": "p1", "deployment_status": "Stopped"})),
            )
            .expect(1)
            .mount(&server)
            .await;
        Mock::given(method("POST"))
            .and(path("/v0/pipelines/p1/start"))
            .respond_with(ResponseTemplate::new(202))
            .expect(1)
            .mount(&server)
            .await;
        gateway(&server)
            .await
            .execute(Action::Restart {
                pipeline: "p1".to_string(),
            })
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn restart_propagates_a_failing_status_poll() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/v0/pipelines/p1/stop"))
            .respond_with(ResponseTemplate::new(202))
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/v0/pipelines/p1"))
            .respond_with(ResponseTemplate::new(404).set_body_json(json!({"message": "gone"})))
            .mount(&server)
            .await;
        let error = gateway(&server)
            .await
            .execute(Action::Restart {
                pipeline: "p1".to_string(),
            })
            .await
            .unwrap_err();
        assert!(error.to_string().contains("gone"));
    }

    #[tokio::test]
    async fn lifecycle_actions_hit_the_right_endpoints() {
        let server = MockServer::start().await;
        for endpoint in ["start", "pause", "resume", "stop", "clear"] {
            Mock::given(method("POST"))
                .and(path(format!("/v0/pipelines/p1/{endpoint}")))
                .respond_with(ResponseTemplate::new(202))
                .expect(1)
                .mount(&server)
                .await;
        }
        Mock::given(method("DELETE"))
            .and(path("/v0/pipelines/p1"))
            .respond_with(ResponseTemplate::new(200))
            .expect(1)
            .mount(&server)
            .await;
        Mock::given(method("POST"))
            .and(path("/v0/pipelines/p1/tables/t/connectors/c/pause"))
            .respond_with(ResponseTemplate::new(200))
            .expect(1)
            .mount(&server)
            .await;
        let gateway = gateway(&server).await;
        let pipeline = "p1".to_string();
        for action in [
            Action::Start {
                pipeline: pipeline.clone(),
            },
            Action::Pause {
                pipeline: pipeline.clone(),
            },
            Action::Resume {
                pipeline: pipeline.clone(),
            },
            Action::Stop {
                pipeline: pipeline.clone(),
                force: true,
            },
            Action::Clear {
                pipeline: pipeline.clone(),
            },
            Action::Delete {
                pipeline: pipeline.clone(),
            },
            Action::ConnectorPause {
                pipeline: pipeline.clone(),
                table: "t".to_string(),
                connector: "c".to_string(),
            },
        ] {
            gateway.execute(action).await.unwrap();
        }
    }

    #[tokio::test]
    async fn action_errors_surface_the_api_message() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .respond_with(ResponseTemplate::new(400).set_body_json(json!({
                "message": "pipeline is stopped", "error_code": "X", "details": {}
            })))
            .mount(&server)
            .await;
        let error = gateway(&server)
            .await
            .execute(Action::Resume {
                pipeline: "p1".to_string(),
            })
            .await
            .unwrap_err();
        assert!(error.to_string().contains("pipeline is stopped"));
    }

    #[tokio::test]
    async fn quick_stats_hits_only_the_stats_endpoint() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/v0/pipelines/p1/stats"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "global_metrics": {"total_processed_records": 8}
            })))
            .expect(1)
            .mount(&server)
            .await;
        let gateway = gateway(&server).await;
        assert_eq!(gateway.host(), server.uri());
        let stats = gateway.quick_stats("p1").await.unwrap();
        assert_eq!(stats.total_processed_records, 8);
    }

    #[tokio::test]
    async fn undeclared_statuses_and_garbage_bodies_map_to_errors() {
        let server = MockServer::start().await;
        // 418 is not in the OpenAPI spec: the typed client reports it as an
        // unexpected response.
        Mock::given(method("POST"))
            .and(path("/v0/pipelines/teapot/pause"))
            .respond_with(ResponseTemplate::new(418))
            .mount(&server)
            .await;
        // A declared status with a body that is not an ErrorResponse.
        Mock::given(method("POST"))
            .and(path("/v0/pipelines/garbage/pause"))
            .respond_with(ResponseTemplate::new(500).set_body_string("<html>oops</html>"))
            .mount(&server)
            .await;
        let gateway = gateway(&server).await;
        let error = gateway
            .execute(Action::Pause {
                pipeline: "teapot".to_string(),
            })
            .await
            .unwrap_err();
        assert!(error.to_string().contains("418"), "{error}");
        let error = gateway
            .execute(Action::Pause {
                pipeline: "garbage".to_string(),
            })
            .await
            .unwrap_err();
        assert!(matches!(error, crate::http::ApiError::Decode(_)), "{error}");
    }

    #[tokio::test]
    async fn unreachable_hosts_fail_typed_writes_as_transport() {
        let gateway = RestGateway::connect(&ConnectionOptions {
            host: "http://192.0.2.1:9".to_string(),
            timeout_secs: 1,
            ..Default::default()
        })
        .unwrap();
        let error = gateway
            .execute(Action::Pause {
                pipeline: "p".to_string(),
            })
            .await
            .unwrap_err();
        assert!(error.is_connection_loss(), "{error}");
    }

    #[tokio::test]
    async fn diagnostics_come_from_the_pipeline_descriptor() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/v0/pipelines/p1"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "name": "p1",
                "program_error": {"rust_compilation": {
                    "exit_code": 101,
                    "stderr": "error[E0432]: unresolved import"
                }}
            })))
            .mount(&server)
            .await;
        let sections = gateway(&server).await.diagnostics("p1").await.unwrap();
        assert_eq!(sections.len(), 1);
        assert!(sections[0].title.contains("RUST"));
    }

    #[tokio::test]
    async fn log_streams_forward_lines() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/v0/pipelines/p1/logs"))
            .respond_with(ResponseTemplate::new(200).set_body_string("one\ntwo\n"))
            .mount(&server)
            .await;
        let (line_tx, mut line_rx) = tokio::sync::mpsc::unbounded_channel();
        gateway(&server)
            .await
            .stream_logs("p1", line_tx)
            .await
            .unwrap();
        assert_eq!(line_rx.recv().await.unwrap(), "one");
        assert_eq!(line_rx.recv().await.unwrap(), "two");
    }

    #[tokio::test]
    async fn typed_writes_pick_up_minted_tokens() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/v0/pipelines/p1/pause"))
            .and(header("Authorization", "Bearer minted-token"))
            .respond_with(ResponseTemplate::new(202))
            .expect(1)
            .mount(&server)
            .await;
        let gateway = RestGateway::connect(&ConnectionOptions {
            host: server.uri(),
            auth_command: Some("echo minted-token".to_string()),
            ..Default::default()
        })
        .unwrap();
        gateway
            .execute(Action::Pause {
                pipeline: "p1".to_string(),
            })
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn samply_recording_starts_with_a_post_and_the_profile_comes_from_a_get() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/v0/pipelines/p1/samply_profile"))
            .and(query_param("duration_secs", "5"))
            .respond_with(ResponseTemplate::new(202))
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/v0/pipelines/p1/samply_profile"))
            .and(query_param("latest", "true"))
            .respond_with(ResponseTemplate::new(200).set_body_bytes(b"profile".to_vec()))
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/v0/pipelines/busy/samply_profile"))
            .and(query_param("latest", "true"))
            .respond_with(ResponseTemplate::new(204).insert_header("Retry-After", "4"))
            .mount(&server)
            .await;
        // The default options ask for one fresh collection and nothing else.
        Mock::given(method("GET"))
            .and(path("/v0/pipelines/p1/support_bundle"))
            .and(query_param("limit", "1"))
            .respond_with(ResponseTemplate::new(200).set_body_bytes(b"zip".to_vec()))
            .mount(&server)
            .await;
        let gateway = gateway(&server).await;
        gateway
            .execute(Action::Samply {
                pipeline: "p1".to_string(),
                duration_secs: 5,
            })
            .await
            .unwrap();
        assert_eq!(
            gateway
                .download(DownloadKind::SamplyProfile, "p1", BundleOptions::default())
                .await
                .unwrap(),
            Download::Ready(b"profile".to_vec())
        );
        assert_eq!(
            gateway
                .download(
                    DownloadKind::SamplyProfile,
                    "busy",
                    BundleOptions::default()
                )
                .await
                .unwrap(),
            Download::Pending {
                retry_after_secs: 4
            }
        );
        assert_eq!(
            gateway
                .download(DownloadKind::SupportBundle, "p1", BundleOptions::default())
                .await
                .unwrap(),
            Download::Ready(b"zip".to_vec()),
            "support bundles come straight from GET /support_bundle"
        );
    }

    #[tokio::test]
    async fn tenant_switches_reach_reads_and_writes() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(header("Feldera-Tenant", "acme"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!([])))
            .expect(3)
            .mount(&server)
            .await;
        Mock::given(method("POST"))
            .and(header("Feldera-Tenant", "acme"))
            .and(path("/v0/pipelines/p1/pause"))
            .respond_with(ResponseTemplate::new(202))
            .expect(1)
            .mount(&server)
            .await;
        let gateway = gateway(&server).await;
        gateway.set_tenant(Some("acme".to_string()));
        assert_eq!(gateway.tenant().as_deref(), Some("acme"));
        gateway.overview().await.unwrap();
        gateway
            .execute(Action::Pause {
                pipeline: "p1".to_string(),
            })
            .await
            .unwrap();
    }
}
