//! Test doubles shared by unit tests across modules.

use std::collections::VecDeque;
use std::sync::{Arc, Mutex};

use crate::gateway::{Action, BundleOptions, DownloadKind, Gateway, Overview, PipelineDetail};
use crate::http::{ApiError, Download};
use crate::model::dataflow::HotspotReport;
use crate::model::diagnostics::DiagnosticSection;
use crate::model::header::Session;
use crate::model::pipeline::PipelineRow;
use crate::model::stats::PipelineStats;

/// What the fake returns, call by call, and what it observed.
#[derive(Default)]
pub struct FakeGatewayState {
    pub overviews: VecDeque<Result<Overview, ApiError>>,
    pub details: VecDeque<Result<PipelineDetail, ApiError>>,
    pub quick_stats: VecDeque<Result<PipelineStats, ApiError>>,
    pub sql: VecDeque<Result<String, ApiError>>,
    pub hotspot_reports: VecDeque<Result<HotspotReport, ApiError>>,
    pub action_results: VecDeque<Result<(), ApiError>>,
    pub executed_actions: Vec<Action>,
    /// Scripted answers to `download`; exhausted means "ready, empty".
    pub downloads: VecDeque<Result<Download, ApiError>>,
    pub download_requests: Vec<(DownloadKind, String)>,
    pub detail_requests: Vec<String>,
    pub quick_stats_requests: Vec<String>,
    pub diagnostics: VecDeque<Result<Vec<DiagnosticSection>, ApiError>>,
    /// Scripted log lines; each `stream_logs` call sends them, then ends.
    pub log_lines: Vec<String>,
    pub log_stream_requests: Vec<String>,
    /// How long each `quick_stats` call takes, to model a slow link.
    pub quick_stats_delay: Option<std::time::Duration>,
    pub tenant: Option<String>,
}

/// A scriptable [`Gateway`]: queue responses, inspect recorded calls.
/// An exhausted queue answers with a transport error.
#[derive(Clone, Default)]
pub struct FakeGateway {
    pub state: Arc<Mutex<FakeGatewayState>>,
}

impl FakeGateway {
    pub fn push_overview(&self, overview: Overview) {
        self.state.lock().unwrap().overviews.push_back(Ok(overview));
    }

    pub fn push_detail(&self, detail: PipelineDetail) {
        self.state.lock().unwrap().details.push_back(Ok(detail));
    }

    pub fn push_quick_stats(&self, stats: PipelineStats) {
        self.state.lock().unwrap().quick_stats.push_back(Ok(stats));
    }

    fn exhausted() -> ApiError {
        ApiError::Transport("fake gateway exhausted".to_string())
    }
}

impl Gateway for FakeGateway {
    fn host(&self) -> String {
        "http://fake.test".to_string()
    }

    fn tenant(&self) -> Option<String> {
        self.state.lock().unwrap().tenant.clone()
    }

    fn set_tenant(&self, tenant: Option<String>) {
        self.state.lock().unwrap().tenant = tenant;
    }

    async fn overview(&self) -> Result<Overview, ApiError> {
        self.state
            .lock()
            .unwrap()
            .overviews
            .pop_front()
            .unwrap_or_else(|| Err(Self::exhausted()))
    }

    async fn detail(&self, pipeline: &str) -> Result<PipelineDetail, ApiError> {
        let mut state = self.state.lock().unwrap();
        state.detail_requests.push(pipeline.to_string());
        state
            .details
            .pop_front()
            .unwrap_or_else(|| Err(Self::exhausted()))
    }

    async fn quick_stats(&self, pipeline: &str) -> Result<PipelineStats, ApiError> {
        let delay = {
            let mut state = self.state.lock().unwrap();
            state.quick_stats_requests.push(pipeline.to_string());
            state.quick_stats_delay
        };
        if let Some(delay) = delay {
            tokio::time::sleep(delay).await;
        }
        self.state
            .lock()
            .unwrap()
            .quick_stats
            .pop_front()
            .unwrap_or_else(|| Err(Self::exhausted()))
    }

    async fn program_sql(&self, _pipeline: &str) -> Result<String, ApiError> {
        self.state
            .lock()
            .unwrap()
            .sql
            .pop_front()
            .unwrap_or_else(|| Err(Self::exhausted()))
    }

    async fn hotspots(&self, _pipeline: &str) -> Result<HotspotReport, ApiError> {
        self.state
            .lock()
            .unwrap()
            .hotspot_reports
            .pop_front()
            .unwrap_or_else(|| Err(Self::exhausted()))
    }

    async fn execute(&self, action: Action) -> Result<(), ApiError> {
        let mut state = self.state.lock().unwrap();
        state.executed_actions.push(action);
        state.action_results.pop_front().unwrap_or(Ok(()))
    }

    async fn download(
        &self,
        kind: DownloadKind,
        pipeline: &str,
        _bundle: BundleOptions,
    ) -> Result<Download, ApiError> {
        let mut state = self.state.lock().unwrap();
        state.download_requests.push((kind, pipeline.to_string()));
        state
            .downloads
            .pop_front()
            .unwrap_or(Ok(Download::Ready(Vec::new())))
    }

    async fn diagnostics(&self, _pipeline: &str) -> Result<Vec<DiagnosticSection>, ApiError> {
        self.state
            .lock()
            .unwrap()
            .diagnostics
            .pop_front()
            .unwrap_or_else(|| Ok(Vec::new()))
    }

    async fn stream_logs(
        &self,
        pipeline: &str,
        sink: tokio::sync::mpsc::UnboundedSender<String>,
    ) -> Result<(), ApiError> {
        let lines = {
            let mut state = self.state.lock().unwrap();
            state.log_stream_requests.push(pipeline.to_string());
            state.log_lines.clone()
        };
        for line in lines {
            let _ = sink.send(line);
        }
        Ok(())
    }
}

/// An [`Overview`] with the given pipeline names, all Running and queryable.
pub fn overview_with(names: &[&str]) -> Overview {
    Overview {
        instance: Default::default(),
        session: Session::default(),
        pipelines: names
            .iter()
            .map(|name| PipelineRow {
                name: name.to_string(),
                deployment_status: "Running".to_string(),
                program_status: "Success".to_string(),
                ..Default::default()
            })
            .collect(),
    }
}

/// Stats with the given processed-record counter.
pub fn stats_with_processed(processed_records: i64) -> PipelineStats {
    PipelineStats::from_json(&serde_json::json!({
        "global_metrics": {
            "state": "Running",
            "total_processed_records": processed_records,
            "rss_bytes": 1024,
            "storage_bytes": 2048,
            "cpu_msecs": 100,
            "uptime_msecs": 60_000
        },
        "inputs": [{
            "endpoint_name": "orders_in",
            "config": {"stream": "orders"},
            "metrics": {"total_records": processed_records, "total_bytes": 500}
        }],
        "outputs": []
    }))
}
