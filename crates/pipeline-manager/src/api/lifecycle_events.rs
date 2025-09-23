use chrono::NaiveDateTime;
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;
use uuid::Uuid;

// Pipeline Lifecycle Events
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, ToSchema)]
pub struct PipelineLifecycleEvent {
    pub event_id: Uuid,
    pub deployment_resources_status: String,
    pub deployment_runtime_status: Option<String>,
    pub deployment_runtime_desired_status: Option<String>,
    pub info: Option<String>,
    pub recorded_at: NaiveDateTime,
}
