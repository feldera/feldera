use chrono::NaiveDateTime;
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;
use uuid::Uuid;

use crate::db::types::pipeline::PipelineStatus;

// Pipeline Lifecycle Events
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, ToSchema)]
pub struct PipelineLifecycleEvent {
    pub event_id: Uuid,
    // pub tenant_id: TenantId,
    // pub pipeline_id: PipelineId,
    pub deployment_status: PipelineStatus,
    pub info: Option<String>,
    pub recorded_at: NaiveDateTime,
}
