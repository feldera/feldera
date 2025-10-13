use std::collections::HashMap;

use serde::{Deserialize, Serialize};
use serde_json::Value;
use utoipa::ToSchema;

use crate::{CalciteId, SourcePosition};

pub type MirNodeId = String;

#[derive(Clone, Debug, Serialize, Deserialize, ToSchema, PartialEq, Eq)]
pub struct MirInput {
    pub node: String,
    pub output: usize,

    #[serde(flatten)]
    pub extra: HashMap<String, Value>,
}

#[derive(Clone, Debug, Serialize, Deserialize, ToSchema, PartialEq, Eq)]
pub struct MirNode {
    #[serde(default)]
    pub calcite: Option<CalciteId>,

    #[serde(default)]
    pub table: Option<String>,

    #[serde(default)]
    pub view: Option<String>,

    #[serde(default)]
    pub inputs: Vec<MirInput>,

    pub operation: String,

    #[serde(default)]
    pub outputs: Option<Vec<Option<MirInput>>>,

    #[serde(default)]
    pub persistent_id: Option<String>,

    #[serde(default)]
    pub positions: Vec<SourcePosition>,

    #[serde(flatten)]
    pub extra: HashMap<String, Value>,
}
