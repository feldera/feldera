use std::collections::HashMap;

use feldera_types::program_schema::SourcePosition;
use serde::Deserialize;
use serde_json::Value;

use crate::CalciteId;

pub type MirNodeId = String;

#[derive(Debug, Deserialize)]
pub struct MirInput {
    pub node: String,
    pub output: usize,

    #[serde(flatten)]
    pub extra: HashMap<String, Value>,
}

#[derive(Debug, Deserialize)]
pub struct MirNode {
    #[serde(default)]
    pub calcite: Option<CalciteId>,

    #[serde(default)]
    pub table: Option<String>,

    #[serde(default)]
    pub inputs: Vec<MirInput>,

    pub operation: String,

    #[serde(default)]
    pub outputs: Option<Vec<MirInput>>,

    #[serde(default)]
    pub persistent_id: Option<String>,

    #[serde(default)]
    pub positions: Vec<SourcePosition>,

    #[serde(flatten)]
    pub extra: HashMap<String, Value>,
}
