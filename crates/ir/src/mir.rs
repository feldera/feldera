use std::collections::HashMap;

use feldera_types::{pipeline_diff::ProgramDiff, program_schema::SourcePosition};
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
    pub view: Option<String>,

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

pub fn program_diff(
    old_mir: &HashMap<MirNodeId, MirNode>,
    new_mir: &HashMap<MirNodeId, MirNode>,
) -> ProgramDiff {
    let old_tables: HashMap<String, String> = old_mir
        .iter()
        .filter(|(_, node)| node.persistent_id.is_some())
        .filter_map(|(_, node)| {
            node.table
                .as_ref()
                .map(|name| (name.clone(), node.persistent_id.clone().unwrap()))
        })
        .collect();

    let old_views: HashMap<String, String> = old_mir
        .iter()
        .filter(|(_, node)| node.persistent_id.is_some())
        .filter_map(|(_, node)| {
            node.view
                .as_ref()
                .map(|name| (name.clone(), node.persistent_id.clone().unwrap()))
        })
        .collect();

    let new_tables: HashMap<String, String> = new_mir
        .iter()
        .filter(|(_, node)| node.persistent_id.is_some())
        .filter_map(|(_, node)| {
            node.table
                .as_ref()
                .map(|name| (name.clone(), node.persistent_id.clone().unwrap()))
        })
        .collect();

    let new_views: HashMap<String, String> = new_mir
        .iter()
        .filter(|(_, node)| node.persistent_id.is_some())
        .filter_map(|(_, node)| {
            node.view
                .as_ref()
                .map(|name| (name.clone(), node.persistent_id.clone().unwrap()))
        })
        .collect();

    let added_tables = new_tables
        .keys()
        .filter(|k| !old_tables.contains_key(*k))
        .cloned()
        .collect();
    let removed_tables = old_tables
        .keys()
        .filter(|k| !new_tables.contains_key(*k))
        .cloned()
        .collect();
    let modified_tables = new_tables
        .iter()
        .filter_map(|(name, id)| {
            if let Some(old_id) = old_tables.get(name) {
                if old_id != id {
                    Some(name.clone())
                } else {
                    None
                }
            } else {
                None
            }
        })
        .collect();

    let added_views = new_views
        .keys()
        .filter(|k| !old_views.contains_key(*k))
        .cloned()
        .collect();
    let removed_views = old_views
        .keys()
        .filter(|k| !new_views.contains_key(*k))
        .cloned()
        .collect();
    let modified_views = new_views
        .iter()
        .filter_map(|(name, id)| {
            if let Some(old_id) = old_views.get(name) {
                if old_id != id {
                    Some(name.clone())
                } else {
                    None
                }
            } else {
                None
            }
        })
        .collect();

    ProgramDiff {
        added_tables,
        removed_tables,
        modified_tables,
        added_views,
        removed_views,
        modified_views,
    }
}
