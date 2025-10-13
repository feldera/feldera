use feldera_ir::{MirNode, MirNodeId};
use serde::{Deserialize, Serialize};
use std::{collections::HashMap, fmt::Display};
use utoipa::ToSchema;

/// Summary of changes in the program between checkpointed and new versions.
#[derive(Debug, Default, Clone, Serialize, Deserialize, ToSchema, PartialEq, Eq)]
pub struct ProgramDiff {
    pub added_tables: Vec<String>,
    pub removed_tables: Vec<String>,
    pub modified_tables: Vec<String>,

    pub added_views: Vec<String>,
    pub removed_views: Vec<String>,
    pub modified_views: Vec<String>,
}

impl Display for ProgramDiff {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        fn quoted_list(list: &[String]) -> String {
            list.iter()
                .map(|s| format!("'{}'", s))
                .collect::<Vec<_>>()
                .join(", ")
        }

        if !self.added_tables.is_empty() {
            writeln!(f, "Added tables: {}", quoted_list(&self.added_tables))?;
        }
        if !self.removed_tables.is_empty() {
            writeln!(f, "Removed tables: {}", quoted_list(&self.removed_tables))?;
        }
        if !self.modified_tables.is_empty() {
            writeln!(f, "Modified tables: {}", quoted_list(&self.modified_tables))?;
        }
        if !self.added_views.is_empty() {
            writeln!(f, "Added views: {}", quoted_list(&self.added_views))?;
        }
        if !self.removed_views.is_empty() {
            writeln!(f, "Removed views: {}", quoted_list(&self.removed_views))?;
        }
        if !self.modified_views.is_empty() {
            writeln!(f, "Modified views: {}", quoted_list(&self.modified_views))?;
        }
        Ok(())
    }
}

impl ProgramDiff {
    pub fn is_empty(&self) -> bool {
        self.added_tables.is_empty()
            && self.removed_tables.is_empty()
            && self.modified_tables.is_empty()
            && self.added_views.is_empty()
            && self.removed_views.is_empty()
            && self.modified_views.is_empty()
    }

    pub fn is_affected_relation(&self, relation_name: &str) -> bool {
        let relation_name = relation_name.to_string();
        self.added_tables.contains(&relation_name)
            || self.removed_tables.contains(&relation_name)
            || self.modified_tables.contains(&relation_name)
            || self.added_views.contains(&relation_name)
            || self.removed_views.contains(&relation_name)
            || self.modified_views.contains(&relation_name)
    }
}

/// Summary of changes in the pipeline between checkpointed and new versions.
#[derive(Debug, Serialize, Deserialize, ToSchema, Clone, PartialEq, Eq)]
pub struct PipelineDiff {
    /// IR changes or the reason why we couldn't compute them.
    pub program_diff: Option<ProgramDiff>,
    pub program_diff_error: Option<String>,

    pub added_input_connectors: Vec<String>,
    pub modified_input_connectors: Vec<String>,
    pub removed_input_connectors: Vec<String>,

    pub added_output_connectors: Vec<String>,
    pub modified_output_connectors: Vec<String>,
    pub removed_output_connectors: Vec<String>,
}

impl Display for PipelineDiff {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if let Some(err) = &self.program_diff_error {
            writeln!(f, "Could not compute program diff: {err}")?;
        }

        if let Some(diff) = &self.program_diff {
            if !diff.is_empty() {
                writeln!(f, "Program changes:")?;
                for change in diff.to_string().lines() {
                    writeln!(f, "  {change}")?;
                }
            }
        }

        if !self.added_input_connectors.is_empty() {
            writeln!(
                f,
                "Added input connectors: {}",
                self.added_input_connectors.join(", ")
            )?;
        }

        if !self.removed_input_connectors.is_empty() {
            writeln!(
                f,
                "Removed input connectors: {}",
                self.removed_input_connectors.join(", ")
            )?;
        }

        if !self.modified_input_connectors.is_empty() {
            writeln!(
                f,
                "Modified input connectors: {}",
                self.modified_input_connectors.join(", ")
            )?;
        }

        if !self.added_output_connectors.is_empty() {
            writeln!(
                f,
                "Added output connectors: {}",
                self.added_output_connectors.join(", ")
            )?;
        }

        if !self.removed_output_connectors.is_empty() {
            writeln!(
                f,
                "Removed output connectors: {}",
                self.removed_output_connectors.join(", ")
            )?;
        }

        if !self.modified_output_connectors.is_empty() {
            writeln!(
                f,
                "Modified output connectors: {}",
                self.modified_output_connectors.join(", ")
            )?;
        }

        Ok(())
    }
}

impl PipelineDiff {
    pub fn is_empty(&self) -> bool {
        self.program_diff
            .as_ref()
            .map(|diff| diff.is_empty())
            .unwrap_or(false)
            && self.added_input_connectors.is_empty()
            && self.removed_input_connectors.is_empty()
            && self.modified_input_connectors.is_empty()
            && self.added_output_connectors.is_empty()
            && self.removed_output_connectors.is_empty()
            && self.modified_output_connectors.is_empty()
    }

    pub fn clear_program_diff(&mut self) {
        self.program_diff = Some(ProgramDiff::default());
    }

    pub fn is_affected_connector(&self, connector_name: &str) -> bool {
        let connector_name = connector_name.to_string();
        self.added_input_connectors.contains(&connector_name)
            || self.removed_input_connectors.contains(&connector_name)
            || self.modified_input_connectors.contains(&connector_name)
            || self.added_output_connectors.contains(&connector_name)
            || self.removed_output_connectors.contains(&connector_name)
            || self.modified_output_connectors.contains(&connector_name)
    }
}

pub fn program_diff(
    old_mir: &HashMap<MirNodeId, MirNode>,
    new_mir: &HashMap<MirNodeId, MirNode>,
) -> ProgramDiff {
    let old_tables: HashMap<String, String> = old_mir
        .values()
        .filter(|node| node.persistent_id.is_some())
        .filter_map(|node| {
            node.table
                .as_ref()
                .map(|name| (name.clone(), node.persistent_id.clone().unwrap()))
        })
        .collect();

    let old_views: HashMap<String, String> = old_mir
        .values()
        .filter(|node| node.persistent_id.is_some())
        .filter_map(|node| {
            node.view
                .as_ref()
                .map(|name| (name.clone(), node.persistent_id.clone().unwrap()))
        })
        .collect();

    let new_tables: HashMap<String, String> = new_mir
        .values()
        .filter(|node| node.persistent_id.is_some())
        .filter_map(|node| {
            node.table
                .as_ref()
                .map(|name| (name.clone(), node.persistent_id.clone().unwrap()))
        })
        .collect();

    let new_views: HashMap<String, String> = new_mir
        .values()
        .filter(|node| node.persistent_id.is_some())
        .filter_map(|node| {
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
