use crate::config::PipelineConfigProgramInfo;
use crate::program_schema::ProgramSchema;
use feldera_ir::{MirNode, MirNodeId};
use serde::{Deserialize, Serialize};
use std::{
    collections::{BTreeMap, HashMap},
    fmt::Display,
};
use utoipa::ToSchema;

/// Summary of changes in the program between checkpointed and new versions.
#[derive(Debug, Default, Clone, Serialize, Deserialize, ToSchema, PartialEq, Eq)]
pub struct ProgramDiff {
    added_tables: Vec<String>,
    removed_tables: Vec<String>,
    modified_tables: Vec<String>,

    added_views: Vec<String>,
    removed_views: Vec<String>,
    modified_views: Vec<String>,
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
    pub fn new() -> Self {
        Self {
            added_tables: Vec::new(),
            removed_tables: Vec::new(),
            modified_tables: Vec::new(),
            added_views: Vec::new(),
            removed_views: Vec::new(),
            modified_views: Vec::new(),
        }
    }

    pub fn with_added_tables(mut self, mut tables: Vec<String>) -> Self {
        tables.sort();
        self.added_tables = tables;
        self
    }

    pub fn with_removed_tables(mut self, mut tables: Vec<String>) -> Self {
        tables.sort();
        self.removed_tables = tables;
        self
    }

    pub fn with_modified_tables(mut self, mut tables: Vec<String>) -> Self {
        tables.sort();
        self.modified_tables = tables;
        self
    }

    pub fn with_added_views(mut self, mut views: Vec<String>) -> Self {
        views.sort();
        self.added_views = views;
        self
    }

    pub fn with_removed_views(mut self, mut views: Vec<String>) -> Self {
        views.sort();
        self.removed_views = views;
        self
    }

    pub fn with_modified_views(mut self, mut views: Vec<String>) -> Self {
        views.sort();
        self.modified_views = views;
        self
    }

    pub fn added_tables(&self) -> &Vec<String> {
        &self.added_tables
    }

    pub fn removed_tables(&self) -> &Vec<String> {
        &self.removed_tables
    }

    pub fn modified_tables(&self) -> &Vec<String> {
        &self.modified_tables
    }

    pub fn added_views(&self) -> &Vec<String> {
        &self.added_views
    }

    pub fn removed_views(&self) -> &Vec<String> {
        &self.removed_views
    }

    pub fn modified_views(&self) -> &Vec<String> {
        &self.modified_views
    }

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
    program_diff: Option<ProgramDiff>,
    program_diff_error: Option<String>,

    added_input_connectors: Vec<String>,
    modified_input_connectors: Vec<String>,
    removed_input_connectors: Vec<String>,

    added_output_connectors: Vec<String>,
    modified_output_connectors: Vec<String>,
    removed_output_connectors: Vec<String>,
}

impl Display for PipelineDiff {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if let Some(err) = &self.program_diff_error {
            writeln!(f, "Could not compute program diff: {err}")?;
        }

        if let Some(diff) = &self.program_diff
            && !diff.is_empty()
        {
            writeln!(f, "Program changes:")?;
            for change in diff.to_string().lines() {
                writeln!(f, "  {change}")?;
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
    pub fn new(program_diff_or_err: Result<ProgramDiff, String>) -> Self {
        match program_diff_or_err {
            Ok(program_diff) => Self::new_with_program_diff(program_diff),
            Err(program_diff_error) => Self::new_with_program_diff_error(program_diff_error),
        }
    }

    pub fn new_with_program_diff(program_diff: ProgramDiff) -> Self {
        Self {
            program_diff: Some(program_diff),
            program_diff_error: None,
            added_input_connectors: Vec::new(),
            modified_input_connectors: Vec::new(),
            removed_input_connectors: Vec::new(),
            added_output_connectors: Vec::new(),
            modified_output_connectors: Vec::new(),
            removed_output_connectors: Vec::new(),
        }
    }

    pub fn new_with_program_diff_error(program_diff_error: String) -> Self {
        Self {
            program_diff: None,
            program_diff_error: Some(program_diff_error),
            added_input_connectors: Vec::new(),
            modified_input_connectors: Vec::new(),
            removed_input_connectors: Vec::new(),
            added_output_connectors: Vec::new(),
            modified_output_connectors: Vec::new(),
            removed_output_connectors: Vec::new(),
        }
    }

    pub fn with_added_input_connectors(mut self, mut connectors: Vec<String>) -> Self {
        connectors.sort();
        self.added_input_connectors = connectors;
        self
    }

    pub fn with_modified_input_connectors(mut self, mut connectors: Vec<String>) -> Self {
        connectors.sort();
        self.modified_input_connectors = connectors;
        self
    }

    pub fn with_removed_input_connectors(mut self, mut connectors: Vec<String>) -> Self {
        connectors.sort();
        self.removed_input_connectors = connectors;
        self
    }

    pub fn with_added_output_connectors(mut self, mut connectors: Vec<String>) -> Self {
        connectors.sort();
        self.added_output_connectors = connectors;
        self
    }

    pub fn with_modified_output_connectors(mut self, mut connectors: Vec<String>) -> Self {
        connectors.sort();
        self.modified_output_connectors = connectors;
        self
    }

    pub fn with_removed_output_connectors(mut self, mut connectors: Vec<String>) -> Self {
        connectors.sort();
        self.removed_output_connectors = connectors;
        self
    }

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

    pub fn is_affected_relation(&self, relation_name: &str) -> bool {
        self.program_diff
            .as_ref()
            .map(|diff| diff.is_affected_relation(relation_name))
            .unwrap_or(false)
    }

    pub fn program_diff(&self) -> Option<&ProgramDiff> {
        self.program_diff.as_ref()
    }

    pub fn program_diff_error(&self) -> Option<&String> {
        self.program_diff_error.as_ref()
    }

    pub fn added_input_connectors(&self) -> &Vec<String> {
        &self.added_input_connectors
    }

    pub fn modified_input_connectors(&self) -> &Vec<String> {
        &self.modified_input_connectors
    }

    pub fn removed_input_connectors(&self) -> &Vec<String> {
        &self.removed_input_connectors
    }

    pub fn added_output_connectors(&self) -> &Vec<String> {
        &self.added_output_connectors
    }

    pub fn modified_output_connectors(&self) -> &Vec<String> {
        &self.modified_output_connectors
    }

    pub fn removed_output_connectors(&self) -> &Vec<String> {
        &self.removed_output_connectors
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

    ProgramDiff::new()
        .with_added_tables(added_tables)
        .with_removed_tables(removed_tables)
        .with_modified_tables(modified_tables)
        .with_added_views(added_views)
        .with_removed_views(removed_views)
        .with_modified_views(modified_views)
}

/// Reasons bootstrapping cannot be performed.
struct BootstrapBlockers {
    /// The new version of the program has relations with lateness.
    new_relations_with_lateness: Vec<String>,

    /// The old version of the program has relations with lateness.
    old_relations_with_lateness: Vec<String>,
}

impl BootstrapBlockers {
    fn is_empty(&self) -> bool {
        self.new_relations_with_lateness.is_empty() && self.old_relations_with_lateness.is_empty()
    }
}

impl Display for BootstrapBlockers {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if !self.new_relations_with_lateness.is_empty() {
            writeln!(
                f,
                "- The new version of the program has relations with lateness: {}",
                self.new_relations_with_lateness.join(", ")
            )?;
        }
        if !self.old_relations_with_lateness.is_empty() {
            writeln!(
                f,
                "- The checkpointed version of the program has relations with lateness: {}",
                self.old_relations_with_lateness.join(", ")
            )?;
        }
        Ok(())
    }
}

/// Error returned by [`compute_pipeline_diff`].
#[derive(Debug, Clone, thiserror::Error)]
pub enum PipelineDiffError {
    /// The change requires bootstrapping, but bootstrapping is not allowed
    /// because the old or the new program has relations with lateness.
    #[error("{error}")]
    BootstrapNotAllowed { error: String },
}

fn compute_program_diff(
    old_config: &PipelineConfigProgramInfo,
    new_config: &PipelineConfigProgramInfo,
) -> Result<(ProgramDiff, BootstrapBlockers), String> {
    let Some(old_dataflow) = &old_config.program_ir else {
        return Err("Unable to compute the diff between the checkpointed and new pipeline configurations: the checkpointed configuration does not contain program information. It was likely created by an old version of Feldera.".to_owned());
    };

    let Some(new_dataflow) = &new_config.program_ir else {
        return Err("Unable to compute the diff between the checkpointed and new pipeline configurations: the new configuration does not contain program information. It was likely created by an old version of Feldera.".to_owned());
    };

    // TODO: consider parsing only the necessary subset of schema fields to avoid compatibility issues
    // with older runtime versions.
    let new_program_schema: ProgramSchema =
        serde_json::from_value(new_dataflow.program_schema.clone())
            .map_err(|e| format!("Error parsing new program schema: {}", e))?;
    let old_program_schema: ProgramSchema =
        serde_json::from_value(old_dataflow.program_schema.clone())
            .map_err(|e| format!("Error parsing old program schema: {}", e))?;

    let new_relations_with_lateness = new_program_schema
        .relations_with_lateness()
        .into_iter()
        .map(|s| s.name().to_string())
        .collect::<Vec<_>>();
    let old_relations_with_lateness = old_program_schema
        .relations_with_lateness()
        .into_iter()
        .map(|s| s.name().to_string())
        .collect::<Vec<_>>();

    let blockers = BootstrapBlockers {
        new_relations_with_lateness,
        old_relations_with_lateness,
    };

    Ok((program_diff(&old_dataflow.mir, &new_dataflow.mir), blockers))
}

/// Compute the diff between two pipeline configurations.
///
/// Compares the compiler-generated program IR and connector configuration of
/// `old_config` and `new_config`, reporting added, removed, and modified
/// tables, views, and connectors. It drives bootstrapping when a pipeline
/// resumes from a checkpoint, and backs the `/diff` API endpoint that previews
/// changes without restarting the pipeline.
pub fn compute_pipeline_diff(
    old_config: &PipelineConfigProgramInfo,
    new_config: &PipelineConfigProgramInfo,
) -> Result<PipelineDiff, PipelineDiffError> {
    let diff = compute_program_diff(old_config, new_config);

    if let Ok((diff, blockers)) = &diff
        && !blockers.is_empty()
        && !diff.is_empty()
    {
        return Err(PipelineDiffError::BootstrapNotAllowed {
            error: blockers.to_string(),
        });
    };

    let mir_diff = diff.map(|(diff, _)| diff.clone());

    let mut old_configured_inputs = old_config
        .inputs
        .iter()
        .filter(|(_, cfg)| !cfg.connector_config.transport.is_transient())
        .map(|(name, cfg)| (name.clone(), cfg.clone()))
        .collect::<BTreeMap<_, _>>();

    old_configured_inputs.remove("now");

    let old_configured_outputs = old_config
        .outputs
        .iter()
        .filter(|(_, cfg)| !cfg.connector_config.transport.is_transient())
        .map(|(name, cfg)| (name.clone(), cfg.clone()))
        .collect::<BTreeMap<_, _>>();

    let added_input_connectors = new_config
        .inputs
        .keys()
        .filter(|k| !old_configured_inputs.contains_key(*k))
        .map(|k| k.to_string())
        .collect::<Vec<_>>();

    let removed_input_connectors = old_configured_inputs
        .iter()
        .filter(|(k, config)| {
            !new_config.inputs.contains_key(*k)
                && !mir_diff
                    .as_ref()
                    .map(|mir_diff| {
                        mir_diff
                            .removed_tables()
                            .contains(&config.stream.to_string())
                    })
                    .unwrap_or(true)
        })
        .map(|(k, _)| k.to_string())
        .collect::<Vec<_>>();

    let modified_input_connectors = new_config
        .inputs
        .iter()
        .filter(|(k, v)| {
            old_configured_inputs.contains_key(*k)
                && !old_configured_inputs
                    .get(*k)
                    .unwrap()
                    .connector_config
                    .equal_for_input_checkpoint_replay(&v.connector_config)
        })
        .map(|(k, _)| k.to_string())
        .collect::<Vec<_>>();

    let added_output_connectors = new_config
        .outputs
        .keys()
        .filter(|k| !old_configured_outputs.contains_key(*k))
        .map(|k| k.to_string())
        .collect::<Vec<_>>();

    let removed_output_connectors = old_configured_outputs
        .iter()
        .filter(|(k, config)| {
            !new_config.outputs.contains_key(*k)
                && !mir_diff
                    .as_ref()
                    .map(|mir_diff| {
                        mir_diff
                            .removed_views()
                            .contains(&config.stream.to_string())
                    })
                    .unwrap_or(true)
        })
        .map(|(k, _)| k.to_string())
        .collect::<Vec<_>>();

    let modified_output_connectors = new_config
        .outputs
        .iter()
        .filter(|(k, v)| {
            old_configured_outputs.contains_key(*k)
                && !old_configured_outputs
                    .get(*k)
                    .unwrap()
                    .connector_config
                    .equal_modulo_paused(&v.connector_config)
        })
        .map(|(k, _)| k.to_string())
        .collect::<Vec<_>>();

    Ok(PipelineDiff::new(mir_diff)
        .with_added_input_connectors(added_input_connectors)
        .with_modified_input_connectors(modified_input_connectors)
        .with_removed_input_connectors(removed_input_connectors)
        .with_added_output_connectors(added_output_connectors)
        .with_modified_output_connectors(modified_output_connectors)
        .with_removed_output_connectors(removed_output_connectors))
}

#[cfg(test)]
mod tests {
    use super::compute_pipeline_diff;
    use crate::config::PipelineConfigProgramInfo;
    use serde_json::{Map, Value, json};

    fn input_endpoint(mut fields: Map<String, Value>) -> Value {
        let mut endpoint = Map::from_iter([
            ("stream".to_string(), json!("t1")),
            ("transport".to_string(), json!({"name": "empty_input"})),
        ]);
        endpoint.append(&mut fields);
        Value::Object(endpoint)
    }

    fn pipeline_config(input: Value) -> PipelineConfigProgramInfo {
        serde_json::from_value(json!({
            "inputs": {
                "t1.connector": input
            }
        }))
        .unwrap()
    }

    #[test]
    fn input_flow_control_changes_do_not_modify_connector() {
        let old_config = pipeline_config(input_endpoint(Map::new()));
        let new_config = pipeline_config(input_endpoint(Map::from_iter([
            ("max_queued_records".to_string(), json!(5000)),
            ("max_queued_bytes".to_string(), json!(6000)),
            ("max_batch_size".to_string(), json!(7000)),
            ("max_worker_batch_size".to_string(), json!(8000)),
            ("paused".to_string(), json!(true)),
        ])));

        let diff = compute_pipeline_diff(&old_config, &new_config).unwrap();

        assert!(diff.modified_input_connectors().is_empty());
    }

    #[test]
    fn input_transport_changes_still_modify_connector() {
        let old_config = pipeline_config(input_endpoint(Map::new()));
        let new_config = pipeline_config(input_endpoint(Map::from_iter([(
            "transport".to_string(),
            json!({
                "name": "datagen",
                "config": {
                    "plan": [{"limit": 1}]
                }
            }),
        )])));

        let diff = compute_pipeline_diff(&old_config, &new_config).unwrap();

        assert_eq!(
            diff.modified_input_connectors(),
            &vec!["t1.connector".to_string()]
        );
    }
}
