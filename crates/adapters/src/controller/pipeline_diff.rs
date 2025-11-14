use feldera_adapterlib::errors::journal::ControllerError;
use feldera_types::{
    config::PipelineConfig,
    pipeline_diff::{program_diff, PipelineDiff, ProgramDiff},
};
use std::collections::BTreeMap;
use std::fmt::Display;

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

fn compute_program_diff(
    old_config: &PipelineConfig,
    new_config: &PipelineConfig,
) -> Result<(ProgramDiff, BootstrapBlockers), String> {
    let Some(old_dataflow) = &old_config.program_ir else {
        return Err("Unable to compute the diff between the checkpointed and new pipeline configurations: the checkpointed configuration does not contain program information. It was likely created by an old version of Feldera.".to_owned());
    };

    let Some(new_dataflow) = &new_config.program_ir else {
        return Err("Unable to compute the diff between the checkpointed and new pipeline configurations: the new configuration does not contain program information. It was likely created by an old version of Feldera.".to_owned());
    };

    let new_relations_with_lateness = new_dataflow
        .program_schema
        .relations_with_lateness()
        .into_iter()
        .map(|s| s.name())
        .collect::<Vec<_>>();
    let old_relations_with_lateness = old_dataflow
        .program_schema
        .relations_with_lateness()
        .into_iter()
        .map(|s| s.name())
        .collect::<Vec<_>>();

    let blockers = BootstrapBlockers {
        new_relations_with_lateness,
        old_relations_with_lateness,
    };

    Ok((program_diff(&old_dataflow.mir, &new_dataflow.mir), blockers))
}

pub fn compute_pipeline_diff(
    old_config: &PipelineConfig,
    new_config: &PipelineConfig,
) -> Result<PipelineDiff, ControllerError> {
    let diff = compute_program_diff(old_config, new_config);

    if let Ok((diff, blockers)) = &diff {
        if !blockers.is_empty() && !diff.is_empty() {
            return Err(ControllerError::BootstrapNotAllowed {
                error: blockers.to_string(),
            });
        }
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
                    .equal_modulo_paused(&v.connector_config)
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
