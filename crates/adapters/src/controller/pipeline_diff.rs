use feldera_adapterlib::errors::journal::ControllerError;
use feldera_types::{
    config::PipelineConfig,
    pipeline_diff::{PipelineDiff, ProgramDiff, program_diff},
    program_schema::ProgramSchema,
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

pub fn compute_pipeline_diff(
    old_config: &PipelineConfig,
    new_config: &PipelineConfig,
) -> Result<PipelineDiff, ControllerError> {
    let diff = compute_program_diff(old_config, new_config);

    if let Ok((diff, blockers)) = &diff
        && !blockers.is_empty()
        && !diff.is_empty()
    {
        return Err(ControllerError::BootstrapNotAllowed {
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
    use feldera_types::config::PipelineConfig;
    use serde_json::{Map, Value, json};

    fn input_endpoint(mut fields: Map<String, Value>) -> Value {
        let mut endpoint = Map::from_iter([
            ("stream".to_string(), json!("t1")),
            ("transport".to_string(), json!({"name": "empty_input"})),
        ]);
        endpoint.append(&mut fields);
        Value::Object(endpoint)
    }

    fn pipeline_config(input: Value) -> PipelineConfig {
        serde_json::from_value(json!({
            "name": "test",
            "workers": 1,
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
