use crate::db::error::DBError;
use crate::db::types::pipeline::ExtendedPipelineDescr;
use crate::db::types::resources_status::{ResourcesDesiredStatus, ResourcesStatus};
use crate::db::types::utils::ValidationError;
use feldera_types::config::{PipelineConfig, ResourceConfig, RuntimeConfig};
use feldera_types::size::{ConfigSize, LegacySizeUnit};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use utoipa::ToSchema;

/// The part of a deployment config that can change while the pipeline runs.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize, ToSchema)]
#[serde(deny_unknown_fields)]
pub struct PipelineDeploymentConfig {
    #[serde(default)]
    pub resources: DeploymentResources,
}

feldera_types::size_setting!(size_memory_min, "memory_min", LegacySizeUnit::Megabytes);
feldera_types::size_setting!(size_memory_max, "memory_max", LegacySizeUnit::Megabytes);

// `merge_deployment_config` and `merge_deployment_config_on_edit` must cover the same fields.
/// CPU and memory, the only fields that can change while the pipeline runs.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize, ToSchema)]
#[serde(deny_unknown_fields)]
pub struct DeploymentResources {
    pub cpu_cores_min: Option<f64>,
    pub cpu_cores_max: Option<f64>,
    /// The minimum memory, such as `"4Gi"`. `memory_mb_min`, a number of
    /// megabytes, is a deprecated alias.
    #[serde(default, alias = "memory_mb_min", deserialize_with = "size_memory_min")]
    #[schema(value_type = Option<String>)]
    pub memory_min: Option<ConfigSize>,
    /// The maximum memory, such as `"16Gi"`. `memory_mb_max`, a number of
    /// megabytes, is a deprecated alias.
    #[serde(default, alias = "memory_mb_max", deserialize_with = "size_memory_max")]
    #[schema(value_type = Option<String>)]
    pub memory_max: Option<ConfigSize>,
}

/// The API view of a stored deployment config.
///
/// A config stored before the size rename holds its memory under
/// `memory_mb_min` and `memory_mb_max`, as numbers of megabytes; the view
/// shows them under the current names, as sizes.
pub fn deployment_config_view(deployment_config: &Value) -> Value {
    let resources = deployment_config.get("resources");
    let field = |name: &str| {
        resources
            .and_then(|r| r.get(name))
            .cloned()
            .unwrap_or(Value::Null)
    };
    let size = |name: &str, legacy: &str| match field(name) {
        Value::Null => match field(legacy) {
            // Written as a size is serialized, so that a megabyte count the
            // size format cannot show with a unit, such as 0, reads back.
            Value::Number(mb) => mb
                .as_u64()
                .and_then(|mb| serde_json::to_value(ConfigSize::from_mb(mb)).ok())
                .unwrap_or(Value::Number(mb)),
            other => other,
        },
        current => current,
    };
    json!({"resources": {
        "cpu_cores_min": field("cpu_cores_min"),
        "cpu_cores_max": field("cpu_cores_max"),
        "memory_min": size("memory_min", "memory_mb_min"),
        "memory_max": size("memory_max", "memory_mb_max"),
    }})
}

/// Checks that a `PUT` or `POST` does not change the deployment config.
pub fn check_unchanged_deployment_config(
    stored: Option<&Value>,
    given: &Value,
) -> Result<(), DBError> {
    if given.is_null() {
        return Ok(());
    }
    let invalid = |reason: String| DBError::InvalidDeploymentPatch { reason };
    let given: PipelineDeploymentConfig =
        serde_json::from_value(given.clone()).map_err(|e| invalid(e.to_string()))?;
    let stored: PipelineDeploymentConfig = match stored {
        Some(stored) => serde_json::from_value(deployment_config_view(stored))
            .map_err(|e| invalid(e.to_string()))?,
        None => PipelineDeploymentConfig::default(),
    };
    let (given, stored) = (given.resources, stored.resources);
    let changed = (given.cpu_cores_min.is_some() && given.cpu_cores_min != stored.cpu_cores_min)
        || (given.cpu_cores_max.is_some() && given.cpu_cores_max != stored.cpu_cores_max)
        || (given.memory_min.is_some() && given.memory_min != stored.memory_min)
        || (given.memory_max.is_some() && given.memory_max != stored.memory_max);
    if changed {
        return Err(invalid(
            "`deployment_config` can only be changed with PATCH while the pipeline is running"
                .to_string(),
        ));
    }
    Ok(())
}

/// The deployment config, if the pipeline is running.
pub fn running_deployment_config(pipeline: &ExtendedPipelineDescr) -> Result<&Value, DBError> {
    match (
        pipeline.deployment_resources_status,
        pipeline.deployment_resources_desired_status,
        &pipeline.deployment_config,
    ) {
        (
            ResourcesStatus::Provisioned,
            ResourcesDesiredStatus::Provisioned,
            Some(deployment_config),
        ) => Ok(deployment_config),
        _ => Err(DBError::DeploymentRestrictedToRunning),
    }
}

/// Parses the `deployment_config` of a `PATCH`.
pub fn parse_deployment_config_patch(patch: &Value) -> Result<PipelineDeploymentConfig, DBError> {
    serde_json::from_value(patch.clone()).map_err(|e| DBError::InvalidDeploymentPatch {
        reason: e.to_string(),
    })
}

/// Applies `patch` to a stored deployment config.
pub fn patch_deployment_config(
    deployment_config: &Value,
    patch: &PipelineDeploymentConfig,
) -> Result<Value, DBError> {
    let invalid = |error: String| DBError::InvalidDeploymentConfig {
        value: deployment_config.clone(),
        error: ValidationError::DeserializationFailed(error),
    };
    let current = resources_of(deployment_config).map_err(invalid)?;
    let mut stored: PipelineConfig =
        serde_json::from_value(deployment_config.clone()).map_err(|e| invalid(e.to_string()))?;
    let resources = apply_patch(stored.global.hosts, current, &patch.resources)
        .map_err(|reason| DBError::InvalidDeploymentPatch { reason })?;
    let mut patched = stored.global.clone();
    patched.resources = resources;
    merge_deployment_config(&mut stored, &patched);
    serde_json::to_value(stored).map_err(|e| invalid(e.to_string()))
}

/// The resources after `patch`. The runner checks what Kubernetes allows.
fn apply_patch(
    hosts: usize,
    current: ResourceConfig,
    resize: &DeploymentResources,
) -> Result<ResourceConfig, String> {
    if resize == &DeploymentResources::default() {
        return Err(
            "specify at least one of resources.cpu_cores_min, resources.cpu_cores_max, \
             resources.memory_min, resources.memory_max"
                .to_string(),
        );
    }
    if hosts > 1 {
        return Err("only single-host pipelines can be resized".to_string());
    }
    check_cpu("cpu_cores_min", resize.cpu_cores_min)?;
    check_cpu("cpu_cores_max", resize.cpu_cores_max)?;
    check_memory("memory_min", resize.memory_min)?;
    check_memory("memory_max", resize.memory_max)?;
    let mut new = current.clone();
    new.cpu_cores_min = resize.cpu_cores_min.or(current.cpu_cores_min);
    new.cpu_cores_max = resize.cpu_cores_max.or(current.cpu_cores_max);
    new.memory_min = resize.memory_min.or(current.memory_min);
    new.memory_max = resize.memory_max.or(current.memory_max);
    if let (Some(min), Some(max)) = (new.cpu_cores_min, new.cpu_cores_max)
        && min > max
    {
        return Err(format!(
            "cpu_cores_min ({min}) exceeds cpu_cores_max ({max})"
        ));
    }
    if let (Some(min), Some(max)) = (new.memory_min, new.memory_max)
        && min > max
    {
        return Err(format!("memory_min ({min}) exceeds memory_max ({max})"));
    }

    Ok(new)
}

/// Copies CPU and memory from `from` into `into`.
pub fn merge_deployment_config(into: &mut PipelineConfig, from: &RuntimeConfig) {
    into.global.resources.cpu_cores_min = from.resources.cpu_cores_min;
    into.global.resources.cpu_cores_max = from.resources.cpu_cores_max;
    into.global.resources.memory_min = from.resources.memory_min;
    into.global.resources.memory_max = from.resources.memory_max;
}

/// The stored deployment config with CPU and memory from an edited runtime config, or `None` if
/// they did not change.
pub fn merge_deployment_config_on_edit(
    deployment_config: &Value,
    old_runtime_config: &Value,
    new_runtime_config: &Value,
) -> Option<Value> {
    let old: RuntimeConfig = serde_json::from_value(old_runtime_config.clone()).ok()?;
    let new: RuntimeConfig = serde_json::from_value(new_runtime_config.clone()).ok()?;
    // Only an edit of a runtime modifiable field replaces a runtime change.
    let live = |config: &RuntimeConfig| {
        (
            config.resources.cpu_cores_min,
            config.resources.cpu_cores_max,
            config.resources.memory_min,
            config.resources.memory_max,
        )
    };
    if live(&old) == live(&new) {
        return None;
    }
    let mut stored: PipelineConfig = serde_json::from_value(deployment_config.clone()).ok()?;
    merge_deployment_config(&mut stored, &new);
    serde_json::to_value(stored).ok()
}

/// Parses the `resources` of a deployment config.
fn resources_of(config: &Value) -> Result<ResourceConfig, String> {
    let resources = config
        .get("resources")
        .ok_or("deployment configuration has no resources")?;
    ResourceConfig::deserialize(resources).map_err(|e| format!("unable to parse resources: {e}"))
}

/// Checks a CPU value from a patch.
fn check_cpu(name: &str, value: Option<f64>) -> Result<(), String> {
    match value {
        Some(value) if !value.is_finite() => Err(format!("{name} must be a finite number")),
        Some(value) if value <= 0.0 => Err(format!("{name} must be greater than 0")),
        _ => Ok(()),
    }
}

/// Checks a memory value from a patch.
fn check_memory(name: &str, value: Option<ConfigSize>) -> Result<(), String> {
    match value {
        Some(size) if size.is_zero() => Err(format!("{name} must be greater than 0")),
        _ => Ok(()),
    }
}

#[cfg(test)]
mod test {
    use super::*;

    fn deployment() -> Value {
        json!({
            "workers": 4,
            "name": "pipeline-x",
            "inputs": {},
            "resources": {
                "cpu_cores_min": 4.0, "cpu_cores_max": 8.0,
                "memory_min": "16G", "memory_max": "16G",
                "storage_max": "1G", "namespace": null
            }
        })
    }

    fn resize(value: Value) -> Value {
        json!({ "resources": value })
    }

    /// `config` as it reads after a round trip through `PipelineConfig`.
    fn normalized(config: Value) -> Value {
        serde_json::to_value(serde_json::from_value::<PipelineConfig>(config).unwrap()).unwrap()
    }

    /// The patched config, or why the patch was refused.
    fn try_patch(config: &Value, patch: &Value) -> Result<Value, String> {
        parse_deployment_config_patch(patch)
            .and_then(|patch| patch_deployment_config(config, &patch))
            .map_err(|e| match e {
                DBError::InvalidDeploymentPatch { reason } => reason,
                e => panic!("unexpected error: {e:?}"),
            })
    }

    #[test]
    fn resize_changes_only_the_given_fields() {
        let new = try_patch(
            &deployment(),
            &resize(json!({"cpu_cores_min": 1.0, "memory_min": "4G", "memory_max": "4G"})),
        )
        .unwrap();
        let mut expected = deployment();
        expected["resources"]["cpu_cores_min"] = json!(1.0);
        expected["resources"]["memory_min"] = json!("4G");
        expected["resources"]["memory_max"] = json!("4G");
        assert_eq!(new, normalized(expected));

        // The deprecated spelling of a patch means the same.
        let legacy = try_patch(
            &deployment(),
            &resize(json!({"cpu_cores_min": 1.0, "memory_mb_min": 4000, "memory_mb_max": 4000})),
        )
        .unwrap();
        assert_eq!(legacy, new);
    }

    #[test]
    fn increases_are_allowed() {
        let new = try_patch(
            &deployment(),
            &resize(json!({"cpu_cores_max": 16.0, "memory_min": "24G", "memory_max": "24G"})),
        )
        .unwrap();
        assert_eq!(new["resources"]["cpu_cores_max"], json!(16.0));
        assert_eq!(new["resources"]["memory_max"], json!("24G"));
    }

    fn assert_refused(value: Value, expected: &str) {
        let error = try_patch(&deployment(), &resize(value.clone())).unwrap_err();
        assert!(error.contains(expected), "{value}: {error}");
    }

    #[test]
    fn invalid_resizes_are_refused() {
        assert_refused(json!({}), "specify at least one of");
        assert_refused(json!({"cpu_cores_min": null}), "specify at least one of");
        assert_refused(json!({"cpu_cores_max": -1.0}), "greater than 0");
        assert_refused(json!({"cpu_cores_min": 0.0}), "greater than 0");
        assert_refused(json!({"memory_max": "0M"}), "greater than 0");
        assert_refused(json!({"memory_min": "17G"}), "exceeds");
        assert_refused(json!({"memory_min": "17 GB"}), "unknown unit");
        assert_refused(json!({"cpu_cores_min": 9.0}), "exceeds");
        let mut multihost = deployment();
        multihost["hosts"] = json!(2);
        let error = try_patch(&multihost, &resize(json!({"cpu_cores_min": 1.0}))).unwrap_err();
        assert!(error.contains("single-host"), "{error}");
        for (patch, expected) in [
            (
                json!({"workers": 2}),
                "unknown field `workers`, expected `resources`",
            ),
            (json!({"no_such_field": 2}), "unknown field `no_such_field`"),
            (
                json!({"resources": {"workers": 2}}),
                "unknown field `workers`, expected one of `cpu_cores_min`",
            ),
            // Refused even when it repeats the current value.
            (
                json!({"resources": {"storage_mb_max": 1000}}),
                "unknown field `storage_mb_max`",
            ),
            (
                json!({"resources": {"cpu_cores_min": "two"}}),
                "invalid type",
            ),
            (json!(["resources"]), "invalid type"),
        ] {
            let error = try_patch(&deployment(), &patch).unwrap_err();
            assert!(error.contains(expected), "{patch}: {error}");
        }
    }

    /// The runner, not the API, refuses what Kubernetes cannot change while the pod runs.
    #[test]
    fn kubernetes_rules_are_left_to_the_runner() {
        // CPU request equal to its limit: a QoS class change.
        let new = try_patch(&deployment(), &resize(json!({"cpu_cores_max": 4.0}))).unwrap();
        assert_eq!(new["resources"]["cpu_cores_max"], json!(4.0));
        // An unset value can be set, and an unset value that is not given stays unset.
        let mut config = deployment();
        config["resources"]["cpu_cores_min"] = json!(null);
        let new = try_patch(&config, &resize(json!({"cpu_cores_max": 2.0}))).unwrap();
        assert_eq!(new["resources"]["cpu_cores_min"], json!(null));
        let new = try_patch(&config, &resize(json!({"cpu_cores_min": 1.0}))).unwrap();
        assert_eq!(new["resources"]["cpu_cores_min"], json!(1.0));
    }

    #[test]
    fn non_finite_cpu_is_refused() {
        for value in [f64::INFINITY, f64::NEG_INFINITY, f64::NAN] {
            let error = check_cpu("cpu_cores_max", Some(value)).unwrap_err();
            assert_eq!(error, "cpu_cores_max must be a finite number", "{value}");
        }
    }

    #[test]
    fn multihost_pipeline_cannot_be_resized() {
        let mut config = deployment();
        config["hosts"] = json!(2);
        let error = try_patch(&config, &resize(json!({"cpu_cores_min": 1.0}))).unwrap_err();
        assert_eq!(error, "only single-host pipelines can be resized");
    }

    #[test]
    fn broken_stored_resources_are_a_server_error() {
        let patch = parse_deployment_config_patch(&resize(json!({"cpu_cores_min": 1.0}))).unwrap();
        let error = patch_deployment_config(&json!({"workers": 4}), &patch).unwrap_err();
        assert!(
            matches!(error, DBError::InvalidDeploymentConfig { .. }),
            "{error:?}"
        );
    }

    #[test]
    fn single_host_and_other_fields_are_kept() {
        let mut config = deployment();
        config["hosts"] = json!(1);
        let new = try_patch(&config, &resize(json!({"cpu_cores_min": 1.0}))).unwrap();
        assert_eq!(new["resources"]["storage_max"], json!("1G"));
        assert_eq!(new["name"], json!("pipeline-x"));
        assert_eq!(new["hosts"], json!(1));
    }

    #[test]
    fn broken_deployment_config_is_an_error() {
        for config in [
            json!(1),
            json!({"workers": 4}),
            json!({"resources": {"cpu_cores_min": "one"}}),
        ] {
            assert!(resources_of(&config).is_err(), "{config}");
        }
        assert!(resources_of(&deployment()).is_ok());
    }

    #[test]
    fn view_shows_only_the_runtime_modifiable_fields() {
        assert_eq!(
            deployment_config_view(&deployment()),
            json!({"resources": {
                "cpu_cores_min": 4.0, "cpu_cores_max": 8.0,
                "memory_min": "16G", "memory_max": "16G",
            }})
        );
        // A config stored before the size rename shows its memory as sizes.
        assert_eq!(
            deployment_config_view(&json!({"resources": {"memory_mb_max": 16000}})),
            json!({"resources": {
                "cpu_cores_min": null, "cpu_cores_max": null,
                "memory_min": null, "memory_max": "16G",
            }})
        );
        // A zero megabyte count has no unit as a size, so it is written as
        // the size format writes it, which a size field reads back.
        assert_eq!(
            deployment_config_view(&json!({"resources": {"memory_mb_max": 0}}))["resources"]["memory_max"],
            json!("0B")
        );
        // A config that does not parse still shows what it has.
        assert_eq!(
            deployment_config_view(&json!({"resources": {"cpu_cores_min": "one"}})),
            json!({"resources": {
                "cpu_cores_min": "one", "cpu_cores_max": null,
                "memory_min": null, "memory_max": null,
            }})
        );
    }

    #[test]
    fn put_accepts_only_an_unchanged_deployment_config() {
        let stored = deployment();
        let view = deployment_config_view(&stored);
        let unchanged = [
            json!(null),
            view.clone(),
            json!({}),
            json!({"resources": {"cpu_cores_min": 4, "memory_max": null}}),
            json!({"resources": {"memory_mb_max": 16000}}),
        ];
        for given in unchanged {
            assert!(
                check_unchanged_deployment_config(Some(&stored), &given).is_ok(),
                "{given}"
            );
        }
        // A stored zero megabyte count reads as a size too.
        let zero = json!({"resources": {"memory_mb_max": 0}});
        for given in [json!({}), json!({"resources": {"memory_max": "0B"}})] {
            assert!(
                check_unchanged_deployment_config(Some(&zero), &given).is_ok(),
                "{given}"
            );
        }
        let changed = [
            json!({"resources": {"cpu_cores_min": 2.0}}),
            json!({"resources": {"memory_max": "1M"}}),
            json!({"resources": {"workers": 4}}),
            json!({"workers": 4}),
            json!(1),
        ];
        for given in changed {
            assert!(
                check_unchanged_deployment_config(Some(&stored), &given).is_err(),
                "{given}"
            );
        }
        // Without a stored config, only empty values are unchanged.
        assert!(check_unchanged_deployment_config(None, &json!(null)).is_ok());
        assert!(
            check_unchanged_deployment_config(None, &json!({"resources": {"cpu_cores_min": null}}))
                .is_ok()
        );
        assert!(check_unchanged_deployment_config(None, &view).is_err());
    }

    #[test]
    fn merge_copies_only_live_settings() {
        let mut into: PipelineConfig = serde_json::from_value(deployment()).unwrap();
        let mut from = into.global.clone();
        from.resources.cpu_cores_min = Some(1.0);
        from.resources.storage_max = Some(ConfigSize::from_mb(5000));
        from.workers = 9;
        merge_deployment_config(&mut into, &from);
        assert_eq!(into.global.resources.cpu_cores_min, Some(1.0));
        assert_eq!(
            into.global.resources.storage_max,
            Some(ConfigSize::from_mb(1000))
        );
        assert_eq!(into.global.workers, 4);
    }

    /// Each runtime modifiable field is detected as changed and copied by the merge, so the three
    /// stay in step when one is added.
    #[test]
    fn every_live_setting_is_merged_and_detected() {
        let base = deployment();
        let fields = serde_json::to_value(DeploymentResources::default()).unwrap();
        for field in fields.as_object().unwrap().keys() {
            let path = format!("resources.{field}");
            let pointer = format!("/resources/{field}");
            // A memory field holds a size, written back as it was given.
            let value = if field.starts_with("memory") {
                json!("12345M")
            } else {
                json!(12345.0)
            };
            let mut changed = base.clone();
            *changed.pointer_mut(&pointer).unwrap() = value.clone();
            assert!(
                merge_deployment_config_on_edit(&base, &base, &changed).is_some(),
                "{path}"
            );
            let new: RuntimeConfig = serde_json::from_value(changed.clone()).unwrap();
            let mut into: PipelineConfig = serde_json::from_value(base.clone()).unwrap();
            merge_deployment_config(&mut into, &new);
            let merged = serde_json::to_value(into).unwrap();
            assert_eq!(merged.pointer(&pointer), Some(&value), "{path}");
        }
    }

    #[test]
    fn only_a_cpu_or_memory_edit_updates_the_deployment_config() {
        let runtime = json!({"workers": 4, "resources": {"cpu_cores_min": 4, "cpu_cores_max": 8}});
        let mut resized = deployment();
        resized["resources"]["cpu_cores_min"] = json!(1.0);
        // The same values written differently, and other fields, are not a CPU or memory edit.
        let mut same =
            json!({"workers": 8, "resources": {"cpu_cores_min": 4.0, "cpu_cores_max": 8.0}});
        assert_eq!(
            merge_deployment_config_on_edit(&resized, &runtime, &same),
            None
        );
        same["resources"]["memory_max"] = json!("20G");
        let edited = merge_deployment_config_on_edit(&resized, &runtime, &same).unwrap();
        let mut expected = deployment();
        expected["resources"]["memory_min"] = json!(null);
        expected["resources"]["memory_max"] = json!("20G");
        assert_eq!(edited, normalized(expected));
    }
}
