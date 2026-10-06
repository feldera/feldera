use crate::db::error::DBError;
use crate::db::types::pipeline::ExtendedPipelineDescr;
use crate::db::types::resources_status::{ResourcesDesiredStatus, ResourcesStatus};
use crate::db::types::utils::ValidationError;
use feldera_types::config::{PipelineConfig, ResourceConfig, RuntimeConfig};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value, json};
use utoipa::ToSchema;

/// Fields that can change while the pipeline runs. `merge_deployment_config` and
/// `merge_deployment_config_on_edit` must cover the same ones.
const RUNTIME_MODIFIABLE_FIELDS: [&str; 4] = [
    "resources.cpu_cores_min",
    "resources.cpu_cores_max",
    "resources.memory_mb_min",
    "resources.memory_mb_max",
];

/// The deployment of a running pipeline.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, ToSchema)]
pub struct PipelineDeployment {
    pub resources: ResourceConfig,
}

impl PipelineDeployment {
    pub fn from_deployment_config(deployment_config: &Value) -> Result<Self, DBError> {
        let resources =
            resources_of(deployment_config).map_err(|error| DBError::InvalidDeploymentConfig {
                value: deployment_config.clone(),
                error: ValidationError::DeserializationFailed(error),
            })?;
        Ok(Self { resources })
    }
}

/// The deployment config of a running pipeline.
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

/// Applies `patch`, a partial runtime configuration, to a deployment configuration and returns
/// the new one. Only the CPU and memory in `resources` can change.
pub fn patch_deployment_config(deployment_config: &Value, patch: &Value) -> Result<Value, DBError> {
    let invalid = |error: String| DBError::InvalidDeploymentConfig {
        value: deployment_config.clone(),
        error: ValidationError::DeserializationFailed(error),
    };
    let current = PipelineDeployment::from_deployment_config(deployment_config)?.resources;
    let mut stored: PipelineConfig =
        serde_json::from_value(deployment_config.clone()).map_err(|e| invalid(e.to_string()))?;
    let resources = apply_patch(stored.global.hosts, current, patch)
        .map_err(|reason| DBError::InvalidDeploymentPatch { reason })?;
    let mut patched = stored.global.clone();
    patched.resources = resources;
    merge_deployment_config(&mut stored, &patched);
    serde_json::to_value(stored).map_err(|e| invalid(e.to_string()))
}

/// The resources after `patch`, checked against what Kubernetes can change while the pod runs.
fn apply_patch(
    hosts: usize,
    current: ResourceConfig,
    patch: &Value,
) -> Result<ResourceConfig, String> {
    let known = known_fields::<RuntimeConfig>(patch)?;
    for (field, value) in patch.as_object().into_iter().flatten() {
        if value.is_null() || field == "resources" {
            continue;
        }
        if !known.contains_key(field) {
            return Err(format!("unknown field `{field}`"));
        }
        return Err(format!(
            "`{field}` cannot be changed while the pipeline is running"
        ));
    }
    let resources_patch = patch
        .get("resources")
        .filter(|v| !v.is_null())
        .cloned()
        .unwrap_or_else(|| json!({}));
    let known = known_fields::<ResourceConfig>(&resources_patch)?;
    for (field, value) in resources_patch.as_object().into_iter().flatten() {
        if value.is_null()
            || RUNTIME_MODIFIABLE_FIELDS.contains(&format!("resources.{field}").as_str())
        {
            continue;
        }
        if !known.contains_key(field) {
            return Err(format!("unknown field `resources.{field}`"));
        }
        return Err(format!(
            "`resources.{field}` cannot be changed while the pipeline is running"
        ));
    }
    let resize: ResourceConfig = serde_json::from_value(resources_patch)
        .map_err(|e| format!("unable to parse resources: {e}"))?;
    if resize.cpu_cores_min.is_none()
        && resize.cpu_cores_max.is_none()
        && resize.memory_mb_min.is_none()
        && resize.memory_mb_max.is_none()
    {
        return Err(
            "specify at least one of resources.cpu_cores_min, resources.cpu_cores_max, \
             resources.memory_mb_min, resources.memory_mb_max"
                .to_string(),
        );
    }
    if hosts > 1 {
        return Err(format!("a pipeline with {hosts} hosts cannot be resized"));
    }
    let mut new = current.clone();
    new.cpu_cores_min = check_cpu("cpu_cores_min", current.cpu_cores_min, resize.cpu_cores_min)?;
    new.cpu_cores_max = check_cpu("cpu_cores_max", current.cpu_cores_max, resize.cpu_cores_max)?;
    new.memory_mb_min = check_memory("memory_mb_min", current.memory_mb_min, resize.memory_mb_min)?;
    new.memory_mb_max = check_memory("memory_mb_max", current.memory_mb_max, resize.memory_mb_max)?;
    if let (Some(min), Some(max)) = (new.cpu_cores_min, new.cpu_cores_max)
        && min > max
    {
        return Err(format!(
            "cpu_cores_min ({min}) exceeds cpu_cores_max ({max})"
        ));
    }
    if let (Some(min), Some(max)) = (new.memory_mb_min, new.memory_mb_max)
        && min > max
    {
        return Err(format!(
            "memory_mb_min ({min}) exceeds memory_mb_max ({max})"
        ));
    }
    if !is_same_qos_class(&current, &new) {
        return Err(
            "each request must stay equal to, or different from, its limit, so the pod keeps \
             its Kubernetes QoS class"
                .to_string(),
        );
    }

    Ok(new)
}

/// Copies the runtime modifiable fields of `from` into a deployment config, leaving every other
/// setting.
pub fn merge_deployment_config(into: &mut PipelineConfig, from: &RuntimeConfig) {
    into.global.resources.cpu_cores_min = from.resources.cpu_cores_min;
    into.global.resources.cpu_cores_max = from.resources.cpu_cores_max;
    into.global.resources.memory_mb_min = from.resources.memory_mb_min;
    into.global.resources.memory_mb_max = from.resources.memory_mb_max;
}

/// The stored deployment config with the runtime modifiable fields of the edited runtime config,
/// or `None` when the edit leaves them unchanged.
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
            config.resources.memory_mb_min,
            config.resources.memory_mb_max,
        )
    };
    if live(&old) == live(&new) {
        return None;
    }
    let mut stored: PipelineConfig = serde_json::from_value(deployment_config.clone()).ok()?;
    merge_deployment_config(&mut stored, &new);
    serde_json::to_value(stored).ok()
}

/// The fields of `value` that `T` keeps after a round trip, which drops unknown fields.
fn known_fields<T: DeserializeOwned + Serialize>(
    value: &Value,
) -> Result<Map<String, Value>, String> {
    if !value.is_object() {
        return Err(format!("expected a JSON object, found {value}"));
    }
    let parsed: T = serde_json::from_value(value.clone()).map_err(|e| e.to_string())?;
    match serde_json::to_value(parsed) {
        Ok(Value::Object(fields)) => Ok(fields),
        _ => Err("unable to serialize the configuration".to_string()),
    }
}

fn resources_of(config: &Value) -> Result<ResourceConfig, String> {
    let resources = config
        .get("resources")
        .ok_or("deployment configuration has no resources")?;
    ResourceConfig::deserialize(resources).map_err(|e| format!("unable to parse resources: {e}"))
}

/// Kubernetes cannot change a running pod's QoS class. Keeping each request equal to, or
/// different from, its limit keeps the class without knowing how unset values are filled in.
fn is_same_qos_class(current: &ResourceConfig, new: &ResourceConfig) -> bool {
    let cpu = |r: &ResourceConfig| r.cpu_cores_min == r.cpu_cores_max;
    let memory = |r: &ResourceConfig| r.memory_mb_min == r.memory_mb_max;
    cpu(current) == cpu(new) && memory(current) == memory(new)
}

fn check_cpu(name: &str, current: Option<f64>, new: Option<f64>) -> Result<Option<f64>, String> {
    let Some(new) = new else {
        return Ok(current);
    };
    if !new.is_finite() || new <= 0.0 {
        return Err(format!("{name} must be greater than 0"));
    }
    match current {
        None => Err(format!("{name} is not set, so it cannot be resized")),
        Some(_) => Ok(Some(new)),
    }
}

fn check_memory(name: &str, current: Option<u64>, new: Option<u64>) -> Result<Option<u64>, String> {
    let Some(new) = new else {
        return Ok(current);
    };
    if new == 0 {
        return Err(format!("{name} must be greater than 0"));
    }
    match current {
        None => Err(format!("{name} is not set, so it cannot be resized")),
        Some(_) => Ok(Some(new)),
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
                "memory_mb_min": 16000, "memory_mb_max": 16000,
                "storage_mb_max": 1000, "namespace": null
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
        patch_deployment_config(config, patch).map_err(|e| match e {
            DBError::InvalidDeploymentPatch { reason } => reason,
            e => panic!("unexpected error: {e:?}"),
        })
    }

    #[test]
    fn resize_changes_only_the_given_fields() {
        let new = try_patch(
            &deployment(),
            &resize(json!({"cpu_cores_min": 1.0, "memory_mb_min": 4000, "memory_mb_max": 4000})),
        )
        .unwrap();
        let mut expected = deployment();
        expected["resources"]["cpu_cores_min"] = json!(1.0);
        expected["resources"]["memory_mb_min"] = json!(4000);
        expected["resources"]["memory_mb_max"] = json!(4000);
        assert_eq!(new, normalized(expected));
    }

    #[test]
    fn increases_are_allowed() {
        let new = try_patch(
            &deployment(),
            &resize(json!({"cpu_cores_max": 16.0, "memory_mb_min": 24000, "memory_mb_max": 24000})),
        )
        .unwrap();
        assert_eq!(new["resources"]["cpu_cores_max"], json!(16.0));
        assert_eq!(new["resources"]["memory_mb_max"], json!(24000));
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
        assert_refused(json!({"memory_mb_max": 0}), "greater than 0");
        assert_refused(json!({"memory_mb_min": 17000}), "exceeds");
        assert_refused(json!({"cpu_cores_min": 9.0}), "exceeds");
        // Every request equal to its limit would make the pod Guaranteed.
        assert_refused(json!({"cpu_cores_max": 4.0}), "QoS");
        let mut multihost = deployment();
        multihost["hosts"] = json!(2);
        let error = try_patch(&multihost, &resize(json!({"cpu_cores_min": 1.0}))).unwrap_err();
        assert!(error.contains("2 hosts"), "{error}");
        for (patch, expected) in [
            (json!({"workers": 2}), "`workers` cannot be changed"),
            (json!({"no_such_field": 2}), "unknown field `no_such_field`"),
            (
                json!({"resources": {"workers": 2}}),
                "unknown field `resources.workers`",
            ),
            // Refused even when it repeats the current value.
            (
                json!({"resources": {"storage_mb_max": 1000}}),
                "`resources.storage_mb_max` cannot be changed",
            ),
            (
                json!({"resources": {"storage_mb_max": 5}}),
                "`resources.storage_mb_max` cannot be changed",
            ),
            (
                json!({"resources": {"cpu_cores_min": "two"}}),
                "invalid type",
            ),
            (json!(["resources"]), "expected a JSON object"),
        ] {
            let error = try_patch(&deployment(), &patch).unwrap_err();
            assert!(error.contains(expected), "{patch}: {error}");
        }
    }

    #[test]
    fn unset_field_cannot_be_resized() {
        let mut config = deployment();
        config["resources"]["cpu_cores_min"] = json!(null);
        let error = try_patch(&config, &resize(json!({"cpu_cores_min": 1.0}))).unwrap_err();
        assert!(error.contains("not set"), "{error}");
    }

    #[test]
    fn request_keeps_its_relation_to_the_limit() {
        let mut config = deployment();
        config["resources"]["cpu_cores_max"] = json!(4.0);
        // Memory request and limit are equal, so they must stay equal.
        let error = try_patch(&config, &resize(json!({"memory_mb_min": 8000}))).unwrap_err();
        assert!(error.contains("QoS"), "{error}");
        assert!(
            try_patch(
                &config,
                &resize(json!({"memory_mb_min": 8000, "memory_mb_max": 8000}))
            )
            .is_ok()
        );
    }

    #[test]
    fn unset_memory_limit_cannot_be_resized() {
        let mut config = deployment();
        config["resources"]["memory_mb_max"] = json!(null);
        let error = try_patch(&config, &resize(json!({"memory_mb_max": 8000}))).unwrap_err();
        assert!(error.contains("not set"), "{error}");
    }

    #[test]
    fn unset_cpu_request_stays_unset() {
        let mut config = deployment();
        config["resources"]["cpu_cores_min"] = json!(null);
        let new = try_patch(&config, &resize(json!({"cpu_cores_max": 2.0}))).unwrap();
        assert_eq!(new["resources"]["cpu_cores_max"], json!(2.0));
        assert_eq!(new["resources"]["cpu_cores_min"], json!(null));
        let error = try_patch(&config, &resize(json!({"memory_mb_min": 8000}))).unwrap_err();
        assert!(error.contains("QoS"), "{error}");
    }

    #[test]
    fn same_qos_class_is_checked_per_resource() {
        // CPU 2/4 and memory 3000/4000: making only CPU equal keeps the pod Burstable in
        // Kubernetes, but each resource must keep its own relation.
        let mut config = deployment();
        config["resources"] = json!({"cpu_cores_min": 2.0, "cpu_cores_max": 4.0, "memory_mb_min": 3000, "memory_mb_max": 4000});
        let error = try_patch(&config, &resize(json!({"cpu_cores_min": 4.0}))).unwrap_err();
        assert!(error.contains("QoS class"), "{error}");
    }

    #[test]
    fn broken_stored_resources_are_a_server_error() {
        let error = patch_deployment_config(
            &json!({"workers": 4}),
            &resize(json!({"cpu_cores_min": 1.0})),
        )
        .unwrap_err();
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
        assert_eq!(new["resources"]["storage_mb_max"], json!(1000));
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
            assert!(
                PipelineDeployment::from_deployment_config(&config).is_err(),
                "{config}"
            );
        }
        assert!(PipelineDeployment::from_deployment_config(&deployment()).is_ok());
    }

    #[test]
    fn merge_copies_only_live_settings() {
        let mut into: PipelineConfig = serde_json::from_value(deployment()).unwrap();
        let mut from = into.global.clone();
        from.resources.cpu_cores_min = Some(1.0);
        from.resources.storage_mb_max = Some(5000);
        from.workers = 9;
        merge_deployment_config(&mut into, &from);
        assert_eq!(into.global.resources.cpu_cores_min, Some(1.0));
        assert_eq!(into.global.resources.storage_mb_max, Some(1000));
        assert_eq!(into.global.workers, 4);
    }

    /// Each runtime modifiable field is detected as changed and copied by the merge, so the three
    /// stay in step when one is added.
    #[test]
    fn every_live_setting_is_merged_and_detected() {
        let base = deployment();
        for path in RUNTIME_MODIFIABLE_FIELDS {
            let pointer = format!("/{}", path.replace('.', "/"));
            let mut changed = base.clone();
            *changed.pointer_mut(&pointer).unwrap() = json!(12345);
            assert!(
                merge_deployment_config_on_edit(&base, &base, &changed).is_some(),
                "{path}"
            );
            let new: RuntimeConfig = serde_json::from_value(changed.clone()).unwrap();
            let mut into: PipelineConfig = serde_json::from_value(base.clone()).unwrap();
            merge_deployment_config(&mut into, &new);
            let merged = serde_json::to_value(into).unwrap();
            assert_eq!(
                merged.pointer(&pointer).and_then(Value::as_f64),
                Some(12345.0),
                "{path}"
            );
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
        same["resources"]["memory_mb_max"] = json!(20000);
        let edited = merge_deployment_config_on_edit(&resized, &runtime, &same).unwrap();
        let mut expected = deployment();
        expected["resources"]["memory_mb_min"] = json!(null);
        expected["resources"]["memory_mb_max"] = json!(20000);
        assert_eq!(edited, normalized(expected));
    }
}
