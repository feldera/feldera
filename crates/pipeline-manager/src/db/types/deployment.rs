use crate::db::error::DBError;
use crate::db::types::pipeline::ExtendedPipelineDescr;
use crate::db::types::resources_status::{ResourcesDesiredStatus, ResourcesStatus};
use crate::db::types::utils::ValidationError;
use feldera_types::config::{PipelineConfig, ResourceConfig, RuntimeConfig};
use feldera_types::size::ConfigSize;
use serde::Deserialize;
use serde_json::{Value, json};

// `merge_deployment_config` and `merge_deployment_config_on_edit` must cover the same fields as
// `DynamicResources`.
pub use feldera_types::config::{DynamicConfig, DynamicResources};

/// The API view of a stored deployment config.
///
/// A config stored before the size rename holds its memory and storage under
/// `memory_mb_min`, `memory_mb_max`, `storage_mb_min`, and `storage_mb_max`, as numbers of
/// megabytes; the view shows them under the current names, as sizes.
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
        "storage_min": size("storage_min", "storage_mb_min"),
        "storage_max": size("storage_max", "storage_mb_max"),
        "volume_attributes_class": field("volume_attributes_class"),
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
    let given: DynamicConfig =
        serde_json::from_value(given.clone()).map_err(|e| invalid(e.to_string()))?;
    let stored: DynamicConfig = match stored {
        Some(stored) => serde_json::from_value(deployment_config_view(stored))
            .map_err(|e| invalid(e.to_string()))?,
        None => DynamicConfig::default(),
    };
    let (given, stored) = (given.resources, stored.resources);
    let changed = (given.cpu_cores_min.is_some() && given.cpu_cores_min != stored.cpu_cores_min)
        || (given.cpu_cores_max.is_some() && given.cpu_cores_max != stored.cpu_cores_max)
        || (given.memory_min.is_some() && given.memory_min != stored.memory_min)
        || (given.memory_max.is_some() && given.memory_max != stored.memory_max)
        || (given.storage_min.is_some() && given.storage_min != stored.storage_min)
        || (given.storage_max.is_some() && given.storage_max != stored.storage_max)
        || (given.volume_attributes_class.is_some()
            && given.volume_attributes_class != stored.volume_attributes_class);
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
pub fn parse_deployment_config_patch(patch: &Value) -> Result<DynamicConfig, DBError> {
    serde_json::from_value(patch.clone()).map_err(|e| DBError::InvalidDeploymentPatch {
        reason: e.to_string(),
    })
}

/// Applies `patch` to a stored deployment config.
pub fn patch_deployment_config(
    deployment_config: &Value,
    patch: &DynamicConfig,
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
    // As at startup, the DataFusion pool must leave memory for the circuit.
    if let (Some(budget), Some(datafusion)) = (
        patched.effective_memory(),
        patched.resolved_datafusion_memory(),
    ) && datafusion >= budget
    {
        return Err(DBError::InvalidDeploymentPatch {
            reason: format!(
                "the memory budget ({budget}) must exceed datafusion_memory ({datafusion})"
            ),
        });
    }
    merge_deployment_config(&mut stored, &patched);
    serde_json::to_value(stored).map_err(|e| invalid(e.to_string()))
}

/// The resources after `patch`. The runner checks what Kubernetes allows.
fn apply_patch(
    hosts: usize,
    current: ResourceConfig,
    resize: &DynamicResources,
) -> Result<ResourceConfig, String> {
    if resize == &DynamicResources::default() {
        return Err(
            "specify at least one of resources.cpu_cores_min, resources.cpu_cores_max, \
             resources.memory_min, resources.memory_max, resources.storage_min, \
             resources.storage_max, resources.volume_attributes_class"
                .to_string(),
        );
    }
    if hosts > 1 {
        return Err("only single-host pipelines can be resized".to_string());
    }
    check_cpu("cpu_cores_min", resize.cpu_cores_min)?;
    check_cpu("cpu_cores_max", resize.cpu_cores_max)?;
    check_size("memory_min", resize.memory_min)?;
    check_size("memory_max", resize.memory_max)?;
    check_size("storage_min", resize.storage_min)?;
    check_size("storage_max", resize.storage_max)?;
    if resize.volume_attributes_class.as_deref() == Some("") {
        return Err("volume_attributes_class must not be empty".to_string());
    }
    let mut new = current.clone();
    new.cpu_cores_min = resize.cpu_cores_min.or(current.cpu_cores_min);
    new.cpu_cores_max = resize.cpu_cores_max.or(current.cpu_cores_max);
    new.memory_min = resize.memory_min.or(current.memory_min);
    new.memory_max = resize.memory_max.or(current.memory_max);
    new.storage_min = resize.storage_min.or(current.storage_min);
    new.storage_max = resize.storage_max.or(current.storage_max);
    new.volume_attributes_class = resize
        .volume_attributes_class
        .clone()
        .or(current.volume_attributes_class.clone());
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
    // A patch cannot unset a field, so this also keeps a fixed size on the field it is set by.
    if storage_autoscaling(&new) != storage_autoscaling(&current) {
        return Err(
            "storage autoscaling cannot be turned on or off while the pipeline runs: set \
             storage_min and storage_max in the runtime configuration"
                .to_string(),
        );
    }
    if let (Some(min), Some(max)) = (new.storage_min, new.storage_max)
        && min > max
    {
        return Err(format!("storage_min ({min}) exceeds storage_max ({max})"));
    }
    if let (Some(old), Some(new)) = (storage_size(&current), storage_size(&new))
        && new < old
    {
        return Err(format!(
            "storage cannot shrink from {old} to {new} (the size is storage_min, else storage_max)"
        ));
    }

    Ok(new)
}

/// Whether storage autoscales: both `storage_min` and `storage_max` are set.
fn storage_autoscaling(resources: &ResourceConfig) -> bool {
    resources.storage_min.is_some() && resources.storage_max.is_some()
}

/// The size storage starts at and grows to: `storage_min`, else `storage_max`.
fn storage_size(resources: &ResourceConfig) -> Option<ConfigSize> {
    resources.storage_min.or(resources.storage_max)
}

/// Copies CPU and memory from `from` into `into`.
fn merge_cpu_memory(into: &mut ResourceConfig, from: &ResourceConfig) {
    into.cpu_cores_min = from.cpu_cores_min;
    into.cpu_cores_max = from.cpu_cores_max;
    into.memory_min = from.memory_min;
    into.memory_max = from.memory_max;
}

/// Copies storage from `from` into `into`.
fn merge_storage(into: &mut ResourceConfig, from: &ResourceConfig) {
    into.storage_min = from.storage_min;
    into.storage_max = from.storage_max;
}

/// Copies the volume attributes class from `from` into `into`.
fn merge_volume_attributes_class(into: &mut ResourceConfig, from: &ResourceConfig) {
    into.volume_attributes_class = from.volume_attributes_class.clone();
}

/// Copies CPU, memory, storage, and the volume attributes class from `from` into `into`.
pub fn merge_deployment_config(into: &mut PipelineConfig, from: &RuntimeConfig) {
    merge_cpu_memory(&mut into.global.resources, &from.resources);
    merge_storage(&mut into.global.resources, &from.resources);
    merge_volume_attributes_class(&mut into.global.resources, &from.resources);
}

/// The stored deployment config with CPU and memory, or the volume attributes class, from an
/// edited runtime config, or `None` if neither changed. Each is copied only when edited, so a CPU
/// edit keeps a class change. Storage is not copied: it can only be edited with storage cleared,
/// and a start with storage cleared takes it from the runtime config.
pub fn merge_deployment_config_on_edit(
    deployment_config: &Value,
    old_runtime_config: &Value,
    new_runtime_config: &Value,
) -> Option<Value> {
    let old: RuntimeConfig = serde_json::from_value(old_runtime_config.clone()).ok()?;
    let new: RuntimeConfig = serde_json::from_value(new_runtime_config.clone()).ok()?;
    let cpu_memory = |config: &RuntimeConfig| {
        (
            config.resources.cpu_cores_min,
            config.resources.cpu_cores_max,
            config.resources.memory_min,
            config.resources.memory_max,
        )
    };
    let cpu_memory_edited = cpu_memory(&old) != cpu_memory(&new);
    let class_edited =
        old.resources.volume_attributes_class != new.resources.volume_attributes_class;
    if !cpu_memory_edited && !class_edited {
        return None;
    }
    let mut stored: PipelineConfig = serde_json::from_value(deployment_config.clone()).ok()?;
    if cpu_memory_edited {
        merge_cpu_memory(&mut stored.global.resources, &new.resources);
    }
    if class_edited {
        merge_volume_attributes_class(&mut stored.global.resources, &new.resources);
    }
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

/// Checks a memory or storage value from a patch.
fn check_size(name: &str, value: Option<ConfigSize>) -> Result<(), String> {
    match value {
        Some(size) if size.is_zero() => Err(format!("{name} must be greater than 0")),
        _ => Ok(()),
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use utoipa::ToSchema;

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

    /// A resize that leaves no memory for the circuit is refused, as at
    /// startup. The default DataFusion pool (5%) always fits.
    #[test]
    fn resize_must_leave_memory_after_datafusion() {
        let mut config = deployment();
        config["datafusion_memory"] = json!("2000M");
        let memory = |mb: u64| {
            let size = ConfigSize::from_mb(mb);
            resize(json!({"memory_min": size, "memory_max": size}))
        };
        let error = try_patch(&config, &memory(2_000)).unwrap_err();
        assert!(error.contains("must exceed datafusion_memory"), "{error}");
        try_patch(&config, &memory(2_001)).unwrap();
        try_patch(&deployment(), &memory(1)).unwrap();
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
        assert_refused(json!({"storage_max": "0M"}), "greater than 0");
        assert_refused(json!({"storage_min": "2G"}), "cannot be turned on or off");
        assert_refused(json!({"volume_attributes_class": ""}), "must not be empty");
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
                json!({"resources": {"namespace": null}}),
                "unknown field `namespace`",
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

    /// Storage grows to `storage_min`, else `storage_max`, and never shrinks. The maximum alone
    /// is the autoscaling ceiling once the minimum is set.
    #[test]
    fn storage_grows_but_never_shrinks() {
        let storage = |config: &Value| {
            let resources = &config["resources"];
            (
                resources["storage_min"].as_str().map(str::to_string),
                resources["storage_max"].as_str().map(str::to_string),
            )
        };
        let size = |s: &str| Some(s.to_string());
        // Fixed size from `storage_max`.
        let fixed = deployment();
        let mut autoscaling = deployment();
        autoscaling["resources"]["storage_min"] = json!("1G");
        autoscaling["resources"]["storage_max"] = json!("8G");
        for (config, patch, expected) in [
            (&fixed, json!({"storage_max": "2G"}), (None, size("2G"))),
            (&fixed, json!({"storage_max": "1000M"}), (None, size("1G"))),
            // The deprecated spelling means the same.
            (&fixed, json!({"storage_mb_max": 2000}), (None, size("2G"))),
            (
                &autoscaling,
                json!({"storage_min": "4G"}),
                (size("4G"), size("8G")),
            ),
            // A lower ceiling leaves the size alone.
            (
                &autoscaling,
                json!({"storage_max": "2G"}),
                (size("1G"), size("2G")),
            ),
        ] {
            let new = try_patch(config, &resize(patch.clone())).unwrap();
            assert_eq!(storage(&new), expected, "{patch}");
        }
        for (config, patch, expected) in [
            (
                &fixed,
                json!({"storage_max": "999M"}),
                "storage cannot shrink",
            ),
            (
                &autoscaling,
                json!({"storage_min": "500M"}),
                "storage cannot shrink",
            ),
            (&autoscaling, json!({"storage_min": "9G"}), "exceeds"),
        ] {
            let error = try_patch(config, &resize(patch.clone())).unwrap_err();
            assert!(error.contains(expected), "{patch}: {error}");
        }
        // Without a size, the runner's default applies and any size is accepted.
        let mut default = deployment();
        default["resources"]["storage_max"] = json!(null);
        let new = try_patch(&default, &resize(json!({"storage_max": "10M"}))).unwrap();
        assert_eq!(storage(&new), (None, size("10M")));
    }

    /// A patch compares with a size stored in the deprecated spelling or another unit, and both
    /// spellings of one setting are refused.
    #[test]
    fn storage_patch_reads_the_deprecated_spelling() {
        let mut legacy = deployment();
        let resources = legacy["resources"].as_object_mut().unwrap();
        resources.remove("storage_max");
        resources.insert("storage_mb_max".to_string(), json!(1000));
        let error = try_patch(&legacy, &resize(json!({"storage_max": "500M"}))).unwrap_err();
        assert!(error.contains("cannot shrink"), "{error}");
        let new = try_patch(&legacy, &resize(json!({"storage_max": "2G"}))).unwrap();
        assert_eq!(new["resources"]["storage_max"], json!("2G"));
        assert_eq!(new["resources"].get("storage_mb_max"), None);
        let both = resize(json!({"storage_min": "2G", "storage_mb_min": 1000}));
        assert!(parse_deployment_config_patch(&both).is_err());
        // Sizes in different units compare by bytes: 1G is less than 1Gi.
        let mut gi = deployment();
        gi["resources"]["storage_max"] = json!("1Gi");
        let error = try_patch(&gi, &resize(json!({"storage_max": "1G"}))).unwrap_err();
        assert!(error.contains("cannot shrink"), "{error}");
        try_patch(&gi, &resize(json!({"storage_max": "1024Mi"}))).unwrap();
    }

    /// A patch keeps storage fixed, on the field it is set by, or autoscaling. Without a size,
    /// a patch can set a fixed one.
    #[test]
    fn storage_autoscaling_cannot_be_turned_on_or_off() {
        let with_storage = |min: Option<&str>, max: Option<&str>| {
            let mut config = deployment();
            config["resources"]["storage_min"] = json!(min);
            config["resources"]["storage_max"] = json!(max);
            config
        };
        let fixed_max = with_storage(None, Some("1G"));
        let fixed_min = with_storage(Some("1G"), None);
        let autoscaling = with_storage(Some("1G"), Some("8G"));
        let default = with_storage(None, None);
        for (config, patch) in [
            (&fixed_max, json!({"storage_min": "1G"})),
            (
                &fixed_max,
                json!({"storage_min": "1G", "storage_max": "8G"}),
            ),
            (&fixed_min, json!({"storage_max": "8G"})),
            (&default, json!({"storage_min": "1G", "storage_max": "8G"})),
        ] {
            let error = try_patch(config, &resize(patch.clone())).unwrap_err();
            assert!(
                error.contains("cannot be turned on or off"),
                "{config} {patch}: {error}"
            );
        }
        for (config, patch, expected) in [
            (&default, json!({"storage_min": "1G"}), (Some("1G"), None)),
            (&default, json!({"storage_max": "1G"}), (None, Some("1G"))),
            (&fixed_min, json!({"storage_min": "2G"}), (Some("2G"), None)),
            (
                &autoscaling,
                json!({"storage_min": "2G", "storage_max": "9G"}),
                (Some("2G"), Some("9G")),
            ),
            // `null` leaves a field unchanged, so it cannot turn autoscaling off.
            (
                &autoscaling,
                json!({"storage_min": null, "storage_max": "9G"}),
                (Some("1G"), Some("9G")),
            ),
        ] {
            let new = try_patch(config, &resize(patch.clone())).unwrap();
            let resources = &new["resources"];
            assert_eq!(
                (
                    resources["storage_min"].as_str(),
                    resources["storage_max"].as_str()
                ),
                expected,
                "{config} {patch}"
            );
        }
    }

    /// A patch changes the volume attributes class, and `null` leaves it unchanged.
    #[test]
    fn volume_attributes_class_changes_but_is_not_removed() {
        let class = |config: &Value| config["resources"]["volume_attributes_class"].clone();
        let set = try_patch(
            &deployment(),
            &resize(json!({"volume_attributes_class": "fast"})),
        )
        .unwrap();
        assert_eq!(class(&set), json!("fast"));
        for (patch, expected) in [
            (json!({"volume_attributes_class": "slow"}), json!("slow")),
            (
                json!({"volume_attributes_class": null, "cpu_cores_min": 1.0}),
                json!("fast"),
            ),
            (json!({"cpu_cores_min": 1.0}), json!("fast")),
        ] {
            let new = try_patch(&set, &resize(patch.clone())).unwrap();
            assert_eq!(class(&new), expected, "{patch}");
        }
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
                "storage_min": null, "storage_max": "1G",
                "volume_attributes_class": null,
            }})
        );
        // A config stored before the size rename shows its memory and storage as sizes.
        assert_eq!(
            deployment_config_view(
                &json!({"resources": {"memory_mb_max": 16000, "storage_mb_max": 2000}})
            ),
            json!({"resources": {
                "cpu_cores_min": null, "cpu_cores_max": null,
                "memory_min": null, "memory_max": "16G",
                "storage_min": null, "storage_max": "2G",
                "volume_attributes_class": null,
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
                "storage_min": null, "storage_max": null,
                "volume_attributes_class": null,
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
            json!({"resources": {"storage_min": "1G"}}),
            json!({"resources": {"storage_max": "2G"}}),
            json!({"resources": {"volume_attributes_class": "fast"}}),
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
        from.resources.storage_class = Some("fast".to_string());
        from.workers = 9;
        merge_deployment_config(&mut into, &from);
        assert_eq!(into.global.resources.cpu_cores_min, Some(1.0));
        assert_eq!(
            into.global.resources.storage_max,
            Some(ConfigSize::from_mb(5000))
        );
        assert_eq!(into.global.resources.storage_class, None);
        assert_eq!(into.global.workers, 4);
    }

    /// Each runtime modifiable field is copied by the merge, and detected as edited unless it is
    /// storage, so the three stay in step when one is added.
    #[test]
    fn every_live_setting_is_merged_and_detected() {
        let base = deployment();
        // From the schema: serializing skips unset fields.
        let schema = serde_json::to_value(DynamicResources::schema().1).unwrap();
        for field in schema["properties"].as_object().unwrap().keys() {
            let path = format!("resources.{field}");
            let pointer = format!("/resources/{field}");
            // A memory or storage field holds a size, written back as it was given.
            let value = if field.starts_with("memory") || field.starts_with("storage") {
                json!("12345M")
            } else if field == "volume_attributes_class" {
                json!("class-12345")
            } else {
                json!(12345.0)
            };
            let mut changed = base.clone();
            changed["resources"][field] = value.clone();
            assert_eq!(
                merge_deployment_config_on_edit(&base, &base, &changed).is_some(),
                !field.starts_with("storage"),
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
    fn only_a_cpu_memory_or_class_edit_updates_the_deployment_config() {
        let runtime = json!({"workers": 4, "resources": {"cpu_cores_min": 4, "cpu_cores_max": 8}});
        let mut resized = deployment();
        resized["resources"]["cpu_cores_min"] = json!(1.0);
        // The same values written differently, storage, and other fields are not such an edit.
        let mut same = json!({"workers": 8, "resources": {
            "cpu_cores_min": 4.0, "cpu_cores_max": 8.0, "storage_max": "2G"
        }});
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

    /// An edit of CPU and memory, or the volume attributes class, keeps a change of the other and
    /// a storage resize.
    #[test]
    fn cpu_memory_and_class_edits_are_merged_separately() {
        let runtime = json!({"resources": {"cpu_cores_min": 4, "storage_max": "1G"}});
        let mut resized = deployment();
        resized["resources"]["cpu_cores_min"] = json!(1.0);
        resized["resources"]["storage_max"] = json!("5G");
        resized["resources"]["volume_attributes_class"] = json!("fast");
        let live = |config: &Value| {
            let resources = &config["resources"];
            (
                resources["cpu_cores_min"].clone(),
                resources["storage_max"].clone(),
                resources["volume_attributes_class"].clone(),
            )
        };
        for (edit, expected) in [
            (
                json!({"resources": {"cpu_cores_min": 2, "storage_max": "1G"}}),
                (json!(2.0), json!("5G"), json!("fast")),
            ),
            (
                json!({"resources": {
                    "cpu_cores_min": 4, "storage_max": "1G", "volume_attributes_class": "slow"
                }}),
                (json!(1.0), json!("5G"), json!("slow")),
            ),
        ] {
            let edited = merge_deployment_config_on_edit(&resized, &runtime, &edit).unwrap();
            assert_eq!(live(&edited), expected, "{edit}");
        }
    }
}
