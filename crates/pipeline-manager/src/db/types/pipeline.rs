use crate::db::error::DBError;
use crate::db::types::program::{ProgramError, ProgramStatus};
use crate::db::types::resources_status::{ResourcesDesiredStatus, ResourcesStatus};
use crate::db::types::storage::StorageStatus;
use crate::db::types::utils::{validate_description, validate_tags};
use crate::db::types::version::Version;
use chrono::{DateTime, Utc};
use feldera_types::error::ErrorResponse;
use feldera_types::runtime_status::{
    BootstrapConfig, BootstrapPolicy, RuntimeDesiredStatus, RuntimeStatus,
};
use serde::{Deserialize, Serialize};
use std::fmt;
use std::fmt::Display;
use tracing::warn;
use utoipa::ToSchema;
use uuid::Uuid;

/// Pipeline identifier.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Ord, PartialOrd, Serialize, Deserialize, ToSchema)]
#[cfg_attr(test, derive(proptest_derive::Arbitrary))]
#[repr(transparent)]
#[serde(transparent)]
pub struct PipelineId(
    #[cfg_attr(test, proptest(strategy = "crate::db::test::limited_uuid()"))] pub Uuid,
);
impl Display for PipelineId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

/// Converts the `RuntimeStatus` to its string representation.
/// This is used by the database operations.
pub fn runtime_status_to_string(runtime_status: RuntimeStatus) -> String {
    match runtime_status {
        RuntimeStatus::Unavailable => "unavailable",
        RuntimeStatus::Coordination => "coordination",
        RuntimeStatus::Standby => "standby",
        RuntimeStatus::AwaitingApproval => "awaiting_approval",
        RuntimeStatus::Initializing => "initializing",
        RuntimeStatus::Bootstrapping => "bootstrapping",
        RuntimeStatus::ConcurrentBootstrapping => "concurrent_bootstrapping",
        RuntimeStatus::Synchronizing => "synchronizing",
        RuntimeStatus::Replaying => "replaying",
        RuntimeStatus::Paused => "paused",
        RuntimeStatus::Running => "running",
        RuntimeStatus::Suspended => "suspended",
    }
    .to_string()
}

/// Parses the string as a `RuntimeStatus`.
/// This is used by the database operations.
/// Upon failure returns a database error.
pub fn parse_string_as_runtime_status(s: String) -> Result<RuntimeStatus, DBError> {
    match s.as_str() {
        "unavailable" => Ok(RuntimeStatus::Unavailable),
        "coordination" => Ok(RuntimeStatus::Coordination),
        "standby" => Ok(RuntimeStatus::Standby),
        "awaiting_approval" => Ok(RuntimeStatus::AwaitingApproval),
        "initializing" => Ok(RuntimeStatus::Initializing),
        "bootstrapping" => Ok(RuntimeStatus::Bootstrapping),
        "concurrent_bootstrapping" => Ok(RuntimeStatus::ConcurrentBootstrapping),
        "synchronizing" => Ok(RuntimeStatus::Synchronizing),
        "replaying" => Ok(RuntimeStatus::Replaying),
        "paused" => Ok(RuntimeStatus::Paused),
        "running" => Ok(RuntimeStatus::Running),
        "suspended" => Ok(RuntimeStatus::Suspended),
        _ => Err(DBError::InvalidRuntimeStatus { value: s }),
    }
}

/// Converts the `RuntimeDesiredStatus` to its string representation.
/// This is used by the database operations.
pub fn runtime_desired_status_to_string(runtime_desired_status: RuntimeDesiredStatus) -> String {
    match runtime_desired_status {
        RuntimeDesiredStatus::Unavailable => "unavailable",
        RuntimeDesiredStatus::Coordination => "coordination",
        RuntimeDesiredStatus::Standby => "standby",
        RuntimeDesiredStatus::Paused => "paused",
        RuntimeDesiredStatus::Running => "running",
        RuntimeDesiredStatus::Suspended => "suspended",
    }
    .to_string()
}

/// Parses the string as a `RuntimeDesiredStatus`.
/// This is used by the database operations. Upon failure returns a database error.
pub fn parse_string_as_runtime_desired_status(s: String) -> Result<RuntimeDesiredStatus, DBError> {
    match s.as_str() {
        "unavailable" => Ok(RuntimeDesiredStatus::Unavailable),
        "standby" => Ok(RuntimeDesiredStatus::Standby),
        "paused" => Ok(RuntimeDesiredStatus::Paused),
        "running" => Ok(RuntimeDesiredStatus::Running),
        "suspended" => Ok(RuntimeDesiredStatus::Suspended),
        _ => Err(DBError::InvalidRuntimeDesiredStatus { value: s }),
    }
}

pub fn bootstrap_policy_to_string(bootstrap: BootstrapPolicy) -> String {
    match bootstrap {
        BootstrapPolicy::Allow => "allow",
        BootstrapPolicy::Reject => "reject",
        BootstrapPolicy::AwaitApproval => "await_approval",
    }
    .to_string()
}

/// Serialize BootstrapConfig as a JSON string.
pub fn bootstrap_config_to_string(bootstrap: BootstrapConfig) -> String {
    serde_json::to_string(&bootstrap).unwrap()
}

/// Backward compatible deserialization of the bootstrap_policy field.
///
/// Old format: bootstrap policy only, silent_bootstrap is implied to be false.
/// New format: BootstrapConfig serialized as a JSON string.
pub fn parse_string_as_bootstrap_config(s: String) -> Result<BootstrapConfig, DBError> {
    match s.as_str() {
        "allow" => Ok(BootstrapConfig::from(BootstrapPolicy::Allow)),
        "reject" => Ok(BootstrapConfig::from(BootstrapPolicy::Reject)),
        "await_approval" => Ok(BootstrapConfig::from(BootstrapPolicy::AwaitApproval)),
        _ => serde_json::from_str::<BootstrapConfig>(&s)
            .map_err(|_| DBError::InvalidBootstrap { value: s.clone() }),
    }
}

/// Client-generated data stored alongside a pipeline.
///
/// The fields are stored together as a single JSON object in the
/// `client_metadata` text column, so adding a field needs no migration.
/// Deserialization is lenient: missing keys take their default and unknown
/// keys are ignored. [`PatchClientMetadata`] is the optional, per-field form
/// used by `PATCH` request bodies.
#[derive(Default, Eq, PartialEq, Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct ClientMetadata {
    /// Human-readable description of the pipeline. Default is an empty string.
    #[serde(default)]
    pub description: String,

    /// Free-form labels used to organize, group, and filter pipelines.
    /// Default is no tags (empty vector).
    #[serde(default)]
    pub tags: Vec<String>,
}

impl ClientMetadata {
    /// Parses the value read from the `client_metadata` column. The empty
    /// string (the column default) parses to empty metadata. Invalid JSON is
    /// also treated as empty rather than returning an error: a single
    /// corrupted row must not block reads, and the only data lost is
    /// client-generated.
    pub fn from_db_string(s: &str) -> Self {
        if s.is_empty() {
            return Self::default();
        }
        serde_json::from_str(s).unwrap_or_else(|e| {
            // Leave a breadcrumb so corruption is visible in the logs rather
            // than silently dropping client data. The snippet is truncated to
            // keep a large row from flooding the log.
            warn!(
                "ignoring corrupted client_metadata, reading as empty (parse error: {e}); \
                 column starts with: {:.100}",
                s
            );
            Self::default()
        })
    }

    /// Serializes to the value written to the `client_metadata` column. Empty
    /// metadata is stored as `""` (the column default) rather than `"{}"`, so
    /// a freshly written empty row matches a row defaulted by the migration.
    pub fn to_db_string(&self) -> String {
        if self.is_empty() {
            return String::new();
        }
        // The fields are owned strings, so serialization cannot fail; the
        // fallback to `""` exists only so a hypothetical serde bug can never
        // panic a request handler.
        serde_json::to_string(self).unwrap_or_default()
    }

    /// True when no field carries a value. Destructured so adding a field
    /// forces this check to account for it: otherwise a row holding only the
    /// new field would be stored as the empty string (see `to_db_string`) and
    /// the value would be lost.
    pub fn is_empty(&self) -> bool {
        let ClientMetadata { description, tags } = self;
        description.is_empty() && tags.is_empty()
    }

    /// Expresses this complete metadata as a patch that sets every field. A
    /// `POST`/`PUT` replace is then just a patch in which nothing is `None`, so
    /// replace and `PATCH` share the single merge path in `apply_patch`. The
    /// exhaustive struct literal makes a newly added field impossible to omit
    /// from a replace.
    pub fn as_full_patch(&self) -> PatchClientMetadata {
        PatchClientMetadata {
            description: Some(self.description.clone()),
            tags: Some(self.tags.clone()),
        }
    }

    /// Merges `patch` into `self`: each `Some` field of `patch` overwrites the
    /// corresponding field here; each `None` field leaves it unchanged. The
    /// provided value is stored verbatim, so an empty string or empty list is
    /// a value in its own right, not a request to unset the field.
    pub fn apply_patch(&mut self, patch: &PatchClientMetadata) {
        // Destructured so adding a field forces this check to account for it.
        let PatchClientMetadata { description, tags } = patch;
        if let Some(description) = description {
            self.description = description.clone();
        }
        if let Some(tags) = tags {
            self.tags = tags.clone();
        }
    }

    /// Validates every field. Used when creating a pipeline, where all
    /// client-provided values are new.
    pub fn validate(&self) -> Result<(), DBError> {
        let ClientMetadata { description, tags } = self;
        validate_description(description)?;
        validate_tags(tags)?;
        Ok(())
    }

    /// Validates only the fields that differ from `previous`. Used when
    /// updating a pipeline, so that a value stored before a constraint existed
    /// (e.g. a description longer than today's limit, migrated from the old
    /// `description` column) is left untouched until the client actually
    /// changes it. Re-submitting an unchanged value is therefore always
    /// allowed; changing it is validated against the current rules.
    pub fn validate_changes(&self, previous: &ClientMetadata) -> Result<(), DBError> {
        let ClientMetadata { description, tags } = self;
        if *description != previous.description {
            validate_description(description)?;
        }
        if *tags != previous.tags {
            validate_tags(tags)?;
        }
        Ok(())
    }
}

/// Client-generated metadata as supplied in a `PATCH` request body: the
/// field-by-field patch form of [`ClientMetadata`].
///
/// Each field is optional. A `Some` value overwrites the stored field; a
/// `None` (absent) field leaves it unchanged. An empty string or empty list is
/// a value in its own right, not a request to unset the field.
#[derive(Default, Eq, PartialEq, Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct PatchClientMetadata {
    /// Human-readable description of the pipeline.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,

    /// Free-form labels used to organize, group, and filter pipelines.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tags: Option<Vec<String>>,
}

impl PatchClientMetadata {
    /// True when every field is `None`, i.e. the patch sets nothing and so
    /// cannot change anything.
    pub fn contains_only_nones(&self) -> bool {
        // Destructured so adding a field forces this check to account for it.
        let PatchClientMetadata { description, tags } = self;
        description.is_none() && tags.is_none()
    }
}

/// Pipeline descriptor.
#[derive(Eq, PartialEq, Debug, Clone)]
pub struct PipelineDescr {
    /// Pipeline name.
    pub name: String,

    /// Human-readable description of the pipeline.
    pub description: String,

    /// Free-form labels used to organize, group, and filter pipelines.
    pub tags: Vec<String>,

    /// Pipeline runtime configuration.
    pub runtime_config: serde_json::Value,

    /// Program SQL code.
    pub program_code: String,

    /// Rust code for UDFs.
    pub udf_rust: String,

    /// Rust dependencies in the TOML format.
    pub udf_toml: String,

    /// Program compilation configuration.
    pub program_config: serde_json::Value,
}

impl PipelineDescr {
    /// Bundles the client-generated fields into the [`ClientMetadata`] form
    /// used for storage. The exhaustive struct literal is deliberate: adding a
    /// field to [`ClientMetadata`] fails to compile here until it is included,
    /// so a new client-metadata field can never be silently dropped on write.
    pub fn client_metadata(&self) -> ClientMetadata {
        ClientMetadata {
            description: self.description.clone(),
            tags: self.tags.clone(),
        }
    }

    #[cfg(test)]
    pub(crate) fn test_descr() -> Self {
        use serde_json::json;
        Self {
            name: "test_pipeline".to_string(),
            description: "Test pipeline".to_string(),
            tags: vec![],
            runtime_config: json!({}),
            program_code: "CREATE TABLE test (col1 INT);".to_string(),
            udf_rust: "".to_string(),
            udf_toml: "".to_string(),
            program_config: json!({
                "profile": "unoptimized",
                "cache": false
            }),
        }
    }
}

/// Pipeline descriptor which besides the basic fields in direct regular control of the user
/// also has all additional fields generated and maintained by the back-end.
#[derive(Eq, PartialEq, Debug, Clone, Serialize, Deserialize)]
pub struct ExtendedPipelineDescr {
    /// Assigned globally unique pipeline identifier.
    pub id: PipelineId,

    /// Pipeline name.
    pub name: String,

    /// Human-readable description of the pipeline.
    pub description: String,

    /// Free-form labels used to organize, group, and filter pipelines.
    pub tags: Vec<String>,

    /// Timestamp when the pipeline was originally created.
    pub created_at: DateTime<Utc>,

    /// Pipeline version, incremented every time name, runtime_config, program_code,
    /// udf_rust, udf_toml, program_config or platform_version is/are modified.
    /// Changes to client metadata (description, tags, ...) do not bump `version`.
    pub version: Version,

    /// Pipeline platform version.
    pub platform_version: String,

    /// Pipeline runtime configuration.
    pub runtime_config: serde_json::Value,

    /// Program SQL code.
    pub program_code: String,

    /// Rust code for UDFs.
    pub udf_rust: String,

    /// Rust dependencies in the TOML format.
    pub udf_toml: String,

    /// Program compilation configuration.
    pub program_config: serde_json::Value,

    /// Program version, incremented every time program_code, udf_rust,
    /// udf_toml, program_config or platform_version is/are modified.
    pub program_version: Version,

    /// Program compilation status.
    pub program_status: ProgramStatus,

    /// Timestamp when the current program status was set.
    pub program_status_since: DateTime<Utc>,

    /// Log, warning and error information about the program compilation.
    pub program_error: ProgramError,

    /// Program information which includes schema, input connectors and output connectors.
    /// It is set once SQL compilation has been successfully completed
    /// (i.e., the `program_status` field reaches >= `ProgramStatus::SqlCompiled`).
    pub program_info: Option<serde_json::Value>,

    /// Combined checksum of all the inputs that influenced Rust compilation to a binary.
    pub program_binary_source_checksum: Option<String>,

    /// Checksum of the binary file itself.
    pub program_binary_integrity_checksum: Option<String>,

    /// Checksum of the program information.
    pub program_info_integrity_checksum: Option<String>,

    /// Resource or runtime error that caused the pipeline to stop unexpectedly.
    ///
    /// Can only be set when `Stopping` or `Stopped`.
    pub deployment_error: Option<ErrorResponse>,

    // Pipeline deployment configuration.
    pub deployment_config: Option<serde_json::Value>,

    /// Location where the pipeline can be reached at runtime
    /// (e.g., a TCP port number or a URI).
    pub deployment_location: Option<String>,

    /// Refresh version, incremented for the same fields as `version` but also including
    /// `program_info` and `program_error` as it contains information of interest to the user
    /// regarding the pipeline. It is a notification mechanism for users. If a user detects
    /// it changed while monitoring only the status fields, it should refresh fully (retrieve
    /// all fields).
    pub refresh_version: Version,

    /// Storage status.
    pub storage_status: StorageStatus,

    /// Storage status details.
    pub storage_status_details: Option<serde_json::Value>,

    /// Identifier of the current deployment.
    pub deployment_id: Option<Uuid>,

    /// Initial runtime desired status of the current deployment.
    pub deployment_initial: Option<RuntimeDesiredStatus>,

    /// Configuration used to bootstrap the pipeline if changes are detected.
    // TODO: This field is called `bootstrap_policy` to match the corresponding column
    // in the database. We should rename the column and the field, but I don't want to
    // introduce a DB migration just for this. Perhaps we can bundle this change with
    // some other migration.
    pub bootstrap_policy: Option<BootstrapConfig>,

    /// Resources status of the current deployment.
    pub deployment_resources_status: ResourcesStatus,

    /// Timestamp when the `deployment_resources_status` last changed.
    pub deployment_resources_status_since: DateTime<Utc>,

    /// Details about the resources status.
    /// No assumptions should be made about the structure of this JSON value.
    pub deployment_resources_status_details: Option<serde_json::Value>,

    /// Resources desired status of the current deployment.
    pub deployment_resources_desired_status: ResourcesDesiredStatus,

    /// Timestamp when the `deployment_resources_desired_status` last changed.
    pub deployment_resources_desired_status_since: DateTime<Utc>,

    /// Observed runtime status of the current deployment.
    pub deployment_runtime_status: Option<RuntimeStatus>,

    /// Details about the runtime status.
    /// No assumptions should be made about the structure of this JSON value.
    pub deployment_runtime_status_details: Option<serde_json::Value>,

    /// Timestamp when the `deployment_runtime_status` observation last changed.
    pub deployment_runtime_status_since: Option<DateTime<Utc>>,

    /// Observed runtime desired status of the current deployment.
    pub deployment_runtime_desired_status: Option<RuntimeDesiredStatus>,

    /// Timestamp when the `deployment_runtime_desired_status` last changed.
    pub deployment_runtime_desired_status_since: Option<DateTime<Utc>>,
}

impl ExtendedPipelineDescr {
    /// Bundles the client-generated fields into the [`ClientMetadata`] form.
    /// The exhaustive struct literal is deliberate: adding a field to
    /// [`ClientMetadata`] fails to compile here until it is included, so the
    /// "did client metadata change?" check (see `update_pipeline`) can never
    /// silently omit a new field and wrongly leave the version unchanged.
    pub fn client_metadata(&self) -> ClientMetadata {
        ClientMetadata {
            description: self.description.clone(),
            tags: self.tags.clone(),
        }
    }
}

/// Pipeline descriptor which includes the fields relevant to system monitoring.
/// The advantage of this descriptor over the [`ExtendedPipelineDescr`] is that it
/// excludes fields which can be quite large (e.g., the generated Rust code stored
/// in `program_info` can become several MiB in size). This is particularly relevant
/// for monitoring in which the pipeline tuple is retrieved very frequently, which would
/// result in high CPU usage to retrieve large fields that are not of interest.
#[derive(Eq, PartialEq, Debug, Clone, Serialize)]
pub struct ExtendedPipelineDescrMonitoring {
    pub id: PipelineId,
    pub name: String,
    pub description: String,
    pub tags: Vec<String>,
    pub created_at: DateTime<Utc>,
    pub version: Version,
    pub platform_version: String,
    pub runtime_config: serde_json::Value,
    pub program_config: serde_json::Value,
    pub program_version: Version,
    pub program_status: ProgramStatus,
    pub program_status_since: DateTime<Utc>,
    pub deployment_error: Option<ErrorResponse>,
    pub deployment_location: Option<String>,
    pub refresh_version: Version,
    pub storage_status: StorageStatus,
    pub storage_status_details: Option<serde_json::Value>,
    pub deployment_id: Option<Uuid>,
    pub deployment_initial: Option<RuntimeDesiredStatus>,
    pub deployment_resources_status: ResourcesStatus,
    pub deployment_resources_status_since: DateTime<Utc>,
    pub deployment_resources_desired_status: ResourcesDesiredStatus,
    pub deployment_resources_desired_status_since: DateTime<Utc>,
    pub deployment_runtime_status: Option<RuntimeStatus>,
    pub deployment_runtime_status_details: Option<serde_json::Value>,
    pub deployment_runtime_status_since: Option<DateTime<Utc>>,
    pub deployment_runtime_desired_status: Option<RuntimeDesiredStatus>,
    pub bootstrap_policy: Option<BootstrapConfig>,
    pub deployment_runtime_desired_status_since: Option<DateTime<Utc>>,
}

impl ExtendedPipelineDescrMonitoring {
    /// Bundles the client-generated fields into the [`ClientMetadata`] form.
    /// The exhaustive struct literal is deliberate: adding a field to
    /// [`ClientMetadata`] fails to compile here until it is included.
    pub fn client_metadata(&self) -> ClientMetadata {
        ClientMetadata {
            description: self.description.clone(),
            tags: self.tags.clone(),
        }
    }
}

/// Pipeline descriptor with all fields needed to create a monitor event.
#[derive(Eq, PartialEq, Debug, Clone, Serialize)]
pub struct ExtendedPipelineDescrEventInfo {
    pub id: PipelineId,
    pub program_status: ProgramStatus,
    pub storage_status: StorageStatus,
    pub storage_status_details: Option<serde_json::Value>,
    pub deployment_error: Option<ErrorResponse>,
    pub deployment_resources_status: ResourcesStatus,
    pub deployment_resources_status_details: Option<serde_json::Value>,
    pub deployment_resources_desired_status: ResourcesDesiredStatus,
    pub deployment_runtime_status: Option<RuntimeStatus>,
    pub deployment_runtime_status_details: Option<serde_json::Value>,
    pub deployment_runtime_desired_status: Option<RuntimeDesiredStatus>,
}

#[cfg(test)]
mod tests {
    use super::{ClientMetadata, PatchClientMetadata};

    /// Builds an always-present [`ClientMetadata`].
    fn meta(description: &str, tags: &[&str]) -> ClientMetadata {
        ClientMetadata {
            description: description.to_string(),
            tags: tags.iter().map(|t| t.to_string()).collect(),
        }
    }

    /// Builds a [`PatchClientMetadata`]: each field present or absent.
    fn patch(description: Option<&str>, tags: Option<Vec<&str>>) -> PatchClientMetadata {
        PatchClientMetadata {
            description: description.map(str::to_string),
            tags: tags.map(|t| t.into_iter().map(str::to_string).collect()),
        }
    }

    #[test]
    fn empty_metadata_stores_as_empty_string() {
        assert_eq!(ClientMetadata::default().to_db_string(), "");
        assert_eq!(
            ClientMetadata::from_db_string(""),
            ClientMetadata::default()
        );
    }

    #[test]
    fn db_string_round_trips() {
        let original = meta("a pipeline", &["prod", "billing"]);
        let restored = ClientMetadata::from_db_string(&original.to_db_string());
        assert_eq!(restored, original);
    }

    #[test]
    fn invalid_db_string_reads_as_empty() {
        assert_eq!(
            ClientMetadata::from_db_string("not json"),
            ClientMetadata::default()
        );
    }

    #[test]
    fn unknown_fields_and_missing_fields_default() {
        // Unknown keys are ignored; a missing `tags` key defaults to empty.
        let restored = ClientMetadata::from_db_string(r#"{"description":"d","future_field":42}"#);
        assert_eq!(restored, meta("d", &[]));
    }

    #[test]
    fn patch_overwrites_only_present_fields() {
        let mut cm = meta("old", &["keep"]);
        cm.apply_patch(&patch(Some("new"), None));
        assert_eq!(cm, meta("new", &["keep"]));
    }

    #[test]
    fn contains_only_nones_detects_the_empty_patch() {
        assert!(PatchClientMetadata::default().contains_only_nones());
        assert!(!patch(Some(""), None).contains_only_nones());
        assert!(!patch(None, Some(vec![])).contains_only_nones());
    }

    #[test]
    fn full_patch_replaces_every_field() {
        // A `POST`/`PUT` replace is expressed as a full patch. Applying it to
        // any prior value must yield exactly the replacement, including when
        // the replacement clears a previously set field.
        let replacement = meta("", &[]);
        let mut current = meta("old description", &["old"]);
        current.apply_patch(&replacement.as_full_patch());
        assert_eq!(current, replacement);

        // A full patch never has a `None` field, so it is never a no-op.
        assert!(!replacement.as_full_patch().contains_only_nones());
    }

    #[test]
    fn patch_stores_empty_values_verbatim() {
        // An empty string and an empty list are values in their own right, not
        // a request to unset the field.
        let mut cm = meta("old", &["a"]);
        cm.apply_patch(&patch(Some(""), Some(vec![])));
        assert_eq!(cm, meta("", &[]));
        assert!(cm.is_empty());
    }

    #[test]
    fn empty_patch_leaves_value_unchanged() {
        let mut cm = meta("keep", &["keep"]);
        cm.apply_patch(&PatchClientMetadata::default());
        assert_eq!(cm, meta("keep", &["keep"]));
    }

    #[test]
    fn validate_rejects_invalid_tags_but_allows_free_form_description() {
        // The description is free-form, so any characters (including those
        // disallowed in tags) are accepted within the length limit.
        assert!(meta("any text! 日本語 %$#", &[]).validate().is_ok());
        // Valid tags pass.
        assert!(meta("", &["prod", "env/staging"]).validate().is_ok());
        // An invalid tag is rejected.
        assert!(meta("d", &["bad,tag"]).validate().is_err());
        // An over-long description is rejected on create.
        assert!(meta(&"a".repeat(301), &[]).validate().is_err());
    }

    #[test]
    fn validate_changes_leaves_unchanged_overlong_description_alone() {
        // Simulate a description longer than today's limit, e.g. one migrated
        // from the old `description` column before the limit existed.
        let legacy = meta(&"x".repeat(500), &[]);

        // Re-submitting the same (unchanged) value is allowed.
        assert!(legacy.validate_changes(&legacy).is_ok());

        // Changing only the tags is allowed even though the stored description
        // exceeds the limit.
        let mut tags_only = legacy.clone();
        tags_only.tags = vec!["new-tag".to_string()];
        assert!(tags_only.validate_changes(&legacy).is_ok());

        // Attempting to change the description is validated: another over-long
        // value is rejected.
        let mut still_long = legacy.clone();
        still_long.description = "y".repeat(400);
        assert!(still_long.validate_changes(&legacy).is_err());

        // Shortening the description to within the limit is allowed.
        let mut shortened = legacy.clone();
        shortened.description = "now short".to_string();
        assert!(shortened.validate_changes(&legacy).is_ok());
    }
}
