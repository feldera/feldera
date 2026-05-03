use crate::config::CompilerConfig;
use crate::db::error::DBError;
use crate::db::types::pipeline::PipelineId;
use crate::db::types::utils::validate_name;
use crate::has_unstable_feature;
use clap::Parser;
use feldera_adapterlib::connector::ConnectorManifestEntry;
use feldera_ir::Dataflow;
use feldera_types::config::{
    ConnectorConfig, InputEndpointConfig, MultihostConfig, OutputEndpointConfig, PipelineConfig,
    PipelineConfigProgramInfo, ProgramIr, RuntimeConfig,
};
use feldera_types::program_schema::{ProgramSchema, PropertyValue, SourcePosition, SqlIdentifier};
use regex::Regex;
use serde::{Deserialize, Serialize};
use std::borrow::Cow;
use std::collections::{BTreeMap, HashMap};
use std::fmt;
use std::fmt::{Display, Formatter};
use std::path::PathBuf;
use std::str::FromStr;
use std::string::ParseError;
use thiserror::Error as ThisError;
use tracing::warn;
use utoipa::ToSchema;

/// Map from connector name to manifest entry, used for direction validation.
pub type ConnectorManifest = HashMap<String, ConnectorManifestEntry>;

/// Enumeration of possible compilation profiles that can be passed to the Rust compiler
/// as an argument via `cargo build --profile <>`. A compilation profile affects among
/// other things the compilation speed (how long till the program is ready to be run)
/// and runtime speed (the performance while running).
#[derive(Parser, Eq, PartialEq, Serialize, Deserialize, Debug, Clone, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum CompilationProfile {
    /// Used primarily for development. Adds source information to binaries.
    ///
    /// This corresponds to cargo's out-of-the-box "debug" mode
    Dev,
    /// Prioritizes compilation speed over runtime speed
    Unoptimized,
    /// Prioritizes runtime speed over compilation speed
    Optimized,
    /// Optimized compilation profile with minimal debug information.
    OptimizedSymbols,
}

impl CompilationProfile {
    pub fn to_target_folder(&self) -> &'static str {
        match self {
            CompilationProfile::Dev => "debug",
            CompilationProfile::Unoptimized => "unoptimized",
            CompilationProfile::Optimized => "optimized",
            CompilationProfile::OptimizedSymbols => "optimized_symbols",
        }
    }
}

impl FromStr for CompilationProfile {
    type Err = ParseError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "dev" => Ok(CompilationProfile::Dev),
            "unoptimized" => Ok(CompilationProfile::Unoptimized),
            "optimized" => Ok(CompilationProfile::Optimized),
            "optimized_symbols" => Ok(CompilationProfile::OptimizedSymbols),
            e => unimplemented!(
                "Unsupported option {e}. Available choices are 'dev', 'unoptimized' and 'optimized', 'optimized_symbols'."
            ),
        }
    }
}

impl Display for CompilationProfile {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match &self {
            CompilationProfile::Dev => write!(f, "dev"),
            CompilationProfile::Unoptimized => write!(f, "unoptimized"),
            CompilationProfile::Optimized => write!(f, "optimized"),
            CompilationProfile::OptimizedSymbols => write!(f, "optimized_symbols"),
        }
    }
}

/// A SQL compiler error.
///
/// The SQL compiler returns a list of errors in the following JSON format if
/// it's invoked with the `-je` option.
///
/// ```ignore
///  [ {
/// "start_line_number" : 2,
/// "start_column" : 4,
/// "end_line_number" : 2,
/// "end_column" : 8,
/// "warning" : false,
/// "error_type" : "PRIMARY KEY cannot be nullable",
/// "message" : "PRIMARY KEY column 'C' has type INTEGER, which is nullable",
/// "snippet" : "    2|   c INT PRIMARY KEY\n         ^^^^^\n    3|);\n"
/// } ]
/// ```
#[derive(Debug, Deserialize, Serialize, Eq, PartialEq, ToSchema, Clone)]
#[cfg_attr(test, derive(proptest_derive::Arbitrary))]
pub struct SqlCompilerMessage {
    start_line_number: usize,
    start_column: usize,
    end_line_number: usize,
    end_column: usize,
    warning: bool,
    pub(crate) error_type: String,
    message: String,
    snippet: Option<String>,
}

impl Display for SqlCompilerMessage {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{error_type}: {message} (line {start_line_number}, column {start_column})",
            error_type = self.error_type,
            message = self.message,
            start_line_number = self.start_line_number,
            start_column = self.start_column
        )
    }
}

impl SqlCompilerMessage {
    pub(crate) fn new_from_connector_generation_error(error: ConnectorGenerationError) -> Self {
        let position = match error {
            ConnectorGenerationError::PropertyDoesNotExist { position, .. } => position,
            // ConnectorGenerationError::PropertyMissing { position, .. } => position,
            ConnectorGenerationError::InvalidPropertyValue { position, .. } => position,
            ConnectorGenerationError::ExpectedInputConnector { position, .. } => position,
            ConnectorGenerationError::ExpectedOutputConnector { position, .. } => position,
            ConnectorGenerationError::RelationConnectorNameCollision { position, .. } => position,
            ConnectorGenerationError::UnknownConnector { position, .. } => position,
        };
        SqlCompilerMessage {
            start_line_number: position.start_line_number,
            start_column: position.start_column,
            end_line_number: position.end_line_number,
            end_column: position.end_column,
            warning: false,
            error_type: "ConnectorGenerationError".to_string(),
            message: error.to_string(),
            snippet: None,
        }
    }
}

/// Program compilation status.
#[derive(Debug, Deserialize, Serialize, Eq, PartialEq, ToSchema, Clone, Copy)]
#[cfg_attr(test, derive(proptest_derive::Arbitrary))]
pub enum ProgramStatus {
    /// Awaiting to be picked up for SQL compilation.
    Pending,
    /// Compilation of SQL is ongoing.
    CompilingSql,
    /// Compilation of SQL has been completed; awaiting to be picked up for Rust compilation.
    SqlCompiled,
    /// Compilation of Rust is ongoing.
    CompilingRust,
    /// Compilation (both SQL and Rust) succeeded.
    #[cfg_attr(test, proptest(weight = 2))]
    Success,
    /// SQL compiler returned one or more errors.
    SqlError,
    /// Rust compiler returned an error.
    RustError,
    /// System/OS returned an error when trying to invoke commands.
    SystemError,
}

impl TryFrom<String> for ProgramStatus {
    type Error = DBError;
    fn try_from(value: String) -> Result<Self, DBError> {
        match value.as_str() {
            "pending" => Ok(Self::Pending),
            "compiling_sql" => Ok(Self::CompilingSql),
            "sql_compiled" => Ok(Self::SqlCompiled),
            "compiling_rust" => Ok(Self::CompilingRust),
            "success" => Ok(Self::Success),
            "sql_error" => Ok(Self::SqlError),
            "rust_error" => Ok(Self::RustError),
            "system_error" => Ok(Self::SystemError),
            _ => Err(DBError::invalid_program_status(value)),
        }
    }
}

impl From<ProgramStatus> for &'static str {
    fn from(val: ProgramStatus) -> Self {
        match val {
            ProgramStatus::Pending => "pending",
            ProgramStatus::CompilingSql => "compiling_sql",
            ProgramStatus::SqlCompiled => "sql_compiled",
            ProgramStatus::CompilingRust => "compiling_rust",
            ProgramStatus::Success => "success",
            ProgramStatus::SqlError => "sql_error",
            ProgramStatus::RustError => "rust_error",
            ProgramStatus::SystemError => "system_error",
        }
    }
}

impl Display for ProgramStatus {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let status: &'static str = (*self).into();
        write!(f, "{status}")
    }
}

/// SQL compilation information.
#[derive(Debug, Deserialize, Serialize, Eq, PartialEq, ToSchema, Clone)]
pub struct SqlCompilationInfo {
    /// Exit code of the SQL compiler.
    pub exit_code: i32,
    /// Messages (warnings and errors) generated by the SQL compiler.
    pub messages: Vec<SqlCompilerMessage>,
}

impl SqlCompilationInfo {
    #[cfg(test)]
    pub(crate) fn success() -> Self {
        Self {
            exit_code: 0,
            messages: vec![],
        }
    }
}

/// Rust compilation information.
#[derive(Debug, Deserialize, Serialize, Eq, PartialEq, ToSchema, Clone)]
pub struct RustCompilationInfo {
    /// Exit code of the `cargo` compilation command.
    pub exit_code: i32,
    /// Output printed to stdout by the `cargo` compilation command.
    pub stdout: String,
    /// Output printed to stderr by the `cargo` compilation command.
    pub stderr: String,
}

impl RustCompilationInfo {
    #[cfg(test)]
    pub(crate) fn success() -> Self {
        Self {
            exit_code: 0,
            stdout: "".to_string(),
            stderr: "".to_string(),
        }
    }
}

impl Display for RustCompilationInfo {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Rust compilation process exited with code {}\n\nstdout:\n{}\n\nstderr:\n\n{}",
            self.exit_code, self.stdout, self.stderr
        )
    }
}

/// Log, warning and error information about the program compilation.
#[derive(Debug, Deserialize, Serialize, Eq, PartialEq, ToSchema, Clone)]
pub struct ProgramError {
    /// Information about the SQL compilation.
    /// - Set `Some(...)` upon transition to `SqlCompiled` or `SqlError`
    /// - Set `None` upon transition to `Pending`
    pub sql_compilation: Option<SqlCompilationInfo>,

    /// Information about the Rust compilation.
    /// - Set `Some(...)` upon transition to `Success` or `RustError`
    /// - Set `None` upon transition to `Pending` or `SqlCompiled`
    pub rust_compilation: Option<RustCompilationInfo>,

    /// System error that occurred.
    /// - Set `Some(...)` upon transition to `SystemError`
    /// - Set `None` upon transition to `Pending`
    pub system_error: Option<String>,
}

/// Validates the program status transition from current status to a new one.
pub fn validate_program_status_transition(
    current_status: ProgramStatus,
    new_status: ProgramStatus,
) -> Result<(), DBError> {
    if matches!(
        (current_status, new_status),
        (ProgramStatus::Pending, ProgramStatus::CompilingSql)
            | (ProgramStatus::Pending, ProgramStatus::SystemError)
            | (ProgramStatus::CompilingSql, ProgramStatus::Pending)
            | (ProgramStatus::CompilingSql, ProgramStatus::SqlCompiled)
            | (ProgramStatus::CompilingSql, ProgramStatus::SqlError)
            | (ProgramStatus::CompilingSql, ProgramStatus::SystemError)
            | (ProgramStatus::SqlCompiled, ProgramStatus::Pending)
            | (ProgramStatus::SqlCompiled, ProgramStatus::CompilingRust)
            | (ProgramStatus::CompilingRust, ProgramStatus::Pending)
            | (ProgramStatus::CompilingRust, ProgramStatus::SqlCompiled)
            | (ProgramStatus::CompilingRust, ProgramStatus::Success)
            | (ProgramStatus::CompilingRust, ProgramStatus::RustError)
            | (ProgramStatus::CompilingRust, ProgramStatus::SystemError)
            | (ProgramStatus::Success, ProgramStatus::Pending)
            | (ProgramStatus::SqlError, ProgramStatus::Pending)
            | (ProgramStatus::RustError, ProgramStatus::Pending)
            | (ProgramStatus::SystemError, ProgramStatus::Pending)
    ) {
        Ok(())
    } else {
        Err(DBError::InvalidProgramStatusTransition {
            current_status,
            new_status,
        })
    }
}

/// A selector for the runtime to use.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, ToSchema)]
#[serde(try_from = "String", into = "Option<String>")]
pub enum RuntimeSelector {
    /// A SHA of the runtime.
    ///
    /// The string is guaranteed to be a valid git SHA hash.
    Sha(String),
    /// A tagged version of the runtime.
    ///
    /// The string is guaranteed to be in the form of a git version tag `vX.Y.Z`.
    Version(String),
    /// The platform's default runtime version.
    ///
    /// The string corresponds to the git SHA of feldera/feldera at the time of the build.
    Platform(String),
}

impl From<RuntimeSelector> for Option<String> {
    fn from(value: RuntimeSelector) -> Self {
        match value {
            RuntimeSelector::Sha(sha) => Some(sha),
            RuntimeSelector::Version(version) => Some(version),
            RuntimeSelector::Platform(_platform_sha) => None,
        }
    }
}

impl RuntimeSelector {
    /// Returns a path to sources of the runtime which the compilation should be using.
    pub fn runtime_sources(&self, config: &CompilerConfig) -> String {
        if self.is_platform() {
            config.dbsp_override_path.clone()
        } else {
            assert!(has_unstable_feature("runtime_version"));
            let repo_location =
                PathBuf::from(&config.compiler_working_directory).join("feldera-checkout");
            repo_location.to_string_lossy().to_string()
        }
    }

    /// Is this the platform's default runtime version?
    pub fn is_platform(&self) -> bool {
        matches!(self, RuntimeSelector::Platform(_))
    }

    /// Bytes representation of the runtime selector (for hashing).
    pub fn as_bytes(&self) -> &[u8] {
        match self {
            RuntimeSelector::Sha(sha) => sha.as_bytes(),
            RuntimeSelector::Version(version) => version.as_bytes(),
            RuntimeSelector::Platform(platform_sha) => platform_sha.as_bytes(),
        }
    }

    /// Commitish representation of the runtime selector (for git operations).
    pub fn as_commitish(&self) -> &str {
        match self {
            RuntimeSelector::Sha(sha) => sha,
            RuntimeSelector::Version(version) => version,
            RuntimeSelector::Platform(platform_sha) => platform_sha,
        }
    }
}

impl Display for RuntimeSelector {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            RuntimeSelector::Sha(sha) => write!(f, "{sha}"),
            RuntimeSelector::Version(version) => write!(f, "{version}"),
            RuntimeSelector::Platform(platform_sha) => write!(f, "{platform_sha}"),
        }
    }
}

impl Default for RuntimeSelector {
    fn default() -> Self {
        RuntimeSelector::Platform(env!("VERGEN_GIT_SHA").to_string())
    }
}

impl TryFrom<String> for RuntimeSelector {
    type Error = String;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        fn is_valid_git_sha(s: &str) -> bool {
            let sha_regex = Regex::new(r"^[0-9a-f]{40}$").unwrap();
            sha_regex.is_match(s)
        }

        fn is_version_tag(s: &str) -> bool {
            let version_regex = Regex::new(r"^v\d+\.\d+\.\d+$").unwrap();
            version_regex.is_match(s)
        }

        if is_valid_git_sha(&value) {
            Ok(RuntimeSelector::Sha(value))
        } else if is_version_tag(&value) {
            Ok(RuntimeSelector::Version(value))
        } else {
            Err(format!(
                "The requested runtime version '{value}' is neither a valid version (vX.Y.Z) nor a valid git hash.",
            ))
        }
    }
}

/// Program configuration.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, ToSchema)]
#[serde(default)]
pub struct ProgramConfig {
    /// Compilation profile.
    /// If none is specified, the compiler default compilation profile is used.
    pub profile: Option<CompilationProfile>,

    /// If `true` (default), when a prior compilation with the same checksum
    /// already exists, the output of that (i.e., binary) is used.
    /// Set `false` to always trigger a new compilation, which might take longer
    /// and as well can result in overriding an existing binary.
    pub cache: bool,

    /// Override runtime version of the pipeline being executed.
    ///
    /// Warning: This setting is experimental and may change in the future.
    /// Requires the platform to run with the unstable feature `runtime_version`
    /// enabled. Should only be used for testing purposes, and requires
    /// network access.
    ///
    /// A runtime version can be specified in the form of a version
    /// or SHA taken from the `feldera/feldera` repository main branch.
    ///
    /// Examples: `v0.96.0` or `f4dcac0989ca0fda7d2eb93602a49d007cb3b0ae`
    ///
    /// A platform of version `0.x.y` may be capable of running future and past
    /// runtimes with versions `>=0.x.y` and `<=0.x.y` until breaking API changes happen,
    /// the exact bounds for each platform version are unspecified until we reach a
    /// stable version. Compatibility is only guaranteed if platform and runtime version
    /// are exact matches.
    ///
    /// Note that any enterprise features are currently considered to be part of
    /// the platform.
    ///
    /// If not set (null), the runtime version will be the same as the platform version.
    #[schema(value_type = Option<String>)]
    pub runtime_version: Option<RuntimeSelector>,
}

impl ProgramConfig {
    pub(crate) fn runtime_version(&self) -> RuntimeSelector {
        let rt = self.runtime_version.clone().unwrap_or_default();
        if has_unstable_feature("runtime_version") {
            rt
        } else if !has_unstable_feature("runtime_version") && !rt.is_platform() {
            warn!("Runtime version {rt} specified but argument is ignored because the platform does not have the unstable feature `runtime_version` enabled.");
            RuntimeSelector::default()
        } else {
            RuntimeSelector::default()
        }
    }
}

impl Default for ProgramConfig {
    fn default() -> Self {
        Self {
            profile: None,
            cache: true,
            runtime_version: None,
        }
    }
}

#[derive(ThisError, Serialize, Deserialize, PartialEq, Debug, ToSchema)]
pub enum ConnectorGenerationError {
    #[error("relation '{relation}': property '{key}' does not exist")]
    PropertyDoesNotExist {
        position: SourcePosition,
        relation: String,
        key: String,
    },
    // #[error("relation '{relation}': required property '{key}' is missing")]
    // PropertyMissing { position: SourcePosition, relation: String, key: String },
    #[error(
        "relation '{relation}': property '{key}' has value '{value}' which is invalid: {reason}"
    )]
    InvalidPropertyValue {
        position: SourcePosition,
        relation: String,
        key: String,
        value: String,
        reason: Box<String>, // Put into a Box because else the error type becomes too large according to clippy
    },
    #[error("relation '{relation}': expected an input variant but got an output variant for connector '{connector_name}'")]
    ExpectedInputConnector {
        position: SourcePosition,
        relation: String,
        connector_name: String,
    },
    #[error("relation '{relation}': expected an output variant but got an input variant for connector '{connector_name}'")]
    ExpectedOutputConnector {
        position: SourcePosition,
        relation: String,
        connector_name: String,
    },
    #[error("relation '{relation}': encountered collision when using {{relation}}.{{connector_name}} as unique name: '{relation}.{connector_name}' is not unique")]
    RelationConnectorNameCollision {
        position: SourcePosition,
        relation: String,
        connector_name: String,
    },
    #[error("relation '{relation}': connector '{name}' is not recognized; verify it is listed in connectors.toml and the describer build succeeded")]
    UnknownConnector {
        position: SourcePosition,
        relation: String,
        name: String,
    },
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize, ToSchema)]
struct NamedConnector {
    pub name: Option<String>,

    #[serde(flatten)]
    pub config: ConnectorConfig,
}

/// Parses the properties to create a vector of connectors with optional name and configuration.
/// It also returns the originating property value it parsed, which is used for error reporting
/// later on.
fn parse_named_connectors(
    relation: SqlIdentifier,
    properties: &BTreeMap<String, PropertyValue>,
) -> Result<(Vec<NamedConnector>, Option<PropertyValue>), ConnectorGenerationError> {
    match properties.get("connectors") {
        Some(value) => {
            let connectors =
                serde_json::from_str::<Vec<NamedConnector>>(&value.value).map_err(|e| {
                    ConnectorGenerationError::InvalidPropertyValue {
                        position: value.value_position,
                        relation: relation.sql_name(),
                        key: "connectors".to_string(),
                        value: value.value.clone(),
                        reason: Box::new(format!(
                            "deserialization failed: {e} (position is within the string itself)"
                        )),
                    }
                })?;
            for connector in &connectors {
                if let Some(name) = &connector.name {
                    validate_name(name).map_err(|e| {
                        ConnectorGenerationError::InvalidPropertyValue {
                            position: value.value_position,
                            relation: relation.sql_name(),
                            key: "connectors".to_string(),
                            value: value.value.clone(),
                            reason: Box::new(format!("connector name '{name}' is not valid: {e}")),
                        }
                    })?;
                }
            }
            Ok((connectors, Some(value.clone())))
        }
        None => Ok((vec![], None)),
    }
}

/// Receives a list of all connectors with for each:
/// - The name of the stream it belongs to
/// - (Optionally) given connector name scoped to stream
/// - Connector configuration
/// - Property value it was parsed from (for error reporting)
///
/// The stream and connector name are used to form the endpoint name as `{stream}.{connector}`.
/// If no connector name was given, it will be given the connector name `unnamed-{index}`, with the
/// index scoped to the stream. An error is returned if an endpoint name collides with another.
///
/// Returns the stream name, endpoint name, and connector configuration for each connector.
fn determine_connector_endpoint_names(
    connectors: Vec<(String, Option<String>, ConnectorConfig, PropertyValue)>,
) -> Result<Vec<(String, String, ConnectorConfig)>, ConnectorGenerationError> {
    let mut result = vec![];
    let mut existing_endpoints = vec![];
    let mut stream_counter: BTreeMap<String, u64> = BTreeMap::new();
    for (stream, given_connector_name, connector_config, origin_value) in connectors.into_iter() {
        // Given name is used if it exists, else it will default to `unnamed-{index}`
        let mut counter = stream_counter.get(&stream).copied().unwrap_or(0);
        let connector_name = given_connector_name.unwrap_or(format!("unnamed-{counter}"));
        counter += 1;
        stream_counter.insert(stream.clone(), counter);

        // The endpoint name is a combination of: `{stream}.{connector}`
        let endpoint_name = format!("{}.{connector_name}", SqlIdentifier::from(&stream).name());

        // The endpoint name can collide if:
        // - Two or more connectors are given the same name in the stream
        // - A connector is given the name `unnamed-0` and collides with the name generated for an
        //   unnamed connector at the first position
        if existing_endpoints.contains(&endpoint_name) {
            return Err(ConnectorGenerationError::RelationConnectorNameCollision {
                position: origin_value.value_position,
                relation: stream.clone(),
                connector_name,
            });
        }
        existing_endpoints.push(endpoint_name.clone());

        // Result has the stream, endpoint name, and connector configuration
        result.push((stream, endpoint_name, connector_config))
    }
    Ok(result)
}

/// Program information is the output of the SQL compiler.
///
/// It includes information needed for Rust compilation (e.g., generated Rust code)
/// as well as only for runtime (e.g., schema, input/output connectors).
#[derive(Default, Deserialize, Serialize, Eq, PartialEq, Debug, Clone, ToSchema)]
pub struct ProgramInfo {
    /// Schema of the compiled SQL.
    pub schema: ProgramSchema,

    /// Generated main program Rust code: main.rs
    #[serde(default)] // TODO: when a breaking migration can happen, remove this default
    pub main_rust: String,

    /// Generated user defined function (UDF) stubs Rust code: stubs.rs
    #[serde(default)] // TODO: when a breaking migration can happen, remove this default
    pub udf_stubs: String,

    /// Dataflow graph of the program.
    #[serde(default)]
    pub dataflow: Option<Dataflow>,

    /// Input connectors derived from the schema.
    pub input_connectors: BTreeMap<Cow<'static, str>, InputEndpointConfig>,

    /// Output connectors derived from the schema.
    pub output_connectors: BTreeMap<Cow<'static, str>, OutputEndpointConfig>,
}

impl ProgramInfo {
    /// Extract fields from `ProgramInfo` to create a `PipelineConfigProgramInfo` instance.
    pub fn to_pipeline_config_program_info(&self) -> PipelineConfigProgramInfo {
        let program_ir = self.dataflow.as_ref().map(|d| ProgramIr {
            mir: d.mir.clone(),
            program_schema: self.schema.clone(),
        });

        PipelineConfigProgramInfo {
            inputs: self.input_connectors.clone(),
            outputs: self.output_connectors.clone(),
            program_ir,
        }
    }
}

/// Generates the program info using the program schema.
///
/// The `manifest` maps connector names to their manifest entries and is used
/// for direction validation (input vs. output). Pass
/// `build_in_process_manifest()` for bundled-only tenants; pass a manifest
/// parsed from the cached JSON for tenants with a non-empty `connectors.toml`.
pub fn generate_program_info(
    program_schema: ProgramSchema,
    main_rust: String,
    udf_stubs: String,
    dataflow: Option<Dataflow>,
    manifest: &ConnectorManifest,
) -> Result<ProgramInfo, ConnectorGenerationError> {
    // Input connectors
    let mut input_connectors = vec![];
    for input_relation in &program_schema.inputs {
        let (connectors, origin_value) =
            parse_named_connectors(input_relation.name.clone(), &input_relation.properties)?;
        for connector in connectors {
            let origin_value = origin_value
                .clone()
                .expect("Origin value cannot be None if connectors is non-empty");
            let transport_name = connector.config.transport.name();
            match manifest.get(&transport_name) {
                None => {
                    return Err(ConnectorGenerationError::UnknownConnector {
                        position: origin_value.value_position,
                        relation: input_relation.name.sql_name(),
                        name: transport_name,
                    });
                }
                Some(entry) if !entry.direction.allows_input() => {
                    return Err(ConnectorGenerationError::ExpectedInputConnector {
                        position: origin_value.value_position,
                        relation: input_relation.name.sql_name(),
                        connector_name: connector.name.unwrap_or("<unnamed>".to_string()),
                    });
                }
                Some(_) => {}
            }
            input_connectors.push((
                input_relation.name.sql_name(),
                connector.name,
                connector.config,
                origin_value,
            ));
        }
    }

    // Convert input connectors to ones with unique names which are turned into endpoints
    let mut inputs: BTreeMap<Cow<'static, str>, InputEndpointConfig> = BTreeMap::new();
    for (stream, endpoint_name, connector_config) in
        determine_connector_endpoint_names(input_connectors.clone())?
    {
        inputs.insert(
            Cow::from(endpoint_name),
            InputEndpointConfig {
                stream: Cow::from(stream),
                connector_config,
            },
        );
    }
    assert_eq!(inputs.len(), input_connectors.len());

    // Output connectors
    let mut output_connectors = vec![];
    for output_relation in &program_schema.outputs {
        let (connectors, origin_value) =
            parse_named_connectors(output_relation.name.clone(), &output_relation.properties)?;
        for connector in connectors {
            let origin_value = origin_value
                .clone()
                .expect("Origin value cannot be None if connectors is non-empty");
            let transport_name = connector.config.transport.name();
            match manifest.get(&transport_name) {
                None => {
                    return Err(ConnectorGenerationError::UnknownConnector {
                        position: origin_value.value_position,
                        relation: output_relation.name.sql_name(),
                        name: transport_name,
                    });
                }
                Some(entry) if !entry.direction.allows_output() => {
                    return Err(ConnectorGenerationError::ExpectedOutputConnector {
                        position: origin_value.value_position,
                        relation: output_relation.name.sql_name(),
                        connector_name: connector.name.unwrap_or("<unnamed>".to_string()),
                    });
                }
                Some(_) => {}
            }
            output_connectors.push((
                output_relation.name.sql_name(),
                connector.name,
                connector.config,
                origin_value,
            ));
        }
    }

    // Convert output connectors to ones with unique names which are turned into endpoints
    let mut outputs: BTreeMap<Cow<'static, str>, OutputEndpointConfig> = BTreeMap::new();
    for (stream, endpoint_name, connector_config) in
        determine_connector_endpoint_names(output_connectors.clone())?
    {
        outputs.insert(
            Cow::from(endpoint_name),
            OutputEndpointConfig {
                stream: Cow::from(stream),
                connector_config,
            },
        );
    }
    assert_eq!(outputs.len(), output_connectors.len());

    Ok(ProgramInfo {
        schema: program_schema,
        udf_stubs,
        main_rust,
        dataflow,
        input_connectors: inputs,
        output_connectors: outputs,
    })
}

/// Generates the pipeline configuration derived from the runtime configuration and the
/// input/output connectors derived from the program schema.
///
/// `program_info` is specified if the program was compiled by an older version of
/// the runtime and its program info hasn't been uploaded to the compiler server.
/// Such programs require the pipeline manager to provide program info via PipelineConfig.
// TODO: remove the program_info parameter once we're allowed to stop supporting
// platform versions <=0.199.0.
pub fn generate_pipeline_config(
    pipeline_id: PipelineId,
    pipeline_name: &str,
    runtime_config: &RuntimeConfig,
    program_info: Option<&ProgramInfo>,
) -> PipelineConfig {
    let (inputs, outputs, program_ir) = if let Some(program_info) = program_info {
        // Only keep tables and views, ignoring intermediate nodes.
        // These are currently the only nodes used by the pipeline
        // (to compute pipeline diffs). Including all nodes can cause the IR
        // to exceed the maximum ConfigMap size supported by k8s (1MB).
        let mir = program_info
            .dataflow
            .as_ref()
            .map(|d| {
                d.mir
                    .iter()
                    .filter_map(|(k, v)| {
                        if v.is_relation() {
                            Some((k.clone(), v.clone()))
                        } else {
                            None
                        }
                    })
                    .collect()
            })
            .unwrap_or_default();

        // Remove inputs and outputs that do not have lateness.
        // This field is currently only used for backfill avoidance, which only cares about
        // relations with lateness. Including the entire schema would cause the IR to exceed the
        // maximum ConfigMap size supported by k8s (1MB).
        let mut program_schema = program_info.schema.clone();
        program_schema.inputs.retain(|input| input.has_lateness());
        program_schema
            .outputs
            .retain(|output| output.has_lateness());

        let program_ir = ProgramIr {
            mir,
            program_schema,
        };

        (
            program_info.input_connectors.clone(),
            program_info.output_connectors.clone(),
            Some(program_ir),
        )
    } else {
        Default::default()
    };

    PipelineConfig {
        name: Some(format!("pipeline-{pipeline_id}")),
        given_name: Some(pipeline_name.to_string()),
        global: runtime_config.clone(),
        storage_config: None, // Set by the runner based on global field
        secrets_dir: None,
        multihost: if runtime_config.hosts > 1 {
            Some(MultihostConfig {
                hosts: runtime_config.hosts,
            })
        } else {
            None
        },
        inputs,
        outputs,
        program_ir,
    }
}

#[cfg(test)]
mod tests {
    use super::{determine_connector_endpoint_names, RuntimeSelector};
    use crate::db::types::program::ConnectorGenerationError::RelationConnectorNameCollision;
    use feldera_types::config::{ConnectorConfig, TransportConfig};
    use feldera_types::program_schema::{PropertyValue, SourcePosition};
    use feldera_types::transport::datagen::DatagenInputConfig;

    #[test]
    fn test_runtime_version_validation() {
        assert!(RuntimeSelector::try_from("invalid".to_string()).is_err());
        assert!(RuntimeSelector::try_from("/bin/bash".to_string()).is_err());
        assert!(RuntimeSelector::try_from("v2".to_string()).is_err());
        assert!(RuntimeSelector::try_from("v0.3".to_string()).is_err());
        assert!(RuntimeSelector::try_from("v4.3".to_string()).is_err());
        assert!(RuntimeSelector::try_from("v0.0".to_string()).is_err());
        assert!(RuntimeSelector::try_from("0.0.0".to_string()).is_err());
        assert!(RuntimeSelector::try_from("v1.2.34".to_string()).is_ok());
        assert!(
            RuntimeSelector::try_from("d0b45d8f87056c9d2c89c6f63b2531b0c5905f9b".to_string())
                .is_ok()
        );
        assert!(
            RuntimeSelector::try_from("d0b45d8f87056c9d2c89c6f63b2531b0c5905f9".to_string())
                .is_err()
        );
        assert!(
            RuntimeSelector::try_from("d0b45d8f87056c9d2c-9c6f63b2531b0c5905f9b".to_string())
                .is_err()
        );
    }

    #[test]
    #[rustfmt::skip]
    fn test_connector_endpoint_name_determination() {
        // Reuse the configuration as it is not used in the function
        let config = ConnectorConfig {
            transport: TransportConfig::Datagen(DatagenInputConfig::default()),
            format: None,
            preprocessor: None,
            index: None,
            output_buffer_config: Default::default(),
            max_batch_size: Some(0),
            max_worker_batch_size: None,
            max_queued_records: 0,
            paused: false,
            labels: vec![],
            start_after: None,
        };

        // Reuse property value as it is only used in the errors
        let property_value = PropertyValue {
            value: "".to_string(),
            key_position: SourcePosition {
                start_line_number: 1,
                start_column: 2,
                end_line_number: 3,
                end_column: 4,
            },
            value_position: SourcePosition {
                start_line_number: 5,
                start_column: 6,
                end_line_number: 7,
                end_column: 8,
            },
        };

        // Success: empty
        assert_eq!(determine_connector_endpoint_names(vec![]).unwrap(), vec![]);

        // Success: several combinations
        assert_eq!(
            determine_connector_endpoint_names(
                vec![
                    ("s1".to_string(), None, config.clone(), property_value.clone()),
                    ("s2".to_string(), Some("c1".to_string()), config.clone(), property_value.clone()),
                    ("s3".to_string(), None, config.clone(), property_value.clone()),
                    ("s3".to_string(), Some("c1".to_string()), config.clone(), property_value.clone()),
                    ("s4".to_string(), None, config.clone(), property_value.clone()),
                    ("s4".to_string(), Some("c2".to_string()), config.clone(), property_value.clone()),
                    ("s4".to_string(), None, config.clone(), property_value.clone()),
                    ("s4".to_string(), Some("c1".to_string()), config.clone(), property_value.clone()),
                ]
            ).unwrap(),
            vec![
                ("s1".to_string(), "s1.unnamed-0".to_string(), config.clone()),
                ("s2".to_string(), "s2.c1".to_string(), config.clone()),
                ("s3".to_string(), "s3.unnamed-0".to_string(), config.clone(),),
                ("s3".to_string(), "s3.c1".to_string(), config.clone()),
                ("s4".to_string(), "s4.unnamed-0".to_string(), config.clone()),
                ("s4".to_string(), "s4.c2".to_string(), config.clone()),
                ("s4".to_string(), "s4.unnamed-2".to_string(), config.clone()),
                ("s4".to_string(), "s4.c1".to_string(), config.clone()),
            ]
        );

        // Collision: unnamed-0
        assert_eq!(
            determine_connector_endpoint_names(
                vec![
                    ("s1".to_string(), None, config.clone(), property_value.clone()),
                    ("s1".to_string(), Some("unnamed-0".to_string()), config.clone(), property_value.clone()),
                ]
            ).unwrap_err(),
            RelationConnectorNameCollision {
                position: property_value.value_position,
                relation: "s1".to_string(),
                connector_name: "unnamed-0".to_string(),
            }
        );

        // Collision: given
        assert_eq!(
            determine_connector_endpoint_names(
                vec![
                    ("t1".to_string(), Some("example".to_string()), config.clone(), property_value.clone()),
                    ("t1".to_string(), Some("example".to_string()), config.clone(), property_value.clone()),
                ]
            ).unwrap_err(),
            RelationConnectorNameCollision {
                position: property_value.value_position,
                relation: "t1".to_string(),
                connector_name: "example".to_string(),
            }
        );
    }

    /// A `TransportConfig::Plugin` config surfaces a clean `UnknownConnector` error
    /// rather than crashing or returning a misleading direction error.
    #[test]
    fn plugin_connector_yields_unknown_connector_error() {
        use crate::db::types::program::ConnectorGenerationError;
        use feldera_types::program_schema::SourcePosition;

        let pos = SourcePosition {
            start_line_number: 1,
            start_column: 0,
            end_line_number: 1,
            end_column: 10,
        };

        let err = ConnectorGenerationError::UnknownConnector {
            position: pos,
            relation: "my_table".to_string(),
            name: "acme_snowflake".to_string(),
        };

        // Verify the error message is human-readable and names the connector.
        let msg = err.to_string();
        assert!(msg.contains("acme_snowflake"), "message should name the connector: {msg}");
        assert!(msg.contains("my_table"), "message should name the relation: {msg}");
        assert!(msg.contains("connectors.toml"), "message should mention connectors.toml: {msg}");
    }

    /// `generate_program_info` with a stub manifest accepts known input connectors,
    /// rejects output connectors on input relations, and rejects unknown connectors.
    ///
    /// The manifest is built inline rather than from `build_in_process_manifest()`
    /// because the pipeline-manager unit-test binary does not link the adapter
    /// crates that register connectors via `inventory::submit!`.
    #[test]
    fn generate_program_info_manifest_lookup() {
        use crate::db::types::program::{
            generate_program_info, ConnectorGenerationError, ConnectorManifest,
        };
        use feldera_adapterlib::connector::{
            ConnectorFlags, ConnectorKind, ConnectorManifestEntry, Direction,
        };
        use feldera_types::constants::ADAPTERLIB_API_VERSION;
        use feldera_types::program_schema::{
            ProgramSchema, PropertyValue, Relation, SourcePosition, SqlIdentifier,
        };
        use std::collections::BTreeMap;

        /// Build a minimal manifest entry for testing.
        fn entry(name: &str, direction: Direction) -> ConnectorManifestEntry {
            ConnectorManifestEntry {
                adapterlib_api_version: ADAPTERLIB_API_VERSION,
                name: name.to_string(),
                direction,
                kind: ConnectorKind::Regular,
                fault_tolerance: None,
                flags_bits: ConnectorFlags::empty().bits(),
                has_build_input: false,
                has_build_output: false,
                has_build_integrated_input: false,
                has_build_integrated_output: false,
                builder_crate: "dbsp_adapters".to_string(),
            }
        }

        // Stub manifest: "my_input" is input-only, "my_output" is output-only.
        let manifest: ConnectorManifest = [
            ("my_input".to_string(), entry("my_input", Direction::Input)),
            ("my_output".to_string(), entry("my_output", Direction::Output)),
        ]
        .into_iter()
        .collect();

        let pos = SourcePosition {
            start_line_number: 1,
            start_column: 0,
            end_line_number: 1,
            end_column: 10,
        };

        // Helper: build a relation with a single connector property.
        let make_relation = |name: &str, connector_json: &str| {
            let mut properties = BTreeMap::new();
            properties.insert(
                "connectors".to_string(),
                PropertyValue {
                    value: connector_json.to_string(),
                    key_position: pos,
                    value_position: pos,
                },
            );
            Relation {
                name: SqlIdentifier::new(name, false),
                fields: vec![],
                materialized: false,
                properties,
            }
        };

        // An input connector on an input relation must be accepted.
        let input_json = r#"[{"transport":{"name":"my_input","config":{}}}]"#;
        let schema = ProgramSchema {
            inputs: vec![make_relation("t1", input_json)],
            outputs: vec![],
        };
        let result = generate_program_info(schema, String::new(), String::new(), None, &manifest);
        assert!(
            result.is_ok(),
            "known input connector should be accepted on an input relation, got: {result:?}"
        );

        // An output-only connector used on an input relation must be rejected.
        let output_as_input_json = r#"[{"transport":{"name":"my_output","config":{}}}]"#;
        let schema = ProgramSchema {
            inputs: vec![make_relation("t1", output_as_input_json)],
            outputs: vec![],
        };
        let err =
            generate_program_info(schema, String::new(), String::new(), None, &manifest)
                .unwrap_err();
        assert!(
            matches!(err, ConnectorGenerationError::ExpectedInputConnector { .. }),
            "output connector used as input should return ExpectedInputConnector, got: {err:?}"
        );

        // An output connector on an output relation must be accepted.
        let output_json = r#"[{"transport":{"name":"my_output","config":{}}}]"#;
        let schema = ProgramSchema {
            inputs: vec![],
            outputs: vec![make_relation("v1", output_json)],
        };
        let result = generate_program_info(schema, String::new(), String::new(), None, &manifest);
        assert!(
            result.is_ok(),
            "known output connector should be accepted on an output relation, got: {result:?}"
        );

        // An input-only connector used on an output relation must be rejected.
        let input_as_output_json = r#"[{"transport":{"name":"my_input","config":{}}}]"#;
        let schema = ProgramSchema {
            inputs: vec![],
            outputs: vec![make_relation("v1", input_as_output_json)],
        };
        let err =
            generate_program_info(schema, String::new(), String::new(), None, &manifest)
                .unwrap_err();
        assert!(
            matches!(err, ConnectorGenerationError::ExpectedOutputConnector { .. }),
            "input connector used as output should return ExpectedOutputConnector, got: {err:?}"
        );

        // An unknown connector name must yield UnknownConnector.
        let unknown_json = r#"[{"transport":{"name":"acme_snowflake","config":{}}}]"#;
        let schema = ProgramSchema {
            inputs: vec![make_relation("t1", unknown_json)],
            outputs: vec![],
        };
        let err =
            generate_program_info(schema, String::new(), String::new(), None, &manifest)
                .unwrap_err();
        assert!(
            matches!(err, ConnectorGenerationError::UnknownConnector { .. }),
            "unknown connector should return UnknownConnector, got: {err:?}"
        );
    }
}
