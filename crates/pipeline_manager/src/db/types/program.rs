use crate::db::error::DBError;
use crate::db::types::common::validate_name;
use crate::db::types::pipeline::PipelineId;
use clap::Parser;
use log::error;
use pipeline_types::config::{
    ConnectorConfig, InputEndpointConfig, OutputEndpointConfig, PipelineConfig, RuntimeConfig,
    TransportConfig,
};
use pipeline_types::program_schema::ProgramSchema;
use rand::distributions::Uniform;
use rand::{thread_rng, Rng};
use serde::{Deserialize, Serialize};
use std::borrow::Cow;
use std::collections::BTreeMap;
use std::fmt::Display;
use std::str::FromStr;
use std::string::ParseError;
use thiserror::Error as ThisError;
use utoipa::ToSchema;

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
}

impl CompilationProfile {
    pub fn to_target_folder(&self) -> &'static str {
        match self {
            CompilationProfile::Dev => "debug",
            CompilationProfile::Unoptimized => "unoptimized",
            CompilationProfile::Optimized => "optimized",
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
            e => unimplemented!(
                "Unsupported option {e}. Available choices are 'dev', 'unoptimized' and 'optimized'"
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
/// "startLineNumber" : 2,
/// "startColumn" : 4,
/// "endLineNumber" : 2,
/// "endColumn" : 8,
/// "warning" : false,
/// "errorType" : "PRIMARY KEY cannot be nullable",
/// "message" : "PRIMARY KEY column 'C' has type INTEGER, which is nullable",
/// "snippet" : "    2|   c INT PRIMARY KEY\n         ^^^^^\n    3|);\n"
/// } ]
/// ```
#[derive(Debug, Deserialize, Serialize, Eq, PartialEq, ToSchema, Clone)]
#[cfg_attr(test, derive(proptest_derive::Arbitrary))]
#[serde(rename_all = "camelCase")]
pub struct SqlCompilerMessage {
    start_line_number: usize,
    start_column: usize,
    end_line_number: usize,
    end_column: usize,
    warning: bool,
    error_type: String,
    message: String,
    snippet: Option<String>,
}

impl SqlCompilerMessage {
    pub(crate) fn new_from_connector_generation_error(error: ConnectorGenerationError) -> Self {
        SqlCompilerMessage {
            start_line_number: 0,
            start_column: 0,
            end_line_number: 0,
            end_column: 0,
            warning: false,
            error_type: "ConnectorGenerationError".to_string(),
            message: error.to_string(),
            snippet: None,
        }
    }
}

/// Program compilation status.
#[derive(Debug, Deserialize, Serialize, Eq, PartialEq, ToSchema, Clone)]
#[cfg_attr(test, derive(proptest_derive::Arbitrary))]
pub enum ProgramStatus {
    /// Compilation request received from the user; program has been placed
    /// in the queue.
    Pending,
    /// Compilation of SQL -> Rust in progress.
    CompilingSql,
    /// Compiling Rust -> executable in progress.
    CompilingRust,
    /// Compilation succeeded.
    #[cfg_attr(test, proptest(weight = 2))]
    Success,
    /// SQL compiler returned an error.
    SqlError(Vec<SqlCompilerMessage>),
    /// Rust compiler returned an error.
    RustError(String),
    /// System/OS returned an error when trying to invoke commands.
    SystemError(String),
}

/// The database encodes program status using two columns: `status`, which has
/// type `string`, but acts as an enum, and `error`, only used if `status` is
/// one of `"sql_error"` or `"rust_error"`.
impl ProgramStatus {
    /// Return true if program has been successfully compiled
    pub(crate) fn is_fully_compiled(&self) -> bool {
        *self == ProgramStatus::Success
    }

    /// Return true if the program has failed to compile (for any reason).
    pub(crate) fn has_failed_to_compile(&self) -> bool {
        matches!(
            self,
            ProgramStatus::SqlError(_)
                | ProgramStatus::RustError(_)
                | ProgramStatus::SystemError(_)
        )
    }

    /// Return true if program is currently compiling.
    pub(crate) fn is_compiling(&self) -> bool {
        *self == ProgramStatus::CompilingRust || *self == ProgramStatus::CompilingSql
    }

    /// Decode `ProgramStatus` from the values of `error` and `status` columns.
    pub fn from_columns(
        status_string: &str,
        error_string: Option<String>,
    ) -> Result<Self, DBError> {
        match status_string {
            "success" => Ok(Self::Success),
            "pending" => Ok(Self::Pending),
            "compiling_sql" => Ok(Self::CompilingSql),
            "compiling_rust" => Ok(Self::CompilingRust),
            "sql_error" => {
                let error = error_string.unwrap_or_default();
                if let Ok(messages) = serde_json::from_str(&error) {
                    Ok(Self::SqlError(messages))
                } else {
                    error!("Expected valid json for SqlCompilerMessage but got {:?}, did you update the struct without adjusting the database?", error);
                    Ok(Self::SystemError(error))
                }
            }
            "rust_error" => Ok(Self::RustError(error_string.unwrap_or_default())),
            "system_error" => Ok(Self::SystemError(error_string.unwrap_or_default())),
            status => Err(DBError::invalid_program_status(status.to_string())),
        }
    }
    pub fn to_columns(&self) -> (Option<String>, Option<String>) {
        match self {
            ProgramStatus::Success => (Some("success".to_string()), None),
            ProgramStatus::Pending => (Some("pending".to_string()), None),
            ProgramStatus::CompilingSql => (Some("compiling_sql".to_string()), None),
            ProgramStatus::CompilingRust => (Some("compiling_rust".to_string()), None),
            ProgramStatus::SqlError(error) => {
                if let Ok(error_string) = serde_json::to_string(&error) {
                    (Some("sql_error".to_string()), Some(error_string))
                } else {
                    error!("Expected valid json for SqlError, but got {:?}", error);
                    (Some("sql_error".to_string()), None)
                }
            }
            ProgramStatus::RustError(error) => {
                (Some("rust_error".to_string()), Some(error.clone()))
            }
            ProgramStatus::SystemError(error) => {
                (Some("system_error".to_string()), Some(error.clone()))
            }
        }
    }
}

/// Validates the program status transition from current status to a new one.
pub fn validate_program_status_transition(
    current_status: &ProgramStatus,
    new_status: &ProgramStatus,
) -> Result<(), DBError> {
    if matches!(
        (current_status, new_status),
        (ProgramStatus::Pending, ProgramStatus::CompilingSql)
            | (ProgramStatus::Pending, ProgramStatus::SystemError(_))
            | (ProgramStatus::CompilingSql, ProgramStatus::Pending)
            | (ProgramStatus::CompilingSql, ProgramStatus::CompilingRust)
            | (ProgramStatus::CompilingSql, ProgramStatus::SqlError(_))
            | (ProgramStatus::CompilingSql, ProgramStatus::SystemError(_))
            | (ProgramStatus::CompilingRust, ProgramStatus::Pending)
            | (ProgramStatus::CompilingRust, ProgramStatus::Success)
            | (ProgramStatus::CompilingRust, ProgramStatus::RustError(_))
            | (ProgramStatus::CompilingRust, ProgramStatus::SystemError(_))
            | (ProgramStatus::Success, ProgramStatus::Pending)
    ) {
        Ok(())
    } else {
        Err(DBError::InvalidProgramStatusTransition {
            current: current_status.clone(),
            transition_to: new_status.clone(),
        })
    }
}

/// Program configuration.
#[derive(Clone, Debug, Eq, PartialEq, Default, Serialize, Deserialize, ToSchema)]
pub struct ProgramConfig {
    /// Compilation profile.
    /// If none is specified, the compiler default compilation profile is used.
    pub profile: Option<CompilationProfile>,
}

impl ProgramConfig {
    pub fn from_yaml(s: &str) -> Self {
        serde_yaml::from_str(s).unwrap()
    }

    pub fn to_yaml(&self) -> String {
        serde_yaml::to_string(self).unwrap()
    }
}

#[derive(ThisError, Serialize, Deserialize, Debug, ToSchema)]
pub enum ConnectorGenerationError {
    #[error("relation '{relation}': property '{key}' does not exist")]
    PropertyDoesNotExist { relation: String, key: String },
    #[error("relation '{relation}': required property '{key}' is missing")]
    PropertyMissing { relation: String, key: String },
    #[error(
        "relation '{relation}': property '{key}' has value '{value}' which is invalid: {reason}"
    )]
    InvalidPropertyValue {
        relation: String,
        key: String,
        value: String,
        reason: String,
    },
    #[error("relation '{relation}': expected an input variant but got an output variant for connector '{connector_name}'")]
    ExpectedInputConnector {
        relation: String,
        connector_name: String,
    },
    #[error("relation '{relation}': expected an output variant but got an input variant for connector '{connector_name}'")]
    ExpectedOutputConnector {
        relation: String,
        connector_name: String,
    },
    #[error("relation '{relation}': encountered collision when using {{relation}}.{{connector_name}} as unique name: '{relation}.{connector_name}' is not unique")]
    RelationConnectorNameCollision {
        relation: String,
        connector_name: String,
    },
    #[error("relation '{relation}': failed to generate a unique connector name")]
    GeneratedUniqueConnectorNameFailed { relation: String },
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize, ToSchema)]
struct NamedConnector {
    pub name: Option<String>,

    #[serde(flatten)]
    pub config: ConnectorConfig,
}

/// Parses the properties to create a vector of connectors with optional name and configuration.
fn parse_named_connectors(
    relation: String,
    properties: &BTreeMap<String, String>,
) -> Result<Vec<NamedConnector>, ConnectorGenerationError> {
    for property in properties.keys() {
        if property != "connectors" && property != "materialized" {
            return Err(ConnectorGenerationError::PropertyDoesNotExist {
                relation,
                key: property.to_string(),
            });
        }
    }
    match properties.get("connectors") {
        Some(s) => {
            let connectors = serde_json::from_str::<Vec<NamedConnector>>(s).map_err(|e| {
                ConnectorGenerationError::InvalidPropertyValue {
                    relation: relation.clone(),
                    key: "connectors".to_string(),
                    value: s.clone(),
                    reason: format!(
                        "deserialization failed: {e} (position is within the string itself)"
                    ),
                }
            })?;
            for connector in &connectors {
                if let Some(name) = &connector.name {
                    validate_name(name).map_err(|e| {
                        ConnectorGenerationError::InvalidPropertyValue {
                            relation: relation.clone(),
                            key: "connectors".to_string(),
                            value: s.clone(),
                            reason: format!("connector name '{name}' is not valid: {e}"),
                        }
                    })?;
                }
            }
            Ok(connectors)
        }
        None => Ok(vec![]),
    }
}

/// Generates a random 10-character-long name.
fn generate_random_name() -> String {
    let chars = [
        '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'a', 'b', 'c', 'd', 'e', 'f',
    ];
    thread_rng()
        .sample_iter(Uniform::from(0..16))
        .take(10)
        .map(|i| chars[i])
        .collect()
}

/// Takes in a list of all connectors with (stream, optional given name scoped to stream, configuration).
/// Replaces the optional given name (unique to stream scope) with a unique name (unique across all streams).
/// Any optional given name will be named as {stream}.{name} -- this could technically lead to collisions.
///
/// Panics if:
/// - Optional given unique naming leads to collisions
/// - Random name generation fails to generate a unique name within a certain number of tries
fn convert_connectors_with_unique_names(
    connectors: Vec<(String, Option<String>, ConnectorConfig)>,
) -> Result<Vec<(String, String, ConnectorConfig)>, ConnectorGenerationError> {
    // Give connectors that have a given name already their unique name
    let mut unique_names = vec![];
    for (stream, connector_name, _) in &connectors {
        if let Some(name) = connector_name {
            let unique_name = format!("{stream}.{name}");
            if unique_names.contains(&Some(unique_name.clone())) {
                return Err(ConnectorGenerationError::RelationConnectorNameCollision {
                    relation: stream.clone(),
                    connector_name: name.clone(),
                });
            }
            unique_names.push(Some(unique_name));
        } else {
            unique_names.push(None);
        }
    }

    // For the connectors without a name, generate one
    let mut result = vec![];
    for i in 0..unique_names.len() {
        let stream = connectors[i].0.clone();
        if unique_names[i].is_none() {
            // Try several times to generate a unique name which is not extremely long
            let mut found = None;
            for _retry in 0..20 {
                let unique_name = format!("{}.{}", &stream, generate_random_name());
                if !unique_names.contains(&Some(unique_name.clone())) {
                    found = Some(unique_name);
                    break;
                }
            }

            // It is possible we failed to find a unique name
            match found {
                None => {
                    return Err(
                        ConnectorGenerationError::GeneratedUniqueConnectorNameFailed {
                            relation: stream.clone(),
                        },
                    );
                }
                Some(unique_name) => {
                    unique_names[i] = Some(unique_name.clone());
                }
            }
        }
        let connector_config = connectors[i].2.clone();
        result.push((
            stream,
            unique_names[i].as_ref().unwrap().clone(),
            connector_config,
        ))
    }
    Ok(result)
}

/// Program information which includes schema, input connectors and output connectors.
#[derive(Deserialize, Serialize, ToSchema, Eq, PartialEq, Debug, Clone)]
pub struct ProgramInfo {
    /// Schema of the compiled SQL program.
    pub schema: ProgramSchema,

    /// Input connectors derived from the schema.
    pub input_connectors: BTreeMap<Cow<'static, str>, InputEndpointConfig>,

    /// Output connectors derived from the schema.
    pub output_connectors: BTreeMap<Cow<'static, str>, OutputEndpointConfig>,
}

impl ProgramInfo {
    pub fn from_yaml(s: &str) -> Self {
        serde_yaml::from_str(s).unwrap()
    }

    pub fn to_yaml(&self) -> String {
        serde_yaml::to_string(self).unwrap()
    }
}

/// Generates the program info using the program schema.
/// The info includes the schema and the input/output connectors derived from it.
pub fn generate_program_info_from_schema(
    program_schema: ProgramSchema,
) -> Result<ProgramInfo, ConnectorGenerationError> {
    // Input connectors
    let mut input_connectors = vec![];
    for input_relation in &program_schema.inputs {
        for connector in parse_named_connectors(input_relation.name(), &input_relation.properties)?
        {
            match connector.config.transport {
                TransportConfig::FileInput(_)
                | TransportConfig::KafkaInput(_)
                | TransportConfig::UrlInput(_)
                | TransportConfig::S3Input(_)
                | TransportConfig::DeltaTableInput(_)
                | TransportConfig::Datagen(_) => {}
                _ => {
                    return Err(ConnectorGenerationError::ExpectedInputConnector {
                        relation: input_relation.name(),
                        connector_name: connector.name.unwrap_or("<unnamed>".to_string()),
                    });
                }
            }
            input_connectors.push((input_relation.name(), connector.name, connector.config));
        }
    }

    // Convert input connectors to ones with unique names which are turned into endpoints
    let mut inputs: BTreeMap<Cow<'static, str>, InputEndpointConfig> = BTreeMap::new();
    for (stream, connector_unique_name, connector_config) in
        convert_connectors_with_unique_names(input_connectors.clone())?
    {
        inputs.insert(
            Cow::from(connector_unique_name),
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
        for connector in
            parse_named_connectors(output_relation.name(), &output_relation.properties)?
        {
            match connector.config.transport {
                TransportConfig::FileOutput(_)
                | TransportConfig::KafkaOutput(_)
                | TransportConfig::DeltaTableOutput(_) => {}
                _ => {
                    return Err(ConnectorGenerationError::ExpectedOutputConnector {
                        relation: output_relation.name(),
                        connector_name: connector.name.unwrap_or("<unnamed>".to_string()),
                    });
                }
            }
            output_connectors.push((output_relation.name(), connector.name, connector.config));
        }
    }

    // Convert output connectors to ones with unique names which are turned into endpoints
    let mut outputs: BTreeMap<Cow<'static, str>, OutputEndpointConfig> = BTreeMap::new();
    for (stream, connector_unique_name, connector_config) in
        convert_connectors_with_unique_names(output_connectors.clone())?
    {
        outputs.insert(
            Cow::from(connector_unique_name),
            OutputEndpointConfig {
                stream: Cow::from(stream),
                query: Default::default(),
                connector_config,
            },
        );
    }
    assert_eq!(outputs.len(), output_connectors.len());

    Ok(ProgramInfo {
        schema: program_schema,
        input_connectors: inputs,
        output_connectors: outputs,
    })
}

/// Generates the pipeline configuration derived from the runtime configuration and the
/// input/output connectors derived from the program schema.
pub fn generate_pipeline_config(
    pipeline_id: PipelineId,
    runtime_config: &RuntimeConfig,
    inputs: &BTreeMap<Cow<'static, str>, InputEndpointConfig>,
    outputs: &BTreeMap<Cow<'static, str>, OutputEndpointConfig>,
) -> PipelineConfig {
    PipelineConfig {
        name: Some(format!("pipeline-{pipeline_id}")),
        global: runtime_config.clone(),
        storage_config: None, // Set by the runner based on global field
        inputs: inputs.clone(),
        outputs: outputs.clone(),
    }
}
