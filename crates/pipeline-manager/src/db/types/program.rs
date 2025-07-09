use crate::db::error::DBError;
use crate::db::types::pipeline::PipelineId;
use crate::db::types::utils::validate_name;
use clap::Parser;
use feldera_types::config::{
    ConnectorConfig, InputEndpointConfig, OutputEndpointConfig, PipelineConfig, RuntimeConfig,
    TransportConfig,
};
use feldera_types::program_schema::{ProgramSchema, PropertyValue, SourcePosition, SqlIdentifier};
use log::error;
use rand::distributions::Uniform;
use rand::{thread_rng, Rng};
use serde::{Deserialize, Serialize};
use std::borrow::Cow;
use std::collections::BTreeMap;
use std::fmt;
use std::fmt::{Display, Formatter};
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
            ConnectorGenerationError::GeneratedUniqueConnectorNameFailed { position, .. } => {
                position
            }
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
    current_status: &ProgramStatus,
    new_status: &ProgramStatus,
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
            current: *current_status,
            transition_to: *new_status,
        })
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
}

impl Default for ProgramConfig {
    fn default() -> Self {
        Self {
            profile: None,
            cache: true,
        }
    }
}

#[derive(ThisError, Serialize, Deserialize, Debug, ToSchema)]
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
    #[error("relation '{relation}': failed to generate a unique connector name")]
    GeneratedUniqueConnectorNameFailed {
        position: SourcePosition,
        relation: String,
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
    connectors: Vec<(String, Option<String>, ConnectorConfig, PropertyValue)>,
) -> Result<Vec<(String, String, ConnectorConfig)>, ConnectorGenerationError> {
    // Give connectors that have a given name already their unique name
    let mut unique_names = vec![];
    for (stream, connector_name, _, origin_value) in &connectors {
        if let Some(name) = connector_name {
            let unique_name = format!("{}.{name}", SqlIdentifier::from(&stream).name());
            if unique_names.contains(&Some(unique_name.clone())) {
                return Err(ConnectorGenerationError::RelationConnectorNameCollision {
                    position: origin_value.value_position,
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
                let unique_name = format!(
                    "{}.{}",
                    SqlIdentifier::from(&stream).name(),
                    generate_random_name()
                );
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
                            position: connectors[i].3.value_position,
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

/// Program information is the output of the SQL compiler.
///
/// It includes information needed for Rust compilation (e.g., generated Rust code)
/// as well as only for runtime (e.g., schema, input/output connectors).
#[derive(Deserialize, Serialize, Eq, PartialEq, Debug, Clone, ToSchema)]
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
    pub dataflow: serde_json::Value,

    /// Input connectors derived from the schema.
    pub input_connectors: BTreeMap<Cow<'static, str>, InputEndpointConfig>,

    /// Output connectors derived from the schema.
    pub output_connectors: BTreeMap<Cow<'static, str>, OutputEndpointConfig>,
}

/// Generates the program info using the program schema.
/// The info includes the schema and the input/output connectors derived from it.
pub fn generate_program_info(
    program_schema: ProgramSchema,
    main_rust: String,
    udf_stubs: String,
    dataflow: serde_json::Value,
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
            match connector.config.transport {
                TransportConfig::FileInput(_)
                | TransportConfig::NatsInput(_)
                | TransportConfig::KafkaInput(_)
                | TransportConfig::PubSubInput(_)
                | TransportConfig::UrlInput(_)
                | TransportConfig::S3Input(_)
                | TransportConfig::DeltaTableInput(_)
                | TransportConfig::PostgresInput(_)
                | TransportConfig::IcebergInput(_)
                | TransportConfig::Datagen(_)
                | TransportConfig::Nexmark(_) => {}
                _ => {
                    return Err(ConnectorGenerationError::ExpectedInputConnector {
                        position: origin_value.value_position,
                        relation: input_relation.name.sql_name(),
                        connector_name: connector.name.unwrap_or("<unnamed>".to_string()),
                    });
                }
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
        let (connectors, origin_value) =
            parse_named_connectors(output_relation.name.clone(), &output_relation.properties)?;
        for connector in connectors {
            let origin_value = origin_value
                .clone()
                .expect("Origin value cannot be None if connectors is non-empty");
            match connector.config.transport {
                TransportConfig::FileOutput(_)
                | TransportConfig::PostgresOutput(_)
                | TransportConfig::KafkaOutput(_)
                | TransportConfig::DeltaTableOutput(_)
                | TransportConfig::RedisOutput(_) => {}
                _ => {
                    return Err(ConnectorGenerationError::ExpectedOutputConnector {
                        position: origin_value.value_position,
                        relation: output_relation.name.sql_name(),
                        connector_name: connector.name.unwrap_or("<unnamed>".to_string()),
                    });
                }
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
    for (stream, connector_unique_name, connector_config) in
        convert_connectors_with_unique_names(output_connectors.clone())?
    {
        outputs.insert(
            Cow::from(connector_unique_name),
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
