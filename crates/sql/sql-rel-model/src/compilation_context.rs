//! Compiler configuration and source state needed by detached statement lowering.
//! Explicit fields make Java option additions a schema change instead of silently
//! dropping them. Paths are transported as data; validation performs no I/O.
use crate::{
    TypeTable, ValidationError, ValidationLimits, catalog::nullable_schema, source::Origin,
};
use schemars::JsonSchema;
use serde::{Deserialize, Deserializer, Serialize, de};
use std::collections::BTreeMap;

type Result<T> = std::result::Result<T, ValidationError>;
fn require(condition: bool, message: &'static str) -> Result<()> {
    if condition {
        Ok(())
    } else {
        Err(ValidationError {
            path: "compilation_context".into(),
            message,
        })
    }
}
#[derive(Clone, Debug, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct CompilationContext {
    pub options: CompilerOptions,
    pub sources: SourceState,
    pub input_source: InputSource,
    pub unresolved_view_lateness: Vec<PendingLateness>,
}
#[derive(Clone, Debug, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct SourceState {
    #[serde(deserialize_with = "Option::deserialize")]
    #[schemars(required, schema_with = "nullable_schema::<String>")]
    pub file: Option<String>,
    pub program: String,
    pub lines: Vec<String>,
}
#[derive(Clone, Debug, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct PendingLateness {
    pub view: String,
    pub range: Origin,
}
#[derive(Clone, Debug, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub enum InputSource {
    /// Java's single-statement submission API leaves this marker unset.
    None,
    Stdin,
    File,
    #[serde(rename = "API")]
    Api,
}
#[derive(Clone, Debug, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct CompilerOptions {
    pub help: bool,
    #[serde(rename = "languageOptions")]
    pub language: LanguageOptions,
    #[serde(rename = "ioOptions")]
    pub io: IoOptions,
}
#[derive(Clone, Debug, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct LanguageOptions {
    pub incrementalize: bool,
    #[serde(rename = "optimizationLevel")]
    pub optimization_level: i32,
    #[serde(rename = "throwOnError")]
    pub throw_on_error: bool,
    #[serde(rename = "generateInputForEveryTable")]
    pub generate_input_for_every_table: bool,
    #[serde(rename = "ignoreOrderBy")]
    pub ignore_order_by: bool,
    #[serde(rename = "outputsAreSets")]
    pub outputs_are_sets: bool,
    pub streaming: bool,
    pub lenient: bool,
    #[serde(rename = "unrestrictedIOTypes")]
    pub unrestricted_io_types: bool,
    #[serde(rename = "unaryPlusNoop")]
    pub unary_plus_noop: bool,
    #[serde(rename = "cteViews")]
    pub cte_views: bool,
}
#[derive(Clone, Debug, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct IoOptions {
    pub anonymize: bool,
    pub format: bool,
    #[serde(rename = "loggingLevel", deserialize_with = "logging_levels")]
    pub logging_level: BTreeMap<String, String>,
    #[serde(rename = "noRust")]
    pub no_rust: bool,
    pub enterprise: bool,
    #[serde(rename = "outputFile")]
    pub output_file: String,
    #[serde(rename = "errorFile")]
    pub error_file: String,
    #[serde(rename = "emitJpeg")]
    pub emit_jpeg: bool,
    #[serde(rename = "emitPng")]
    pub emit_png: bool,
    #[serde(rename = "emitSvg")]
    pub emit_svg: bool,
    #[serde(rename = "interpreterJson")]
    pub interpreter_json: bool,
    pub gen2: bool,
    #[serde(deserialize_with = "Option::deserialize")]
    #[schemars(required, schema_with = "nullable_schema::<String>")]
    #[serde(rename = "emitPlan")]
    pub emit_plan: Option<String>,
    #[serde(deserialize_with = "Option::deserialize")]
    #[schemars(required, schema_with = "nullable_schema::<String>")]
    #[serde(rename = "emitDataflow")]
    pub emit_dataflow: Option<String>,
    #[serde(rename = "emitJsonErrors")]
    pub emit_json_errors: bool,
    #[serde(deserialize_with = "Option::deserialize")]
    #[schemars(required, schema_with = "nullable_schema::<String>")]
    #[serde(rename = "emitJsonSchema")]
    pub emit_json_schema: Option<String>,
    pub quiet: bool,
    #[serde(deserialize_with = "Option::deserialize")]
    #[schemars(required, schema_with = "nullable_schema::<String>")]
    #[serde(rename = "inputFile")]
    pub input_file: Option<String>,
    pub verbosity: i32,
    #[serde(rename = "emitHandles")]
    pub emit_handles: bool,
    #[serde(rename = "trimInputs")]
    pub trim_inputs: bool,
    pub crates: String,
    #[serde(rename = "runtimePath")]
    pub runtime_path: String,
    #[serde(rename = "correlatedColumns")]
    pub correlated_columns: bool,
    #[serde(rename = "sqlNames")]
    pub sql_names: bool,
    pub raw: bool,
    #[serde(rename = "inputCircuit")]
    pub input_circuit: bool,
    #[serde(rename = "skipCalciteOptimizations")]
    pub skip_calcite_optimizations: String,
    pub testing: bool,
    #[serde(rename = "checkSerialization")]
    pub check_serialization: bool,
    #[serde(rename = "dumpCircuits")]
    pub dump_circuits: String,
    #[serde(rename = "dumpProducerCommit")]
    pub dump_producer_commit: String,
    #[serde(rename = "dumpRuntimeAbi")]
    pub dump_runtime_abi: String,
    #[serde(rename = "writeS2")]
    pub write_s2: String,
    #[serde(rename = "readS2")]
    pub read_s2: bool,
    #[serde(rename = "writeS3")]
    pub write_s3: String,
    #[serde(rename = "readS3")]
    pub read_s3: bool,
    #[serde(rename = "nowStream")]
    pub now_stream: bool,
    #[serde(rename = "showVersion")]
    pub show_version: bool,
}

/// Serde's map implementation overwrites duplicate keys. Compiler options must
/// survive transport exactly, so reject duplicates before building the map.
fn logging_levels<'de, D>(
    deserializer: D,
) -> std::result::Result<BTreeMap<String, String>, D::Error>
where
    D: Deserializer<'de>,
{
    struct LoggingLevels;
    impl<'de> de::Visitor<'de> for LoggingLevels {
        type Value = BTreeMap<String, String>;

        fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            formatter.write_str("logging levels with unique keys")
        }

        fn visit_map<M>(self, mut entries: M) -> std::result::Result<Self::Value, M::Error>
        where
            M: de::MapAccess<'de>,
        {
            let mut levels = BTreeMap::new();
            while let Some((name, level)) = entries.next_entry::<String, String>()? {
                if levels.insert(name, level).is_some() {
                    return Err(de::Error::custom("duplicate logging level key"));
                }
            }
            Ok(levels)
        }
    }
    deserializer.deserialize_map(LoggingLevels)
}

impl CompilationContext {
    pub(crate) fn validate(&self, types: &TypeTable, limits: ValidationLimits) -> Result<()> {
        let mut bytes = limits.source_bytes;
        let mut text = |value: &str| {
            require(value.len() <= bytes, "compiler context text exceeds limit")?;
            bytes -= value.len();
            Ok(())
        };
        text(&self.sources.program)?;
        if let Some(file) = &self.sources.file {
            text(file)?;
        }
        require(
            self.sources.lines.len() <= limits.references,
            "source line count exceeds limit",
        )?;
        for line in &self.sources.lines {
            text(line)?;
            require(!line.contains('\n'), "source line contains newline")?;
        }
        let source = types.sources.first().ok_or_else(|| ValidationError {
            path: "compilation_context".into(),
            message: "missing user source",
        })?;
        require(
            source.text == self.sources.lines.join("\n"),
            "diagnostic lines differ from user source",
        )?;
        require(
            source.name == self.sources.file.as_deref().unwrap_or("(no input file)"),
            "source filename differs",
        )?;
        let io = &self.options.io;
        text(&io.output_file)?;
        text(&io.error_file)?;
        text(&io.crates)?;
        text(&io.runtime_path)?;
        text(&io.skip_calcite_optimizations)?;
        text(&io.dump_circuits)?;
        text(&io.dump_producer_commit)?;
        text(&io.dump_runtime_abi)?;
        text(&io.write_s2)?;
        text(&io.write_s3)?;
        if let Some(value) = &io.emit_plan {
            text(value)?;
        }
        if let Some(value) = &io.emit_dataflow {
            text(value)?;
        }
        if let Some(value) = &io.emit_json_schema {
            text(value)?;
        }
        if let Some(value) = &io.input_file {
            text(value)?;
        }
        require(
            io.logging_level.len() <= limits.references,
            "logging option count exceeds limit",
        )?;
        for (name, level) in &io.logging_level {
            text(name)?;
            text(level)?;
        }
        require(
            self.unresolved_view_lateness.len() <= limits.types,
            "pending lateness count exceeds limit",
        )?;
        for pending in &self.unresolved_view_lateness {
            text(&pending.view)?;
            require(
                pending.range.source().is_none_or(|source| source.0 == 0),
                "pending lateness must use user source",
            )?;
            types.origin(&pending.range, "pending lateness")?;
        }
        Ok(())
    }
}
