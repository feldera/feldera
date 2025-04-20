use clap::{Parser, Subcommand, ValueEnum, ValueHint};
use clap_complete::engine::{ArgValueCompleter, CompletionCandidate};
use std::fmt::Display;
use std::path::PathBuf;

use crate::make_client;
use feldera_rest_api::types::{CompilationProfile, ProgramConfig};

/// Autocompletion for pipeline names by trying to fetch them from the server.
fn pipeline_names(current: &std::ffi::OsStr) -> Vec<CompletionCandidate> {
    let mut completions = vec![];
    // We parse FELDERA_HOST and FELDERA_API_KEY from the environment
    // using the `try_parse_from` method.
    let cli = Cli::try_parse_from(["fda", "pipelines"]);
    if let Ok(cli) = cli {
        let client = make_client(cli.host, cli.auth, cli.timeout).unwrap();

        let r = futures::executor::block_on(async {
            client
                .list_pipelines()
                .send()
                .await
                .map(|r| r.into_inner())
                .unwrap_or_else(|_| vec![])
        });

        let current = current.to_string_lossy();
        for pipeline in r {
            if pipeline.name.starts_with(current.as_ref()) {
                completions.push(CompletionCandidate::new(pipeline.name));
            }
        }
    }

    completions
}

#[derive(Parser)]
#[command(
    name = "fda",
    about = "A CLI to interact with the Feldera REST API.",
    version
)]
pub struct Cli {
    #[command(subcommand)]
    pub command: Commands,
    /// The format in which the outputs from feldera should be displayed.
    ///
    /// Note that this flag may have no effect on some commands in case
    /// the requested output format is not supported for it.
    #[arg(
        long,
        env = "FELDERA_OUTPUT_FORMAT",
        global = true,
        help_heading = "Global Options",
        default_value = "text"
    )]
    pub format: OutputFormat,
    /// The Feldera host to connect to.
    #[arg(
        long,
        env = "FELDERA_HOST",
        value_hint = ValueHint::Url,
        global = true,
        help_heading = "Global Options",
        default_value_t = String::from("https://try.feldera.com")
    )]
    pub host: String,
    /// Which API key to use for authentication.
    ///
    /// The provided string should start with "apikey:" followed by the random characters.
    ///
    /// If not specified, a request without authentication will be used.
    #[arg(
        long,
        env = "FELDERA_API_KEY",
        global = true,
        hide_env_values = true,
        help_heading = "Global Options"
    )]
    pub auth: Option<String>,
    /// The client timeout for requests in seconds.
    ///
    /// In almost all cases you should not need to set this value, but it can
    /// be useful to limit the execution of certain commands (e.g., `query` or
    /// `logs`).
    ///
    /// By default, no timeout is set.
    #[arg(
        long,
        env = "FELDERA_REQUEST_TIMEOUT",
        global = true,
        help_heading = "Global Options"
    )]
    pub timeout: Option<u64>,
}

#[derive(ValueEnum, Clone, Copy, Debug, PartialEq)]
#[value(rename_all = "snake_case")]
pub enum OutputFormat {
    /// Return the output in a human-readable text format.
    Text,
    /// Return the output in JSON format.
    ///
    /// This usually corresponds to the exact response returned from the server.
    Json,
    /// Request the output in Arrow IPC format.
    ///
    /// This format can only be specified for SQL queries.
    ArrowIpc,
    /// Return the output in Parquet format.
    ///
    /// This format can only be specified for SQL queries.
    Parquet,
}

impl Display for OutputFormat {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let output = match self {
            OutputFormat::Text => "text",
            OutputFormat::Json => "json",
            OutputFormat::ArrowIpc => "arrow_ipc",
            OutputFormat::Parquet => "parquet",
        };
        write!(f, "{}", output)
    }
}

#[derive(Subcommand)]
pub enum Commands {
    /// List the available pipelines.
    #[command(next_help_heading = "Pipeline Commands")]
    Pipelines,
    /// Interact with a pipeline.
    ///
    /// If no sub-command is specified retrieves all configuration data for the pipeline.
    #[command(flatten)]
    Pipeline(PipelineAction),
    /// Manage API keys.
    Apikey {
        #[command(subcommand)]
        action: ApiKeyActions,
    },
    /// Debugging tools.
    Debug {
        #[command(subcommand)]
        action: DebugActions,
    },
}

#[derive(Subcommand)]
pub enum ApiKeyActions {
    /// List available API keys
    List,
    /// Create a new API key
    Create {
        /// The name of the API key to create
        name: String,
    },
    /// Delete an existing API key
    #[clap(aliases = &["del"])]
    Delete {
        /// The name of the API key to delete
        name: String,
    },
}

#[derive(Subcommand)]
pub enum DebugActions {
    /// Print a MessagePack file, such as `steps.bin` in a checkpoint directory,
    /// to stdout.
    MsgpCat {
        /// The MessagePack file to read.
        #[arg(value_hint = ValueHint::FilePath)]
        path: PathBuf,
    },
}

/// A list of possible configuration options.
#[derive(ValueEnum, Clone, Copy, Debug)]
#[value(rename_all = "snake_case")]
pub enum RuntimeConfigKey {
    Workers,
    Storage,
    FaultTolerance,
    CheckpointInterval,
    CpuProfiler,
    Tracing,
    TracingEndpointJaeger,
    MinBatchSizeRecords,
    MaxBufferingDelayUsecs,
    CpuCoresMin,
    CpuCoresMax,
    MemoryMbMin,
    MemoryMbMax,
    StorageMbMax,
    StorageClass,
    MinStorageBytes,
    ClockResolutionUsecs,
}

#[derive(ValueEnum, Clone, Copy, Debug)]
#[value(rename_all = "snake_case")]
pub enum Profile {
    Dev,
    Unoptimized,
    Optimized,
}

#[allow(clippy::from_over_into)]
impl Into<CompilationProfile> for Profile {
    fn into(self) -> CompilationProfile {
        match self {
            Profile::Dev => CompilationProfile::Dev,
            Profile::Unoptimized => CompilationProfile::Unoptimized,
            Profile::Optimized => CompilationProfile::Optimized,
        }
    }
}

#[allow(clippy::from_over_into)]
impl Into<ProgramConfig> for Profile {
    fn into(self) -> ProgramConfig {
        ProgramConfig {
            profile: Some(self.into()),
            cache: true,
        }
    }
}

#[derive(Subcommand)]
pub enum PipelineAction {
    /// Create a new pipeline.
    Create {
        /// The name of the pipeline.
        #[arg(value_hint = ValueHint::Other, add = ArgValueCompleter::new(pipeline_names))]
        name: String,
        /// A path to a file containing the SQL code.
        ///
        /// If no path is provided, the pipeline will be created with an empty program.
        /// See the `stdin` flag for reading from stdin instead.
        #[arg(value_hint = ValueHint::FilePath, conflicts_with = "stdin")]
        program_path: Option<String>,
        /// A path to a file containing the Rust UDF functions.
        #[arg(short = 'u', long, value_hint = ValueHint::FilePath, conflicts_with = "stdin")]
        udf_rs: Option<String>,
        /// A path to the TOML file containing the dependencies for the UDF functions.
        #[arg(short = 't', long, value_hint = ValueHint::FilePath, conflicts_with = "stdin")]
        udf_toml: Option<String>,
        /// The compilation profile to use.
        #[arg(default_value = "optimized")]
        profile: Profile,
        /// Read the program code from stdin.
        ///
        /// EXAMPLES:
        ///
        /// * cat program.sql | fda create p1 -s
        /// * echo "SELECT 1" | fda create p2 -s
        /// * fda program get p2 | fda create p3 -s
        #[arg(
            verbatim_doc_comment,
            short = 's',
            long,
            default_value_t = false,
            conflicts_with = "program_path"
        )]
        stdin: bool,
    },
    /// Start a pipeline.
    ///
    /// If the pipeline is compiling it will wait for the compilation to finish.
    Start {
        /// The name of the pipeline.
        #[arg(value_hint = ValueHint::Other, add = ArgValueCompleter::new(pipeline_names))]
        name: String,
        /// Force the recompilation of the pipeline before starting.
        ///
        /// This is useful for dev purposes in case the Feldera source-code has changed.
        #[arg(long, short = 'r', default_value_t = false)]
        recompile: bool,
        /// Don't wait for pipeline to reach the status before returning.
        #[arg(long, short = 'n', default_value_t = false)]
        no_wait: bool,
    },
    /// Checkpoint a fault-tolerant pipeline.
    Checkpoint {
        /// The name of the pipeline.
        #[arg(value_hint = ValueHint::Other, add = ArgValueCompleter::new(pipeline_names))]
        name: String,
    },
    /// Pause a pipeline.
    Pause {
        /// The name of the pipeline.
        #[arg(value_hint = ValueHint::Other, add = ArgValueCompleter::new(pipeline_names))]
        name: String,
        /// Don't wait for pipeline to reach the status before returning.
        #[arg(long, short = 'n', default_value_t = false)]
        no_wait: bool,
    },
    /// Shutdown a pipeline, then restart it.
    ///
    /// This is a shortcut for calling `fda shutdown p1 && fda start p1`.
    Restart {
        /// The name of the pipeline.
        #[arg(value_hint = ValueHint::Other, add = ArgValueCompleter::new(pipeline_names))]
        name: String,
        /// Force the recompilation of the pipeline before starting.
        ///
        /// This is useful for dev purposes in case the Feldera source-code has changed.
        #[arg(long, short = 'r', default_value_t = false)]
        recompile: bool,
        /// Don't wait for pipeline to reach the status before returning.
        #[arg(long, short = 'n', default_value_t = false)]
        no_wait: bool,
    },
    /// Shutdown a pipeline.
    #[clap(aliases = &["stop"])]
    Shutdown {
        /// The name of the pipeline.
        #[arg(value_hint = ValueHint::Other, add = ArgValueCompleter::new(pipeline_names))]
        name: String,
        /// Don't wait for pipeline to reach the status before returning.
        #[arg(long, short = 'n', default_value_t = false)]
        no_wait: bool,
    },
    /// Retrieve the entire state of a pipeline.
    ///
    /// EXAMPLES:
    ///
    /// - `fda status test | jq .program_info.schema`
    ///
    /// - `fda status test | jq .program_info.input_connectors`
    ///
    /// - `fda status test | jq .deployment_config`
    Status {
        /// The name of the pipeline.
        #[arg(value_hint = ValueHint::Other, add = ArgValueCompleter::new(pipeline_names))]
        name: String,
    },
    /// Retrieve the runtime statistics of a pipeline.
    #[clap(aliases = &["statistics"])]
    Stats {
        /// The name of the pipeline.
        #[arg(value_hint = ValueHint::Other, add = ArgValueCompleter::new(pipeline_names))]
        name: String,
    },
    /// Retrieve the logs of a pipeline.
    #[clap(aliases = &["log"])]
    Logs {
        /// The name of the pipeline.
        #[arg(value_hint = ValueHint::Other, add = ArgValueCompleter::new(pipeline_names))]
        name: String,
        /// Watch the log endpoint and emit new pipeline log messages as they are written.
        ///
        /// When `true`, the command will listen to pipeline logs until the pipeline terminates or
        /// the command is interrupted by the user. When `false`, the command outputs pipeline logs
        /// accumulated so far and exits.
        #[arg(long, short = 'w', default_value_t = false)]
        watch: bool,
    },
    /// Interact with the program of the pipeline.
    ///
    /// If no sub-command is specified retrieves the program.
    Program {
        #[command(subcommand)]
        action: ProgramAction,
    },
    /// Retrieve the runtime configuration of a pipeline.
    #[clap(aliases = &["cfg"])]
    Config {
        /// The name of the pipeline.
        #[arg(value_hint = ValueHint::Other, add = ArgValueCompleter::new(pipeline_names))]
        name: String,
    },
    /// Update the runtime configuration of a pipeline.
    #[clap(aliases = &["set-cfg"])]
    SetConfig {
        /// The name of the pipeline.
        #[arg(value_hint = ValueHint::Other, add = ArgValueCompleter::new(pipeline_names))]
        name: String,
        /// The key of the configuration to update.
        key: RuntimeConfigKey,
        /// The new value for the configuration.
        value: String,
    },
    /// Delete a pipeline.
    #[clap(aliases = &["del"])]
    Delete {
        /// The name of the pipeline.
        #[arg(value_hint = ValueHint::Other, add = ArgValueCompleter::new(pipeline_names))]
        name: String,
    },
    /// Control an input connector belonging to a table of a pipeline.
    Connector {
        /// The name of the pipeline.
        #[arg(value_hint = ValueHint::Other, add = ArgValueCompleter::new(pipeline_names))]
        name: String,
        /// The name of the table or view.
        relation_name: String,
        /// The name of the connector.
        connector_name: String,
        #[command(subcommand)]
        action: ConnectorAction,
    },
    /// Obtains a heap profile for a pipeline.
    ///
    /// By default, or with `--pprof`, this command retrieves the heap profile
    /// to a temporary file and then displays it with `pprof`. With `--output`,
    /// this command instead writes the heap profile to the specified file.
    ///
    /// Get `pprof` from <https://github.com/google/pprof>. There is at least
    /// one other program named `pprof` that is unrelated and will not work.
    HeapProfile {
        /// The name of the pipeline.
        #[arg(value_hint = ValueHint::Other, add = ArgValueCompleter::new(pipeline_names))]
        name: String,
        /// The `pprof` command to run as a subprocess. The name of the `pprof`
        /// file will be provided as an additional command-line argument.
        #[arg(long, short = 'p', default_value = "pprof -http :")]
        pprof: String,
        /// The file to write the profile to.
        #[arg(value_hint = ValueHint::FilePath, long, short = 'o')]
        output: Option<PathBuf>,
    },
    /// Enter the ad-hoc SQL shell for a pipeline.
    Shell {
        /// The name of the pipeline.
        #[arg(value_hint = ValueHint::Other, add = ArgValueCompleter::new(pipeline_names))]
        name: String,
        /// Start the pipeline before entering the shell.
        #[arg(long, short = 's', default_value_t = false)]
        start: bool,
    },
    /// Execute an ad-hoc query against a pipeline and return the result.
    #[clap(aliases = &["exec"])]
    Query {
        /// The name of the pipeline.
        #[arg(value_hint = ValueHint::Other, add = ArgValueCompleter::new(pipeline_names))]
        name: String,

        /// The SQL query to execute against the pipeline.
        ///
        /// EXAMPLES:
        ///
        /// * fda exec p1 "SELECT 1;"
        #[arg(verbatim_doc_comment, conflicts_with = "stdin")]
        sql: Option<String>,

        /// Read the SQL query from stdin.
        ///
        /// EXAMPLES:
        ///
        /// * cat query.sql | fda exec p1 -s
        /// * echo "SELECT 1" | fda exec p1 -s
        #[arg(
            verbatim_doc_comment,
            short = 's',
            long,
            default_value_t = false,
            conflicts_with = "sql"
        )]
        stdin: bool,
    },
}

#[derive(Subcommand)]
pub enum ProgramAction {
    /// Retrieve the program code.
    ///
    /// By default, this returns the SQL code, but you can use the flags to retrieve
    /// the Rust UDF code instead.
    Get {
        /// The name of the pipeline.
        #[arg(value_hint = ValueHint::Other, add = ArgValueCompleter::new(pipeline_names))]
        name: String,
        /// Retrieve the Rust UDF code.
        #[arg(short = 'u', long, default_value_t = false)]
        udf_rs: bool,
        /// Retrieve the TOML dependencies file for the UDF code.
        #[arg(short = 't', long, default_value_t = false)]
        udf_toml: bool,
    },
    /// Sets a new program.
    Set {
        /// The name of the pipeline.
        #[arg(value_hint = ValueHint::Other, add = ArgValueCompleter::new(pipeline_names))]
        name: String,
        /// A path to a file containing the SQL code.
        ///
        /// See the `stdin` flag for reading from stdin instead.
        #[arg(value_hint = ValueHint::FilePath, conflicts_with = "stdin")]
        program_path: Option<String>,
        /// A path to a file containing the Rust UDF functions.
        #[arg(short = 'u', long, value_hint = ValueHint::FilePath, conflicts_with = "stdin")]
        udf_rs: Option<String>,
        /// A path to the TOML file containing the dependencies for the UDF functions.
        #[arg(short = 't', long, value_hint = ValueHint::FilePath, conflicts_with = "stdin")]
        udf_toml: Option<String>,
        /// Read the SQL program code from stdin.
        ///
        /// EXAMPLES:
        ///
        /// * cat program.sql | fda program set p1 -s
        /// * echo "SELECT 1" | fda program set p1 -s
        /// * fda program get p2 | fda program set p1 -s
        #[arg(verbatim_doc_comment, short = 's', long, default_value_t = false)]
        stdin: bool,
    },
    /// Retrieve the configuration of the program.
    #[clap(aliases = &["cfg"])]
    Config {
        /// The name of the pipeline.
        #[arg(value_hint = ValueHint::Other, add = ArgValueCompleter::new(pipeline_names))]
        name: String,
    },
    /// Set the configuration of the program.
    #[clap(aliases = &["set-cfg"])]
    SetConfig {
        /// The name of the pipeline.
        #[arg(value_hint = ValueHint::Other, add = ArgValueCompleter::new(pipeline_names))]
        name: String,
        /// The updated configuration for the pipeline.
        ///
        /// The profile accepts the following values:
        /// `dev`, `unoptimized`, `optimized`
        profile: Profile,
    },
    /// Retrieve the compilation status of the program.
    Status {
        /// The name of the pipeline.
        #[arg(value_hint = ValueHint::Other, add = ArgValueCompleter::new(pipeline_names))]
        name: String,
    },
    /// Return compile-time information about the program.
    Info {
        /// The name of the pipeline.
        #[arg(value_hint = ValueHint::Other, add = ArgValueCompleter::new(pipeline_names))]
        name: String,
    },
}

#[derive(Subcommand)]
pub enum ConnectorAction {
    Start,
    #[clap(aliases = &["stop"])]
    Pause,
    #[clap(aliases = &["status"])]
    Stats,
}
