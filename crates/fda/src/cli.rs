use clap::{Parser, Subcommand, ValueEnum, ValueHint};
use clap_complete::engine::{ArgValueCompleter, CompletionCandidate};

use crate::cd::types::{CompilationProfile, ProgramConfig};
use crate::make_client;

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
#[command(name = "fda", about = "A CLI to interact with the Feldera REST API.")]
pub struct Cli {
    #[command(subcommand)]
    pub command: Commands,
    /// The format in which the output should be displayed.
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
    /// In almost all cases you should not need to change this value.
    /// It can be helpful to increase this if you want to evaluate long-running
    /// ad-hoc queries in the shell.
    #[arg(
        long,
        env = "FELDERA_REQUEST_TIMEOUT",
        global = true,
        help_heading = "Global Options",
        default_value_t = 120
    )]
    pub timeout: u64,
}

#[derive(ValueEnum, Clone, Copy, Debug)]
#[value(rename_all = "snake_case")]
pub enum OutputFormat {
    Text,
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

/// A list of possible configuration options.
#[derive(ValueEnum, Clone, Copy, Debug)]
#[value(rename_all = "snake_case")]
pub enum RuntimeConfigKey {
    Workers,
    Storage,
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
        /// See the `stdin` flag for reading from stdin instead.
        #[arg(value_hint = ValueHint::FilePath)]
        program_path: String,
        /// The compilation profile to use.
        #[arg(default_value = "optimized")]
        profile: Profile,
        /// Read the program code from stdin.
        ///
        /// EXAMPLES:
        ///
        /// * cat program.sql | fda create p1 -s -
        /// * echo "SELECT 1" | fda create p2 -s - dev
        /// * fda program p2 | fda create p3 -s -
        #[arg(verbatim_doc_comment, short = 's', long, default_value_t = false)]
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
    },
    /// Pause a pipeline.
    Pause {
        /// The name of the pipeline.
        #[arg(value_hint = ValueHint::Other, add = ArgValueCompleter::new(pipeline_names))]
        name: String,
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
    },
    /// Shutdown a pipeline.
    #[clap(aliases = &["stop"])]
    Shutdown {
        /// The name of the pipeline.
        #[arg(value_hint = ValueHint::Other, add = ArgValueCompleter::new(pipeline_names))]
        name: String,
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
    /// Interact with the program of the pipeline.
    ///
    /// If no sub-command is specified retrieves the program.
    Program {
        /// The name of the pipeline.
        #[arg(value_hint = ValueHint::Other, add = ArgValueCompleter::new(pipeline_names))]
        name: String,
        #[command(subcommand)]
        action: Option<ProgramAction>,
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
    /// Control an endpoint of a pipeline.
    Endpoint {
        /// The name of the pipeline.
        #[arg(value_hint = ValueHint::Other, add = ArgValueCompleter::new(pipeline_names))]
        name: String,
        /// The name of the pipeline endpoint.
        endpoint_name: String,
        #[command(subcommand)]
        action: EndpointAction,
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
    /// Set a new SQL program.
    Set {
        /// A path to a file containing the SQL code.
        ///
        /// See the `stdin` flag for reading from stdin instead.
        #[arg(value_hint = ValueHint::FilePath)]
        program_path: String,

        /// Read the program code from stdin.
        ///
        /// EXAMPLES:
        ///
        /// * cat program.sql | fda program p1 set -s -
        /// * echo "SELECT 1" | fda program p1 set -s -
        /// * fda program p2 | fda program p1 set -s -
        #[arg(verbatim_doc_comment, short = 's', long, default_value_t = false)]
        stdin: bool,
    },
    /// Retrieve the configuration of the program.
    #[clap(aliases = &["cfg"])]
    Config,
    /// Set the configuration of the program.
    #[clap(aliases = &["set-cfg"])]
    SetConfig {
        /// The updated configuration for the pipeline.
        ///
        /// The profile accepts the following values:
        /// `dev`, `unoptimized`, `optimized`
        profile: Profile,
    },
    /// Retrieve the compilation status of the program.
    Status,
}

#[derive(Subcommand)]
pub enum EndpointAction {
    Start,
    Pause,
}
