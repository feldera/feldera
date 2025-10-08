use clap::{Args, Parser, Subcommand, ValueEnum, ValueHint};
use clap_complete::engine::{ArgValueCompleter, CompletionCandidate};
use std::fmt::Display;
use std::path::PathBuf;

use crate::make_client;
use feldera_rest_api::types::CompilationProfile;

/// Autocompletion for pipeline names by trying to fetch them from the server.
fn pipeline_names(current: &std::ffi::OsStr) -> Vec<CompletionCandidate> {
    let mut completions = vec![];
    // We parse FELDERA_HOST and FELDERA_API_KEY from the environment
    // using the `try_parse_from` method.
    let cli = Cli::try_parse_from(["fda", "pipelines"]);
    if let Ok(cli) = cli {
        let client = make_client(cli.host, cli.insecure, cli.auth, cli.timeout).unwrap();

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
    /// Accept invalid HTTPS certificates.
    #[arg(
        short = 'k',
        long,
        env = "FELDERA_TLS_INSECURE",
        global = true,
        default_value_t = false,
        help_heading = "Global Options"
    )]
    pub insecure: bool,
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
    /// Returns the output in Prometheus format.
    ///
    /// This format can only be specified for the `metrics` command.
    Prometheus,
    /// Returns a hash of the result instead of the result.
    ///
    /// This format can only be specified for ad-hoc SQL queries.
    /// The output in this case is a single string/line containing a SHA256 hash.
    Hash,
}

impl Display for OutputFormat {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let output = match self {
            OutputFormat::Text => "text",
            OutputFormat::Json => "json",
            OutputFormat::ArrowIpc => "arrow_ipc",
            OutputFormat::Parquet => "parquet",
            OutputFormat::Prometheus => "prometheus",
            OutputFormat::Hash => "hash",
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

    /// Reads metrics from a file and prints them in an easier-to-read form.
    Metrics {
        /// The Prometheus metrics file to read.
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
    Logging,
    HttpWorkers,
    IoWorkers,
    DevTweaks,
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
        /// Override the runtime version of the pipeline.
        ///
        /// If not specified, the default version of the platform will be used.
        ///
        /// Note: This feature needs to be enabled in the platform configuration
        /// and is still in development. Use for testing purposes only.
        #[arg(long, short = 'r', env = "FELDERA_RUNTIME_VERSION")]
        runtime_version: Option<String>,
        /// The compilation profile to use.
        #[arg(default_value = "optimized")]
        profile: CompilationProfile,
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
        /// Initial desired runtime status once the pipeline is started.
        #[arg(long, short = 'i', default_value = "running")]
        initial: String,
    },
    /// Checkpoint a fault-tolerant pipeline.
    Checkpoint {
        /// The name of the pipeline.
        #[arg(value_hint = ValueHint::Other, add = ArgValueCompleter::new(pipeline_names))]
        name: String,
        /// Don't wait for pipeline to complete the checkpoint.
        #[arg(long, short = 'n', default_value_t = false)]
        no_wait: bool,
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
    /// Resume a pipeline.
    Resume {
        /// The name of the pipeline.
        #[arg(value_hint = ValueHint::Other, add = ArgValueCompleter::new(pipeline_names))]
        name: String,
        /// Don't wait for pipeline to reach the status before returning.
        #[arg(long, short = 'n', default_value_t = false)]
        no_wait: bool,
    },
    /// Stop a pipeline, then start it again.
    ///
    /// This is a shortcut for calling `fda stop p1 && fda start p1`.
    Restart {
        /// The name of the pipeline.
        #[arg(value_hint = ValueHint::Other, add = ArgValueCompleter::new(pipeline_names))]
        name: String,
        /// Force the recompilation of the pipeline before starting.
        ///
        /// This is useful for dev purposes in case the Feldera source-code has changed.
        #[arg(long, short = 'r', default_value_t = false)]
        recompile: bool,
        /// Checkpoint the pipeline before restarting it.
        #[arg(long, short = 'c', default_value_t = false)]
        checkpoint: bool,
        /// Don't wait for pipeline to reach the status before returning.
        #[arg(long, short = 'n', default_value_t = false)]
        no_wait: bool,
        /// Initial desired runtime status once the pipeline is restarted.
        #[arg(long, short = 'i', default_value = "running")]
        initial: String,
    },
    /// Stop a pipeline.
    #[clap(aliases = &["shutdown"])]
    Stop {
        /// The name of the pipeline.
        #[arg(value_hint = ValueHint::Other, add = ArgValueCompleter::new(pipeline_names))]
        name: String,
        /// Checkpoint the pipeline before stopping it.
        #[arg(long, short = 'c', default_value_t = false)]
        checkpoint: bool,
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
    /// Retrieve the pipeline metrics.
    ///
    /// Metrics are available in `json` and `prometheus` output formats.
    Metrics {
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
    /// Recompile a pipeline with the Feldera runtime version included in the
    /// currently installed Feldera platform.
    UpdateRuntime {
        /// The name of the pipeline.
        #[arg(value_hint = ValueHint::Other, add = ArgValueCompleter::new(pipeline_names))]
        name: String,
    },
    /// Delete a pipeline.
    #[clap(aliases = &["del"])]
    Delete {
        /// The name of the pipeline.
        #[arg(value_hint = ValueHint::Other, add = ArgValueCompleter::new(pipeline_names))]
        name: String,
        /// Clears any associated storage first to force deletion of the pipeline.
        ///
        /// EXAMPLES:
        ///
        /// - fda delete --force my-pipeline
        ///
        /// Is equivalent to:
        ///
        /// - fda clear my-pipeline && fda delete my-pipeline
        #[arg(long, short = 'f')]
        force: bool,
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
    /// Obtains a circuit profile for a pipeline.
    CircuitProfile {
        /// The name of the pipeline.
        #[arg(value_hint = ValueHint::Other, add = ArgValueCompleter::new(pipeline_names))]
        name: String,
        /// The ZIP file to write the profile to.
        #[arg(value_hint = ValueHint::FilePath, long, short = 'o')]
        output: Option<PathBuf>,
    },
    /// Download a support bundle which contains debug information about the pipeline.
    SupportBundle {
        /// The name of the pipeline.
        #[arg(value_hint = ValueHint::Other, add = ArgValueCompleter::new(pipeline_names))]
        name: String,
        /// The ZIP file to write the bundle to.
        #[arg(value_hint = ValueHint::FilePath, long, short = 'o')]
        output: Option<PathBuf>,
        /// Skip circuit profile collection.
        #[arg(long)]
        no_circuit_profile: bool,
        /// Skip heap profile collection.
        #[arg(long)]
        no_heap_profile: bool,
        /// Skip metrics collection.
        #[arg(long)]
        no_metrics: bool,
        /// Skip logs collection.
        #[arg(long)]
        no_logs: bool,
        /// Skip stats collection.
        #[arg(long)]
        no_stats: bool,
        /// Skip pipeline configuration collection.
        #[arg(long)]
        no_pipeline_config: bool,
        /// Skip system configuration collection.
        #[arg(long)]
        no_system_config: bool,
    },
    /// Enter the ad-hoc SQL shell for a pipeline.
    Shell {
        /// The name of the pipeline.
        #[arg(value_hint = ValueHint::Other, add = ArgValueCompleter::new(pipeline_names))]
        name: String,
        /// Start the pipeline before entering the shell.
        #[arg(long, short = 's', default_value_t = false)]
        start: bool,
        /// Initial desired runtime status once the pipeline is started
        /// (ignored unless `--start` is provided).
        #[arg(long, short = 'i', default_value = "running")]
        initial: String,
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
    /// Generate a completion token for a SQL table/connector pair in a pipeline.
    #[clap(aliases = &["generate-token", "generate-completion-token"])]
    CompletionToken {
        /// The name of the pipeline.
        #[arg(value_hint = ValueHint::Other, add = ArgValueCompleter::new(pipeline_names))]
        name: String,
        /// The name of the SQL table to generate the token for.
        table: String,
        /// The name of the connector to generate the token for.
        ///
        /// This can be read from the `name` field in the connector config.
        connector: String,
    },
    /// Check the status of a completion token for a pipeline.
    #[clap(aliases = &["check-token", "check-completion-token"])]
    CompletionStatus {
        /// The name of the pipeline.
        #[arg(value_hint = ValueHint::Other, add = ArgValueCompleter::new(pipeline_names))]
        name: String,
        /// The token to check the status for.
        ///
        /// A token can be optained by running `fda completion-token`.
        /// Or when ingesting data over HTTP.
        token: String,
    },
    /// Start a new transaction.
    #[clap(aliases = &["transaction-start"])]
    StartTransaction {
        /// The name of the pipeline.
        #[arg(value_hint = ValueHint::Other, add = ArgValueCompleter::new(pipeline_names))]
        name: String,
    },
    /// Commit the current transaction.
    ///
    /// Commits the currently active transaction for the specified pipeline.
    /// Optionally waits for the commit to complete.
    #[clap(aliases = &["commit", "transaction-commit"])]
    CommitTransaction {
        /// The name of the pipeline.
        #[arg(value_hint = ValueHint::Other, add = ArgValueCompleter::new(pipeline_names))]
        name: String,
        /// The transaction ID to verify against the current active transaction.
        /// If provided, the function verifies that the currently active transaction matches this ID.
        #[arg(long = "tid", short = 't')]
        transaction_id: Option<u64>,
        /// Don't wait for the transaction to commit before returning.
        #[arg(long, short = 'n', default_value_t = false)]
        no_wait: bool,
    },
    /// Clear the storage resources of a pipeline.
    ///
    /// Note that the pipeline must be stopped before clearing its storage resources.
    Clear {
        /// The name of the pipeline.
        #[arg(value_hint = ValueHint::Other, add = ArgValueCompleter::new(pipeline_names))]
        name: String,
        /// Don't wait for pipeline storage to clear before returning.
        #[arg(long, short = 'n', default_value_t = false)]
        no_wait: bool,
    },
    /// Benchmark the performance of a pipeline.
    ///
    /// This command will perform the following steps
    /// 1. ensure the benchmark is compiled with the latest platform version
    /// 2. run the pipeline & record stats over time
    /// 3. post-process and check recorded statistics
    /// 4. aggregate basic performance metrics and output them in the specified format
    Bench {
        #[command(flatten)]
        args: BenchmarkArgs,
    },
}

#[derive(Args, Debug)]
pub(crate) struct BenchmarkArgs {
    /// The name of the pipeline.
    #[arg(value_hint = ValueHint::Other, add = ArgValueCompleter::new(pipeline_names))]
    pub name: String,
    /// Duration of the benchmark in seconds.
    ///
    /// If not specified, the benchmark will run until the pipeline indicates it has processed all input.
    #[arg(long, short = 'd')]
    pub duration: Option<u64>,

    /// Do not recompile the pipeline before starting.
    ///
    /// If set to true, one might end up with a pipeline that's not
    /// compiled with the latest feldera runtime.
    #[arg(long, short = 'n', default_value_t = false)]
    pub no_recompile: bool,

    /// Do not wrap the benchmark in a transaction.
    #[arg(long, default_value_t = false)]
    pub no_transaction: bool,

    /// If set upload results to feldera benchmark host.
    ///
    /// For development purposes, you most likely don't want to set this to true.
    ///
    /// Requires `benchmark_token` token to be set.
    #[arg(long, short = 'u', default_value_t = false)]
    pub upload: bool,

    /// Slug or UUID of the project to add results to when uploading
    /// (requires: `--upload`).
    #[arg(long, short = 'p', env = "BENCHER_PROJECT")]
    pub project: String,

    /// Branch name, slug, or UUID. By default it will be set to `main`
    /// (requires: `--upload`).
    #[arg(long, default_value_t = String::from("main"))]
    pub branch: String,

    /// Use the specified branch name as the start point for `branch`
    /// (requires: `--upload`).
    ///
    /// - If `branch` already exists and the start point is different, a new
    ///   branch will be created.
    #[arg(long)]
    pub start_point: Option<String>,

    /// Use the specified full `git` hash as the start point for `branch`
    /// (requires: `--start-point` and `--upload`).
    ///
    /// - If `start_point` already exists and the start point hash is different,
    ///   a new branch will be created
    #[arg(long)]
    pub start_point_hash: Option<String>,

    /// The maximum number of historical branch versions to include
    /// (requires: `--start-point` and `--upload`).
    ///
    /// Versions beyond this number will be omitted.
    #[arg(long, default_value_t = 255)]
    pub start_point_max_versions: u32,

    /// Clone thresholds from the start point branch
    /// (requires: `--branch-start-point` and `--upload`).
    #[arg(long)]
    pub start_point_clone_thresholds: bool,

    /// Reset the branch head to an empty state
    /// (requires: `--branch-start-point` and `--upload`).
    ///
    /// If `start_point` is specified, the new branch head will begin at that start point.
    /// Otherwise, the branch head will be reset to an empty state
    #[arg(long)]
    pub start_point_reset: bool,

    /// Where to upload benchmark results to
    /// (requires: `--upload`).
    #[arg(
        long,
        short = 'b',
        env = "BENCHER_HOST",
        value_hint = ValueHint::Url,
        default_value_t = String::from("https://benchmarks.feldera.io/")
    )]
    pub benchmark_host: String,

    /// Which API key to use for authentication with benchmarks server
    /// (requires: `--upload`).
    #[arg(long, short = 't', env = "BENCHER_API_TOKEN", hide_env_values = true)]
    pub benchmark_token: Option<String>,
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
        ///
        /// If not specified, the optimized profile will be used.
        #[arg(short = 'p', long)]
        profile: Option<CompilationProfile>,
        /// Override the runtime version of the pipeline.
        ///
        /// EXPERIMENTAL:
        ///
        /// This feature is still in development and may change in future releases.
        /// Use for testing purposes only. Note that currently no compatibility
        /// guarantees are provided in case the runtime version does not match
        /// the deployed platform version.
        ///
        /// EXAMPLES:
        ///
        ///  - --runtime-version v0.100.0
        ///  - --runtime-version 2880dd6fe206d10c966cc23868ee41a3c9e4e543
        ///    (valid git commit hash of feldera/feldera main branch)
        ///
        /// If not specified, the default version will be used.
        #[arg(verbatim_doc_comment, short = 'r', long)]
        runtime_version: Option<String>,
    },
    /// Retrieve the compilation status of the program.
    Status {
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
