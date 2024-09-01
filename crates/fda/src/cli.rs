use crate::cd::types::{CompilationProfile, ProgramConfig};
use clap::{Parser, Subcommand, ValueEnum};

#[derive(Parser)]
#[command(name = "fda")]
#[command(about = "A CLI to interact with the Feldera REST API.")]
pub struct Cli {
    #[command(subcommand)]
    pub command: Commands,
    /// The Feldera host to connect to.
    #[arg(long, env = "FELDERA_HOST", default_value_t = String::from("https://try.feldera.com"))]
    pub host: String,
    /// Which API key to use for authentication.
    ///
    /// The provided string should start with "apikey:" followed by the random characters.
    ///
    /// If not specified, a request without authentication will be used.
    #[arg(long, env = "FELDERA_API_KEY")]
    pub auth: Option<String>,
    /// The format in which the output should be displayed.
    #[arg(long, default_value = "text", env = "FELDERA_OUTPUT_FORMAT")]
    pub format: OutputFormat,
}

#[derive(ValueEnum, Clone, Copy, Debug)]
#[value(rename_all = "snake_case")]
pub enum OutputFormat {
    Text,
}

#[derive(Subcommand)]
pub enum Commands {
    /// List the available pipelines.
    Pipelines,
    /// Interact with a pipeline.
    ///
    /// If no sub-command is specified retrieves all configuration data for the pipeline.
    Pipeline {
        /// The name of the pipeline.
        name: String,
        #[command(subcommand)]
        action: Option<PipelineAction>,
    },
    /// Manage API keys
    Apikey {
        #[command(subcommand)]
        action: ApiKeyActions,
    },
    /// Generate a completion script for your shell.
    ///
    /// The script is written to stdout.
    ///
    /// EXAMPLES:
    ///
    /// bash:
    /// $ fda shell-completion > fda.bash
    /// $ sudo mv fda.bash /usr/share/bash-completion/completions/fda.bash
    ///
    /// zsh:
    /// $ fda shell-completion > _fda
    /// $ sudo mv fda.zsh /usr/local/share/zsh/site-functions/_fda
    ///
    /// or with oh-my-zsh:
    /// $ mkdir -p ~/.oh-my-zsh/completions
    /// $ fda shell-completion > ~/.oh-my-zsh/completions/_fda
    ///
    /// powershell:
    /// $ fda shell-completion > fda.ps1
    /// Add contents to ~\Documents\PowerShell\Microsoft.PowerShell_profile.ps1
    #[command(verbatim_doc_comment)]
    ShellCompletion,
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
        /// A path to a file containing the SQL code.
        ///
        /// See the `stdin` flag for reading from stdin instead.
        program_path: String,
        /// The compilation profile to use.
        #[arg(default_value = "optimized")]
        profile: Profile,
        /// Read the program code from stdin.
        ///
        /// EXAMPLES:
        ///
        /// * cat program.sql | fda pipeline p1 create -s -
        /// * echo "SELECT 1" | fda pipeline p1 create -s - dev
        /// * fda pipeline p2 program | fda pipeline p1 create -s -
        #[arg(verbatim_doc_comment, short = 's', long, default_value_t = false)]
        stdin: bool,
    },
    /// Start the pipeline.
    ///
    /// If the pipeline is compiling it will wait for the compilation to finish.
    Start {
        /// Force the recompilation of the pipeline before starting.
        ///
        /// This is useful for dev purposes in case the Feldera source-code has changed.
        #[arg(long, short = 'r', default_value_t = false)]
        recompile: bool,
    },
    /// Pause the pipeline.
    Pause,
    /// Shutdown the pipeline, then restart it.
    ///
    /// This is a shortcut for calling `fda pipeline p1 shutdown` followed by `fda pipeline p1 start`.
    Restart {
        /// Force the recompilation of the pipeline before starting.
        ///
        /// This is useful for dev purposes in case the Feldera source-code has changed.
        #[arg(long, short = 'r', default_value_t = false)]
        recompile: bool,
    },
    /// Shutdown the pipeline.
    #[clap(aliases = &["stop"])]
    Shutdown,
    /// Retrieve the deployment status of a pipeline.
    Status,
    /// Get the runtime stats of a pipeline.
    #[clap(aliases = &["statistics"])]
    Stats,
    /// Interact with the program of the pipeline.
    ///
    /// If no sub-command is specified retrieves the program.
    Program {
        #[command(subcommand)]
        action: Option<ProgramAction>,
    },
    /// Retrieve the runtime config of a pipeline.
    #[clap(aliases = &["cfg"])]
    Config,
    /// Update the runtime config of a pipeline.
    #[clap(aliases = &["set-cfg"])]
    SetConfig {
        /// The key of the configuration to update.
        key: RuntimeConfigKey,
        /// The new value for the configuration.
        value: String,
    },
    /// Delete a pipeline.
    #[clap(aliases = &["del"])]
    Delete,
    /// Start/Stop an endpoint of the pipeline.
    Endpoint {
        endpoint_name: String,
        #[command(subcommand)]
        action: EndpointAction,
    },
    /// Enter the ad-hoc SQL shell of the pipeline.
    Shell {
        /// Start the pipeline before entering the shell.
        #[arg(long, short = 's', default_value_t = false)]
        start: bool,
    },
}

#[derive(Subcommand)]
pub enum ProgramAction {
    /// Set a new SQL program.
    Set {
        /// A path to a file containing the SQL code.
        ///
        /// See the `stdin` flag for reading from stdin instead.
        program_path: String,

        /// Read the program code from stdin.
        ///
        /// EXAMPLES:
        ///
        /// * cat program.sql | fda pipeline p1 program set -s -
        /// * echo "SELECT 1" | fda pipeline p1 program set -s -
        /// * fda pipeline p2 program | fda pipeline p1 program set -s -
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
