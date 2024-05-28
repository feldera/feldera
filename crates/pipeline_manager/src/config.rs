use crate::db::{PipelineId, ProgramId, Version};
use actix_web::http::header;
use anyhow::{Error as AnyError, Result as AnyResult};
use clap::Parser;
use serde::{Deserialize, Serialize};
use std::{
    fmt::Display,
    fs::{canonicalize, create_dir_all},
    path::{Path, PathBuf},
    str::FromStr,
    string::ParseError,
};
use utoipa::ToSchema;

const fn default_server_port() -> u16 {
    8080
}

fn default_server_address() -> String {
    "127.0.0.1".to_string()
}

fn default_working_directory() -> String {
    let home = dirs::home_dir().expect("Cannot infer a home directory. Please use CLI arguments to explicitly set working directories (like --api-server-working-directory...)")
        .join(".feldera");
    home.into_os_string().into_string().unwrap()
}

fn default_override_path() -> String {
    ".".to_string()
}

fn default_sql_compiler_home() -> String {
    "sql-to-dbsp-compiler".to_string()
}

#[cfg(feature = "pg-embed")]
fn default_db_connection_string() -> String {
    "postgres-embed".to_string()
}

#[cfg(not(feature = "pg-embed"))]
fn default_db_connection_string() -> String {
    "".to_string()
}

fn default_binary_ref_port() -> u16 {
    9090
}

fn default_compilation_profile() -> CompilationProfile {
    CompilationProfile::Optimized
}

/// Pipeline manager configuration read from a YAML config file or from command
/// line arguments.
#[derive(Parser, Deserialize, Debug, Clone)]
#[command(author, version, about, long_about = None)]
pub struct DatabaseConfig {
    /// Point to a relational database to use for state management. Accepted
    /// values are `postgres://<host>:<port>` or `postgres-embed`. For
    /// postgres-embed we create a DB in the current working directory. For
    /// postgres, we use the connection string as provided.
    #[serde(default = "default_db_connection_string")]
    #[arg(short, long, default_value_t = default_db_connection_string())]
    pub db_connection_string: String,
}

impl DatabaseConfig {
    /// Database connection string.
    pub(crate) fn database_connection_string(&self) -> String {
        if self.db_connection_string.starts_with("postgres") {
            // this starts_with works for `postgres://` and `postgres-embed`
            self.db_connection_string.clone()
        } else {
            panic!("Invalid connection string {}", self.db_connection_string)
        }
    }
}

#[derive(Parser, Deserialize, Debug, Clone, Default, PartialEq, Eq, clap::ValueEnum)]
pub enum AuthProviderType {
    #[default]
    None,
    AwsCognito,
    GoogleIdentity,
}

impl std::fmt::Display for AuthProviderType {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match *self {
            AuthProviderType::None => write!(f, "none"),
            AuthProviderType::AwsCognito => write!(f, "aws-cognito"),
            AuthProviderType::GoogleIdentity => write!(f, "google-identity"),
        }
    }
}

/// Pipeline manager configuration read from a YAML config file or from command
/// line arguments.
#[derive(Parser, Deserialize, Debug, Clone)]
#[command(author, version, about, long_about = None)]
pub struct ApiServerConfig {
    /// Directory where the api-server stores its filesystem state:
    /// generated Rust crates, pipeline logs, etc.
    #[serde(default = "default_working_directory")]
    #[arg(short, long, default_value_t = default_working_directory())]
    pub api_server_working_directory: String,

    /// Port number for the HTTP service, defaults to 8080.
    #[serde(default = "default_server_port")]
    #[arg(short, long, default_value_t = default_server_port())]
    pub port: u16,

    /// Bind address for the HTTP service, defaults to 127.0.0.1.
    #[serde(default = "default_server_address")]
    #[arg(short, long, default_value_t = default_server_address())]
    pub bind_address: String,

    /// Enable bearer-token based authorization.
    ///
    /// Usage depends on two environment variables to be set
    ///
    /// AUTH_CLIENT_ID, the client-id or application
    /// AUTH_ISSUER, the issuing service
    ///
    /// ** AWS Cognito provider **
    /// If the auth_provider is aws-cognito, there are two more
    /// environment variables that need to be set. This is required
    /// to make use of the AWS hosted login UI
    /// (see <https://docs.aws.amazon.com/cognito/latest/developerguide/cognito-user-pools-app-integration.html#cognito-user-pools-app-integration-amplify>):
    ///
    /// AWS_COGNITO_LOGIN_URL
    /// AWS_COGNITO_LOGOUT_URL
    ///
    /// These two URLs correspond to the login and logout endpoints.
    /// See here: <https://docs.aws.amazon.com/cognito/latest/developerguide/login-endpoint.html>
    /// There is one caveat though. You need to remove the "state"
    /// and "redirect_uri" URL parameers from the login/logout URLs.
    /// We expect to remove this requirement in the future.
    ///
    /// We also only support implicit grants for now. We expect to
    /// support PKCE soon.
    #[serde(default)]
    #[arg(long, action = clap::ArgAction::Set, default_value_t=AuthProviderType::None)]
    pub auth_provider: AuthProviderType,

    /// [Developers only] dump OpenAPI specification to `openapi.json` file and
    /// exit immediately.
    #[serde(skip)]
    #[arg(long)]
    pub dump_openapi: bool,

    /// Server configuration YAML file.
    #[serde(skip)]
    #[arg(short, long)]
    pub config_file: Option<String>,

    /// Allowed origins for CORS configuration. Cannot be used together with
    /// --dev-mode=true.
    #[serde(default)]
    #[arg(long)]
    pub allowed_origins: Option<Vec<String>>,

    /// [Developers only] Run in development mode.
    ///
    /// This runs with permissive CORS settings and allows the manager to be
    /// accessed from a different host/port.
    ///
    /// The default is `false`.
    #[serde(default)]
    #[arg(long)]
    pub dev_mode: bool,

    /// Used for supplying clients like the UI with demos. Administrators
    /// can use this option to set up environment-specific demos for users
    /// (e.g., ones that connect to an internal data source).
    #[serde(default)]
    #[arg(long, value_name = "HTTPS_URL")]
    pub demos: Vec<String>,
}

impl ApiServerConfig {
    /// Convert all directory paths in the `self` to absolute paths.
    ///
    /// Converts `working_directory` `sql_compiler_home`, and
    /// `dbsp_override_path` fields to absolute paths;
    /// fails if any of the paths doesn't exist or isn't readable.
    pub fn canonicalize(mut self) -> AnyResult<Self> {
        create_dir_all(&self.api_server_working_directory).map_err(|e| {
            AnyError::msg(format!(
                "unable to create or open working directory '{}': {e}",
                self.api_server_working_directory
            ))
        })?;

        self.api_server_working_directory = canonicalize(&self.api_server_working_directory)
            .map_err(|e| {
                AnyError::msg(format!(
                    "error canonicalizing working directory path '{}': {e}",
                    self.api_server_working_directory
                ))
            })?
            .to_string_lossy()
            .into_owned();

        Ok(self)
    }

    /// CORS configuration
    pub(crate) fn cors(&self) -> actix_cors::Cors {
        if self.dev_mode {
            if self.allowed_origins.is_some() {
                panic!("Allowed origins set while dev-mode is enabled.");
            }
            actix_cors::Cors::permissive()
        } else {
            let mut cors = actix_cors::Cors::default();
            if let Some(ref origins) = self.allowed_origins {
                for origin in origins {
                    cors = cors.allowed_origin(origin);
                }
            } else {
                cors = cors.allow_any_origin();
            }
            cors.allowed_methods(vec!["GET", "POST", "PATCH", "PUT", "DELETE"])
                .allowed_headers(vec![header::AUTHORIZATION, header::ACCEPT])
                .supports_credentials()
        }
    }

    /// Where Postgres embed stores the database.
    ///
    /// e.g., `<working-directory>/data`
    #[cfg(feature = "pg-embed")]
    pub(crate) fn postgres_embed_data_dir(&self) -> PathBuf {
        Path::new(&self.api_server_working_directory).join("data")
    }
}

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
    fn to_target_folder(&self) -> &'static str {
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

/// Pipeline manager configuration read from a YAML config file or from command
/// line arguments.
#[derive(Parser, Deserialize, Debug, Clone)]
pub struct CompilerConfig {
    /// Directory where the manager stores its filesystem state:
    /// generated Rust crates, pipeline logs, etc.
    #[serde(default = "default_working_directory")]
    #[arg(long, default_value_t = default_working_directory())]
    pub compiler_working_directory: String,

    /// Profile used for programs that do not explicitly provide their
    /// own compilation profile in their configuration.
    ///
    /// Available choices are:
    /// * 'dev', for development.
    /// * 'unoptimized', for faster compilation times
    /// at the cost of lower runtime performance.
    /// * 'optimized', for faster runtime performance
    /// at the cost of slower compilation times.
    #[serde(default = "default_compilation_profile")]
    #[arg(long, default_value_t = default_compilation_profile())]
    pub compilation_profile: CompilationProfile,

    /// Location of the SQL-to-DBSP compiler.
    #[serde(default = "default_sql_compiler_home")]
    #[arg(long, default_value_t = default_sql_compiler_home())]
    pub sql_compiler_home: String,

    /// Override DBSP dependencies in generated Rust crates.
    ///
    /// By default the Rust crates generated by the SQL compiler depend on local
    /// folder structure assumptions to find crates it needs like the `dbsp`
    /// crate. This configuration option modifies the dependency to point to
    /// a source tree in the local file system.
    #[arg(long, default_value_t = default_override_path())]
    pub dbsp_override_path: String,

    /// Precompile Rust dependencies in the working directory.
    ///
    /// Instructs the manager to download and compile all crates needed by
    /// the Rust code generated by the SQL compiler and exit immediately.
    /// This is useful to prepare the working directory, so that the first
    /// compilation job completes quickly.  Also creates the `Cargo.lock`
    /// file, making sure that subsequent `cargo` runs do not access the
    /// network.
    #[serde(skip)]
    #[arg(long)]
    pub precompile: bool,

    /// The hostname to use in a URL for making compiled binaries available
    /// for runners. This will typically be a DNS name for the host running
    /// this compiler service.
    #[arg(long, default_value = "127.0.0.1")]
    pub binary_ref_host: String,

    /// The port to use in a URL for making compiled binaries available
    /// for runners.
    #[arg(long, default_value_t = default_binary_ref_port())]
    pub binary_ref_port: u16,
}

impl CompilerConfig {
    /// Binary name for a project and version.
    ///
    /// Note: we rely on the program id and not name, so projects can
    /// be renamed without recompiling.
    pub(crate) fn binary_name(program_id: ProgramId, version: Version) -> String {
        format!("project_{program_id}_v{version}")
    }

    /// Directory where the manager maintains the generated cargo workspace.
    ///
    /// e.g., `<working-directory>/cargo_workspace`
    pub(crate) fn workspace_dir(&self) -> PathBuf {
        Path::new(&self.compiler_working_directory).join("cargo_workspace")
    }

    /// Directory where the manager stores binary artefacts needed to
    /// run versioned pipeline configurations.
    ///
    /// e.g., `<working-directory>/binaries`
    pub(crate) fn binaries_dir(&self) -> PathBuf {
        Path::new(&self.compiler_working_directory).join("binaries")
    }

    /// Location of the versioned executable.
    /// e.g., `<working-directory>/binaries/
    /// project0188e0cd-d8b0-71d5-bb5a-2f66c7b07dfb-v11`
    pub(crate) fn versioned_executable(&self, program_id: ProgramId, version: Version) -> PathBuf {
        Path::new(&self.binaries_dir()).join(Self::binary_name(program_id, version))
    }

    /// Location of the compiled executable for the project in the cargo target
    /// dir.
    /// Note: This is generally not an executable that's run as a pipeline.
    pub(crate) fn target_executable(
        &self,
        program_id: ProgramId,
        profile: &CompilationProfile,
    ) -> PathBuf {
        // Always pick the compiler server's compilation profile if it is configured.
        Path::new(&self.workspace_dir())
            .join("target")
            .join(profile.to_target_folder())
            .join(Self::crate_name(program_id))
    }

    /// Crate name for a project.
    ///
    /// Note: we rely on the program id and not name, so projects can
    /// be renamed without recompiling.
    pub(crate) fn crate_name(program_id: ProgramId) -> String {
        format!("project{program_id}")
    }

    /// File name where the manager stores the SQL code of the project.
    pub(crate) fn sql_file_path(&self, program_id: ProgramId) -> PathBuf {
        self.project_dir(program_id).join("project.sql")
    }

    /// Directory where the manager generates the rust crate for the project.
    ///
    /// e.g., `<working-directory>/cargo_workspace/
    /// project0188e0cd-d8b0-71d5-bb5a-2f66c7b07dfb`
    pub(crate) fn project_dir(&self, program_id: ProgramId) -> PathBuf {
        self.workspace_dir().join(Self::crate_name(program_id))
    }

    /// The path to `schema.json` that contains a JSON description of input and
    /// output tables.
    pub(crate) fn schema_path(&self, program_id: ProgramId) -> PathBuf {
        const SCHEMA_FILE_NAME: &str = "schema.json";
        let sql_file_path = self.sql_file_path(program_id);
        let project_directory = sql_file_path.parent().unwrap();

        PathBuf::from(project_directory).join(SCHEMA_FILE_NAME)
    }

    /// Path to the generated `main.rs` for the project.
    pub(crate) fn rust_program_path(&self, program_id: ProgramId) -> PathBuf {
        self.project_dir(program_id).join("src").join("main.rs")
    }

    /// Path to the generated `Cargo.toml` file for the project.
    pub(crate) fn project_toml_path(&self, program_id: ProgramId) -> PathBuf {
        self.project_dir(program_id).join("Cargo.toml")
    }

    /// Top-level `Cargo.toml` file for the generated Rust workspace.
    pub(crate) fn workspace_toml_path(&self) -> PathBuf {
        self.workspace_dir().join("Cargo.toml")
    }
    /// Convert all directory paths in the `self` to absolute paths.
    ///
    /// Converts `working_directory` `sql_compiler_home`, and
    /// `dbsp_override_path` fields to absolute paths;
    /// fails if any of the paths doesn't exist or isn't readable.
    pub fn canonicalize(mut self) -> AnyResult<Self> {
        create_dir_all(&self.compiler_working_directory).map_err(|e| {
            AnyError::msg(format!(
                "unable to create or open working directory '{}': {e}",
                self.compiler_working_directory
            ))
        })?;
        create_dir_all(self.binaries_dir()).map_err(|e| {
            AnyError::msg(format!(
                "unable to create or open binaries directory '{:?}': {e}",
                self.binaries_dir()
            ))
        })?;

        self.sql_compiler_home = canonicalize(&self.sql_compiler_home)
            .map_err(|e| {
                AnyError::msg(format!(
                    "failed to access SQL compiler home '{}': {e}",
                    self.sql_compiler_home
                ))
            })?
            .to_string_lossy()
            .into_owned();

        let path = self.dbsp_override_path;
        self.dbsp_override_path = canonicalize(&path)
            .map_err(|e| {
                AnyError::msg(format!(
                    "failed to access dbsp override directory '{path}': {e}"
                ))
            })?
            .to_string_lossy()
            .into_owned();

        Ok(self)
    }

    /// SQL compiler executable.
    pub(crate) fn sql_compiler_path(&self) -> PathBuf {
        Path::new(&self.sql_compiler_home)
            .join("SQL-compiler")
            .join("sql-to-dbsp")
    }

    /// Location of the Rust libraries that ship with the SQL compiler.
    pub(crate) fn sql_lib_path(&self) -> PathBuf {
        Path::new(&self.sql_compiler_home).join("lib")
    }

    /// Location of the template `Cargo.toml` file that ships with the SQL
    /// compiler.
    pub(crate) fn project_toml_template_path(&self) -> PathBuf {
        Path::new(&self.sql_compiler_home)
            .join("temp")
            .join("Cargo.toml")
    }

    /// File to redirect compiler's stdout stream.
    pub(crate) fn compiler_stdout_path(&self, program_id: ProgramId) -> PathBuf {
        self.project_dir(program_id).join("out.log")
    }

    /// File to redirect compiler's stderr stream.
    pub(crate) fn compiler_stderr_path(&self, program_id: ProgramId) -> PathBuf {
        self.project_dir(program_id).join("err.log")
    }
}

#[derive(Parser, Deserialize, Debug, Clone)]
#[command(author, version, about, long_about = None)]
pub struct LocalRunnerConfig {
    /// Directory where the local runner stores its filesystem state:
    /// fetched binaries, configuration files etc.
    #[serde(default = "default_working_directory")]
    #[arg(long, default_value_t = default_working_directory())]
    pub runner_working_directory: String,

    /// The hostname or IP address over which pipelines created by
    /// this local runner will be reachable
    #[serde(default = "default_server_address")]
    #[arg(long, default_value_t = default_server_address())]
    pub pipeline_host: String,
}

impl LocalRunnerConfig {
    /// Convert all directory paths in `self` to absolute paths.
    ///
    /// Converts `working_directory` fails if any of the paths doesn't exist or
    /// isn't readable.
    pub fn canonicalize(mut self) -> AnyResult<Self> {
        create_dir_all(&self.runner_working_directory).map_err(|e| {
            AnyError::msg(format!(
                "unable to create or open working directory '{}': {e}",
                self.runner_working_directory
            ))
        })?;

        self.runner_working_directory = canonicalize(&self.runner_working_directory)
            .map_err(|e| {
                AnyError::msg(format!(
                    "error canonicalizing working directory path '{}': {e}",
                    self.runner_working_directory
                ))
            })?
            .to_string_lossy()
            .into_owned();

        Ok(self)
    }
    /// Location to store pipeline files at runtime.
    pub(crate) fn pipeline_dir(&self, pipeline_id: PipelineId) -> PathBuf {
        Path::new(&self.runner_working_directory)
            .join("pipelines")
            .join(format!("pipeline{pipeline_id}"))
    }

    /// Location to write the fetched pipeline binary to.
    pub(crate) fn binary_file_path(
        &self,
        pipeline_id: PipelineId,
        program: ProgramId,
        version: Version,
    ) -> PathBuf {
        self.pipeline_dir(pipeline_id)
            .join(format!("program_{program}_v{version}"))
    }

    /// Location to write the pipeline config file.
    pub(crate) fn config_file_path(&self, pipeline_id: PipelineId) -> PathBuf {
        self.pipeline_dir(pipeline_id).join("config.yaml")
    }

    /// Location for pipeline port file
    pub(crate) fn port_file_path(&self, pipeline_id: PipelineId) -> PathBuf {
        self.pipeline_dir(pipeline_id)
            .join(pipeline_types::transport::http::SERVER_PORT_FILE)
    }
}

fn default_prober_web_server_port() -> u16 {
    44444
}

fn default_probe_sleep_inbetween_ms() -> u64 {
    1000
}

fn default_probe_timeout_ms() -> u64 {
    1500
}

/// Prober configuration.
/// This configuration is read (deserialized) from a YAML config file
/// or parsed from command-line arguments.
#[derive(Parser, Deserialize, Debug, Clone)]
pub struct ProberConfig {
    /// General working directory where the prober will store its logs.
    #[serde(default = "default_working_directory")]
    #[arg(long, default_value_t = default_working_directory())]
    pub prober_working_directory: String,

    /// The port used by the web server of the prober.
    #[arg(long, default_value_t = default_prober_web_server_port())]
    pub prober_http_server_port: u16,

    /// Duration (ms) slept between checks when no probes to perform are
    /// available.
    #[arg(long, default_value_t = default_probe_sleep_inbetween_ms())]
    pub probe_sleep_inbetween_ms: u64,

    /// Probe timeout (ms).
    #[arg(long, default_value_t = default_probe_timeout_ms())]
    pub probe_timeout_ms: u64,
}

impl ProberConfig {
    /// Convert all directory paths in the `self` to absolute paths.
    pub fn canonicalize(mut self) -> AnyResult<Self> {
        create_dir_all(&self.prober_working_directory).map_err(|e| {
            AnyError::msg(format!(
                "unable to create or open directory '{}': {e}",
                self.prober_working_directory
            ))
        })?;

        self.prober_working_directory = canonicalize(&self.prober_working_directory)
            .map_err(|e| {
                AnyError::msg(format!(
                    "error canonicalizing directory path '{}': {e}",
                    self.prober_working_directory
                ))
            })?
            .to_string_lossy()
            .into_owned();

        Ok(self)
    }
}
