use crate::db::types::program::CompilationProfile;
use crate::db::types::version::Version;
use crate::db::{error::DBError, types::pipeline::PipelineId};
use actix_web::http::header;
use anyhow::{Error as AnyError, Result as AnyResult};
use clap::Parser;
use log::warn;
use openssl::ssl::{SslConnector, SslMethod, SslVerifyMode};
use postgres_openssl::MakeTlsConnector;
use serde::Deserialize;
use std::{
    env,
    fs::{canonicalize, create_dir_all},
    path::{Path, PathBuf},
    sync::Once,
};

/// The default `platform_version` is formed using three compilation environment variables:
/// - `CARGO_PKG_VERSION` set by Cargo
/// - `FELDERA_PLATFORM_VERSION_SUFFIX` set by the custom `build.rs` script,
///   which is determined using the similarly named environment variable
///
/// ... and whether the `feldera-enterprise` feature is enabled.
fn default_platform_version() -> String {
    let suffix = env!("FELDERA_PLATFORM_VERSION_SUFFIX").to_string();
    let version = env!("CARGO_PKG_VERSION").to_string();
    if cfg!(feature = "feldera-enterprise") {
        if suffix.is_empty() {
            format!("{version}+enterprise")
        } else {
            format!("{version}+enterprise.{suffix}")
        }
    } else if suffix.is_empty() {
        version
    } else {
        format!("{version}+{suffix}")
    }
}

/// Default working directory: ~/.feldera
fn default_working_directory() -> PathBuf {
    dirs::home_dir()
        .expect("Cannot infer a home directory. Please use CLI arguments to explicitly set working directories.")
        .join(".feldera")
}

/// Default embedded postgres working directory: ~/.feldera/data
/// Note that it also creates a password file at: ~/.feldera/data.pwfile
fn default_pg_embed_working_directory() -> String {
    default_working_directory()
        .join("data")
        .into_os_string()
        .into_string()
        .unwrap()
}

/// Default compiler working directory: ~/.feldera/compiler
fn default_compiler_working_directory() -> String {
    default_working_directory()
        .join("compiler")
        .into_os_string()
        .into_string()
        .unwrap()
}

/// Default local runner working directory: ~/.feldera/local-runner
fn default_local_runner_working_directory() -> String {
    default_working_directory()
        .join("local-runner")
        .into_os_string()
        .into_string()
        .unwrap()
}

#[cfg(feature = "postgresql_embedded")]
fn default_db_connection_string() -> String {
    "postgres-embed".to_string()
}

#[cfg(not(feature = "postgresql_embedded"))]
fn default_db_connection_string() -> String {
    "".to_string()
}

/// Default address the API server, compiler and runner bind to.
fn default_server_address() -> String {
    "127.0.0.1".to_string()
}

/// Default port of the API server.
const fn default_api_server_port() -> u16 {
    8080
}

/// Default port of the compiler.
const fn default_compiler_port() -> u16 {
    8085
}

/// Default port of the local runner.
const fn default_local_runner_port() -> u16 {
    8089
}

/// Default demos directory used by the API server.
fn default_demos_dir() -> Vec<String> {
    vec!["demo/packaged/sql".to_string()]
}

/// Override to inform the compiler where to locate the DBSP crates
/// it needs for Rust compilation.
fn default_dbsp_override_path() -> String {
    ".".to_string()
}

/// Location of the SQL compiler which the compiler needs to know to
/// perform SQL compilation.
fn default_sql_compiler_path() -> String {
    "sql-to-dbsp-compiler/SQL-compiler/target/sql2dbsp-jar-with-dependencies.jar".to_string()
}

/// Default location of the Rust compilation `Cargo.lock`.
fn default_compilation_cargo_lock_path() -> String {
    "Cargo.lock".to_string()
}

fn default_sql_compiler_cache_url() -> String {
    "https://feldera-sql2dbsp.s3.us-west-1.amazonaws.com/".to_string()
}

/// The default Rust compilation profile.
fn default_compilation_profile() -> CompilationProfile {
    CompilationProfile::Optimized
}

/// Creates the directory.
fn help_create_dir(dir: &str) -> AnyResult<()> {
    create_dir_all(dir).map_err(|e| {
        AnyError::msg(format!(
            "unable to create or open working directory '{}': {e}",
            dir
        ))
    })?;
    Ok(())
}

/// Converts the directory path to an absolute path.
fn help_canonicalize_dir(dir: &str) -> AnyResult<String> {
    Ok(canonicalize(dir)
        .map_err(|e| {
            AnyError::msg(format!(
                "error canonicalizing working directory path '{}': {e}",
                dir
            ))
        })?
        .to_string_lossy()
        .into_owned())
}

/// Configuration common to API server, compiler and runner.
#[derive(Parser, Deserialize, Debug, Clone)]
#[command(author, version, about, long_about = None)]
pub struct CommonConfig {
    /// Platform version which is used to determine if an upgrade occurred.
    /// Default is determined at compile time.
    #[serde(default = "default_platform_version")]
    #[arg(long, default_value_t = default_platform_version())]
    pub platform_version: String,
}

/// Embedded Postgres configuration.
#[derive(Parser, Deserialize, Debug, Clone)]
#[command(author, version, about, long_about = None)]
pub struct PgEmbedConfig {
    /// Directory where the embedded Postgres instance stores its data.
    #[serde(default = "default_pg_embed_working_directory")]
    #[arg(long, default_value_t = default_pg_embed_working_directory())]
    pub pg_embed_working_directory: String,
}

impl PgEmbedConfig {
    /// Converts all directory paths in the `self` to absolute paths.
    pub fn canonicalize(mut self) -> AnyResult<Self> {
        help_create_dir(&self.pg_embed_working_directory)?;
        self.pg_embed_working_directory = help_canonicalize_dir(&self.pg_embed_working_directory)?;
        Ok(self)
    }

    #[cfg(feature = "postgresql_embedded")]
    pub(crate) fn pg_embed_data_dir(&self) -> PathBuf {
        Path::new(&self.pg_embed_working_directory).to_path_buf()
    }
}

/// Database configuration.
#[derive(Parser, Deserialize, Debug, Clone)]
#[command(author, version, about, long_about = None)]
pub struct DatabaseConfig {
    /// Point to a relational database to use for state management. Accepted
    /// values are `postgres://<host>:<port>` or `postgres-embed`. For
    /// postgres-embed we create a DB in the current working directory. For
    /// postgres, we use the connection string as provided.
    #[serde(default = "default_db_connection_string")]
    #[arg(long, default_value_t = default_db_connection_string())]
    pub db_connection_string: String,

    /// Create a TLS connector by loading a certificate from the path specified argument.
    ///
    /// If the argument is not set, tries to connect without TLS.
    #[arg(long, env = "FELDERA_DB_TLS_CERT_PATH")]
    pub db_tls_certificate_path: Option<String>,

    /// Disables TLS certificate verification.
    #[serde(default)]
    #[arg(long, env = "FELDERA_DB_TLS_DISABLE_VERIFY")]
    pub disable_tls_verify: bool,
}

impl DatabaseConfig {
    pub fn new(db_connection_string: String, db_tls_certificate_path: Option<String>) -> Self {
        Self {
            db_connection_string,
            db_tls_certificate_path,
            disable_tls_verify: false,
        }
    }

    pub(crate) fn tokio_postgres_config(
        &self,
    ) -> Result<tokio_postgres::Config, tokio_postgres::Error> {
        #[cfg(test)]
        {
            if self.uses_pg_client_config() {
                return Ok(pg_client_config::load_config(None).unwrap());
            }
        }
        let connection_str = self.database_connection_string();
        connection_str.parse::<tokio_postgres::Config>()
    }

    #[cfg(feature = "postgresql_embedded")]
    pub(crate) fn uses_postgres_embed(&self) -> bool {
        self.db_connection_string.starts_with("postgres-embed")
    }

    #[cfg(test)]
    pub(crate) fn uses_pg_client_config(&self) -> bool {
        self.db_connection_string
            .starts_with("postgres-pg-client-embed")
    }

    /// Database connection string.
    fn database_connection_string(&self) -> String {
        if self.db_connection_string.starts_with("postgres") {
            // this starts_with works for `postgres://`, `postgres-embed` and `postgres-pg-client-embed`
            self.db_connection_string.clone()
        } else {
            panic!("Invalid connection string {}", self.db_connection_string)
        }
    }

    pub(crate) fn tls_connector(&self) -> Result<MakeTlsConnector, DBError> {
        let mut builder =
            SslConnector::builder(SslMethod::tls()).map_err(|e| DBError::TlsConnection {
                hint: "Unable to build TLS Connector to connect to PostgreSQL".to_string(),
                openssl_error: Some(e),
            })?;

        if self.disable_tls_verify {
            static ONCE: Once = Once::new();
            ONCE.call_once(|| {
                warn!("PostgreSQL TLS verification is disabled -- not recommended for production environments.");
            });
            builder.set_verify(SslVerifyMode::NONE);
        }

        if let Some(ca_path) = &self.db_tls_certificate_path {
            builder
                .set_ca_file(ca_path)
                .map_err(|e| DBError::TlsConnection {
                    hint: format!(
                        "Unable to find TLS certificate at {:?}",
                        self.db_tls_certificate_path
                    ),
                    openssl_error: Some(e),
                })?;
        }

        Ok(MakeTlsConnector::new(builder.build()))
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

/// API server configuration read from command-line arguments.
#[derive(Parser, Deserialize, Debug, Clone)]
#[command(author, version, about, long_about = None)]
pub struct ApiServerConfig {
    /// Port number for the API server HTTP service.
    #[serde(default = "default_api_server_port")]
    #[arg(short, long, default_value_t = default_api_server_port())]
    pub port: u16,

    /// Bind address for the API server HTTP service.
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

    /// Local directories in which demos are stored for supplying clients like the UI with
    /// a set of demos to present to the user. Administrators can use this option to set
    /// up environment-specific demos for users (e.g., ones that connect to an internal
    /// data source).
    ///
    /// For each directory, the files are read sorted on the filename.
    /// For multiple directories, the lists of demos are appended one after the other into a single one.
    /// Files which do not end in `.sql` and directories are ignored. Symlinks are followed.
    /// If a `<filename>.sql` exists, checks for `<filename>.udf.rs` and `<filename>.udf.toml`.
    /// If present, these will be included in the demo as well.
    #[arg(long, default_values_t = default_demos_dir())]
    pub demos_dir: Vec<String>,

    /// Telemetry key.
    ///
    /// If a telemetry key is set, anonymous usage data will be collected
    /// and sent to our telemetry service.
    #[arg(long, default_value = "", env = "FELDERA_TELEMETRY")]
    pub telemetry: String,

    /// The hostname:port to use in a URL to reach the runner web server, which for instance
    /// provides access to pipeline logs. The hostname will typically be a DNS name for the host
    /// running the runner service.
    #[arg(long, default_value = "127.0.0.1:8089")]
    pub runner_hostname_port: String,
}

impl ApiServerConfig {
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
}

/// Compiler server configuration read from command-line arguments.
#[derive(Parser, Deserialize, Debug, Clone)]
pub struct CompilerConfig {
    /// Directory where the manager stores its filesystem state:
    /// generated Rust crates, pipeline logs, etc.
    #[serde(default = "default_compiler_working_directory")]
    #[arg(long, default_value_t = default_compiler_working_directory())]
    pub compiler_working_directory: String,

    /// Profile used for programs that do not explicitly provide their
    /// own compilation profile in their configuration.
    ///
    /// Available choices are:
    /// * 'dev', for development.
    /// * 'unoptimized', for faster compilation times at the cost of lower runtime performance.
    /// * 'optimized', for faster runtime performance at the cost of slower compilation times.
    #[serde(default = "default_compilation_profile")]
    #[arg(long, default_value_t = default_compilation_profile())]
    pub compilation_profile: CompilationProfile,

    /// Location of the SQL-to-DBSP compiler JAR file.
    #[serde(default = "default_sql_compiler_path")]
    #[arg(long, default_value_t = default_sql_compiler_path())]
    pub sql_compiler_path: String,

    /// Base URL of the SQL compiler JAR cache.
    ///
    /// In case a different runtime version is specified, system will try to download
    /// the corresponding compiler from this location.
    #[serde(default = "default_sql_compiler_cache_url")]
    #[arg(long, default_value_t = default_sql_compiler_cache_url())]
    pub sql_compiler_cache_url: String,

    /// Location of the `Cargo.lock` file which will be copied overriding
    /// at each pipeline Rust compilation.
    #[serde(default = "default_compilation_cargo_lock_path")]
    #[arg(long, default_value_t = default_compilation_cargo_lock_path())]
    pub compilation_cargo_lock_path: String,

    /// Override DBSP dependencies in generated Rust crates.
    ///
    /// By default, the Rust crates generated by the SQL compiler depend on local
    /// folder structure assumptions to find crates it needs like the `dbsp`
    /// crate. This configuration option modifies the dependency to point to
    /// a source tree in the local file system.
    #[arg(long, default_value_t = default_dbsp_override_path())]
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
    #[arg(long, long, default_value_t = default_compiler_port())]
    pub binary_ref_port: u16,
}

impl CompilerConfig {
    pub(crate) fn working_dir(&self) -> PathBuf {
        Path::new(&self.compiler_working_directory).to_path_buf()
    }

    /// Convert all directory paths in the `self` to absolute paths.
    pub fn canonicalize(mut self) -> AnyResult<Self> {
        help_create_dir(&self.compiler_working_directory)?;
        self.compiler_working_directory = help_canonicalize_dir(&self.compiler_working_directory)?;
        self.sql_compiler_path = help_canonicalize_dir(&self.sql_compiler_path)?;
        self.compilation_cargo_lock_path =
            help_canonicalize_dir(&self.compilation_cargo_lock_path)?;
        self.dbsp_override_path = help_canonicalize_dir(&self.dbsp_override_path)?;
        Ok(self)
    }
}

#[derive(Parser, Deserialize, Debug, Clone)]
#[command(author, version, about, long_about = None)]
pub struct LocalRunnerConfig {
    /// Local runner main HTTP server port.
    #[serde(default = "default_local_runner_port")]
    #[arg(long, default_value_t = default_local_runner_port())]
    pub runner_main_port: u16,

    /// Directory where the local runner stores its filesystem state:
    /// fetched binaries, configuration files etc.
    #[serde(default = "default_local_runner_working_directory")]
    #[arg(long, default_value_t = default_local_runner_working_directory())]
    pub runner_working_directory: String,

    /// The hostname or IP address over which pipelines created by
    /// this local runner will be reachable
    #[serde(default = "default_server_address")]
    #[arg(long, default_value_t = default_server_address())]
    pub pipeline_host: String,
}

impl LocalRunnerConfig {
    /// Creates and converts all directory paths in `self` to absolute paths.
    pub fn canonicalize(mut self) -> AnyResult<Self> {
        help_create_dir(&self.runner_working_directory)?;
        self.runner_working_directory = help_canonicalize_dir(&self.runner_working_directory)?;
        Ok(self)
    }

    /// Location to store pipeline files at runtime.
    pub(crate) fn pipeline_dir(&self, pipeline_id: PipelineId) -> PathBuf {
        Path::new(&self.runner_working_directory).join(format!("pipeline-{pipeline_id}"))
    }

    /// Location to write the fetched pipeline binary to.
    pub(crate) fn binary_file_path(&self, pipeline_id: PipelineId, version: Version) -> PathBuf {
        self.pipeline_dir(pipeline_id)
            .join(format!("program_{pipeline_id}_v{version}"))
    }

    /// Location to write the pipeline config file.
    pub(crate) fn config_file_path(&self, pipeline_id: PipelineId) -> PathBuf {
        self.pipeline_dir(pipeline_id).join("config.yaml")
    }

    /// Location for pipeline port file
    pub(crate) fn port_file_path(&self, pipeline_id: PipelineId) -> PathBuf {
        self.pipeline_dir(pipeline_id)
            .join(feldera_types::transport::http::SERVER_PORT_FILE)
    }
}
