use crate::db::types::program::CompilationProfile;
use crate::db::types::version::Version;
use crate::db::{error::DBError, types::pipeline::PipelineId};
use actix_web::http::header;
use anyhow::{Error as AnyError, Result as AnyResult};
use clap::Parser;
use log::warn;
use openssl::ssl::{SslConnector, SslMethod, SslVerifyMode};
use postgres_openssl::MakeTlsConnector;
use reqwest::Certificate;
use rustls::pki_types::pem::PemObject;
use rustls::pki_types::{CertificateDer, PrivateKeyDer};
use rustls::{ClientConfig, RootCertStore};
use serde::Deserialize;
use std::sync::Arc;
use std::{
    env,
    fs::{canonicalize, create_dir_all},
    path::{Path, PathBuf},
    sync::Once,
    thread,
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

/// Default demos directory used by the API server.
fn default_demos_dir() -> Vec<String> {
    vec!["demo/packaged/sql".to_string()]
}

/// Default value for individual_tenant flag.
fn default_individual_tenant() -> bool {
    true
}

/// Default audience claim value for OIDC authentication.
fn default_auth_audience() -> String {
    "feldera-api".to_string()
}

/// Determines the default amount of worker threads to spawn.
fn default_http_workers() -> usize {
    thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(2)
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

/// Converts the directory or file path to an absolute path.
fn help_canonicalize_path(path: &str) -> AnyResult<String> {
    Ok(canonicalize(path)
        .map_err(|e| AnyError::msg(format!("error canonicalizing path '{}': {e}", path)))?
        .to_string_lossy()
        .into_owned())
}

/// This parses a CPU quantity string, which can be either in millicores
/// (e.g., "500m") or in cores (e.g., "0.5", "1", "2.25") and converts
/// it to an integer that defines the number of threads to spawn for the
/// given CPU quantity.
///
/// The input is expected to be in the Kubernetes CPU quantity format
/// described here.
fn cpu_quantity_to_workers(s: &str) -> Result<usize, String> {
    let quantity = if let Some(stripped) = s.strip_suffix('m') {
        let millicores: f64 = stripped.parse::<f64>().map_err(|e| {
            format!("Invalid worker count specified, valid examples: `2`, `0.5`, `500m`: {e}")
        })?;
        millicores / 1000.0
    } else {
        s.parse::<f64>().map_err(|e| {
            format!("Invalid worker count specified, valid values include: `2`, `0.5`, `500m`: {e}")
        })?
    };

    Ok(quantity.ceil().max(1.0) as usize)
}

/// Configuration common to API server, compiler and runner.
#[derive(Parser, Debug, Clone)]
#[command(author, version, about, long_about = None)]
pub struct CommonConfig {
    /// Platform version which is used to determine if an upgrade occurred.
    /// Default is determined at compile time.
    #[arg(long, default_value_t = default_platform_version())]
    pub platform_version: String,

    /// IP address on which the HTTP server of the API server, compiler, runner and pipelines bind themselves.
    /// This should be set to either `127.0.0.1` (default) or `0.0.0.0`.
    #[arg(long, default_value = "127.0.0.1")]
    pub bind_address: String,

    /// Port used by the API server to both bind its HTTP server, and on which it can be reached.
    #[arg(long, default_value_t = 8080)]
    pub api_port: u16,

    /// Host (hostname or IP address) at which the compiler HTTP server can be reached by the others (e.g., pipelines).
    #[arg(long, default_value = "127.0.0.1")]
    pub compiler_host: String,

    /// Port used by the compiler to both bind its HTTP server, and on which it can be reached.
    #[arg(long, long, default_value_t = 8085)]
    pub compiler_port: u16,

    /// Host (hostname or IP address) at which the runner HTTP server can be reached by the others (e.g., API server).
    #[arg(long, default_value = "127.0.0.1")]
    pub runner_host: String,

    /// Port used by the runner to both bind its HTTP server, and on which it can be reached.
    #[arg(long, default_value_t = 8089)]
    pub runner_port: u16,

    /// How many HTTP worker threads to spawn for the HTTP runtime system.
    ///
    /// If not specified, it will default to using the number of available CPUs
    /// in the system.
    ///
    /// The input is expected to be in the Kubernetes CPU quantity format
    /// <https://kubernetes.io/docs/concepts/configuration/manage-resources-containers/#resource-units-in-kubernetes>
    ///
    /// EXAMPLES:
    ///
    /// - `0` -> 1 threads
    /// - `2` -> 2 threads
    /// - `0.5` -> 1 thread
    /// - `500m` -> 1 thread
    /// - `1500m` -> 1 thread
    #[arg(verbatim_doc_comment, long, default_value_t = default_http_workers(), env = "FELDERA_HTTP_WORKERS", value_parser = cpu_quantity_to_workers)]
    pub http_workers: usize,

    /// Enable experimental platform features.
    ///
    /// These features are not yet stable and may change or be removed in the future.
    ///
    /// Currently supported features:
    /// - `runtime_version`: Allows to override the runtime version of a pipeline on the platform.
    #[arg(verbatim_doc_comment, long, env = "FELDERA_UNSTABLE_FEATURES")]
    pub unstable_features: Option<String>,

    /// Whether to enable TLS on the HTTP servers (API server, compiler, runner, pipelines).
    /// Iff true, is it allowed and required to set `https_tls_cert_path` and `https_tls_key_path`.
    #[arg(long, default_value_t = false)]
    pub enable_https: bool,

    /// Path to the TLS x509 certificate PEM file (e.g., `/path/to/tls.crt`).
    /// The same TLS certificate is used by all HTTP servers.
    #[arg(long)]
    pub https_tls_cert_path: Option<String>,

    /// Path to the TLS key PEM file corresponding to the x509 certificate (e.g.,
    /// `/path/to/tls.key`).
    #[arg(long)]
    pub https_tls_key_path: Option<String>,
}

impl CommonConfig {
    /// Converts all paths in the `self` to absolute paths.
    pub fn canonicalize(mut self) -> AnyResult<Self> {
        let _ = self.https_config();
        if let Some(https_tls_cert_path) = self.https_tls_cert_path {
            self.https_tls_cert_path = Some(help_canonicalize_path(&https_tls_cert_path)?);
        }
        if let Some(https_tls_key_path) = self.https_tls_key_path {
            self.https_tls_key_path = Some(help_canonicalize_path(&https_tls_key_path)?);
        }
        Ok(self)
    }

    /// Parses the HTTPS configuration arguments.
    /// If HTTPS is enabled, returns a pair of strings `Some((certificate path, key path))`.
    /// Otherwise, if HTTPS is not enabled, returns `None`.
    pub fn https_config(&self) -> Option<(String, String)> {
        if self.enable_https
            || self.https_tls_cert_path.is_some()
            || self.https_tls_key_path.is_some()
        {
            assert!(
                self.enable_https,
                "--enable-https is required to pass --https-tls-cert-path or --https-tls-key-path"
            );
            let https_tls_cert_path = self
                .https_tls_cert_path
                .as_ref()
                .expect("CLI argument --https-tls-cert-path is required");
            let https_tls_key_path = self
                .https_tls_key_path
                .as_ref()
                .expect("CLI argument --https-tls-key-path is required");
            Some((https_tls_cert_path.clone(), https_tls_key_path.clone()))
        } else {
            None
        }
    }

    /// Creates the `ServerConfig` for the HTTP servers of the API server, compiler and runner if
    /// HTTPS is enabled. Otherwise, returns `None`.
    pub fn https_server_config(&self) -> Option<rustls::ServerConfig> {
        if let Some((https_tls_cert_path, https_tls_key_path)) = self.https_config() {
            // Load in certificate (public)
            let cert_chain = CertificateDer::pem_file_iter(https_tls_cert_path)
                .expect("HTTPS TLS certificate should be read")
                .flatten()
                .collect();

            // Load in key (private)
            let key_der = PrivateKeyDer::from_pem_file(https_tls_key_path)
                .expect("HTTPS TLS key should be read");

            // Server configuration
            let server_config = rustls::ServerConfig::builder()
                .with_no_client_auth()
                .with_single_cert(cert_chain, key_der)
                .expect(
                    "server configuration should be built using the certificate and private key",
                );

            Some(server_config)
        } else {
            None
        }
    }

    /// Creates `awc` client.
    ///
    /// - If HTTPS is enabled for Feldera HTTP servers, the client will only have our HTTPS certificate
    ///   as the only valid one. As such, it will be only able to connect to Feldera HTTPS servers.
    ///   Unfortunately, it is not possible to configure `awc` to refuse to connect over HTTP at all.
    ///
    /// - If HTTPS is not enabled for Feldera HTTP servers, it will return the default client which can
    ///   connect to both HTTP and HTTPS (for any valid system certificates).
    pub fn awc_client(&self) -> awc::Client {
        if let Some((https_tls_cert_path, _)) = self.https_config() {
            let cert_chain: Vec<_> = CertificateDer::pem_file_iter(https_tls_cert_path)
                .expect("HTTPS TLS certificate should be read")
                .flatten()
                .collect();
            let mut root_cert_store = RootCertStore::empty();
            for certificate in cert_chain {
                root_cert_store.add(certificate).unwrap();
            }
            let config = ClientConfig::builder()
                .with_root_certificates(root_cert_store)
                .with_no_client_auth();
            // In the `awc` implementation, the connection is kept open for endpoints that return a
            // streaming response (e.g., `/logs`) when they are half closed. As a workaround,
            // increase the max connections limit to 25'000. This is also the default maximum number
            // of connections for an actix server.
            awc::Client::builder()
                .connector(
                    awc::Connector::new()
                        .rustls_0_23(Arc::new(config))
                        .limit(25000),
                )
                .finish()
        } else {
            awc::Client::new()
        }
    }

    /// Creates `reqwest` client.
    ///
    /// - If HTTPS is enabled for Feldera HTTP servers, the client will only have our HTTPS certificate
    ///   as the only valid one. As such, it will be only able to connect to Feldera HTTPS servers.
    ///   It will refuse to connect over HTTP at all.
    ///
    /// - If HTTPS is not enabled for Feldera HTTP servers, it will return the default client which can
    ///   connect to both HTTP and HTTPS (for any valid system certificates).
    pub async fn reqwest_client(&self) -> reqwest::Client {
        if let Some((https_tls_cert_path, _)) = self.https_config() {
            let cert_pem = tokio::fs::read_to_string(https_tls_cert_path)
                .await
                .expect("HTTPS TLS certificate should be read");
            let certificate = Certificate::from_pem(cert_pem.as_bytes())
                .expect("HTTPS TLS certificate should be readable");
            reqwest::ClientBuilder::new()
                .https_only(true) // Only connect to HTTPS
                .add_root_certificate(certificate) // Add our own TLS certificate which is used
                .tls_built_in_root_certs(false) // Other TLS certificates are not used
                .build()
                .expect("HTTPS client should be built")
        } else {
            reqwest::Client::new()
        }
    }

    #[cfg(test)]
    pub(crate) fn test_config() -> Self {
        Self {
            bind_address: "127.0.0.1".to_string(),
            api_port: 8080,
            compiler_host: "127.0.0.1".to_string(),
            compiler_port: 8085,
            runner_host: "127.0.0.1".to_string(),
            runner_port: 8089,
            platform_version: "v0".to_string(),
            http_workers: 1,
            unstable_features: None,
            enable_https: false,
            https_tls_cert_path: None,
            https_tls_key_path: None,
        }
    }
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
        self.pg_embed_working_directory = help_canonicalize_path(&self.pg_embed_working_directory)?;
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

    /// Disables TLS hostname verification.
    #[serde(default)]
    #[arg(long, env = "FELDERA_DB_TLS_DISABLE_HOSTNAME_VERIFY")]
    pub disable_tls_hostname_verify: bool,
}

impl DatabaseConfig {
    pub fn new(db_connection_string: String, db_tls_certificate_path: Option<String>) -> Self {
        Self {
            db_connection_string,
            db_tls_certificate_path,
            disable_tls_verify: false,
            disable_tls_hostname_verify: false,
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

        let mut connector = MakeTlsConnector::new(builder.build());

        if self.disable_tls_hostname_verify {
            warn!("PostgreSQL TLS hostname verification is disabled. The PostgreSQL server's hostname may not match the one specified in the SSL certificate.");
            connector.set_callback(|ctx, _| {
                ctx.set_verify_hostname(false);
                Ok(())
            });
        }

        Ok(connector)
    }
}

#[derive(Parser, Deserialize, Debug, Clone, Default, PartialEq, Eq, clap::ValueEnum)]
pub enum AuthProviderType {
    #[default]
    None,
    AwsCognito,
    GenericOidc,
}

impl std::fmt::Display for AuthProviderType {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match *self {
            AuthProviderType::None => write!(f, "none"),
            AuthProviderType::AwsCognito => write!(f, "aws-cognito"),
            AuthProviderType::GenericOidc => write!(f, "generic-oidc"),
        }
    }
}

/// API server configuration read from command-line arguments.
#[derive(Parser, Deserialize, Debug, Clone)]
#[command(author, version, about, long_about = None)]
pub struct ApiServerConfig {
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
    ///
    /// ** Okta provider **
    /// If the auth_provider is okta, the AUTH_ISSUER should be your Okta domain
    /// including the authorization server ID (e.g., "<https://your-domain.okta.com/oauth2/default>").
    /// The AUTH_CLIENT_ID should be the client ID from your Okta application configuration.
    ///
    /// ** Tenant Assignment **
    /// Tenant assignment follows this priority order:
    /// 1. 'tenant' claim in JWT (if configured by Okta admin)
    /// 2. Issuer domain extraction (if --issuer-tenant flag is set)
    /// 3. Individual user tenant from 'sub' claim (if --individual-tenant=true)
    ///
    /// Use --individual-tenant=false for enterprise deployments requiring explicit tenant assignment.
    /// Use --issuer-tenant for simple multi-user access using organization domain as tenant.
    #[serde(default)]
    #[arg(long, action = clap::ArgAction::Set, env = "AUTH_PROVIDER", default_value_t=AuthProviderType::None)]
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

    /// Support data collection frequency (in seconds).
    ///
    /// This parameter determines how often data should be collected and stored
    /// for support bundles, note that we only actively collect data for running pipelines.
    ///
    /// If set to 0, the continous collection is disabled.
    #[arg(
        long,
        default_value_t = 10 * 60,
        env = "FELDERA_SUPPORT_DATA_COLLECTION_FREQUENCY"
    )]
    pub support_data_collection_frequency: u64,

    /// How many support data collections to keep per pipeline.
    ///
    /// Example: If `--support-data-collection-frequency` is 15 minutes and
    /// `--support-data-retention` is 5, then the support bundle data will
    /// be collected every 15 minutes and the oldest collection will be discarded when
    /// the 6th collection is made.
    #[arg(long, default_value_t = 3, env = "FELDERA_SUPPORT_DATA_RETENTION")]
    pub support_data_retention: u64,

    /// Use the issuer domain as tenant identifier for multi-user deployments.
    ///
    /// When enabled, extracts the subdomain from the JWT issuer claim as the tenant.
    /// For example, "<https://acme-corp.okta.com/oauth2/default>" becomes "acme-corp".
    /// Useful for simple multi-user access without requiring custom tenant claims.
    #[serde(default)]
    #[arg(long, env = "FELDERA_AUTH_ISSUER_TENANT")]
    pub issuer_tenant: bool,

    /// Allow individual user tenants based on the 'sub' claim.
    ///
    /// When true (default), users without explicit tenant or issuer-based tenant
    /// assignment get individual tenants based on their 'sub' claim.
    /// When false, users must have explicit tenant assignment or access is denied.
    /// Set to false for enterprise deployments requiring explicit tenant assignment.
    #[serde(default = "default_individual_tenant")]
    #[arg(long, action = clap::ArgAction::Set, default_value_t = true, env = "FELDERA_AUTH_INDIVIDUAL_TENANT")]
    pub individual_tenant: bool,

    /// Comma-separated list of group names that users must belong to for access.
    /// When specified, a user must have at least one group from the authorized_groups list
    /// present in the `groups` claim of the Access OIDC token to access the platform.
    /// If empty, no group restrictions are applied.
    /// Example: "feldera-users,analytics-team"
    #[serde(default)]
    #[arg(long, value_delimiter = ',', env = "FELDERA_AUTH_AUTHORIZED_GROUPS")]
    pub authorized_groups: Vec<String>,

    /// Expected audience claim value in OIDC Access tokens.
    /// This value must match the 'aud' claim in JWT tokens for successful authentication.
    /// For Okta custom authorization servers, this is typically the API identifier.
    /// Default: "feldera-api"
    #[serde(default = "default_auth_audience")]
    #[arg(long, default_value = "feldera-api", env = "FELDERA_AUTH_AUDIENCE")]
    pub auth_audience: String,
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

    #[cfg(test)]
    pub(crate) fn test_config() -> Self {
        Self {
            support_data_collection_frequency: 1, // 1 second for testing
            support_data_retention: 2,            // Keep 2 collections
            auth_provider: crate::config::AuthProviderType::None,
            dev_mode: false,
            allowed_origins: None,
            telemetry: "test".to_string(),
            demos_dir: vec!["demos".to_string()],
            dump_openapi: false,
            issuer_tenant: false,
            individual_tenant: true,
            authorized_groups: vec![],
            auth_audience: "feldera-api".to_string(),
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
}

impl CompilerConfig {
    pub(crate) fn working_dir(&self) -> PathBuf {
        Path::new(&self.compiler_working_directory).to_path_buf()
    }

    /// Convert all directory paths in the `self` to absolute paths.
    pub fn canonicalize(mut self) -> AnyResult<Self> {
        help_create_dir(&self.compiler_working_directory)?;
        self.compiler_working_directory = help_canonicalize_path(&self.compiler_working_directory)?;
        self.sql_compiler_path = help_canonicalize_path(&self.sql_compiler_path)?;
        self.compilation_cargo_lock_path =
            help_canonicalize_path(&self.compilation_cargo_lock_path)?;
        self.dbsp_override_path = help_canonicalize_path(&self.dbsp_override_path)?;
        Ok(self)
    }
}

#[derive(Parser, Deserialize, Debug, Clone)]
#[command(author, version, about, long_about = None)]
pub struct LocalRunnerConfig {
    /// Directory where the local runner stores its filesystem state:
    /// fetched binaries, configuration files etc.
    #[serde(default = "default_local_runner_working_directory")]
    #[arg(long, default_value_t = default_local_runner_working_directory())]
    pub runner_working_directory: String,
}

impl LocalRunnerConfig {
    /// Creates and converts all directory paths in `self` to absolute paths.
    pub fn canonicalize(mut self) -> AnyResult<Self> {
        help_create_dir(&self.runner_working_directory)?;
        self.runner_working_directory = help_canonicalize_path(&self.runner_working_directory)?;
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_cpu_quantity() {
        assert_eq!(cpu_quantity_to_workers("100m").unwrap(), 1);
        assert_eq!(cpu_quantity_to_workers("999m").unwrap(), 1);
        assert_eq!(cpu_quantity_to_workers("1000m").unwrap(), 1);
        assert_eq!(cpu_quantity_to_workers("1500m").unwrap(), 2);

        assert_eq!(cpu_quantity_to_workers("0").unwrap(), 1);
        assert_eq!(cpu_quantity_to_workers("0.0").unwrap(), 1);
        assert_eq!(cpu_quantity_to_workers("0.0005").unwrap(), 1);
        assert_eq!(cpu_quantity_to_workers("0.1").unwrap(), 1);
        assert_eq!(cpu_quantity_to_workers("0.99").unwrap(), 1);
        assert_eq!(cpu_quantity_to_workers("1.0").unwrap(), 1);
        assert_eq!(cpu_quantity_to_workers("2.1").unwrap(), 3);
        assert_eq!(cpu_quantity_to_workers("2").unwrap(), 2);

        assert!(cpu_quantity_to_workers("foo").is_err());
        assert!(cpu_quantity_to_workers("500x").is_err());
        assert!(cpu_quantity_to_workers("").is_err());
        assert!(cpu_quantity_to_workers("m500").is_err());
        assert!(cpu_quantity_to_workers("500M").is_err()); // uppercase M is invalid
    }
}
