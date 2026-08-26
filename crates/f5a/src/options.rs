//! Command line and connection settings shared by both HTTP clients.

use std::path::PathBuf;

use anyhow::{Context, Result, bail};
use clap::Parser;

/// f5a: a k9s-style terminal console for Feldera.
#[derive(Debug, Parser)]
#[command(name = "f5a", version, about)]
pub struct Cli {
    /// Feldera API endpoint.
    #[arg(long, env = "FELDERA_HOST", default_value = "http://127.0.0.1:8080")]
    pub host: String,

    /// API key, sent as a bearer token.
    #[arg(long, env = "FELDERA_API_KEY", hide_env_values = true)]
    pub api_key: Option<String>,

    /// Shell command whose stdout is a bearer token, re-run as the token
    /// ages, e.g. `feldera-tsidp-token` for the internal instances.
    #[arg(long, env = "FELDERA_AUTH_COMMAND", conflicts_with = "api_key")]
    pub auth_command: Option<String>,

    /// Tenant to act in (requires a multi-tenant instance).
    #[arg(long, env = "FELDERA_TENANT")]
    pub tenant: Option<String>,

    /// Accept invalid TLS certificates.
    #[arg(short = 'k', long, env = "FELDERA_TLS_INSECURE")]
    pub insecure: bool,

    /// Per-request timeout in seconds.
    #[arg(long, default_value_t = 15)]
    pub timeout_secs: u64,

    /// Refresh cadence in seconds (change at runtime with :refresh-every).
    #[arg(long, default_value_t = 2)]
    pub refresh_secs: u64,

    /// Ask for confirmation before destructive actions (stop, force-stop,
    /// clear, restart). Without this flag every action runs immediately.
    #[arg(long)]
    pub ask: bool,

    /// Directory that receives downloads such as Samply profiles. Defaults to
    /// the platform's Downloads folder, else the working directory.
    #[arg(long, env = "FELDERA_DOWNLOAD_DIR")]
    pub download_dir: Option<PathBuf>,
}

/// Runtime behavior settings, separate from how to reach the instance.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct UiSettings {
    pub refresh_secs: u64,
    pub ask_before_destructive: bool,
    /// Where downloaded files land; always an existing directory or `.`.
    pub download_dir: PathBuf,
}

impl Cli {
    /// Validated connection options plus the UI settings.
    pub fn into_settings(self) -> Result<(ConnectionOptions, UiSettings)> {
        let options = ConnectionOptions {
            host: self.host,
            api_key: self.api_key,
            auth_command: self.auth_command,
            tenant: self.tenant,
            insecure_tls: self.insecure,
            timeout_secs: self.timeout_secs,
        }
        .validated()?;
        Ok((
            options,
            UiSettings {
                refresh_secs: self.refresh_secs.clamp(1, 60),
                ask_before_destructive: self.ask,
                download_dir: self.download_dir.unwrap_or_else(default_download_dir),
            },
        ))
    }
}

/// The platform's Downloads folder when it exists, else the working
/// directory. Follows each OS's convention: `XDG_DOWNLOAD_DIR` from the XDG
/// user dirs on Linux, `~/Downloads` on macOS, the `Downloads` known folder
/// on Windows.
///
/// ```
/// use f5a::options::default_download_dir;
///
/// assert!(default_download_dir().is_dir());
/// ```
pub fn default_download_dir() -> PathBuf {
    dirs::download_dir()
        .or_else(|| dirs::home_dir().map(|home| home.join("Downloads")))
        .filter(|directory| directory.is_dir())
        .unwrap_or_else(|| PathBuf::from("."))
}

/// How to reach and authenticate against a Feldera instance.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ConnectionOptions {
    /// Base URL, e.g. `https://try.feldera.com`.
    pub host: String,
    /// Static API key, sent as a bearer token.
    pub api_key: Option<String>,
    /// Command minting short-lived bearer tokens; mutually exclusive with
    /// `api_key`.
    pub auth_command: Option<String>,
    /// Tenant to act in, sent as the `Feldera-Tenant` header.
    pub tenant: Option<String>,
    /// Accept invalid TLS certificates.
    pub insecure_tls: bool,
    /// Per-request timeout.
    pub timeout_secs: u64,
}

impl Default for ConnectionOptions {
    fn default() -> Self {
        Self {
            host: "http://127.0.0.1:8080".to_string(),
            api_key: None,
            auth_command: None,
            tenant: None,
            insecure_tls: false,
            timeout_secs: 15,
        }
    }
}

impl ConnectionOptions {
    /// Normalize and validate; refuses API keys on unencrypted remote hosts.
    pub fn validated(mut self) -> Result<Self> {
        self.host = self.host.trim_end_matches('/').to_string();
        if !(self.host.starts_with("https://") || self.host.starts_with("http://")) {
            bail!(
                "host must start with http:// or https:// (got `{}`)",
                self.host
            );
        }
        let has_credentials = self.api_key.is_some() || self.auth_command.is_some();
        if has_credentials && self.host.starts_with("http://") && !self.is_loopback() {
            bail!("refusing to send credentials over unencrypted HTTP to a remote host");
        }
        if self.api_key.is_some() && self.auth_command.is_some() {
            bail!("--api-key and --auth-command are mutually exclusive");
        }
        if self.timeout_secs == 0 {
            bail!("request timeout must be at least one second");
        }
        Ok(self)
    }

    fn is_loopback(&self) -> bool {
        let authority = self
            .host
            .trim_start_matches("http://")
            .trim_start_matches("https://");
        let host_name = authority.split(['/', ':']).next().unwrap_or("");
        matches!(host_name, "localhost" | "127.0.0.1" | "[::1]" | "::1")
    }

    /// Build the shared reqwest client with auth headers installed.
    pub fn build_http_client(&self) -> Result<reqwest::Client> {
        // TLS needs a process-wide crypto provider; repeat installs are no-ops.
        let _ = rustls::crypto::CryptoProvider::install_default(
            rustls::crypto::aws_lc_rs::default_provider(),
        );
        let mut headers = reqwest::header::HeaderMap::new();
        if let Some(api_key) = &self.api_key {
            let mut value = reqwest::header::HeaderValue::from_str(&format!("Bearer {api_key}"))
                .context("API key contains characters not allowed in a header")?;
            value.set_sensitive(true);
            headers.insert(reqwest::header::AUTHORIZATION, value);
        }
        reqwest::ClientBuilder::new()
            .danger_accept_invalid_certs(self.insecure_tls)
            .timeout(std::time::Duration::from_secs(self.timeout_secs))
            .default_headers(headers)
            .build()
            .context("failed to build the HTTP client")
    }
}

#[cfg(test)]
mod tests {
    use super::{Cli, ConnectionOptions};
    use clap::Parser;

    #[test]
    fn cli_defaults_and_flags_parse() {
        // The host may come from FELDERA_HOST in the environment, so only
        // environment-independent defaults are asserted here.
        let cli = Cli::try_parse_from(["f5a", "--host", "http://127.0.0.1:9"]).unwrap();
        let (options, settings) = cli.into_settings().unwrap();
        assert_eq!(options.timeout_secs, 15);
        assert_eq!(settings.refresh_secs, 2);
        assert!(
            !settings.ask_before_destructive,
            "actions run without dialogs by default"
        );
        assert!(settings.download_dir.is_dir());

        let cli = Cli::try_parse_from([
            "f5a",
            "--host",
            "https://try.feldera.com/",
            "--api-key",
            "apikey:x",
            "--tenant",
            "acme",
            "-k",
            "--timeout-secs",
            "9",
            "--refresh-secs",
            "600",
            "--ask",
            "--download-dir",
            "/var/tmp/f5a-profiles",
        ])
        .unwrap();
        let (options, settings) = cli.into_settings().unwrap();
        assert_eq!(options.host, "https://try.feldera.com");
        assert_eq!(options.tenant.as_deref(), Some("acme"));
        assert!(options.insecure_tls);
        assert_eq!(options.timeout_secs, 9);
        assert_eq!(settings.refresh_secs, 60, "cadence clamps to a minute");
        assert!(settings.ask_before_destructive);
        assert_eq!(
            settings.download_dir,
            std::path::PathBuf::from("/var/tmp/f5a-profiles"),
            "an explicit directory is taken as given, even before it exists"
        );
    }

    #[test]
    fn cli_settings_inherit_option_validation() {
        let cli = Cli::try_parse_from(["f5a", "--host", "ftp://x"]).unwrap();
        assert!(cli.into_settings().is_err());
    }

    #[test]
    fn hosts_are_normalized_and_scheme_checked() {
        let options = ConnectionOptions {
            host: "https://try.feldera.com///".to_string(),
            ..Default::default()
        }
        .validated()
        .unwrap();
        assert_eq!(options.host, "https://try.feldera.com");

        let error = ConnectionOptions {
            host: "try.feldera.com".to_string(),
            ..Default::default()
        }
        .validated()
        .unwrap_err();
        assert!(error.to_string().contains("http://"));
    }

    #[test]
    fn api_keys_never_travel_plaintext_to_remote_hosts() {
        let error = ConnectionOptions {
            host: "http://feldera.internal:8080".to_string(),
            api_key: Some("apikey:secret".to_string()),
            ..Default::default()
        }
        .validated()
        .unwrap_err();
        assert!(error.to_string().contains("unencrypted"));

        // Loopback development instances are exempt.
        for loopback in ["http://localhost:8080", "http://127.0.0.1:9090/api"] {
            ConnectionOptions {
                host: loopback.to_string(),
                api_key: Some("apikey:secret".to_string()),
                ..Default::default()
            }
            .validated()
            .unwrap();
        }
    }

    #[test]
    fn auth_command_follows_the_same_credential_rules() {
        // Plaintext to a remote host is refused, loopback is fine.
        let error = ConnectionOptions {
            host: "http://feldera.internal:8080".to_string(),
            auth_command: Some("feldera-tsidp-token".to_string()),
            ..Default::default()
        }
        .validated()
        .unwrap_err();
        assert!(error.to_string().contains("unencrypted"));
        ConnectionOptions {
            host: "https://amd64-eks.staging.feldera.io".to_string(),
            auth_command: Some("feldera-tsidp-token".to_string()),
            ..Default::default()
        }
        .validated()
        .unwrap();

        let error = ConnectionOptions {
            api_key: Some("k".to_string()),
            auth_command: Some("cmd".to_string()),
            ..Default::default()
        }
        .validated()
        .unwrap_err();
        assert!(error.to_string().contains("mutually exclusive"));

        // clap enforces the same exclusion on the command line.
        assert!(Cli::try_parse_from(["f5a", "--api-key", "k", "--auth-command", "cmd"]).is_err());
    }

    #[test]
    fn zero_timeouts_are_rejected() {
        let error = ConnectionOptions {
            timeout_secs: 0,
            ..Default::default()
        }
        .validated()
        .unwrap_err();
        assert!(error.to_string().contains("timeout"));
    }

    #[test]
    fn the_http_client_builds_with_and_without_a_key() {
        ConnectionOptions::default().build_http_client().unwrap();
        ConnectionOptions {
            api_key: Some("apikey:secret".to_string()),
            ..Default::default()
        }
        .build_http_client()
        .unwrap();
    }
}
