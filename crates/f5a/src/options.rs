//! Command line and connection settings shared by both HTTP clients.

use std::path::{Path, PathBuf};

use anyhow::{Context, Result, bail};
use clap::{Parser, ValueHint};

/// f5a: a k9s-style terminal console for Feldera.
///
/// The connection flags match `fda`'s: same names, environment variables,
/// and rules.
#[derive(Debug, Parser)]
#[command(name = "f5a", version, about)]
pub struct Cli {
    /// The Feldera host to connect to.
    #[arg(
        long,
        env = "FELDERA_HOST",
        value_hint = ValueHint::Url,
        default_value = "https://try.feldera.com"
    )]
    pub host: String,

    /// Accept invalid HTTPS certificates.
    #[arg(short = 'k', long, env = "FELDERA_TLS_INSECURE")]
    pub insecure: bool,

    /// Path to a PEM-encoded certificate to trust as an additional root
    /// certificate authority for HTTPS connections, for a deployment with a
    /// self-signed or private-CA certificate.
    #[arg(
        long = "tls-cert",
        env = "FELDERA_HTTPS_TLS_CERT",
        value_hint = ValueHint::FilePath,
        conflicts_with = "insecure"
    )]
    pub tls_cert: Option<PathBuf>,

    /// Which API key to use for authentication; it starts with `apikey:`.
    /// Sent only over https.
    #[arg(long, env = "FELDERA_API_KEY", hide_env_values = true)]
    pub auth: Option<String>,

    /// File holding a bearer token, typically an OIDC identity token. Sent
    /// only over https. Unlike `fda`, which runs once, f5a re-reads the file
    /// before every request, so whatever keeps it current (a Kubernetes
    /// projected volume, a refresh loop around a token command) rotates the
    /// credential. Conflicts with `--auth`.
    #[arg(
        long,
        env = "FELDERA_OIDC_TOKEN_FILE",
        value_hint = ValueHint::FilePath,
        conflicts_with = "auth"
    )]
    pub oidc_token_file: Option<PathBuf>,

    /// Extra HTTP header to send with every request, spelled `Name: Value`.
    /// Repeat the flag for several headers. A header given here replaces the
    /// one f5a would otherwise send under that name, so
    /// `--header 'Authorization: Bearer ...'` overrides `--auth`.
    #[arg(short = 'H', long = "header", value_name = "NAME: VALUE")]
    pub headers: Vec<String>,

    /// The client timeout for requests in seconds. Unlike `fda`, f5a has a
    /// default: the poller must not wait forever on a dead connection.
    #[arg(long, env = "FELDERA_REQUEST_TIMEOUT", default_value_t = 15)]
    pub timeout: u64,

    /// The tenant to act in, by name or id, sent as the `Feldera-Tenant`
    /// header on every request.
    #[arg(long, env = "FELDERA_TENANT")]
    pub tenant: Option<String>,

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
            insecure_tls: self.insecure,
            tls_cert: self.tls_cert,
            api_key: self.auth,
            oidc_token_file: self.oidc_token_file,
            headers: self.headers,
            timeout_secs: self.timeout,
            tenant: self.tenant,
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
    /// Accept invalid TLS certificates.
    pub insecure_tls: bool,
    /// Extra PEM-encoded root certificates to trust for HTTPS.
    pub tls_cert: Option<PathBuf>,
    /// Static API key, sent as a bearer token.
    pub api_key: Option<String>,
    /// File holding a bearer token, re-read before every request; mutually
    /// exclusive with `api_key`.
    pub oidc_token_file: Option<PathBuf>,
    /// Extra headers, each spelled `Name: Value`, sent on every request.
    pub headers: Vec<String>,
    /// Per-request timeout.
    pub timeout_secs: u64,
    /// Tenant to act in, sent as the `Feldera-Tenant` header.
    pub tenant: Option<String>,
}

impl Default for ConnectionOptions {
    fn default() -> Self {
        Self {
            host: "https://try.feldera.com".to_string(),
            insecure_tls: false,
            tls_cert: None,
            api_key: None,
            oidc_token_file: None,
            headers: Vec::new(),
            timeout_secs: 15,
            tenant: None,
        }
    }
}

/// Header names whose value is a credential, used to warn before one travels
/// over an unencrypted connection.
const SENSITIVE_HEADERS: [&str; 4] = [
    "authorization",
    "cookie",
    "proxy-authorization",
    "x-api-key",
];

impl ConnectionOptions {
    /// Normalize and validate, failing fast on everything the first request
    /// would otherwise trip over: the scheme, the headers, the certificate,
    /// and the token file.
    pub fn validated(mut self) -> Result<Self> {
        self.host = self.host.trim_end_matches('/').to_string();
        if !(self.host.starts_with("https://") || self.host.starts_with("http://")) {
            bail!(
                "host must start with http:// or https:// (got `{}`)",
                self.host
            );
        }
        if self.api_key.is_some() && self.oidc_token_file.is_some() {
            bail!("--auth and --oidc-token-file are mutually exclusive");
        }
        if self.timeout_secs == 0 {
            bail!("request timeout must be at least one second");
        }
        for spec in &self.headers {
            parse_header(spec).map_err(anyhow::Error::msg)?;
        }
        self.root_certificates()?;
        if let Some(path) = self.bearer_token_file() {
            read_oidc_token_file(path).map_err(anyhow::Error::msg)?;
        }
        Ok(self)
    }

    /// Credentials travel over https alone.
    pub fn sends_credentials(&self) -> bool {
        self.host.starts_with("https://")
    }

    /// The token file whose contents authorize each request: none over
    /// plain HTTP, and none when `--header` sets `Authorization` itself.
    pub fn bearer_token_file(&self) -> Option<&Path> {
        self.oidc_token_file.as_deref().filter(|_| {
            self.sends_credentials()
                && !self.overrides_header(reqwest::header::AUTHORIZATION.as_str())
        })
    }

    /// Whether `--header` sets `name`, which then wins over what f5a sends.
    pub fn overrides_header(&self, name: &str) -> bool {
        self.headers.iter().any(|spec| {
            parse_header(spec).is_ok_and(|(header, _)| header.as_str().eq_ignore_ascii_case(name))
        })
    }

    /// What the user should know before the console starts: credentials
    /// left out over plain HTTP, and credentials sent in the clear.
    pub fn warnings(&self) -> Vec<String> {
        if self.sends_credentials() {
            return Vec::new();
        }
        let mut warnings = Vec::new();
        if self.api_key.is_some() || self.oidc_token_file.is_some() {
            warnings.push(format!(
                "The provided credentials are not added to the request because {} does not use `https`.",
                self.host
            ));
        }
        for (name, _) in self
            .headers
            .iter()
            .filter_map(|spec| parse_header(spec).ok())
        {
            if SENSITIVE_HEADERS.contains(&name.as_str()) {
                warnings.push(format!(
                    "Header `{name}` is sent in the clear because {} does not use `https`.",
                    self.host
                ));
            }
        }
        warnings
    }

    /// The certificates of `--tls-cert`, read and parsed.
    fn root_certificates(&self) -> Result<Vec<reqwest::Certificate>> {
        let Some(path) = &self.tls_cert else {
            return Ok(Vec::new());
        };
        let pem = std::fs::read(path)
            .with_context(|| format!("Failed to read TLS certificate file `{}`", path.display()))?;
        let certificates = reqwest::Certificate::from_pem_bundle(&pem).with_context(|| {
            format!(
                "Failed to parse TLS certificate file `{}` as PEM",
                path.display()
            )
        })?;
        if certificates.is_empty() {
            bail!(
                "TLS certificate file `{}` did not contain any PEM-encoded certificates",
                path.display()
            );
        }
        Ok(certificates)
    }

    /// Build an HTTP client with the TLS settings, the timeout, and the
    /// headers every request carries: `bearer` when credentials travel,
    /// `extra` next, and `--header` last so it overrides both.
    pub fn build_client(
        &self,
        bearer: Option<&str>,
        extra: reqwest::header::HeaderMap,
    ) -> Result<reqwest::Client> {
        // TLS needs a process-wide crypto provider; repeat installs are no-ops.
        let _ = rustls::crypto::CryptoProvider::install_default(
            rustls::crypto::aws_lc_rs::default_provider(),
        );
        let mut headers = reqwest::header::HeaderMap::new();
        if let Some(token) = bearer.filter(|_| self.sends_credentials()) {
            let mut value = reqwest::header::HeaderValue::from_str(&format!("Bearer {token}"))
                .context("the credential contains characters not allowed in a header")?;
            value.set_sensitive(true);
            headers.insert(reqwest::header::AUTHORIZATION, value);
        }
        headers.extend(extra);
        for spec in &self.headers {
            let (name, value) = parse_header(spec).map_err(anyhow::Error::msg)?;
            headers.insert(name, value);
        }
        let mut builder = reqwest::ClientBuilder::new()
            .danger_accept_invalid_certs(self.insecure_tls)
            .timeout(std::time::Duration::from_secs(self.timeout_secs))
            .default_headers(headers);
        for certificate in self.root_certificates()? {
            builder = builder.add_root_certificate(certificate);
        }
        builder.build().context("failed to build the HTTP client")
    }

    /// The client for raw reads. The tenant and a token file's bearer token
    /// are added per request, because both change at runtime.
    pub fn build_http_client(&self) -> Result<reqwest::Client> {
        let mut extra = reqwest::header::HeaderMap::new();
        // Profiles and stats compress about 10x; `Http` decodes the bodies.
        extra.insert(
            reqwest::header::ACCEPT_ENCODING,
            reqwest::header::HeaderValue::from_static("gzip"),
        );
        self.build_client(self.api_key.as_deref(), extra)
    }
}

/// Parse one `--header` argument the way `fda` and `curl -H` do: a name, a
/// colon, and the value, with surrounding whitespace removed. The value is
/// marked sensitive so a cookie or token passed this way stays out of logs.
pub fn parse_header(
    spec: &str,
) -> std::result::Result<(reqwest::header::HeaderName, reqwest::header::HeaderValue), String> {
    let (name, value) = spec
        .split_once(':')
        .ok_or_else(|| format!("Invalid --header `{spec}`: expected `Name: Value`"))?;
    let name = reqwest::header::HeaderName::from_bytes(name.trim().as_bytes())
        .map_err(|error| format!("Invalid --header `{spec}`: {error}"))?;
    let mut value = reqwest::header::HeaderValue::from_str(value.trim())
        .map_err(|error| format!("Invalid --header `{spec}`: {error}"))?;
    value.set_sensitive(true);
    Ok((name, value))
}

/// The bearer token in `path`, trimmed, with `fda`'s checks: the file must
/// be readable, hold something besides whitespace, and hold a single line.
pub fn read_oidc_token_file(path: &Path) -> std::result::Result<String, String> {
    let token = std::fs::read_to_string(path).map_err(|error| {
        format!(
            "failed to read OIDC token file `{}`: {error}",
            path.display()
        )
    })?;
    check_oidc_token(path, &token)
}

/// Validate the contents of a token file, read by whatever means.
pub fn check_oidc_token(path: &Path, contents: &str) -> std::result::Result<String, String> {
    let token = contents.trim();
    if token.is_empty() {
        return Err(format!("OIDC token file `{}` is empty", path.display()));
    }
    // The header rejects the same set; checking here names the file.
    if let Some(offset) =
        token.find(|character: char| character.is_ascii_control() && character != '\t')
    {
        let found = match token.as_bytes()[offset] {
            b'\n' | b'\r' => "a line break",
            _ => "a control character",
        };
        return Err(format!(
            "OIDC token file `{}` does not hold a single-line token: found {found} at byte {offset}",
            path.display()
        ));
    }
    Ok(token.to_string())
}

#[cfg(test)]
mod tests {
    use super::{Cli, ConnectionOptions, check_oidc_token, parse_header, read_oidc_token_file};
    use clap::Parser;
    use std::path::{Path, PathBuf};

    /// A file under the temp directory with `contents`, unique per test.
    fn scratch_file(name: &str, contents: &str) -> PathBuf {
        let path = std::env::temp_dir().join(format!("f5a-options-{}-{name}", std::process::id()));
        std::fs::write(&path, contents).unwrap();
        path
    }

    #[test]
    fn cli_flags_match_fda() {
        // FELDERA_* variables in the environment may set defaults, so only
        // flags given here are asserted.
        let token = scratch_file("token", "header.payload.signature\n");
        let cli = Cli::try_parse_from([
            "f5a",
            "--host",
            "https://try.feldera.com/",
            "--oidc-token-file",
            token.to_str().unwrap(),
            "-H",
            "Cookie: session=1",
            "--header",
            "X-Trace: on",
            "--tenant",
            "acme",
            "-k",
            "--timeout",
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
        assert_eq!(options.oidc_token_file.as_deref(), Some(token.as_path()));
        assert_eq!(options.headers, vec!["Cookie: session=1", "X-Trace: on"]);
        assert_eq!(options.tenant.as_deref(), Some("acme"));
        assert!(options.insecure_tls);
        assert_eq!(options.timeout_secs, 9);
        assert_eq!(settings.refresh_secs, 60, "cadence clamps to a minute");
        assert!(settings.ask_before_destructive);
        assert_eq!(
            settings.download_dir,
            PathBuf::from("/var/tmp/f5a-profiles"),
            "an explicit directory is taken as given, even before it exists"
        );
        let cli = Cli::try_parse_from(["f5a", "--auth", "apikey:x"]).unwrap();
        assert_eq!(cli.auth.as_deref(), Some("apikey:x"));
        assert!(
            Cli::try_parse_from(["f5a", "--auth", "k", "--oidc-token-file", "t"]).is_err(),
            "fda's exclusion"
        );
        assert!(Cli::try_parse_from(["f5a", "-k", "--tls-cert", "c.pem"]).is_err());
        let _ = std::fs::remove_file(token);
    }

    #[test]
    fn defaults_match_fda_except_the_timeout() {
        let options = ConnectionOptions::default();
        assert_eq!(options.host, "https://try.feldera.com");
        assert_eq!(options.timeout_secs, 15, "the poller needs a bound");
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
    fn credentials_travel_over_https_alone_with_a_warning_otherwise() {
        let plain = ConnectionOptions {
            host: "http://feldera.internal:8080".to_string(),
            api_key: Some("apikey:secret".to_string()),
            headers: vec!["Cookie: session=1".to_string(), "X-Trace: on".to_string()],
            ..Default::default()
        }
        .validated()
        .unwrap();
        assert!(!plain.sends_credentials());
        let warnings = plain.warnings();
        assert_eq!(warnings.len(), 2, "{warnings:?}");
        assert!(warnings[0].contains("not added to the request"));
        assert!(warnings[1].contains("`cookie` is sent in the clear"));
        assert!(
            ConnectionOptions {
                api_key: Some("apikey:secret".to_string()),
                ..Default::default()
            }
            .warnings()
            .is_empty()
        );
    }

    #[test]
    fn a_token_file_authorizes_only_over_https_and_without_a_header_override() {
        let token = scratch_file("bearer", "t0k3n");
        let options = |host: &str, headers: Vec<String>| ConnectionOptions {
            host: host.to_string(),
            oidc_token_file: Some(token.clone()),
            headers,
            ..Default::default()
        };
        assert_eq!(
            options("https://x", Vec::new()).bearer_token_file(),
            Some(token.as_path())
        );
        assert_eq!(options("http://x", Vec::new()).bearer_token_file(), None);
        assert_eq!(
            options("https://x", vec!["authorization: Bearer mine".to_string()])
                .bearer_token_file(),
            None,
            "--header Authorization wins"
        );
        let error = ConnectionOptions {
            api_key: Some("k".to_string()),
            ..options("https://x", Vec::new())
        }
        .validated()
        .unwrap_err();
        assert!(error.to_string().contains("mutually exclusive"));
        let missing = ConnectionOptions {
            oidc_token_file: Some(PathBuf::from("/nonexistent/f5a-token")),
            ..Default::default()
        }
        .validated()
        .unwrap_err();
        assert!(
            missing
                .to_string()
                .contains("failed to read OIDC token file")
        );
        let _ = std::fs::remove_file(token);
    }

    #[test]
    fn token_files_hold_one_trimmed_line() {
        let path = Path::new("t");
        assert_eq!(check_oidc_token(path, "  abc \n").unwrap(), "abc");
        assert!(
            check_oidc_token(path, " \n")
                .unwrap_err()
                .contains("is empty")
        );
        assert!(
            check_oidc_token(path, "a\nb")
                .unwrap_err()
                .contains("found a line break at byte 1")
        );
        assert!(
            check_oidc_token(path, "a\u{7}b")
                .unwrap_err()
                .contains("a control character")
        );
        let file = scratch_file("rotating", "first");
        assert_eq!(read_oidc_token_file(&file).unwrap(), "first");
        std::fs::write(&file, "second").unwrap();
        assert_eq!(read_oidc_token_file(&file).unwrap(), "second", "read anew");
        let _ = std::fs::remove_file(file);
    }

    #[test]
    fn headers_parse_like_curl_and_fda() {
        let (name, value) = parse_header(" X-Trace :  on ").unwrap();
        assert_eq!((name.as_str(), value.to_str().unwrap()), ("x-trace", "on"));
        assert!(value.is_sensitive());
        assert!(
            parse_header("no colon")
                .unwrap_err()
                .contains("expected `Name: Value`")
        );
        assert!(
            parse_header("bad name: x")
                .unwrap_err()
                .contains("Invalid --header")
        );
        let error = ConnectionOptions {
            headers: vec!["nope".to_string()],
            ..Default::default()
        }
        .validated()
        .unwrap_err();
        assert!(error.to_string().contains("Invalid --header"));
    }

    #[test]
    fn tls_certificates_fail_fast_with_fda_messages() {
        let with_cert = |path: PathBuf| ConnectionOptions {
            tls_cert: Some(path),
            ..Default::default()
        };
        let missing = with_cert(PathBuf::from("/nonexistent/f5a.pem"))
            .validated()
            .unwrap_err();
        assert!(
            missing
                .to_string()
                .contains("Failed to read TLS certificate file")
        );
        let empty = scratch_file("empty.pem", "not a certificate");
        let error = with_cert(empty.clone()).validated().unwrap_err();
        assert!(
            error
                .to_string()
                .contains("did not contain any PEM-encoded certificates")
                || error.to_string().contains("as PEM"),
            "{error}"
        );
        let _ = std::fs::remove_file(empty);
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
    fn the_http_client_builds_with_and_without_credentials() {
        ConnectionOptions::default().build_http_client().unwrap();
        ConnectionOptions {
            api_key: Some("apikey:secret".to_string()),
            headers: vec!["Authorization: Bearer override".to_string()],
            ..Default::default()
        }
        .build_http_client()
        .unwrap();
    }
}
