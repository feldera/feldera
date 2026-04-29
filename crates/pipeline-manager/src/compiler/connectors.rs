//! Loading and resolution of `connectors.toml` dependency content.
//!
//! A `connectors.toml` file lists third-party connector Cargo dependencies, one
//! per line, with no section header. The contents are spliced verbatim into the
//! `[dependencies]` block of the per-pipeline `Cargo.toml` and the describer
//! workspace (Phase 8 / PR 10). Cargo is the authoritative parser; this module
//! treats the file as opaque text and never calls `toml::from_str`.
//!
//! # Lookup order
//!
//! 1. Per-tenant file: `<connectors_d_dir>/<sanitized-tenant-id>.toml`, when
//!    both the directory and a tenant ID are available.
//! 2. Global file: `connectors_toml_path`, when set.
//! 3. Empty string — only the bundled connectors compiled into `dbsp_adapters`
//!    are available, matching today's default deployments.
//!
//! A missing file at any step is not an error; lookup falls through to the
//! next option. Any other I/O error is returned to the caller.

use crate::config::CompilerConfig;
use std::io;
use std::path::Path;

/// The raw content of a `connectors.toml` file.
///
/// An empty string is the valid baseline representing no additional connector
/// dependencies beyond the bundled connectors compiled into `dbsp_adapters`.
pub struct ConnectorsTomlContent(pub String);

impl ConnectorsTomlContent {
    /// Returns the raw dependency text, suitable for splicing into a
    /// `[dependencies]` block.
    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// Returns `true` when there are no additional connector dependencies.
    pub fn is_empty(&self) -> bool {
        self.0.trim().is_empty()
    }
}

/// Sanitize a tenant identifier for use as a file-name component.
///
/// Replaces the POSIX+Windows union of unsafe path characters (`/`, `\`, `:`,
/// `*`, `?`, `"`, `<`, `>`, `|`, NUL) with `_`. The `@` character is left
/// unchanged, so an email-style identifier like `user@example.com` maps to
/// `user@example.com.toml`, which is valid on Linux, macOS, and Windows in
/// practice. UUID, simple-string, and domain-name tenant IDs are
/// byte-for-byte identical before and after sanitization.
pub fn sanitize_tenant_name(tenant_id: &str) -> String {
    tenant_id
        .chars()
        .map(|c| {
            if matches!(c, '/' | '\\' | ':' | '*' | '?' | '"' | '<' | '>' | '|') || c == '\0' {
                '_'
            } else {
                c
            }
        })
        .collect()
}

/// Load connector dependency content for the given optional tenant ID.
///
/// The returned [`ConnectorsTomlContent`] holds the raw text ready for
/// splicing into a `[dependencies]` block. An empty string is returned when
/// no file is configured or found.
///
/// # Errors
///
/// Returns an [`io::Error`] if a configured file exists but cannot be read.
/// [`io::ErrorKind::NotFound`] at any lookup step is treated as "absent" and
/// falls through to the next option — it is never surfaced as an error.
pub fn load_connectors_toml(
    config: &CompilerConfig,
    tenant_id: Option<&str>,
) -> io::Result<ConnectorsTomlContent> {
    // 1. Per-tenant file.
    if let (Some(dir), Some(tid)) = (config.connectors_d_dir.as_deref(), tenant_id) {
        let path = Path::new(dir).join(format!("{}.toml", sanitize_tenant_name(tid)));
        match std::fs::read_to_string(&path) {
            Ok(content) => return Ok(ConnectorsTomlContent(content)),
            Err(e) if e.kind() == io::ErrorKind::NotFound => {}
            Err(e) => return Err(e),
        }
    }

    // 2. Global file.
    if let Some(global_path) = config.connectors_toml_path.as_deref() {
        match std::fs::read_to_string(global_path) {
            Ok(content) => return Ok(ConnectorsTomlContent(content)),
            Err(e) if e.kind() == io::ErrorKind::NotFound => {}
            Err(e) => return Err(e),
        }
    }

    // 3. Bundled-only baseline.
    Ok(ConnectorsTomlContent(String::new()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::CompilerConfig;
    use crate::db::types::program::CompilationProfile;
    use std::fs;
    use tempfile::TempDir;

    /// Build a minimal `CompilerConfig` for testing the connectors loader.
    fn make_config(toml_path: Option<&str>, d_dir: Option<&str>) -> CompilerConfig {
        CompilerConfig {
            compiler_working_directory: "/tmp".to_string(),
            compilation_profile: CompilationProfile::Dev,
            sql_compiler_path: String::new(),
            sql_compiler_cache_url: String::new(),
            compilation_cargo_lock_path: String::new(),
            dbsp_override_path: String::new(),
            binary_upload_endpoint: None,
            binary_upload_timeout_secs: 600,
            binary_upload_max_retries: 3,
            binary_upload_retry_delay_ms: 1000,
            precompile: false,
            connectors_toml_path: toml_path.map(str::to_owned),
            connectors_d_dir: d_dir.map(str::to_owned),
        }
    }

    // --- sanitize_tenant_name ---

    #[test]
    fn sanitize_plain_string() {
        assert_eq!(sanitize_tenant_name("acme-corp"), "acme-corp");
    }

    #[test]
    fn sanitize_uuid() {
        let uuid = "550e8400-e29b-41d4-a716-446655440000";
        // UUIDs contain only hex digits and hyphens — no unsafe characters.
        assert_eq!(sanitize_tenant_name(uuid), uuid);
    }

    #[test]
    fn sanitize_email_preserves_at() {
        // `@` must pass through so `user@example.com` stays readable on disk.
        assert_eq!(sanitize_tenant_name("user@example.com"), "user@example.com");
    }

    #[test]
    fn sanitize_replaces_slash() {
        assert_eq!(sanitize_tenant_name("org/tenant"), "org_tenant");
    }

    #[test]
    fn sanitize_replaces_backslash() {
        assert_eq!(sanitize_tenant_name("org\\tenant"), "org_tenant");
    }

    #[test]
    fn sanitize_replaces_colon() {
        assert_eq!(sanitize_tenant_name("org:tenant"), "org_tenant");
    }

    #[test]
    fn sanitize_replaces_nul() {
        assert_eq!(sanitize_tenant_name("org\0tenant"), "org_tenant");
    }

    #[test]
    fn sanitize_replaces_all_unsafe() {
        // 10 unsafe characters → 10 underscores.
        assert_eq!(sanitize_tenant_name("/\\:*?\"<>|\0"), "__________");
    }

    // --- load_connectors_toml ---

    #[test]
    fn no_config_returns_empty() {
        let config = make_config(None, None);
        let result = load_connectors_toml(&config, None).unwrap();
        assert!(result.is_empty());
    }

    #[test]
    fn no_config_with_tenant_returns_empty() {
        let config = make_config(None, None);
        let result = load_connectors_toml(&config, Some("acme")).unwrap();
        assert!(result.is_empty());
    }

    #[test]
    fn global_file_read() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("connectors.toml");
        let content = "acme_snowflake = \"0.3\"\n";
        fs::write(&path, content).unwrap();

        let config = make_config(Some(path.to_str().unwrap()), None);
        let result = load_connectors_toml(&config, None).unwrap();
        assert_eq!(result.as_str(), content);
    }

    #[test]
    fn global_file_missing_returns_empty() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("nonexistent.toml");

        let config = make_config(Some(path.to_str().unwrap()), None);
        let result = load_connectors_toml(&config, None).unwrap();
        assert!(result.is_empty());
    }

    #[test]
    fn empty_global_file_returns_empty() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("connectors.toml");
        fs::write(&path, "").unwrap();

        let config = make_config(Some(path.to_str().unwrap()), None);
        let result = load_connectors_toml(&config, None).unwrap();
        assert!(result.is_empty());
    }

    #[test]
    fn per_tenant_file_takes_priority_over_global() {
        let dir = TempDir::new().unwrap();
        let global_path = dir.path().join("connectors.toml");
        let tenant_dir = dir.path().join("connectors.d");
        fs::create_dir(&tenant_dir).unwrap();

        fs::write(&global_path, "global_dep = \"1.0\"\n").unwrap();
        fs::write(tenant_dir.join("acme.toml"), "tenant_dep = \"2.0\"\n").unwrap();

        let config = make_config(
            Some(global_path.to_str().unwrap()),
            Some(tenant_dir.to_str().unwrap()),
        );
        let result = load_connectors_toml(&config, Some("acme")).unwrap();
        assert_eq!(result.as_str(), "tenant_dep = \"2.0\"\n");
    }

    #[test]
    fn falls_back_to_global_when_tenant_file_missing() {
        let dir = TempDir::new().unwrap();
        let global_path = dir.path().join("connectors.toml");
        let tenant_dir = dir.path().join("connectors.d");
        fs::create_dir(&tenant_dir).unwrap();

        fs::write(&global_path, "global_dep = \"1.0\"\n").unwrap();
        // No per-tenant file for "acme".

        let config = make_config(
            Some(global_path.to_str().unwrap()),
            Some(tenant_dir.to_str().unwrap()),
        );
        let result = load_connectors_toml(&config, Some("acme")).unwrap();
        assert_eq!(result.as_str(), "global_dep = \"1.0\"\n");
    }

    #[test]
    fn tenant_name_with_at_sign_resolves_correctly() {
        let dir = TempDir::new().unwrap();
        let tenant_dir = dir.path().join("connectors.d");
        fs::create_dir(&tenant_dir).unwrap();

        // `@` passes through sanitization unchanged.
        fs::write(
            tenant_dir.join("user@example.com.toml"),
            "email_dep = \"3.0\"\n",
        )
        .unwrap();

        let config = make_config(None, Some(tenant_dir.to_str().unwrap()));
        let result = load_connectors_toml(&config, Some("user@example.com")).unwrap();
        assert_eq!(result.as_str(), "email_dep = \"3.0\"\n");
    }

    #[test]
    fn tenant_name_with_slash_sanitized_to_underscore() {
        let dir = TempDir::new().unwrap();
        let tenant_dir = dir.path().join("connectors.d");
        fs::create_dir(&tenant_dir).unwrap();

        // `org/tenant` sanitizes to `org_tenant`.
        fs::write(tenant_dir.join("org_tenant.toml"), "dep = \"1.0\"\n").unwrap();

        let config = make_config(None, Some(tenant_dir.to_str().unwrap()));
        let result = load_connectors_toml(&config, Some("org/tenant")).unwrap();
        assert_eq!(result.as_str(), "dep = \"1.0\"\n");
    }

    #[test]
    fn cargo_spec_shapes_survive_verbatim() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("connectors.toml");
        // Four spec shapes: plain version, git+rev, path, features array.
        let content = concat!(
            "acme_snowflake = \"0.3\"\n",
            "my_sap_cdc = { git = \"https://github.com/acme/sap-cdc\", rev = \"abc123\" }\n",
            "local_thing = { path = \"/opt/feldera/connectors/local-thing\" }\n",
            "fancy = { version = \"1.0\", features = [\"async\", \"tls\"] }\n",
        );
        fs::write(&path, content).unwrap();

        let config = make_config(Some(path.to_str().unwrap()), None);
        let result = load_connectors_toml(&config, None).unwrap();
        assert_eq!(result.as_str(), content);
    }

    #[test]
    fn no_tenant_id_skips_per_tenant_lookup() {
        let dir = TempDir::new().unwrap();
        let tenant_dir = dir.path().join("connectors.d");
        fs::create_dir(&tenant_dir).unwrap();
        let global_path = dir.path().join("connectors.toml");
        fs::write(&global_path, "global_dep = \"1.0\"\n").unwrap();
        // A per-tenant file exists, but no tenant ID is provided — must be ignored.
        fs::write(tenant_dir.join("anyone.toml"), "should_not_appear = \"0\"\n").unwrap();

        let config = make_config(
            Some(global_path.to_str().unwrap()),
            Some(tenant_dir.to_str().unwrap()),
        );
        let result = load_connectors_toml(&config, None).unwrap();
        assert_eq!(result.as_str(), "global_dep = \"1.0\"\n");
    }
}
