//! Loading and resolution of `connectors.toml` dependency content.
//!
//! A `connectors.toml` blob lists third-party connector Cargo dependencies,
//! one per line, with no section header. The contents are spliced verbatim
//! into the `[dependencies]` block of the per-pipeline `Cargo.toml` and the
//! describer workspace. Cargo is the authoritative parser; this module
//! treats the blob as opaque text and never calls `toml::from_str`.
//!
//! # Storage and lookup
//!
//! The blob lives in the `tenant_connector_config` database table — one row
//! per tenant. [`load_connectors_toml`] returns the row's `content`,
//! lazily inserting a row on first read using the optional bootstrap seed.
//!
//! The bootstrap seed is the contents of `CompilerConfig::connectors_toml_path`
//! when set (a deployment-wide default file baked into the deployment image),
//! or the empty string otherwise. The seed is consulted only on the very
//! first read for a tenant; thereafter the database is authoritative.

use crate::compiler::build_log_bus::BuildLogBus;
use crate::config::CompilerConfig;
use crate::db::error::DBError;
use crate::db::storage::Storage;
use crate::db::storage_postgres::StoragePostgres;
use crate::db::types::tenant::TenantId;
use sha2::{Digest, Sha256};
use std::io;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncBufReadExt, AsyncRead, BufReader};
use tokio::process::Command;
use tokio::sync::Notify;
use tracing::{error, info, warn};

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

/// Errors that can occur while loading the tenant's connector configuration.
#[derive(Debug, thiserror::Error)]
pub enum ConnectorsConfigError {
    #[error("reading bootstrap seed file: {0}")]
    Io(#[from] io::Error),
    #[error("database error: {0}")]
    Db(#[from] DBError),
}

/// Read the bootstrap seed from `connectors_toml_path` if configured.
///
/// Returns an empty string when the field is unset, when the path does not
/// exist, or when the file is empty. Other I/O errors are surfaced to the
/// caller. The seed is consulted only on the *first* call to
/// [`load_connectors_toml`] for a given tenant; subsequent calls return
/// whatever the database holds.
fn load_seed(config: &CompilerConfig) -> io::Result<String> {
    let Some(path) = config.connectors_toml_path.as_deref() else {
        return Ok(String::new());
    };
    match std::fs::read_to_string(path) {
        Ok(content) => Ok(content),
        Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(String::new()),
        Err(e) => Err(e),
    }
}

/// Load the tenant's connector dependency content from the database,
/// lazily bootstrapping a row from the seed file on first access.
///
/// The returned [`ConnectorsTomlContent`] holds the row's `content` as raw
/// text ready for splicing into a `[dependencies]` block. An empty string
/// is the bundled-connectors-only baseline.
///
/// # Errors
///
/// Returns [`ConnectorsConfigError::Io`] if the bootstrap seed file is
/// configured but unreadable, or [`ConnectorsConfigError::Db`] for any
/// database-layer failure.
pub async fn load_connectors_toml(
    db: &StoragePostgres,
    config: &CompilerConfig,
    tenant_id: TenantId,
) -> Result<ConnectorsTomlContent, ConnectorsConfigError> {
    let seed = load_seed(config)?;
    let row = db
        .get_or_bootstrap_connectors_config(tenant_id, &seed)
        .await?;
    Ok(ConnectorsTomlContent(row.content))
}

// ── Force-link helpers ────────────────────────────────────────────────────────

/// Extract Rust crate identifier names from `connectors.toml` content.
///
/// Each non-blank, non-comment line is expected to contain `<key> = ...`.
/// The substring before the first `=` is trimmed to yield the dep key; `-`
/// characters are replaced with `_` to form a valid Rust identifier.
pub fn extract_force_link_names(content: &ConnectorsTomlContent) -> Vec<String> {
    content
        .as_str()
        .lines()
        .filter_map(|line| {
            let trimmed = line.trim();
            if trimmed.is_empty() || trimmed.starts_with('#') {
                return None;
            }
            let key = trimmed.split('=').next()?.trim().replace('-', "_");
            if key.is_empty() {
                None
            } else {
                Some(key)
            }
        })
        .collect()
}

/// Validate the line-shape of a `connectors.toml` blob without parsing it.
///
/// Each non-blank, non-`#` line must have a non-empty key before the first
/// `=` sign. Cargo is the authoritative parser of the value side; we only
/// reject inputs that the line-based force-link extractor in
/// [`extract_force_link_names`] cannot handle.
///
/// On failure returns `(line_number, reason)` (1-indexed).
pub fn validate_connectors_toml_shape(content: &str) -> Result<(), (u32, String)> {
    for (i, line) in content.lines().enumerate() {
        let line_no = (i as u32) + 1;
        let trimmed = line.trim();
        if trimmed.is_empty() || trimmed.starts_with('#') {
            continue;
        }
        let Some((key, _rest)) = trimmed.split_once('=') else {
            return Err((line_no, format!("missing `=` in `{trimmed}`")));
        };
        if key.trim().is_empty() {
            return Err((line_no, "empty key before `=`".to_string()));
        }
    }
    Ok(())
}

/// Generate the content of `force_link.rs` for the given connector names.
///
/// The file has up to two sections:
/// 1. Built-in section: `extern crate feldera_datagen as _;` — emitted only
///    when `include_builtin_datagen` is `true`. Used by the platform
///    describer (which links `dbsp_adapters` + `feldera-datagen`); the
///    per-tenant user describer omits it because it does not link
///    `feldera-datagen` (the platform manifest already carries datagen).
/// 2. User section: one line per name extracted from `connectors.toml`.
///
/// Placing this file in any compilation unit of the workspace forces the
/// linker to include the rlibs so `inventory::submit!` calls fire.
pub fn generate_force_link_rs(user_names: &[String], include_builtin_datagen: bool) -> String {
    let mut out = String::new();
    if include_builtin_datagen {
        out.push_str(
            "// === Built-in (implicit; not user-configurable) ===\n\
             extern crate feldera_datagen as _;\n",
        );
    }
    if !user_names.is_empty() {
        out.push_str("// === User-listed (from connectors.toml) ===\n");
        for name in user_names {
            out.push_str(&format!("extern crate {name} as _;\n"));
        }
    }
    out
}

// ── Platform manifest (built-in connector descriptors) ───────────────────────
//
// Built-in connectors (the ones shipped in `dbsp_adapters` plus
// `feldera-datagen`) are enumerated **at platform build time** by the
// `feldera-platform-manifest` crate's `build.rs`.  The resulting JSON is
// baked into pipeline-manager's binary via `include_str!`, so startup is
// a const-string read with no Cargo invocation — no first-startup compile
// cost, no on-disk cache to invalidate.  Per-tenant describer builds only
// compile user-listed crates against `feldera-adapterlib`.

/// Load the platform manifest as a `name → entry` map.
///
/// Reads from the build-time-baked
/// [`feldera_platform_manifest::PLATFORM_MANIFEST_JSON`] string.  Falls
/// back to a fresh in-process inventory walk on parse failure (which
/// would only happen if the build.rs emitted malformed JSON — a
/// programming error, not a runtime configuration issue).
pub fn load_platform_manifest(
) -> std::collections::HashMap<String, feldera_adapterlib::connector::ConnectorManifestEntry> {
    match parse_manifest_json(feldera_platform_manifest::PLATFORM_MANIFEST_JSON) {
        Ok(m) => m,
        Err(e) => {
            warn!(
                "failed to parse build-time platform manifest: {e}; \
                 falling back to in-process inventory walk"
            );
            build_in_process_manifest()
        }
    }
}

/// Merge the platform manifest with a per-tenant user manifest.
///
/// On name collisions the platform entry wins — built-in names are
/// reserved.  The user describer runs in an environment that does not
/// link `dbsp_adapters`, so name collisions only happen when a user
/// crate intentionally registers a built-in name; surfacing the
/// collision as a "user entry shadowed" diagnostic is the right
/// behaviour, but is left to a future PR (the merge currently logs at
/// `warn` level and silently keeps the platform entry).
pub fn merge_manifests(
    platform: std::collections::HashMap<String, feldera_adapterlib::connector::ConnectorManifestEntry>,
    user: std::collections::HashMap<String, feldera_adapterlib::connector::ConnectorManifestEntry>,
) -> std::collections::HashMap<String, feldera_adapterlib::connector::ConnectorManifestEntry> {
    let mut out = platform;
    for (name, entry) in user {
        if out.contains_key(&name) {
            warn!(
                "connector '{name}' is registered by both the platform and a \
                 user-listed crate; keeping the platform entry"
            );
            continue;
        }
        out.insert(name, entry);
    }
    out
}

// ── Describer cache key ───────────────────────────────────────────────────────

/// Compute the describer cache key as a 16-character hex prefix of SHA-256.
///
/// The key covers the `connectors.toml` content and
/// [`feldera_types::constants::ADAPTERLIB_API_VERSION`].  Two Feldera
/// deployments that share the same plugin API version and the same
/// `connectors.toml` content share the same cache directory — so upgrading
/// Feldera without changing the plugin ABI does **not** force a connector
/// rebuild.  The key changes only when the ABI version bumps (breaking
/// change) or the `connectors.toml` blob changes.
pub fn describer_cache_key(content: &ConnectorsTomlContent) -> String {
    use feldera_types::constants::ADAPTERLIB_API_VERSION;
    let mut h = Sha256::new();
    h.update(content.as_str().as_bytes());
    h.update(b"\0");
    h.update(ADAPTERLIB_API_VERSION.to_le_bytes());
    let digest = h.finalize();
    // 16 hex chars (8 bytes) — short enough to be readable, long enough to
    // be collision-free in practice for this use case.
    format!("{:016x}", u64::from_be_bytes(digest[..8].try_into().unwrap()))
}

/// Return the path to the describer workspace for the given tenant and
/// cache key.
///
/// Layout: `<working_dir>/describer/<tenant_id>/<cache_key>/` where
/// `cache_key` is [`describer_cache_key`] — a hash of the
/// `connectors.toml` content and [`feldera_types::constants::ADAPTERLIB_API_VERSION`].
/// Tenants share **nothing** in the build cache — each pays its own
/// first-build cost. This is intentional: it eliminates cross-tenant
/// correctness bugs (one tenant's `[patch]` cannot shadow another
/// tenant's deps; one tenant's git-rev pin cannot poison another
/// tenant's resolution).
pub fn describer_workspace_dir(
    config: &CompilerConfig,
    tenant_id: TenantId,
    cache_key: &str,
) -> PathBuf {
    config
        .working_dir()
        .join("describer")
        .join(tenant_id.0.to_string())
        .join(cache_key)
}

// ── Describer workspace preparation ──────────────────────────────────────────

/// Write the user describer workspace files (`Cargo.toml`, `src/main.rs`,
/// `src/force_link.rs`).
///
/// The user describer compiles ONLY `feldera-adapterlib` + the user-listed
/// connector crates from `connectors.toml`.  Built-in connectors live in
/// the platform manifest cache produced by [`bootstrap_platform_manifest`]
/// — pulling `dbsp_adapters` or `feldera-datagen` in here would link the
/// entire built-in connector tree on every `connectors.toml` edit, which
/// is the cost this split eliminates.
///
/// Idempotent: existing files are overwritten only when their content changes.
pub fn prepare_describer_workspace(
    config: &CompilerConfig,
    workspace_dir: &Path,
    content: &ConnectorsTomlContent,
) -> io::Result<()> {
    // Ensure source directory exists.
    let src_dir = workspace_dir.join("src");
    std::fs::create_dir_all(&src_dir)?;

    // Cargo.toml — standalone workspace (no [workspace] parent).
    let connectors_deps = content.as_str();
    let dbsp_path = &config.dbsp_override_path;
    let cargo_toml = format!(
        r#"[workspace]

[package]
name = "feldera-describer"
version = "0.1.0"
edition = "2021"

[[bin]]
name = "feldera-describer"
path = "src/main.rs"

[dependencies]
feldera-adapterlib = {{ path = "{dbsp_path}/crates/adapterlib" }}
serde_json = "1"

# User-listed connectors (from connectors.toml):
{connectors_deps}
"#
    );
    write_if_changed(workspace_dir.join("Cargo.toml"), cargo_toml.as_bytes())?;

    // src/main.rs — collect all registered descriptors, check for duplicates,
    // then print the JSON manifest to stdout.
    let main_rs = r#"mod force_link;

use feldera_adapterlib::connector::{registered_connectors, ConnectorManifestEntry};
use std::collections::HashMap;

fn main() {
    // Duplicate name detection — fail fast with a clear error.
    let mut seen: HashMap<&'static str, bool> = HashMap::new();
    let mut has_dupe = false;
    for d in registered_connectors() {
        if seen.insert(d.name, true).is_some() {
            eprintln!(
                "error: duplicate connector name '{}' — \
                 remove one of the conflicting entries from connectors.toml",
                d.name
            );
            has_dupe = true;
        }
    }
    if has_dupe {
        std::process::exit(1);
    }

    let entries: Vec<ConnectorManifestEntry> = registered_connectors()
        .map(ConnectorManifestEntry::from_descriptor)
        .collect();
    println!("{}", serde_json::to_string(&entries).unwrap());
}
"#;
    write_if_changed(src_dir.join("main.rs"), main_rs.as_bytes())?;

    // src/force_link.rs — forces the linker to pull in user connector
    // rlibs so `inventory::submit!` calls fire.  No built-in section: the
    // user describer does not depend on `feldera-datagen`, and adding an
    // `extern crate` for a crate that is not declared as a dep would fail
    // to compile.
    let user_names = extract_force_link_names(content);
    let force_link_rs = generate_force_link_rs(&user_names, false);
    write_if_changed(src_dir.join("force_link.rs"), force_link_rs.as_bytes())?;

    Ok(())
}

/// Write `bytes` to `path` only if the existing content differs (or `path`
/// does not exist).  Avoids unnecessary filesystem writes that would
/// perturb sccache hashes.
fn write_if_changed(path: impl AsRef<Path>, bytes: &[u8]) -> io::Result<()> {
    let path = path.as_ref();
    if let Ok(existing) = std::fs::read(path) {
        if existing == bytes {
            return Ok(());
        }
    }
    std::fs::write(path, bytes)
}

// ── DescriberError ────────────────────────────────────────────────────────────

/// Error variants produced during the describer build / run pipeline.
#[derive(Debug, thiserror::Error)]
pub enum DescriberError {
    #[error("I/O error while preparing describer workspace: {0}")]
    Io(#[from] io::Error),
    #[error("loading connector configuration: {0}")]
    LoadConfig(#[from] ConnectorsConfigError),
    #[error("cargo build failed:\n{0}")]
    Build(String),
    #[error("describer binary exited with error:\n{0}")]
    Run(String),
    #[error("failed to parse describer output as JSON: {0}")]
    Parse(String),
    #[error("duplicate connector name(s) detected: {0}")]
    DuplicateName(String),
    #[error(
        "connector '{connector_name}' was compiled against \
         feldera-adapterlib API v{required_version}, \
         but this deployment uses v{deployment_version}; \
         recompile the connector against the current feldera-adapterlib"
    )]
    IncompatibleApiVersion {
        connector_name: String,
        required_version: u32,
        deployment_version: u32,
    },
}

// ── Build and run ─────────────────────────────────────────────────────────────

/// Build and run the describer binary in `workspace_dir`, returning the raw
/// JSON manifest string.
///
/// `update_deps` — when `true`, runs `cargo update` before building to
/// refresh the `Cargo.lock` (used by `POST /v0/connectors/refresh`).
pub async fn build_and_run_describer(
    config: &CompilerConfig,
    workspace_dir: &Path,
    update_deps: bool,
    bus: Arc<BuildLogBus>,
    tenant_id: TenantId,
) -> Result<String, DescriberError> {
    let env_path = std::env::var_os("PATH").unwrap_or_default();
    // Begin a fresh log session for this build.  Any prior session for
    // this tenant is replaced — late HTTP subscribers connecting to
    // `GET /v0/connectors/build-log` will see the live tail of *this*
    // build, not a stale one.
    bus.begin_session(tenant_id).await;

    // Lockfile discipline:
    //
    //   • Warm path (`describer.lock` exists, no `update_deps`): copy the
    //     last-good lock as `Cargo.lock`.
    //   • Cold path (no `describer.lock` yet): seed `Cargo.lock` from the
    //     main Feldera repo's `Cargo.lock` at `{dbsp_path}/Cargo.lock`.
    //     This pins every shared transitive (e.g. `schema_registry_converter`,
    //     `mappings`) to the version the platform itself was built against,
    //     which sccache has already cached — eliminating drift between the
    //     describer build and the platform build.
    //
    // The build then runs with `--offline`, which preserves the
    // no-network guarantee while letting Cargo extend the seeded lock
    // from the local registry cache for the describer's own package and
    // any user-listed connector crates that the seed does not cover.
    //
    // When `update_deps` is true, network is required for `cargo update`,
    // so the build step also runs without `--offline` to pick up any
    // newly-resolved crates whose tarballs are not yet cached locally.
    //
    // After every successful build, `describer.lock` is refreshed so the
    // next invocation can warm-start from it.
    let lock_src = workspace_dir.join("Cargo.lock");
    let lock_persistent = workspace_dir.join("describer.lock");
    let main_lock = PathBuf::from(&config.dbsp_override_path).join("Cargo.lock");

    if !update_deps {
        if lock_persistent.exists() {
            std::fs::copy(&lock_persistent, &lock_src)?;
        } else if main_lock.exists() {
            std::fs::copy(&main_lock, &lock_src)?;
        }
    }

    // Build log: both `cargo update` and `cargo build` write to the same
    // file (truncated at the start of the build) so it is available on
    // disk even if the process is killed (e.g. by the timeout in
    // `spawn_describer_build`).  Each line is also tee'd into the
    // `BuildLogBus` so HTTP subscribers see the live tail.
    let log_path = workspace_dir.join("build.log");
    {
        // Truncate at session start.  Subsequent writes from each tee
        // task open the file in append mode.
        std::fs::OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&log_path)
            .map_err(|e| io::Error::new(e.kind(), format!("creating build.log: {e}")))?;
    }

    // Optionally update dependencies first. `cargo update` rewrites
    // `Cargo.lock` even when the blob is unchanged — used by the refresh
    // endpoint to pick up upstream patch releases.
    if update_deps {
        let status = run_cargo_with_tee(
            &["update"],
            workspace_dir,
            &env_path,
            &log_path,
            Arc::clone(&bus),
            tenant_id,
        )
        .await?;
        if !status.success() {
            let log_content = std::fs::read_to_string(&log_path)
                .unwrap_or_else(|_| "(build log unavailable)".to_string());
            return Err(DescriberError::Build(format!(
                "cargo update failed:\n{log_content}"
            )));
        }
    }

    // Build the describer binary.
    let mut build_args: Vec<&str> = vec!["build", "--profile", "dev"];
    if !update_deps {
        build_args.push("--offline");
    }
    let status = run_cargo_with_tee(
        &build_args,
        workspace_dir,
        &env_path,
        &log_path,
        Arc::clone(&bus),
        tenant_id,
    )
    .await?;

    if !status.success() {
        let log_content = std::fs::read_to_string(&log_path)
            .unwrap_or_else(|_| "(build log unavailable)".to_string());
        return Err(DescriberError::Build(log_content));
    }

    // Persist the lock so the next build can warm-start from it.
    if lock_src.exists() {
        std::fs::copy(&lock_src, &lock_persistent)?;
    }

    // Run the describer binary.
    let binary = workspace_dir
        .join("target")
        .join("debug")
        .join("feldera-describer");
    let run_output = Command::new(&binary)
        .current_dir(workspace_dir)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .output()
        .await
        .map_err(|e| io::Error::new(io::ErrorKind::Other, format!("running describer: {e}")))?;

    if !run_output.status.success() {
        let stderr = String::from_utf8_lossy(&run_output.stderr).into_owned();
        if stderr.contains("duplicate connector name") {
            return Err(DescriberError::DuplicateName(stderr));
        }
        return Err(DescriberError::Run(stderr));
    }

    let json = String::from_utf8_lossy(&run_output.stdout).into_owned();

    // Parse and validate the manifest: shape check + API version check.
    check_manifest_api_versions(&json)?;

    // Persist the manifest.
    let manifest_path = workspace_dir.join("manifest.json");
    std::fs::write(&manifest_path, json.as_bytes())?;

    Ok(json)
}

// ── Manifest validation ───────────────────────────────────────────────────────

/// Parse the describer's JSON output and verify that every connector entry
/// was compiled against the current deployment's `feldera-adapterlib` API.
///
/// Returns `Ok(())` when all entries carry
/// [`feldera_types::constants::ADAPTERLIB_API_VERSION`].  Returns
/// [`DescriberError::Parse`] when the JSON is not a valid manifest array, or
/// [`DescriberError::IncompatibleApiVersion`] for the first entry whose
/// `adapterlib_api_version` differs from the deployment's own version.
fn check_manifest_api_versions(json: &str) -> Result<(), DescriberError> {
    use feldera_adapterlib::connector::ConnectorManifestEntry;
    use feldera_types::constants::ADAPTERLIB_API_VERSION;

    let entries: Vec<ConnectorManifestEntry> = serde_json::from_str(json)
        .map_err(|e| DescriberError::Parse(e.to_string()))?;
    for entry in entries {
        if entry.adapterlib_api_version != ADAPTERLIB_API_VERSION {
            return Err(DescriberError::IncompatibleApiVersion {
                connector_name: entry.name,
                required_version: entry.adapterlib_api_version,
                deployment_version: ADAPTERLIB_API_VERSION,
            });
        }
    }
    Ok(())
}

// ── CommandExt for sccache env propagation ────────────────────────────────────

trait CargoCommandExt {
    fn propagate_sccache_env(&mut self) -> &mut Self;
}

impl CargoCommandExt for Command {
    /// Propagate sccache / RUSTFLAGS env from the parent process into this
    /// Cargo invocation.
    ///
    /// RUSTFLAGS handling: the parent's value is forwarded unchanged unless
    /// it contains no `-fuse-ld` directive AND `mold` is present on PATH.  In
    /// that case `-C link-arg=-fuse-ld=mold` is appended, halving link times
    /// for the describer binary.  The mold probe runs on every call so that
    /// CI, dev, and production environments can differ without configuration
    /// changes.  Any existing `-fuse-ld` choice (gold, lld, …) is always
    /// respected.
    fn propagate_sccache_env(&mut self) -> &mut Self {
        for (key, val) in std::env::vars() {
            if key.starts_with("SCCACHE") || key == "RUSTC_WRAPPER" || key == "CARGO_INCREMENTAL" {
                self.env(key, val);
            }
        }
        let parent_flags = std::env::var("RUSTFLAGS").unwrap_or_default();
        let flags = if !parent_flags.contains("-fuse-ld") && mold_is_available() {
            let sep = if parent_flags.is_empty() { "" } else { " " };
            format!("{parent_flags}{sep}-C link-arg=-fuse-ld=mold")
        } else {
            parent_flags
        };
        if !flags.is_empty() {
            self.env("RUSTFLAGS", flags);
        }
        self
    }
}

/// Return `true` when `mold` is installed and responds to `--version`.
fn mold_is_available() -> bool {
    std::process::Command::new("mold")
        .arg("--version")
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .status()
        .map(|s| s.success())
        .unwrap_or(false)
}

// ── Tee'd cargo invocation ───────────────────────────────────────────────────

/// Run `cargo <args>` inside `workspace_dir`, with stdout/stderr tee'd to
/// both the on-disk `log_path` (append mode) and the per-tenant
/// `BuildLogBus` for live streaming.
///
/// Returns the cargo process's exit status.  The caller is responsible
/// for inspecting `status.success()` and converting failure into
/// `DescriberError::Build`.
async fn run_cargo_with_tee(
    args: &[&str],
    workspace_dir: &Path,
    env_path: &std::ffi::OsString,
    log_path: &Path,
    bus: Arc<BuildLogBus>,
    tenant_id: TenantId,
) -> Result<std::process::ExitStatus, DescriberError> {
    let mut child = Command::new("cargo")
        .args(args)
        .current_dir(workspace_dir)
        .env_clear()
        .env("PATH", env_path)
        .propagate_sccache_env()
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true)
        .spawn()
        .map_err(|e| io::Error::new(io::ErrorKind::Other, format!("cargo {args:?}: {e}")))?;

    let stdout = child
        .stdout
        .take()
        .expect("stdout was piped above; take() must succeed");
    let stderr = child
        .stderr
        .take()
        .expect("stderr was piped above; take() must succeed");

    // Cargo's default is to let parallel rustc workers finish after a
    // fatal compile error, which can add minutes of dead time to a
    // cold build.  When the tee tasks see cargo's "could not compile"
    // marker (or the equivalent run-time exit signal), they fire
    // `fatal_error` and the parent kills cargo immediately.
    let fatal_error = Arc::new(Notify::new());

    let log_path_buf = log_path.to_path_buf();
    let bus_for_stdout = Arc::clone(&bus);
    let log_path_for_stdout = log_path_buf.clone();
    let fatal_error_stdout = Arc::clone(&fatal_error);
    let stdout_task = tokio::spawn(async move {
        tee_lines(
            stdout,
            &log_path_for_stdout,
            bus_for_stdout,
            tenant_id,
            fatal_error_stdout,
        )
        .await
    });
    let fatal_error_stderr = Arc::clone(&fatal_error);
    let stderr_task = tokio::spawn(async move {
        tee_lines(stderr, &log_path_buf, bus, tenant_id, fatal_error_stderr).await
    });

    let status = tokio::select! {
        biased;
        status = child.wait() => status,
        _ = fatal_error.notified() => {
            warn!(
                %tenant_id,
                ?args,
                "describer cargo: fatal compile error detected, killing cargo to skip drain of parallel rustc workers"
            );
            let _ = child.start_kill();
            child.wait().await
        }
    }
    .map_err(|e| io::Error::new(io::ErrorKind::Other, format!("cargo {args:?} wait: {e}")))?;
    info!(
        %tenant_id,
        ?args,
        success = status.success(),
        code = ?status.code(),
        "describer cargo: process exited"
    );
    // Ensure both readers have drained any final bytes the child wrote
    // before exiting.  Bound the wait so a stuck pipe (e.g. fd inherited
    // by an orphan rustc subprocess) cannot pin the build task.
    let drain = Duration::from_secs(5);
    let _ = tokio::time::timeout(drain, stdout_task).await;
    let _ = tokio::time::timeout(drain, stderr_task).await;
    Ok(status)
}

/// Return a single-line summary suitable for tracing without flooding
/// the log when the underlying error embeds a multi-MB build log.
fn short_error(e: &DescriberError) -> String {
    let s = e.to_string();
    let first_line = s.lines().next().unwrap_or("").to_string();
    if first_line.len() > 200 {
        format!("{}…", &first_line[..200])
    } else {
        first_line
    }
}

/// Read `reader` line by line.  For each line, append to `log_path` (in
/// append mode) and forward to the build log bus for the active session.
/// When a fatal compile-error marker is observed, signal `fatal_error`
/// so the parent task can kill cargo without waiting for parallel
/// rustc workers to drain.
///
/// Errors writing to the file are logged at `error!` level but do not
/// abort the loop — losing a line in the on-disk log is preferable to
/// silently dropping the live stream.
async fn tee_lines<R: AsyncRead + Unpin>(
    reader: R,
    log_path: &Path,
    bus: Arc<BuildLogBus>,
    tenant_id: TenantId,
    fatal_error: Arc<Notify>,
) {
    let mut file = match std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(log_path)
    {
        Ok(f) => Some(f),
        Err(e) => {
            error!("opening describer build.log for append: {e}");
            None
        }
    };

    let mut buf = BufReader::new(reader);
    let mut line = String::new();
    loop {
        line.clear();
        match buf.read_line(&mut line).await {
            Ok(0) => break, // EOF
            Ok(_) => {}
            Err(e) => {
                error!("reading describer build output: {e}");
                break;
            }
        }
        if let Some(file) = file.as_mut() {
            if let Err(e) = file.write_all(line.as_bytes()) {
                error!("appending to describer build.log: {e}");
            }
        }
        if is_fatal_compile_error(&line) {
            fatal_error.notify_one();
        }
        // The bus strips the trailing newline; pass the line as-is.
        bus.append_line(tenant_id, &line).await;
    }
}

/// Return `true` when `line` is cargo's "give up" marker — once this
/// has been printed, cargo will not start any new compilation units
/// and any in-flight rustc workers are pure dead time.
///
/// Examples (all from cargo / rustc output):
///   * `error: could not compile \`jemalloc_pprof\` (lib) due to 1 previous error`
///   * `error: could not compile \`feldera-describer\` (bin "feldera-describer")`
///
/// We do **not** match individual `error[E0...]:` diagnostics — a
/// single rustc diagnostic does not necessarily fail the unit, and
/// killing cargo on the first diagnostic would suppress useful
/// follow-on errors that operators may want to read in the build log.
fn is_fatal_compile_error(line: &str) -> bool {
    line.starts_with("error: could not compile ")
}

// ── High-level manifest management ───────────────────────────────────────────

/// Return the cached manifest JSON if the workspace for the current cache key
/// already has one; otherwise build the describer and return the fresh manifest.
pub async fn load_or_build_manifest(
    db: &StoragePostgres,
    config: &CompilerConfig,
    tenant_id: TenantId,
    bus: Arc<BuildLogBus>,
) -> Result<String, DescriberError> {
    let content = load_connectors_toml(db, config, tenant_id).await?;
    let cache_key = describer_cache_key(&content);
    let workspace_dir = describer_workspace_dir(config, tenant_id, &cache_key);

    // Return cached manifest when it exists.
    let manifest_path = workspace_dir.join("manifest.json");
    if manifest_path.exists() {
        if let Ok(json) = std::fs::read_to_string(&manifest_path) {
            return Ok(json);
        }
    }

    // Prepare workspace and build.
    prepare_describer_workspace(config, &workspace_dir, &content)?;
    build_and_run_describer(config, &workspace_dir, false, bus, tenant_id).await
}

/// Build a connector manifest from the in-process `inventory` (bundled connectors only).
///
/// Used for tenants with empty `connectors.toml` — no describer build is needed.
pub fn build_in_process_manifest(
) -> std::collections::HashMap<String, feldera_adapterlib::connector::ConnectorManifestEntry> {
    use feldera_adapterlib::connector::{registered_connectors, ConnectorManifestEntry};
    registered_connectors()
        .map(|d| (d.name.to_string(), ConnectorManifestEntry::from_descriptor(d)))
        .collect()
}

/// Parse a connector manifest from the JSON string produced by the describer binary.
///
/// Returns a map from connector name to entry, ready for direction validation.
pub fn parse_manifest_json(
    json: &str,
) -> Result<
    std::collections::HashMap<String, feldera_adapterlib::connector::ConnectorManifestEntry>,
    serde_json::Error,
> {
    use feldera_adapterlib::connector::ConnectorManifestEntry;
    let entries: Vec<ConnectorManifestEntry> = serde_json::from_str(json)?;
    Ok(entries.into_iter().map(|e| (e.name.clone(), e)).collect())
}

/// Force a rebuild of the describer manifest after running `cargo update`.
///
/// Called by `POST /v0/connectors/refresh`.
pub async fn refresh_manifest(
    db: &StoragePostgres,
    config: &CompilerConfig,
    tenant_id: TenantId,
    bus: Arc<BuildLogBus>,
) -> Result<String, DescriberError> {
    let content = load_connectors_toml(db, config, tenant_id).await?;
    let cache_key = describer_cache_key(&content);
    let workspace_dir = describer_workspace_dir(config, tenant_id, &cache_key);

    prepare_describer_workspace(config, &workspace_dir, &content)?;
    build_and_run_describer(config, &workspace_dir, true, bus, tenant_id).await
}

// ── Cache-coordinated background build ────────────────────────────────────

/// Try to begin a describer rebuild and, if the slot was acquired, spawn a
/// background task that runs the build and reports completion to the cache.
///
/// `force` is forwarded both to [`ManifestCache::try_begin_build`] (bypass
/// the `Ready` idempotency check) and to [`build_and_run_describer`] (run
/// `cargo update` first). Used by `POST /v0/connectors/refresh`.
///
/// Returns `true` when a build was launched, `false` when the cache
/// short-circuited (an in-flight or already-fresh build matches).
///
/// # Failure handling
///
/// The build task is wrapped in [`tokio::time::timeout`] using
/// [`CompilerConfig::describer_build_timeout_secs`].  On timeout the cargo
/// subprocess is killed (it was spawned with `kill_on_drop(true)`) and the
/// cache is marked failed.
///
/// A second watcher task awaits the build task's `JoinHandle`.  If the build
/// task panics the watcher calls `mark_failed`, preventing the cache from
/// being permanently stuck in the `Building` state.
pub async fn spawn_describer_build(
    cache: Arc<crate::compiler::manifest_cache::ManifestCache>,
    bus: Arc<BuildLogBus>,
    config: CompilerConfig,
    tenant_id: TenantId,
    content: ConnectorsTomlContent,
    content_hash: String,
    version: i64,
    force: bool,
) -> bool {
    let acquired = cache
        .try_begin_build(tenant_id, content_hash.clone(), version, force)
        .await;
    if !acquired {
        return false;
    }

    // Clone fields needed by the panic-watcher task spawned below.
    let cache_watcher = Arc::clone(&cache);
    let content_hash_watcher = content_hash.clone();
    let timeout_secs = config.describer_build_timeout_secs;

    let content_hash_for_log = content_hash.clone();
    let handle = tokio::spawn(async move {
        info!(%tenant_id, content_hash = %content_hash_for_log, "describer build: starting");
        let cache_key = describer_cache_key(&content);
        let workspace_dir = describer_workspace_dir(&config, tenant_id, &cache_key);
        let build_result = tokio::time::timeout(
            Duration::from_secs(timeout_secs),
            async {
                match prepare_describer_workspace(&config, &workspace_dir, &content) {
                    Err(e) => Err(DescriberError::Io(e)),
                    Ok(()) => {
                        build_and_run_describer(
                            &config,
                            &workspace_dir,
                            force,
                            Arc::clone(&bus),
                            tenant_id,
                        )
                        .await
                    }
                }
            },
        )
        .await;
        let result = match build_result {
            Ok(r) => r,
            Err(_elapsed) => {
                warn!(%tenant_id, "describer build: timed out after {timeout_secs}s");
                Err(DescriberError::Build(format!(
                    "build timed out after {timeout_secs}s"
                )))
            }
        };
        match result {
            Ok(json) => {
                info!(%tenant_id, content_hash = %content_hash_for_log, "describer build: marking ready");
                cache.mark_ready(tenant_id, content_hash, version, json).await
            }
            Err(e) => {
                warn!(
                    %tenant_id,
                    content_hash = %content_hash_for_log,
                    "describer build: marking failed: {}",
                    short_error(&e),
                );
                cache.mark_failed(tenant_id, content_hash, version, e.to_string()).await
            }
        }
    });

    // Await the build handle in a watcher task.  If the build task panics
    // (programming error), the manifest cache would be stuck in `Building`
    // forever without this guard.
    tokio::spawn(async move {
        if let Err(join_err) = handle.await {
            error!(
                %tenant_id,
                "describer build task panicked: {join_err}; marking manifest failed"
            );
            cache_watcher
                .mark_failed(
                    tenant_id,
                    content_hash_watcher,
                    version,
                    "internal error: build task panicked".to_string(),
                )
                .await;
        }
    });

    true
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::CompilerConfig;
    use crate::db::types::program::CompilationProfile;
    use std::fs;
    use tempfile::TempDir;

    /// Build a minimal `CompilerConfig` for testing the seed loader.
    fn make_config(toml_path: Option<&str>) -> CompilerConfig {
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
            describer_build_timeout_secs: 1800,
            precompile: false,
            connectors_toml_path: toml_path.map(str::to_owned),
        }
    }

    // --- load_seed ---

    #[test]
    fn seed_unset_returns_empty() {
        let config = make_config(None);
        assert!(load_seed(&config).unwrap().is_empty());
    }

    #[test]
    fn fatal_compile_error_marker_recognised() {
        // Real cargo output samples — these end the build.
        assert!(is_fatal_compile_error(
            "error: could not compile `jemalloc_pprof` (lib) due to 1 previous error\n"
        ));
        assert!(is_fatal_compile_error(
            "error: could not compile `feldera-describer` (bin \"feldera-describer\")\n"
        ));
        // Non-fatal lines must NOT match: a single rustc diagnostic, a
        // warning, or an unrelated stderr line should let cargo keep
        // running so the rest of the failure context can be captured.
        assert!(!is_fatal_compile_error(
            "error[E0308]: mismatched types\n"
        ));
        assert!(!is_fatal_compile_error("warning: unused variable: `x`\n"));
        assert!(!is_fatal_compile_error("   Compiling foo v0.1.0\n"));
        assert!(!is_fatal_compile_error(""));
    }

    /// `run_cargo_with_tee` must return a non-success status (not hang)
    /// when cargo exits with an error — for example when given an
    /// invalid argument. This guards the no-stuck-Building invariant
    /// the live operator depends on.
    #[tokio::test]
    async fn run_cargo_with_tee_returns_on_cargo_failure() {
        use uuid::Uuid;
        let bus = BuildLogBus::new();
        let tid = TenantId(Uuid::nil());
        let dir = TempDir::new().unwrap();
        let log_path = dir.path().join("build.log");
        let env_path = std::env::var_os("PATH").unwrap_or_default();

        // Cargo with an unknown subcommand exits non-zero in well under
        // a second.  The 30-second timeout catches any reader-task hang.
        let result = tokio::time::timeout(
            Duration::from_secs(30),
            run_cargo_with_tee(
                &["this-subcommand-does-not-exist"],
                dir.path(),
                &env_path,
                &log_path,
                bus,
                tid,
            ),
        )
        .await
        .expect("run_cargo_with_tee must not hang on cargo failure");

        let status = result.expect("expected Ok with non-success exit");
        assert!(!status.success(), "cargo with bad subcommand should exit non-zero");
    }

    #[test]
    fn seed_missing_file_returns_empty() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("nonexistent.toml");
        let config = make_config(Some(path.to_str().unwrap()));
        assert!(load_seed(&config).unwrap().is_empty());
    }

    #[test]
    fn seed_empty_file_returns_empty() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("connectors.toml");
        fs::write(&path, "").unwrap();
        let config = make_config(Some(path.to_str().unwrap()));
        assert!(load_seed(&config).unwrap().is_empty());
    }

    #[test]
    fn cache_key_changes_with_content() {
        let a = ConnectorsTomlContent("acme = \"1.0\"\n".to_string());
        let b = ConnectorsTomlContent("acme = \"2.0\"\n".to_string());
        assert_ne!(describer_cache_key(&a), describer_cache_key(&b));
    }

    #[test]
    fn cache_key_is_stable_for_identical_content() {
        let a = ConnectorsTomlContent("acme = \"1.0\"\n".to_string());
        let b = ConnectorsTomlContent("acme = \"1.0\"\n".to_string());
        assert_eq!(describer_cache_key(&a), describer_cache_key(&b));
    }

    #[test]
    fn workspace_dir_keys_by_tenant_then_hash() {
        // Layout: <working_dir>/describer/<tenant_id>/<content_hash>/.
        // Tenants share nothing in the cache; two tenants with identical
        // content_hash still get distinct directories.
        let config = make_config(None);
        let tenant_a = TenantId(uuid::Uuid::parse_str("00000000-0000-0000-0000-000000000001").unwrap());
        let tenant_b = TenantId(uuid::Uuid::parse_str("00000000-0000-0000-0000-000000000002").unwrap());
        let key = "deadbeefdeadbeef";

        let dir_a = describer_workspace_dir(&config, tenant_a, key);
        let dir_b = describer_workspace_dir(&config, tenant_b, key);
        assert_ne!(dir_a, dir_b);
        assert!(dir_a.ends_with(format!("describer/{}/{key}", tenant_a.0)));
        assert!(dir_b.ends_with(format!("describer/{}/{key}", tenant_b.0)));
    }

    #[test]
    fn tenant_content_update_does_not_affect_other_tenants_path() {
        // When tenant A changes connectors.toml the old workspace dir is left
        // untouched; tenant B's dir never changes regardless.
        //
        // Isolation mechanism: the path is
        //   <working_dir>/describer/<tenant_uuid>/<content_hash>/
        // The tenant UUID component guarantees that even two tenants with byte-
        // identical content live in separate directories and can never interfere
        // with each other's manifest, lockfile, or sccache artifacts.
        let config = make_config(None);
        let tenant_a = TenantId(uuid::Uuid::parse_str("00000000-0000-0000-0000-000000000001").unwrap());
        let tenant_b = TenantId(uuid::Uuid::parse_str("00000000-0000-0000-0000-000000000002").unwrap());

        let content_a_v1 = ConnectorsTomlContent("acme = \"1.0\"\n".to_string());
        let content_a_v2 = ConnectorsTomlContent("acme = \"2.0\"\n".to_string());
        // Tenant B happens to use the same bytes as tenant A's original content.
        let content_b = ConnectorsTomlContent("acme = \"1.0\"\n".to_string());

        let key_a_v1 = describer_cache_key(&content_a_v1);
        let key_a_v2 = describer_cache_key(&content_a_v2);
        let key_b = describer_cache_key(&content_b);

        // Content change for A produces a new cache key.
        assert_ne!(key_a_v1, key_a_v2);
        // B's content is byte-identical to A v1, so they share the cache key.
        assert_eq!(key_a_v1, key_b);

        let dir_a_v1 = describer_workspace_dir(&config, tenant_a, &key_a_v1);
        let dir_a_v2 = describer_workspace_dir(&config, tenant_a, &key_a_v2);
        let dir_b = describer_workspace_dir(&config, tenant_b, &key_b);

        // A PUT by tenant A creates a new directory; the old one is not removed.
        assert_ne!(dir_a_v1, dir_a_v2);

        // Tenant B's directory is always distinct from both of tenant A's
        // directories, even when the cache keys match, because the tenant UUID
        // is embedded in the path.
        assert_ne!(dir_b, dir_a_v1);
        assert_ne!(dir_b, dir_a_v2);
    }

    #[test]
    fn seed_file_read_verbatim() {
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
        let config = make_config(Some(path.to_str().unwrap()));
        assert_eq!(load_seed(&config).unwrap(), content);
    }

    // --- check_manifest_api_versions ---

    fn manifest_json_with_version(version: u32) -> String {
        format!(
            r#"[{{"adapterlib_api_version":{version},"name":"acme","direction":"input","kind":"regular","fault_tolerance":null,"flags_bits":0,"has_build_input":true,"has_build_output":false,"has_build_integrated_input":false,"has_build_integrated_output":false}}]"#
        )
    }

    #[test]
    fn manifest_matching_version_is_accepted() {
        use feldera_types::constants::ADAPTERLIB_API_VERSION;
        let json = manifest_json_with_version(ADAPTERLIB_API_VERSION);
        assert!(check_manifest_api_versions(&json).is_ok());
    }

    #[test]
    fn manifest_future_version_is_rejected() {
        use feldera_types::constants::ADAPTERLIB_API_VERSION;
        let json = manifest_json_with_version(ADAPTERLIB_API_VERSION + 1);
        let err = check_manifest_api_versions(&json).unwrap_err();
        assert!(matches!(err, DescriberError::IncompatibleApiVersion { .. }));
        let msg = err.to_string();
        assert!(msg.contains("acme"));
        assert!(msg.contains(&(ADAPTERLIB_API_VERSION + 1).to_string()));
        assert!(msg.contains(&ADAPTERLIB_API_VERSION.to_string()));
    }

    #[test]
    fn empty_manifest_is_accepted() {
        assert!(check_manifest_api_versions("[]").is_ok());
    }

    #[test]
    fn malformed_manifest_json_returns_parse_error() {
        let err = check_manifest_api_versions("not json").unwrap_err();
        assert!(matches!(err, DescriberError::Parse(_)));
    }
}
