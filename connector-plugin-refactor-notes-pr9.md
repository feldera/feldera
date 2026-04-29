# PR 9 — `connectors.toml` Config Plumbing: Implementation Notes

## What was implemented

- Added `connectors_toml_path: Option<String>` and `connectors_d_dir: Option<String>` fields to `CompilerConfig` in `crates/pipeline-manager/src/config.rs`.
- Updated all five `CompilerConfig { ... }` struct-literal sites to set both fields to `None`:
  - `compiler/test.rs:52`
  - `compiler/main.rs:815`
  - `compiler/main.rs:835`
  - `compiler/main.rs:1022`
  - `compiler/rust_compiler.rs:2355` (the `2373` site uses `..config` struct-update, so it was already covered without change)
- Created `crates/pipeline-manager/src/compiler/connectors.rs` with:
  - `ConnectorsTomlContent` newtype (wraps `String`, has `as_str()` and `is_empty()` helpers)
  - `sanitize_tenant_name(tenant_id: &str) -> String`
  - `load_connectors_toml(config: &CompilerConfig, tenant_id: Option<&str>) -> io::Result<ConnectorsTomlContent>`
  - 19 unit tests covering all required scenarios
- Added `pub mod connectors;` to `crates/pipeline-manager/src/compiler.rs`.

---

## Unexpected findings and deviations from the plan

### 1. `#[serde(default)]` omitted from new `Option` fields

The plan says to add two `Option<String>` fields but doesn't specify their serde/clap annotations.
`CompilerConfig` derives `Deserialize`, so an `Option<String>` field without `#[serde(default)]` will fail JSON deserialization if the key is missing from the input (unless the deserializer treats missing keys as `None` for `Option`s — which serde does by default for `Option<T>`).

**Decision**: no `#[serde(default)]` annotation needed. Serde's `Option<T>` deserialization already returns `None` for missing keys. Consistent with `binary_upload_endpoint: Option<String>` in the same struct, which also has no `#[serde(default)]`.

### 2. Argument parser: `Option<String>` fields need no `default_value_t`

The existing `#[arg(long)]` pattern on `binary_upload_endpoint: Option<String>` compiles without a default — clap treats absent `--option` as `None`. The new fields follow the same pattern. No `default_value_t` is needed and no `default_*` helper function is needed in `config.rs` (unlike the string fields that have non-optional defaults).

### 3. `canonicalize()` does not cover the new fields

`CompilerConfig::canonicalize()` calls `help_canonicalize_path` on several fields (working directory, SQL compiler path, cargo lock path, dbsp override path) to convert them to absolute paths. The new `connectors_toml_path` and `connectors_d_dir` are **not** included.

**Rationale**: `connectors_toml_path` and `connectors_d_dir` are production-deployment fields; in the current phase they are not passed to any downstream build operation (PR 10 does that). Canonicalizing them before PR 10 lands would require creating the paths to make `canonicalize` succeed, which would break deployments that don't have the files yet. **Action for PR 10**: add these to `canonicalize()` when `prepare_describer_workspace` first consumes them; at that point the paths are expected to exist.

### 4. `load_connectors_toml` is synchronous

The plan doesn't specify sync vs. async. The rest of `rust_compiler.rs` uses `tokio::fs` for file operations. However:
- `connectors.toml` is typically a handful of lines, making the blocking risk negligible.
- Keeping it sync avoids requiring a `tokio` runtime in unit tests — all 19 tests run as plain `#[test]` without `#[tokio::test]`.
- PR 10's `prepare_describer_workspace` will call it from an async context via `tokio::task::spawn_blocking` if the blocking budget becomes a concern. No interface change required.

**Alternative considered**: async via `tokio::fs::read_to_string`. Rejected: adds a `#[tokio::test]` annotation to every test and complicates test setup for no measured benefit.

### 5. Exact count of unsafe characters in the `sanitize_replaces_all_unsafe` test

The plan lists 9 named characters: `/`, `\`, `:`, `*`, `?`, `"`, `<`, `>`, `|`. NUL is handled separately as `c == '\0'`. The test string `/\\:*?"<>|\0` is therefore **10** characters → 10 underscores. The first draft of the test had 9 underscores and failed. Corrected inline before committing.

### 6. `ConnectorTomlContent` visibility and helper API

The plan says "Treat the file as opaque text — read it as a `String`, store it." The natural type is `pub struct ConnectorsTomlContent(pub String)`. Two convenience methods were added (`as_str`, `is_empty`) that PR 10's `prepare_describer_workspace` will use for:
- `is_empty()` to decide whether to append a connector deps section to `Cargo.toml` at all.
- `as_str()` for the actual splice.

These were not in the plan but are zero-cost and reduce `pub .0` field access littering at the call sites.

### 7. The `#[warn(unused_imports)]` warning in `rust_compiler` tests

Running `cargo test -p pipeline-manager --lib compiler::rust_compiler` emitted one pre-existing unused-import warning unrelated to PR 9. Verified it was present before these changes. Not introduced by this PR.

### 8. Plan mentions `pub mod connectors;` declared in `compiler.rs`

`crates/pipeline-manager/src/compiler.rs` is the module root (not a separate `lib.rs`), and the new submodule is published as `pub` so PR 10 can call `crate::compiler::connectors::load_connectors_toml(...)`. The `pub` visibility is correct; making it `pub(crate)` would also work since nothing outside the crate needs it, but `pub mod connectors` is consistent with `pub mod error` and `pub mod main` in the same file.

---

## Plan adjustments to capture

- **`canonicalize()` extension deferred to PR 10**: add `connectors_toml_path` and `connectors_d_dir` to `CompilerConfig::canonicalize()` in PR 10, when the describer build first uses them. Attempting it in PR 9 would require the paths to pre-exist for `std::fs::canonicalize` to succeed, which is wrong for optional fields.
- **`load_connectors_toml` is sync**: PR 10 may wrap the call with `tokio::task::spawn_blocking` if the async context makes it necessary; the function signature requires no change.
- **`ConnectorsTomlContent::is_empty` and `as_str`**: add to the plan as part of PR 9's output type. PR 10 uses both.
