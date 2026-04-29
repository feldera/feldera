# PR 10 — Describer Binary + Lockfile-Based Caching: Discrepancies from Plan

### 1. `ApiError` has no generic `NotFound`/`InternalServerError` variants

The plan sketched 404/500 responses without specifying how to produce them. `ApiError` uses named semantic variants — there is no `NotFound { url }` or `InternalServerError { error }`. Two new variants must be added: `CompilerConfigNotAvailable` (→ 404) and `ConnectorManifestBuildFailed { error: String }` (→ 500), each wired into `error_code`, `Display`, and `ResponseError::status_code`. Plan should specify variant names explicitly or note the naming convention.

### 2. `CargoCommandExt::propagate_sccache_env` must be `&mut self`, not `self`

`tokio::process::Command` builder methods return `&mut Self`, not `Self`. A by-value `fn propagate_sccache_env(self) -> Self` cannot be called on the result of a builder chain because it tries to move from a `&mut` reference. The correct signature is `fn propagate_sccache_env(&mut self) -> &mut Self`. Plan should specify this, or show the method in context of a full chain.

### 3. `ServerState::new` has a second call site in `auth.rs`

The plan notes adding `compiler_config: Option<CompilerConfig>` to `ServerState::new`. There are two call sites: one in `api/main.rs` (obvious) and one inside a `#[cfg(test)]` block in `auth.rs` (easy to miss). The compiler catches the mismatch, but the plan should call out that `ServerState::new` is called in auth test code.

### 4. `std::fs::canonicalize` fails on non-existent optional paths

The plan deferred canonicalization of `connectors_toml_path`/`connectors_d_dir` to PR 10. But `help_canonicalize_path` (used for other fields) calls `std::fs::canonicalize`, which errors when the path doesn't exist — wrong for optional deployment fields. A new helper `help_canonicalize_path_if_exists` is needed that skips canonicalization when the path is absent. Plan should note this distinction for any optional path field.

### 5. `--locked` for describer builds is not straightforward for first builds

The plan mentions `--locked` as a future option without explaining the prerequisite: `cargo build --locked` requires a `Cargo.lock` to already exist at the workspace root. On a first build there is none. The correct sequence is: build freely → persist `Cargo.lock` as `describer.lock` → on subsequent builds, copy `describer.lock` back as `Cargo.lock` before invoking `--locked`. Plan should describe this copy step explicitly or drop `--locked` from the scope.

### 6. `force_link.rs` location: globals crate, not a new crate

The plan was silent on where in the per-pipeline workspace `force_link.rs` should live. A new dedicated crate would require adding `[workspace.members]` plumbing; placing it in the globals crate reuses existing `prepare_workspace` injection with no new moving parts. Plan should specify the globals crate as the target and note that `src_content` for globals must include `("force_link.rs", true)` or `DirectoryContent::validate()` will fail.
