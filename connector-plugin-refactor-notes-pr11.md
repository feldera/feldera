# PR 11 — Manifest-Based Direction Validation: Discrepancies from Plan

### 1. `ManifestCache` must be created before both services, not inside `ServerState`

The plan says "`Arc<ManifestCache>` held in `ServerState`", implying `ServerState` owns its own cache. But the SQL compiler (a separate tokio task) needs to read the same cache to gate program advancement. The fix is to create the `Arc<ManifestCache>` in `pipeline-manager.rs` before spawning either the compiler or the API server, then pass it to both. `ServerState::new` should accept `manifest_cache: Arc<ManifestCache>` rather than constructing one internally. Plan should say "create at the outermost scope and pass to both services".

### 2. Manifest parameter must be threaded through `generate_program_info` and `perform_sql_compilation`

The plan says "replace exhaustive matches at `program.rs:682,735` with manifest lookup" without spelling out that the manifest needs a new parameter all the way down the call chain: `attempt_end_to_end_sql_compilation` → `perform_sql_compilation` → `generate_program_info`. `perform_sql_compilation` also has a second call site in the precompile path (`compiler/main.rs:592`) which needs to pass `build_in_process_manifest()` explicitly. Plan should name both call sites and specify the signature change.

### 3. Manifest availability check happens after `get_next_sql_compilation`, not before

The plan says "compiler loop polls and only advances once the manifest is available." The tenant ID is unknown until after `get_next_sql_compilation` returns a pipeline. The check order is: (1) dequeue the oldest Pending pipeline, (2) look up the manifest for THAT tenant, (3) if not ready, return `Ok(false)` — leaving the pipeline in `Pending` and sleeping the full `POLL_INTERVAL` (250 ms). Returning `Ok(false)` is correct here: the "no work found" sleep applies when a manifest is not yet available too. Plan should clarify the sequencing.

### 4. Test binaries for `pipeline-manager` need `feldera-datagen` force-linked as a dev-dep

`build_in_process_manifest()` walks `inventory::iter` at runtime. The `pipeline-manager` test binary does not transitively link `feldera-datagen` (only the production binary does, via `dbsp_adapters`). Without an explicit `extern crate feldera_datagen as _` in the test module that calls `build_in_process_manifest()`, the datagen descriptor is absent from the test inventory and tests that expect it to be present fail silently with an empty manifest. Fix: add `feldera-datagen = { workspace = true }` to `pipeline-manager`'s `[dev-dependencies]` and `extern crate feldera_datagen as _` at the top of the test file. Plan should note this for any test that exercises `build_in_process_manifest()`.
