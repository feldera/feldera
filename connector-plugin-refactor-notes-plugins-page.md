# Plugins page + describer build log streaming: Discrepancies from Plan

This batch covers the parts of PR 12 (build-log fan-out), PR 14 (the
`GET /v0/connectors/build-log` endpoint), and PR 15 (the `/plugins`
page) that turned out differently than the plan described.

### 1. Reuse `runner::pipeline_logs::start_thread_pipeline_logs` directly — do not invent a broadcast channel

The plan says "tee through a `tokio::sync::mpsc` channel (mirroring the
per-pipeline runtime-log fan-out)". The first useful design was a fresh
`tokio::sync::broadcast`-based `BuildLogBus` with its own buffer.  In
practice the runner already exposes everything needed: `LogsBuffer`,
`LogsSender`, and `start_thread_pipeline_logs` handle the buffer,
follower registry, late-subscriber catch-up, and slow-follower eviction.
The implementation collapsed to a per-tenant registry of
`(LogsSender, follow_request_sender, terminate, JoinHandle)` tuples and
delegated everything else.  The plan should say "construct one
`start_thread_pipeline_logs` per active session per tenant" rather than
"add a broadcast channel".

### 2. `Stdio::from(log_file)` cannot be tee'd — `Stdio::piped()` + reader tasks is a non-trivial refactor

The existing code redirects cargo stdout/stderr at the kernel level via
`Stdio::from(log_file)`, which leaves Rust no opportunity to inspect or
forward the bytes.  Adding a live tail required converting to
`Stdio::piped()` and spawning two tokio tasks that use
`BufReader::lines()` to read line-by-line, then forward to both the
file (open in append mode) and the log bus.  The plan said simply "tee
through a channel" without flagging that the data path itself has to
change.  Flag this as a substantive code restructure, not a one-line
addition.

### 3. `compiler_main` does **not** invoke describer builds — do not thread the bus through it

The plan implicitly suggests passing the bus to the compiler subsystem
("the compiler thread feeds it as cargo emits stdout/stderr").  In the
actual call graph `spawn_describer_build` is called only from API
endpoint handlers (`PUT /v0/connectors/connectors.toml`,
`POST /v0/connectors/refresh`, and the cache-materialise path on
`GET /v0/connectors/status`).  `compiler_main` handles SQL/Rust
*pipeline* compilation, not describer builds.  The bus belongs in
`ServerState` only — the binary entry creates it once and hands the
clone to `api::main::run`.  Do not add it to `compiler_main`'s
signature; the wiring just adds noise and a one-call backout.

### 4. The first line received by every subscriber is the "Fresh start of pipeline logs" preamble

`start_thread_pipeline_logs` writes a control-plane `LogMessage` ("Fresh
start of pipeline logs ...") into its buffer before any pipeline lines.
Tests that assert "the first line received equals my appended line"
fail.  All tests (and frontend log filters that look for specific
content) must drain control-plane lines first.  The convention used in
the new tests is a `wait_for_line(rx, needle)` helper that reads with
a deadline until the needle appears.  Plan should note: either filter
the preamble, or write tests with substring search rather than equality
on the first message.

### 5. `LogsStreamList.svelte` is a *renderer*, not a stream manager — the streaming pipeline must be mirrored from `TabLogs.svelte`

The plan says "render `LogsStreamList.svelte` against the live build
output".  `LogsStreamList` only consumes a static
`{ rows, firstRowIndex, totalSkippedBytes }` object — it does no
streaming, no fetch, no parsing.  All of the actual stream management
(`api.connectorsBuildLogStream` → `parseCancellable` →
`SplitNewlineTransformStream` → `pushAsCircularBuffer` → state) must be
duplicated from `TabLogs.svelte`'s composition.  The plan should say:
"mirror `TabLogs.svelte`'s composition; `LogsStreamList` is the inner
renderer only".

### 6. The frontend must reset the log stream on `Apply`, not only on page unmount

`bus.begin_session(tenant_id)` replaces the prior session and closes
existing subscribers.  When the operator clicks Apply, the page needs
to abort its current `AbortController`, clear the local circular buffer,
and reopen the stream — otherwise the panel sits on the previous
build's log until the page is reloaded.  The plan covered the
PUT → `building` status-dot transition but did not call out the
log-panel reset, which is a separate concern from status polling.

### 7. `utoipa`'s `(body = ErrorResponse)` produces a spurious "unused import" warning

Endpoint annotations like
`(status = INTERNAL_SERVER_ERROR, body = ErrorResponse)` reference
`ErrorResponse` via macro expansion that the rustc import-tracker does
not see, so `use feldera_types::error::ErrorResponse;` is flagged as
unused even though the macro literally requires it to be in scope.
The warning is benign.  Either accept it or annotate with
`#[allow(unused_imports)]`.  The plan's "every endpoint consumed via
`mapResponse` needs a typed error response" item should note the
follow-on lint warning so future implementers don't chase it.

### 8. Describer must seed `Cargo.lock` from the main repo, and use `--offline` — not `--locked`

The original plan treated the describer's lockfile as opaque: cold
builds resolved from scratch, warm builds passed `--locked` against
`describer.lock` plus a cache key keyed only on the Feldera package
version (`describer.build_version`).  Two failure modes:

  • Cold resolution picks the latest registry version of every shared
    transitive (e.g. `schema_registry_converter` 4.8 vs. the platform's
    4.4), so the describer ends up linked against a different version
    of a crate than `dbsp_adapters` was compiled with — the build
    fails inside the platform code.

  • `--locked` rejects any manifest change with `cannot update the
    lock file ... because --locked was passed`, even harmless ones
    (user edits `connectors.toml`, a path-dep version advances).

The right architecture is to mirror what the pipeline workspace
already does: seed `Cargo.lock` from `{dbsp_path}/Cargo.lock` on cold
start, and run with `--offline` rather than `--locked`.  Seeding
inherits every shared-transitive version from the platform build, so
sccache cache hits stay aligned and there is no drift between the
describer and the platform.  `--offline` keeps the no-network
guarantee but still lets Cargo extend the lock from the local
registry cache for the describer's own package and any user-listed
connector crates the seed doesn't cover.  Drop `describer.build_version`
and any `=`-pins for shared transitives in the generated `Cargo.toml` —
the seeded lock is the pin.  The plan should say "describer seeds
its lock from the main repo's Cargo.lock and builds `--offline`".

### 9. Globals crate force-link injection has two SQL-compiler pitfalls

`generate_force_link_rs` always emits `extern crate feldera_datagen as
_;`, but the SQL-compiler-generated globals `Cargo.toml` does **not**
list `feldera-datagen`, and the workspace it sits in does **not**
declare it under `[workspace.dependencies]` — so neither the bare
import nor a `feldera-datagen = { workspace = true }` injection
resolves.  The injection block must use a direct path dep
(`feldera-datagen = { path = "{dbsp_path}/crates/datagen" }`),
mirroring the describer.

The same `lib.rs` file starts with a block of inner attributes
(`#![allow(dead_code)]`, etc.).  Naively prepending `mod force_link;`
at offset 0 displaces those attributes and rustc rejects them with
`an inner attribute is not permitted in this context`.  Splice the
`mod` declaration in *after* the leading prelude (line comments,
inner doc comments, `#![...]`, blank lines), not at the very top.
The plan should call out both pitfalls under "force-link injection",
since the generated globals workspace deviates from the workspace
contract that injecting code typically assumes.

### 10. `bun run generate-openapi` exits non-zero because of unrelated biome lint errors

The npm script runs `openapi-ts && bun run format`, and the format
step fails on pre-existing lint findings in unrelated files (843
warnings, 21 errors at the time of this work).  The SDK files
themselves are generated correctly before the format step runs; you
have to ignore the non-zero exit and confirm the generation succeeded
by grepping `sdk.gen.ts` for the new operation.  The plan's
"regenerate the SDK" item should warn against treating the failed exit
as a generation failure.
