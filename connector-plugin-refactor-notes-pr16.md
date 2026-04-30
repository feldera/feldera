# PR 16 — Reference plugin + end-to-end integration test: Discrepancies from Plan

### 1. Seek datum is a JSON byte-range object, not a raw 8-byte `u64`

The plan says "serialized as 8 bytes" (implying a raw `u64`), but the
implementation uses `{"start": u64, "end": u64}` — a JSON object with two
fields.  The plan's own `Resume::Replay { seek, replay, hash }` description
already implies two offsets are needed (start for replay, end for resume), so
a single `u64` would not have been sufficient.  Using a JSON object keeps the
metadata human-readable and consistent with how the `file` transport stores its
`Metadata { offsets: Range<u64> }`.

### 2. `inventory` must be a direct dependency of the connector crate

The plan claims "Cargo.toml depends only on `feldera-adapterlib`".  In
practice, `register_connector!` expands to `::inventory::submit!`, so
`inventory = "0.3"` must also be listed in the connector's own `Cargo.toml`.
Without it, the compiler emits `E0433: could not find 'inventory'`.

The plan's "depends only on feldera-adapterlib" statement is best understood
as "no `dbsp`, `adapters`, or other heavy Feldera crates" — the lightweight
`inventory`, `serde`, `anyhow`, etc. are expected transitive helpers.

### 3. `[workspace]` table is required in connector-example's `Cargo.toml`

Because the connector lives under `/workspaces/feldera/crates/`, Cargo
automatically associates it with the parent workspace.  A bare `[package]`
without `[workspace]` causes `cargo check` to fail with "current package
believes it's in a workspace when it's not".  Adding an empty `[workspace]`
table makes the crate its own workspace root and prevents the association.

This is an important detail for any connector developed inside a source tree
that already contains a workspace.

### 4. Checkpoint test strategy simplified relative to plan

The plan says "checkpoint after three rows, restart, assert only the remaining
two rows are emitted".  This requires a pipeline-stepping mechanism to pause
the connector at exactly row 3.  The Feldera platform exposes no `/step`
endpoint for running pipelines; stepping is only available in the paused state
and is not exposed via the Python test helpers.

The implemented test instead verifies the _no-duplicate_ property: all 5 rows
are consumed, a checkpoint is taken, the pipeline is force-stopped and
restarted, and the row count must remain 5 (not double to 10).  A connector
that ignores the seek payload would re-read from position 0 and produce 10
rows, so the assertion catches that failure.  The test is marked `@enterprise_only`
because explicit checkpoint is an enterprise feature.

### 5. `default_format` uses `JsonLines::Single`, not `Multiple`

The plan does not specify which `JsonLines` variant to use for the default
JSON format.  `JsonLines::Multiple` (the parser's default) would require the
parser to see a complete JSON object before it emits a record, which happens
to work for whole-object lines but is not the correct semantic.  `Single` is
more precise: each call to `parser.parse()` delivers exactly one
newline-terminated JSON object, avoiding the parser buffering across calls.
