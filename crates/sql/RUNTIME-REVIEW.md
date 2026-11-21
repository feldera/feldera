# Independent P0 runtime review

Reviewed on 2026-11-18 by a separate Codex reviewer, against the uncommitted
workspace. This is an independent technical review, not acceptance by a Feldera
maintainer, a staffing commitment, or a P0 phase decision.

The review found and repaired one reproducible shadow-supervision defect.
Inspection found no demonstrated undefined behavior in the native experiment's
two unsafe boundaries. N1 and N2 remain incomplete experiments; their current
results do not establish native pipeline compatibility or performance.

The subsequent [ten-program inventory](NATIVE-PROGRAMS.md) freezes actual corpus
cases and their complete explicit row/key types before extending the native
prototype. Its strict validation covers ten exports and 21 steps, with 208 type
shapes and 18 operator classes. The source review identifies typed aggregate and
rolling-window factories, descriptor lifetime, archived/live comparisons and
hashing, atomic transaction sums, and variable-size expressions as concrete
remaining work. A derived JSON input fixture preserves Unicode, nullability and
signed multiplicities; it has not been executed through the connector.

## Repaired: a shadow helper could survive its compiler

The SQL runner previously reaped the Java process with `try_wait`, then cancelled
its process-group terminator. A helper in the same group could continue running
after the shadow completed, its directory was removed, and the concurrency slot
was released. The existing timeout test covered a sleeping Java leader, which
does not exercise this exit order.

[Shadow polling](../pipeline-manager/src/compiler/shadow.rs) now observes an
exited leader with `waitid(WNOWAIT)`, terminates the process group, and then lets
Tokio reap the leader. Keeping the leader waitable reserves its numerical process
id until the signal is sent. Killing the group only after `try_wait` would instead
introduce a process-id reuse race. `ECHILD` disables the old guard and reports an
error; it does not signal an id whose ownership can no longer be established.
Interrupted polling retries on the next iteration. Full and IR-only compilation
retain their existing completion behavior.

The review checked Tokio 1.53.1's Unix signal and pidfd reapers: the readiness
drivers wake the child waiter; an owned child is reaped when that waiter is
polled or `try_wait` is called. This loop holds the child exclusively and uses
neither operation before the non-reaping check. Cancellation drops the group
guard before dropping the child, as before.

Two focused regressions pass on Linux ARM64 in the devcontainer and on macOS
ARM64. One verifies that a group member is killed after its leader exits
successfully; the other verifies the externally-reaped-leader error path. The
old wait/cancel sequence fails the first test. Test group members are directly
owned children, so the test reaps them without relying on the container's PID 1.
The macOS extracted harness also passes Clippy with warnings denied.

The [evidence record](evidence/shadow-supervision-review.json) pins the changed
source and archived control/fixed harnesses. These tests compile the exact
extracted supervision code and regression functions, not the entire
pipeline-manager crate.

Subsequent [devcontainer integration validation](evidence/shadow-supervision-integration.json)
passes the full pipeline-manager `cargo check --tests`, all five nonignored
shadow-module tests, and the explicitly selected real-Java shadow integration
test. The latter verifies equality and difference detection, output isolation,
missing-compiler failure, admission limits, and timeout cleanup. Cargo used
`--offline --locked` and two build jobs. The initial attempt failed in the
web-console build script because local Bun workspace packages were unavailable;
the passing runs use the supported `WEBCONSOLE_BUILD_DIR` override with a minimal
static-assets fixture. They do not validate the UI or run every pipeline-manager
test. The archive retains both build attempts, the passing test logs, the frozen
compiler jar, Java sources, and source/binary hashes. Java class and source hashes
were checked again after execution.

## Native ABI and ownership

The experiment keeps essential invariants behind private fields:

- [`Program`](native-probe/src/model.rs) checks column references, topological
  expression references, depth, and expanded expression size before codegen.
- `Batch` owns initialized payloads and NULL masks, checks row widths, and bounds
  rows and cells. Callers cannot mutate its storage after validation.
- [`CompiledProgram`](native-probe/src/runtime.rs) loads only its generated
  object from a private temporary directory. The library owns the function
  pointer and outlives every call. The wrapper supplies disjoint initialized
  output buffers and discards the output on overflow.
- [`codegen.rs`](native-probe/src/codegen.rs) uses the checked row/column bounds,
  emits byte NULL flags, and returns an error record without callbacks or Rust
  unwinding. The native ABI is restricted to 64-bit hosts.

No native implementation change was required by this inspection. It was not a
formal proof, a Miri run, or a rerun of the 6,732-vector sqllib comparison.

## Open measurements and architecture decisions

1. **N1 has no ten-program C7 result.** The
   [DBSP integration](native-dbsp-probe/src/lib.rs) uses `Vec<Option<i64>>`, appends
   one BIGINT expression, and manually assembles a join and linear sum in its
   tests. The linear sum intentionally omits SQL SUM's group-presence/NULL
   contract. SQL/IR lowering, strings, decimals, a connector, the final native row
   descriptor, and controlled throughput/memory/compile-time comparisons are
   still needed. These are stated limits, not failures hidden by the tests.

2. **N2 has no state-format answer.** The key prototype preserves nullable
   BIGINT ordering, but its nine-byte fields are explicitly not an archive
   format. DBSP's existing `Vec` archive support does not prove compatibility
   with generated tuple archives. The native encoder comparison, checkpoint
   restore, and remaining key types must be implemented and measured before
   decision 15 can be supported.

3. **ABI chunk bounds are not process-memory bounds.** `Projection::evaluate`
   collects the entire input Z-set and retains the entire output, in addition
   to each bounded native chunk. Its peak allocation therefore scales with the
   DBSP batch. Measure this in N1; a streaming builder may be needed if the
   approved memory budget requires it. The current README accurately promises
   bounded ABI calls, not constant total memory.

4. **Shadow deployment budgets remain open.** The worker limits concurrency,
   JVM heap/metaspace/direct buffers, individual output-file sizes, and elapsed
   compilation time. Those limits do not impose a hard total RSS quota. The
   documented cgroup budget and telemetry/consent decisions still need an owner
   and measurement. Log outcomes alone do not establish the distinct-program,
   feature-class, or engine-mode populations required for later shadow gates.

The Java-only selector rejects unsupported families and preserves explicit Java
selection. It cannot exercise cross-family state migration, artifact identity,
fallback, checkpoint compatibility, or rollback. Those later contracts remain
design requirements; this review does not count Java-only tests as their proof.

## Full-type bridge and next experiment

The descriptor sketch in PLAN.md section 4.2 omitted two storage-facing
invariants. DBSP's [`OrdRepr`](../dbsp/src/dynamic/ord_repr.rs) compares archived
values with live keys; disagreement with live `Ord` can silently return the
wrong rows. [`HashRepr`](../dbsp/src/dynamic/hash_repr.rs) must reproduce the
decoded hash used by membership filters, or use the existing decode-and-hash
fallback. A compare/hash pair that works only on two live rows is insufficient.
The plan now states these requirements explicitly.

[`Factory`](../dbsp/src/dynamic/factory.rs) also creates default values and
interprets archived bytes. Its current blanket implementation obtains layout
from a concrete Rust type. A generic native row containing values from several
SQL layouts needs a factory that retains each layout and a clear lifetime for
its descriptor and loaded code. Byte equality of a standalone encoder alone
does not establish that this factory can safely interpret old stored batches.
Prototype that boundary before expanding Cranelift's function list.

The next implementation steps are:

1. Freeze the ten corpus programs and every row/key type they use, including
   the required joins, aggregates, string/decimal functions, and connector.
   This defines the prototype's type denominator before implementation.
2. Implement the descriptor/factory bridge for that denominator, with explicit
   NULL and variable-length ownership, archived/live ordering, and faithful or
   fallback hashing. Test spill searches and compare archive bytes with the
   corresponding generated Rust types.
3. Lower those programs' actual IR and input/update sequences into native
   circuits, preserving SQL aggregate group/NULL behavior and runtime errors.
   Run C7 against their generated Rust pipelines, then perform the required
   checkpoint handoff and controlled N1/N2 measurements.

A narrower lifecycle test now passes in
[`native-dbsp-probe/tests/checkpoint.rs`](native-dbsp-probe/tests/checkpoint.rs).
It checkpoints a Rust-evaluated BIGINT join, destroys its runtime, restores the
same state with a freshly compiled native projection, and checks subsequent
NULLs, inserts, and retractions against explicit signed deltas and an
uninterrupted Rust oracle. Its persisted state uses identical Rust `i64` keys
and `Option<i64>` values on both sides; `Vec` is the input/output row container.
The checkpoint test, all three existing circuit tests, and Clippy with warnings
denied pass in the devcontainer on the first attempt. The
[archive and source record](evidence/native-checkpoint.json) retains logs and
435 local source/configuration hashes. Checkpoint bytes were temporary and were
not retained. This establishes the tested lifecycle subset, not generated-tuple
archive compatibility, full-type C7, or the N2 exit.


## Python runtime-negative follow-up

The [local runtime experiment](PYTHON-RUNTIME-DIAGNOSTICS.md) executes all 61
compile-success registered Python negatives against generated catalog circuits.
All match their original expected text with eight workers and exact registered
JSON records/table order. The error stage is a circuit table transaction in every
case; cleanup succeeds. The final binary's empty-input control does not produce
the expected error. This fills the bounded runtime-diagnostic evidence gap while
preserving the distinction from the dormant normal suite and platform transports.

## Direct native archive review, 2026-11-18

The [native archive experiment](NATIVE-ARCHIVES.md) now closes the earlier
standalone-encoder gap for six actual integer tuple shapes. Cranelift writes
2,642 complete archives across the six shapes and one adversarial mixed-nullable
shape; rkyv byte equality precedes typed decoding. Archived/live order and the
DBSP Xxh3 hash agree, and outputs survive code unloading. Eight new tests,
the existing four circuit/checkpoint tests, twelve native tests, Clippy and
formatting pass in the devcontainer. The failed attempt to call a private DBSP
hash helper is retained; the final test uses its identical cached implementation.

The implementation also exposes a remaining architecture boundary: legacy
small tuples use derived layouts, while wide tuples have custom sparse/dense
archives. Explicit integer offsets solve neither that full type denominator nor
`Factory::archived_value`'s need to return a descriptor-aware archived trait
reference tied to stored bytes. The current typed `Erase::erase_archived` path
obtains that context from a concrete Rust type. A generic native row/factory
needs a demonstrated context and lifetime design, not only a matching encoder.
The [source and execution certificate](evidence/native-archives.json) therefore
records a scoped N2 result, with native `Data`, full-type state restoration,
ten-program C7 and performance criteria still open.
