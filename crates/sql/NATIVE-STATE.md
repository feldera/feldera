# Native-serialized key checkpoint experiment

A real DBSP join checkpoint written with `Tup2<i32, i64>` keys now restores into
a different live key type, `NativePair`, whose archive bytes are produced by the
Cranelift encoder. It then writes native state, shuts down, unloads its native
library, recompiles the descriptor, and restores its native checkpoint. Six
post-restore transactions match explicit signed bags and an uninterrupted tuple
oracle. **This is one prebuilt layout adapter, not completion of N1/N2 or a generic
runtime-layout factory.** No production DBSP code changed.

## Actual storage path

`Reader` now counts its column factory vector in `SizeOf`, which slightly increases reported memory use.

The [test](native-dbsp-probe/tests/native_state.rs) builds identical join topology
with either the original tuple key or the [native key adapter](native-dbsp-probe/tests/native_state/mod.rs).
Input and output records remain reference tuples; the two join traces use the
selected key type. Both spill thresholds are zero. The original runtime writes
its checkpoint through the ordinary tuple serializer and is destroyed before the
native runtime starts. The circuit fingerprints match; the test neither patches
checkpoint metadata nor bypasses fingerprint validation.

`NativePair` owns a generic cell vector and an `Arc` to the checked layout and
loaded Cranelift encoder. Its rkyv `Serialize` invokes that native encoder and
passes the resulting bytes to `resolve`; `resolve` copies the complete checked
row into rkyv's output allocation. It does not serialize a Rust reference tuple
instead. A transparent archived wrapper supplies the existing factory's static
archived-type metadata. Archived comparisons and decoding are ordinary Rust
adapter functions for this one known layout. The original and native key types
have identical archive size, alignment, complete bytes, ordering and hash
contracts on the tested target.

This lets DBSP's existing `WithFactory` create the concrete and composite
factories, including the stored key/value pairs, without any new storage API.
Defaults acquire this adapter's known layout through a weak cache. Live rows keep
the code owner alive; the cache itself does not retain a strong reference. Weak
reference assertions verify that all native code ownership is released after
each native runtime stops. The second restore uses a newly compiled descriptor.

The final retained run observes 37 native key serializations, 88 deserializations
into native keys and eight archived/live comparisons in the storage path, before
the second restore. These counters exclude the preceding direct archive-contract
checks. Counts can vary with background storage work; the contract assertions
require each path to execute, not an exact count. The resumed checkpoint then
processes a sixth retraction correctly. Tests include duplicate weights,
negative keys, INTEGER/BIGINT extrema, right and left retractions, an absent key,
and new keys introduced after restore.

## Retained evidence and attempts

`$SCRATCH/p0-native-state-v1` retains all attempt logs. The initial compilation failed
because the reference archived tuple lacks `Debug`; the unused derive was
removed. The first execution then passed. The strengthened run adds complete
reference-byte, alignment, hash and archived/live checks, checkpoint file
retention, and full library release/recompile between native runtimes. Initial
Clippy reported five copies written as `.clone()`; these became dereferences.
Final all-target Clippy and formatting checks pass.

The final checkpoint capture contains actual DBSP files:

- `checkpoint-evidence-final/rust-checkpoint`: 11 files, including two 2 KiB
  `.feldera` batch files, checkpoint metadata, spine data and dependency records.
- `checkpoint-evidence-final/native-checkpoint`: 20 files, retaining the original
  checkpoint and the new native checkpoint, with two additional 2 KiB batch
  files. The saved `result.json` records observed path counters and scope.

These are snapshots of the actual storage directories after shutdown, not raw
rows relabeled as checkpoints. Their metadata preserves original paths; the test
restores the original live directories, while the archived copies supply evidence.
Reusing an archived copy elsewhere may require the normal checkpoint relocation
procedure. The evidence archive also retains the final source, locked dependency
metadata, toolchain, test executable and failed logs. No failed source snapshot
or performance measurement is claimed.

## Migration disposition under decision 15

State reuse requires the same persistent IDs, a compatible storage format and
verified restore for every persisted layout used by the pipeline. The prototype
certificates apply to the tested target, feature set, runtime fingerprint and
specific adapter. Apply the [migration preflight](../../PLAN.md#753-state) to the complete pipeline.

| State exercised | Verified restore coverage | Evidence needed to extend reuse |
|---|---|---|
| Fixed INTEGER/BIGINT join keys | The original DBSP tuple checkpoint restores into the native serializer and the native checkpoint restores after relinking | Verify the value layout, index metadata and any additional persisted state of the target pipeline. [Join checkpoint](evidence/native-state.json) |
| Nullable decimal and rolling-window keys | The selected decimal and rolling circuits read classic checkpoints with native-encoded keys | Verify each pipeline's additional window state, retention bounds, row ownership and storage features. [Decimal layouts](evidence/native-variable-archives.json) · [Rolling checkpoint](evidence/native-rolling-checkpoint.json) |
| Nullable SUM and COUNT DISTINCT accumulator weights | The two fixed generated circuits restore in both engine directions and into relinked native circuits at the same worker count on both sides, tested with one and four workers | Verify accumulator descriptors, derived key/value pairs, other aggregate signatures and changes to worker count. [Aggregate checkpoints](evidence/native-aggregate-checkpoints.json) · [Multi-worker restores](evidence/native-multiworker-state.json) |
| Variable-length and implicit composite state | Selected archive bytes, ownership, hashing and ordering have witnesses | Verify the runtime layout's factories and archived views through spill, lookup, clone, drop and reload. Match connector and checkpoint metadata on the target build. [Implicit layouts](evidence/native-implicit-archives.json) · [Native ownership](NATIVE-CONTEXT.md) |

For a pipeline whose state is incompatible, use a preflighted bootstrap from
retained materialized state or replayable inputs, or obtain the owner's
explicit reset approval. If neither is viable, keep the current compiler
family. The preflight records input and sink progress, including how delivery
continues or is reconciled across the change and rollback. A LATENESS policy,
non-materialized input, changed source ID or unreconciled sink can block the
transition. [Decision 15 and migration rules](../../PLAN.md#75-deployment-identity-fallback-and-state)

## What remains

The static archived wrapper supplies the per-layout context that the generic
owned-view experiment still needs to propagate through storage. Adding one Rust
adapter per arbitrary runtime schema would defeat the prebuilt-engine goal.
This test is therefore a bounded proof of the native serializer and real DBSP
checkpoint/search contract, not the final architecture. Arbitrary runtime layouts
still need the owned factory/view design or another reviewed solution.

The six integer layout byte comparisons, this one state adapter, and the earlier
unchanged-Rust-state handoff remain distinct evidence. N2 still requires complete
row/key/state types for the frozen ten-program denominator and normalized-key
sort/merge measurements. The test's manually constructed join is not one of those
ten corpus programs or a complete native compiler. N1's native circuit builder,
full C7, strings/decimals, connector format, operator completeness and required
measurements remain open. These results do not justify either a full state-format
compatibility decision or a claim that native compilation is infeasible.

### Persistent-mode checkpoints

SUM and COUNT DISTINCT restore from classic to native, relinked native,
and back to classic at one and four workers. The rolling decimal-key
fixture restores from classic to native and relinked native. The
experiments run in DBSP persistent mode and compare signed continuations.
Earlier ephemeral-mode measurements remain evidence about the archive
layouts and algorithms. [Persistent interchange evidence](evidence/native-persistent-mode.json)

A separate classic LATENESS circuit confirms the state ID requirement in
G9. A GC-branch delay outside all view input cones writes under its
compiler-assigned ID. Changing only that ID causes DBSP to request
backfill. [Outside-view witness](evidence/state-outside-view-cones.json)

The direct native COUNT DISTINCT builder also saves and reopens its own
checkpoint at four workers. The caller assigns stable state IDs, and the
regression verifies signed retractions after restore. The classic/native
checkpoint interchange continues to use the separately verified archive
adapter. [Direct builder evidence](evidence/native-persistent-count-latency.json)

A four-worker persistent join also exchanges checkpoints between the
classic and native nullable VARCHAR key encoders. It compares three
post-restore signed transactions including NULL, an empty string,
embedded NUL, multibyte Unicode and a heap-backed string. Two long
payloads serialized together at a nonzero offset match the reference
archive byte for byte. The encoder uses the reference serializer above
its one-MiB limit. [String checkpoint evidence](evidence/native-persistent-string-state.json)
