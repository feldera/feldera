# Native archive compatibility subset

The Cranelift encoder now writes bytes equal to rkyv's archives of six integer
tuple shapes from the [frozen ten-program inventory](NATIVE-PROGRAMS.md).
This closes a concrete N2 experiment that the earlier checkpoint test did not
exercise: native code writes the archive, without constructing a Rust tuple or
calling its serializer. **N1 and N2 remain open.** No corpus circuit runs in this
experiment, and its values are synthetic boundary/random cases rather than the
corpus update sequences.

| Reference Rust type | Frozen corpus witness |
| --- | --- |
| `Tup1<i32>` | `OptimizedIncrementalTests#leftOuterJoinTest/0`, operator 2 index key |
| `Tup1<Option<i32>>` | `OptimizedIncrementalTests#optionAggregateTest/0`, operator 1 element type |
| `Tup1<i64>` | `OptimizedIncrementalTests#emptyAggregate/0`, operator 2 element type |
| `Tup2<i64, i64>` | `OptimizedIncrementalTests#emptyAggregate/0`, output / operator 4 element type |
| `Tup2<i32, i64>` | `OptimizedIncrementalTests#overDecimalTest/0`, output / operator 6 element type |
| `Tup2<i32, i32>` | `PostgresNumericTests#testAdd/0`, operator 11 index key |

The certificate records full case IDs, type hashes and JSON paths. The similarly
shaped nullable tuple `Option<Tup1<i64>>` in `overDecimalTest` is **not** covered.
The mixed nullable `Tup2<Option<i32>, Option<i64>>` regression is an additional
adversarial shape, not another corpus coverage claim.

## Layout and ownership

[`archive.rs`](native-probe/src/archive.rs) accepts an explicit archive descriptor:
row byte count, integer width, payload offsets and optional NULL-tag offsets.
It rejects out-of-bounds and overlapping writes, empty or oversized descriptors,
NULLs in required fields, mismatched input width and INTEGER overflow. The
bounded prototype supports at most eight integer fields and 256 bytes per row.
Cranelift emits one native batch loop. Rust owns and zero-initializes the output
allocation, including padding; native code writes payloads and option tags.
The loaded library remains owned by the encoder for every call. Returned bytes
remain valid after the encoder, its library and temporary files are destroyed.

The descriptor proves write safety; it does not certify that arbitrary caller
supplied offsets describe a valid rkyv type. The public API therefore returns
owned bytes, without typed archived references or an alignment promise.
The tests obtain offsets and sizes from the actual `Archived<TupN<...>>` types.
For option payloads they inspect a valid `ArchivedOption::Some` value's field
address; the discriminant follows rkyv's `repr(u8)` declaration. Typed reads
occur only after every byte matches rkyv's trusted serialization, and after
copying into an aligned buffer. The encoder itself has no rkyv dependency.

This is target- and feature-specific compatibility, not a stable layout inferred
from SQL types. [`declare_tuple_impl`](../feldera-macros/src/tuples.rs) derives
the legacy rkyv representation only for arities up to eight; wider tuples use
custom bitmap, dense/sparse and relative-pointer layouts. Derived archive fields
can follow Rust layout decisions. A future compiler must preserve or discover
those exact layouts and pin the relevant target, features and storage version.

## Validation

The Linux ARM64 devcontainer passes eight archive tests, the four existing
DBSP circuit/checkpoint tests, twelve native evaluator/key tests, both crates'
all-target Clippy checks with warnings denied, and formatting checks. Runs use
offline locked dependencies and two build jobs. These are correctness checks;
no throughput, memory or compile-time benchmark was performed.

The archive checks compare **2,642 complete row archives**: 1,586 rows across
six selected shapes and 1,056 mixed-nullable robustness rows. Cases include
integer extrema, duplicate zero values, deterministic random values and every
mixed-field NULL mask. For every row, tests verify decoding and archived/live
hash equality using the `Xxh3Default` algorithm and default seed used by DBSP.
They also compare archived/archived and archived/live order against the Rust
ordering for the first 25 reference rows, including NULLs and boundaries.

The first complete test and Clippy runs passed using `DefaultHasher` for the
hash assertion. Strengthening it to DBSP's hasher first failed compilation
because DBSP's `hash` module is private. The repair adds the same already-cached
`xxhash-rust` implementation as a test dependency, and final tests and Clippy
pass. All attempt logs are retained; the source snapshot describes the final
version. No failed execution was discarded or relabeled a pass.

The [certificate](evidence/native-archives.json) retains final source identities,
dependency metadata, successful and failed logs, and executable test binaries.
The tests do not retain every individual row's archive bytes; their deterministic
inputs and native/reference comparison code are frozen for reproduction.

## Remaining feasibility boundary

Matching bytes does not supply DBSP's native `Data`, factory or archived-view
implementation. `FactoryImpl::default_box` currently obtains layout from a
concrete Rust type. Its `archived_value` returns a reference whose lifetime is
borrowed from stored bytes, and `Erase::erase_archived` constructs the archived
trait view through that concrete type. Merely retaining a runtime descriptor in
a new factory does not show how this reference also obtains the descriptor's
context and code lifetime. That boundary needs an explicit implementation and
spill/search tests; embedding a process pointer in persisted bytes would not
preserve this format.

Next, implement the context-carrying factory and archived view, then add nested
rows, strings, decimals and the remaining explicit and implicit state layouts
from all ten programs. Verify faithful archived/live order and hashing or the
supported decode fallback, and owned variable-length clone/drop behavior. Only
then can the actual generated pipelines and native circuits establish C7,
checkpoint restoration across native state representations, normalized-key
coverage and the required measurements. The state-format decision for the full
ten-program denominator remains unresolved; these six shapes alone do not
require a byte-format change on the tested target.

The subsequent [context ownership experiment](NATIVE-CONTEXT.md) implements real
DBSP dynamic traits and a context-carrying owned archive handle for three bounded
layouts. It identifies the factory/storage API extension explicitly and retains
the code owner without persisted pointers. It does not yet integrate that handle
with DBSP batch factories or satisfy the full checkpoint/type requirements above.

The later [decimal/string experiment](NATIVE-VARIABLE-ARCHIVES.md) adds five
inventory row shapes, actual SqlString relative-pointer archives, and a real
nullable-decimal native-serializer checkpoint restore through a prebuilt adapter.
Its [verified evidence](evidence/native-variable-archives.json) preserves those
bounded results; arbitrary-layout factories and full ten-program N2 remain open.
