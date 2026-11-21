# Native batch integration with DBSP

This isolated P0 N1 experiment appends a Cranelift-generated expression result to
DBSP rows and feeds them to an incremental join and linear aggregate. Its own
workspace and lockfile keep JIT dependencies out of production crates.

Rows use `Vec<Option<i64>>`, whose existing DBSP `Data` implementation supplies
comparison, hashing and archive support. A runtime column descriptor validates
row widths. The compiled module owns its library and temporary files until the
operator is dropped. Projection crosses the native batch ABI once per bounded
chunk, preserving signed weights and NULLs. Chunking respects the existing native
row/cell limits; overflow reports retain their position across chunk boundaries.

Run on Linux with Rust and a C linker:

```sh
cargo test --locked --manifest-path crates/sql/native-dbsp-probe/Cargo.toml
cargo clippy --locked --manifest-path crates/sql/native-dbsp-probe/Cargo.toml --all-targets -- -D warnings
```

The tests check exact operator deltas for duplicate inserts, NULL keys and values,
left and right retractions, a batch spanning two chunks, schema mismatch and
native overflow. The stateful circuit runs inside a DBSP runtime so its background
spine merger is available. Test assertions deliberately fail on an unexpected
native error; production integration still needs transactional error propagation.

The aggregate is DBSP's linear sum with NULL mapped to zero, which omits zero
weights. It is not SQL SUM's group-presence/NULL contract. This is a manually
assembled operator circuit, not one of N1's ten corpus programs. The final native
row layout, strings, decimals, connectors, SQL lowering, C7 parity, controlled
performance measurements, operator completeness and N2 checkpoint/archive
compatibility remain open. Existing `Vec` archives do not prove compatibility
with archives of generated Rust tuple rows.

The [frozen ten-program inventory](../NATIVE-PROGRAMS.md) now identifies the
actual candidate circuits, 21 weighted steps, 208 explicit type shapes, and 18
operator classes. It includes a derived JSON connector fixture and source-backed
factory/archive requirements. All ten programs pass native replay. A paired
test verifies real JSON parsing and byte-identical encoding for the connector
fixture. [Native execution ledger](../evidence/native-coverage-ledger.json)

## BIGINT checkpoint handoff

The [checkpoint test](tests/checkpoint.rs) runs a Rust-evaluated projection into
a stateful join, forces spill, takes one checkpoint, and destroys that runtime.
A new runtime restores the checkpoint with a freshly compiled native projection.
Five subsequent transactions check exact signed deltas against both explicit
expectations and an uninterrupted Rust oracle, including NULLs and retractions
from both inputs. The initial state contains duplicate weights and a NULL key.

This test and the three existing circuit tests pass in the Linux ARM64
devcontainer; Clippy with warnings denied and Rust formatting also pass. The
[evidence](../evidence/native-checkpoint.json) retains the logs, dependency graph,
and 435 local source/configuration hashes. The tests' temporary checkpoint files
are cleaned up; the archive does not retain their bytes.

The persisted join state uses identical Rust `i64` keys and `Option<i64>` values
on both sides. Input/output rows use `Vec<Option<i64>>`. This demonstrates a
checkpoint handoff while changing expression evaluation. It does not test a
native row encoder against generated tuple archives, the ten required corpus
programs, or the remaining SQL types, and does not close N2.

## Native tuple archive comparison

The [archive experiment](../NATIVE-ARCHIVES.md) now compares bytes written by a
Cranelift batch encoder with six actual integer `Tup1`/`Tup2` shapes from the
frozen inventory. Eight new tests cover complete byte equality, decoding,
archived/live ordering, DBSP hash fidelity, mixed NULL masks, padding, descriptor
refusals and byte ownership after unloading native code. All twelve tests in
this crate and Clippy pass in the devcontainer. This is a bounded N2 result:
full-type archives, native `Data`/factory integration and the ten-program C7
and checkpoint requirements were outside this component test.

## Runtime descriptor ownership

The [native context experiment](../NATIVE-CONTEXT.md) uses DBSP's dynamic row
and archived traits with a view that owns the archive bytes and layout. It
checks encoder lifetime, layout-specific defaults and clone/move operations,
archived/live ordering, and byte and hash compatibility for integer layouts
and full-width nullable decimal keys. The same descriptors can write ordinary
DBSP layers, reopen them without generated Rust factories, and compact signed
updates with DBSP's list merger. The ten case-specific circuits still use the
existing batch factory interfaces.

## Actual native-key checkpoint restore

The [native state experiment](../NATIVE-STATE.md) restores a real tuple-key join
checkpoint into a different native-serialized key type, checks retractions,
writes native spills/checkpoint, unloads the native library, and restores again.
The archived adapter is prebuilt for one `Tup2<i32,i64>` shape. This verifies a
changed live/state representation through actual DBSP storage while leaving the
arbitrary-runtime-layout factory and full ten-program requirements open.

## Decimal and string archives

The [variable-value experiment](../NATIVE-VARIABLE-ARCHIVES.md) adds full-width
decimal mantissa writes, actual `SqlString` inline and relative-pointer archives,
and an actual nullable-decimal checkpoint restored into a native serializer.
The rows come from the frozen ten-program inventory. A generated gap ledger
preserves all ten programs and their operator, expression, stored-type and key
requirements; component checks grant no whole-program C7 credit.

## Frozen end-to-end cases

The [C7 certificates](../evidence/native-json-execution.json) record ten
exact cases and 21 weighted steps against the original generated Rust tests.
The fixture paths come from the [inventory](../NATIVE-PROGRAMS.md). Supply the
case files from that archive and run the five corpus test targets with
`--ignored`. The [full-step archive](../evidence/native-corpus-full-steps-archive.json)
retains the combined devcontainer build logs, executables and results.

| Cargo test target | Fixture environment variables |
| --- | --- |
| `string_range_corpus` | `P0_STRING_PROJECTION_CASE`, `P0_STRING_RANGE_CASE` |
| `left_join_corpus` | `P0_LEFT_JOIN_CASE`, `P0_FULL_JOIN_CASE` |
| `aggregate_corpus` | `P0_NULLABLE_SUM_CASE`, `P0_COUNT_DISTINCT_CASE`, `P0_DECIMAL_ROLLING_CASE` |
| `decimal_join_corpus` | `P0_DECIMAL_ADD_CASE`, `P0_DECIMAL_MULTIPLY_CASE` |
| `json_address_corpus` | `P0_JSON_ADDRESS_CASE` |

For instance, with the two outer-join paths set:

```sh
cargo test --locked --manifest-path crates/sql/native-dbsp-probe/Cargo.toml \
  --test left_join_corpus -- --ignored
```

The production connector, selected rolling checkpoint, and paired runtime
experiments compile alongside the original generated Rust circuits. Their
archives include the exact temporary manifests and fixture sources.
[Connector](../evidence/native-connector-execution-archive.json) ·
[Checkpoint](../evidence/native-rolling-checkpoint-archive.json) ·
[Benchmark](../evidence/native-runtime-benchmark-archive.json)
