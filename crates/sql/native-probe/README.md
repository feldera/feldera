# Native batch experiment

This isolated P0 N1 crate compiles a bounded nullable BIGINT expression graph
with Cranelift, emits a position-independent object, links it with `cc`, and
loads the shared library through `dlopen2`. Primitive arithmetic stays in native
instructions. One C call processes a batch; the row loop runs in generated code.
The prototype currently requires 64-bit Linux.

`model.rs` owns validation and an independent checked Rust evaluator.
`codegen.rs` emits the batch loop and expression control flow. `runtime.rs` owns
the library and buffers and contains the two unsafe boundaries. Expressions and
batches become immutable after validation. References point only to preceding
nodes; limits bound columns, rows, cells, depth, and the expanded expression tree.
No caller-supplied object or shared-library path can reach the loader.

The temporary ABI is:

```c
uint64_t feldera_batch(const int64_t *values, const uint64_t *null_masks,
                      size_t rows, int64_t *output, uint8_t *output_nulls,
                      int64_t error_record[3]);
```

Payloads are row-major; one NULL bitmap serves each row, with bit zero for column
zero. NULL payloads are initialized. Output NULL flags are bytes. A return value
of zero means success; otherwise it is the first overflowing row's index plus
one. On failure, the error record holds the operation tag and both operands;
the wrapper uses them to reproduce sqllib's exact overflow message while
retaining the row index in its typed error. The safe wrapper discards the whole
output on error. COALESCE is lazy;
arithmetic evaluates both child expressions before propagating NULL.

Run inside the devcontainer:

```sh
cargo test --locked --manifest-path crates/sql/native-probe/Cargo.toml
cargo clippy --locked --manifest-path crates/sql/native-probe/Cargo.toml --all-targets -- -D warnings
cargo build --locked --manifest-path crates/sql/native-probe/Cargo.toml --example integer_vectors
cargo build --locked -p feldera-sqllib --example native_integer_reference
python3 crates/sql/scripts/run_native_probe.py $SCRATCH/native-results \
  --probe crates/sql/native-probe/target/debug/examples/integer_vectors \
  --runtime-reference target/debug/examples/native_integer_reference
```

Use the actual Cargo target directories when `CARGO_TARGET_DIR` is set. The
script requires a fresh output directory and preserves inputs, raw observations,
stderr, source/binary hashes, and a completion record. CI is configured for both Linux
architectures. The local ARM64 result compares 6,732 actual-runtime vectors,
including 1,350 NULL outcomes and 1,399 exact overflow messages, with zero differences.

This is an expression/ABI foundation, not the N1 exit. It has no SQL/IR importer,
DBSP circuit or `Data` row implementation, string/decimal functions, connector,
ten-program C7 measurement, or throughput/memory/compile-time comparison.
These three BIGINT operations match sqllib, including diagnostic text, but
the restricted result does not establish full C12. N2 archive, checkpoint, and normalized-key
performance experiments remain open. The ABI is private to this experiment and is not a
production compatibility commitment.

`keys.rs` supplies an initial N2 correctness prototype for nullable BIGINT keys.
Each field encodes a NULL tag and eight ordered payload bytes. Explicit NULL
placement is independent of ascending/descending direction. Keys occupy one
bounded allocation per batch; duplicate fields and empty keys remain valid.
Three tests check 20,736 pairwise comparisons, stable sorting and signed-weight
consolidation, the highest NULL bit, schema bounds and the 16 MiB output limit.
All eleven native-prototype tests and Clippy pass, and the 6,732-vector actual
runtime comparison remains equal. This key encoding is not a persisted DBSP
archive or hash format. Variable-length types, the ten programs' key types,
representative full-type sort/merge performance, archive equivalence and checkpoint
restore remain open. The bounded BIGINT timing result is recorded below.

## Key sort/merge measurement

`examples/key_benchmark.rs` compares byte keys with a dynamic field-by-field
comparator over the same `Batch` storage. It uses 4,096 and 65,536 rows, one,
four and twelve key fields, and three fixed distributions: wide integers, a
common prefix, and 64 duplicate row shapes. NULL placement and ascending versus
descending order vary by field. The deterministic generator changes its seed
between pairs. With one field, the common-prefix input equals the uniform input.
Both algorithms must produce identical stable sort and merge
results on every pair, including warmups.

Encoding is measured separately. Report both sort cost with reused keys and
sort cost including encoding; a faster comparison loop alone does not establish
a net gain. The merge consumes two already sorted runs, allocating its output
inside the timed region. These operations permute row indices, not payloads.
Input construction, initial index allocation, oracle checks and key destruction
are outside timing. Preparation warms the input cache. The baseline is a dynamic
comparator, not the specialized tuple comparison emitted by the SQL compiler.

Before running, create a gitignored `.bench-results/native-keys/` archive with
the commit, diff, untracked source snapshot, description and environment. Build
the example in release mode, freeze the binary, and stop competing build/test
jobs. In the devcontainer:

```sh
cargo build --release --locked --manifest-path crates/sql/native-probe/Cargo.toml \
  --example key_benchmark
python3 crates/sql/scripts/benchmark_keys.py $SCRATCH/NEW-RESULT-DIRECTORY \
  --binary PATH/TO/FROZEN/key_benchmark --samples 31
```

The runner pins one allowed CPU, retains every raw sample, validates all eighteen
workloads and their seeds, and reports paired bootstrap intervals. Copy the
complete output into the archive. The extra key bytes are reported alongside
the batch's payload and NULL storage; this is not a total RSS measurement.
These synthetic BIGINT measurements do not close N2's full key-type coverage,
archive or checkpoint requirements.

The first [measured result](../BENCHMARKS.md#native-bigint-keys-2026-11-16)
passes all 594 paired output checks. Twelve-field uniform inputs lose 18–28%
including encoding, while common prefixes benefit substantially. All results,
including unresolved small differences and extra key memory, are retained in
[the certificate](../evidence/native-key-sort-merge.json). This supports a
workload-dependent choice, not universal normalization.

Add `--mixed` to the runner command for the six nullable text, decimal and
BIGINT workloads. The mixed runner reports the minimum and maximum packed
key bytes across samples. The row boundary index is stored separately.
See the [mixed-key results](../BENCHMARKS.md#mixed-text-decimal-and-bigint-keys).

## Native archive writes

`CompiledArchive` emits a separate native batch encoder for explicit, validated
integer archive layouts. It checks write bounds and overlap, input widths,
required fields and INTEGER range, and returns owned bytes. It never creates a
Rust tuple or calls a serializer. The [DBSP-side tests](../NATIVE-ARCHIVES.md)
compare six frozen-corpus tuple shapes with rkyv's generated archives and verify
ordering, hashing and lifetime behavior. The composite adapters add ordered
string payloads, fixed-width slots and explicit root alignment. The complete
selected-row test matrix covers all 29 stored layouts. Native `Data` and
context-aware factories remain open.
[Selected-row evidence](../evidence/native-selected-rows.json)
