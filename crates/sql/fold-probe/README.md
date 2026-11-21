# Constant-folding experiment

This disposable P0 prototype evaluates a bounded projection of Calcite Rex
expressions. It is not the future FRex IR or a production replacement for
`feldera-sqllib`. Decimal conversion and arithmetic use the runtime's
`feldera-fxp` kernels directly. Integer, string, and temporal operations are
bounded adaptations of sqllib; their independent runtime checks are deliberately
reported separately from Java agreement.

Semantics selection is explicit. `runtime-v1` is the library and CLI default.
`legacy-exp-log-libm-0.2.15-v1` uses pinned pure-Rust kernels for EXP and
two-argument LOG to preserve the measured Java results; all other operations
retain runtime behavior. `legacy-janino-v1` also preserves the measured legacy
LOG domain checks, nonfinite float spellings, padded timestamp syntax, and
empty-string SPLIT result. The profiles are distinct experimental contracts;
neither establishes complete Java/runtime equivalence.
Pass `--semantics-profile PROFILE` before the CLI's input
paths or `--stream`. Every response identifies its profile, and the Java
adapter rejects a missing or mismatched identity. This experiment-level check
is not the production compiler/runtime compatibility handshake.

`model.rs` owns the wire projection and typed values. `eval.rs` owns registration,
lazy control flow, and budgets. The numeric, rounding, temporal, comparison,
string, pattern, extraction, math, and array modules implement operations on those values.
The CLI owns JSONL and process I/O. Unsupported operations,
runtime errors, and exhausted budgets have different results. None counts as a
successful value comparison. Unknown calls are refused before NULL propagation;
user functions, dynamic calls, and nondeterministic functions are never executed.

Evaluation permits 4,096 visited nodes and depth 64. Each input record is limited
to 8 MiB, strings to 1 MiB, and string growth is checked before allocation.
A cumulative 1 MiB budget charges each intermediate value, including nested
payloads and spare capacity, before evaluation proceeds to the next operand.
This conservative budget prevents a wide constructor from retaining thousands
of individually legal strings; it is not a total process RSS limit.
CASE, COALESCE, AND, and OR evaluate only the required operands. Decimal
arithmetic currently admits only operands exactly representable as
`Fixed<38,18>` and result scales at most 18. Casts support scales through 38.
Rounding uses the runtime's fixed-decimal operations at scales through 38.
Array operations preserve NULL elements and order, with limits of 4,096 elements
and 1 MiB including nested payloads. DISTINCT also caps its quadratic search at
256 elements. LIKE uses dynamic programming with at most one million cells;
it cannot trigger regex backtracking. Known Java/runtime differences in Unicode
indexing, case folding, line terminators, and negative integer rounding remain
explicit refusals. Same-family interval casts truncate toward zero to the target’s last field;
seconds retain microseconds. All thirteen interval qualifiers have independent
runtime vectors, including negative values and storage limits. Cross-family and
number-to-interval casts remain unsupported.
Array casts, many functions, and values outside this envelope remain explicit
refusals. This is a coverage limit, not an allow-list of approved deviations.

The ordinary compiler remains on Java. `FoldProbe` alone activates a worker to
replace the planner's Rex executor and the temporal folds in `RexOptimize`.
It never falls back to Java when Rust refuses an expression. A runtime error in
an executor batch leaves the whole batch unchanged, matching Janino's atomic
batch behavior; this preserves the source provenance of other batch members.
Other simplification, validation, and literal construction still use Java. Thus
the experiment does not establish that all constant evaluation has been replaced.

## Reproduce

Build the Java test classes and dependency JAR first. In the repository root:

```sh
cargo test --locked --manifest-path crates/sql/fold-probe/Cargo.toml
cargo clippy --locked --manifest-path crates/sql/fold-probe/Cargo.toml --all-targets -- -D warnings
cargo build --locked --manifest-path crates/sql/fold-probe/Cargo.toml
cargo build --locked -p feldera-sqllib --example fold_reference
rustup target add wasm32-unknown-unknown x86_64-pc-windows-msvc
cargo build --locked --manifest-path crates/sql/fold-probe/Cargo.toml --lib --target wasm32-unknown-unknown
cargo build --locked --manifest-path crates/sql/fold-probe/Cargo.toml --lib --target x86_64-pc-windows-msvc
```

Pass the prepared corpus from `determinism.py prepare`, the two executable paths,
and the test/classes/dependency-JAR classpath to:

```sh
python3 crates/sql/scripts/run_fold_probe.py INPUT.jsonl NEW_OUTPUT_DIR \
  --stage S3 --semantics-profile legacy-janino-v1 \
  --classpath CLASSPATH --probe FOLD_PROBE --runtime-reference FOLD_REFERENCE
```

The runner uses new output paths, captures executable and source hashes, and
checks those hashes again after the run. Each child has a 20-minute deadline;
Java heaps are 1 GiB. A sampled 1 GiB output budget and 1 GiB free-space floor
cancel the process group, including the Rust worker, retaining partial evidence.
These are local experiment limits, not approved product G7 resource limits.
The example selects the experimental Janino profile for Java comparison;
runtime edge checks explicitly use `runtime-v1` regardless of that choice.

Each population runs in three fresh JVMs: unobserved Java, observed Java, and
Rust-assisted compilation. Acceptance, schema, diagnostic text, position, and
snippet are compared exactly. Internal compiler failures fail the comparison
even when both sides fail identically. The separate value comparison reads Java
literal results; it does not compute the expected answer from the input
expression. Nonliteral Java results and unsupported Rust folds stay visible.

`--stage S3` includes the Java optimizer and its later validation, including
nondeterministic defaults. CI uses this mode. The default `--stage S2` stops
before the optimizer, matching the initial experiment; its acceptance result
does not certify checks performed later. Each case and completion record names
the stage, and comparisons between stages fail. Neither mode executes the
generated program.

`fold_reference` calls actual sqllib functions for the selected decimal,
Unicode, and temporal edge vectors. These are independent runtime checks, not
execution of all corpus programs or all observed folds. Full C7 remains separate.

## Portability and remaining split

The prototype library builds as an rlib for Windows MSVC and wasm32. These checks
do not link or run a Windows executable and do not execute WebAssembly.
The first wasm build exposed an unconditional `rand`/`getrandom` dependency in
`feldera-fxp`. Its new `random` feature is enabled by default, and also by `dbsp`,
preserving existing callers. The prototype disables default features. Default
and feature-free fxp tests pass.

This separates decimal kernels, not the entire SQL runtime. Sqllib still depends
on DBSP, engine serialization, and runtime-specific types. Temporal/string ports
need extraction into a shared crate before production use; an independent
runtime sample cannot replace shared semantics. The full registry, fold-semantics
version handshake, and engineering estimate for that extraction remain open.

See [SEMANTICS.md](SEMANTICS.md) for measured Java/runtime differences and the
shared-kernel boundary, [CONSTANTS.md](CONSTANTS.md) for verdict-relevant sites and
[../P0.md](../P0.md) for measured results and remaining P0 gates.

Binary literals and casts preserve bytes, including fixed-width zero padding.
The bounded binary operations cover concatenation, substring, LEFT/RIGHT,
OVERLAY, OCTET_LENGTH and TO_HEX. String-to-binary casts use UTF-8. Negative
substring lengths and OVERLAY ranges on which Java and the runtime disagree
remain unsupported. The full binary audit passes 620 focused compilations and
519 runtime vectors, retaining 198 diagnostic differences and six math-bit
differences over the corpus; see `../evidence/fold-binary-s3.json`.

Unsigned integer literals, arithmetic bounds, and casts from integers, booleans
and strings now cover all four widths. String casts use unsigned parsing so
`'-0'` remains an error, as in sqllib. Decimal/float-to-unsigned casts remain
explicit refusals. The expanded audit passes 748 focused compilations and
619 runtime vectors. All 8,571 corpus verdicts and schemas match, with the
same 198 diagnostic and six math-bit differences; see
`../evidence/fold-unsigned-s3.json`.

Calendar FLOOR/CEIL and DATE_TRUNC preserve the runtime's legal date limits,
Sunday weeks, century origins and pre-epoch subsecond rounding. All 1,728 new
temporal vectors match actual sqllib, bringing the runtime comparison to 2,347
vectors. The full audit retains 196 corpus diagnostic differences and six
math-bit differences. The expanded 860-case focused suite has 48 diagnostic
differences: folding expressions Java leaves unevaluated can introduce a
constant-view warning. No diagnostic difference is waived. See
`../evidence/fold-temporal-rounding-s3.json`.

The subsequent binary/UUID extension adds MD5, STARTS_WITH, ENDS_WITH, POSITION,
and UUID byte casts. The early temporal pass now respects Java's fold-site
eligibility. All 900 focused compilations and 2,403 actual-runtime vectors pass;
162 corpus diagnostic differences and six math-bit differences remain. See
`../evidence/fold-temporal-sites-s3.json`; the failed pre-repair audit remains
in `../evidence/fold-binary-uuid-s3.json`.

Decimal arithmetic now promotes integer operands when the inferred result is
decimal and supports remainder through the fixed-point runtime kernel. All
160 new decimal vectors match actual sqllib; the full audit passes 932 focused
compilations and 2,563 runtime vectors, retaining 142 corpus diagnostic and six
math-bit differences. See `../evidence/fold-decimal-arithmetic-s3.json`.


Selection folds add NULLIF, strict and NULL-ignoring extrema, and bounded array
sorting. The [explicit-profile audit](../evidence/fold-selection-legacy-math-s3.json)
passes all **996 focused compilations** and **3,137 runtime vectors**. With the
legacy EXP/LOG profile, all **16,429 comparable Java values** match. All 8,571
corpus verdicts and schemas match, but **114 diagnostic differences** remain.
The adapter preserves Janino's whole-batch translation boundary and the pinned
Calcite fork's nonfinite literals. The 12 focused regressions found before these
repairs remain in `fold-selection-before-eligibility-s3.json`.

Integer-to-binary casts now preserve source width and left truncation/zero
padding. Bounded text and map operations preserve NULLs, duplicate keys and
Unicode strings. The [combined audit](../evidence/fold-collections-s3.json)
preserves all 8,571 verdicts/schemas and matches all 16,738 comparable Java
values and 4,615 runtime vectors, retaining 38 diagnostic differences.
The original 1,260 focused compilations pass after stable window-bound
messages and empty-array casts. A strengthened warning-policy audit removes
unused input tables that could mask later warnings. These measurements do not
complete the shared-runtime extraction or the production compatibility contract.
