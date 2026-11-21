# SQL compiler differential tests

This is the first Phase 0a component of the compiler migration harness. It
compares raw `DBSPCircuit` JSON and versioned observations emitted by the Java
compiler. It depends only on
Serde and `serde_json`; it does not require either compiler implementation or an engine.

From the repository root:

```sh
cargo run --locked -p feldera-sql-difftest --bin sql-difftest -- expected.json actual.json
cargo run --locked -p feldera-sql-difftest --bin sql-difftest -- --envelope expected-dump.json actual-dump.json
cargo run --locked -p feldera-sql-difftest --bin sql-difftest -- --final-s3 direct-s3.json resumed-s3.json
cargo run --locked -p feldera-sql-difftest --bin sql-difftest -- --directories run-a/compilation-0000 run-b/compilation-0000
cargo test --locked -p feldera-sql-difftest
cargo run --locked -p feldera-sql-difftest -- --validate-corpus $SCRATCH/sql-corpus
cargo run --locked -p feldera-sql-difftest -- --summarize-optimizer $SCRATCH/optimizer.jsonl
```

The CLI exits with 0 for equality, 1 for a structural difference or invalid circuit
graph, and 2 for incorrect arguments, an unreadable file, or invalid JSON.

## Comparison contract

Allocation ids, object-key order, and the location of a node's full definition
versus a reference may differ. Operator and expression sharing, array order,
output ports, annotations, source positions, and metadata must match. Inner and
outer nodes have separate id namespaces. Duplicate definitions and dangling
references are errors, even when comparing a dump with itself.

The format rules follow `ToJsonOuterVisitor`, `ToJsonInnerVisitor`, and
`TestSerialize` in the Java compiler. Unlike `TestSerialize`, this comparator
preserves sharing and compares metadata and source positions. The circuit's root
metadata is plain schema JSON. Operator metadata can contain expression and type
nodes, so it participates in graph comparison. Annotations are plain JSON;
their `id`, `class`, and `node` properties are never treated as IR references.

Internally, definitions are indexed, then visited in deterministic breadth-first
order. Graph edges become typed references into separate inner and outer node
lists. Each definition is processed once; cycles and long reference chains do not
recurse or expand into trees. The normalized form is private and is **not** a
persistent-id format. A mismatch path such as `/outer/2/inputs/0/outputNumber`
refers to those canonical node lists; malformed-input paths refer to the input
JSON. Diagnostics omit literal values and SQL text, though field names remain in
paths. Use the tool locally, not as a telemetry redactor.

## Capturing Java observations

The hidden `--dump-circuits DIRECTORY` flag records the circuit before optimization
(S2), after optimization (S3), and after each observed pass (S4). `Passes` and
`Repeat` supply nested pass paths and zero-based iteration numbers. Executed
`Conditional` bodies and direct substeps of `MonotoneAnalyzer`, `LowerAsof`, and
`LinearPostprocessRetainKeys` are also observed. See [PORTING.md](../PORTING.md)
for the side-state inventory and replay strategy.

Build the Java compiler with the repository's pinned Calcite revision, then run:

```sh
java -jar sql-to-dbsp-compiler/SQL-compiler/target/sql2dbsp-jar-with-dependencies.jar \
  -i --alltables --ignoreOrder --noRust \
  --dump-circuits $SCRATCH/compiler-observations-a \
  --dump-producer-commit YOUR_COMPILER_SOURCE_REVISION \
  --dump-runtime-abi YOUR_RUNTIME_BUILD_ID \
  program.sql
```

Record the revision that built the jar, including a dirty-tree identifier when
appropriate. The ABI field is a caller-supplied compatibility identity; a runtime
build commit can serve until the runtime defines a versioned ABI. Neither field
is inferred from the current checkout or given an `unknown` default. Add `--gen2`
for the Gen-2 variant; `--input_circuit` records only S2. These flags do not wrap
or change the existing `--jit` or `--gen2` output payloads.

The output contains `compilation-0000/`, then `compilation-0001/` for a later
compilation in the same compiler instance. Each directory has numerically named
observations, such as `000000-S2.json`, and `complete.json` with the count and
final stage. Existing compilation directories and files are never overwritten.
Failed optimizer runs and failed post-compilation checks leave partial directories
without a completion marker. Callers must also check the compiler's process exit
status and diagnostics: backend file generation follows circuit compilation.

Run the compiler in three fresh JVMs with separate output directories. Use
`--directories` to compare the first run with each later run. This mode requires
completion markers, matching counts and final stages, every numbered observation,
and no extra files. It checks that each observation's stage and sequence match its
filename. It also accepts an S3-only capture containing one `000000-S3.json`
observation. `--final-s3` compares final observations from different entry
points, allowing their local sequence numbers to differ while retaining all
other compatibility and graph checks. Directory comparisons still require
matching complete sequences. The tool holds one pair of dumps in memory at a time.

For spike 0, also capture and compare schema, diagnostics, dataflow, hash inputs,
and generated Rust. The example above suppresses Rust generation to exercise the
dump path alone; it is not the complete spike or a corpus parity result.

## Observation schema version 1

Each envelope has exactly these fields:

| Field | Contract |
|---|---|
| `schema_version` | Integer `1`; reject other or absent versions before decoding circuits |
| `producer_commit` | Nonempty source/build revision; recorded but may differ between compilers |
| `runtime_abi` | Nonempty compatibility identity; must match |
| `engine_mode` | `classic` or `gen2`; must match |
| `options` | Object of effective compiler options; must match |
| `observation` | `stage`, zero-based `sequence`, and ordered `path`; must match |
| `replayable` | Must be `false`; side state is incomplete |
| `circuit` | Raw `DBSPCircuit` JSON; compared structurally |

An observation's path is empty for S2 and S3. S2 has sequence zero. S3 can
have sequence zero when captured directly after import. S4 has a positive
sequence and at least one frame. A frame contains `container`
and `pass` names, a zero-based pass `index`, and a zero-based `iteration` (zero
outside a repeat loop). Header and frame fields are required; unknown fields are
rejected. Payload comparison paths have a `/circuit` prefix.

Java captures public option fields by default, including newly added options.
It removes input/output destinations, dump configuration, logging levels,
verbosity, quiet/version/help flags, and replaces the crate output directory with
the `multiCrates` boolean. SQL `SET` state is not yet carried in the envelope.
Source text, frontend diagnostics, provenance, and cross-pass analysis state also
need separate capture before these observations can serve as hybrid boundaries.

## Current limits

This is a structural oracle, not an IR type checker. It does not check operator
arity, type validity, or completeness of pass side data. Raw-circuit mode accepts
a circuit, not a `program_ir` object; extract its circuit first. Envelope mode
checks the internal observation schema, but does not establish compatibility with
an engine or prove that the supplied build identities are accurate.

The CLI uses serde_json's default nesting limit; the library also limits recursive
normalization of inline JSON to 128 levels. Graph references do not consume that
limit. Inputs are local test artifacts held in memory, not untrusted service
requests. The CLI rejects duplicate object keys before decoding. Library callers
should use `parse_json` to enforce that rule; an already parsed `Value` cannot
reveal keys its parser discarded. Decimal numbers retain arbitrary precision.

Rust tests use small hand-authored serializer-shaped circuits, mutations, a large
cyclic graph, and complete/partial capture directories. Java `CircuitDumpTests`
exercise both engine modes, nested passes, S2-only capture, errors, and overwrite
protection. Run them with `mvn -pl SQL-compiler -Dtest=CircuitDumpTests test` from
`sql-to-dbsp-compiler` after building the pinned Calcite dependency. The full
corpus determinism spike remains Phase 0a work; these tests do not establish Java
corpus parity.

Validation on 2026-11-12 used the repository's devcontainer image (Linux ARM64,
JDK 21, Rust 1.99) and the pinned Calcite commit. All 19 Java tests above and 21
Rust tests passed, along with Clippy and formatting. Seven programs from
`Gen2JsonTests` passed three fresh-JVM captures plus a capture-disabled baseline
in each mode: 56 compilations and 28 complete-directory comparisons, covering
6,294 observation points across the 14 program/mode combinations. Schema,
dataflow, diagnostics, generated Rust, and Gen-2 circuit output also matched
between the baseline and captured runs. This checks compilation and capture
transparency; it does not execute the generated programs or cover the full corpus.

## Exporting the runtime-oracle corpus

From `sql-to-dbsp-compiler`, after building its dependencies:

```sh
CI=true mvn -B -ntp -pl SQL-compiler test \
  -Dfeldera.corpus.dir=$SCRATCH/sql-corpus \
  -Dfeldera.corpus.producer=YOUR_COMPILER_BUILD \
  -Dfeldera.corpus.calcite=YOUR_CALCITE_COMMIT \
  -Dfeldera.corpus.runtime=YOUR_RUNTIME_COMMIT \
  -Dfeldera.corpus.only=true
```

Use a fresh output directory for each run. `only=true` skips backend execution;
omit it to export alongside the normal runtime tests. CI uses the latter mode.
Always check Maven's exit status and report: the exporter cannot detect an entire
test that was never run. `--validate-corpus` checks every per-test completion
manifest, case identity/count, build identity, command, circuit graph and typed
Z-set graph, then prints a JSON summary. It rejects empty, partial, mixed-build,
or malformed exports. It reads one case at a time.

Each full JUnit identity has a SHA-256 directory name and numbered case files.
Source, options, schema and diagnostics are captured when a case is registered;
steps are captured after the test body fills its stream. This prevents later
INSERT statements from changing the saved compilation source. A body that fails
or skips records its registered cases with an explicit reason. Expected runtime
errors and compaction commands remain part of the export.

`submissions` preserves each API call's exact SQL, single-statement/list parsing
mode, and compilation batch. Concatenated `program` text is for inspection; it
may omit semicolons between API calls and must not replace these boundaries on
replay. Early version-1 exports without `submissions` remain readable as value
evidence, but the replay preparer rejects them. Runtime-failure cases retain
compile warnings before the test helper clears its message buffer.

`step_semantics` records the original test's `delta` or `whole_relation` semantics.
Input/output permutations and circuit interface identifiers are explicit. Each
typed Z-set owns a separate inner-node namespace; decimal values are not rounded.
Validation requires a declared element type and collection type, typed row
references, and one signed 64-bit weight per row. Use
`sql-difftest --zsets EXPECTED.json ACTUAL.json` on two `zset_ir` values to
compare the bags independently of row order and allocation IDs. The comparator
consolidates identical typed row graphs, ignores zero net weights, and rejects
overflow in the consolidated signed weight. It compares row graphs exactly.

Use `sql-difftest --portable-output-zset ZSET.json` to encode an expected output
as a version-1 bag with a SQL row type, typed JSON rows and signed decimal
weights. Rows are sorted by their serialized value, and equal rows consolidate.
Decimal values use a signed coefficient and base-ten exponent. Float values
use fixed-width hexadecimal IEEE bits, including signed zero and NaNs. Integral
values use decimal strings (date is an integer day count since 1970-01-01).
Binary values use base64. Tuples, arrays, map entries and point coordinates use
JSON arrays. A variant containing a value includes that value's SQL type.
SQL NULL is JSON `null`, and JSON null inside a variant has a separate tag.
This conversion covers literal expected outputs. Input expressions that require
evaluation stay in the original typed IR. `--validate-portable-outputs` checks
the format across a corpus and reports the converted output bag and row counts.

### Executing frozen runtime cases

Build the test classes and their classpath from `sql-to-dbsp-compiler` using
the same compiler source and Calcite build as the export.

```sh
mvn -B -ntp -pl SQL-compiler test-compile dependency:build-classpath \
  -DincludeScope=test -Dmdep.outputFile=target/replay-classpath
compiler=SQL-compiler/target
replay_cp="$compiler/test-classes:$compiler/classes:$(cat "$compiler/replay-classpath")"
runner=org.dbsp.sqlCompiler.compiler.sql.tools.CorpusRuntimeReplay
```

For classic DBSP, generate the Rust test crate from case files with compatible
test options. The runner prints the generated Rust test name for each case.

```sh
replay=$(mktemp -d "$PWD/corpus-replay.XXXXXX")
mkdir "$replay/src"
cp temp/Cargo.toml "$replay/Cargo.toml"
printf '\n[workspace]\n' >> "$replay/Cargo.toml"
cp ../Cargo.lock "$replay/Cargo.lock"
java -ea -cp "$replay_cp" "$runner" "$replay/src/lib.rs" $SCRATCH/sql-corpus/HASH/case-0000.json
cargo test --manifest-path "$replay/Cargo.toml" --lib
```

For Gen-2, use `java -ea -cp "$replay_cp" "$runner" --gen2 CASE.json...`
with `GEN2_PATH` pointing to the matching pipeline and expander binaries.
`--decode-only` checks circuit and stream reconstruction without an engine.
The runners read the saved circuit, signed input changes, expected outputs,
compaction commands and expected panic. They need no SQL source or JUnit
invocation. External UDF source and files must be supplied separately.
`runtime_verified` remains false in the frozen case because it records export
status. Consult the runner results for runtime evidence.

Full-corpus counts and the remaining P0 acceptance checks are in [P0.md](../P0.md).

## Full-corpus determinism

After building the Java compiler and its test classes, prepare a strictly
validated corpus and run the experiment:

```sh
cargo build --locked --release -p feldera-sql-difftest --bin sql-difftest
python3 crates/sql/scripts/determinism.py prepare $SCRATCH/sql-corpus $SCRATCH/replay.jsonl \
  --comparator target/release/sql-difftest
python3 crates/sql/scripts/run_determinism.py $SCRATCH/replay.jsonl $SCRATCH/determinism \
  --comparator target/release/sql-difftest \
  --classpath 'sql-to-dbsp-compiler/SQL-compiler/target/test-classes:sql-to-dbsp-compiler/SQL-compiler/target/classes:sql-to-dbsp-compiler/SQL-compiler/target/sql2dbsp-jar-with-dependencies.jar' \
  --producer YOUR_COMPILER_BUILD --runtime YOUR_RUNTIME_BUILD --workers 4 --compress
```

Every case runs in three independent JVMs, plus a baseline without circuit
capture. Workers split the ordered workload without deduplicating cases; each
shard has the same order in all four runs. The JVM-global name allocator is not
reset between cases. This is workload isolation, not a new JVM per case. The
current runner rejects multiple compilation batches rather than guessing how
to reconstruct intermediate compiler state.

`--compress` writes case records directly as gzip, preserving the complete
records without first creating an uncompressed copy. The comparator accepts
either encoding and checks gzip integrity. Completion records name the selected
file; an interrupted run still has no completion certificate.

`--max-output-mib` bounds the shared artifact tree (512 MiB by default), including
input shards, logs, case records, and temporary generated files. Workers check
it every half second and at process completion. Exceeding the limit cancels all
workers and their child processes while retaining partial evidence. This is a
sampled limit, not a filesystem quota: leave free-space headroom for writes
between checks and for other builds. `--workers` and `--heap` bound concurrency
and JVM heap separately; heap does not bound total process RSS.
`--min-free-mib` also stops workers below 512 MiB free by default. The output
filesystem is always checked. Add `--free-space-path $REPO` when
that mount reports the host filesystem's free space, so a growing VM disk cannot
exhaust its host unnoticed.

Preparation normally rejects registered skips. For an explicitly partial
measurement, `prepare --record-skipped $SCRATCH/exclusions.json` retains every skipped
identity, reason, and source digest in a separate inventory while preparing only
exported cases. That file records `approved_dispositions: false`; it is not an
allow-list and cannot certify the full corpus. Compare it with the JUnit report
as well, because tests that fail before registration cannot appear in the export.

`compilation_options` records the circuit's effective incremental mode separately
from the original test options and its expected-step semantics. This matters for
Gen-2 tests, whose runner temporarily forces incremental compilation. Gen-2
replay requires this field; older classic exports remain readable. Gen-2 runs
compare emitted circuit IR and dataflow files, while classic runs compare every
generated Rust workspace file. Both compare the Merkle hash preimages.

`--canonicalize-stream` accepts one compact observation envelope per stdin line
and returns its opaque canonical comparison bytes on one stdout line. It checks
the same fields as `--envelope`, including graph sharing, metadata, ordered
ports, pass identity, and annotations. Java records length-framed SHA-256
fingerprints for S2/S3/S4 and the exact Merkle hash preimages. It also hashes
every generated workspace file, preserving paths relative to the workspace.
Only one observation crosses the process boundary at a time; intermediate
circuits are not retained on disk.

Completion requires all cases, unchanged compiler/comparator build digests,
equal observations, hash preimages, generated files, schemas, and diagnostics.
The baseline also has to reproduce the exported schema and diagnostics. A crash,
missing stage, timeout, changed input, or truncated run cannot certify equality.
Timeouts, cancellation, and a failed worker terminate the JVM process groups,
including their canonicalizer children. Completed evidence is retained; an
interrupted JVM must replay its complete ordered shard from the start.
Raw dumps must be regenerated to diagnose a fingerprint difference. These
checks do not execute generated programs or certify S2/S4 checkpoint replay.

The nightly P0 workflow captures every nested pass across the corpus.
For a manual run, select `full_determinism` to include this capture.
Both runs export and validate the corpus and run the scalar oracle.

## Calcite rule census

Add `-Dfeldera.optimizer.trace=$SCRATCH/optimizer.jsonl` to a compiler JVM or Maven
test run to record a JSON-lines journal. Use a fresh file. Each optimizer step
records its level, completion status, configured HEP rules, attempts and successful
productions, and before/after relational node counts and correlate summaries.
Each listener event is counted only once. The Rust `--summarize-optimizer` command
keeps levels and steps separate and lists configured rules with no productions.
An unobserved rule still needs a targeted legality test before omission.

`-Dfeldera.operator.inventory=$SCRATCH/operators.json` enables `OperatorInventoryTests`
to export the actual initial validator table. Membership does not establish that
an operator lowers or executes; the file explicitly leaves that classification
unverified. Tracing has no effect unless its system property is set.

`-Dfeldera.rule.inventory=$SCRATCH/rules.json` enables `RuleInventoryTests` to record
actual implementation classes and ordered instances. It constructs programs for
VALUES, a three-join plan, and UNCOLLECT at each optimization level to exercise
conditional rule registration. Reconcile it with a completed corpus census:

```sh
python3 crates/sql/scripts/rule_port_inventory.py $SCRATCH/rules.json \
  $SCRATCH/optimizer-census.json sql-to-dbsp-compiler/calcite $SCRATCH/rule-port-inventory.json
```

The tool requires exact rule-description coverage, resolves every implementation
to a pinned source file, and retains zero-production rules in the proposed port
scope. Descriptions can cover several configurations; inspect the ordered raw
instances and source before porting. The report is prepared for review and does
not approve rule removal or classify function-lowering support.

## Scalar type probes

```sh
cargo run --locked -p feldera-sql-difftest --bin sql-type-probes -- $SCRATCH/probes.jsonl
```

The generator writes a fresh file and prints its coverage counts. It enumerates
16 scalar type/precision/length points × two nullabilities × two origins
(column/literal), giving 64 argument points. It takes the full product for
binary operations: 30 unary functions and six aggregates × 64, plus ten binary
functions, six binary operators, CASE and three ALL set operations × 64² =
84,224 probes. A nullable literal is a typed NULL. The fixed literal values are
declared in the generator; integer maxima and DECIMAL precision boundaries are
included. No random sampling is involved. The CI workflow generates this matrix,
runs the Java oracle, validates its output, and retains the results.

Generate Java results in one JVM, from `sql-to-dbsp-compiler`:

```sh
CI=true mvn -B -ntp -pl SQL-compiler -Dtest=TypeProbeTests test \
  -Dfeldera.probes.input=$SCRATCH/probes.jsonl \
  -Dfeldera.probes.output=$SCRATCH/probe-goldens \
  -Dfeldera.probes.expectedCount=84224 \
  -Dfeldera.probes.producer=YOUR_COMPILER_BUILD \
  -Dfeldera.probes.calcite=YOUR_CALCITE_COMMIT \
  -Dfeldera.probes.runtime=YOUR_RUNTIME_COMMIT
```

The output directory must not exist. Each record contains the source probe,
acceptance, diagnostics, schema, effective options and the unoptimized S2 graph
when accepted. The runner uses optimization level zero and `inputCircuit`.
It preserves exact numeric text and emits one physical line per record.
`complete.json` records counts and build identities. Internal compiler failures
are counted separately, set `oracle_ready` to false, and fail the JUnit run;
they are never accepted as ordinary rejection goldens. Validate a successful
table with `sql-difftest --validate-probes $SCRATCH/probe-goldens`.
The expected count comes from the generator's report; a truncated input cannot
produce a completed golden table.

The runner resets the existing test-only variable-name allocator before each
independent probe, after the preceding circuit has been serialized. This makes
names independent of earlier probes and other JUnit classes. It runs sequentially;
the reset must not be used by concurrent compilers or with live circuits that
will undergo further passes. Graph allocation IDs remain canonicalized by the
Rust comparator, and variable names remain part of the comparison.

To review a Java revision, including repairs to compiler-bug cases:

```sh
sql-difftest --compare-probes $SCRATCH/old-goldens $SCRATCH/new-goldens > $SCRATCH/probe-diff.json
```

The comparison validates both batches and their completion counts, requires the
same probe inputs/options, and compares verdict, compiler-bug status, schema,
diagnostics, and canonical S2. It permits a failing baseline for diagnosis but
returns exit code 1 for every difference, including intentional bug fixes.
The JSON report lists all changed probe IDs and fields. `--validate-probes`
continues to reject any batch with compiler bugs. Build identities are retained
in the comparison report; different revisions are expected.

For a migration comparison under decision 3, use `--compare-probes-c0` with
the same two batch directories. It requires both batches to be free of
compiler failures. It compares verdict and the ordered diagnostics by count,
warning flag, error class, start position and snippet. The
`c0_diagnostic_metrics` report counts wording and end-position changes among
diagnostics that match the required fields. Review those changes with the
compiler output. The mode still checks probe inputs, options, schema and
canonical S2.

This scalar matrix is a measured starting subset. Nested types, unsigned types,
intervals, additional boundary values, runtime value vectors, C6a printer text,
and the held-out binder split remain to be added. The generator and Java oracle
do not implement the Rust type rules or complete spikes 5, 5b, or D1.

## Diagnostic inventory

Add these properties to a full JUnit run to capture evaluated negative-test
expectations, actual diagnostics, and the pinned Calcite message resources:

```sh
-Dfeldera.diagnostics.dir=$SCRATCH/sql-diagnostics \
-Dfeldera.diagnostics.producer=YOUR_COMPILER_BUILD \
-Dfeldera.diagnostics.calcite=YOUR_CALCITE_COMMIT \
-Dfeldera.diagnostics.runtime=YOUR_RUNTIME_COMMIT
```

Use a fresh directory. The completion manifest records the JUnit result and the
number of captured invocations. From the repository root, build the review list:

```sh
python3 crates/sql/scripts/diagnostic_census.py \
  $SCRATCH/sql-diagnostics $SCRATCH/diagnostic-census.json
python3 -m unittest discover -s crates/sql/scripts -p 'test_*.py'
```

The script requires a passing, completed Java export. It reads Python expectation
assignments through the AST without importing tests, distinguishes forwarding
expressions, and retains unresolved expressions. Calcite resource matches are
candidates: ambiguous matches, unmatched messages, skipped Java tests, and the
separate Python platform assertion still require catalog review. The counts do
not certify spike 9's 150-template threshold. CI retains the raw export and report.
