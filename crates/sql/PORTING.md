# Compiler porting contract

Rust modules own concepts, not Java class hierarchies. Keep graph storage, JSON
decoding, validation, transformations, and process/file handling separate. Use
typed IDs for distinct arenas, exhaustive matches for IR variants, and explicit
inputs and outputs for analyses. Public functions document their invariants and
failure modes. Prefer ordinary Rust data types and iterators over visitor
frameworks where a direct traversal is clearer.

Preserve observable ordering. Java `LinkedHashMap` iteration generally becomes
an insertion-ordered collection; a `HashMap` is appropriate only when iteration
cannot affect generated output, diagnostics, or persistent identities. Never
sort a semantic array to make a differential test pass. Keep source positions,
annotations, metadata, sharing, and output-port numbers in comparison fixtures.
Allocation IDs are not persistent IDs.

## Local compiler and phase status

The local Gen-2 driver takes a SQL file through the Rust parser and binder,
Java optimization and Rust artifact emission. The Rust lowerer handles table scans,
column projections, selected constant rows and signed-integer comparison filters. Other supported queries use Java lowering. The binder accepts
selected constant views, multiple table declarations, and views with
projections, filters, global aggregates, COUNT(DISTINCT) and grouped SUM over a base table.
It reports unsupported inputs explicitly. Its bootstrap contains compiler-owned
objects from an empty user submission. User relations and expressions are
constructed from the SQL. [Bootstrap provenance](evidence/a-binder-constant-program-archive.json)

| Phase | Implemented and exercised | Work before phase exit |
|---|---|---|
| P0 | The owner recorded go for tracks M, N and F and confirmed the Forge IR contract owner. [Exit checklist](P0.md#exit-checklist) | Independent maintainer and licensing acceptance. |
| P1 | The arena codec passed 7,030,934 S2/S3/S4 round-trips. The three Spike 8 ports matched all 296 directed Java S4 results and four Gen-2 refusals. Java and Forge consumer results are recorded below. | Independent maintainer acceptance under PLAN.md §4.1. |
| P2 | Java imported Rust-written S3 for all [8,571 classic cases](evidence/p2-classic-worker.json) and reproduced the generated workspace and persistent-ID hash inputs. All [8,527 Gen-2 cases](evidence/p2-gen2-worker.json) matched the Java circuit after canonicalization. Both populations matched the Java dataflow. All [8,256 classic runtime cases](evidence/p2-classic-runtime.json) passed. The [hybrid driver](evidence/p2-hybrid-worker.json), checkpoint restores and [one-CPU-hour decoder campaign](evidence/p2-decoder-campaign.json) passed the local gates. | Independent maintainer acceptance under PLAN.md §4.1. |
| P3 | The opt-in mixed optimizer executed 99 Rust pass kinds in the verified replay. Detached S2 replay matched direct Java on all 8,571 classic and 8,527 Gen-2 programs through final S3, persistent-ID inputs and engine output. [Pass comparisons](#p3-optimizer-replay) | Port the remaining Java passes, including projection optimization, monotonicity and limiter insertion. Verify its S4 boundaries and run the phase gates on that complete build. The latency ceiling and field-shadow window remain open. |
| P4 | Rust S1-to-S2 lowering for unkeyed tables, table scans, literal rows, column projections and selected integer comparisons. Java resumes Rust checkpoints. | Query operators and scalar expressions through S2, then corpus parity. |
| P5 | JVM-free syntax checking, structured syntax diagnostics, formatting, one-batch API submissions, bound schema preview and low-verbosity query-plan output for supported programs. | Negative syntax and diagnostic parity, catalog and binder coverage, relational rules and decorrelation, product integration and P5d acceptance gates. |

The P2 checkpoint test compiled the same grouped COUNT through Java and the hybrid driver,
then built each generated Rust file as a separate executable. Each executable restored the
other's checkpoint on a non-materialized input. Deleting one of two input rows produced
a retraction of count two and an insertion of count one. DBSP reported no bootstrap in
either direction. A second program declared `LATENESS 2`. Both restored
binaries dropped an input at zero after the source had reached seven, and
both emitted the expected retraction for the on-time delete.
[COUNT restore](evidence/p2-classic-checkpoint.json) · [Watermark restore](evidence/p2-watermark-checkpoint.json)

The classic runtime replay executed 11,407 input commands across all 8,256
executable cases, including 232 expected runtime errors. The other 315 frozen
cases carried compile-only assertions. The verifier checked all 271 batch
logs against the frozen sources and generated test names.
[Classic runtime results](evidence/p2-classic-runtime.json)

All 8,571 classic and 8,527 Gen-2 inputs matched the Java optimizer tail on
both sides of its persistent-ID hash pass. The checks compare graph structure,
sharing, provenance, operator order, annotations and all 15 statistics counters.
Sixteen directed traces also matched at each intermediate pass.
[Tail corpus](evidence/p2-corpus-tail.json)

Across the classic corpus, 550 occurrences of defaults and lateness expressions
reduced to 44 distinct graphs. All matched Java, including generated variable
names. All 4,395 distinct named-row types matched Java's conversion to tuples.
Directed cases also exercise safe and unsafe collection conversions, generated-name order
across sibling casts, literal rounding, overflow and nullability. [Expression comparisons](evidence/p2-column-defaults.json)

In five rounds with four concurrent hybrid workers waiting for SQL input,
signal-to-reap p95 was 24.40 ms against a 25 ms ceiling. The largest observed
driver and front-end high-water RSS sum was 170.1 MiB on this profile.
[Worker measurements](evidence/p2-worker-limits.json)

The release circuit decoder processed 6,977,760 cases in one worker CPU-hour.
It accepted 2,713,917 and checked that each accepted graph was stable across
serialization and decoding. No worker crashed or timed out. Maximum RSS was
26,512 KiB and the slowest batch of 32 took 117 ms rounded up, against a
two-second deadline. [Decoder campaign](evidence/p2-decoder-campaign.json)

A directed self-join also matched both Java conflict
messages and their source ranges. The pipeline-manager devcontainer run passed all
ten shadow tests. The manager compared the full Java primary with a hybrid process
for each engine and detected missing and changed artifacts. After a failed S3
export on a stateless request, Java completed the retry with the same files as
a direct compilation in classic source mode and both IR-only modes.
[Devcontainer checks](evidence/p2-devcontainer-checks.json) · [Tail traces](evidence/p2-tail-passes.json)

The Java parity baseline includes the VARIANT and Unicode fixes tracked in
[Feldera issue #8025](https://github.com/feldera/feldera/issues/8025).

Build from the workspace root with Cargo. The mixed compiler also needs the
SQL compiler dependency JAR and compiled Maven test classes from this checkout.

```sh
cargo build --release --locked -p sql2dbsp -p feldera-sql-difftest
compiler=target/release/sql2dbsp
java_build=sql-to-dbsp-compiler/SQL-compiler/target
classpath="$java_build/test-classes:$java_build/sql2dbsp-jar-with-dependencies.jar"
"$compiler" --check-syntax query.sql
"$compiler" --syntax-diagnostics query.sql
"$compiler" --emit-s1 query.sql
"$compiler" --schema query.sql
"$compiler" --emit-plan query.sql
"$compiler" --bind-submissions submissions.json
# Supported table declarations, column projections and filters
"$compiler" --emit-s2 local-build local-abi tables.sql > tables.s2.json
# Lower a typed S1 checkpoint in a separate process
"$compiler" --lower-s1 local-build local-abi bound.json > bound.s2.json
python3 crates/sql/scripts/compile_bound_gen2.py query.sql /tmp/gen2-run \
  --driver "$compiler" --classpath "$classpath" --revision "$(git rev-parse HEAD)"
python3 crates/sql/scripts/benchmark_bound_gen2.py query.sql /tmp/gen2-bench \
  --driver "$compiler" --comparator target/release/sql-difftest \
  --classpath "$classpath" --revision "$(git rev-parse HEAD)" --rust-lowering
```

Use a fresh output directory. For projections and filters supported by the Rust lowerer,
pass `--rust-lowering` to either runner. Omit it to measure Java lowering for supported
aggregates. The Rust worker reads S3 and publishes its status and files together
under `tail/result/`. An interrupted worker leaves the attempt unpublished.

The runner writes `circuit.json`, the bound S1
and optimized S3 checkpoints, `lowered/schema.json`, `lowered/dataflow.json`, structured diagnostics in `lowered/diagnostics.json` and per-stage wall times. The benchmark alternates fresh-process Java and mixed
compilations of the same file. It compares the circuits structurally and the
schema, diagnostics and profiler artifacts as JSON before counting a pair. It saves raw
samples, executable hashes and compiler classpath hashes. JVM startup, disk
transport and output serialization are included. Runtime startup and query
execution have separate measurement boundaries.

The filtered-projection workload uses this input.

```sql
CREATE TABLE readings (sensor INTEGER NOT NULL, temperature INTEGER NOT NULL);
CREATE VIEW hot AS SELECT sensor FROM readings WHERE temperature > 20;
```

The Rust parser accepted all 8,571 frozen positives across 34,015 submissions.
All 101 cases previously refused by Rust passed the Java AST, schema and
diagnostic comparison and strict typed S1 replay of the original and formatted
SQL. In the full semantic run the current Java build failed on the *original*
`INTERSECT ALL` and `EXCEPT ALL` nullable-widening cases. All 8,569 other
comparisons passed. All 8,571 cases passed both semantic comparison and strict typed S1 comparison
in a separate run that placed the P0 archived `FelderaTypeCoercion` class first on the same classpath.
That class overrides inferred column nullability. The current build omits the
override. The run retains both original failures. Among 2,136 adversarial grammar cases the two parsers accepted 1,029 with equivalent Java ASTs.
Rust rejected all 1,061 Java rejections and refused 46 additional Java acceptances.
Of the shared rejections, 672 had equal start coordinates, 382 had different
coordinates, and seven lacked one parser's position. Against the product compiler,
659 matched severity, error class, start and snippet together. The remaining
402 are open P5a differences. [Diagnostic comparison](scripts/validate_syntax_diagnostics.py)

Seventeen focused programs matched direct Java on the downstream observations, schema preview,
relational plan, public Gen-2 circuit, dataflow, and diagnostic payloads. The pinned
Forge reader loaded all seventeen circuits with their profiler reports. The cases include
COUNT(*), a filtered COUNT(*), COUNT(DISTINCT), a global SUM, a grouped SUM, and a multiline
constant view. [Executable comparison](scripts/validate_bound_programs.py) · [Measured evidence](evidence/local-compiler-20261119.json)

The full-corpus binding audit preserves the original submission calls and uses
default nonincremental Gen-2 options. Rust bound 75 of 8,571 programs. Java
compilation of all 75 matched Java continuation from Rust S1 at every captured
optimizer observation, the persistent-ID inputs, schema, diagnostics and final
Gen-2 bytes in the Linux devcontainer. Rust also lowered 23 of those programs to S2. All 23 matched an independent
Java lowering of the same bound program, and the Java optimizer resumed every
checkpoint. The other 8,496 programs remain outside
the verified path. The parser's 8,571 successes measure a different boundary.
[Submission audit](scripts/measure_submission_binding.py) · [Downstream comparison](scripts/compare_submission_binding.py) · [S2 consumer test](scripts/compare_submission_lowering.py)

Ten additional incremental workloads used Java to compile raw SQL through S3.
For all ten, the Rust worker matched the direct Java circuit, dataflow, schema and
diagnostics. They cover inner, left and anti joins, set operations, rolling aggregates,
LAG and top-k. The pinned Forge reader loaded nine from each compiler. It rejected
both EXCEPT ALL circuits at `DBSPPositiveOperator`. This limits engine compatibility
for that workload. [Forge consumer results](evidence/p1-forge-consumer.json) · [Repeat the comparison](scripts/validate_gen2_tail.py)

In the Linux ARM64 devcontainer, three warmup pairs preceded 20 measured pairs for each
workload. These measurements include fresh JVM startup and file transport. Each mixed invocation used Java for optimization. The table identifies which compiler lowered the query.

| Workload | Mixed lowering | Java p50 | Mixed p50 | Java empirical p95 | Mixed empirical p95 |
|---|---|---:|---:|---:|---:|
| Table-scan view | Rust | 1.094 s | 0.557 s | 1.234 s | 0.591 s |
| Renamed and reordered columns | Rust | 1.120 s | 0.580 s | 1.151 s | 0.607 s |
| Filtered projection | Rust | 1.142 s | 0.575 s | 1.186 s | 0.596 s |
| Character and date projection | Rust | 1.159 s | 0.588 s | 1.192 s | 0.598 s |
| Grouped SUM | Java | 1.190 s | 0.956 s | 1.290 s | 1.007 s |

To repeat the consumer test, use a disposable checkout of the Forge and
platform revisions pinned in [GEN2.md](GEN2.md). Build the example in that
workspace so Cargo uses the engine's dependency versions.

```sh
mkdir -p "$FORGE/crates/circuit-ir/examples"
cp crates/sql/scripts/check_forge_output.rs "$FORGE/crates/circuit-ir/examples/"
cargo run --locked --manifest-path "$FORGE/Cargo.toml" -p circuit-ir \
  --example check_forge_output -- /tmp/gen2-run
```

The S2 system circuit was exported from an empty SQL file under the default
Gen-2 options with `--input_circuit --write-s2`. The stored bootstrap omits
compiler identity, runtime identity and input source, which the driver supplies
for each compilation. The Rust lowerer builds every user table and its schema
from typed S1. Pass `--rust-lowering` to the driver or benchmark for supported
constant views, scans, column projections and integer comparisons. Rust writes
S2 and Java converts S2 to S3. The separate-process
lowering test compares the Rust checkpoint with Java lowering of the same S1.
[Detached comparison](scripts/validate_s1_lowering.py)

The production IR reader and writer round-tripped **7,030,934 observations**
from all 8,571 classic and 8,527 exported Gen-2 programs in the frozen
corpora. Each graph passed provenance validation, independent structural
comparison and exact JSON value equality, including definition sites.
Each of the four completed shards matched its frozen input identities and
recorded zero failed cases. The full prefix-replay comparison is described
under S4 below. The observation totals per engine match the frozen Java
prefix-replay certificates. To repeat the check, run
`scripts/run_determinism.py INPUT OUTPUT --verify-ir` with the Java classpath,
Rust comparator and build identities for each export. Its captured runs also
compare repeated compilations. [IR corpus audit](evidence/p1-corpus-ir.json)

The earlier 16-program capture exercised 6,174 observations and showed
that the Rust CompactNames and TagRegions passes reproduce Java at every
invocation, including a placement-conflict case.
[Pass comparison](scripts/validate_optimizer_observations.py)

The Rust CSE and ARRAY_AGG fusion passes match 272 Java S4 invocations across
17 SQL programs on classic and Gen-2. The cases include joins, shared readers,
rolling windows, retained arrays and lambdas. The harness checks the exit name
counter on all 68 fusion invocations. Comparator failures exposed loss of type
sharing on a rebuilt sink and variable capture in nested closures.
[Directed Java and Rust comparisons](evidence/p1-spike8.json)

Java imported Rust-written S2 and S3 for two frozen programs per engine.
The resumed runs matched the direct runs on the checkpoint artifact contract.
Classic matched on bytes. Both Gen-2 programs differed on raw JSON bytes and
matched on the canonical circuit and dataflow artifacts.
[Checkpoint consumers](evidence/p1-checkpoint-consumers.json)

The expression-builder oracle exports inputs before Rust runs and compares
the result against a separate Java output, preserving sharing across input and
result. Its fixtures cover nested and nullable tuple expansion, SQL binary
coercion, balanced and left-to-right reductions, and aggregate lists of one,
two, three and five inputs. Both serialization modes include a positioned
aggregate call. The observation fixture supplies the call position held by
the Java prefix. The checkpoint fixture recovers it from relational provenance.
[Builder contract tests](sql-difftest/tests/builders.rs)

The bounded Rust port of `InsertLimiters` uses the original graph at
`OrderErrorConsumers` and reads the monotonicity analysis from the matching
expanded circuit. The port covers Java's source and view LATENESS logic,
key retention, window adaptation, sum and difference bounds, and error-view
collection (the slice from `waterlineValue` through the view visitor).
The directed comparisons exercise nullable columns, multiple declared
waterlines, inherited bounds, composite keys, materialized tables and
collection and delta streams. They check the complete S4 graph, the
checkpoint graph including provenance, and the exit name allocator.
Java rejects keyed LATENESS on Gen-2. The harness checks the Rust refusal
against the same input and Java diagnostic. Separate Java fixtures exercise
window adaptation and pointwise minimum on one, two, three and five inputs.
[Directed SQL](sql-opt/tests/fixtures/limiters.json) · [Builder comparisons](sql-difftest/tests/limiters.rs)

### P1 independent technical review

On 2026-11-19, an independent Codex reviewer traced
`CARDINALITY(ARRAY_DISTINCT(ARRAY_AGG(v))) GROUP BY k` from
`duplicate_branches.sql` through the typed circuit and the optimizer.
The aggregate and its reader use numbered output ports, and expression IDs
preserve sharing across both sites. The reviewer selected three pass invariants.

| Pass | What the reviewer verified in Rust |
|---|---|
| CSE | Equal projections share an earlier producer within the same body. Stateful candidates also need matching garbage collectors and control inputs. Merging one pair can expose another pair in the next iteration. |
| FuseIntoArrayAgg | A scalar reader moves into a grouped aggregate only when exclusive use and capture checks pass. The normal postprocessor and the empty-group result both change. Variables shadowed by a nested closure retain their original binding. |
| InsertLimiters | A separate LATENESS source trace showed saturating subtraction, a delayed bound for filtering, accepted rows on port 0 and errors on port 1. Reachability from ERROR_VIEW prevents a feedback edge. The grouped aggregate visitor lies outside this slice. |

The review found mismatches in SQL NULL construction, type equality, tuple
reads, positioned references and aggregate construction. The regression suite
now checks Java's rejection of a NULL struct field in a typed tuple, positioned
aggregate output types and exhaustion of operator IDs before any aggregate
operators are scheduled. A mismatched limiter sidecar now returns an error
when it substitutes the circuit root for a source. The reviewer ran 65 tests
and repeated 44 comparisons on 22 successful limiter invocations. The
independent technical review passed. The five-crate devcontainer run passed
68 Rust tests and Clippy with warnings treated as errors. Named maintainer
acceptance under §4.1 is pending.

### Shared expression dependencies assigned in P1

| Java behavior and caller | Rust owner and boundary |
|---|---|
| `ToRustInnerVisitor.generateFromTrait` calls `EliminateStructs`. Its rewrite descends through field types and preserves field order and nullability. | `sql-expr` owns the recursive transformation and inner-node rewrite traversal. `sql-ir` owns the typed graph, source identities and tuple constructors. |
| Column defaults in `ToRustInnerVisitor` call `CreateRuntimeErrorWrappers.wrapCasts`. It runs `Simplify`, then safe and unsafe cast expansion to their separate fixed points (100 iterations each), then constructs the runtime error handlers. | `sql-expr` owns this ordered composition, cast transformations, error-position context and expression substitution. `sql-opt` owns pass scheduling. The native writer consumes the same shared transformation. |
| Default printing applies `RewriteConnectorMetadata` to the wrapped expression before emitting the adapter closure. | The backend owns the adapter closure and its metadata argument. Any expression traversal it uses goes through `sql-expr`. The Java printer remains the migration oracle. |
| `ExpressionCompiler.expandTuple`, binary coercion and `combineAggregateList` construct typed graph fragments used by lowering and passes. | `sql-ir` owns these constructors and their checked arena edits. `sql-opt` owns monotonicity projections, CSE equivalence and ARRAY_AGG use analysis. |

Twenty-two directed programs went through Rust lowering, Java optimization and the Rust
artifact worker. They include constant views with and without a table, repeated column
references, string and date projections, and comparisons on nullable inputs.
[End-to-end lowering comparison](scripts/validate_s1_lowering.py)

The table oracle compares the checkpoint envelope, structural circuit
and the artifacts produced after resuming Java from the Rust checkpoint.
[Table lowering comparison](scripts/validate_table_lowering.py)

## Relational optimizer stages

[The stage contracts](optimizer-stage-contracts.json) classify all 32 placements
in `CalciteOptimizer`, and [the reconciled inventory](evidence/optimizer-stage-contracts.json)
attaches those purposes to all 80 configured rule descriptions and 71 rule
implementation classes. A rule can participate in several stage families.

The [local implementation decisions](evidence/rule-port-approaches.json)
choose faithful translation for 51 implementation classes and direct Rust
rewrites for 20. The rewrite choices cover simple relational rules and the
placements that use the shared typed folder. Each entry lists the parity
checks, original placements and failure policy. All 24 descriptions with
zero measured productions retain directed activation tests as an implementation
requirement. The three direct stages have explicit dispositions as well.
The archive identifies the local compiler and Calcite class files used to
verify the inventory.

Keep join-search candidate enumeration, cost comparisons, tie-breaking and
shared builder state in the translation. Preserve correlation bindings and
per-operator field remaps before circuit lowering. The folder rewrites use
the typed folding registry with each stage's substitution rules and failure
handling.

Preserve more than the rule set. Subquery removal and unsupported-plan rejection
abort on failure, while the remaining stages log and skip failed transformations.
The two join-order placements share one step instance, as do the two merge
placements. Several HEP builders append rules during `apply`; their history also
survives successive view compilations. A Rust port must preserve these observed
lifetimes until a separately reviewed change establishes equivalent behavior.
Window, aggregate, correlation, and set-operation rewrites retain the explicit
ordering constraints in the contracts. The CI audit rejects missing stages,
changed failure policy, and an unreviewed change to the construction source.
Independent maintainer acceptance remains pending.

## S4 observations and replay

All version-1 observations have `replayable: false`. The replay strategy for
**every observation point** is to run the same compiler from the original SQL,
with the same options and SQL `SET` state, through the complete prefix. An S4
file alone never authorizes resuming a pass. The version-4 S2 protocol in
[S2.md](S2.md) resumes the complete prefix. Its frozen-build audit passes strict
comparison on all 8,571 classic and 8,527 exported Gen-2 cases, including
dataflow, metadata, diagnostics, hash inputs and persistent ids; classic Rust
artifacts are byte-equal. See [the full certificates](evidence/s2-full-v4.json).
This certifies complete-prefix replay for those builds and populations, not
isolated S4 checkpoints or the excluded original Gen-2 tests.

`Passes`, `Repeat`, and executed `Conditional` bodies supply nested paths. Each
frame names the container and pass, pass index, and repeat iteration. An
unexecuted conditional has no inner observation. Direct composite calls are
instrumented as listed below. A graph-analysis observation may leave the circuit
unchanged while changing essential side data.

| Observation family | Side data not represented by the circuit | Replay requirement |
|---|---|---|
| `CircuitOptimizer` prefix and tail, including ordinary `Passes` entries | Compiler options, SQL `SET` variables and their positions, declarations/catalog, view origins, diagnostics, source contents, allocator/name state | Original frontend and complete prefix |
| `OptimizeWithGraph` / its `Repeat` / `Graph` | `CircuitGraphs`, predecessor/fanout information, convergence state | Rebuild graph on that iteration's input; preserve preceding iterations |
| The three `OptimizeProjections` placements | The shared `AnalyzedSet<DBSPOperator>` created by `CircuitOptimizer` | Preserve its lifetime across placements; restarting only the later wrapper loses history |
| `addWithMultisets` wrappers: `EnsureDistinctOutputs`, `PropagateConstants`, `OptimizeDistinctVisitor` | The `MultisetAnalysis` object passed into the following transform | Run the analysis and its consumer together on matching node identities |
| `ExpandAggregates` / `ExpandStreamAggregates` | The compiler-owned row-weight variable and each aggregate list's cached empty-result tuple/type, shared with existing expressions | Restore these identities at S2, then run the prefix; constructing structurally equal replacements changes the expanded graph and can break variable binding |
| `MonotoneAnalyzer` 0 `EnsureExpressionTrees`, 1 `Graph`, optional 2 `SeparateIntegrators`, optional 3 `Graph` | Original graph, separation results, expression identity | Run the composite prefix |
| `MonotoneAnalyzer` 4 `OrderErrorConsumers` | Error-reachable set and the ordering constraint used by the in-place sort | Recompute reachability and ordering from the prefix |
| `MonotoneAnalyzer` 5 `DeltaExpandOperators`, 6 `Monotonicity` | Expanded circuit, expansion map, `Monotonicity.info` | Keep original and expanded graphs and their identity correspondence |
| `MonotoneAnalyzer` 7 `InsertLimiters` | Expanded circuit, expansion map, monotonicity facts, error reachability | Apply to the **original** circuit, with facts from the expanded one |
| `MonotoneAnalyzer` 8 `MergeGC`, 9 `Graph`, 10 `CheckRetain` | GC grouping/rewrites and rebuilt circuit graphs | Run the complete composite; retain graph/consumer pairing |
| `MergeGC` nested passes | Shared grouping state and graph analyses used by its inner transforms | Replay the enclosing composite |
| `LowerAsof` 0 `Graph`, 1 `LowerAsofInner`, optional 2 `Graph`, 3 `OrderGcDependencies`, 4 `MoveGC` | Lowering's GC maps, artificial ordering edges, rebuilt graph | Replay lowering and ordering; serialized edges do not encode all scheduling constraints |
| `LinearPostprocessRetainKeys` 0 `Graph`, optional 1 `OrderWaterlineDependencies`, 2 `Graph`, 3 `ReplaceLinear` | Artificial waterline dependency edges and graph state | Replay the composite before replacement |
| Other composite transforms | Analysis/consumer pairs, repeated visitors, and declaration state listed in [S4-STATE.md](S4-STATE.md) | Complete prefix; never infer statelessness from an unchanged circuit |

The `DeltaExpandOperators` and `Monotonicity` observations describe the expanded
graph. The later `InsertLimiters` observation returns to the original graph after
inserting limiters. Comparing adjacent files as if they were successive versions
of one graph would misrepresent this control flow.

Observation-only circuit dumps do not restore frontend sources, `SET` state,
provenance objects, or analysis state. A schema JSON snapshot alone is insufficient. The
separate version-4 S2/S3 files carry source, SET and provenance state,
including pending post-optimization LATENESS checks.
S2 passes the full exported-population complete-prefix checks linked above;
S3 passes its recorded final-graph and Java-tail checks. Neither format is an
isolated S4 checkpoint or certified production seam.

The test compiler can replace 45 scheduled placements while the same
Java optimizer retains its state and runs the intervening passes. It also
supports Rust window-group selection within a Java graph rewrite. The adapter
checks the schema before reattaching live metadata and rebinds the compiler's
weight variable when the serialized graph carries that identity. Error-table
removal validates and applies a single schema edit to the live compiler.
Constant-view analysis returns sink identities, and the adapter emits the
diagnostics from the live source locations in sink order. Both RemoveNoops
sites preserve the current Java node identities on unchanged graphs, which
preserves repeat convergence. The identity-map transform and both noop sites
carry the generated-name counter. Rust advances it for temporary candidate
bindings and for rebuilt noop closures, and the adapter applies it after
decoding. The weight-validator transform also stays inside its
original Java condition so an imported S2 retains the same SQL option behavior.
Both recursive legality checks preserve Java's visit order. The connector
metadata check preserves rejection diagnostics for executable calls and allows
the same calls in column defaults.
The determinism and join-hint checks report through the live diagnostic API.
Replay JSON carries join-hint source positions through reattachment and
detached import.
A control that moves MergeSums to S2 changes 32 final results.
[Scheduled replay and side effects](evidence/multiset-twenty-two-placements.json) ·
[Weight-validator replay](evidence/multiset-weight-validation.json) ·
[Identity-map replay](evidence/multiset-identity-maps.json) ·
[Recursion validation](evidence/recursive-optimizer-validation.json) ·
[Connector metadata validation](evidence/connector-metadata-optimizer.json) ·
[Determinism and join-hint validation](evidence/optimizer-diagnostics.json) ·
[Aggregate region tagging](evidence/optimizer-regions.json) ·
[Trace retention and differential movement](evidence/optimizer-differentials.json) ·
[Join balancing](evidence/optimizer-balanced-joins.json) ·
[Linear retention](evidence/optimizer-linear-retention.json) ·
[Fanout cloning](evidence/optimizer-fanout-clones.json) ·
[Placement control](evidence/multiset-merge-scheduled-replay.json) ·
[Grouped stream placement](evidence/optimizer-stream-group.json) ·
[Window selection](evidence/optimizer-window-selection.json)

## Reproduce the P2 comparisons

Run from the workspace root in the devcontainer. Set `JAVA_TEST_CLASSPATH` to
the SQL compiler test classes, its dependency JAR and its Maven test dependencies.
The runtime replay reads the classic JSONL population and its corresponding
per-test corpus directory. The tail comparison accepts either engine's population.
Each run writes to a fresh directory.

```bash
set -o pipefail
p2_run=$(mktemp -d)
compiler=target/release/sql2dbsp
comparator=target/release/sql-difftest
tools=crates/sql/scripts
cargo build --release --locked -p sql2dbsp -p feldera-sql-difftest
cargo build --release --locked -p feldera-sql-difftest --example check_tail_corpus
python3 "$tools/validate_p2_hybrid.py" "$p2_run/hybrid" --compiler "$compiler" --comparator "$comparator" --classpath "$JAVA_TEST_CLASSPATH"
python3 "$tools/validate_p2_checkpoint.py" "$p2_run/hybrid" "$p2_run/restore.json"
java -ea -cp "$JAVA_TEST_CLASSPATH" org.dbsp.sqlCompiler.compiler.sql.tools.TailCorpusOracle "$CLASSIC_POPULATION" |
  target/release/examples/check_tail_corpus "$p2_run/tail-failures" > "$p2_run/tail.jsonl"
python3 "$tools/validate_corpus_tail.py" "$CLASSIC_POPULATION" "$p2_run/tail.jsonl"
python3 "$tools/run_p2_runtime.py" "$CLASSIC_POPULATION" "$CLASSIC_CORPUS" "$p2_run/runtime" --jobs 4 --target-pool "$p2_run/targets" --worker "$compiler" --classpath "$JAVA_TEST_CLASSPATH"
python3 "$tools/verify_p2_runtime.py" "$CLASSIC_POPULATION" "$CLASSIC_CORPUS" "$p2_run/runtime"
```

To check watermark restoration, pass `--lateness` to both the hybrid and
checkpoint scripts and use a fresh output directory. The runtime script's
`--resume` option verifies saved inputs, toolchain, classpath and batch logs
before reusing successful cases. Each batch builds a Cargo package named from
its input digest. The verifier matches each frozen case to its generated Rust
test name and to the result Cargo reported, including expected runtime errors.

To repeat the decoder budget on Linux, mount a fresh writable directory at
`/campaign` in the devcontainer. From the checkout, use the P0 archive identified
by its [recorded digest](evidence/resource-calibration-cpu-hour-archive.json).

```bash
cargo build --release --locked -p feldera-sql-ir --example decoder_campaign
tar -xzf .p0-artifacts/p0-resource-calibration-cpu-hour-20261118-v1.tar.gz -C /campaign campaign.py seeds/decoder.jsonl
mkdir -p /campaign/results
install -D "${CARGO_TARGET_DIR:-target}/release/examples/decoder_campaign" /campaign/target/release/p0-resource-calibration
python3 /campaign/campaign.py decoder
```

The result in `/campaign/results/decoder.json` records whether the worker
reached its CPU-hour budget and completed every batch within the deadline.

### P3 optimizer replay

| Pass | Classic invocations | Gen-2 invocations | Invocations that changed the graph |
|---|---:|---:|---:|
| [RemoveNoops](evidence/p3-remove-noops.json) | 18,067 | 18,341 | 1,148 |
| [CSE](evidence/p3-cse.json) | 50,927 | 50,641 | 1,105 |
| [FuseIntoArrayAgg](evidence/p3-array-agg.json) | 17,142 | 17,054 | 30 |
| [RemoveDeindexOperators](evidence/p3-remove-deindex.json) | 8,571 | 8,527 | 692 |
| [RemoveIAfterD](evidence/p3-remove-integrates.json) | 8,571 | 8,527 | 6,142 |
| [RemoveViewOperators](evidence/p3-remove-views.json) | 17,142 | 17,054 | 34,046 |
| [MergeSums](evidence/p3-merge-sums.json) | 8,571 | 8,527 | 8 |
| [LinearPostprocessRetainKeys](evidence/p3-linear-retention.json) | 8,571 | 8,527 | 103 |
| [ExpandHop](evidence/p3-hop.json) | 8,571 | 8,527 | 40 |
| [RemoveIdentityOperators](evidence/p3-identity-maps.json) | 8,571 | 8,527 | 746 |
| [FixupViewReferences](evidence/p3-recursive-views.json) | 8,571 | 8,527 | 59 |
| [BuildNestedOperators](evidence/p3-nested-operators.json) | 8,571 | 8,527 | 59 |
| [ChainVisitor iterations](evidence/p3-chain-fusion.json) | 18,919 | 22,657 | 7,380 |
| [OptimizeWithGraph(ChainVisitor)](evidence/p3-chain-fusion.json) | 17,142 | 17,054 | 7,380 |
| [PropagateConstants](evidence/p3-propagate-constants.json) | 8,571 | 8,527 | 990 |
| [OptimizeProjectionVisitor iterations](evidence/p3-optimize-projection-visitor.json) | 12,635 | 8,603 | 4,098 |
| [PullFilterVisitor iterations](evidence/p3-pull-filters.json) | 9,112 | 9,055 | 1,069 |
| [SimplifyConditionals](evidence/p3-simplify-conditionals.json) | 8,571 | 8,527 | 1,809 |
| [Simplify](evidence/p3-simplify-expressions.json) | 34,284 | 34,108 | 4,663 |
| [IndexedInputs](evidence/p3-indexed-inputs.json) | 8,571 | 8,527 | 1,678 |
| [ExpandStreamAggregates](evidence/p3-expand-stream-aggregates.json) | 8,571 | 8,527 | 1,907 |
| [FindUnusedInputFields](evidence/p3-find-unused-input-fields.json) | 8,571 | 8,527 | 2,496 |
| [TrimInputs](evidence/p3-trim-inputs.json) | 2 | 2 | 0 |
| [DeferCarriedColumns](evidence/p3-defer-carried-columns.json) | 8,571 | 8,527 | 18 |
| [TrimFilters](evidence/p3-trim-filters.json) | 16,643 | 16,533 | 962 |
| [TrimWindows](evidence/p3-trim-windows.json) | 16,643 | 16,533 | 82 |
| [FindCommonProjections](evidence/p3-find-common-projections.json) | 25,214 | 25,060 | 0 |
| [ReplaceCommonProjections](evidence/p3-replace-common-projections.json) | 25,214 | 25,060 | 1,024 |
| [LowerAsof.MoveGC](evidence/p3-asof-retention.json) | 9 | 9 | 18 |
| [ImplementChains](evidence/p3-implement-chains.json) | 8,571 | 8,527 | 7,323 |
| [UDFInliner](evidence/p3-udf-inlining.json) | 8,571 | 8,527 | 21 |
| [SubstituteLeftJoins](evidence/p3-recursive-left-joins.json) | 8,571 | 8,527 | 12 |
| [CheckRetain](evidence/p3-trace-integrators.json) | 8,571 | 8,527 | 0 |
| [SeparateIntegrators](evidence/p3-trace-integrators.json) | 601 | 8,526 | 583 |
| [OptimizeWithGraph(StrayGC)](evidence/p3-trace-retention.json) | 8,571 | 8,527 | 0 |
| [RemoveUselessLeftJoinsVisitor](evidence/p3-left-join-projection.json) | 8,571 | 8,527 | 13 |
| [DuplicateSharedIndexes](evidence/p3-shared-indexes.json) | 8,571 | 8,527 | 275 |
| [FindMapChains](evidence/p3-map-chains.json) | 8,571 | 8,527 | 0 |
| [CollapseSharedChains](evidence/p3-collapse-shared-chains.json) | 8,571 | 8,527 | 634 |
| [FindWindowGroups](evidence/p3-window-groups.json) | 8,571 | 8,527 | 0 |
| [ImplementWindowGroups](evidence/p3-implement-window-groups.json) | 8,571 | 8,527 | 6 |
| [ShareOutputIntegrators](evidence/p3-share-output-integrators.json) | 8,571 | 8,527 | 68 |
| [FilterJoinVisitor](evidence/p3-filter-joins.json) | 17,207 | 17,231 | 242 |
| [AppendOnly](evidence/p3-append-only.json) | 8,571 | 8,527 | 0 |
| [DecomposeExpensiveFilters](evidence/p3-hoist-filters.json) | 8,571 | 8,527 | 20 |
| [SimplifyWaterline](evidence/p3-simplify-waterlines.json) | 8,571 | 8,527 | 0 |
| [CircuitContainsNow](evidence/p3-clock-uses.json) | 8,642 | 8,589 | 0 |
| [BreakFilters iterations](evidence/p3-break-temporal-filters.json) | 138 | 125 | 130 |
| [DesugarNowEquality](evidence/p3-clock-equality.json) | 71 | 62 | 67 |

Rust and Java agreed on all 564,114 output ports across the three scheduled
multiplicity analyses per frozen program. This includes secondary outputs and
the facts used to generate persistent IDs. DISTINCT optimization changed 32
graphs in each engine population. On the 399 additional programs with set
outputs enabled it changed another 26 graphs. All pass boundaries matched,
and the packaged compilers produced identical artifacts.
[Multiplicity comparisons](evidence/p3-multiplicities.json) · [DISTINCT rewrites](evidence/p3-optimize-distinct.json)

With `--outputsAreSets` enabled on every 43rd frozen case, the DISTINCT insertion
pass matched the Java graph on 200 classic and 199 Gen-2 programs. The packaged
compiler also matched the direct SQL compile through S3, schema, diagnostics,
persistent-ID inputs and engine artifacts for all 399. The original frozen
options leave this pass disabled in both populations. [Set output comparisons](evidence/p3-distinct-outputs.json)

The two Merkle prefix passes, assignment of precomputed operator hashes and
persistent-ID validation matched Java on every frozen case. Java computes the
underlying Merkle hashes. The packaged replay also verifies that Java can
decode the type and closure of each synthesized GC operator before printing.
[Hash and validation comparisons](evidence/p3-operator-hashes.json) · [GC comparisons](evidence/p3-merge-gc.json)

Rust selected compatible accumulator groups for 632 classic and 631 Gen-2
aggregate lists containing multiple implementations. Each grouping and column
permutation matched Java. Rust expands those groups into the aggregate graph
using the preceding append-only analysis. The JVM reconnects a newly introduced
weight variable to the compiler's binding. All 17,098 pass boundaries matched,
including 1,907 graph changes.
[Aggregate grouping](evidence/p3-aggregate-groups.json) · [Expansion](evidence/p3-expand-stream-aggregates.json)

The clock analysis matched Java’s boolean result on the initial scan and on every
post-rewrite scan. Splitting temporal conjunctions and disjunctions changed 130
graphs. For disjunctions, the split subtracts the intersection once so a row
that satisfies both predicates keeps its original weight.

The frozen waterline closures required no rewrites. Four directed comparisons
exercised a boolean validity flag in both SQL and raw tuples. Rust specialized
both annotated closures and preserved both unannotated controls.

The comparison covers all 8,571 classic and 8,527 Gen-2 cases. SeparateIntegrators runs only for incremental circuits. CSE replay retains
the state of each Java pass instance across repeat invocations. It captures
shuffle object identity on entry because separate shuffle objects can contain
the same indexes and still compare unequal in Java. The standalone `--cse`
command starts a fresh pass. Reproducing an in-progress compilation requires
the scheduler to retain its CSE instance and the shuffle identities across
calls. ARRAY_AGG fusion removes
dead branches after each iteration and recomputes how many operators use each
producer. Removing a dead reader can make its producer eligible for fusion.

Chain fusion preserves single-reader map and filter closures, their relational
origins and the input stream type of an existing chain. A new chain below a
window takes the rebuilt window's output type. All 41,576 individual iterations
matched Java on graph structure, sharing, provenance and the generated-name
counter. [Chain fusion results](evidence/p3-chain-fusion.json)

The final trace-retention check validates the data input, the choice of
accumulating or non-accumulating retention, and the trace available to each
key or value policy. It checks the surviving siblings again after pruning.
Directed tests cover shared value-policy slots, distinct data ports and the
loss of the only integrating sibling. [Retention pass results](evidence/p3-trace-retention.json)

The recursive reference pass redirects readers inside a recursive component to
the declaration and external readers to its definition. Both packaged engines
reported the same undeclared-recursion error as direct Java, including the
source span and SQL snippet. Rust builds each recursive component as a nested
circuit, shares one delta operator for repeated uses of an external port, and
preserves each view's export number. The packaged compiler matched Java's
LATENESS errors for both a declared recursive view and its local helper view
in each engine. [Nested circuit results](evidence/p3-nested-operators.json) · [Recursive view results](evidence/p3-recursive-views.json)

Rust also ordered the consumers of the system error stream. The graph and the ordered suffix supplied to the limiter pass matched at all 17,098 placements. [Error-stream order](evidence/p3-error-consumers.json)

A separate comparison used every scheduled invocation of the following passes,
including calls inside composite passes. The zero classic Gen-2 count is the
expected result of its engine condition. One case in the Gen-2 population
explicitly selects classic compilation. [Individual pass results](evidence/p3-pass-boundaries.json)

| Pass | Classic invocations | Gen-2 population invocations | Changed graphs |
|---|---:|---:|---:|
| DeadCode | 317,006 | 314,506 | 42,991 |
| ComparatorDeclarations | 8,571 | 8,527 | 46 |
| Gen2Passes | 0 | 8,526 | 1,751 |
| CompactNames | 8,571 | 8,527 | 17,098 |
| TagRegions | 8,571 | 8,527 | 165 |

Conversion to incremental circuits matched Java at all 9,127 placements, including
the generated-name counter. Each conversion changed the graph. [Incrementalization](evidence/p3-incrementalize.json)

The incrementalization invariant and recursive legality checks matched Java at
9,127 and 34,196 placements respectively. Their CLI commands
are `--check-no-integral` and `--check-recursion`. A directed nested case checks
which violation is reported first and whether it is classified as an internal
compiler error. The mixed schedule runs both recursive legality checks in Rust, including the check
inside the recursive-components pass.
[Validator comparisons](evidence/p3-optimizer-validators.json)

Rust folded constant filters and preserved their input clock. All 17,098
individual placements matched Java, including 44 changed graphs. Empty-source
propagation matched all 51,294 placements, including 2,258 changed graphs.
Five additional join circuits exercised the empty-right rewrite with the
original typed join closure. The standalone commands are
`--remove-constant-filters` and `--propagate-empty-sources`. For the directed
cases, run `EmptyRightJoinOracle` on the five frozen case IDs and pass its
output to `check_pass_corpus`.
[Constant filters](evidence/p3-constant-filters.json) · [Empty sources](evidence/p3-empty-sources.json)

Rust pushes differentiation through maps with a single reader. The individual
pass matched Java at all 18,072 invocations. The enclosing fixed-point pass,
including dead-code elimination, matched at all 17,098 placements. Each
comparison included 974 changed graphs. Rust also matched all four enabled
weight-validation placements. Snapshot join lowering matched all 17,098
placements, including 195 that expanded a join into differentiators, a
physical join and an integrator. [Stream rewrites](evidence/p3-stream-rewrites.json)

Global aggregate expansion matched all 17,098 Java placements, including 832
graph rewrites. Removal of the two compiler-created tables matched at 34,067
placements. Rust selects unused table IDs. The JVM removes those inputs from
its graph and metadata, preserving the expression identities used by later
optimizations. Both function validators matched at all 17,098 placements each.
Direct Java and the packaged mixed CLI also returned the same complete diagnostics
for a misplaced connector metadata call, a function calling NOW and a
nondeterministic column default in both engine modes.
[Pass comparisons](evidence/p3-stream-rewrites.json) · [CLI diagnostics](evidence/p3-function-diagnostics.json)

Rust follows references and nested tuple projections to find which columns a
single reader uses. It passes the resulting paths to the live compiler, which
records warnings and unused-column flags and saves the analysis for input
trimming. The independent comparison checked field paths and both copies of
the source metadata at all 17,098 placements. Metadata changed at 2,496.
[Input field analysis](evidence/p3-find-unused-input-fields.json)

The typed Rust clock lookup matched Java on all 189,158 published outputs in
17,960 frozen S4 snapshots, including 66,990 delta streams and 1,064
waterlines. To reproduce, decompress each Java JSONL in the P0 stream-kind
archive and pipe it into the `check_stream_clocks` example from
`feldera-sql-difftest`, with a fresh failure directory.
[Clock comparisons](evidence/p3-devcontainer-checks.json)

The optimizer also runs RemoveNoops and source-preserving dead-code elimination
to a fixed point. Rust took the group's input and matched its final graph and
generated-name counter at all 17,142 classic and 17,054 Gen-2 invocations.
The group changed the graph 2,212 times. The RemoveNoops invocations inside
the group are already counted in the table. [Group results](evidence/p3-remove-noops-group.json)

The public `sql2dbsp --optimize-noops` command reproduced both placements of
the group for the array-index case in each engine mode. The comparator
rejected a deliberately rewired output and a changed name counter. The packaged build passed 184 Rust tests, 271 Python harness tests, 34 Maven
replay tests and 12 manager shadow tests. Clippy passed with warnings treated
as errors. [Devcontainer checks](evidence/p3-devcontainer-checks.json)

`PassCorpusOracle` records each invocation of a selected Java pass during the
uninterrupted SQL compilation, including iterations inside composite passes.
The Rust comparison uses the incoming circuit and generated-name counter. It checks
the outgoing counter, graph edges, sharing, provenance and output ports. Run this
for each frozen engine population from the workspace root.

```bash
set -o pipefail
pass=RemoveNoops
run=$(mktemp -d)
cargo build --release --locked -p feldera-sql-difftest --example check_pass_corpus
java -ea -Dfeldera.optimizer.streamInvocations=true -cp "$JAVA_TEST_CLASSPATH" org.dbsp.sqlCompiler.compiler.sql.tools.PassCorpusOracle "$CLASSIC_POPULATION" "$pass" |
  target/release/examples/check_pass_corpus "$run/failures" > "$run/results.jsonl"
python3 crates/sql/scripts/validate_pass_corpus.py "$pass" "$CLASSIC_POPULATION" "$run/results.jsonl"
```

The Rust consumer graph matched 812,633 Java graph constructions across both engines, including
recursive inputs and repeated uses of an input by the same operator. The two boundedness scans
matched on 34,196 invocations. All 17,098 expression-tree comparisons preserved source positions,
expression sharing and generated names. [Consumer graphs](evidence/p3-consumer-graph.json) ·
[Boundedness](evidence/p3-state-bounds.json) · [Expression trees](evidence/p3-expression-trees.json)

The GC comparison checks which Noops can share a trace, which policies can be removed, and
which retainers can be combined. Windows with a finite lower bound keep a private trace.
[Retention policy results](evidence/p3-merge-gc.json)

#### Live S2 replay and compilation

The Java optimizer can call Rust through JNI at the original pass positions.
It uses Rust for the four top-level CSE placements and every iteration of unused-field removal, sum simplification,
dead-code elimination, both no-op groups, both ARRAY_AGG placements,
integration cancellation, both view-removal placements, deindex removal,
hint checks, comparator declarations, Gen-2 rewrites, compact names and
region tagging. Rust lowers snapshot joins, pushes differentiators through maps
and inserts weight checks when the program enables them. Rust folds constant predicates and propagates empty sources
at all three scheduled placements. Rust also converts snapshot circuits to incremental form, runs
both integration-pushdown placements, checks that no integrator remains at the designated boundary and validates the final
recursive components. CSE receives the live flatmap shuffle identities and visited
constants for its placement. Rust also selects balanced joins and collects all
15 circuit statistics, including output types of nested ports. Rust applies CSE substitutions to live operators in each round, preserving the identities
held by the next unused-field iteration. The live comparison also checks the number of iterations
against direct Java. Java runs the remaining optimizer passes and calculates persistent IDs. With all-table generation
disabled, Rust returns unused table IDs and Java reports the warnings in
declaration order. Rust uses the compiler's canonical error-view name when
deciding which views to preserve.

Rust and Java selected the same shared projections across 50,274 analysis invocations.
Rust selected 747 producer groups on classic and 801 on Gen-2. The comparison
covered the readers, their field paths and the depth of each projection.
Java constructs the closures. Rust inserts the shared map or index and rewrites its
readers. The replacement changed 512 graphs per engine across the same 50,274
invocations. [Analysis](evidence/p3-find-common-projections.json) ·
[Graph replacement](evidence/p3-replace-common-projections.json)

Rust rebinds both ASOF retention predicates to the concrete join's ordered value
type and preserves each waterline input. All nine placements per engine changed
the graph. Java lowers the abstract ASOF join and supplies the association
between retainers and joins. [ASOF retention](evidence/p3-asof-retention.json)

Rust ordered the concrete joins before their retainers at all 18 active ASOF
placements. The exact permutations of the original operators matched Java in
both engines. The shared sort also preserved the linear aggregate retention
results on all 17,098 frozen cases. [Ordering comparisons](evidence/p3-asof-gc-order.json)

Rust replaced maximal join trees with star joins at 17,096 placements. Thirty
placements changed the graph, all matching Java. One program per engine disabled
star joins. Rust also discovers the maximal trees and builds their closures.
At all 17,096 active placements the ordered roots, input ports, shared expressions
and generated-name counter matched Java. A directed indexed-root test checks
the ordered leaves and names on both implementations. The subsequent replacement
on that directed circuit exposes a Java stream-kind error when it connects
collection inputs directly to the delta-only indexed star join.
[Join-tree analysis](evidence/p3-find-join-trees.json) ·
[Star join replacements](evidence/p3-star-joins.json)

Rust assigns canonical names to closure parameters and local variables, including
the row shared by an aggregate's closures. The names, types, expression sharing
and source positions matched Java at all 17,098 placements. The pass changed
6,930 classic and 6,906 Gen-2 graphs. A directed test checks binding order
when a closure receives another closure as an argument.
[Canonical name comparisons](evidence/p3-canonical-form.json)

Given the sibling groups discovered by Java, Rust builds one map for their
distinct output columns and one projection per original map. The rewrite
matched all 17,098 Java pass boundaries. Five programs per engine changed,
including a repeated expensive expression within one map and expressions
shared across views. [Map fusion comparisons](evidence/p3-map-fusion.json)

With star joins disabled, Rust expands multiway joins into a balanced tree of binary
indexed joins and projects the result back to the original column order. Each
engine exercised two visitor iterations on the frozen population, with one graph
change. Directed Java and Rust tests also cover five-way filtered joins, the
optional trailing map and expansion inside a nested circuit.
[Star join expansion comparisons](evidence/p3-remove-star-joins.json)

Only two frozen cases per engine enable input trimming, and neither changes its graph.
A separate 12-program comparison exercises ten rewrites per engine, including
retaining a nested ROW and trimming every column for COUNT(*). Both regressions
failed in direct Java before the fixes. The corrected Java and mixed compilers
agree on schema, diagnostics, final S3, persistent-ID inputs and emitted artifacts.
[Input trimming comparisons](evidence/p3-trim-inputs.json) · [Upstream fix](https://github.com/feldera/feldera/pull/8035)

In temporal filter analysis, Rust chooses which filters share each window input.
Java classifies the temporal expressions and supplies their source fingerprints.
Detached replay matched direct Java on all 71 classic and 62 Gen-2 programs in
the frozen population that ran temporal filter splitting, including competing
timestamps across views. [Temporal input selection](evidence/p3-temporal-input-selection.json)

Rust also orders the predicates on shared, bounded and upper-only timestamps.
Java rebuilds the typed conjunction after each partition and classifies any
casts it introduces. At the two pass boundaries, all 266 invocations matched
the Java graph and name allocator. The scans agreed on all 16 selected filter
inputs, and the reorders agreed on all 19 changed circuits.
[Temporal filter ordering](evidence/p3-temporal-filter-ordering.json)

Independent Java controls compiled the frozen SQL. Each candidate loaded a
detached S2 and ran the mixed schedule. Its Rust pass adapters completed 2,931,889 scheduled invocations
across the two populations, including fast paths that returned an unchanged circuit. Final S3, hash inputs, schema, diagnostics,
classic workspace bytes, canonical Gen-2 IR and parsed dataflow matched.
The clean Maven package initially hit Calcite nullability assertions on two
frozen set operations. Restoring the P0 coercion fix allowed all four
case/engine combinations to compile. The corrected package also matched the
earlier frozen controls on the entire population. Both sides start each SQL
case at generated variable zero, as a fresh CLI
process does. Some UDF bodies emit these names verbatim.
[Corpus receipts](evidence/p3-live-optimizer.json)

To repeat the comparison, build the comparator and the JNI library together.
Run it once per exported engine population in a fresh results directory.
The report checks the expected Rust placements for every case and records the
input, classpath, comparator and native library hashes.

```sh
cargo build --release --locked -p feldera-sql-difftest -p feldera-sql-opt-jni
python3 crates/sql/scripts/run_s2_corpus.py "$POPULATION" "$NEW_RESULT_DIR" \
  --rust-optimizer --classpath "$JAVA_TEST_CLASSPATH" \
  --comparator target/release/sql-difftest --timeout 5400 \
  --producer "$BUILD_COMMIT" --runtime "$BUILD_COMMIT"
```

Build the driver and native library together. Supply a Java compiler built
from the same checkout. A hybrid request selects this schedule with
`"rust_optimizer_passes": true`. The classic printer stays in the front-end
JVM while the isolated Rust worker reads the checkpoint. Once the worker
publishes its S3, a fresh compiler instance in that JVM prints the Rust code.
The Linux packaging workflow puts the driver and its matching JNI library
in the same directory on the manager image’s executable path. A local Linux
ARM64 test compiled grouped COUNT in both engine modes from that layout.
Select `--shadow-sql-compiler rust-optimizer`.
The comparison checks Gen-2 graph structure, sharing and persistent IDs
across compiler-local node numbering. A completed comparison emits UTC-day counters.
The manager counts successful comparisons of distinct accepted SQL texts, up to 200
per process and day. Duplicate compilations and rejected inputs add no programs.
Differences and incomplete attempts have separate counts. No SQL or fingerprint
leaves the process. Across restarts or replicas use the largest reported distinct
count for that day, and check all instances for differences. Adding distinct counts
across processes can count the same program twice.

```bash
cargo build --release --locked -p sql2dbsp -p feldera-sql-opt-jni
target/release/sql2dbsp --hybrid request.json new-attempt
```

The mixed compiler and direct Java each generated a classic executable for
grouped COUNT and a second program with LATENESS. All four cross-compiler
restores resumed without bootstrap and emitted the expected deltas.
[Reduced G9 checkpoints](evidence/p3-checkpoints.json)

All 8,256 classic executable cases passed on the 99-pass build with retained relational snapshots. The
verifier checked 11,407 input commands, 232 expected runtime errors and the
identities of the 315 compile-only cases across the 8,571-program population.
[Classic runtime replay](evidence/p3-classic-runtime.json)

The 81-pass build met the 1.2× p95 limit on the correlated array subquery (1.057×)
and the outer join (1.008×). Nexmark took 3.139× direct Java p95.
On the 83-pass build the ratios were 1.315× for the subquery, 2.560× for Nexmark and 1.222× for the outer join.
On the 84-pass build the ratios were 1.344×, 2.995× and 1.017×, respectively.
On the 86-pass build all three exceeded the ceiling at 1.293×, 2.781× and 1.289×.
The corrected 88-pass build measured 1.161×, 3.221× and 1.403×.
The 89-pass build measured 1.211×, 2.606× and 1.271×. The 91-pass build
measured 1.270×, 2.774× and 1.158×. With live graph encoding reuse, the
91-pass build measured 1.273×, 2.449× and 1.259×. The 94-pass build
measured 1.303×, 2.693× and 1.230×, all above the 1.2× ceiling.
With streaming circuit decode the 96-pass build measured 1.290×, 2.367× and 1.171×.
The corrected 97-pass build measured 1.175×, 1.980× and 1.241×.
The 99-pass build before the retained-relation change measured 1.312×, 1.994× and 1.248×.
With relational snapshots retained in the JVM the ratios were 1.177×, 2.061× and 1.180×.
Nexmark exceeded the ceiling with a hybrid p95 of 14.85 s against direct Java's 7.21 s.
Each timing covers SQL through emitted classic artifacts.
[Paired latency and profile](BENCHMARKS.md#packaged-mixed-optimizer-2026-11-20)

On the 21-pass build, cancellation p95 for four occupied workers was 33.23 ms
during runtime replay and 41.06 ms afterwards. All 40 cancellations returned
the expected status and published no artifacts. The 23-pass build measured
36.86 ms under replay and 25.09 ms afterwards, above the 25 ms limit.
With expression reuse, the 56-pass build measured 17.67 ms under replay
across 20 cancellations, with peak waiting RSS of 78,204,928 bytes per worker.
The 79-pass build measured 13.06 ms p95 across 20 cancellations during replay,
with peak waiting RSS of 78,290,944 bytes per worker. The 82-pass build
exceeded the cancellation budget at 38.03 ms p95. Peak waiting RSS was 78,192,640 bytes.
The next two builds measured 12.44 ms (83 passes) and 32.68 ms (84 passes).
Peak waiting RSS for the 84-pass build was 78,360,576 bytes. The 85-pass
build measured 16.89 ms p95 and 78,188,544 bytes peak waiting RSS.
The 86-pass build measured 26.83 ms, above the 25 ms budget, and
78,295,040 bytes peak waiting RSS. The 87-pass build measured 22.94 ms and
78,065,664 bytes. The initial 88-pass build measured 33.11 ms and 78,069,760 bytes. After the input-trimming
fixes, the 88-pass build measured 13.84 ms and 78,323,712 bytes.
The 89-pass build measured 23.26 ms and 78,123,008 bytes. The 90-pass build measured
15.88 ms and 78,131,200 bytes. On the 91-pass build, cancellation exceeded the
25 ms limit at 33.01 ms p95. Peak waiting RSS was 78,213,120 bytes. After
the live codec change, the four-worker run measured 10.81 ms p95 and
78,073,856 bytes peak waiting RSS. The 92-pass build measured 17.82 ms p95
and 78,204,928 bytes peak waiting RSS. The 93-pass run exceeded the cancellation
limit at 29.04 ms p95, with peak waiting RSS of 78,196,736 bytes.
The 94-pass build met the limit at 22.18 ms p95. Peak waiting RSS was 78,159,872 bytes.
The 95-pass build measured 18.60 ms p95 and 78,131,200 bytes peak waiting RSS.
The 96-pass build measured 29.04 ms p95, above the 25 ms limit, and 78,196,736 bytes peak waiting RSS.
The 97-pass build measured 8.55 ms p95 and 78,139,392 bytes peak waiting RSS.
The 99-pass build exceeded the cancellation ceiling at 27.54 ms p95. Peak waiting RSS was 78,307,328 bytes.
With relational snapshots retained in the JVM, the 99-pass build measured
8.02 ms cancellation p95 and 78,331,904 bytes peak waiting RSS.
The temporal input selection build measured 13.83 ms and 78,159,872 bytes, respectively.
The temporal ordering build matched both frozen populations and both restore directions
for COUNT and LATENESS. Its cancellation p95 was 13.82 ms with 78,217,216 bytes peak waiting RSS.
With the UDF and aggregate eligibility checks, replay and both restore directions
again matched. Cancellation p95 was 26.98 ms, above the 25 ms limit, with
78,045,184 bytes peak waiting RSS.
[Worker limits and trial timings](evidence/p3-worker-limits.json)

The release decoder processed 5,807,488 inputs in 3,600 worker CPU seconds
with zero crashes or hangs. Its slowest 32-input batch took 0.456 s against
the two-second deadline. [Decoder campaign](evidence/p3-decoder-campaign.json)

For an exported field window, run the daily counter report on all manager log
shards for that deployment. It reads structured JSON-lines logs, checks the
compiler build and counts only completed UTC days. An observed difference
starts a new window immediately, including on the current day. A day below the sample minimum contributes no qualifying day.

```sh
python3 crates/sql/scripts/shadow_sample.py --build "$COMPILER_BUILD" field-logs/*.jsonl
```

The owner confirmed on 2026-11-20 that no local field report exists. The P3
field gate stays open for 30 qualifying days with at least 200 distinct
field programs. The frozen corpus runs above are local engineering evidence.

## Review and measurements

[FREL.md](FREL.md) records the source-backed lowering and transient-model
requirements for the neutral relational model. The bounded schema and
[complete detached S1 replay](evidence/s1-prefix.json) now match all 17,098
exported programs through the full optimizer prefix. Transient forms and
production use beyond that frozen population still need explicit dispositions.

Each port records the Java source revision, supported subset, rejected forms,
and test evidence. A phase exit needs the independent maintainer review
required by `PLAN.md` section 4.1. The author
cannot substitute passing tests or self-review for that acceptance.

Use the corpus's explicit step semantics and table ordering. Nonincremental
tests contain whole relations; Gen-2's runner differentiates them before feeding
an incremental circuit. Preserve typed values, signed weights, expected errors,
and compaction boundaries. An exported expected table is not evidence that an
engine produced it.
