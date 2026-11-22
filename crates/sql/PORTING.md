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
| P3 | The opt-in mixed optimizer executed 106 Rust pass kinds in the verified replay. Detached S2 replay matched direct Java on all 8,571 classic and 8,527 Gen-2 programs through final S3, persistent-ID inputs and engine output. [Pass comparisons](#p3-optimizer-replay) | Finish temporal filter rewriting, monotonicity analysis and limiter insertion for the remaining circuits. Verify the complete optimizer at each S4 boundary and run the phase gates on that build. The latency ceiling and field-shadow window remain open. |
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

The hybrid optimizer now exposes the individual analysis, rewrite and cleanup
steps of dead-code elimination, CSE, chain fusion and ARRAY_AGG fusion. It also
exposes the final unchanged iteration of a fixed point. For the incremental
left join, all 422 classic S4 observations matched direct Java. Grouped and
rolling ARRAY_AGG also matched at every boundary in both engines. The corpus
runner's `--rust-optimizer --full-optimizer-trace` mode compares those ordered
observations through detached S2 replay. Fifteen sampled engine/program cases
matched through emitted artifacts. COUNT and LATENESS restored in both
directions. [Nested optimizer comparisons](evidence/p3-nested-optimizer-traces.json)

Rust also runs limiter insertion for snapshot DISTINCT and aggregates, and
applies the fused predicate to each of the four terms of a filtered left join.
The limiter entries, exits, diagnostics and generated names matched Java on 243
snapshot cases and 25 filtered left joins. A snapshot DISTINCT and GROUP BY
over a table with LATENESS matched at all 446 classic and 448 Gen-2 observations.
[Snapshot and filtered left join replay](evidence/p3-snapshot-filtered-left-limiters.json)

Rust inserts recursive limiters in the body that owns the operator. The 30
classic and 29 Gen-2 recursive cases matched Java at limiter entry and exit.
A transitive closure over a table with LATENESS matched at all 380 classic
and 382 Gen-2 observations. The native selector covers every operator class
in the archived limiter inputs for all 17,098 programs, including nested bodies.
[Recursive limiter comparisons](evidence/p3-recursive-limiter-bounds.json)

For a preceding ASOF match, the right timestamp waterline bounds the left
trace. The right trace keeps values above the minimum of both waterlines and
the latest value below it. All 46 newly eligible ASOF cases matched Java
at limiter entry and exit. The bounded streaming regression matched all 491
classic and 493 Gen-2 observations. [ASOF join bounds](evidence/p3-asof-join-bounds.json)

Rust walks the binary trees of a star join in delta order and takes the minimum
waterline across the resulting terms. All 61 eligible cases per engine matched
at limiter entry and exit. A grouped aggregate with LATENESS, HAVING and EMIT
FINAL matched through 442 classic and 444 Gen-2 observations.
[Star join bounds](evidence/p3-star-join-bounds.json)

Snapshot joins and indexed snapshot joins propagate waterlines on their expanded
output ports. The original ports still determine whether a downstream stateful
operator can retain its trace. All 533 selected classic limiter comparisons
matched Java. A bounded two-table join matched all 376 classic and 378 Gen-2
observations. [Snapshot join bounds](evidence/p3-snapshot-join-bounds.json)

For filtered inner joins, Rust uses the bound from each filtered delta to retain
values on the opposite input. All 36 classic and 130 Gen-2 cases that this
change enabled matched Java at limiter entry and exit. Seven programs per engine,
including bounded self-joins, also matched through all optimizer observations
and emitted artifacts. [Filtered inner join replay](evidence/p3-filtered-inner-joins.json)

Cast expansion preserves a written IS NULL when a nullable cast becomes a
`Some` expression. The two engines matched Java across all optimizer observations
on the streaming regression and four Gen-2 UNPIVOT cases.
[Cast expansion regressions](evidence/p3-cast-expansion-null-tests.json)

At `2a2398a2a`, Rust lowered trace-key retention for DISTINCT, positive
weights and linear aggregates. Each trace receives the preceding step's
waterline. Raw tuple and scalar bounds pass through a typed tuple so DBSP
can checkpoint the delay. The full limiter pass ran in Rust 6,580 times
on classic and 6,943 times on Gen-2. On both frozen populations, the
mixed optimizer matched direct Java at all 17,098 limiter entries and
exits, including generated-name counters. All final circuits, persistent-ID
inputs and artifacts matched. The 217 selected executable programs and
both checkpoint workloads passed.
[Full limiter comparison and runtime results](evidence/p3-aggregate-key-origin.json)

At `45ffbdba0`, Rust also propagated waterlines through no-op, negation,
deindex and snapshot antijoin operators. Four directed circuits exercised
projection reconstruction with and without LATENESS. The exit graphs and
generated-name counters matched Java in all four. Rust performed 6,492
whole limiter insertions on classic and 6,479 on Gen-2. All 17,098 frozen
programs matched through S3, persistent-ID inputs and engine artifacts.
[Projection and antijoin replay, runtime and restores](evidence/p3-limiter-transfer-operators.json)

At `5cbda903a`, Rust ran the entire `InsertLimiters` pass 6,484 times on
classic and 6,264 times on Gen-2. The selector covers circuits built from
sources, views, sinks, constants, maps, filters, HOP, flat maps and sums or
differences. This build left EMIT FINAL and Gen-2 keyed inputs with the Java
visitor. Across both paths, 550 selected limiter pass invocations matched
their Java entry and exit circuits. The limiter dumps from both compilers
also resumed independently with matching warnings and generated names.
[Limiter-insertion replay and runtime results](evidence/p3-limiter-checkpoints.json)

At `bdd647536`, Rust also handled EMIT FINAL and Gen-2 keyed-source
diagnostics. The worker returned Java's exact structured errors for a missing
waterline and unsupported keyed LATENESS. The 44 directed engine/program pairs
had seven expected refusals and matching emitted artifacts. In four
snapshot-mode warnings, the ordinary Java CLI included process-local operator
numbers that the circuit seam omits. Class, location, snippet, and warning
order matched. [Directed diagnostics and frozen replay](evidence/p3-limiter-diagnostics.json)

Rust builds the lower-bound expressions for scalar and temporal binary operations.
The JVM reconnects the live operands after Rust selects the operation, casts and
interval units. On `47efb1af2`, Java and Rust produced equal graphs in all 68
directed cases with a bound and agreed on the eight cases without one. All 550
selected monotonicity comparisons and all 17,098 frozen programs matched. The
217 executable classic programs in the selected runtime suite passed 2,076
commands. COUNT and LATENESS checkpoints restored in both directions.
[Binary waterline comparisons](evidence/p3-binary-bound-lowering.json)

Rust selects deindex operators from the circuit topology. The JVM builds each
map from the existing closure, declared output type and remapped input port.
The Java and hybrid pass matched at entry and exit on all 17,098 frozen programs.
The pass replaced 483 operators in 403 classic programs and 324 in 289 Gen-2
programs. On Nexmark, the request shrank from 2,669,326 bytes to 126,238 and
the reply from 307,954 bytes to 36. Both paired runs published identical files.
[Deindex pass, runtime and checkpoint comparisons](evidence/p3-deindex-live-maps.json)

For a typed NULL literal of a bounded scalar type, Rust emits the greatest
value of that type as its waterline. The Java and Rust expression graphs
matched in 140 comparisons covering SQL source positions, shared types and
freshly constructed types. All 550 selected monotonicity observations and
17,098 frozen programs matched direct Java. The selected classic suite passed
2,076 runtime commands, and COUNT and LATENESS checkpoints restored in both
directions. A separate comparison on the same build matched consumer order and
input slots at all 32,375 graph constructions on the 550 selected programs.
[Typed NULL replay and consumer graphs](evidence/p3-null-literal-waterlines.json)

At both `RemoveNoops` placements, Rust selects which no-ops have no retention
consumer. Each iteration forwards their original input ports and removes dead
operators. The selected replay matched Java at 648 classic and 587 Gen-2
iterations. Both full frozen populations matched through the engine artifacts.
On Nexmark the two placements sent 624,207 bytes across JNI, compared with
2,711,557 bytes on the preceding build, including graph construction and cleanup.
[No-op removal replay and transfer measurements](evidence/p3-noop-bypass-topology.json)

Rust orders linear aggregates after their retention waterlines and selects the
first matching retainer in the resulting order. The JVM reconnects the selected
live predicate and waterline. On the Nexmark suite the pass sent 150,815 bytes
to Rust, down from 2,679,666 bytes, including both graph builds. Artifacts matched
in both paired runs. All 103 active reorderings and 447 inactive placements
matched Java in the selected S4 replay. The full 17,098-program replay, selected
runtime commands and both checkpoint workloads passed.
[Linear aggregate retention evidence](evidence/p3-linear-retention-topology.json)

Rust derives lower bounds for the unsigned sort keys used by rolling windows.
The directed matrix covered 88 encodings and 88 decodings across integer and
temporal types, sort direction, nullability and null placement. All 55 cases
without an encoding bound agreed with Java, as did the 121 expression graphs
for cases with a bound. Both frozen engine populations and the 550 selected
monotonicity placements matched. Four-worker cancellation p95 was 8.19 ms in
isolation. [Sort-key replay and runtime evidence](evidence/p3-sort-key-waterline.json)

Rust computes the UNNEST waterline before applying the output shuffle. Each
copied input column keeps its bound. Array elements, map keys and values,
postprocessing results and ordinals receive unknown bounds of their own types.
Twelve directed cases across classic and Gen-2 matched Java on the closure,
final S3, persistent-ID inputs, diagnostics, schema and emitted artifacts.
The same build matched 550 monotonicity placements, 2,382 shared-projection
replacements and all 17,098 frozen programs. The selected classic programs
passed 2,076 runtime commands. COUNT and LATENESS checkpoints restored in
both directions. Four-worker cancellation p95 was 38.99 ms during replay and
6.41 ms in isolation against the 25 ms limit.
[Replay and resource measurements](evidence/p3-unnest-waterlines.json) ·
[Upstream regression](https://github.com/feldera/feldera/pull/8040)

Rust constructs the ASOF waterline closure for matches where the left timestamp
is greater than or equal to, or strictly greater than, the right timestamp. The
left timestamp takes the minimum of the two input bounds. Right columns and
the remaining left columns have unknown bounds. All 30 directed closure graphs
matched Java, including nullability, type sharing, source positions and generated
names. All 550 selected monotonicity placements and both full frozen populations
matched. [ASOF waterline evidence](evidence/p3-asof-waterline-transfer.json)

Rust selects chain fusions from reader counts, operator kinds, map closure
availability and `NoChain` annotations. The JVM constructs the selected
operators with their existing closure objects. Rust prunes dead operators
between rounds. Both scheduled placements matched Java on each of 550 selected
programs. Full replay matched in both engines, 2,076 runtime commands passed,
and COUNT and LATENESS checkpoints restored in both directions.
[Chain optimizer evidence](evidence/p3-chain-topology.json)

Rust constructs the HOP lower-bound closure. It forwards the input row and uses
the earliest window start as the lower bound for both window columns. Direct
closure comparisons matched Java for six nonnullable timestamps and one nullable
timestamp in each engine mode, including source positions and generated names.
The build matched the 550 selected monotonicity placements and all 17,098 frozen
programs. It passed 2,076 runtime commands and restored COUNT and LATENESS
checkpoints in both directions.
[HOP waterline evidence](evidence/p3-hop-waterline-transfer.json)

Filter trimming sends Rust the circuit edges, then the reader and predicate
closures for filters with one reader. The live pass matched Java at 1,832
placements across 550 selected programs and on both full frozen populations.
Across seven paired Nexmark compilations the median time spent in this pass
fell from 728 ms to 127 ms. Each pair produced identical artifacts.
[Filter-trimming measurements](evidence/p3-trim-filter-closures.json)

The two view-removal passes send view metadata and input ports to Rust. Their
placement, generated names, settings, diagnostics and canonical entry and exit
circuits matched Java at all 1,100 observed LATENESS/NOW placements. On Nexmark,
their combined request shrank from 3,544,712 bytes to 1,725. Across six measured
pairs their median combined time fell from 511 ms to 35 ms. The median time
summed over all instrumented passes rose from 5.18 s to 5.72 s. All seven pairs,
including warmup, produced byte-identical published artifacts and manifests.
[View-removal observations and profile](evidence/p3-view-forwarding.json)

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

Passing just the ordered inputs and recursive view roles reproduced all 812,633
consumer graphs. Across 12 paired JVM runs, the median time spent constructing
the 98 consumer graphs in the frozen Nexmark program fell from 246.9 ms to
80.3 ms. [Per-pass measurements](evidence/p3-consumer-graph.json)

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

Rust selected 747 shared projection groups on classic and 801 on Gen-2. It builds
the producer's projection and rebinds each reader to the narrower row. For a
nullable row it preserves the unwrap before the field access. The first reader
determines the type shared by the group. Across all 50,274 invocations, the
producer and reader closures matched Java, including their references into the
entry circuit, and the allocator and graph matched after replacement. The
replacement changed 512 graphs per engine. [Selection](evidence/p3-find-common-projections.json) ·
[Closure comparisons](evidence/p3-shared-projection-closures.json) · [Graph replacement](evidence/p3-replace-common-projections.json)

Rust rebinds both ASOF retention predicates to the concrete join's ordered value
type and preserves each waterline input. All nine placements per engine changed
the graph. Rust associates each retainer
with the join that reads the same data port. A pass comparison also found that
decoding a RANK operator broke a shared type reference to a downstream chain.
With the decoder repaired, all 46 active LowerAsof comparisons matched Java
through the complete pass in fresh JVMs.
[ASOF retention](evidence/p3-asof-retention.json) · [Pass comparisons and original failure](evidence/p3-asof-retainers.json)

Rust lowers each abstract ASOF join to two ordered indexes and a concrete join.
The Java type printer supplies the row text used to name the comparators. Rust
preserves retainer traversal order and records which retainers the clone visitor
would rebuild. All 24 classic and 22 Gen-2 ASOF cases matched Java at the inner
rewrite and after the complete pass, including generated names and provenance
sharing. [ASOF lowering comparisons](evidence/p3-asof-circuit.json)

Rust ordered the concrete joins before their retainers at all 18 active ASOF
placements. The exact permutations of the original operators matched Java in
both engines. The shared sort also preserved the linear aggregate retention
results on all 17,098 frozen cases. [Ordering comparisons](evidence/p3-asof-gc-order.json)

Rust replaced maximal join trees with star joins at 17,096 placements. Thirty
placements changed the graph, all matching Java. One program per engine disabled
star joins. Rust also discovers the maximal trees and builds their closures.
At all 17,096 active placements the ordered roots, input ports, shared expressions
and generated-name counter matched Java. On a directed indexed-root tree, both compilers differentiate the three leaves
in order and integrate the indexed result for its downstream consumer. The
closures produced the same nested row, and the complete rewritten circuits
were structurally equal. The corrected Rust pass also reproduced all eight
frozen result shards. [Upstream Java fix](https://github.com/feldera/feldera/pull/8037)
Rust needed only the graph edges for 16,962 of the 17,096 join-tree analyses.
For each isolated binary join it reserved the three names Java allocates for
the key and input values. The other 134 analyses read the closures.
[Join-tree analysis](evidence/p3-find-join-trees.json) ·
[Star join replacements](evidence/p3-star-joins.json)

Rust assigns canonical names to closure parameters and local variables, including
the row shared by an aggregate's closures. The names, types, expression sharing
and source positions matched Java at all 17,098 placements. The pass changed
6,930 classic and 6,906 Gen-2 graphs. A directed test checks binding order
when a closure receives another closure as an argument.
[Canonical name comparisons](evidence/p3-canonical-form.json)

Rust resolves variable references in each map closure and finds the smallest
expensive expressions that can be shared. It preserves repeated uses and can
move a whole let when its expensive child depends on the let binding. All
13 directed expression tests passed, including a let that shadows a map
parameter. Java renders canonical fingerprints and checks single maps for
duplicated columns. Rust uses those fingerprints to group sibling maps. On
95 classic and 94 Gen-2 programs the groups and resulting circuits matched
Java, including all five programs per engine that fused maps. Three fused two
maps and two shared a computation within a single map. Rust builds one map
for the distinct columns and one projection per original map. The rewrite
matched Java at all 17,098 frozen pass boundaries.
[Expression selection](evidence/p3-map-fusion-fragments.json) ·
[Sibling groups](evidence/p3-map-fusion-groups.json) · [Map rewrites](evidence/p3-map-fusion.json)

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
Java supplies source fingerprints for the timestamp expressions.
Detached replay matched direct Java on all 71 classic and 62 Gen-2 programs in
the frozen population that ran temporal filter splitting, including competing
timestamps across views. [Temporal input selection](evidence/p3-temporal-input-selection.json)

Rust also orders the predicates on shared, bounded and upper-only timestamps.
The first implementation rebuilt the typed conjunction in Java after each partition. At the two pass boundaries, all 266 invocations matched
the Java graph and name allocator. The scans agreed on all 16 selected filter
inputs, and the reorders agreed on all 19 changed circuits.
[Temporal filter ordering](evidence/p3-temporal-filter-ordering.json)

The first classification port analyzed each filter's conjunction in Rust using clock-use
and monotonicity facts from Java. A one-sided monotone clock comparison becomes a
window bound. The first residual condition constrains how the remaining predicates
can be grouped. Scan, Reorder, MergeFilters and RewriteNow matched at all 532
observed invocations, including 35 merges and 133 clock rewrites.
[Temporal comparison replay](evidence/p3-temporal-comparisons.json)

Rust builds the index and output projections for temporal windows. A timestamp
column moves into the index key and returns to its original position after the
window. A computed key retains the full row as its value. All 133 active
`RewriteNow` invocations matched the preceding Java build on both engines.
[Temporal window projections](evidence/p3-temporal-window-projections.json)

Rust also builds the bound closures. They guard clock-dependent expressions
until the first clock value arrives and preserve the SQL source locations of
rewritten calls. All 133 temporal comparisons matched the same Java reference.
[Window bound comparisons](evidence/p3-temporal-window-bounds.json)

For maps and residual filters that need a stream join, Rust builds the index,
appends the clock to the joined row and drops it from the filtered output.
The pass matched Java on all 71 classic and 62 Gen-2 temporal cases.
[Clock join comparisons](evidence/p3-clock-join.json)

Rust rebinds each joined map or residual filter to its widened input and reads
the clock from its final column. The 133 temporal pass comparisons matched,
and 52 classic programs passed all 181 input commands. Two directed compiler
runs matched Java’s ordered diagnostics when a nested lambda used `NOW()`,
one in a projection and one in a filter. [Clock closure comparisons](evidence/p3-clock-closures.json)

The built-in and table-backed clocks use Rust to index the timestamp. Rust
also builds the table’s maximum aggregate and the scalar waterline that feeds
each temporal window. Both Java and hybrid paths matched the saved Java
reference at all 133 temporal pass boundaries. [Clock source comparisons](evidence/p3-clock-source.json)

Rust folds compatible bounds for each temporal window in source order. It builds
the bound function, nullable-key filter, index and optional row restoration in
one call. Both compiler paths matched the Java reference at all 133 active
`RewriteNow` boundaries. [Temporal window comparisons](evidence/p3-temporal-window.json)

The hybrid optimizer uses the Rust consumer graph when looking for windows
that can share an input. Rust rebuilds the filter conjunction in the chosen
order, preserving nullable casts between successive partitions. All 133
active `Reorder` boundaries matched the saved Java reference.
[Temporal conjunction comparisons](evidence/p3-temporal-conjunction.json)

Rust traces each filter through single-reader maps, composes its timestamp with
those maps, selects sharing groups in circuit order, and rewires the reordered
filters. Java supplies the clock monotonicity facts and prints the timestamp
fingerprints. Both pass boundaries matched the Java reference on 71 classic and
62 Gen-2 programs each. Full replay of the 105-pass build matched all 17,098
programs. [Scan and reorder comparisons](evidence/p3-temporal-scan-reorder.json)

The initial Rust analysis identified indexes with one eligible consumer and a decomposable value.
It partitioned them by the producer output port and value nullability, then
built the key closures for Java to compare. Across both frozen populations,
the ordered groups, visited indexes, name counters and diagnostics matched
at all 17,098 boundaries. The 813 groups contained 2,266 index inputs.
[Shared-index candidate comparisons](evidence/p3-shared-index-candidates.json)

Rust now selects the ordered groups and compares their keys, including
temporal arithmetic, session-window ordering and calls with generic path
arguments. The standalone optimizer matched the same 17,098 Java observations
and all 2,431 key comparisons ran without invoking the JVM. In the live
compiler, Java can compare expression forms outside the native comparator.
[Standalone and live shared-index results](evidence/p3-shared-index-groups.json)

Rust projects the shared key and each narrow value field onto the same row
variable. It preserves required fields inside nullable tuples. Both compiler
paths matched the Java reference at every frozen `ReplaceSharedIndexes`
boundary, including the graph consumed by the following dead-code pass.
[Wide index field projections](evidence/p3-wide-index-fields.json)

The Rust splitter tries fixed-width consumers first, widest first, and keeps
the first equivalent expression for each wide column. Across the two frozen
populations the standalone checker matched Java's 723 wide indexes from 813
candidate groups, including all 2,205 narrow column positions. The live
compiler matched Java at all 17,098 pass boundaries, including diagnostics
and generated names. [Split and layout comparisons](evidence/p3-split-wide-indexes.json)

Rust also emits the wide index at its first reader, rebinds join and aggregate
closures by declaration and updates the consumer graph. The standalone pass
matched all 17,098 Java exits, including generated names and SQL source
locations. It changed 321 circuits. The live replay matched every final S3,
persistent-ID input and engine artifact. COUNT and LATENESS restored in both
directions. Four-worker cancellation p95 was 18.90 ms.
[Replacement pass](evidence/p3-replace-shared-indexes.json) · [Live replay](evidence/p3-live-optimizer.json)

On build `80f10608a`, all 8,571 classic and 8,527 Gen-2 programs matched
direct Java through final S3 and their emitted artifacts. COUNT and LATENESS
checkpoints restored in both directions. With four workers and corpus replay
running, cancellation p95 was 24.96 ms against the 25 ms ceiling. The
largest waiting worker used 80,048,128 bytes across its driver and front end.
[Artifact replay](evidence/p3-live-optimizer.json) · [Checkpoint restores](evidence/p3-checkpoints.json) · [Worker measurements](evidence/p3-worker-limits.json)

Rust now finds clock calls in the predicate itself and combines residual
conditions using the typed expression builders. Java supplies the monotonicity
of comparison operands. All 532 `Scan`, `Reorder`, `MergeFilters` and
`RewriteNow` invocations matched the saved Java reference.
[Typed temporal comparison results](evidence/p3-typed-temporal-comparisons.json)

Rust builds the row and indexed identity closures used by monotonicity analysis.
For an unindexed filter it extracts lower bounds from comparisons, including
columns offset by numeric or temporal literals. The Java transfer functions
consume the resulting expressions. All 291 classic and 259 Gen-2 programs
containing `LATENESS` or `NOW()` matched through limiter insertion and cleanup,
including the generated-name counter and diagnostics.
[Monotonicity and filter bounds](evidence/p3-monotone-filter-bounds.json)

Rust also selects the lower bound for binary arithmetic, extrema and
`COALESCE`. A nullable operand can bound `MAX_IGNORE_NULLS` only when the
expression cannot produce NULL. Time-of-day arithmetic can wrap at midnight.
The expanded-graph comparison matched Java on all 550 selected programs,
including tuple-valued expressions with no lower bound.
[Binary waterline comparisons](evidence/p3-binary-waterlines.json)

Rust selects the runtime function that bounds a scalar call. It checks which
argument is monotone, which companions must be constant and when a saturating
implementation is needed. Java builds the call from the selected runtime name.
The selector matched Java on 29,888 combinations covering 475 registered names
and unregistered variants. All 550 selected monotonicity boundaries matched.
[Function waterline comparisons](evidence/p3-function-waterlines.json)

Rust selects the waterline for a scalar cast. A cast that can fail can use a
saturating bound when the runtime provides one. A failing `SAFE_CAST` rules
out that bound because it can produce NULL. The selector matched Java on
8,836 combinations of source type, destination type, nullability and cast
mode. All 550 selected monotonicity boundaries matched.
[Cast waterline comparisons](evidence/p3-cast-waterlines.json)

The standalone limiter replay infers collection and delta clocks from the
original circuit. It checks the recorded clocks when replaying older traces.
All 24 directed limiter invocations matched Java on the S4 graph, checkpoint
and next generated name. Four keyed Gen-2 inputs produced the expected refusal.
[Limiter clock comparisons](evidence/p3-limiter-clocks.json)

The live optimizer applies empty-stream edits to existing operators. When
the right side of a left join is empty, Rust specializes the join closure to
produce the null-extended row. Direct Java and the live optimizer matched
at all 6,558 empty-stream placements in the selected classic and Gen-2
populations and on five directed empty-right joins in each engine.
[Empty-stream comparisons](evidence/p3-empty-sources.json)

For an `EMIT FINAL` view, Rust indexes rows by the finality column and filters
out NULL keys before the window stores them. The detached pass also builds
the clock adapters and delayed key retention. Incremental and collection
examples matched the Java checkpoint, S4 graph, and generated names in both
engines. The live optimizer uses the same Rust closure builder.

Rust derives a temporal window's output waterline from its lower endpoint.
For a window with an unbounded lower endpoint, it takes the minimum of the
incoming key waterline and the upper endpoint. All 550 selected limiter
boundaries matched direct Java. The classic runtime replay passed 2,076 input
commands across 217 programs. [Window waterline comparisons](evidence/p3-window-waterlines.json)

Rust binds limiter closures to the actual waterline port types and reduces
their calls before Java inserts the apply operators. The resulting graph,
generated names and diagnostics matched Java at all 550 selected
`InsertLimiters` boundaries. [Limiter closure comparisons](evidence/p3-limiter-applications.json)

Rust builds the closure that combines a join’s input waterlines. It uses the
available bound when only one side supplies a key. When both sides bound the
key it takes the pointwise maximum. All 291 classic and 259 Gen-2 selected
limiter boundaries matched Java. [Join waterline comparisons](evidence/p3-join-waterlines.json)

Rust constructs the transfer closures for LAG/LEAD, ordered filter bounds,
grouped aggregates and rolling aggregates. The rolling frame propagates its
timestamp when it ends at or before the current row. LAG/LEAD matched Java
on eight selected cases per engine. The filter and aggregate builds each
matched Java at all 550 selected monotonicity boundaries. Each build matched
both full frozen populations, passed 2,076 runtime commands and restored
COUNT and LATENESS checkpoints in both directions.
[Waterline comparisons](evidence/p3-aggregate-and-filter-waterlines.json)

For a primitive MIN, Rust propagates the bound on the compared field through
the aggregate's postprocessing closure. For ARG_MIN it leaves payload fields
unbounded. A directed test checks a nested nullable tuple that requires a cast
on the compared field. Both frozen populations and all 550 selected
monotonicity boundaries matched Java. [Primitive minimum comparisons](evidence/p3-primitive-min-waterline.json)

For a chain of minimums, Rust binds each update's compared field to the
incoming row and builds the bound for each accumulator. It checks that every
initializer and update minimizes the same row expression. Comparisons that
need an additional Java expression-equivalence rule use the Java check.
All 550 selected monotonicity boundaries and both full frozen replays matched.
[Chained minimum transfers](evidence/p3-chain-minimum-waterline.json)
[Eligibility comparisons](evidence/p3-chain-minimum-eligibility.json)

For tables and views with LATENESS, Rust builds the predicate that admits a
row and the error payload for a rejected row. The 550 selected limiter
boundaries matched Java, including 997 controlled filters and 29 keyed
sources. The classic runtime replay executed all 217 cases with input
steps in this selection and checked 2,076 input commands. The packaged
build also matched both full frozen populations and restored COUNT and
LATENESS checkpoints in both directions. [Late-row filter comparisons](evidence/p3-lateness-filters.json)

Rust also filters deletions and partial updates that supply only a primary
key. The classic build matched Java for all 29 keyed waterline sources in
25 programs. Thirteen programs supplied runtime inputs and passed all 104
commands. A directed case checks two late key columns at different positions
in the key and in the waterline. [Late-key filter comparisons](evidence/p3-late-key-filters.json)

Rust constructs each table and view’s declared waterline from its LATENESS
columns. A null field contributes its type minimum, and subtraction saturates.
The mixed build matched Java at all 550 selected limiter boundaries. A directed
case with eleven nullable numeric and temporal columns checks the live and
detached builders in both engines. It covers the midnight `TIME` literal and
the UTC minimum for `TIMESTAMP WITH TIME ZONE`. All 217 executable classic
programs in the selection passed their 2,076 input commands.
[Declared waterline comparisons](evidence/p3-declared-source-waterlines.json)

When a view declares LATENESS and inherits a waterline, Rust combines the
two bounds. A column covered by both takes the maximum. The 550 selected
limiter boundaries matched Java, including views with a tighter inherited
bound, a tighter declared bound, and bounds on different columns.
[View waterline comparisons](evidence/p3-view-waterlines.json)

Rust projects compacted waterline rows and combines bounds with a balanced
pointwise minimum. All 550 selected limiter boundaries matched, including 386
new multi-input apply operators with two to five inputs. A directed test checks
that two absent fields share their declared empty-tuple type. The packaged
compiler matched all 17,098 frozen outputs and restored both checkpoints in
both directions. Four-worker cancellation p95 was 16.84 ms.
[Projection and minimum comparisons](evidence/p3-waterline-projections.json)

Rust chooses the key for each candidate left-join chain and decides whether
deferring columns saves enough work. Across 17 classic and 17 Gen-2 cases,
Java and Rust selected the same 20 chains and agreed on every column layout.
[Left-join key selection](evidence/p3-chain-key-choice.json)

During unused-field removal, Rust finds the fields used by each distinct map
and map-index closure. It builds a projection of the input and rewrites the
reader to use the retained fields. All 33,176 iterations matched Java
across the two frozen populations. The comparison checks graph sharing,
source positions, generated names, diagnostics and pass state. [Map input
closures](evidence/p3-map-input-closures.json) · [Field dependencies](evidence/p3-map-dependencies.json)

Rust also decides when narrowing would reproduce the original map. When a
map reads an index and emits ordinary rows, it checks the reader before
allocating a name for the projection. Field analysis now follows each join
parameter separately and handles the rank and row parameters of TopK. UNNEST
and linear aggregates use the same analysis. All 33,176 iterations matched
the Java package built before these changes. [Parameter field comparisons](evidence/p3-parameter-field-use.json)

Rust also computes the fields read by a map-index after an aggregate or TopK.
The first implementation passed those paths to the Java rewriter. The graphs and
generated-name counters matched at all 1,872 projection iterations on the 296
selected inputs. Of those iterations, 770 changed the graph.
[Projection field comparisons](evidence/p3-projection-fields.json)

Rust composes ordinary maps, indexed maps and join output closures. It also
splits an antijoin projection into a value projection before the join and a key
projection after it. When an indexed producer is expensive, the unrestricted
pass can bind the result once and reuse its key and value.
Rust classifies strict and nested projections and narrows TopK output rows,
preserving the key and rebinding each value field to its new offset.
It drops unread aggregate components and fills their original positions with
the corresponding empty-set results. It also composes UNNEST shuffles and
projects the default row of an aggregate over an empty input.
All 86,472 projection invocations matched Java across 8,571 classic
and 8,527 Gen-2 programs. The comparison checked the entry and exit graphs,
including type sharing and source positions, as well as the allocator,
diagnostics and pass state. [Projection composition comparisons](evidence/p3-projection-composition.json)

For compatible adjacent temporal filters, Rust constructs one predicate with a
shared row parameter and reduces both calls. All 71 classic and 62 Gen-2
pass comparisons matched Java. The executable classic cases passed all 181
input commands. Rust now groups adjacent bounds by row expression, direction and
inclusivity. It preserves the position of residual predicates. Java resolves key
equivalence for expression classes outside the Rust comparator. Both temporal
passes matched Java on the 133 active programs.
[Temporal predicate comparisons](evidence/p3-temporal-predicate-merge.json) · [Filter grouping](evidence/p3-temporal-filter-groups.json)

Rust lowers a fused join, filter and optional map to a single closure. The
unprojected form binds the joined row once. The projected form applies the map
only when the predicate succeeds. The comparison covered 198 inner joins,
20 left joins and two star joins. All 160 pass boundaries matched on graph
structure, generated names and diagnostics. Both JVMs replayed the same S2
files to preserve the Calcite relation IDs observed on entry.
[Join-filter lowering comparisons](evidence/p3-join-filter-map.json)

Rust lowers classic aggregate lists to folds. Java and Rust agreed at all
8,572 applicable lowering boundaries, including the 815 folds constructed in
412 programs. The 8,526 programs that enabled Gen-2 kept their aggregate lists.
[Aggregate fold comparisons](evidence/p3-aggregate-fold.json)

Rust constructs the iterator closure for UNNEST, including nullable arrays and
maps, projections, and ordinality. The two engines exercised 399 flat-map
operators across 373 programs. All 17,098 lowering boundaries and final
artifact comparisons matched Java. The COUNT and LATENESS checkpoints restored
in both directions. [UNNEST lowering comparisons](evidence/p3-flatmap-lowering.json)

Rust now runs the whole circuit lowering pass, including identity streams and
optional debug instrumentation for apply operators. On TPC-H, rebuilding a map
chain after lowering a fused join exposed a type-sharing difference. A rebuilt
map chain now allocates a fresh collection type. Filter-only chains retain
their declared input type. All 17,098 lowering boundaries matched, including
656 changed classic graphs and 356 changed Gen-2 graphs. Three directed cases
also matched Java with apply tracing enabled. Decoding the TPC-H exit graph
exposed a separate checkpoint defect. Each engine's 57 chain operators lost
their shared collection type on decode. All 57 in each engine now retain it.
[Circuit lowering comparisons](evidence/p3-circuit-lowering.json)

Rust counts distinct uses of an expensive expression within each closure. A
borrow, clone, dereference or tuple passes its own uses on to its operands.
Java supplies canonical expression IDs. Rust constructs each lazy initializer, nests
earlier bindings around later consumers and inserts block declarations after
their last local dependency.
All 187 selected pass invocations matched on entry and exit. Two classic cases and one
Gen-2 case introduced a lazy binding. The 22 expression-equivalence tests also
passed with Rust choosing the insertion point.
[Expression-use comparisons](evidence/p3-expression-uses.json) · [Binding placement](evidence/p3-binding-placement.json)

Rust classifies row-independent expressions, selects lazy cells for static storage,
and resolves reads of those cells across lexical scopes. It hashes the classic
printer's rendering of the initializer and its type, then constructs the inline
value or a named declaration and its typed reference. Rust renumbers error
handlers in visit order across static initializers. Repeated visits reserve
new slots while retaining the first translation. Java decides when identical
literals share a declaration.
All 8,572 enabled invocations matched on the saved S2 inputs. Forty added a total
of 279 declarations. The other 8,526 programs ran in Gen-2 mode, which skips
this pass. The expression-equivalence suite also exercised both inline and declared
statics. [Static-cell comparisons](evidence/p3-static-cells.json) ·
[Declaration comparisons](evidence/p3-static-items.json) ·
[Error index comparisons](evidence/p3-static-errors.json)

To repeat a paired pass comparison, capture the population once, then give
both JVMs that checkpoint directory. Each observation records the S2 digest.

```sh
oracle=org.dbsp.sqlCompiler.compiler.sql.tools.PassCorpusOracle
java -ea -cp "$JAVA_TEST_CLASSPATH" "$oracle" --capture-s2 "$POPULATION" "$NEW_S2_DIR"
for rust in false true; do
    java -ea -Djava.library.path="$NATIVE_LIB_DIR" -Dfeldera.optimizer.compareRustJoinFilterMap="$rust" \
        -cp "$JAVA_TEST_CLASSPATH" "$oracle" "$POPULATION" LowerCircuitVisitor "$NEW_S2_DIR" > "lower-$rust.jsonl"
done
target/release/sql-difftest --compare-pass LowerCircuitVisitor "$POPULATION" lower-false.jsonl lower-true.jsonl
```

For the deindex comparison, use `RemoveDeindexOperators` as the pass name in
both JVMs and in `sql-difftest --compare-pass`. Set
`-Dfeldera.optimizer.compareRustTopLevelPass=RemoveDeindexOperators` only in
the candidate JVM. Both runs then use the same Java optimizer prefix.

Independent Java controls compiled the frozen SQL. Each candidate loaded a
detached S2 and ran the mixed schedule. Its Rust pass adapters completed 2,948,987 scheduled invocations
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

All 8,256 classic executable cases passed on the temporal-window build at `6561f4e40`. The
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
On the UDF and aggregate eligibility build, p95 measured 1.231×, 2.222× and 1.337×, respectively. Each timing covers SQL through emitted classic artifacts.
On the static-cell build, the subquery measured 4.52 s hybrid and 3.28 s
direct at p95. Nexmark measured 15.32 s and 8.39 s, and the outer join
measured 3.70 s and 3.28 s. Only the outer join met the 1.2× ceiling.
With compact checkpoints, the Nexmark hybrid p95 was 15.657 s across 21
measured pairs. Direct Java took 7.567 s. [Checkpoint and compilation measurements](evidence/p3-checkpoint-write.json)
The static-declaration build measured 1.237× on the correlated array subquery, 2.435× on Nexmark and 1.198× on the outer join. Each ratio uses 21 measured SQL-to-artifact pairs.
The subsequent circuit-writer build measured 1.246×, 2.112× and 1.214× on the same workloads. All 69 warmup and measured pairs matched on diagnostics and emitted artifacts.
With compact consumer graph inputs (`f08e3eb5d`), the ratios were 1.072×, 2.394× and 1.131×. Nexmark exceeded its p95 ceiling by 5.39 seconds. [Paired timings](BENCHMARKS.md#packaged-mixed-optimizer-2026-11-21)

With compact view forwarding and grouped aggregate transfers, the hybrid/direct
p95 ratios were 1.198× for the array subquery, 2.257× for Nexmark and 1.088×
for the outer join. Nexmark took 13.923 s at p95 against 6.169 s for direct
Java. All 69 pairs matched artifacts and diagnostics, including the six warmups.
[Packaged latency measurements](evidence/p3-hybrid-latency.json)
The checked waterline build (`5a92a1eb3`) measured 1.247×, 2.521× and 1.117× in the same workload order. The outer join met the 1.2× ceiling. [Paired latency and profile](BENCHMARKS.md#packaged-mixed-optimizer-2026-11-21)

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
The temporal classification, left-join key and unused-field builds each
matched both frozen populations and restored COUNT and LATENESS checkpoints
in both directions. Their four-worker cancellation p95s were 35.25 ms,
15.35 ms and 10.82 ms, respectively. Peak waiting RSS on the unused-field
build was 78,012,416 bytes per worker. The shared-projection build measured
25.95 ms during corpus replay and 10.81 ms with replay idle. The subsequent
map-input build measured 7.49 ms with replay idle, with 77,963,264 bytes
peak waiting RSS. On the isolated-join build (`65fbccc7f`), four workers
cancelled in 11.87 ms at p95 during frozen corpus replay. Peak waiting RSS
was 78,434,304 bytes per worker. The lazy-binding build measured 21.23 ms
and 78,258,176 bytes under the same four-worker profile. The filter waterline
build measured 7.92 ms and 78,315,520 bytes.
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
