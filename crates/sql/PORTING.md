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
| P3 | On `bf8cd8423`, all 17,098 frozen compilations matched Java through 6,996,738 ordered S4 observations and emission, and all 8,256 executable classic cases passed. On `1193f3252`, 252 selected aggregate compilations matched across 104,428 ordered observations and emission. COUNT and LATENESS restored in both directions. On `46cbf6890`, live propagation distinguishes key and value waterlines. Its highest cold compile p95 ratio was 1.097×. The earlier decoder campaign completed one CPU-hour on `881233749`. [Frozen replay and classic runtime](evidence/p3-evaluated-statement-fields.json) · [Aggregate trimming and restores](evidence/p3-stream-aggregate-inputs.json) · [Latency](evidence/p3-indexed-waterline-propagation-cold.json) · [Decoder campaign](evidence/p3-cse-tree-rewrite-replay.json) | Port the remaining Java waterline analysis and circuit rewrites that apply field trimming. Repeat exit checks on the finished build, obtain independent maintainer acceptance, and collect 30 qualifying field-shadow days with at least 200 distinct programs per day. |
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

Rust drives the live waterline schedule. A closure that drops its last bound
stops propagation along that branch. An indexed stream tracks bounds on keys
and values separately, so deindex discards a key-only bound. On `46cbf6890`,
24 classic and Gen-2 compilations matched through 11,965 ordered observations
and emission. The build passed 228 Rust tests, 296 nightly optimizer tests
and checkpoint interchange in both directions for COUNT and LATENESS.
[Live propagation](evidence/p3-live-waterline-propagation.json) · [Indexed bounds](evidence/p3-indexed-waterline-propagation.json)

Rust now computes the input fields for UNNEST, linear aggregates, TopK and
grouped aggregates. TopK retains fields used to break ties. Grouped aggregates
share one projection across their components and check the original parameter
bindings before rewriting a reader. The successive builds matched Java on all
373 frozen compilations containing UNNEST, 184 rank and TopK compilations,
and 252 aggregate compilations selected by test name. The grouped-aggregate
build passed 227 Rust tests, 294 nightly Java tests and both directions of
COUNT and LATENESS checkpoint interchange.
[UNNEST](evidence/p3-unnest-reader-paths.json) · [Linear aggregates](evidence/p3-linear-aggregate-reader.json) · [TopK](evidence/p3-topk-indexed-reader.json) · [Grouped aggregates](evidence/p3-stream-aggregate-inputs.json)

Rust also checks copied temporal predicates for shared expressions before
merging filters. A failed merge preserves the names allocated by earlier
successful merges. [Temporal predicate replay](evidence/p3-temporal-predicate-trees.json)

On `c0b527a18`, Rust also narrows joins whose field analysis needs a copied
reader. Both ordinary and ASOF joins preserve the generated-name count and
report a shared variable against its original declarations. ASOF retains both
timestamp columns even if neither contributes to the output. The 273 focused
tests, 290 nightly tests and both checkpoint restores passed.
[Join trimming and restores](evidence/p3-checked-join-readers.json)

On `983217b98`, Rust decides whether to split a map even when field analysis
must inspect a copy of its reader. A shared variable can refer to different
declarations in the original reader. Both compilers diagnose that conflict
only if they rewrite the reader, after allocating the same projection names.
All 272 focused tests and 289 nightly tests passed.
[Map splitting and restores](evidence/p3-inspected-map-splits.json)

On `1af0e4688`, the Rust field-use interpreter produces diagnostics against
the live expression tree. The cold hybrid-to-Java p95 ratios were 0.793× for
the array subquery, 1.049× for Nexmark and 0.799× for the outer join, with 21
measured pairs per workload.
[Field-use diagnostics](evidence/p3-field-use-diagnostics.json) · [Cold timings](evidence/p3-field-use-diagnostics-cold.json)

On `814800520`, Rust handles unexpected source shapes in the optional
`--correlatedColumns` report. A HOP that receives a whole table column or a
null ROW preserves the comparisons reported before the failure and identifies
the offending value. All 17,098 frozen reports matched Java with no fallback.
One case in the Gen-2 manifest explicitly selects classic mode. The 24 selected
compilations matched through 11,965 ordered S4 observations and emission. COUNT
and LATENESS restored in both directions. [Lineage results](evidence/p3-lineage-source-shapes.json)

A projection block can declare a static value, a comparator, a struct, or a
function. On `0b500c23f`, Rust visits those declarations when determining
which input columns to keep. If the whole block is an expression statement,
it retains the reads inside it. It resolves each variable to its declaration,
including when a function parameter shadows the input row. The preceding Rust
build delegated the block-item regression to Java.

On this build, 48 frozen field-trimming compilations matched Java through
30,484 ordered S4 observations. Another 24 selected compilations matched
through 11,965 observations. The two sets overlap on one Gen-2 case. COUNT and
LATENESS checkpoints restored in both directions. The build passed 227 Rust
tests, Clippy, 267 focused JUnit tests, and 284 nightly optimizer tests.
[Block field reads, replay, and restores](evidence/p3-block-item-bindings.json)

Cold compilation through classic artifact emission matched on all 69 pairs,
including warmups. Across 21 measured pairs per workload, hybrid and Java
p95 were 1.937 s and 2.384 s on the array subquery (0.813×), 5.895 s and
5.348 s on Nexmark (1.102×), and 1.767 s and 2.169 s on the outer join
(0.815×). The two compilers used CPU 15 of the shared ARM64 VM.
[Cold compilation pairs](evidence/p3-block-item-bindings-cold.json)

A map can read one column in an expression statement and return a different
column. The optimizer must keep both when narrowing the input. In the
constructed three-column regression, it drops the unused middle column and
rewrites the two surviving reads to positions zero and one. The migration
branch carries the correction in Java and Rust. On upstream main, both IR
regressions failed before the correction and all seven field-use tests passed
after it. On `2c483386e`, all 24 frozen field-trimming and carried-column
cases per engine matched Java, through 15,255 classic and 15,229 Gen-2
observations and emission. The build also passed 227 Rust tests and 273
nightly optimizer tests. The focused upstream PR is assigned to Mihai Budiu.
[Field trimming and checkpoint restores](evidence/p3-discarded-statement-reads.json) ·
[Upstream compiler fix](https://github.com/feldera/feldera/pull/8065)

On upstream PR commit `e482aca35`, Java traces each variable in a discarded
statement through its enclosing field selections, clones, and null tests.
Five of eleven field-use tests failed on the PR base. All eleven passed on the
PR head, along with 106 incremental and redundant-state tests and 51 generated
Rust tests. On migration commit `f2a1f0f1d`, a discarded multiset and a linear
aggregate's empty-set result both retain the columns they read. The constructed
multiset map kept its weight of −3 and the correct fields in both column orders.
The build passed 227 Rust tests, Clippy, 265 focused JUnit tests, and 282
nightly optimizer tests.
[Java and Rust regression results](evidence/p3-variable-statement-reads.json) ·
[Field use, replay, and restores](evidence/p3-statement-expression-results.json)

Each of the three cold compilation workloads had 21 measured pairs. Hybrid
and Java p95 were 1.634 s and 2.076 s for the array subquery (0.787×), 5.110 s
and 4.882 s for Nexmark (1.047×), and 1.532 s and 1.851 s for the outer join
(0.827×). All 69 pairs including warmups matched artifacts and diagnostics.
Each invocation started a fresh compiler process pinned to CPU 15 while other
verification jobs used the same ARM64 VM.
[Paired cold compilation](evidence/p3-statement-expression-results-cold.json)

The upstream follow-up covers aggregate maps through `computeUsedFields`.
Three regressions fail on the PR base, including the aggregate's discarded
read. All eight field-use tests pass on PR commit `6571ea188` and in both
migration modes on `3f6a5cce9`. These are constructed IR cases. In the SQL
pipeline, aggregate expansion, diagnostic lowering and filter-chain
implementation create discarded statements after the field-trimming pass.
[Aggregate regression](https://github.com/feldera/feldera/pull/8065/commits/6571ea188a28257d9f98c45a848fcdf062631ca7)

The revised upstream tests compare the rewritten closures in both column orders.
Five of the eleven field-use tests failed on the PR base. All eleven passed on
`dffd35451`, along with 106 incremental and redundant-state tests. The migration
build `bf8cd8423` keeps a row's null check independent of its column values and
preserves both operands of an explicit tuple selection when copying a closure.
It passed 227 Rust tests, 263 focused JUnit tests, and 280 nightly optimizer
tests. Both engines matched Java through 11,965 observations on the 24 selected
compilations. COUNT and LATENESS restored in both directions. On the three cold
workloads, hybrid/direct p95 was 2.362/2.690 s for the array subquery,
6.807/6.293 s for Nexmark, and 2.121/2.802 s for the outer join. Cancellation
p95 was 15.31 ms with all four workers sharing CPU 14. The full classic runtime replay
passed all 8,256 executable cases across 11,407 input commands, including
232 expected runtime errors.
[Field reads and restores](evidence/p3-evaluated-statement-fields.json) ·
[Paired cold compilation](evidence/p3-evaluated-statement-fields-cold.json)

On `519a01640`, Rust resolves shared reads in the copied reader's scope
and checks the original closure before rewriting its fields. Both binding
regressions failed on the preceding build. All 12 classic and 12 Gen-2
compilations matched through 11,965 ordered S4 observations and emission.
The build passed 227 Rust tests, 259 focused JUnit tests and 276 nightly
optimizer tests. COUNT and LATENESS checkpoints restored in both directions.
Four-worker cancellation p95 was 22.99 ms. Across 21 measured cold pairs per
workload, hybrid/direct p95 ratios were 0.838× for the array subquery, 1.128×
for Nexmark and 0.661× for the outer join.
[Field analysis and restores](evidence/p3-reader-bindings.json) ·
[Cold compilation measurements](evidence/p3-reader-bindings-cold.json)

Full replays on `519a01640`, `9f227a1c5`, and `654b713e7` matched all
8,571 classic and 8,527 Gen-2 cases through 6,996,738 ordered observations
per build. All 8,256 executable classic cases also passed on each of those
builds and on `9fad64252`, including 232 expected runtime errors.
[Reader bindings](evidence/p3-reader-bindings.json) · [Static literals](evidence/p3-static-literal-pool.json) ·
[Temporal diagnostics](evidence/p3-copied-temporal-diagnostics.json) · [Statement field values](evidence/p3-statement-field-values.json)

On `9fad64252`, Rust preserves field reads in discarded returns and
error-handling expressions. The 24 selected compilations matched Java at
all 11,965 ordered S4 observations and through artifact emission. The build
passed 227 Rust tests, 257 focused JUnit tests and 274 nightly optimizer
tests. COUNT and LATENESS checkpoints restored in both directions.
Four-worker cancellation p95 was 13.19 ms.
[Field-use checks and restores](evidence/p3-statement-field-values.json)

Cold compile p95 on the same build was 2.142 s hybrid and 2.732 s Java for
the array subquery (0.784×), 6.696 s and 6.191 s for Nexmark (1.081×),
and 1.948 s and 2.449 s for the outer join (0.795×). Each workload had
21 measured pairs and met the 1.2× ceiling. All 69 pairs, including warmups,
matched emitted artifacts and diagnostics. The compiler processes ran on CPU
15 while verification used other CPUs on the same ARM64 VM.
[Cold compilation measurements](evidence/p3-statement-field-values-cold.json)

On `6aea8eccb`, Rust reports a missing required argument or an unbounded
nonconstant argument when deriving a function's waterline. On `499d7fb7d`,
temporal predicates report the failing argument or target-sized literal in
the same visit, preserving generated variable names. Each build matched
Java on 12 classic and 12 Gen-2 compilations through 11,965 ordered S4
observations and emission. COUNT and LATENESS checkpoints restored in both
directions. The nightly runs produced complete reports for 265 and 266 tests.
[Function argument diagnostics](evidence/p3-waterline-call-arguments.json) ·
[Temporal predicate diagnostics](evidence/p3-temporal-call-diagnostics.json)

On `95d673248`, Rust rebinds each projected reader field using its original
tuple path, including when two fields of the same tuple type retain different
columns. The regression checks closure parameter identity and matches Java at
depths one, two and three. The previous build delegated depth two to Java.
All 24 selected compilations matched through 11,965 ordered S4 observations
and emission. The devcontainer passed 226 Rust tests, 247 focused JUnit tests
and 264 nightly JUnit tests. COUNT and LATENESS restored in both directions.
On `a11850b57`, the same checks passed with the original one-level reader
API and a separate JNI request for nested layouts. Four-worker cancellation
p95 was 10.12 ms on that build.
[Nested reader comparisons](evidence/p3-nested-reader-layouts.json) ·
[Reader protocol and checkpoint restores](evidence/p3-indexed-reader-protocol.json)

On `fccd64201`, Rust checks the declared output type after reducing a MIN
accumulator and preserves an empty tuple in the first compared slot of a
chained MIN. Both regressions failed against the preceding build. The new
build passed 246 focused and 263 nightly optimizer tests. All 24 selected
compilations matched Java through 11,965 ordered S4 observations and
emission. A further replay of 48 MIN cases per engine matched at 23,400
classic and 23,496 Gen-2 observations. COUNT and LATENESS checkpoints
restored in both directions.
[MIN output shapes](evidence/p3-minimum-output-shapes.json)

On `07c9714ea`, Rust checks discarded statements and nested variable bindings
before trimming a map input. A discarded loop needs no symbolic value. A
reader that Rust cannot interpret resumes the live field-use visitor at that
reader, preserving the original diagnostic. On `9f227a1c5`, Rust selects
the first reusable static literal by SQL type and value, including string
nullability and decimal scale. Both builds matched Java across 12 classic
and 12 Gen-2 compilations, all 11,965 ordered S4 observations and emission.
The nightly suites passed 259 and 261 tests, respectively. COUNT and LATENESS
checkpoints restored in both directions on each build.
[Map reader checks](evidence/p3-checked-map-readers.json) · [Static literal comparisons](evidence/p3-static-literal-pool.json)

On `0cd6272af`, column lineage checks the physical output width before
publishing a join's correlations. On `654b713e7`, Rust reports a failed
temporal merge using the expression or type it copied while rewriting the
predicate. Both builds matched Java on 24 selected compilations and 11,965
ordered S4 observations. The nightly optimizer suites passed 256 and 257
tests, respectively. On the latter build, COUNT and LATENESS checkpoints
restored in both directions without bootstrap. Worker cancellation p95 was
12.10 ms across 20 attempts. All 8,527 frozen Gen-2 cases matched at
3,510,660 ordered S4 observations, and all 8,256 executable classic cases
passed 11,407 input commands, including 232 expected runtime errors. The
highest cold p95 ratio was 1.177× on Nexmark.
[Cold measurements](evidence/p3-copied-temporal-diagnostics-cold.json)
[Physical output width](evidence/p3-lineage-output-width.json) · [Temporal diagnostics and restores](evidence/p3-copied-temporal-diagnostics.json)

Rust checks chained MIN updates before pruning an input without a waterline.
Column lineage preserves earlier findings when a later operator reads a missing
port. Map projection analysis resolves bindings before classifying the expression
and checks the number of mapped fields against the result type. On the three
builds, 253, 254 and 255 nightly optimizer tests passed, respectively. Each
matched Java on all 11,965 ordered S4 observations and emitted artifacts for
12 classic and 12 Gen-2 cases.
[Chained MIN](evidence/p3-chain-minimum-diagnostics.json) · [Missing lineage input](evidence/p3-missing-lineage-input.json) · [Map projection roots](evidence/p3-map-projection-roots.json)

On `02de0ae99`, the Rust scheduler checks a rolling window's upper frame
before pruning an input without a waterline. A NULL following offset now
reports the same numeric-literal error as Java. A preceding frame ignores
the offset. All 252 nightly optimizer tests passed, and all 24 selected
compilations matched emission and 11,965 ordered S4 observations.
[Rolling frame replay](evidence/p3-rolling-offset-diagnostic.json)

Rust preserves completed temporal-filter merges when a later predicate fails.
It checks nullable raw tuple field types before using their bounds and uses
the failing predicate's live types to report the diagnostic. The three builds
passed 248, 250 and 251 nightly optimizer tests, respectively. On `b533aad10`
all 67 frozen programs that exercised temporal-filter merging matched Java's
emission and all 41,148 ordered S4 observations. COUNT and LATENESS
checkpoints restored in both directions, and worker cancellation p95 was
15.35 ms over 20 attempts. Cold hybrid/direct compile p95 ratios were 0.886
for the array subquery, 1.029 for Nexmark and 0.865 for the outer join over
21 measured pairs per workload.
[Temporal merge diagnostics](evidence/p3-temporal-merge-diagnostics.json) · [Raw tuple bounds](evidence/p3-raw-tuple-waterlines.json) · [Failed predicate replay and restores](evidence/p3-failed-predicate-projection.json) · [Cold compilation](evidence/p3-failed-predicate-projection-cold.json)

The Rust waterline visitor now checks registered call result types, reads a
nullable unit field without requiring its parent bound, and checks scalar
operands in visitor order. The changes passed 242, 245 and 247 nightly optimizer
tests on `d52a86fc3`, `3f0cd4b20` and `f6b1a1b0c`, respectively. Each build
matched emission and all 11,965 ordered S4 observations for the same 12 classic
and 12 Gen-2 programs. The directed regressions failed on their predecessors.
[Call result types](evidence/p3-call-waterline-types.json) · [Nullable fields](evidence/p3-nullable-read-projection.json) · [Scalar transfers](evidence/p3-scalar-waterline-transfers.json)

On `af439243c`, shared-index planning and map fusion reload both operands
together when a comparison needs shuffle identity or JDK timestamp metadata.
Rust decides their equality and preserves the diagnostic and allocator state
on failure. Replacing the requested operands with two unrelated flatmaps broke
both directed tests. The unchanged build passed 226 Rust tests, 214 focused
JUnit tests and 239 nightly optimizer tests. All 24 selected compilations matched
through 11,965 ordered S4 observations and emission. COUNT and LATENESS
checkpoints restored in both directions without bootstrap. Worker cancellation
p95 was 12.71 ms across 20 attempts. Cold hybrid/direct compile p95 ratios
were 0.831 for the array subquery, 1.047 for Nexmark and 0.823 for the outer
join across 21 measured pairs per workload. All 445 frozen programs that
exercised shared-index replacement (202 classic and 243 Gen-2) matched
emission and all 211,706 ordered S4 observations.
[Live operand comparisons and checkpoint restores](evidence/p3-live-operand-snapshots.json) · [Cold compilation](evidence/p3-live-operand-snapshots-cold.json)

An `UNNEST` input rewrite or deep copy retains its original shuffle. Output
projection creates a distinct shuffle even when the indexes match. Map fusion
publishes completed clusters before reporting a singleton comparison error.
A read of an unbounded field succeeds even when its tuple has a different
monotone field whose bound is missing. The three builds passed 235, 236 and
237 nightly optimizer tests, respectively. Each matched 11,965 ordered S4
observations and emission across 12 classic and 12 Gen-2 programs.
[UNNEST shuffles](evidence/p3-live-flatmap-shuffles.json) · [Map fusion diagnostics](evidence/p3-singleton-fusion-diagnostics.json) · [Selected field bounds](evidence/p3-selected-read-bounds.json)

On `72fdf97df`, lineage retains the comparisons it completes before a
diagnostic. On `7f6b247ff`, candidate-key analysis preserves the declaration
scope of a parameterless closure and the order of errors in computed columns.
On `757d64cec`, binary waterline analysis distinguishes a missing input fact
from a known input whose bound is absent. Each build passed 226 Rust tests
and matched all 11,965 ordered S4 observations and emission across 12 classic
and 12 Gen-2 programs. The nightly suites passed 230, 232 and 234 tests,
respectively.
[Lineage diagnostics](evidence/p3-lineage-diagnostics.json) · [Key comparison scope](evidence/p3-key-comparison-scope.json) · [Missing waterline bounds](evidence/p3-missing-binary-waterlines.json)

On `757d64cec`, COUNT and LATENESS checkpoints restored in both directions
without bootstrap. Worker cancellation p95 was 7.16 ms across 20 attempts.
The cold hybrid/direct compile p95 ratios were 0.859 for the array subquery,
1.084 for Nexmark and 0.787 for the outer join, with 21 measured pairs per
workload on one pinned CPU in the shared VM.
[Cold compilation measurements](evidence/p3-missing-binary-waterlines-cold.json)

Rust reports missing declarations during column comparison, retaining the
preceding warnings and generated names. A shared index checks the incoming
column first. Map fusion checks the existing column first. Directed cases
verified both orders against Java. The build passed 181 targeted and 221
nightly JUnit tests, and matched Java through 11,965 ordered S4 observations
and emission on 24 classic and Gen-2 compilations.
[Column lookup diagnostics](evidence/p3-column-lookup-diagnostics.json)

Rust selects shared indexes, tries their column layouts, and rewrites their
readers. When a comparison needs additional operand metadata, the JVM supplies
the expressions and Rust decides their equality. A failing
comparison retains the warnings and generated names preceding it. Waterline
analysis rejects nullable references at the original read and reports an
unbound variable, a conflicting declaration, or a repeated expression before
publishing any closure facts. The closure-validation build passed 225 Rust
tests, 177 targeted JUnit tests, and all 217 nightly optimizer tests. All 445
frozen programs exercising shared-index replacement matched Java through
211,706 ordered S4 observations and emission.
[Shared-index and nullable-reference tests](evidence/p3-index-and-waterline-diagnostics.json) · [Closure validation](evidence/p3-waterline-closure-validation.json)

Rust counts the closures generated by nested `UNNEST` each time it classifies
a temporal predicate. It preserves the missing-variable diagnostic when an
outer read follows the nested closure and retains shuffle identity when copying
a flatmap. Three regression tests failed on the previous build. The corrected
build passed 179 targeted JUnit tests and matched Java through 11,965 ordered
S4 observations and emission on 24 classic and Gen-2 compilations.
[Nested flatmaps in temporal predicates](evidence/p3-nested-flatmap-window-order.json)

Waterline analysis builds a nested flatmap's synthetic closure and records
the same fact for the closure and the flatmap. If a later reference fails,
its diagnostic retains the facts and generated names from the preceding
expressions. This build passed 180 targeted and 220 nightly JUnit tests.
All 418,474 instrumented decisions ran in Rust across the 17,098 frozen
programs. A separate 24-program replay matched Java through 11,965 ordered
S4 observations and emission.
[Nested flatmaps in waterline closures](evidence/p3-nested-flatmap-closure-facts.json)

Rust validates each nested flatmap's input kind and projected result type
when it visits that expression. A failure preserves the facts and fresh names
allocated earlier in the same closure. On `ad747bd41`, 187 focused and 227
nightly JUnit tests passed, and 24 strict replays matched all 11,965 ordered
S4 observations and emission.
[Nested flatmap validation](evidence/p3-flatmap-validation.json)

When a temporal filter reads an uninitialized local, Rust reports the ID of
the expression whose waterline fact is missing. A read after a nested flatmap
can instead lose its lexical reference, in which case the diagnostic names
the variable. Both failures preserve the number of names allocated by earlier
merges. Directed tests verified the exception class and message against Java.
On `ad1c02edd`, all 520 frozen programs that exercised temporal filter merging,
map-fusion column comparison or shared-index replacement matched Java through
255,346 ordered S4 observations and emission.
[Temporal clock lookups](evidence/p3-temporal-clock-lookups.json)

Map fusion and temporal bounds compare zoned timestamp literals in operand
order. If conversion fails, Rust identifies the offending literal and Java
renders the JDK diagnostic. Directed tests exercised invalid offsets on both
sides. The build passed 182 targeted and 222 nightly JUnit tests and matched
Java on 12 classic and 12 Gen-2 programs.
[Timestamp comparison tests](evidence/p3-timestamp-comparison-errors.json)

The same diagnostic now survives comparison inside a weighted constant
collection, including an array-valued row. Before the fix, the JNI boundary
reported a generic optimizer error. The regression checks the exception class
and message against Java in both operand orders. A separate Rust test checks
that an older snapshot without a resolved instant can decline the comparison.
The build passed 186 focused and 226 nightly JUnit tests and matched all
11,965 ordered S4 observations in the 24-case replay.
[Constant collection comparison results](evidence/p3-collection-zone-errors.json)

Shared-index discovery checks bindings as it visits each eligible index.
A failed binding leaves the index unvisited. A failure during key comparison
retains the seed visit. Earlier groups, warnings and generated names survive
both errors. The candidate passed 183 targeted and 223 nightly JUnit tests
and matched Java on 24 classic and Gen-2 compilations.
[Shared-index discovery tests](evidence/p3-shared-index-discovery-prefix.json)

Fixed-width trials, final column layout and reader replacement now compare
each field as soon as they project it. All three entry points returned the
same first failing column, warnings and generated-name count as direct Java.
The previous standalone trial returned without reporting the error.
[Shared-index field trials](evidence/p3-shared-index-field-trials.json)

Optimizer snapshots carry loop variable names through field reduction. Two
loop-valued columns compare their inputs first. Different inputs occupy
different columns, while equal inputs reach Java's empty-scope binding error.
The build passed 184 targeted and 224 nightly JUnit tests. COUNT and LATENESS
checkpoints restored in both directions, and cancellation p95 was 19.88 ms
across 20 workers.
[Loop comparisons, restore and cancellation checks](evidence/p3-index-loop-bindings.json)

When a variable read has the wrong type, Rust records the declaration's
waterline fact before reporting the error. Both waterline request modes
matched Java's observations and diagnostic for local and parameter reads.
The build passed 185 targeted and 225 nightly JUnit tests and matched Java
at all 11,965 ordered S4 observations across 12 classic and 12 Gen-2 cases.
[Variable read regression and replay](evidence/p3-waterline-read-types.json)

Shared-index readers remap a column beneath 160 nested calls. Indexes that
project the same nested expression still share one output column. Both
operations exceeded the inline snapshot limit on the preceding build.
The corrected build passed 174 JUnit tests and matched 11,965 ordered S4
observations and emission on 12 classic and 12 Gen-2 cases.
[Shared-index readers and projections](evidence/p3-deep-shared-index-functions.json)

Rust merges adjacent temporal filters in producer order. When it needs the
general analyzer for one predicate pair, it passes the current closures,
queued warnings and generated-name counter to Java. Two directed cases
verified continued merging and the error from an uninitialized clock.
The preceding compiler restarted the Java graph traversal in both cases.
The temporal-classification corpus matched on 71 classic and 62 Gen-2
compilations through 72,422 ordered S4 observations and emission.
On the same build, Rust answered all 418,474 instrumented requests across
the 17,098 frozen compilations. COUNT and LATENESS checkpoints restored in
both directions. Four-worker cancellation p95 was 13.90 ms. Nexmark had
the largest cold compilation ratio at 1.112× Java p95.
[Temporal merge tests, coverage and restores](evidence/p3-live-temporal-merge.json) · [Cold compilation](BENCHMARKS.md#temporal-filter-merging-2026-11-23)

Temporal classification and grouping preserve the order and identity of all
192 leaves in a deep conjunction. Both operations exceeded the inline
snapshot limit on the preceding build. The corrected build passed 169 JUnit
tests and matched direct Java on 20 selected compilations through 8,778
ordered S4 observations and emission. [Temporal conjunction replay](evidence/p3-temporal-conjunctions.json)

Waterline analysis reports the first unsupported nested field before examining
the arguments of a registered function. Both visitor modes retain facts from
earlier expressions. The preceding build failed the directed test. The corrected
build passed 225 Rust tests and 167 JUnit tests. Twelve classic and twelve
Gen-2 compilations matched Java through 10,929 ordered S4 observations and
emission. [Waterline diagnostic ordering](evidence/p3-unsupported-waterline-types.json)

Map fusion imported a newly simplified chain of 96 function calls and shared
the resulting column on `aeb1a517b`. The preceding compiler failed while
decoding that closure. All 165 targeted JUnit tests passed on the corrected
build. [Map-fusion snapshot and replay](evidence/p3-map-fusion-closure-snapshots.json)

Rust matched Java when weighted constant arrays contained SQL tuples,
raw tuples and points. A repeated computed row replaced the stored weight,
and a repeated constant row added its weight. Projection preserved the first
key and insertion position on replacement. The selected 12 classic and eight
Gen-2 cases matched through 8,778 ordered S4 observations on each build.
[Collection elements](evidence/p3-collection-element-equality.json) · [Row weights and projection](evidence/p3-computed-row-weights.json)

On `091f41bcb`, a 144-level weighted constant row matched the Java window-key
decision. The preceding build failed at its 128-level comparison limit. All
223 Rust tests and 161 targeted JUnit tests passed. Twelve classic and eight
Gen-2 compilations matched through 8,778 ordered S4 observations and emission.
Across 20 cancellations of four concurrent workers, signal-to-reap p95 was
12.68 ms and peak driver plus front-end RSS was 67.48 MiB.
[Build identities, regressions, replay and cancellation](evidence/p3-nested-constant-rows.json)

On `0efc230dc`, Rust selected the first incompatible field when combining
waterlines for unions, intersections, joins and anti-joins. Java rendered the
same exception from the live operands. The regression verified that the
hybrid skipped fields whose comparisons had already succeeded. All 222 Rust
tests and 160 targeted JUnit tests passed, and 24 detached compilations
matched through 10,929 ordered S4 observations and emission.
[Waterline diagnostics and replay](evidence/p3-bound-combination-errors.json)

On `001cd0637`, the Rust column-lineage report resolved reads by declaration
before visiting the closure body. A free read or a shared read with conflicting
bindings produced the same diagnostic and preceding findings as direct Java.
The complete reports matched on all 8,571 classic and 8,527 Gen-2 population
entries, including one entry that selects classic mode in the Gen-2 export.
Rust produced every report. All 209 Rust tests and 159 targeted JUnit tests
passed. The 24 detached replay cases matched through 10,929 ordered S4
observations and artifact emission. [Lineage reports and replay](evidence/p3-lineage-declarations.json)

Rust groups temporal filters by the printed contents of comments and static
collection initializers in their window keys. The directed cases cover
trailing newlines, a changed map value and the source range of an earlier
cast warning. All 221 Rust tests and 154 targeted JUnit tests passed. The
20 selected classic and Gen-2 compilations matched direct Java at all 8,778
ordered S4 observations and through emission.
[Window key replay](evidence/p3-static-collection-window-keys.json)

Rust now identifies the operand whose waterline is missing and the literal
whose target type has no maximum. Both visitor modes publish the facts
computed up to the failure so Java can report the diagnostic on the original
expression. The build passed 221 Rust tests and 152 targeted JUnit tests.
Six JUnit cases failed against the preceding library. All 24 selected
compilations matched direct Java through 10,929 ordered S4 observations and
emission. [Waterline failures](evidence/p3-waterline-failures.json)

Rust compares printed window keys for static blocks containing calls and
expression statements. The directed scan preserved the preceding cast warning
at its original range. An initializer containing a local declaration produced
the same canonicalization error as direct Java. The rebuilt library passed
221 Rust tests, 151 targeted JUnit tests and 20 compilations comparing 8,778
ordered S4 observations and emitted artifacts.
[Static window keys](evidence/p3-static-window-blocks.json)

The waterline visitor checks a parameter's original type when it visits a read.
It resolves names inside nested lambdas, whose bodies have no waterline facts.
The regression checks both observation modes, a temporal predicate and the
diagnostic on a visited read. It failed on the previous library. All 221 Rust
tests and 150 targeted JUnit tests passed. The 24 LATENESS compilations
matched through 10,929 ordered S4 observations and emission.
[Parameter read parity](evidence/p3-visited-parameter-types.json)

The detached timestamp comparator now handles the JDK's System V daylight
rules and compact GMT offsets. All 198 directed zone/date pairs agreed with
Java, and 58 frozen compilations matched through 23,242 ordered S4 observations
and emission. A wider check of all 632 JVM zone IDs on six dates found six
differences between the JVM and Rust time-zone databases, in northern Mexico
in 1974 and Morocco in 2040. The live optimizer now receives the timestamp in
microseconds resolved by the producing JVM along with its original spelling.
All 3,792 zone/date comparisons then agreed. The new build passed 221 Rust
tests, 149 targeted JUnit tests and the same 58 frozen compilations.
[Detached zoned keys](evidence/p3-jdk-zoned-keys.json) · [Live instant comparison](evidence/p3-zoned-runtime-instants.json)

On `6ef296ec7`, cold SQL-to-classic emission met the 1.2× p95 ceiling on
the array subquery (0.806×), Nexmark (1.145×) and the outer join (0.826×).
All 69 alternating pairs matched on diagnostics and emitted artifacts.
[Paired compilation measurements](evidence/p3-zoned-runtime-instants-cold.json)

Rust now reports the first missing declaration when grouping or merging
temporal bounds. A regression compares the exception class and variable name
with Java when both operands are unbound and when only the right operand is
unbound. The previous hybrid silently rejected the merge. The fix passed 221
Rust tests and 147 targeted JUnit tests. All 20 directed compilations matched
through 8,778 ordered S4 observations and artifact emission.
[Temporal key diagnostics](evidence/p3-temporal-key-diagnostics.json)

Rust visits the children of linear, nonlinear and min/max aggregate descriptors
without assigning the descriptor a waterline. The previous hybrid failed the
regression when a closure discarded the descriptor. The fix passed 221 Rust
tests and 146 targeted JUnit tests. Twelve LATENESS programs per engine matched
direct Java through 10,929 ordered S4 observations and artifact emission.
[Aggregate descriptor replay](evidence/p3-aggregate-descriptor-waterlines.json)

Temporal filters now use the same Rust waterline transfer as LATENESS for
scalar and structured clock operands. All 71 classic and 62 Gen-2 programs
in the frozen clock population matched direct Java, as did 20 directed
compilations. Together they compared 81,200 ordered S4 observations and
the emitted artifacts. Across all 17,098 frozen compilations, Rust answered
all 418,474 instrumented requests. The largest cold p95 ratio was 1.115×
on Nexmark. [Temporal waterline replay and coverage](evidence/p3-temporal-waterline-analysis.json) ·
[Paired measurements](evidence/p3-temporal-waterline-analysis-cold.json)

The hybrid accepts an `INDICATOR` whose operand has no materialized waterline
and preserves the direct visitor's diagnostic when `TYPEDBOX` needs that
operand. Both native closure observation modes passed. The build passed
221 Rust tests and 145 targeted JUnit tests. Twelve programs per engine
matched through 10,929 ordered S4 observations and emission.
[Unary operand waterlines](evidence/p3-unary-operand-waterlines.json)

Row equality over an empty tuple now agrees with direct Java when the tuple
has no materialized waterline. Rust reads only the bounds required by the
binary operation. The regression failed on the previous hybrid compiler.
All 221 Rust tests and 144 targeted JUnit tests passed, and 12 LATENESS
programs per engine matched through 10,929 ordered S4 observations and
emission. [Binary operand waterlines](evidence/p3-binary-operand-waterlines.json)

Rust matches Java's six map-fusion groups for eleven empty tuples, factory
calls, unit values and string literals. All 143 targeted JUnit tests and
220 Rust tests passed. Twelve classic and Gen-2 compilations matched through
4,774 ordered S4 observations and emission. [Fusion comparisons](evidence/p3-printed-empty-values.json)

The hybrid visitor now reports the same error as direct Java when a registered
scalar function reads an uninitialized local. Rust also projects calls to
unregistered functions when an argument has no waterline fact. Both visitor
modes passed the regression. All 142 targeted JUnit tests and 24 detached
LATENESS compilations passed. [Function arguments](evidence/p3-call-argument-waterlines.json)

Rust groups map fragments by their printed function, stream and generic type
arguments. Seventeen call signatures form the same eleven groups as direct Java,
including nullable references printed as `Option<&T>`. All 141 targeted JUnit
tests passed, and twelve detached compilations matched through 4,774 ordered
S4 observations and emission. [Printed type arguments](evidence/p3-printed-type-keys.json)

Rust omits the fact for an uninitialized assignment destination in both waterline
visitor modes. A consumer that requires that local's value still gets the direct
Java diagnostic. All 220 Rust tests and 140 targeted JUnit tests passed.
Twelve frozen LATENESS programs per engine matched direct Java through 10,929
ordered S4 observations and emission. [Waterline projection](evidence/p3-discarded-local-waterlines.json)

Rust rewrites common subexpressions into closure-local lazy cells and returns
the rewritten tree and the next generated name to the compiler. It performed
4,444 rewrites across 47 classic and 47 Gen-2 compilations. All 46,351 ordered
S4 observations and emitted artifacts matched direct Java. The build passed
207 Rust tests and 108 targeted JUnit tests. The largest cold p95 ratio was
1.121× on Nexmark (4.297 s hybrid and 3.833 s direct).
[Expression rewrite replay](evidence/p3-cse-tree-rewrite.json) ·
[Paired compilation timings](evidence/p3-cse-tree-rewrite-cold.json)

Rust chooses the first read of each closure-local lazy cell, whether a read
borrows its value, and where each binding becomes visible. The direct and
hybrid compilers used the same generated names for a method called across
nested scopes. All 105 targeted JUnit tests passed. The 94 detached
compilations matched through 46,351 ordered S4 observations and emission.
Nexmark had the largest cold p95 ratio at 1.047× (5.155 s hybrid, 4.923 s
direct). [Binding replay](evidence/p3-cse-binding-plan.json) ·
[Paired compilation timings](evidence/p3-cse-binding-plan-cold.json)

Rust walks the original expression graph to number values, track mutable
reads and identify shared subexpressions within each closure. Java supplies
the printer spellings used in the value keys. Rust handled 4,444 numbering
requests across the 94 detached compilations. All 46,351 ordered S4
observations and emitted artifacts matched direct Java. The largest cold
p95 ratio was 1.115× on Nexmark (4.491 s hybrid, 4.027 s direct).
[Graph numbering replay](evidence/p3-graph-value-numbers.json) ·
[Paired compilation timings](evidence/p3-graph-value-numbers-cold.json)

Rust resolves variable references for optimizer visitors that accept free
variables and for those that require closed expressions. Both preserve the
bindings established before a diagnostic. All 104 targeted JUnit tests
passed, and 94 detached compilations matched Java through 46,351 ordered S4
observations and emission. Nexmark had the largest cold p95 ratio at 1.121×.
[Reference resolution replay](evidence/p3-general-resolver.json) ·
[Paired compilation timings](evidence/p3-general-resolver-cold.json)

When an optimizer pass needs an expression tree, Rust copies each shared
subtree and gives each nested closure its own parameters. The outer closure
keeps its original parameter objects. The identity checks passed in the
103-test JUnit run. All 94 detached compilations matched direct Java through
46,351 ordered S4 observations and emission.
[Live expression-tree replay](evidence/p3-live-tree-conversion.json)

Rust resolves CSE variable reads and mutable borrows before assigning value
numbers. Free reads and conflicting declarations retain their Java diagnostics.
All 103 targeted JUnit tests passed. Across 94 detached compilations, Rust
handled 3,812 numbering requests and matched all 46,351 S4 observations and
emitted artifacts. The largest cold p95 ratio was 1.177× on Nexmark (4.892 s
hybrid, 4.156 s direct). [Replay](evidence/p3-cse-resolution.json) ·
[Paired compilation timings](evidence/p3-cse-resolution-cold.json)

When a waterline closure needs the general visitor, Rust returns the variable
bindings and shared-expression check from its first traversal. A binding
error takes precedence over a shared-expression diagnostic, as in direct
Java. All 102 targeted JUnit tests passed. The 47 classic and 47 Gen-2
compilations matched through 46,351 ordered S4 observations and emission.
[Waterline fallback replay](evidence/p3-closure-inspection.json)

Rust checks waterline closure bodies for shared expressions when it resolves
their variables. Both visitor entry points report the same first repeated
node and enclosing expressions as direct Java. The 102 targeted JUnit tests
passed. All 94 detached compilations matched through 46,351 ordered S4
observations and artifact emission.
[Closure inspection replay](evidence/p3-waterline-tree.json)

Rust resolves variable reads and LET binders for the general waterline visitor.
An initializer sees the outer declaration, including when a shared read would
conflict with the subsequent binder. All 550 isolated comparisons and 94
detached compilations matched Java through 46,351 ordered S4 observations.
COUNT and LATENESS restored in both directions.
[Binding diagnostics, replay and restores](evidence/p3-lexical-waterlines.json)

Rust compares block initializers and loop inputs in lexical order when
matching index keys. Temporal window keys also preserve the message in
`.expect(message)` and group runtime error handlers with their printed
calls. On each build, 60 detached compilations matched direct Java through
36,794 ordered S4 observations and artifact emission.
[Lexical key comparisons](evidence/p3-block-key-order.json) ·
[Checked window keys](evidence/p3-checked-window-keys.json)

Rust projects clones, `Some` and unwraps in both waterline visitors. The
general visitor exposes the original bound for `Some(())` even though the
nullable unit contributes no bound to its parent row. All 550 isolated
comparisons and 94 detached compilations matched direct Java, including
46,351 ordered S4 observations and emitted artifacts. COUNT and LATENESS
checkpoints restored in both directions.
[Wrapper comparisons and restores](evidence/p3-value-wrappers.json)

Rust selects the last profitable integrator for each left-join chain source
port. A later unprofitable integrator leaves the earlier choice in place.
Direct Java and the hybrid selected the same 20 rewrites across both engines,
including the lookup key order and deferred columns. The 18 detached
compilations matched through 14,254 ordered S4 observations and emission.
[Endpoint and lookup comparisons](evidence/p3-chain-endpoints.json)

Rust groups temporal filters and lowered window indexes by source port and
printed input row type. Distinct signatures can share a port. The Rust test
covers nested row signatures with fixed and variable binary fields. All 60
detached temporal compilations matched Java through 36,794 ordered S4
observations and artifact emission.
[Window input type comparisons](evidence/p3-window-input-types.json)

The general waterline visitor and whole-closure analysis use the same Rust
rules for field reads, borrows and dereferences. All 550 isolated comparisons
and 94 detached compilations matched Java, including 46,351 ordered S4
observations. COUNT and LATENESS checkpoints restored in both directions.
Both readers dropped late input after restoring the watermark.
[Field and reference comparisons](evidence/p3-structural-reads.json)

The general monotonicity visitor uses Rust's whole-closure cast rule to build
the reduced expression. The direct and hybrid visitors agree on identity,
widening, saturation and safe narrowing. All 94 detached compilations
matched Java at 46,351 ordered S4 observations and through emission.
[Cast waterline comparisons](evidence/p3-native-cast.json)

The Rust window scan reports cast warnings at their SQL source ranges. When
a later filter requires replay, the earlier warning appears once. The native
scan also groups a scalar conversion with an ordinary call of the same name
and operands. On each build, 60 temporal programs matched direct Java at all
36,794 ordered S4 observations and through emission.
[Warning replay](evidence/p3-temporal-warning.json) · [Conversion keys](evidence/p3-conversion-window-keys.json)

Rust selects lookup keys and deferred payload columns for left-join chains.
Across both engines, the 20 profitable rewrites in the frozen corpus matched
Java's key selection, physical index order and payload layout. All 14,254
ordered S4 observations and emitted artifacts matched on those compilations.
[Left-join lookup comparisons](evidence/p3-left-join-lookup.json)

Rust builds waterline closures for row, indexed and join inputs in the general
monotonicity visitor. The 94 detached compilations matched Java at all 46,351
ordered S4 observations and on emitted artifacts. All 550 isolated pass
comparisons matched. COUNT and LATENESS checkpoints restored in both directions.
[Closure comparisons and restores](evidence/p3-general-waterline-closures.json)

Rust computes literal waterlines in the general visitor and during closure
projection. A NULL double uses the type maximum, NaN, as its waterline
and cannot serve as a constant in subtraction. Multiplication distinguishes a positive factor
from NaN. NULL target-sized integers retain the compiler's diagnostic. The
packaged build passed 93 JUnit tests and matched Java on all 550 selected
MonotoneAnalyzer invocations and 9,014 ordered S4 observations.
[Literal comparisons](evidence/p3-literal-waterlines.json)

The general monotonicity visitor uses the same Rust clock and tuple
constructors as whole-closure projection. A directed test checks that
a projected SQL row retains its source range and the identities of its
bounded fields, including an empty raw tuple and a call to the clock.
The packaged build passed 92 JUnit tests, 550 isolated monotonicity
comparisons and 22 detached compilations.
[Clock and tuple comparisons](evidence/p3-tuple-clock-waterlines.json)

Rust builds scalar function waterlines for both whole closures and the general
monotonicity visitor. A directed test checks the runtime callee, result
nullability, reuse of the reduced argument and copying of the constant.
Across 11 selected programs per engine, 9,014 ordered S4 observations and
all emitted artifacts matched Java. All 550 isolated MonotoneAnalyzer
comparisons matched. The preceding mutable-borrow CSE port also matched
Java in 550 isolated comparisons.
[Scalar waterline and mutable-borrow checks](evidence/p3-scalar-function-bound.json)

On `17681d65a`, Rust constructed 184 lazy-cell reads in 84 detached
compilations. All 42,215 ordered S4 observations and emitted artifacts
matched direct Java. Both engines also matched in 550 isolated InnerCSE
comparisons. COUNT and LATENESS checkpoints restored in both directions.
[CSE replay and restores](evidence/p3-lazy-cse-reads.json) · [Cold compilation](BENCHMARKS.md#lazy-cse-reads-2026-11-22)

The Rust optimizer groups temporal windows over calls to external SQL
functions. It compares the method name, receiver and ordered arguments.
On classic and Gen-2, two calls to the same function shared a window index
and the call to a different function used its own. Both compilations matched
Java through all 1,396 ordered S4 observations and artifact emission.
[Window grouping regression](evidence/p3-external-function-window-keys.json)

On `72c33626c`, Rust constructed common-subexpression keys and classified
their costs. The 84 selected detached compilations matched Java at 42,215
ordered S4 observations. Rust answered all 3,746 value-numbering requests.
All 550 separate InnerCSE pass comparisons matched, and COUNT and LATENESS
restored checkpoints in both directions.
[Value-numbering comparisons](evidence/p3-typed-cse-keys.json)

Rust groups windows whose timestamp expression contains a typed NULL interval
or a lowered array or map conversion. The two directed interval programs on
each engine matched direct Java through 2,416 ordered S4 observations. With
cast expansion moved before window grouping in the pass test, safe and unsafe
conversions used distinct indexes. All 23 temporal tests passed. Both new
regressions required Java at `WindowGroups/Scan` on the preceding library.
[Window grouping comparisons](evidence/p3-expanded-window-keys.json)

On `4324adf5a`, Rust selects waterline transfers from filter and view stream
types, primitive aggregate functions and ASOF comparators. All 550 selected
monotonicity analyses matched Java. Across 29 classic and 30 Gen-2 detached
compilations, all 31,615 ordered S4 observations matched and Rust answered
all 14,832 instrumented optimizer requests. COUNT and LATENESS restored
checkpoints in both directions. All 8,256 executable classic cases passed,
including 232 expected runtime errors. In a development run with concurrent
verification, the largest cold compile p95 ratio was 1.069× on Nexmark.
[Selected replay and restores](evidence/p3-typed-transfer-inputs.json) ·
[Cold compile measurements](evidence/p3-typed-transfer-inputs-cold.json)

On `32b5c673e`, the Rust waterline projection drops the nullable COUNT
indicator's bound and preserves the neighboring field. Its regression test
failed on the preceding JNI library. The 18 detached COUNT compilations
matched Java at 10,302 ordered S4 observations.
[COUNT replay and test results](evidence/p3-indicator-waterlines.json)

Rust groups shared windows whose timestamp comes from a nullable array or map
lookup. In both engine modes, two equivalent lookups shared an index and a
different subscript used its own index. All four detached compilations matched
Java through 2,410 ordered S4 observations and artifact emission. The previous
library delegated ten window-analysis requests to Java on these programs.
[Nullable collection replay](evidence/p3-nullable-collection-keys.json)

The hybrid compiler projected a field through 144 nested rows and retained
the same field identity and closure as Java. The previous native library
failed to decode the input projection. Across 17,098 frozen compilations,
Rust answered all 337,044 instrumented requests. The 45 selected detached
replays matched all 23,909 ordered S4 observations and the emitted artifacts.
[Projection and replay results](evidence/p3-deep-input-waterlines.json)

Rust grouped temporal windows whose timestamps depend on interval casts or
point distances. It distinguished days from hours, months from years and
points with reversed coordinates. Eight classic and Gen-2 compilations
matched Java through 5,202 ordered S4 observations and artifact emission.
Rust answered all 1,098 instrumented requests. The previous library
delegated 14 of these requests to Java.
[Interval and spatial window comparisons](evidence/p3-interval-spatial-window-keys.json)

The nightly optimizer replay now rejects a Java fallback on any instrumented
request. On the scoped-lambda build, 18 classic and 19 Gen-2 frozen programs
covered all 28 previously measured operator and transfer-rule combinations on
each engine. All 10,564 requests had Rust answers, and all 18,707 ordered S4
observations matched. A separate SQL case grouped equivalent lambdas with
different parameter names on both engines. Running it against the preceding
library made the new check fail at `WindowGroups/Scan`.
[Request counts and negative control](evidence/p3-native-request-replay.json)

On `5740b1a0f`, Rust read rolling-frame upper bounds from the circuit and
selected timestamp waterlines at 56 rolling aggregates. It grouped windows
on VARIANT and array-of-ROW predicates on both engines. All 49 selected
compilations matched Java through 24,420 ordered S4 observations and emission,
and all 550 selected monotonicity comparisons matched. Across both frozen
populations, the 25 instrumented operations answered all 323,464 requests in Rust.
[Rolling frames and composite keys](evidence/p3-rolling-frame-offsets.json)

Rust canonicalized 145 shadowed LET bindings in the same order as Java.
Each initializer read the enclosing binding and each consumer read its own
declaration. All 12 classic and Gen-2 SQL replays matched through 5,884
ordered S4 observations and emission.
[Canonical name comparisons](evidence/p3-nested-canonical-names.json)

Rust grouped temporal windows with REAL and DOUBLE thresholds on classic
and Gen-2. All four compilations matched Java through 2,396 ordered S4
observations and final emission. A JDK 21 comparison checked 1,099,086
floating-point bit patterns, including distinct NaN payloads, signed zero
and values adjacent to powers of ten.
[Numeric and SQL comparisons](evidence/p3-floating-window-keys.json)

On `cb578c70d`, casting a partially bounded row can remove its bound while
preserving the bound and original field identity of an adjacent input
column. The new JNI regression failed on the previous native library. All
27 waterline tests passed with the correction.
[Waterline projection regression](../../sql-to-dbsp-compiler/SQL-compiler/src/test/java/org/dbsp/sqlCompiler/compiler/visitors/outer/monotonicity/WaterlineProjectionTests.java) ·
[Cold compilation measurements](evidence/p3-partial-row-cast-cold.json)

Rust selected shared windows whose timestamp offset depends on a UUID or
VARBINARY comparison. Four compilations on classic and Gen-2 matched Java at
all 2,396 ordered S4 observations and through emission. The three window
analyses answered the ten requests that previously needed Java.
[Temporal key comparisons](evidence/p3-binary-uuid-window-keys.json)

Rust hoisted repeated calculations in filters with 8 and 144 alternating
`AND` and `OR` terms. Both engines matched Java through 1,900 ordered S4
observations and artifact emission. The 144-term streaming test evaluated
`LOWER(name)` once per row and emitted only ID 145. The S3 worker preserved
the 299-level Calcite plan in both its checkpoint and dataflow output.
[Filter replay and runtime receipts](evidence/p3-long-boolean-filters.json)

On `9aec96f7b`, Rust ran limiter insertion on every selected circuit,
including circuits that combine newly handled operators with stateful ones.
All 291 classic and 259 Gen-2 selections and four directed cases per engine
matched Java through detached S2 replay and artifact emission.
[Limiter replay receipts](evidence/p3-mixed-limiter-replay.json)

Rust grouped windows on array and map lookups in six classic and Gen-2
compilations. The three window analyses answered every request, eliminating
ten Java fallbacks. Detached replay matched all 3,616 ordered S4 observations
and emitted artifacts. The same build passed all 181 input commands in the
52 executable classic clock cases. [Collection window keys](evidence/p3-collection-window-keys.json)

On `4b83f145e`, Rust answered all 24 closure projections in the eight
constructed-row `ARG_MIN` compilations. Previously, one cast per compilation
fell back to Java. All eight compilations matched Java at every S4 observation
and through emission. The streaming regression and all 550 selected
monotonicity comparisons passed. The hybrid also executed all nine frozen
programs from the ARG_MIN and minimum-waterline suites, checking 130 input
commands. The upstream PR passed both suites, including ten generated Rust
tests. After a checkpoint restore in either
direction, the aggregate rejected the late input, retracted the old minimum,
and emitted the new minimum with its NULL payload.
[Row cast comparisons](evidence/p3-row-cast-waterlines.json) ·
[ARG_MIN checkpoint interchange](evidence/p3-constructed-row-checkpoint.json)

On `8bb0f52b1`, the Java and Rust optimizers both handled `ARG_MIN` on a
constructed nested row with `LATENESS`. Eight classic and Gen-2 compilations
matched through 3,488 ordered S4 observations and emission. The streaming
regression retracted the old row and emitted the new row with its nullable
field. All 550 selected monotonicity comparisons matched. COUNT and LATENESS
checkpoints restored in both directions without bootstrap.
[Comparison and restore receipts](evidence/p3-constructed-row-arg-min.json) ·
[Cold compilation timings](evidence/p3-constructed-row-arg-min-cold.json) ·
[Upstream compiler fix](https://github.com/feldera/feldera/pull/8046)

On `b679cc7a6`, Rust combined 144-level nested waterline types. Both the
current and earlier Java callers preserved scalar identities in 128 checks
each. Detached replay matched 136 classic and 127 Gen-2 frozen inputs plus
four directed inputs per engine through 138,326 ordered S4 observations
and emission.
[Nested waterline replay](evidence/p3-nested-bound-combinations.json)

On `e9ec5cf98`, Rust grouped window inputs with TIME, DATE, TIMESTAMP and
TIMESTAMP WITH TIME ZONE literals. All 423 pass comparisons matched Java
across 133 frozen clock programs and eight directed compilations. The eight
compilations previously fell back to Java 16 times and now match through
4,824 ordered S4 observations and artifact emission.
[Temporal literal window comparisons](evidence/p3-temporal-literal-window-keys.json)

On `42d63cb45`, all 17,098 frozen SimplifyConditionals results matched Java.
A CASE with 144 nested SIGN calls previously failed in the hybrid while direct
Java emitted code. Classic and Gen-2 now match on both that query and its
16-call control, including all ordered observations in detached S2 replay.
[Conditional rewrite and emission comparisons](evidence/p3-deep-conditionals.json)

On `1226f2b55`, the JNI test passed with 144 nested aggregate fields. All
550 selected monotonicity comparisons and 16 detached compilations matched
direct Java through 11,178 ordered S4 observations and emission. Both engines
also emitted matching artifacts for an ARG_MIN query with four nested row
types. A separate 16-level SQL probe exhausted the Java heap on both paths.
[Aggregate waterline comparisons](evidence/p3-deep-aggregate-payloads.json)

On `3160a9fce`, the Rust window-group passes matched Java for all 133 frozen
clock programs and four directed compilations with decimal thresholds and
floating-point casts. The four directed compilations also matched through
2,412 ordered observations and artifact emission.
[Decimal window comparisons](evidence/p3-decimal-window-keys.json)

On `93fef8ab1`, Rust evaluated function arguments, row fields and local
bindings in lexical order. It matched all 550 selected monotonicity
comparisons, including the limiter graphs they produced. A separate
180-level initializer test matched Java through JNI. On `e5557ed3d`,
both engines compiled 16 and 144 nested SIGN calls through artifact emission
and matched all 1,516 ordered S4 observations during detached replay.
[Builds and comparison receipts](evidence/p3-scoped-waterlines.json)

On `b46957434`, the Rust visitor projected a chain of 180 arithmetic and 180
unary operations and matched Java's reduced expression through JNI. Detached
replay matched all 1,516 ordered observations for the two arithmetic SQL cases
on classic and Gen-2. The preceding checkpoint build met the cold compilation
ceiling on all three workloads during concurrent corpus replay.
[Mixed scalar comparisons](evidence/p3-mixed-scalar-waterlines.json) · [Cold timings](evidence/p3-deep-checkpoint-cold.json)

On `0c136c3eb`, a LATENESS query with 160 alternating additions and
subtractions completed through the supervised worker on classic and Gen-2.
The 16-operation control and the deep query matched direct Java on schema,
dataflow, diagnostics, classic source and stubs, and canonical Gen-2 circuits.
Detached S2 replay matched all 1,516 ordered S4 observations across the four
compilations and preserved persistent-ID hash inputs.
[Inputs, artifacts and replay receipts](evidence/p3-deep-checkpoint-expressions.json)

On `1215a23f5`, the Rust field-use analysis checked expression statements
before using the final value of a block. It preserved tuple and borrow
structure when comparing the two sides of an assignment. All 26,102 live
field-use requests agreed with the Java interpreter across the two frozen
SQL populations. Directed tests cover a discarded read and assignments
that trigger Java's diagnostic.
[Field-use comparisons](evidence/p3-statement-field-uses.json)

On `056b078af`, Rust compared flatmap shuffle identities while discovering
and replacing shared indexes. All 445 frozen compilations that previously
requested replacement matched through 211,706 ordered observations and
artifact emission. Rust answered all 890 discovery and replacement requests.
[Shared-index comparisons](evidence/p3-shared-index-shuffles.json)

On `6e5879fa8`, Rust grouped timestamp windows whose offsets contain
conditionals, string comparisons and null tests. All 423 pass comparisons
matched across the 133 frozen clock programs and eight directed cases. The
directed cases previously needed 20 Java fallbacks. All eight also matched
Java through 4,824 ordered observations and artifact emission.
[Conditional window comparisons](evidence/p3-conditional-window-keys.json)

On `a98a0ce96`, both engines compiled their entire frozen SQL populations
without a fallback at any of the 24 instrumented optimizer operations.
The coverage oracle records requests made by those operations. The full
artifact and runtime comparison in the phase status table used `bf609aab1`.
[Request counts and input identities](evidence/p3-optimizer-requests.json)

On `a45490134`, Rust selected temporal windows for timestamp keys with
integer offsets and scaled intervals. The five directed programs on each
engine previously needed 25 Java fallbacks. All 429 pass comparisons
matched across the directed and frozen clock programs. The ten directed
compilations also matched through 6,012 ordered observations and emission.
[Window key coverage and replay](evidence/p3-numeric-window-keys.json)

Rust projected waterlines through 180 nested LETs and blocks in the live
compiler on `c16f71675`. All 550 monotonicity comparisons matched Java,
and COUNT and LATENESS restored in both directions. On `955f165ae`, Rust
also classified a clock expression under 180 nested bindings and returned
the original window key and clock expression to Java. On each build, the
16 selected detached compilations matched through 11,178 ordered observations
and emission.
[Waterline projections](evidence/p3-nested-waterline-bindings.json) · [Lowering and temporal keys](evidence/p3-lowering-expression-batches.json)

On `c29e9dda5`, the full frozen corpus matched direct Java at 33,176
invocations of `RemoveUnusedFields` and 50,274 of `ReplaceCommonProjections`.
[Field and projection comparisons](evidence/p3-shared-input-field-types.json)

Rust handled all 7,288 binary and star join input-narrowing requests across
the two frozen populations on `4cb050b1e`. Both engines matched Java at
all 6,029 pass invocations on the 1,945 affected cases. All 425 selected
detached compilations matched through emission, and 210 classic programs
passed at runtime. On `90144d092`, all 197 ASOF input rewrites ran in Rust.
All 46 affected cases matched both at the pass boundary and through detached
emission. COUNT and LATENESS restored in both compiler directions on each build.
[Binary and star joins](evidence/p3-binary-and-star-join-inputs.json) · [ASOF joins](evidence/p3-asof-input-rewrite.json)

On `e6f389da0`, Rust handled 806 requests to narrow UNNEST inputs, including
remapping the columns copied from each input. Both engines matched Java at
all 756 pass invocations on the 373 affected frozen programs. All 337 selected
detached compilations matched through emission, all 166 executable classic
selections passed, and COUNT and LATENESS restored in both directions.
[UNNEST input comparisons](evidence/p3-unnest-input-readers.json)

On `2559510a7`, SQL compilation asked Rust to discover rank and tie columns
1,918 times. Rust rebuilt 379 comparator pairs after narrowing. The earlier
`0fcf9e21c` build called Java for discovery. All 1,870 pass invocations on
the 485 affected frozen programs agreed with Java after wiring discovery
into the SQL path. [TopK field discovery](evidence/p3-topk-field-discovery.json)

On `0f703786d`, Rust rebound 5,770 stream aggregate components to narrowed
input rows. All 4,443 pass invocations on the 1,335 affected programs matched
Java. The 296 selected detached compilations matched through emission, all
148 executable classic selections passed, and COUNT and LATENESS checkpoints
restored in both directions. On `aaa37b8c6`, Rust also retained both ASOF
timestamps on all 197 requests, including timestamps absent from the output.
[Aggregate readers](evidence/p3-aggregate-input-readers.json) · [ASOF inputs](evidence/p3-asof-input-fields.json)

On `558a19a6f`, Rust built 3,869 indexed-value projections for aggregates
and TopK, and 41 UNNEST row projections. Java agreed on all 5,689 pass
invocations across the 1,605 frozen cases that exercised them. On
`50e1fc438`, Rust derived narrowed closure parameter types 13,675 times.
Java agreed on all 9,348 pass invocations across the 2,944 affected cases.
For each build, 296 selected detached compilations matched through 133,314
ordered observations and artifact emission.
[Row projections](evidence/p3-project-used-fields.json) · [Narrowed parameter types](evidence/p3-compressed-parameter-types.json)

When narrowing joins, Rust built 2,089 input projections across both frozen
populations on `0a95d4f7b`. All 3,916 invocations of `RemoveUnusedFields` on
the 825 programs that exercised the change matched Java. All 296 selected
detached compilations matched through emission. The 148 executable classic
selections passed, and COUNT and LATENESS restored in both directions.
[Join input projections](evidence/p3-join-input-projection.json)

On `ebf489c23`, Rust preserved indexed keys while projecting values for
aggregates and TopK. Java agreed at all 5,595 pass invocations on the 1,565
programs that exercised this code. The 296 selected detached compilations
matched through emission. [Indexed value projections](evidence/p3-indexed-value-projection.json)

On `813ee9a19`, Rust classified map and index projection rewrites at 190,767
producer visits. Across both frozen populations, all 86,472 invocations of
`OptimizeProjections` matched direct Java. All 296 selected detached
compilations matched through 133,314 ordered observations and artifact emission.
The 148 executable classic selections passed, and COUNT and LATENESS restored
in both compiler directions. [Projection results](evidence/p3-projection-producers.json)

On `432124575`, Rust selected column waterlines at 997 table and 29 view
visits. Its monotonicity analysis and downstream limiter graphs matched Java
on 550 pass comparisons. All 29 frozen programs that exercised view-declared
LATENESS matched through emission, as did 16 selected compilations with
`--correlatedColumns`. COUNT and LATENESS checkpoints restored in both
directions. [Waterline coverage, replay and restores](evidence/p3-declared-column-waterlines.json)

On `d67ea1f8b`, Rust answered all 17,098 left-join chain discovery requests
and all 583 column-propagation requests across the two frozen populations.
Both compilers selected identical chains and columns for all 20 rewrites.
All 228 selected detached compilations matched through emission, all 106
executable classic cases in that selection passed, and COUNT and LATENESS
restored in both directions. [Left-join comparisons](evidence/p3-carried-chain-columns.json)

On `5c6c0cd5b`, Rust answered all 33,587 field-use requests across the two
frozen populations. Java agreed on the fields read from each parameter,
including the accumulator of a filtered ARG_MIN or ARG_MAX. All 228 selected
detached compilations matched through 105,453 ordered S4 observations and
emission. All 106 executable classic selections passed. COUNT and LATENESS
checkpoints restored in both directions.
[Field-use, runtime and checkpoint results](evidence/p3-aggregate-field-uses.json)

On `a9387c693`, Rust answered all 51,294 circuit-wide candidate-key
requests from the two frozen populations. Direct Java agreed on ordered keys
at 374,851 ports and on all 108,044 predecessor transforms, including which
output columns copy each input column. All 228 selected detached compilations
matched through 105,453 S4 observations and emission. The 106 executable
classic selections passed, and COUNT and LATENESS restored in both directions.
[Candidate keys and checkpoint results](evidence/p3-circuit-candidate-keys.json)

On `6d3eaf928`, the filter pass matched all 57 selected invocations and
50 detached compilations. On `5ded4b782`, shared index chain collapse
matched all 634 frozen cases in which Java rewrote the graph. All 42
selected detached compilations also matched. On Nexmark, median
main-thread CPU fell from 204 to 5 ms for the filter pass and from 127
to 7 ms for chain collapse. Each profile used three fresh hybrid JVMs.
[Filter predicates](evidence/p3-filter-predicates.json) · [Shared index closures](evidence/p3-shared-chain-closures.json)

The decoder campaign on `bf609aab1` completed 6,835,072 cases in one
worker CPU-hour. It accepted 2,658,642 inputs and finished every batch
within the two-second deadline. [Decoder and runtime results](evidence/p3-empty-row-clock-inputs.json)

On `aaccfae30`, Rust analyzed each join closure once for both column lineage
and candidate keys. The 11,665 join requests needed no Java fallback.
Across the two frozen populations, direct and hybrid candidate keys matched
at all 374,851 compared ports. The 228 selected detached replays matched
through all 105,453 S4 observations and artifact emission.
[Join key comparisons](evidence/p3-join-closure-keys.json)

On `63dbe7a9f`, direct Java and the hybrid compiler agreed on the ordered
candidate keys of all 374,851 ports visited by ordinary and lossless-cast
analysis across both frozen populations. Rust answered all 145,533 recorded
requests for filters, maps, indexed maps and ranked outputs. All 228
selected detached replays matched through emission, all 106 executable
classic selections passed, and COUNT and LATENESS restored in both directions.
The Gen-2 export runs `CastTests#testAllCasts/0` in classic mode. Replaying
the same SQL explicitly in Gen-2 also matched through all 479 S4 observations
and emission. Run `CandidateKeyOracle POPULATION` with the packaged test
classes and JNI library to compare both key analyses on each live circuit.
[Candidate key comparisons and checkpoint receipts](evidence/p3-projected-candidate-keys.json)

On `14f656d66`, candidate-key analysis distinguished separate flatmap shuffles
and grouped projections that reused the same shuffle. Both the per-closure
and whole-circuit paths passed. The build passed 188 focused and 228 nightly
JUnit tests and matched all 11,965 ordered S4 observations across 24 replays.
COUNT and LATENESS restored Java and hybrid checkpoints in both directions.
Four-worker cancellation p95 was 20.81 ms against the 25 ms limit.
[Candidate-key shuffle identity and checkpoint results](evidence/p3-candidate-key-shuffles.json)

On `5964e657b`, Rust derived filter keys from constant equalities, including
columns that name the same value. The Java and hybrid analyses agreed on
9,262 output ports across 114 programs per engine. The 228 detached replays
matched through all 105,453 ordered S4 observations and artifact emission.
[Filter key comparisons](evidence/p3-filter-candidate-keys.json)

On `9c0cd1ed9`, the Rust window scan and reorder passes compare compound
timestamp expressions by operand order, type and interval unit. Both engines
matched Java on four directed programs, including reversed arguments to
`COALESCE` and equal durations written in different units. All 399 pass
comparisons on the 133 frozen clock programs matched. The 22 selected frozen
replays and eight directed replays matched through emission, and all eight
executable classic selections passed. [Window expressions and replay receipts](evidence/p3-interval-window-projections.json)

The Rust window finder groups indexes by source port and timestamp expression,
then selects the shared columns in first-use order. On `59f09c271`, it
answered all 119 requests in 133 frozen clock programs. The direct Java and
hybrid finders agreed on all nine groups and their 14 members. All 22
selected programs matched through artifact emission. The pass comparator
checks each selected expression against its owning circuit across separate
JVMs. [Window index comparisons](evidence/p3-indexed-row-window-keys.json)

On `bf609aab1`, the Rust clock classifier retains nested nonnullable empty
rows even when the surrounding row has unbounded fields. All 8,571 classic
and 8,527 Gen-2 SQL programs compiled with the coverage observer. Rust
answered all 8,937 requests recorded for the five exercised decisions. The
full detached replay matched direct Java on all 17,098 frozen cases through
6,996,738 ordered optimizer observations and emission. All 8,256 executable classic
cases passed with 11,407 input commands. COUNT and LATENESS checkpoints
restored in both directions. The coverage counters measure the instrumented
decisions only. [Clock projections and compiler coverage](evidence/p3-empty-row-clock-inputs.json)

To verify the per-program counts, run
`python3 crates/sql/scripts/summarize_optimizer_coverage.py POPULATION OBSERVATIONS`.
For observations read from S2, also pass `--checkpoints S2_DIRECTORY`.

The Rust transfer schedule selects the common fields of sums and differences,
the shared key of an antijoin, and the minimum or grouping-key bound of an
aggregate. Across 550 monotonicity comparisons, Rust answered all 85 antijoin
projection requests and all 148 chain-minimum checks. No operator in that
sample used the Java visitor's general transfer dispatch. Both engines
matched direct Java through detached artifact emission on 16 selected cases.
[Set and minimum transfers](evidence/p3-set-and-minimum-transfers.json)

Rust selects waterlines for filters, deindexing, HOP, LAG, stream and ASOF
joins, and linear and rolling aggregates. It computed all 1,652 join argument
projections in the 550-program monotonicity comparison. All 11,178 ordered
optimizer observations matched across 16 detached compilations. Direct Java
also reproduced its pre-refactor records for all 16. COUNT and LATENESS
checkpoints restored from Java to hybrid and from hybrid to Java.
[Temporal and aggregate transfers](evidence/p3-temporal-aggregate-transfers.json)

Rust selected key and value waterlines at all 194 window transfers and 129
annotated clock aggregates in the 550-program monotonicity sample. Rust also
answered all 6,600 whole-closure requests and 452 temporal classifications on
that sample. Run `OptimizerCoverageOracle POPULATION S2_DIRECTORY` with the
matching optimizer library to collect the per-program counts.
[Indexed waterline transfers](evidence/p3-indexed-waterline-transfers.json)

Rust validates expression occurrences and variable bindings before projecting a
whole waterline closure. The compiler preserved Java's diagnostics for shared
expressions, unresolved lambda captures, and mistyped parameter reads. All 550
monotonicity comparisons and 16 detached compilations matched. COUNT and LATENESS
checkpoints restored in both directions. [Closure validation](evidence/p3-closure-validation.json)

Rust compares loop bindings and flatmap shuffle identity when grouping temporal
filters. All 17,098 frozen column-lineage reports matched the corrected baseline.
Across 16 detached compilations, all 11,178 ordered optimizer observations and
both engines' final artifacts matched direct Java. [Temporal key comparisons](evidence/p3-temporal-loop-flatmap.json)

Rust selects the top-level hybrid pass order and controls fixed-point iteration.
The packaged compiler matched Java's pass order for snapshot and incremental
compilation under both set and multiset output semantics. All 16 selected
direct compilations also matched the pre-refactor Java records. Both builds
restored COUNT and LATENESS checkpoints in both directions.
[Optimizer order](evidence/p3-optimizer-order.json) · [Fixed-point scheduling](evidence/p3-repeat-scheduler.json)

Rust projects partially bounded rows through references and preserves the
constructor type of non-NULL `isize` and `usize` literals in waterlines.
All 550 monotonicity comparisons and 11,178 ordered optimizer observations
matched Java across the two engines. [Reference waterlines](evidence/p3-reference-waterlines.json)

Rust compares the declared types of temporal keys structurally, including
function parameters and results, generic type arguments, stream kind and stream
nesting. A regression compares 256 nested rows with shared fields. The build
matched Java on 550 monotonicity cases and 11,178 ordered optimizer observations
in 16 detached compilations. [Type comparisons](evidence/p3-type-equality.json)

Rust builds each supported waterline closure in one call. Its row, indexed and
join bindings use the same expression arena as the projected body.
All 550 selected monotonicity pass comparisons and 16 detached compilations
matched Java. The COUNT and LATENESS checkpoints restored in both directions.
[Waterline closures](evidence/p3-waterline-closures.json)

Rust builds binary and multiway join derivatives, including filtered joins and
null padding for left joins. All 550 binary and 30 multiway pass comparisons
matched Java. The multiway cases covered three to fourteen inputs. Both engines
also matched on all 61 detached programs selected for multiway replay.
[Binary join graphs](evidence/p3-binary-delta-graph.json) · [Multiway join graphs](evidence/p3-star-delta-graph.json)

A rejected `NOW()` inside a filter lambda carries its residual predicate and the
lambda through native lowering. All 133 selected clock pass comparisons matched.
The direct and hybrid CLIs emitted the same ordered warning and error, including
source ranges and snippets, in six negative compilations.
[Clock diagnostics](evidence/p3-clock-lambda-diagnostics.json)

Rust expands aggregates, DISTINCT, and positive weights into their delta graphs.
All 550 selected pass comparisons and 16 detached compilations matched Java.
COUNT and LATENESS checkpoints restored in both directions.
[Unary delta expansion results](evidence/p3-unary-delta-expansion.json)

Rust supplies the ordered terms of the binary join derivative, including
the null-padded term for a left join and the filter on each term of a fused
join. The hybrid compiler binds those terms to the original ports and keeps
their correspondence for limiter insertion. All 550 selected delta graphs
matched Java across 908 binary joins. The packaged compiler also matched on
16 detached programs. COUNT and LATENESS restored in both directions.
[Delta expansion and restart results](evidence/p3-delta-join-formulas.json)

Rust computes the waterline shape of a nested SQL row once per type and
shares it across repeated fields. The regression uses 160 nested row types.
All 550 selected monotonicity invocations and 16 detached compilations matched
Java. With four CPUs reserved for four occupied workers, cancellation p95 was
16.33 ms over 20 cancellations. Peak driver and front-end RSS while waiting
for input was 70,717,440 bytes per worker. [Waterline and worker results](evidence/p3-shared-row-types.json)

With `--correlatedColumns`, Rust traces comparisons to their source columns and
reports columns that joins equate. Both compilers now preserve the input columns
when a Positive operator changes row weights. All 8,571 classic and 8,527 Gen-2
reports completed in Rust and matched Java on stdout, including the 62 cases per
engine that previously failed. With the report enabled, all 124 repaired cases
also matched direct Java through detached compilation and artifact emission,
including 27,774 ordered optimizer observations on classic and 27,990 on Gen-2.
The directed EXCEPT ALL regression checks the reported join keys.
[Corrected baseline](evidence/p3-positive-lineage.json) ·
[Upstream fix](https://github.com/feldera/feldera/pull/8044)

With the report enabled on the earlier build, all 16 selected detached
compilations matched through artifact emission.
Run the test utility `LineageOracle` with the frozen JSONL population and the JNI
library on `java.library.path` to repeat the report comparison.
[Report and replay results](evidence/p3-correlated-columns.json)

Rust follows column lineage through deeply nested closures, references and
shared tuples using explicit stacks. A 512-binding regression test preserves
the input column. Across 228 selected programs, Rust and Java agreed on fixed
columns at 203 filters and on keys at 9,262 ports. All 33 detached compilations
matched through emission. [Column-lineage results](evidence/p3-deep-lineage.json)

Rust interns common subexpressions and decides which uses within each closure
share a computation. All 550 selected pass invocations and 33 detached
compilations matched Java. COUNT and LATENESS checkpoints restored in both
directions. Four-worker cancellation p95 was 35.05 ms during concurrent
verification, above the 25 ms limit.
[Value numbering, detached replay and worker results](evidence/p3-value-numbering.json)

Rust validates stream kinds after every top-level hybrid optimizer pass. Detached
compilations of 36 classic and 33 Gen-2 programs matched Java at all 34,152
ordered pass observations and on emitted artifacts. In a shared devcontainer,
the hybrid/direct p95 ratios for cold SQL-to-artifact compilation were 0.837
for the array subquery, 1.083 for Nexmark and 0.780 for the outer join.
[Stream validation and cold compilation results](evidence/p3-post-pass-streams.json)

Rust tracks block-local waterlines through mutable declarations, discarded
statements and indirect calls. All 6,600 row projections in the frozen
monotonicity selection completed in Rust. The 550 monotonicity comparisons,
133 clock-rewrite comparisons and 11 detached compilations matched Java.
A directed test checks the same compiler error for an uninitialized local
through both entry points. [Block waterline results](evidence/p3-block-waterlines.json)

Rust derives ASOF waterline projections from the join closure. All 24 classic
and 22 Gen-2 frozen ASOF programs matched direct Java through detached replay
and emission. The key-equivalence visitor resolves nested declarations in
index and temporal keys. The engines agreed on candidate keys at 12,270 output
ports across 310 selected cases.
[ASOF replay](evidence/p3-asof-join-projections.json) · [Key comparisons](evidence/p3-scoped-key-comparison.json)

Rust tracks bounds through immutable row bindings, references, arithmetic,
and named scalar functions. Temporal predicates retain their individual clock
bounds when the comparison itself has no bound. All 71 classic and 62 Gen-2
frozen clock programs matched Java at the `RewriteNow` boundary. Four directed
SQL predicates with conditional timestamps also matched on each engine.
[Clock operands](evidence/p3-row-clock-operands.json) ·
[Row bindings](evidence/p3-lexical-row-waterlines.json) ·
[Arithmetic and checkpoint restores](evidence/p3-row-arithmetic.json)

Rust projects scalar casts, unary operators and unsigned sort keys inside row
waterlines. The analyzed scalar keeps the SQL type from the original closure.
All 550 frozen monotonicity comparisons, 12 directed LATENESS/UNNEST comparisons
and 40 detached compilations matched direct Java. COUNT and LATENESS restored
across compilers in both directions. Four-worker cancellation p95 measured
33.67 ms during replay and 4.54 ms in isolation, against the 25 ms limit.
[Scalar waterline comparisons and worker measurements](evidence/p3-scalar-row-waterlines.json)

Rust rewrites custom-ordered index functions together, preserving shared
comparator references and the order of new declarations. All 24 classic
and 22 Gen-2 cases where the pass changed the graph matched Java at its
boundary and through detached emission. COUNT and LATENESS checkpoints
restored in both directions. [Comparator comparisons](evidence/p3-ordered-index-functions.json)

Rust validates trace sharing against the operator topology and returns the
stream substitutions and retention policies to discard. All 63 classic and
215 Gen-2 frozen programs where the pass had changed the graph matched Java
at this pass and through detached replay and emission. COUNT and LATENESS
checkpoints restored in both directions.
[Trace-sharing comparisons](evidence/p3-shared-trace-edits.json)

The classic hybrid driver limits JIT compilation to tier 1 in the JVM that
serves a single compilation request. The cold Nexmark p95 measured 3.893 s
against 3.177 s for direct Java, exceeding the 1.2× ceiling. The array and
outer-join workloads met the ceiling. With four workers waiting for input,
cancellation p95 was 5.16 ms. COUNT and LATENESS restored in both directions.
[Full cold compilation measurements](BENCHMARKS.md#single-request-jvm-2026-11-22)

Rust combines waterlines across join keys and sum inputs, preserving the SQL
type of each selected field. The monotonicity pass matched Java on 291 classic
and 259 Gen-2 frozen programs. Directed three-input union, antijoin, ASOF and
star join cases matched at every optimizer boundary through emission on both
engines. [Waterline comparisons](evidence/p3-combined-waterlines.json)

Rust expands each HOP into a map of window starts and a flat-map of output
rows using the input port, row types, interval, size and offset. All 20 classic
and 20 Gen-2 frozen programs with HOP calls matched at this pass.
Seven per engine matched Java at every boundary through emission.
[HOP graph comparisons](evidence/p3-hop-window-fragments.json)

Rust selects common-expression candidates and traces live output ports for dead-code
elimination from ordered graph edges. Both analyses matched Java through emission
on 30 classic and 29 Gen-2 recursive programs, 25 smoke cases and Nexmark. On
Nexmark, the compact CSE input reduced traffic across its 24 calls from
1,104,939 to 423,497 bytes. Median calling-thread CPU time for the candidate
query fell from 15.7 to 6.9 ms across 12 paired compilations.
[Replay and CSE timings](evidence/p3-compact-operator-inputs.json)

The integral-pushing pass imports closure signatures and reuses the live
closures when it returns the edited circuit. Both invocations matched Java
on all 1,287 classic and 8,526 Gen-2 frozen cases where this pass changed
the graph. Across 12 paired Nexmark compilations, median CPU time for the
two invocations fell from 284 ms to 253 ms during concurrent verification.
[Integral-pushing replay](evidence/p3-integral-signatures.json)

For flat circuits, Rust follows consecutive integrations at each output and
sends the JVM only the moves and derivative cancellations to apply. This
retains the live functions and their source positions. The recursive path
still uses the Rust graph rewrite. All 19,626 pass invocations on previously
changed programs matched Java, as did 572 selected programs through emission.
COUNT and LATENESS checkpoints restored in both directions.
[Flat integral edits](evidence/p3-flat-integral-edits.json)

Rust selects the table and view columns with declared LATENESS and determines
which tuple views inherit input bounds. All 550 selected monotonicity pass
comparisons and 25 detached replays matched Java. COUNT and LATENESS
checkpoints restored in both directions. [Relation waterlines](evidence/p3-relation-waterlines.json)

The Rust clock classifier handles calls with several arguments, including
DATEDIFF, LOG and POWER. All 133 frozen RewriteNow invocations and five
directed argument-order and constant-base cases on each engine matched Java.
Rust also resolves immutable local clock bindings. Across both engines, 12
directed comparisons produced a window bound and eight retained a residual
filter, matching Java for both LET expressions and shadowed block variables.
[Lexical clock bounds](evidence/p3-lexical-clock-bounds.json)

Rust classifies scalar unary and binary clock expressions, including maxima
with nullable operands. Across both engines the directed oracle produced
36 bounds and 114 residual filters. All 133 frozen RewriteNow cases and
14 detached replays matched Java. COUNT and LATENESS checkpoints restored
in both directions. [Clock comparisons](evidence/p3-scalar-clock-comparisons.json)

The clock classifier follows cloned fields of nested input rows and checks
the sign of REAL, DOUBLE, decimal, interval and integer literals. The Rust
rule handled all ten directed filters across both engines. Both their full
replays and the 133 frozen RewriteNow comparisons matched Java.
[Nested field and numeric comparisons](evidence/p3-nested-numeric-clock-bounds.json)

Rust derives collection keys for binary, left and star joins from each input's
candidate keys and the columns copied by the join closure. The regular and
lossless-cast analyses agreed with Java at 6,631 output ports, including 460
join outputs. All 135 classic and 134 Gen-2 programs selected for joins and
expression rewrites matched through ordered S4 and emission. COUNT and LATENESS
restored from each compiler's checkpoint with the other compiler.
[Join key comparisons](evidence/p3-join-collection-keys.json)

The regular and lossless-cast key analyses now use the same Rust closure
interpreter as multiset analysis. Rust reproduced all 11,282 column copies in
2,782 closures, including indexed results and ranked output values. The oracle
compared the source and destination of each copy, then compared keys at 8,625
ports. All 188 programs matched on detached replay. Assignments, returns and
expressions beyond the interpreter's nesting limit still use the Java visitor.
[Column provenance](evidence/p3-column-provenance.json)

Rust removes a column from a candidate key when a top-level filter conjunct
fixes its value. It tracks literal values through the closure, including
whether both branches of a COALESCE carry the same literal. Direct Java and
Rust agreed on 38 fixed columns across 203 filter visits and on the keys at
all 9,262 output ports. The 114 classic and 114 Gen-2 detached replays matched
through emission. COUNT and LATENESS checkpoints restored in both directions.
[Filter key comparisons](evidence/p3-fixed-filter-keys.json)

Rust projects ordinary row closures onto their waterlines. Nested field reads use
the index in the compacted row and retain the analyzed input column’s SQL type.
The full monotonicity pass matched Java on 291 classic and 259 Gen-2 programs,
including the aggregate case that distinguishes two equal integer types by
their sharing. Rust also selects incremental boundaries from the live topology.
That pass matched Java on all 524 selected incremental programs. All 550
detached replays matched through emission. Both compiler directions restored
COUNT and LATENESS state.
[Pass comparisons and checkpoints](evidence/p3-row-waterlines-and-incrementalization.json)

Indexed inputs and three-parameter joins also use the Rust row projection.
An indexed value moves to slot zero when the key has no bound. A join
resolves each field against its own input, including repeated reads of the
same field. All 550 selected monotonicity comparisons and 40 detached
replays matched Java, and both checkpoint programs restored in both directions.
[Indexed and join waterlines](evidence/p3-indexed-join-waterlines.json)

Rust preserves the declared types of unknown row fields when another field has
a waterline. The monotonicity pass matched all 550 selected frozen programs and
12 directed LATENESS and UNNEST compilations, including WITH ORDINALITY. All
40 detached replays matched through emission. COUNT and LATENESS checkpoints
restored in both directions. [Row field comparisons](evidence/p3-unbounded-row-fields.json)

The output-projection pass sends Rust the producer and reader functions and
reattaches the result to the live operator. This preserves chains of projections
rewritten earlier in the same pass. All 580 pass observations and 310 selected
detached replays matched Java. COUNT and LATENESS restored in both directions.
Across eight fresh-JVM pairs, the median calling-thread CPU time for this pass
on Nexmark fell from 176.5 ms to 2.9 ms during concurrent verification.
[Output-projection comparisons](evidence/p3-live-output-projections.json)

Rust groups output columns that evaluate equivalent expressions under the same
closure bindings. Direct Java agreed on all 3,709 observed closures and all
533 groups of equal columns, including lookups into map expressions. COUNT and
LATENESS checkpoint restores passed in both directions.
[Computed column comparisons](evidence/p3-computed-column-groups.json)

The JNI expression reader validates new references against both arenas and
inserts the smaller map into the larger one. Across 41 paired sweeps of
554 Nexmark batches, this saved a median 2.04 ms per sweep (bootstrap 95%
interval 1.83–2.20 ms). [Decoder measurements](evidence/p3-expression-arena.json)

Rust constructs both the data closure and its reduced waterline for identity
operators, including indexed rows where only the key or value has a bound.
The resulting limiter graphs matched Java on 291 classic and 259 Gen-2
programs. All 25 detached smoke comparisons passed through emission.
COUNT and LATENESS checkpoints restored in both directions.
[Identity waterline comparisons](evidence/p3-identity-waterlines.json)

The live Simplify pass sends each selected expression to Rust. All 17,136
invocations matched Java across the 2,144 classic and 2,140 Gen-2 programs
with a changed Simplify boundary. On Nexmark, mean main-thread CPU time in
the four simplifier calls fell from 348 ms to 306 ms across 12 paired
compilations during concurrent corpus replay.
[Simplify pass comparisons and timings](evidence/p3-simplify-root-transport.json)

The live optimizer expands each stream aggregate from its list, indexed input,
output type and row weight. The pass matched Java on all 958 classic and 949
Gen-2 frozen programs where expansion changed the circuit. The comparator
checks the weight's identity at entry and exit and the append-only producer
ports in the entry graph. Six programs per engine matched at every optimizer
boundary through emission. COUNT and LATENESS checkpoints restored in both
directions. [Aggregate expansion comparisons](evidence/p3-stream-aggregate-lists.json)

Rust infers clock bounds for scalar row columns, casts, unary function calls
and scaled intervals. All 163 classic and 151 Gen-2 top-level clock filters
at RewriteNow entry now use this analysis. Twenty frozen programs had
previously needed the general closure analysis. All twenty matched direct
Java at every optimizer boundary through emission. All 532 isolated temporal
pass comparisons matched, and COUNT and LATENESS checkpoints restored in both
directions. Other expression forms can still use the general analysis.
[Clock predicate bounds](evidence/p3-clock-predicate-bounds.json) · [Casts, calls and scaled intervals](evidence/p3-clock-scalar-bounds.json)

The live waterline simplifier sends Rust each selected expression or
aggregate list. The entry and exit circuits matched Java on Nexmark and on
typed and raw tuple closures, including the annotated cases that specialize
both branches. All 13 classic and 12 Gen-2 detached smoke programs matched
through every S4 boundary and final artifacts.
[Waterline replay and timings](evidence/p3-waterline-root-transport.json)

Rust builds both clock sources used by NOW(). The system clock emits a
timestamp singleton and an index. The table-backed clock takes the maximum
timestamp and exposes both the indexed and collection outputs. All 133
frozen RewriteNow comparisons and 14 detached programs matched Java.
COUNT and LATENESS checkpoints restored in both directions, and each
LATENESS reader rejected late input after restore.
[Clock source comparisons](evidence/p3-clock-source-operators.json)

Rust now builds the map or residual filter with its clock join in one call.
After a residual filter runs, a projection removes the timestamp. All 133
frozen invocations matched the preceding Java build. Fourteen detached
cases matched through emission, and both engines preserved the diagnostics
for unsupported clock calls inside map and filter lambdas.
[Clock join consumers](evidence/p3-clock-join-consumers.json)

Rust indexes rows and joins them to the indexed clock when a map or residual
filter needs NOW(). All 133 frozen invocations matched Java. Seven programs
per engine matched through detached replay and emission. The COUNT and
LATENESS checkpoints restored in both directions.
[Clock join comparisons](evidence/p3-clock-join-operators.json)

Rust builds the window for each group of compatible clock comparisons. It
filters nullable keys, indexes rows, differentiates the collection, applies
the bounds and integrates the result. A final map restores a key column to
its original position, or a deindex drops a computed key. All 133 frozen
RewriteNow invocations matched Java. Detached replay also passed for the
seven selected programs and both computed-key cases on each engine.
[Temporal window operators](evidence/p3-temporal-window-operators.json)

Rust constructs the differentiator and waterline that turn the clock
collection into a scalar timestamp for temporal windows. The importer binds
the live input and shares types and expressions across the new operators.
All 133 frozen RewriteNow invocations matched Java. Fourteen detached cases
also matched at each S4 boundary and through emission.
[Scalar clock operators](evidence/p3-scalar-clock-operators.json)

Each window now consumes the clock collection in the same Rust call that
builds its scalar waterline and row index. Both engine populations matched
Java in all 133 isolated clock-pass comparisons and 14 detached replays.
Four waiting workers cancelled in 7.61 ms at p95 during corpus replay.
[Window and clock comparisons](evidence/p3-window-clock-fragment.json)

Rust lowers a filter closure into its ordinary filters, temporal windows and
residual clock join in one call. All 163 classic and 151 Gen-2 clock filters
in the frozen programs used this path. Every isolated `RewriteNow` invocation
matched Java, including generated names and source positions. Both engines
also preserved the warning and refusal for a clock call in a nested lambda.
All 71 classic and 62 Gen-2 clock programs matched direct SQL compilation
from detached S2 through every S4 observation, S3, schema, hash inputs and
emitted artifacts on `32d585d40`.
Java still traverses the circuit and handles expressions outside the Rust
classifier. [Clock filter graphs and checkpoint results](evidence/p3-clock-filter-graphs.json)

The DISTINCT pass uses operator kinds, ports and known stream multiplicities
to check whether it needs the full circuit. It requests expression data when
an unresolved multiplicity could change the result. The reduced path skipped
a 1,424,989-byte Nexmark snapshot. All 200 classic and 199 Gen-2 directed
programs matched Java, including 13 changed invocations on each engine.
[DISTINCT pass comparisons](evidence/p3-distinct-topology.json)

Rust expands each DEFAULT or LATENESS expression independently. The Java
metadata visitor attaches the result to its column, so this pass no longer
serializes the entire circuit. All eight frozen programs with changed
initializers matched at every S4 boundary and through emission on both
engines. The 20 classic ATAN2 regressions also passed on this build.
The classic runtime suite on this revision passed all 8,256 executable cases
across 11,407 commands, including 232 expected errors.
[Column initializer replay](evidence/p3-metadata-initializers.json)

Rust selects the clock calls that require temporal map and filter rewrites.
The last matching call supplies the join's source position, including after
residual predicates have been combined. All 133 frozen RewriteNow invocations
matched Java. Fourteen detached programs matched through emission, and COUNT
and LATENESS checkpoints restored in both directions.
[Temporal clock selection](evidence/p3-temporal-clock-sites.json)

Limiter insertion preserves the live identities of unaffected operators. This
matters when projection optimization revisits a map it already analyzed. All
20 classic ATAN2 cases matched direct Java through ordered S4, final S3 and
emission. The same build passed detached replay of all 15 case/engine pairs
flagged in the earlier partial population scan.
[Limiter operator identity](evidence/p3-limiter-operator-identity.json)

Rust selects the individual consumer inputs that need private traces. It also
chooses which input supplies an identity waterline and which operators need
closure analysis. Java applies these decisions to the live circuit. All 524
trace-separation invocations and 550 enclosing monotonicity analyses matched
direct Java. Detached replay also matched at every observed boundary and
through emission for 13 classic and 12 Gen-2 programs.
[Trace inputs and monotonicity transfers](evidence/p3-trace-inputs-and-transfer-rules.json)

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
and 382 Gen-2 observations. All 59 recursive cases also selected Rust and
matched at every boundary through emitted artifacts in detached S2 replay.
The native selector covers every operator class
in the archived limiter inputs for all 17,098 programs, including nested bodies.
The population verifier's `--require-native-limiters` option checks that Rust
actually ran the pass once per case.
[Recursive limiter comparisons](evidence/p3-recursive-limiter-bounds.json)

Rust schedules monotonicity transfers in expanded circuit order. LATENESS
on a table or view, a bounded window and the clock aggregate can introduce
a waterline. Joins and aggregates then propagate it according to their inputs.
The enclosing analysis and limiter output matched Java on 291 classic and
259 Gen-2 cases. Twenty-five directed cases also matched at every optimizer
boundary and through the emitted artifacts. [Monotonicity schedule](evidence/p3-monotonicity-schedule.json)

For a preceding ASOF match, the right timestamp waterline bounds the left
trace. The right trace keeps values above the minimum of both waterlines and
the latest value below it. All 46 newly eligible ASOF cases matched Java
at limiter entry and exit. The bounded streaming regression matched all 491
classic and 493 Gen-2 observations. The same build passed 1,492 runtime input
commands on 592 executable cases across the four join families. COUNT and
LATENESS restored in both directions. [ASOF join bounds](evidence/p3-asof-join-bounds.json)

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

Rust now traverses adjacent filters in producer order, merges compatible clock
bounds and preserves other readers of the upstream filter. All 71 classic and
62 Gen-2 clock programs matched Java at the pass boundary. Fourteen detached
replays matched through emission, and COUNT and LATENESS checkpoints restored
in both directions. The replay records a Java fallback separately when Rust
cannot classify a predicate. None of these cases required one.
[Temporal filter merge results](evidence/p3-temporal-merge-pass.json)

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
manager build and compiler package and counts only completed UTC days. The
package ID hashes the concatenated 32-byte SHA-256 digests of the worker
executable, native optimizer library and selected compiler JAR, in that order.
Deploy immutable packages for the field window. The worker checks
that all three files retain their identities across its compilation. A missing
package ID contributes zero matches. A difference with a missing ID resets the
window for that manager build. A difference takes effect immediately, even on
the current day. A day below the sample minimum contributes no qualifying day.
Enable `FELDERA_LOG_JSON=1` on the manager to preserve the original timestamps
and counters in the export. The report flags a review at the third observed
reset. It deduplicates overlapping exports and reports a lower bound when
older logs lack individual mismatch events or events share a timestamp.

```sh
python3 crates/sql/scripts/shadow_sample.py --build "$COMPILER_BUILD" --package "$PACKAGE_SHA256" field-logs/*.jsonl
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
