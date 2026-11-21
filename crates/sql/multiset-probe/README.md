# P0 multiset and dead-code experiment

This disposable Rust crate ports the supported rules of `MultisetAnalysis`
and `DeadCode(keepAllSources=true)`. It also rewrites the internal S2/S3 subset
files for resumption by Java. Java still performs the remaining optimizer
passes. This is part of spike 2, not its completed acceptance criterion or the
production `sql-ir` crate.

The experiment selects 50 programs before compilation: 30 projections over
five scalar types, ten cast/conditional cases, and ten constant, keyed-table,
filter, and lateness cases. Each includes an unused local view. Java exports
both S2 and S3 in classic and Gen-2 modes; Rust infers every output port's
multiset flag and removes dead operators. The harness compares the facts and
resulting graphs with independent Java analyses and transformations.

For each mode, separate Java processes resume the Rust-written S2 and S3 files.
The S2 path runs the complete Java optimizer; the S3 path recomputes hashes.
Both must match direct Java S3 on canonical graphs, persistent ids, actual
hash-input strings, schema, diagnostics, Gen-2 dataflow fingerprints, and classic
Rust artifact fingerprints.
The original SQL is not submitted to the resumed compiler. No generated program
is executed by this experiment.

An isolated `RemoveViewOperators(all=false)` comparison bypasses ordinary
view operators and preserves recursive views, views with lateness, and the
system error view. Across the same 200 input graphs, Rust matches Java and
bypasses 282 views. A separate lateness fixture checks a view chain with
ordinary views on both sides of a preserved view. The version-4 Java tail
regression rerun preserves the original pass order and passes all 200
comparisons. See the [run record](../evidence/multiset-view-removal.json).

Two additional Rust passes match the Java optimizer. `MergeSums` preserves
input order and multiplicity when flattening, cancels complements one copy at
a time, and emits the correct type and identity for empty indexed and
unindexed results. `EnsureDistinctOutputs` inserts distinct operators at
multiset sinks on pre-incremental graphs. The version-4 replay rewrite preserves
the sink's provenance and shares the original Calcite relation with the new node.
The measured sink provenance is supported. A sink with composite provenance
requires additional intermediate-lineage handling.

The Java observer captures these passes at their scheduled positions on all
50 programs in classic and Gen-2 modes. With set and bag output settings,
there are 200 scheduled `MergeSums` comparisons and 100 scheduled
`EnsureDistinctOutputs` comparisons. All match. A separate replay run uses the
same selection. The set mode makes 152 insertions across 100 S2 graphs.
Independently transformed Java and Rust files both resume to direct Java S3 in
all 100 cases. Both modes also pass 400 DeadCode S2/S3 regressions.
[Scheduled pass evidence](../evidence/multiset-optimizer-prefix.json) ·
[Replay evidence](../evidence/multiset-distinct-replay.json)

A directed SQL matrix covers 96 UNION and EXCEPT programs in both engine modes
with set and bag outputs. The test compiler replaces its original
`MergeSums` pass with Rust and retains the live Java compiler context. All
384 final comparisons match uninterrupted Java, and 92 pass invocations
change their graph. An independent Rust run against each captured Java pass
input also matches all 384 output graphs. In the early S2 control, matching
Rust and Java rewrites both change the final result on eight programs in all
four profiles. This restricts the replay result to the scheduled position.
[MergeSums replay evidence](../evidence/multiset-merge-scheduled-replay.json)

A combined run replaces nine placements in the Java schedule on the 146
programs from both selections, with set outputs in each engine mode. The
same-input check matches Java on all 2,628 pass graphs, including 952
changed graphs. The 292 hybrid results and 292 separate Java codec controls
match uninterrupted S3. A Rust rewrite of each hybrid S3 also matches after
resumption in a fresh Java process. All 292 results also match when the hybrid
resumes a Rust-written S2 before writing S3. Directed cleanup fixtures supply
literal filters that the 146 SQL inputs do not retain at that placement.
[Combined scheduled and detached replay](../evidence/multiset-nine-scheduled-placements.json)

Rust also replaces the late `RemoveViewOperators(all=true)` after the compiler
has consumed view metadata. It changes all 292 graphs at that placement. The
ten-placement run matches all 2,920 exact-input graphs and all 292 final
compiler results.
[Ten scheduled placements](../evidence/multiset-ten-scheduled-placements.json)

The constant-propagation port adds snapshot negation, literal and projected
filters, scalar comparisons, and identity, literal and single-column maps.
The expanded selection triggers 18 constant rewrites on real SQL. The
fifteen-placement run includes all five top-level pruning transforms and
matches 5,100 exact-input graphs, 340 final compiler outputs and 48 detached
S2-to-S3 chains. Directed fixtures cover the additional closure and
multiplicity cases. Other nonempty constant closures require further scalar
expression support.
[Constant propagation and fifteen placements](../evidence/multiset-fifteen-placements.json)

Checked signed 32-bit addition, subtraction and multiplication also fold in
constant maps and predicates. The evaluator handles overflow, retained
operators, and the selected algebraic identities. The encoder keeps
literal type and source-position identities through chained maps.
[Arithmetic fixtures and live replay](../evidence/multiset-constant-arithmetic.json)

The signed 8-bit extension matches Java on a directed map, predicate and
overflow case. Both ordinary output modes pass all 600 Java-tail comparisons
on the extended build.
[Signed 8-bit evidence](../evidence/multiset-small-integer.json)

The 30-program follow-up adds deindex conversion and both repeated
noop-removal sites. It matches all 1,100 pass graphs and 60 final compiler
results. At each repeated site the adapter preserves the live Java graph on
an unchanged result. The Java repeat driver takes the same iteration count
in all cases, including 20 runs with a second iteration. The original directed
lateness fixture checks noop removal before sinks. The corrected fixture in the
identity-map run uses set outputs to exercise preservation before retention.
[Eighteen scheduled placements](../evidence/multiset-eighteen-placements.json)

The 36-program follow-up adds system error-table and schema removal, ordered
constant-view warnings, compact stream naming and the integration invariant.
Across both engines, 1,610 same-input graphs, 72 final compiler results and
ten detached S2-to-S3 chains match Java. The warning comparison preserves text,
source ranges and order in the live compiler.
[22 scheduled placements](../evidence/multiset-twenty-two-placements.json)

The 23rd placement inserts weight validators for unkeyed tables. Its Java
wrapper retains the existing SQL option condition. Seven fixtures per engine
cover on, explicit off and unset settings, keyed and unkeyed tables, and a
quoted identifier. Their detached S2-to-S3 chains match direct Java output.
The expanded 43-program matrix matches 1,928 pass graphs and 86 final
compiler outputs across both engines.
[Weight-validator replay](../evidence/multiset-weight-validation.json)

The 24th placement removes identity projections. Retention conflicts keep a
noop whose closure matches Java's generated binding and type references.
The Java adapter restores the generated-name counter. All 2,014
exact-input comparisons and 12 detached chains agree. The corrected
lateness fixture explicitly requires a retention operator.
[Identity-map replay](../evidence/multiset-identity-maps.json)

Nullable constant filters distinguish SQL NULL from an expression retained
for runtime. Directed tests cover both outcomes and live tests fold null
tests and wrapped comparisons in each engine.
[Nullable filter evidence](../evidence/multiset-nullable-predicates.json)

## Structure and limits

- `model.rs` defines operator ports, expression arena ids, and the type shapes
  needed by analysis. The typed model is an analysis projection of the wire IR;
  it does not claim to type-check or interpret every carried expression.
- `analysis.rs` computes facts without JSON, I/O, compiler state, or saved
  annotations. A map is certainly injective only if every required parameter
  field survives. The rules preserve Java's conservative treatment of constants,
  lossless casts, and the integral of delta streams.
- `prune.rs` computes inverse reachability over a topologically ordered graph.
  It retains sinks and trace-retention roots and can retain all table sources.
  At the first scheduled pruning pass, Java still reports unused-input
  warnings and Rust computes the retained graph. The decoder rejects nested
  circuits before transformation.
- `empty.rs`, `filters.rs` and `integrate.rs` plan empty-source propagation,
  literal filter removal, and cancellation of redundant stream conversions.
  They distinguish snapshots from deltas when constructing constants.
- `scalar.rs` evaluates the supported scalar operands and integer operations
  independently of the JSON encoder.
- `bypass.rs` resolves view and noop aliases in topological order. Each input reaches
  its retained producer and keeps the producer's output port.
- `sums.rs` performs ordered cancellation, inlines nested sums, and bypasses
  double negation on typed ports. It limits the total number of rewritten edges.
- `decode.rs` validates identities and ports, resolves the separate inner-node
  namespace, and builds the analysis model. The encoder retains other fields
  and relocates shared inner definitions when their first owner is removed.
  JSON field order is preserved for Java's definition-before-reference decoder.
- `provenance.rs` preserves and relocates origin and relation definitions in
  separate identity namespaces. It bounds reference expansion after pruning.
- `main.rs` owns files, record limits, and comparison reports. Expected facts
  and expected transformed graphs are used only by comparison code.

Unsupported operators, types needed by lineage, and lineage expressions fail
the entire run. They are never excluded from the 50-program denominator.
Known expressions with no recoverable field lineage are represented as unknown;
that is an analysis result, not an unsupported-node fallback. Other carried
expressions and metadata remain in the wire graph and are checked by the
structural comparator and Java decoder.

The reader rejects duplicate JSON keys and duplicate graph definitions. It caps
records at 64 MiB before reading further, graph-following nesting at 128,
expanded type nodes at 100,000 per circuit, and decoded expression nodes at
100,000 per closure. The experiment runner has a 512 MiB sampled output budget,
a 512 MiB free-space floor, 1 GiB Java heaps, and five-minute subprocess deadlines.
These are experiment guards, not a production RSS or performance budget.

## Reproduction

Build Java's test classes and dependency JAR first. From the repository root:

```sh
cargo test --locked --manifest-path crates/sql/multiset-probe/Cargo.toml
cargo clippy --locked --manifest-path crates/sql/multiset-probe/Cargo.toml --all-targets -- -D warnings
cargo build --locked --manifest-path crates/sql/multiset-probe/Cargo.toml
python3 crates/sql/scripts/run_multiset_probe.py $SCRATCH/multiset-run \
  --classpath "$JAVA_TARGET/test-classes:$JAVA_TARGET/classes:$JAVA_TARGET/sql2dbsp-jar-with-dependencies.jar" \
  --probe crates/sql/multiset-probe/target/debug/feldera-multiset-probe \
  --producer "$COMPILER_REVISION" --runtime "$RUNTIME_REVISION"
```

If `CARGO_TARGET_DIR` is set, use its binary path. The output directory must not
exist. Repeat with `--sets` and a separate directory to exercise both output
distinct replays against direct set-mode S3. Preserve failed directories and
choose a new one for each rerun. The runner
records input, source, executable, and classpath digests and rejects changes
during execution. The Java workflow runs this experiment and retains its files.
Local validation does not imply that the remote workflow has run.

For scheduled replay, pass
`-Dfeldera.probe.scheduledRunner=/absolute/path/to/feldera-multiset-probe` and
`-Dfeldera.probe.scheduledPasses=EnsureDistinctOutputs,MergeSums` to
`MultisetProbe` while compiling direct S3 or resuming S2 with set outputs.
Use `java` as the runner for the Java codec control. Select a numbered placement
with a suffix such as `PropagateEmptySources#2`. Each placement saves before and after
graphs. The evidence archives include the fixture inputs and exact run scripts.

Set `-Dfeldera.probe.reuseRustWorker=true` to reuse one Rust process across the
replay JVM. The probe sends the pass name and existing input and output paths.
It acknowledges each result after writing it. The Java driver enforces the
same per-request deadline and closes the worker when the run completes.
[Worker comparison](../evidence/reused-optimizer-worker.json)

Select `ValidateRecursiveOperators#1,ValidateRecursiveOperators#2` to run both
recursive legality checks. These two visitors accept nested graphs and report
the first invalid operator ID to the live Java compiler. The other rewrites in
the combined matrix still use the flat operator subset.
[Recursive validation evidence](../evidence/recursive-optimizer-validation.json)

Select `NoConnectorMetadata` to validate calls in circuit declarations and
operator expressions. The check permits calls in column defaults and
reports the first forbidden call with its serialized source position.
[Connector metadata evidence](../evidence/connector-metadata-optimizer.json)

Select `DeterministicFunctions` and `CheckHints` to replace the two
diagnostic-producing visitors. Rust returns affected declaration, input
column, and join annotation identities. The adapter reports through the
compiler's diagnostic API and uses the original source ranges. Set
`-Dfeldera.probe.collectErrors=true` when a negative fixture needs to
exercise every diagnostic in the pass. The default stops at the first error.
Replay JSON carries join-hint spans. Ordinary graph exports keep the
existing join annotation fields.

Select `TagRegions` to compute conflicting global-aggregate region IDs.
The Java adapter removes those tags from the live operators in place,
retaining node identity and the remaining annotation order.

Select `StrayGC` to validate trace-retention modes, duplicate retention
conditions, and trace-owning consumers. The input includes the circuit and
Java's type capabilities for each operator. Rust determines the first violation
from the graph. The Java adapter formats the error with the live operators.

The scheduled `NoIntegralVisitor` check supports nested components. It
reports the first integration operator to Java for diagnostic construction.

Select `PushDifferentialsUp` to move a differentiation ahead of a map or
map-index with a single consumer. The surrounding Java repeat retains graph
construction and dead-code elimination.

Select `BalancedJoins`, `LinearPostprocessRetainKeys` or
`CloneOperatorsWithFanout` to run the Rust graph decisions for those passes.
The Java adapter materializes the selected join, aggregate and clone objects.
The linear-retention request includes the SQL input and output order, which
Rust preserves when it sorts the graph after adding waterline dependencies.

## Attribution and remaining work

The reference is this repository's Java compiler at `c6ede756d`, with exact
working-source digests in the run certificate. The analysis follows
`keys/MultisetAnalysis.java`, `keys/LosslessCastLineage.java`, and the relevant
parts of `keys/KeyAnalysis.java` and `Lineage.java`. The dead-code pass follows
`FindDeadCode.java` and the noop closure codec follows `DBSPNoopOperator.java`.
The crate uses the MIT licence. The 2022 and 2023 VMware ports include
distinct optimization, incremental optimization, generated closures, and
reachability. Distribute the copyright and permission notices with copies
of the ported code. [VMware MIT notice](LICENSE-VMWARE)
No Calcite implementation was translated for this crate.

The remaining optimizer prefix, a complete typed lossless IR subset and
independent maintainer review remain open. See the [P0 checklist](../P0.md#exit-checklist).
