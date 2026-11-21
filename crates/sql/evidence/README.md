# P0 measurements, 2026-11-12 to 2026-11-16

These files record measurements and inventories.
See [P0.md](../P0.md) for revisions, environment, interpretation and remaining work.

The published records use `$REPO` for the worktree, `$SCRATCH` for the
capture temporary directory, `$M2` for the Maven repository, and
`$CARGO_HOME` for the Cargo home directory. Set these paths for local
replay. Each archive under `.p0-artifacts/` is available from the plan owner
on request. Put it at the recorded path and check its SHA-256 digest.
The archives retain their original bytes and paths. The fragment checker
compares each published certificate with its archived copy after path
normalization. To publish a new record, run
`python3 crates/sql/scripts/normalize_evidence.py FILE`.

- `c0-oracle.json` verifies the structured diagnostic comparison against
  controlled wording and end-position changes in the 84,224-probe Java batch.
- `a-binder-c0.json` records structured C0 equality from the real Rust binder
  for 59,264 generated probes, including its errors and warnings.
- `optimizer-stop-on-error.json` records the Rust guard at its original
  placement across both engines and four directed compiler statuses.
- `corpus-zsets.json` records typed value validation and signed bag
  comparisons over both runtime corpus exports.

`type-derivation.json` records two Rust strategies matching all 42,880 scalar
function probes and additional rules matching 41,344 other probes on acceptance
and result type. The 106-file verified archive preserves all attempts and the
historical Java oracle's provenance limitation. The separate cast, function
and value checks cover the broader Spike 5 scope.
See [TYPE-DERIVATION.md](../TYPE-DERIVATION.md).

`type-binding-predictions.json` adds exact validated-SQL operator/type/cast-chain
comparison for all 42,880 function records against the newly frozen Java capture.
It records the COALESCE operand correction, shared encoder, 28 Rust/28 Python
tests and verified 77-file archive. This precedes S2 and does not certify runtime
values or complete C9.

`parser-error-positions.json` records improved error starts, the full negative
test capture, eight over-acceptance repairs and 17 added adversarial cases.
Corpus/keyword coverage is unchanged. Zero measured Rust-only acceptances does
not establish exhaustive rejection or S1 equivalence. The record retains the
missing positions, conservative rejections, failed attempts and source hashes.

`fold-split-inventory.json` pins the sources behind the extraction work packages
in `fold-probe/SPLIT.md`. Its 25–44 engineer-day estimate is explicitly
provisional, not measured labor or an approved rebaseline.

`parser-remaining-positions.json` records the remaining LATENESS/join location
repairs and full population preservation. It supplements the prior position
certificate; column locations are not complete attribute spans.

`gen2-ci-runner.json` records 2,218/2,868 passes from the current compiler's full
runtime suite, with identical test identities to its compile/export baseline.
The result remains below 80%. CI wiring is local and linted, not remotely run.

`let-type-rewrite.json` records the let-binding unboxing repair: four regressions
pass, two previously crashing Gen-2 runtime tests pass, and two now reach the
engine's unsupported waterline operator. Full classic compilation passes 2,853
tests with 71 skips; the strict reader validates 8,571 exported cases. Raw
before/after reports and source digests are retained. Runtime failure counts
from earlier full runs remain historical measurements.

`gen2-runtime-let-rewrite.json` refreshes the complete runtime suite after that
repair: 2,203 passes of 2,853 tests, 69 failures, 510 errors and 71 skips. Two
prior failures become passes, four new tests pass, and no passing test regresses.
Both pass-share denominators remain below 80%.

`fold-temporal-rounding-s3.json` adds 1,728 actual-runtime calendar vectors,
bringing the passing runtime population to 2,347. The full corpus preserves
every verdict and schema but retains 196 diagnostic and six exact math-value
differences. The 860 focused compilations include 48 diagnostic mismatches.
Raw artifacts remain in `$SCRATCH/p0-fold-final-s3-v13`; this is a failing gate.

`parser-reserved-identifiers.json` records the corrected reserved-word adapter:
zero Rust-only acceptances over 13,660 keyword and 185 adversarial probes, with
unchanged full-corpus coverage (516 rejections). It pins the grammar drift
check and retains the failed broad-check run that rejected UNNEST and SET ON.
The final raw results are in `$SCRATCH/p0-parser-reserved-v3`.

`native-normalized-keys.json` records 20,736 nullable BIGINT key comparisons,
sort/consolidation and bounds tests. Eleven native tests and Clippy pass; the
actual-runtime certificate is refreshed from `$SCRATCH/p0-native-runtime-v6`.
The key experiment does not measure performance or establish archive/checkpoint
compatibility.

`fold-probe.json` records the full level-2 capture, 8,571 exact compilation
comparisons, 240 focused SQL/policy comparisons, and 37 actual-runtime vectors.
Its 11,240 comparable corpus values match; refusals and nonliteral Java results
remain separate. Raw evidence is in `$SCRATCH/p0-fold-final-v1`.
`fold-cross-builds.json` records Windows MSVC and wasm32 rlib build identities;
neither target was executed. `fold-null-failures.json` preserves the original
NULL-offset/window-bound failures, now repaired without an allow-list.
`corpus-validation-folds.json` records strict validation after those repairs.

`connector-probe.json` records 6,544 direct and 26,176 SQL/policy comparisons,
all passing the diagnostic contract with zero internal failures. Message text
differences remain counted and unreviewed. Raw evidence is in
`$SCRATCH/p0-connector-v4`; `connector-null-failures.json` preserves the original
12 null-pointer reproductions. The certificate also records source/executable
digests and Java/runtime field drift checks.

- `optimizer-census.json`: per-step and optimization-level HEP candidates,
  attempted matches, productions, incomplete steps, and correlate counts.
  Candidates that never fire still need targeted tests and a port disposition.
- `rule-implementations.json`: ordered runtime rule instances for three plan
  shapes at each optimization level, including conditional registrations.
- `rule-port-inventory.json`: all 80 descriptions reconciled with 71 classes and
  pinned source digests; retains every rule in the proposed scope. Review is open.
- `validator-operators.json`: the actual initial validator table, with operator
  identity matches to STANDARD/spatial tables and exact documentation-name matches.
  Overloads are separate entries. Lowering support is not inferred from membership.
- `gen2-runtime.json`: the local full-suite result against the native pinned forge
  build, with failure messages grouped after replacing allocation-specific node IDs.
  The 80% threshold is not met, with or without skipped tests in the denominator.
- `gen2-compile-export.json`: the full Gen-2 compile/export run with effective
  compilation options, including every JUnit failure and error. It does not
  execute the engine and is not a passing-suite result.
- `gen2-effective-corpus-validation.json`: strict validation of the resulting
  8,527 exported cases and 17 registered skips.
- `gen2-effective-replay.json`: three captures and a baseline for a labeled
  seven-program Gen-2 subset, including binary/input digests. All comparisons
  pass; the subset does not certify full-corpus determinism.
- `type-probe-result.json`: the reviewed scalar matrix's completion counts.
  `oracle_ready: false` is deliberate: compiler bugs are not valid negative goldens.
- `type-probe-corrected.json`: the full matrix after the coercion repairs, with
  zero internal failures and strict graph validation. Full runtime coverage of
  the matrix is still absent.
- `type-probe-revision-comparison.json`: comparison against the frozen pre-fix
  compiler without JVM assertions (production behavior). Every previously
  accepted probe and ordinary rejection is unchanged; 1,224 set-operation
  compiler bugs become acceptances. Both batches isolate generated variable names.
- `type-s2-complete.json`: one comparison of all 84,224 generated inputs.
  All 60,840 accepted inputs match exact compact S2 expression and type text
  within the scalar, set-map/filter and aggregate-component capture scopes.
- `type-values.json`: nine DBSP circuits compared with independent Rust bag
  predictions over 36 transactions with duplicates, NULLs and retractions.
- `java-baseline-archive.json`: verified standalone Java timings and artifacts
  over 1,152 cold and warm compilations in classic and Gen-2 modes.
- `java-pipeline-baseline.json`: incremental artifact and dataflow equality over
  1,152 cold and warm runs, plus measured concurrency and cancellation pilots.
- `resource-calibration.json`: deterministic parser, A binder and decoder
  input pilot with worker resource measurements and the isolated 0.62 timeout.
- `native-mixed-keys.json`: exact runtime-type order comparisons and measured
  encode, sort and merge costs for six nullable mixed-key workloads.
- `native-selected-rows.json`: verified native archive bytes and relocated
  runtime decoding for all 29 explicit stored shapes in the frozen selection.
- `native-corpus-index-keys.json`: paired sort and merge measurements for the
  five explicit index-key types in the frozen ten programs.
- `multiset-optimizer-prefix.json` records 300 matching optimizer pass
  applications from the actual Java schedule and 200 separate Java-tail
  regressions through the existing DeadCode seam.
- `multiset-distinct-replay.json` records Rust-written S2 output-distinct
  replay across 100 cases, with an independent Java rewrite control and exact
  tail artifact and persistent-identity comparisons in both engine modes.
- `multiset-merge-scheduled-replay.json` records Rust running at the scheduled
  Java MergeSums position across 384 cases. An early S2 control changes the
  uninterrupted Java result on 32 comparisons.
- `multiset-nine-scheduled-placements.json` records nine scheduled Rust
  replacements with 2,628 matching graph comparisons, 292 matching final
  hybrid results, 292 matching Rust-written S3 imports, and 292 matching
  detached chains through both S2 and S3.
- `multiset-ten-scheduled-placements.json` adds the late all-views removal to
  the in-process run. It records 2,920 exact-input graph comparisons and
  292 matching final compiler results.
- `multiset-fifteen-placements.json` adds constant propagation and the four
  remaining top-level pruning transforms. It records 5,100 matching graphs,
  340 matching hybrid compiler runs and 48 matching detached SQL chains.
- `multiset-sixteen-placements.json` checks deindex removal with the other
  fifteen Rust placements on 30 programs in both modes.
- `multiset-eighteen-placements.json` checks both repeated noop-removal
  sites, including 20 compiler runs that perform a second iteration.
- `multiset-constant-arithmetic.json` records signed arithmetic and overflow
  parity on directed graphs and live output sinks in both engine modes.
- `multiset-small-integer.json` adds directed signed 8-bit folding and verifies
  both ordinary regressions on the updated Rust build.
- `multiset-twenty-two-placements.json` records error-table and schema removal,
  constant-view diagnostics, compact naming and integral validation alongside
  the earlier placements. It includes 1,610 graph comparisons and ten detached
  S2-to-S3 chains.
- `multiset-weight-validation.json` verifies the SQL-controlled weight-validator
  insertion as the 23rd placement, alone and in the combined scheduler.
- `multiset-identity-maps.json` records the 24-placement run, including
  generated-name state and directed retention conflicts in both collection kinds.
- `multiset-nullable-predicates.json` adds null tests, boolean negation and
  wrapped comparisons, including four live folds in the expanded 45-case matrix.
- `scheduled-optimizer-benchmark.json` measures the current traced Rust
  replay against direct Java and a Java codec control in a warm JVM.
- `reused-optimizer-worker.json` adds release-build process reuse, exact
  comparisons and timeout cleanup evidence.
- `recursive-optimizer-validation.json` verifies both scheduled legality
  checks, nested S2 imports and recursive LAG and OVER diagnostics.
- `connector-metadata-optimizer.json` verifies the executable-call check, the
  column-default exception and the full 27-placement optimizer replay.
- `optimizer-diagnostics.json` verifies nondeterministic defaults and UDFs,
  join-hint conflicts and detached hint positions, and the full 29-placement run.
- `optimizer-regions.json` verifies aggregate-region conflict detection and the
  full 30-placement optimizer replay.
- `optimizer-trace-validation.json` verifies trace-retention contracts and
  the 31-placement optimizer replay.
- `optimizer-differentials.json` verifies differential movement, repeat
  convergence, nested integral diagnostics and the 32-placement run.
- `optimizer-balanced-joins.json` verifies balancing and retention exclusions
  in the 33-placement run.
- `optimizer-linear-retention.json` verifies waterline scheduling and six
  active SQL fusions alongside the 34-placement run.
- `optimizer-fanout-clones.json` verifies the cost decisions for fanout
  cloning and the 35-placement run.
- `decoder-port-validation.json` verifies structural port checks, the
  700-case resource calibration and 13,776 archived optimizer observations.
- `d1-heldout-baseline.json` records the shared split and the archived A/H
  binders' measured reach across the original compiler corpus.
- `native-range-execution.json`: the first frozen case executed from its
  exported graph with a native predicate and a passing classic reference.
- `native-unicode-execution.json`: both frozen Unicode cases executed from
  their exported graphs and matched their classic-runtime reference.
- `native-outer-join-execution.json` records both stateful outer joins,
  including six frozen steps, supplemental retractions and independent classic
  Rust results. The cumulative ledger has four verified programs.
- `native-aggregate-execution.json` records both global aggregates, including
  six frozen steps, seven supplemental steps and independent classic Rust
  results. The cumulative ledger has six verified programs.
- `native-rolling-execution.json` records the three decimal RANGE COUNT steps,
  four late-arrival and NULL updates, and the original classic Rust test.
- `native-implicit-archives.json` verifies fixed-width internal payloads of
  the selected linear aggregates and rolling count against rkyv.
- `native-aggregate-checkpoints.json` records bidirectional checkpoint
  interchange for the original generated SUM and COUNT DISTINCT graphs.
  The archive preserves both on-disk checkpoint formats and the runnable
  scratch package.
- `native-c13-optimized.json` records paired release-profile throughput,
  latency, RSS and spill footprint, plus warm shared-library rebuild times.
- `native-count-benchmark.json` measures the frozen COUNT DISTINCT circuit
  across 124 paired classic and native samples with forced spill.
- `native-decimal-join-execution.json` records both Postgres arithmetic
  cases, 200 paired decimal results and supplemental inequality checks.
- `native-json-execution.json` records the nested-row JSON case, both views,
  and the callback ABI tests. The cumulative ledger covers all ten cases.
- `native-corpus-final-archive.json` freezes a combined rerun of all ten
  programs against one source revision.
- `native-corpus-full-steps-archive.json` retains the combined rerun with
  all whole-relation graphs set to the same full-step scheduling as classic.
- `native-connector-execution.json` records the production JSON parser and
  encoder replay and byte-identical output.
- `native-rolling-checkpoint.json` records the selected rolling program's
  classic and native checkpoints, equal fingerprints, and restored execution.
- `native-runtime-benchmark.json` records 124 matched runtime samples and
  15 warmed-dependency rustc shared-library rebuilds for the Unicode projection.
- `native-current-regressions-archive.json` retains 47 DBSP tests, including
  the ten exact corpus cases in the full-step archive, 17 native-kernel tests,
  both Clippy checks, and 260 Python tests.
- `native-final-regressions.json`: the devcontainer run before the outer joins passed 39
  runtime tests, 15 native-probe tests, and all 256 Python tests.
- `corpus-validation.json`: strict validation counts for all exported cases from
  the initial passing compile-and-export run.
- `corpus-validation-coercion.json`: the passing export after the coercion fixes
  and new runtime fixtures.
- `corpus-validation-ordered-asof.json`: the passing compile/export suite after
  deterministic ASOF GC scheduling; runtime execution is not certified.
- `corpus-validation-point-json.json`: strict validation after the point dataflow
  export fix and parser-oracle tests; all 8,571 registered cases are exported.
- `point-dataflow-export.json`: the failing Gen-2 replay case, serializer repair,
  failing-before/passing-after regressions, and full compile/export result.
- `asof-ordering.json`: the full capture-versus-baseline differences, raw S4
  diagnosis, and regression result. Interrupted runs are explicitly incomplete.
- `parameter-allocation-order.json`: Gen-2 differences traced to replacement
  type allocation order, with all 411 affected cases and regression evidence.
- `parameter-insertion-order-replay.json`: completed 411-case intermediate
  experiment; `equal: false` is intentional. Insertion order alone is insufficient.
- `classic-determinism.json`: the completed repaired run. All 8,571 cases match
  across three captures and a baseline in 24 JVMs, with 3,503,220 observations
  per capture. It includes per-shard comparisons and build/input digests.
- `gen2-capture-transparency.json`: all 8,527 exported cases match their
  capture-disabled baselines after the allocation repair, with 3,527,714
  observations. Effective options select classic for one case. Three-capture
  repeatability is recorded separately in `gen2-determinism.json`.
- `gen2-replay-exclusions.json`: every registered skip omitted from the exported
  Gen-2 replay population. This inventory does not approve any disposition.
- `s2-prefix-replay.json` and `s2-corpus-replay.json`: earlier version-1 S2
  experiments, covering 14 focused cases and the first 50 classic CastTests cases.
- `s2-provenance-replay.json`: version-2 S2 full-prefix comparisons for eight
  focused programs in both modes, with streaming/correlation diagnostics,
  plan/dataflow output, 48 JVMs and 6,174 observations.
- `s3-tail-replay.json`: 16 final-graph comparisons through the S3 file boundary
  in 48 JVMs; exact applicable artifacts and semantic plan/dataflow equality.
- `s2-dataflow-position-failure.json`: completed wider S2 run before restoring
  unused-table identifier positions. Classic passes all 167 CastTests cases;
  Gen-2 dataflow differences remain visible. This is a failing certificate.
- `corpus-validation-seams.json`: strict validation of all 8,571 classic exported
  cases after the S2/S3 and provenance changes; runtime execution is not certified.
- `multiset-probe.json`: 50 fixed programs, 200 Rust/Java analysis and dead-code
  comparisons, and 200 final-result comparisons after Java resumes Rust-written
  S2/S3 files. The remaining optimizer runs in Java; generated programs are not executed.
- `multiset-probe-work.json`: scope, defects, and agent elapsed time. Human review
  and engineering-hour measurements remain unrecorded, so no productivity gain is certified.
- `parser-corpus.json`: both pinned Rust parser versions against Java's raw
  parser on all 8,571 cases. Counts distinguish cases, submissions, and statements.
- `parser-keywords.json`: 13,660 quoted/unquoted identifier probes over 1,366
  keywords in five contexts, with pinned source digests and disagreement counts.
- `parser-syntax.json`: acceptance disagreements on 39 focused syntax probes.
- `parser-semantic-counterexamples.json`: four forms silently misparsed by both
  Rust versions, retaining Java and Rust renderings. Full S1 equivalence is open.
- `calcite-attribution.json`: source-header inventory, three attribution repairs,
  pinned notice/licence resource digests, and Maven resource-copy verification.
- `parser-dependency-licenses.json`: declared licences and file digests for all
  25 packages in the parser experiment. Independent legal review remains open.
- `diagnostic-census.json`: evaluated Java expectations, Python literal
  expectations, and Calcite resource candidates. Ambiguities remain visible;
  the exact platform error assertion is recorded separately. This is a review
  inventory, not a certified count of message templates.

The complete raw artifacts remain in `feldera-compiler-devcontainer`:

| Artifact | Container path |
|---|---|
| Final classic corpus and manifest validation | `$SCRATCH/p0-corpus-final`, `$SCRATCH/p0-corpus-final-summary.json` |
| Final optimizer journal | `$SCRATCH/p0-optimizer-verified.jsonl` |
| Final three-JVM capture comparison | `$SCRATCH/capture-final-validation` |
| Gen-2 JUnit reports and per-test failure list | `$SCRATCH/p0-gen2-surefire-reports`, `$SCRATCH/p0-gen2-summary.json` |
| Reviewed scalar probe input | `$SCRATCH/p0-type-probes-final.jsonl` |
| Probe results, including compiler-bug cases | `$SCRATCH/p0-type-goldens-counted` |
| Final classic JUnit reports | `$SCRATCH/p0-classic-surefire-reports` |
| Corrected scalar probe results | `$SCRATCH/p0-type-goldens-stable` |
| Frozen production-mode reference with isolated names | `$SCRATCH/p0-type-goldens-old-stable` |
| Corpus after coercion repairs | `$SCRATCH/p0-corpus-coercion-verified` |
| Java negative-test diagnostics and pinned message resources | `$SCRATCH/p0-diagnostics` |
| Classic set-operation/coercion runtime reports | `$SCRATCH/p0-coercion-runtime-reports` |
| Packaged compiler with coercion fixes | `$SCRATCH/p0-coercion-compiler.jar` |
| Classic corpus after ASOF scheduling repair | `$SCRATCH/p0-corpus-ordered-asof` |
| Full classic repeated capture after ASOF repair | `$SCRATCH/p0-determinism-v5` |
| Gen-2 corpus with effective compilation options | `$SCRATCH/p0-corpus-gen2-effective` |
| Gen-2 compile/export JUnit reports | `$SCRATCH/p0-gen2-effective-reports` |
| Gen-2 seven-program replay | `$SCRATCH/p0-gen2-effective-replay` |
| Passing full Gen-2 exported-population replay | `$SCRATCH/p0-gen2-full-determinism-v4` |
| Focused version-2 S2 and S3 captures | `$SCRATCH/p0-s2-prefix-final`, `$SCRATCH/p0-s3-prefix-final` |
| Earlier S2 corpus results and failing dataflow reproduction | `$SCRATCH/p0-s2-provenance-corpus-v4`, `$SCRATCH/p0-dataflow-diagnosis` |
| Rust analysis, transformation and Java-tail experiment | `$SCRATCH/p0-multiset-v6` |
| Frozen Rust probe and Java oracle | `$SCRATCH/p0-multiset-tools-v6/probe`, `$SCRATCH/p0-multiset-oracle-v4` |
| Final compile/export suite for seam changes | `$SCRATCH/p0-corpus-seams-final` |
| Rust parser corpus measurements | `$SCRATCH/p0-parser-corpus-v2` |
| Java raw-parser corpus measurements | `$SCRATCH/p0-parser-java-corpus-v2` |
| Focused Rust syntax probes, with ASTs | `$SCRATCH/p0-parser-syntax-v2` |
| Focused Java syntax probes | `$SCRATCH/p0-parser-java-syntax-v2` |
| Keyword Rust and Java probes | `$SCRATCH/p0-parser-keywords-rust`, `$SCRATCH/p0-parser-keywords-java` |
| Full keyword disagreement groups | `$SCRATCH/p0-parser-keyword-comparison.json` |

The probe generator, corpus validator and optimizer summarizer are reproducible
repository tools; their commands are in `sql-difftest/README.md`. CI retains corpus
exports, the full scalar probe results, and diagnostic inventories for seven days.
The workflow has been linted locally; no GitHub CI run was triggered in this session.

The packaged compiler's SHA-256 is
`cd839e531576cf67babe48c92a64f45e45419f222865237ae73540ff7a0c4cb9`.
The original Boolean/REAL reproduction succeeds against it with JVM assertions
enabled. The frozen pre-fix JAR remains at `$SCRATCH/p0-frozen-compiler.jar`; its digest
is recorded in the probe revision comparison.

`gen2-determinism.json` records the successful three-capture/baseline comparison
over 8,527 exported cases in 16 fresh JVMs (3,527,714 observations per capture).
Effective options select Gen-2 for 8,526 cases and classic for one. Original-corpus
failures and the 17 registered skips remain outside this certificate.

`s2-corpus-classic.json` and `s2-corpus-gen2.json` certify the final 167-case
S2 replays from each exported CastTests population (67,509 and 68,505
observations). Raw captures are in `$SCRATCH/p0-s2-corpus-classic-final` and
`$SCRATCH/p0-s2-corpus-gen2-final`. The Gen-2 population retains one classic case.
Gen-2 graph ids are canonicalized under the explicit seam contract; all
166 raw artifact differences remain recorded as metrics. Classic Rust, schemas,
diagnostics, hash inputs, and persistent ids match. These runs compile programs;
they do not execute their expected Z-sets.

`seam-form-coverage.json` records the full 8,571-case inventory and the seven S2
and 14 S3 cases selected to cover its top-level forms. Strict replay passes 4,025
S2 prefix observations and 14 S3 final graphs. Each result retains its own frozen
compiler identity; these are form-coverage certificates, not full-corpus results.

`seam-decoder-regressions.json` retains the failures that led to decoder and
compiler-state repairs, the final 45-test targeted result, the preceding full
2,841-test Java compile/export result, and 200 passing Rust-written seam/Java-tail
comparisons. `corpus-validation-seam-decoders.json` validates that full Java
run's 8,571 exports. Its scope predates the final function-decoder repair, which
the targeted suite covers. Full boundary audits are retained separately under
`$SCRATCH/p0-s2-full-v4` and `$SCRATCH/p0-s3-full-v2`; they are not yet passing certificates.

`closure-sort-lateness-regressions.json` records the subsequent closure/sort
sharing repairs and NULL timestamp LATENESS validation. Its full Java run passes
2,844 tests with zero failures/errors and 71 skips; strict export validation is
in `corpus-validation-final-seams.json`. The complete S3 audit passes all 8,571
cases in `s3-full-corpus.json`; `s3-full-sharing-failures.json` retains the 47
original differences. The subsequent S2 audit found a pending-warning timing
defect, recorded and repaired in `s2-post-compilation-checks.json`. Version-3
full audits were interrupted by disk exhaustion; their fresh reruns are in
`$SCRATCH/p0-s2-full-v7` and `$SCRATCH/p0-s3-full-v5`. See
`validation-disk-interruption.json`. The complete Java compile/export rerun
passes 2,845 tests in `s2-post-checks-full-java.json`.

`fold-metadata-s2.json` records 8,571 corpus and 476 focused comparisons through
S2, including 236 added metadata cases. Its stage annotation comes from the
frozen source; the earlier runner did not emit that field. The new runner records
the stage in every result and can include final optimizer validation. The
full S3 experiment fails with 1,340 diagnostic differences, retained in
`fold-final-stage-failures.json`. The expanded registry's full rerun in
`fold-expanded-s3.json` reduces that count to 538; all verdicts and schemas
match, alongside 504 focused cases, 109 runtime vectors and 12,206 comparable
values. `fold-expanded-cross-builds.json` records Windows/wasm library builds,
without target execution. Neither certificate approves a phase exit.

`s4-composite-state.json` pins the sources inspected for `S4-STATE.md`. It records
composite wiring scope, not independent review or checkpoint certification.

`s2-full-before-repairs.json` preserves the completed earlier full-prefix audit
with 19 differences. `multiset-seam-v3.json` records the repaired protocol's
200 analysis/graph and 200 Java-tail comparisons, all passing. The source audit
in `frel-source-inventory.json` supports `FREL.md`; it is not S1 replay evidence.

`fold-extract-s3.json` reduces the remaining diagnostic differences to 424.
`fold-math-s3.json` records the subsequent completed audit: 276 diagnostic
and six exact math-value differences, 504 matching focused cases, and 370
passing runtime vectors. `fold-math-java-values.json` isolates the six one-bit
Java/runtime differences. `fold-float-normalization-failures.json` retains the
13 negative-zero failures repaired before `fold-runtime-math.json`. None of
these results approves a deviation or closes spike 7.

`s3-full-v3-protocol.json` records all 8,571 passing classic S3 replays on
protocol version 3. Full optimizer-prefix coverage and runtime execution are
separate. `fold-uuid-runtime.json` and `fold-uuid-s3.json` add UUID parsing,
casts and unsigned comparisons: 386 runtime vectors and 520 focused cases pass;
274 diagnostic and six exact math-value differences remain in the full corpus.
`rel-model-types.json` pins the strict type/source model fragment and its nine
tests; it is not a complete S1 contract or importer certificate.

`s3-gen2-full-v3-protocol.json` records complete version-3 S3 replay for the
8,527-case exported population: eight fresh JVMs, zero strict differences.
Gen-2 artifact bytes differ for 8,526 cases under the recorded seam contract;
those metrics are not runtime C7 results. Original-suite exclusions remain open.

`parser-repairs.json` links the complete repaired corpus and syntax comparisons,
32 additional adversarial probes, AST evidence, coverage deltas, and source
hashes. Fourteen Rust and five Java tests pass. The partial 0.64 adapter rejects
908 corpus cases; it does not meet the parser gate. `parser-row-corpus.json`
and `parser-row-syntax.json` retain the earlier ROW-only intermediate, before
explicit NULL field modifiers, fragment merging and Unicode handling.
`parser-semantic-counterexamples.json` remains the original failing evidence.

`fold-overlay-s3.json` records 268 final diagnostic differences and six exact
math-value differences. All 528 focused compilations and 392 runtime vectors
pass; 12,676 of 12,682 comparable Java values match. The subsequent interval
extension is under full-compilation validation; its journal-only results must
not replace this final-stage certificate until that run completes.

`s2-full-v3-protocol.json` records complete classic S2 replay: all 8,571 unique
cases, 12 fresh JVMs, and 3,503,220 observations per side. All strict comparisons
pass, with no failed or excluded cases. This checks the whole optimizer prefix;
it does not execute generated programs or certify isolated S4 checkpoints.

`fold-interval-s3.json` retains the first full interval-cast run, with four new
warning differences. `fold-interval-stage-before.json` proves four corpus and
fourteen focused regressions on the old adapter. The repaired adapter passes
those checks in `fold-interval-stage-repair.json`; `fold-interval-stage-s3.json`
records the completed full rerun: 268 diagnostic differences, six math-bit
differences, all 584 focused cases and 483 runtime vectors passing. The raw
25,185,785-byte Java journal remains available in the recorded run directory.
`fold-portability-interval.json` pins its Windows MSVC and wasm32 rlib builds.

`parser-fold-full-java.json` records the post-repair compile/export suite:
2,847 tests, zero failures/errors, 71 skips, 8,571 exports and 10,408 steps.
Generated programs were not executed by this run.

`fold-binary-s3.json` preserves the full final-stage audit from
`$SCRATCH/p0-fold-final-s3-v11`: 8,571 cases, 198 diagnostic differences, six exact
math-value differences, 620 focused compilations and 519 runtime vectors. All
verdicts and schemas match; no internal failures occurred. Source and artifact
hashes are included. This is not a full-corpus runtime execution certificate.

`parser-maps-corpus.json` and `parser-maps-adversarial.json` record the MAP
extension: 739 of 8,571 corpus cases rejected, and zero Rust-only acceptances
in 54 adversarial probes. `parser-maps.json` pins the sources and validation.
`fold-portability-binary.json` pins the current Windows MSVC and wasm32 rlibs;
neither artifact was linked or executed on its target.

`s2-property-positions.json` records seven Gen-2 S2 dataflow mismatches, a failing
classic regression, and the version-4 repair. Full repaired audits remain
separate. `multiset-seam-v4.json` passes 200 analysis comparisons and 200 tail
comparisons. `multiset-dataflow-format-failure.json` retains the first run's
100 classic mismatches: its newly added dataflow check hashed raw JSON rather
than parsed JSON. The corrected check matches the existing Gen-2 contract;
generated Rust remains byte-exact. Partial full runs in `$SCRATCH/p0-seams-v4-full`
were stopped after this harness defect was identified and remain preserved.
The full Java invocation in `$SCRATCH/p0-seam-full-java-tests-v6.log` omitted required
corpus identities and failed during test setup; its rerun uses a new output path.

`property-positions-full-java.json` pins the passing 2,849-test compile/export
suite and strict validation of 8,571 exports. Backend execution was disabled.
`parser-aggregates-corpus.json`, `parser-aggregates-adversarial.json`, and
`parser-aggregates.json` record the repaired grouping/null-treatment extension:
667 corpus rejections, 21 passing Rust tests, and no Rust-only acceptances in 83
adversarial probes. `parser-aggregates-overacceptance.json` retains the five
failures before scoped rejection was added.

`gen2-runtime-current.json` records the complete version-26 runtime rerun:
2,849 tests, 2,197 passes, 69 failures, 512 errors and 71 skips. The 77.11% total
pass rate and 79.09% rate excluding skips both miss the 80% gate. All 579 prior
failures remain; no prior passing test regresses. The 70 added tests include
67 passes, two failures on the existing unsupported `DBSPPositiveOperator`,
and one skip. Every failure and skip is retained with report hashes. Automated
grouping is not an approved disposition or independent review.

`native-integer-runtime.json` records 6,732 matching actual-sqllib/native vectors
for nullable BIGINT addition, subtraction and multiplication, with source and
binary hashes. It compares values and exact overflow messages.
The prototype emits an object, links a shared library and loads it with
`dlopen2`; this result does not close N1, N2, C7 or C12.
`native-integer-runtime-categories.json` preserves the earlier measurement
before generated code returned the operands needed for exact error text.

`s2-gen2-full-v3-failures.json` preserves the complete original Gen-2 prefix
audit: 8,527 exported cases, 3,527,714 observations, 14 dataflow mismatches and
zero failed compilations. `s2-property-positions-all-failures.json` passes all
14 affected cases on protocol version 4. The full stricter rerun is separate.

`parser-interval-trim.json` pins the interval-CAST/TRIM adapter and its 26 tests.
`parser-interval-trim-corpus.json` leaves 516 of 8,571 cases rejected (6.02%).
`parser-interval-trim-adversarial.json` has zero Rust-only acceptances in 160
probes. `parser-interval-trim-overacceptance.json` preserves the 14 invalid
column-type acceptances before the interval hook was confined to CAST.

`fold-unsigned-s3.json` extends folding to unsigned integer bounds and casts:
748 focused compilations and 619 runtime vectors pass. All corpus verdicts and
schemas match, with 198 diagnostic and six math-value differences still open.
`fold-portability-unsigned.json` records the corresponding Windows MSVC and
wasm32 rlib builds; these artifacts were neither linked nor executed.

`fold-binary-uuid-s3.json` retains the expanded binary/UUID audit before the
temporal fold-site correction. `fold-temporal-sites-s3.json` records the repaired
full S3 run: 900 focused compilations and 2,403 runtime vectors pass, while 162
corpus diagnostic differences and six math-bit differences remain.
`fold-portability-binary-uuid.json` pins the updated Windows MSVC and wasm32 rlibs;
these artifacts were built but not linked or executed.

`s2-let-rewrite-added.json` covers the two newly exported NOW-equality cases
using frozen compiler v28. Raw replay evidence is in
`$SCRATCH/p0-s2-let-rewrite-added-v1`; the population comparison, including explicit
new option defaults, is in `$SCRATCH/p0-gen2-runtime-v3/population-delta-defaults.json`.

`fold-decimal-arithmetic-s3.json` adds 160 actual-runtime vectors for mixed
integer/decimal arithmetic and decimal remainder. All 932 focused compilations
and 2,563 runtime vectors pass. The full corpus retains 142 diagnostic and six
exact math-value differences. Raw files are in `$SCRATCH/p0-fold-final-s3-v16`.

`parser-lateness.json` records 183 newly accepted corpus cases, leaving 333
rejections, and zero Rust-only acceptances in 216 adversarial and 13,660 keyword
probes. Seven Java oracle tests and 30 Rust tests pass. Two interrupted oracle
runs and the repaired untyped-view overacceptance are preserved.

`parser-aggregate-filter.json` records 51 further recovered corpus cases,
leaving 282 rejections (3.29%), and zero Rust-only acceptances in 234 adversarial
and 13,660 keyword probes. The rejected pre-fix null-treatment ordering remains
in `$SCRATCH/p0-parser-filter-v1`; the corrected audit is in `$SCRATCH/p0-parser-filter-v2`.

`parser-function-declarations.json` records typed CREATE FUNCTION support:
8,320 accepted corpus cases, 251 rejected (2.93%), and zero Rust-only
acceptances in 258 adversarial and 13,660 keyword probes. The raw audit is in
`$SCRATCH/p0-parser-functions-v1`; 33 Rust tests and Clippy pass.
`fold-portability-decimal.json` pins the updated Windows MSVC and wasm32 decimal
fold rlibs. Both build, but neither target has been linked or executed.

`s2-full-v4.json` links independently verified full classic and Gen-2 S2
certificates (`s2-classic-full-v4.json`, `s2-gen2-full-v4.json`). Ten shards and
20 JVMs pass all 8,571 classic and 8,527 exported Gen-2 cases, with 3,503,220 and
3,527,714 observations per side respectively. Both engines require parsed
dataflow equality; Rust bytes, hash inputs and persistent ids remain exact.
These are frozen v26 compilation/replay measurements, not runtime certificates
or approval of original-test exclusions. The verifier's source hashes are pinned.

`parser-statement-boundary.json` records the grammar-based statement-family
allowlist and SET/DROP option checks. It closes 35 measured overacceptances and
passes 325 adversarial plus 13,660 keyword probes with zero Rust-only acceptances.
Full-corpus coverage stays at 8,320 accepted and 251 rejected. Thirty-five Rust
tests and Clippy pass; the original failed comparison is retained.


`parser-feldera-statements.json` records typed recursive view and standalone
LATENESS syntax: 8,369 accepted cases, 202 rejected (2.36%), and zero Rust-only
acceptances across 367 adversarial and 13,660 keyword probes. Thirty-seven Rust
tests and Clippy pass. Full top-level consumption prevents END from hiding a
trailing statement.

`parser-vendor-relations.json` records the subsequent five-file vendor patch:
8,450 accepted cases, **121 rejected (1.41%)**, and zero Rust-only acceptances
across **419 adversarial** and **13,660 keyword** probes. It retains before/after
results for seven overacceptances and the failed Java-oracle classification run.
All 34,015 stock comparator submissions are unchanged, and every previously
accepted corpus case remains accepted. Forty-one Rust tests, Clippy and eight
Java oracle tests pass. The 2% raw-parser coverage threshold is met; full AST
rejection, span parity, S1 equivalence and vendor maintenance estimates are not.


`fold-selection-before-eligibility-s3.json` preserves the failed first selection
experiment: 12 focused diagnostic differences despite matching runtime values.
`fold-selection-legacy-math-s3.json` records the repaired full S3 run: all 8,571
verdicts/schemas, 996 focused compilations and 3,137 runtime vectors match, with
114 remaining corpus diagnostic differences. All 16,429 comparable Java values
match under the explicit legacy EXP/LOG profile; runtime vectors use the distinct
runtime profile. This is not a claim that those profiles are equivalent.

`fold-math-origin.json` pins direct Math/StrictMath/Calcite measurements with
warm-up, interpreted execution and disabled math intrinsics, plus the separate
full-journal libm candidate. `fold-portability-profile.json` records Windows and
wasm library builds only; neither target was linked or executed.

`fold-integer-binary-runtime.json` records 728 actual-runtime comparisons;
`fold-integer-binary-s3.json` records the full audit that removes 22 diagnostic
differences. `fold-text-diagnostic-s3.json` is a targeted audit of the 92
remaining cases, not a new full-corpus certificate.

`fold-map-before-registration-s3.json` preserves the first map adapter's
operator-kind refusals. `fold-collections-before-diagnostics-s3.json` retains
the first combined full audit's 22 focused diagnostic differences. The repaired
`fold-collections-s3.json` matches all 8,571 verdicts/schemas, all 16,738
comparable Java values, 1,260 focused compilations and 4,615 runtime vectors,
with 38 remaining corpus diagnostic differences. The focused generator's
unused-table prefix was subsequently removed to prevent masking warning
promotion; stronger policy results are separate evidence.

`fold-portability-collections.json` and `fold-portability-collections-final.json`
record successive Windows/wasm rlib builds. Neither target was linked or run.

`fold-warning-policy-s3.json` retains the stronger audit's failures after removing
the unused-table masking effect: 1,296 focused cases expose 34 diagnostic,
14 acceptance, and two schema differences. `fold-legacy-janino-s3.json` records
the first explicit parser/domain compatibility profile over the 92-case targeted
population: 14 corpus diagnostic differences and 18 focused differences remain;
all 4,615 runtime-profile vectors pass. These are targeted comparisons, not new
full-corpus certificates or approved deviations.

`fold-legacy-split-s3.json` retains the next targeted result, including the
empty-array literal-conversion boundary. `fold-spatial-s3.json` adds 625 actual
runtime comparisons and brings the targeted corpus to seven diagnostic
differences. `fold-typed-map-s3.json` matches all 1,296 focused compilations and
5,240 runtime vectors, retaining three targeted corpus diagnostic differences.
All intermediate failures remain visible. `fold-portability-legacy.json`
records Windows/wasm library builds for the combined legacy-profile work;
neither target was linked or executed.

`fold-legacy-final-s3.json` records the complete combined run: all 8,571
verdicts/schemas/diagnostics, 16,796 comparable Java values, 1,296 strengthened
focused compilations and 5,240 actual-runtime vectors match. Both observation
transparency checks pass, with zero compiler bugs. The certificate retains
260 unsupported corpus observations and 410 nonliteral Java results; those
are not successful value comparisons. This does not certify complete fold-site
coverage, a production compatibility handshake or completion of P0.

`fold-window-default-s3.json` adds the distinct LEAD/LAG third-argument literal
requirement: all 96 new policy cases match, along with the complete expanded
1,392-case focused population and 5,240 runtime vectors. Its corpus population
is one smoke case, not a new full-corpus run. Complete constant-site coverage
and independent deviation review remain open.

Cold-process S2 timing results, reproduction and gitignored raw archives are
listed in [BENCHMARKS.md](../BENCHMARKS.md). These local subset measurements do
not certify production p95 or approve resource budgets.

`parser-maintenance-inventory.json` classifies all 121 positive-corpus refusals
into 31 diagnostic groups, pins the adapter/vendor sources and records the
provisional recurring upkeep estimate. Repair directions and time estimates
come from source inspection; no new upstream rebase or human timing was measured.

`native-key-sort-merge.json` records 594 paired sort/merge observations over
eighteen synthetic nullable-BIGINT configurations, including encoding costs,
extra key storage, paired intervals, source/build hashes and measurement limits.
It reports both gains and losses. It does not close the full N1/N2 experiment.

`replay-context-baseline.json` records the replay-only compiler context's
53 targeted Java tests, five actual Rust tests and separate full compile/export
baseline (2,869 tests, 71 skips, 8,571 exported cases). It preserves initial
failures and the resource-aware source audit. It is not a full S2/S3 certificate.

`parser-values.json` records 29 recovered corpus cases and eight corrected ROW
shapes, leaving 92 refusals. Previous/new ASTs, stock-comparator preservation,
the expanded 488-case adversarial population, keyword and diagnostic comparisons
are retained. The two newly tested old-parser overacceptances are repaired;
five new Java-only forms remain unsupported. This is not an S1 certificate.

`replay-context-full.json` completes the replay-context audit: all 8,571 classic
and 8,527 exported Gen-2 cases pass S2 and S3 over 40 fresh JVMs. It pins all
four summaries, shard artifacts, build identities and input populations. S2
compares 3,503,220 classic and 3,527,714 Gen-2 observations per side. Full runtime
execution, production timing and per-point S4 inventory review remain separate.

`parser-mutations.json` records 27,760 deterministic mutation/control cases and
repairs all 342 Rust-only acceptances, with the 274 previous Java-only cases
unchanged. It includes the failed baseline, final 614-case adversarial matrix,
full-corpus AST preservation, unchanged stock controls, 17 Java oracle tests,
51 Rust tests, 79 harness tests and error-location repairs. The finite mutation
set does not certify exhaustive rejection, typed hint preservation or S1 parity.

`parser-hints.json` records typed hint preservation and attachment, all 75 focused
hint verdicts matching Java, and the expanded 689-case adversarial population.
Fresh corpus, keyword, diagnostic and mutation comparisons preserve earlier
verdicts; AST changes retain previously lost hints. Two actual Java table-hint
unparse failures remain visible with stacks and rendering completion marked false.
55 Rust tests, 80 Python tests and 19 Java tests pass. Full S1 parity remains open.

`s4-path-smoke.json` records opt-in per-case S4 path accounting, exact disabled/
enabled/replay fingerprint equality and matching direct/replay path sidecars
over eight cases. Counts are checked against the existing S4 fingerprints.
Full-population collection and per-point inventory review remain open.

`rel-literal-contract.json` records the draft exact typed-literal payload schema,
bounded iterative payload validation, 16 Rust tests and Clippy. It retains float
bits, exact decimals and ordered nested values. Calcite projection, SQL type
compatibility and the complete S1 protocol remain open.

`s1-literal-payloads.json` records 94 fixed scalar Java/Rust/Java payload round
trips with exact measured RexLiteral values and fixture types. Five negative
checks detect schema, identity, coefficient, missing-case and NaN payload changes.
Types are supplied by the fixture registry; full S1 type/plan import remains open.

`parser-ast-fields.json` records 27 repaired query/DML option overacceptances,
explicit policies for six upstream structs, and the expanded 745-case adversarial
population. Previous corpus, keyword, diagnostic and mutation records are exactly
preserved; the field matrix retains one Java-only LIMIT comma form. Full AST
coverage and S1 parity remain open.

`s1-scalar-types.json` records the 94-case scalar payload/type round trip,
24 Rust tests, eight Java tests, nine malformed-input checks and the collation
identity defect found and repaired during exact Calcite type comparison. It
supersedes the payload-only scope for these fixtures, not the historical evidence.
It does not certify compound type, relational, statement or complete S1 transport.

`native-dbsp-batches.json` records the isolated native batch/DBSP integration:
three passing tests for incremental join/linear-aggregate deltas, NULLs, signed
weights and the 65,537-row chunk boundary. It pins the runtime dependency sources
and executable; it does not claim ten-program N1 C7 or N2 state compatibility.

`s4-full-paths.json` certifies strict S4 path accounting during full S2 prefix
replay for 8,571 classic and 8,527 exported Gen-2 cases over twenty JVMs. It also
records the separately labeled 32-case activation set and constructed-tree census:
1,049 returning paths are observed; the always-throwing Fail path remains in
scope with a failure/path-cleanup regression. All thirty composite inventory rows
and explicit manual points are observed. Raw Gen-2 byte differences stay visible
under seam-v1. Exhaustive field-level state inventory and independent review
remain open; this is not isolated checkpoint or full-runtime evidence.

`s1-point-literals.json` extends scalar/type transport to 105 fixtures, with ten
points and a geometry NULL. Raw coordinate bits, dimensions, measures and SRIDs
survive Java/Rust/Java; unsupported shapes and geometry metadata fail. It records
25 Rust tests, ten Java tests and fifteen negative transport checks, not geometry
runtime C7 or complete S1 parity.

`s1-type-arenas.json` records ordered scalar/compound type transport across 8,571
classic and 8,527 exported Gen-2 optimized-view populations, a 161-root identity
matrix, the 105 existing literal fixtures, and fifteen malformed arenas rejected
by both Rust and Java. It pins separate export and hardened-import classpaths and
retains earlier audit/negative failures. This is not complete statement/catalog
type coverage, FRel/FRex/statement v0, lowering parity, C7 or full S1.

`s1-literals-full.json` certifies observed pre-S1 literal transport for all 8,571
classic and 8,527 exported Gen-2 cases: 42,898 literals, with exact payload/type/
storage and arena-reference checks. It records SEARCH expansion, 146 fixtures,
26 Rust tests, twenty Java tests and fifteen malformed records rejected by both
readers. Earlier builder normalization, temporary-filter and NULL identity failures
are retained. This is not complete FRel/FRex/statement transport, detached S1-to-S2
lowering or C7 parity.

`s1-expressions.json` records the seven-form resolved-expression draft, one actual
Java/Rust/Java Rex fixture, 38 Rust tests, 22 Java tests and fifteen malformed
variants rejected by both readers. The Java oracle has a fixed operator and binding
layout. It does not certify full FRex, corpus expression import, S1/S2 or C7.

`s1-operator-registry.json` records exact lookup of 603 static Calcite operators,
version/key rejection and forged display-name rejection, 27 focused Java tests,
and unchanged seven-node expression transport through the new registry. It is not
a full function catalog, per-call typing proof or complete S1 importer.

`s1-fragment-archive.json` locates a host archive of the completed literal,
expression and operator fragments, including frozen tools, sources, inputs and
failed literal attempts. The archive is gitignored under `.p0-artifacts/`.
Verify its bytes and all 415 artifact, 89 source and ten classpath hashes without
extracting it:

```sh
python3 crates/sql/scripts/verify_fragment_archive.py crates/sql/evidence/s1-fragment-archive.json
```

Archive integrity does not establish experiment acceptance or P0 completion.
The seven verifier tests cover inner hash tampering, outer archive changes,
duplicate members, missing classpaths, completion types and ambiguous mappings.

`s1-symbol-validation.json` retains three unsupported symbols previously accepted
by Rust and now rejected by both readers. It records 40 Rust tests, Clippy and
unchanged revalidation of both frozen literal populations and 146 fixtures. This
is stricter literal validation, not a new SQL capture or full S1/C7 proof.

`s1-symbol-archive.json` locates the corresponding host archive for symbol
hardening. The same verifier checks its 100 artifacts, 26 sources and two
classpaths. Both archives retain their counterexamples; neither establishes P0
completion.

`s1-literal-storage.json` records the retained pre-fix NaN-bit regression, the
Java export storage guard, 29 focused Java tests and fresh Java/Rust/Java captures
of 42,898 literals across both complete populations. All records and 146 fixtures
match the earlier captures exactly. This does not certify full S1 or runtime C7.

`s1-statement-metadata.json` records immutable INSERT/REMOVE target metadata,
three focused observer/AST-independence tests and before/after final-S3 comparisons
for 8,571 classic and 8,527 Gen-2 cases in twenty JVMs. All final artifacts and hash
inputs match, including raw bytes. One separate type-sharing difference remains
explicitly open with its prefix and mixed compiler/collector controls. This is
not full S1 import, prefix replay or runtime C7.

`s1-boundary-archive.json` locates the host archive for ordered type arenas,
statement target metadata and literal storage checks. Its verifier checks 579
artifacts, 63 sources and seventeen classpath records. The historical type-arena
certificate groups its classpaths and uses the original binary-digest directory
format; the index selects that format explicitly. Use the current repository
verifier. Integrity verification still does not establish P0 completion.


`s1-rel-field-walk.json` corrects the traversal coverage of the earlier optimized
view captures: 29,354 classic and 29,307 Gen-2 literal objects, totaling 58,661.
The explicit 17-class walker also includes window frame offsets, aggregate direct
arguments and ASOF match conditions. All expanded type arenas and literals pass
Java/Rust/Java transport across the same 17,098 cases. Two pre-fix regressions,
34 focused Java tests and three isolated field-walking tests are retained. No
statement/catalog completeness or whole-program S1 parity is implied.


`s1-expression-codec.json` records the general bounded expression-fragment
exporter/importer, six Java/Rust/Java constructor graphs, a separate seven-node
oracle, 42 Rust tests, 48 Java tests and forty malformed-reader rejections. Its
registry inventory preserves all 603 Calcite keys and 61 Feldera builtin objects.
Source-bearing calls, specialized Rex subclasses and whole-program context remain
open; this is not corpus FRex or detached S1-to-S2 parity.

`s1-fieldwalk-expression-archive.json` locates the 53.6 MB host archive containing
both new certificates, frozen sources/tools, commands, inputs, passing checks
and retained failures. The integrity verifier checks 484 artifacts, 119 sources
and eleven classpath records. The archive does not establish P0 completion.


`s1-expression-sources.json` adds explicit source text and exact UTF-16 positions
for calls: seven constructor graphs, 42 Rust tests and 52 Java tests, including
actual optimized SQL project expressions. Ten malformed source records distinguish
structural validation from adapter text-bound checks. Whole-program submission
boundaries, nullable file metadata and non-call/type origins remain open.
`s1-expression-sources-archive.json` pins the 36.6 MB host archive and verifies
188 artifacts, 66 sources and five classpath records.


`s1-expression-context.json` records generated partial positions, exact character
input-reference type identity, explicit SARG/SEARCH rejection, 43 Rust tests and
56 Java tests. Nine constructor graphs and a 240-case project-only prefix
(389 projects, 5,217 nodes) pass Java/Rust/Java transport. The full 1,429-case shard
fails next on the named type constructor `address_typ`; that failure remains
counted and is not a full-shard pass. Catalog/statement context and whole-program
S1 remain open. `s1-expression-context-archive.json` locates the 47.3 MB archive
and verifies 362 artifacts, 131 sources and eleven classpath records.


`s1-constructor-objects.json` records the versioned operator-object arena, named
and row constructors, reference nullability widening, 49 Rust tests and 63 Java
tests. Eleven graphs and a 527-case project-only prefix (900 projects, 9,876
expressions) pass Java/Rust/Java transport. The full 1,429-case shard fails next
on a SQL-defined `ExternalFunction` body; it is not a full-shard or S1 pass.

`s1-constructor-objects-archive.json` locates the 52.2 MB host archive, including
retained failed attempts and frozen tools. Its verifier checks 592 artifacts,
75 sources and thirteen classpath records. Integrity verification does not close
any unmet P0 gate.


The reviewed full-prefix replay is in `s1-reviewed-prefix.json` and
`s1-reviewed-prefix-archive.json`, explained by `../S1-REVIEWED-REPLAY.md`.
It independently verifies all ten classic/Gen-2 shards after exactly two
ROW-selector program-record replacements, preserving both original failures
and the separate old-production differences. The archive pins the frozen Java
sources/classes, unchanged Rust comparator sources/executable, dependency jar,
raw inputs/results, and replacement/rollback evidence. Later working-tree
repairs and runtime execution are explicitly outside this capture.


`correlation-coverage.json` records the completed negative spike 6 experiment:
nine C7-equal Correlate occurrences out of 155 at the declared first-inner-stage
boundary, with all 75 invocations and all failure/refusal weights preserved.
The full four-stage census and other 97 related cases remain separately
accounted. `correlation-coverage-archive.json` locates the verified source-bound
archive, including runtime witnesses, final sources, historical failures and
explicit historical model-symlink provenance. The 95% success criterion failed;
the estimate proposal and independent adjudication are in
`../DECORRELATION-ESTIMATE.md` and `../SPIKE6-ADJUDICATION.md`.


`type-bindings.json` and `type-bindings-archive.json` retain the source-pinned
read-only binding observation: all 84,224 original probes, zero capture errors
or compiler bugs, and unchanged verdicts/schemas/diagnostics/input manifests.
Four added default seam options remain strict differences from the historical
oracle. The selected validated-call projection preserves every cast/type/operator
field for all 28,960 accepted calls across forty functions, with 13,920 function
rejections accounted separately. The source/class archive retains failed attempts,
six Java tests, and sixteen Python tests. Validated-call capture is not a C9 pass;
see `../TYPE-DERIVATION-REVIEW.md` for the required S2 and runtime boundary.
