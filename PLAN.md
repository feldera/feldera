# Plan: replace the Java/Calcite SQL compiler with a Rust compiler

Implementation is underway. The owner approved Tracks M, N and F. Independent
P0 maintainer and licensing acceptances remain open. The local Rust driver and
measured milestones through P5 are tracked in the [implementation record](crates/sql/PORTING.md#local-compiler-and-phase-status).
Date of the investigation: 2026-11-11. Branch `new-compiler` at `c6ede756d`.
Original investigation method and confidence: Appendix A. That investigation ran
no compiler build or test suite, apart from one scratch sqlparser test (section
6.2). The implementation evidence below records subsequent builds and tests.
The back-end revision of 2026-11-11 specifies Cranelift code generation
in section 4.2. The [P0 exit checklist](crates/sql/P0.md#exit-checklist) records
the native prototype measurements, the Rust-written S2 output-distinct replay,
and the remaining acceptance criteria.
Review revision: contracts, dependencies, rollout, and gates checked against local
source on 2026-11-11; external project claims remain from the original investigation.

Path prefixes used in this document:

| Prefix | Path |
|---|---|
| `J/` | `sql-to-dbsp-compiler/SQL-compiler/src/main/java/org/dbsp/sqlCompiler/` |
| `F/` | `J/compiler/frontend/` |
| `CC/` | `J/compiler/frontend/calciteCompiler/` |
| `T/` | `sql-to-dbsp-compiler/SQL-compiler/src/test/java/org/dbsp/sqlCompiler/compiler/` |
| `G/` | `sql-to-dbsp-compiler/SQL-compiler/src/main/codegen/` |
| `PM/` | `crates/pipeline-manager/src/` |
| `DF/` | `~/.cargo/registry/src/index.crates.io-*/` (DataFusion 55.2.0 and sqlparser 0.62.0, the versions in `Cargo.lock`) |

## 0. Summary

| Question | Answer |
|---|---|
| Is a full Rust compiler feasible? | Yes. 67% of the main Java code (IR, passes, back end: 85.6k lines) has only 28 direct Calcite imports (date and time string classes, one UUID helper, RelNode kinds in one pass, a type factory in the decoder) and can be ported 1:1 against the Java compiler as an oracle. The hard part is the 28% front end (`F/` and `G/`, 36k lines) that sits on Calcite. |
| What does Calcite supply that must be replaced? | Parser, validator (name resolution, overloads, implicit casts, type derivation), SqlToRel, the plan builder and Rex utilities, sub-query removal and decorrelation, 63 stock rewrite rules, metadata queries, RexSimplify, constant folding, and three serializers (types, plans, Rex to SQL text). Feldera code holds only copies of the convertlet table and of 3 rule classes (4.5k lines). |
| Is DataFusion the right base? | The plan uses A, a Feldera-owned binder on sqlparser-rs. Spike D1 tested H, which used DataFusion `SqlToRel` with Feldera relational IR, and stopped with incomplete coverage. Section 3. |
| How to migrate? | Strangler pattern on the circuit IR JSON that exists today. Three parallel tracks: a mechanical port of the mid-end (structural oracle per pass), a new front end (oracle: verdict, schema, type probes, run-time results), and a new native back end with Cranelift (oracle: run-time results). |
| What does the back end generate? | Native code through Cranelift, loaded by a prebuilt pipeline binary. No Rust source, and no cargo step for a program without UDFs. The Java back end is not ported; it stays for existing pipelines until the Java retirement. Section 4.2. |
| Source scope | The compiler has about 118k lines of main Java outside the back end. The rewrite also replaces the Calcite behavior Feldera uses and adds the native back end and its engine parts. |
| First step | Phase 0 in two parts. P0a: test corpus export, seam flags in the Java compiler, spikes 0, 1, 2, 3, 10, N1, N2, then a go or no-go review for the mid-end track and for the native back end. P0b, in parallel: spikes 4, 5, 5b, 6, 7, 9, 11, D1, then a go or no-go review for the front end. Spike 8 follows in P1. Phase 0 has value even if the project stops. |
| Largest risks | Type-derivation parity with Calcite (1), decorrelation coverage (2), the completeness of the JSON seam (5), persistent IDs and state at the switch (7), Rust architecture (14), the native state format (21), and the causes that removed the earlier JIT (23). See section 11. |

### Implementation progress

- 2026-11-20. P2 passed its local engineering gates. Both engine populations
  matched across S3 and the optimizer tail. All 8,256 executable classic cases
  passed, including 232 expected runtime errors. The checkpoint and decoder
  results and the outstanding independent reviews are recorded in the
  [phase status](crates/sql/PORTING.md#local-compiler-and-phase-status).
- 2026-11-21. The aggregate and distinct build at `2a2398a2a` ran 106 Rust
  pass kinds, including 13,523 whole limiter insertions. The limiter entry
  and exit circuits matched for all 17,098 frozen programs. All matched direct
  Java through S3, persistent-ID inputs and emitted artifacts. The selected
  LATENESS and NOW() programs passed 2,076 runtime commands, and COUNT and
  LATENESS checkpoints restored in both directions. The nested-scheduling build
  (`e81272b2e`) passed all 8,256 executable classic cases across 11,407 input
  commands, including 232 expected runtime errors. On `0cb5bb67e`, all 59
  frozen recursive cases ran Rust limiter insertion and matched Java at every
  optimizer boundary and through the generated artifacts. On `45ffbdba0`, with no concurrent compiler verification,
  hybrid p95 was 1.211× direct Java on the array subquery, 2.202× on Nexmark
  and 1.179× on the outer join.
  [P3 implementation](crates/sql/PORTING.md#p3-optimizer-replay) · [Paired timings](crates/sql/BENCHMARKS.md#packaged-mixed-optimizer-2026-11-21)
- 2026-11-22. On `4f84a9cf6`, isolated hybrid p95 was 1.213× direct Java on
  the array subquery, 2.245× on Nexmark and 1.204× on the outer join. P3
  remains opt-in. Java still runs part of the optimizer, the latency ceiling
  was exceeded on all three workloads, and the field-shadow gate is open.
  [Paired timings and worker measurements](crates/sql/BENCHMARKS.md#column-initializer-build-2026-11-22)
- 2026-11-19. The P1 IR codec passed 7,030,934 observation round-trips across
  17,098 classic and Gen-2 cases. CSE, ARRAY_AGG fusion and the representative
  limiter slice matched 296 Java S4 results and four Gen-2 refusals. The
  [P1 record](crates/sql/PORTING.md#local-compiler-and-phase-status) tracks
  downstream consumer checks and independent review.
- 2026-11-19. The Rust driver parses all 8,571 frozen positive cases and binds
  75 of them to S1 under the default Gen-2 profile. Each matched Java through
  the optimizer observations and public artifacts in the devcontainer. Seventeen
  directed programs also matched through the Forge reader. Rust lowers selected
  tables, constant rows, projections and filters. All 23 supported frozen programs
  produced matching S2 and resumed in the Java optimizer. All 6,174 focused S2/S3/S4
  graphs round-trip through Rust. CompactNames and TagRegions each match all 16
  Java invocations. Five paired benchmarks cover SQL through the Gen-2 artifacts
  published by the Rust worker. All use Java optimization. Of 1,061 Java syntax rejections, 659
  match the product diagnostic class, start, snippet and severity. The remaining
  phase gates are recorded in §7.3. [Phase status](crates/sql/PORTING.md#local-compiler-and-phase-status) · [Measured evidence](crates/sql/evidence/local-compiler-20261119.json)

- 2026-11-19: Track N recorded go after descriptor-owned input replay
  matched all 21 saved transactions. COUNT DISTINCT and nullable SUM also
  restored descriptor-owned checkpoints in fresh DBSP runtimes. The rolling
  benchmark matched checkpoint sizes in all 84 pairs after compaction.
  [Owned input replay](crates/sql/evidence/native-context-frozen-inputs.json) ·
  [Checkpoint measurements](crates/sql/evidence/native-rolling-checkpoint-compacted.json)

- 2026-11-19: the Spike 2 selection passes in classic and Gen-2 with Rust
  scheduled at 99 optimizer placements. All 100 direct runs and detached S2
  runs preserve C6b, C6c and persistent IDs. Java also matches those results
  after opening each Rust-written S3. The isolated pass audit matches all
  2,200 graphs. [Replay evidence](crates/sql/evidence/optimizer-prefix-99.json)

- 2026-11-19: the ten frozen native programs pass on the updated DBSP
  code. Erased constant and accumulated-output operators handle signed
  transactions and exchange checkpoints with the classic forms. All three
  tested classic circuit fingerprints match the original build. Rolling
  artifact startup measures 11.25 ms native p95 and 38.71 s classic p95.
  The general runtime factory interface is tracked in PN-a.
  [Native tests](crates/sql/evidence/native-final-with-outputs.json) ·
  [Artifact timing](crates/sql/evidence/native-rolling-artifact.json)

- 2026-11-18: the frozen corpus now has a portable expected-output format and
  standalone classic and Gen-2 replay. All 20,723 expected output bags convert
  to the portable format. Runtime replay passes 151 classic and 129 Gen-2
  cases. The 24 other selected Gen-2 cases reproduce the original
  `DBSPPositiveOperator` loader failure.
  [Corpus evidence](crates/sql/evidence/corpus-runtime-replay.json)

- 2026-11-18: snapshot antijoin and indexed join lowering now runs in Rust at
  the original optimizer placement. All 92 exact-input graphs and final
  compiler results match Java. Four activating indexed-join cases also match
  after detached S2 replay. [Join lowering evidence](crates/sql/evidence/optimizer-implement-joins.json)

- 2026-11-18: the 44 scheduled placements run together and preserve final
  artifacts and persistent IDs for 100 incremental programs. Rust separates
  shared traces and identifies the error table's downstream operators inside
  the monotonicity analyzer and matches distinct optimization on 204 exact
  pass inputs.
  [Distinct and composition evidence](crates/sql/evidence/optimizer-distinct.json) ·
  [Trace-isolation evidence](crates/sql/evidence/optimizer-trace-isolation.json) ·
  [Error-consumer evidence](crates/sql/evidence/optimizer-error-consumers.json)

- 2026-11-18: aggregate empty-row expansion brings the combined optimizer
  replay to 45 placements with all 100 incremental results preserved. The
  expansion matches 202 pass graphs, including active snapshot and directed
  cases. Rust stream-kind inference matches Java on 17,960 flat pass
  snapshots and now validates the four passes grouped at the first Rust
  handoff. [Aggregate expansion](crates/sql/evidence/optimizer-aggregate-zero.json) ·
  [Stream-kind validation](crates/sql/evidence/optimizer-stream-kinds.json)

- 2026-11-18: binder A matches all 39,776 accepted generated schemas under
  C1a and all 19,488 rejection verdicts. The held-out FRel obligation remains
  open. [Schema comparison](crates/sql/evidence/a-binder-c1a.json)

- 2026-11-18: Rust binds two original constant views into complete S1 programs
  using the fixed compiler prelude. Detached lowering through both backends
  matches their original compiler records, including diagnostics, schemas and
  artifacts. Wider held-out coverage remains open.
  [Whole-program comparison](crates/sql/evidence/a-binder-constant-program.json)

- 2026-11-18: the plan owner kept **Tracks M, N and F open in P0**.
  Spike 2 retains its full-prefix correctness criterion.
  [Decision record](crates/sql/P0.md#track-m-decision)

- 2026-11-18: the original generated nullable SUM and COUNT DISTINCT circuits
  exchange checkpoints with native accumulator serializers in both directions
  and preserve their persistent IDs and runtime fingerprints.
  [Aggregate checkpoint evidence](crates/sql/evidence/native-aggregate-checkpoints.json)

- 2026-11-18: the scheduled replay harness uses Rust at 35 original optimizer
  placements. It matches 3,444 pass results and 100 final compiler outputs
  across classic and Gen-2. In the earlier 24-placement
  benchmark, process reuse cuts paired median replay latency by 11–12%
  versus one-shot Rust. The remaining prefix and production hybrid timing
  stay open.
  [Optimizer evidence](crates/sql/evidence/optimizer-fanout-clones.json) ·
  [Worker benchmark](crates/sql/BENCHMARKS.md#reused-rust-process)

- 2026-11-18: Rust matches Java on all 300 scheduled applications of
  `MergeSums` and `EnsureDistinctOutputs` in the 50-program spike 2 selection.
  The existing DeadCode seam passes 200 separate Java-tail regressions.
  Full Rust prefix replay remains open.
  [Optimizer evidence](crates/sql/evidence/multiset-optimizer-prefix.json)

- 2026-11-18: all ten frozen native programs pass 21 DBSP replay steps and
  their original classic references. The production JSON format emits identical
  bytes from both circuits. The selected rolling pipeline restores a classic
  checkpoint with native-encoded decimal join keys. The matched Unicode pilot
  records throughput, RSS and shared-library build costs. General dynamic row
  factories, implicit state-layout coverage, the full C13 budget and the
  independent phase decisions remain open.
  [Native ledger](crates/sql/evidence/native-coverage-ledger.json)

- 2026-11-18: the shared A/H split freezes 1,563 held-out cases from the
  reviewed classic corpus and Java diagnostic captures. The existing A binder
  emits zero projections and H emits three fields on that partition. Both still
  need equivalent FRel and C0/C1a coverage.
  [D1 baseline](crates/sql/evidence/d1-heldout-baseline.json)

- 2026-11-17: `expressions-v2` separates operator-object identity from call
  signatures and transports named type constructors and row constructors with
  their metadata. Field references preserve equal distinct types and safe outer
  nullability widening. All 49 Rust and 63 Java tests pass. Java/Rust/Java
  transport now passes a 527-case project-only prefix (900 projects, 9,876
  expression nodes). The 1,429-case shard stops next on a SQL-defined function
  body; full statement/catalog/FRel and detached S1-to-S2 remain open. See
  `crates/sql/evidence/s1-constructor-objects.json`.


- 2026-11-17: corpus-driven expression fixes preserve partially unknown generated
  positions, retain character input-reference type objects and reject unexpanded
  SARG/SEARCH explicitly. All 43 Rust and 56 focused Java tests pass. A 240-case
  project-only prefix transports 389 projects and 5,217 expression nodes through
  Java/Rust/Java. The attempted 1,429-case shard fails next on the named type
  constructor `address_typ`; it is retained and not excluded. Catalog declarations,
  full relation/statement transport and detached S1-to-S2 remain open. See
  `crates/sql/evidence/s1-expression-context.json`.


- 2026-11-17: source-bearing expression calls now preserve explicit source text
  and exact UTF-16 coordinates. Seven constructor graphs pass cross-language
  transport; 52 Java tests include actual optimized SQL project expressions.
  Ten malformed source cases test the distinction between Rust structural
  validation and Java text-bound checks. Whole-program source/submission context
  and detached S1-to-S2 remain open. See
  `crates/sql/evidence/s1-expression-sources.json`.


- 2026-11-17: the general expression-fragment exporter/importer replaces the
  fixed reader. `expressions-v1` preserves separate lambda declaration nodes,
  forward references and binding-free sharing between scopes. Six constructor
  graphs pass Java/Rust/Java transport; 20 malformed graphs fail both readers.
  All 42 Rust and 48 focused Java tests pass. The trusted registry covers 603
  Calcite keys and 61 Feldera builtins, preserving cloned operator identity.
  Source-bearing calls, specialized Rex forms, full relational/statement
  transport and whole-program S1-to-S2 remain open. See
  `crates/sql/evidence/s1-expression-codec.json`.


- 2026-11-17: an explicit local-expression walker fixes omissions in generic
  Calcite visitation (VALUES, window constants/bounds, aggregate direct arguments
  and ASOF match conditions). The expanded Java/Rust/Java capture transports
  **58,661 literals**, adding 15,763, and all collected type roots across the
  same 8,571 classic and 8,527 Gen-2 cases. Earlier captures remain historical
  evidence with narrower coverage. See `crates/sql/evidence/s1-rel-field-walk.json`.


- 2026-11-17: the resolved-expression draft represents the seven lowering forms
  with explicit binding scopes and operator signatures. One real Java/Rust/Java
  fixture and fifteen malformed variants pass, alongside 38 Rust and 22 Java
  tests. The Java reader remains a fixed fixture oracle. Full FRex/transient forms,
  relation/statement transport, corpus S1 import and lowering/C7 remain open.

- 2026-11-17: observed literal transport passes all 8,571 classic and 8,527
  exported Gen-2 cases after the lowering's SEARCH expansion: 42,898 literals,
  exact types, storage tags, payloads and shared type references. The adapter fixes
  builder type inference and NULL-type cloning; 146 fixtures and fifteen malformed
  records pass their checks. Complete expression/relational/statement S1 and S2
  lowering parity remain open; see `crates/sql/evidence/s1-literals-full.json`.

- 2026-11-16: ordered compound type transport passes all observed optimized-view
  type roots in 8,571 classic and 8,527 exported Gen-2 cases. The importer preserves
  sharing and distinct equal nodes; corpus-driven repairs retain raw defaults,
  host geometry identity and extended interval units. The 161-root identity matrix,
  105 literal fixtures and fifteen malformed-arena mutations pass. Full S1 remains
  open; see `crates/sql/evidence/s1-type-arenas.json`.

- 2026-11-16: exact JTS point payloads expand the typed scalar experiment to
  105 Java/Rust/Java fixtures. Coordinate bits, dimensions, measures and SRIDs
  survive; unsupported geometry metadata and shapes fail closed. Twenty-five
  Rust tests, ten Java tests and fifteen negative mutations pass. This remains
  a literal slice of S1; see `crates/sql/evidence/s1-point-literals.json`.

- 2026-11-16: strict full S4 accounting passes 8,571 classic and 8,527 exported
  Gen-2 cases over 20 fresh JVMs. Separate option activations exercise all 1,049
  returning constructed paths; the always-throwing Fail path remains in scope.
  All thirty composite inventory rows and explicit manual points are observed.
  Field-level inventory and independent review remain open. See
  `crates/sql/evidence/s4-full-paths.json`.

- 2026-11-16: native batch results now feed an actual incremental DBSP join
  and linear aggregate with exact insertion/retraction deltas. Three tests and
  Clippy pass, including the 65,537-row chunk boundary. The isolated probe keeps
  JIT dependencies out of production crates. N1's ten SQL programs and N2 remain
  open; see `crates/sql/evidence/native-dbsp-batches.json`.

- 2026-11-16: the scalar adapter now imports one-entry SQL type tables through
  all 94 Java/Rust/Java literal fixtures. It preserves exact Calcite type digests,
  including built-in versus named collation identity, and adds bounded Rust
  payload/type verification. Twenty-four Rust tests, eight Java tests and nine
  negative mutations pass. Full S1 remains open; see `crates/sql/P0.md`.

- 2026-11-16: the first scalar S1 adapter slice preserves 94 Java/Rust/Java
  literal payloads with exact measured RexLiteral values and supplied fixture
  types. It includes unsigned boundaries, non-finite floats and all thirteen
  interval qualifiers, with five negative mutation checks. Full type/plan/
  statement transport remains open. Full S4 executed-path coverage now passes both exported populations plus
  32 separate activation programs; field-level inventory and review remain open. See `crates/sql/P0.md`.

- 2026-11-16: typed optimizer hints retain names, ordered options, duplicate keys,
  string fragments, spans and SELECT/table attachment. All 75 focused cases
  match Java; the expanded 689-case adversarial set has zero Rust-only
  acceptances. Full corpus and mutation verdicts remain unchanged. Two Java
  pretty-printer crashes are retained separately from parser acceptance.
  Complete S1 parity remains open; see `crates/sql/evidence/parser-hints.json`.

- 2026-11-16: full replay-context validation passes S2 and S3 for all 8,571
  classic and 8,527 exported Gen-2 cases over 40 fresh JVMs. The parser mutation
  experiment repairs 342 Rust-only acceptances across 27,760 cases while
  preserving all previous positive-corpus verdicts and ASTs. The expanded
  614-case adversarial set also has zero Rust-only acceptances. CI now gates
  mutation overacceptance. These results do not close S1, exhaustive parser
  coverage, runtime execution or the remaining phase decisions; see `crates/sql/P0.md`.

- 2026-11-16: scalar VALUES rows reduce parser refusals to 92/8,571 (1.07%).
  AST comparison also finds and repairs eight previously accepted programs with
  an extra ROW layer. All 488 adversarial, 13,660 keyword and 1,143 diagnostic
  probes have zero Rust-only acceptances; S1 parity remains open. Replay-context
  initialization passes 53 targeted Java tests, five Rust runtime tests and the
  full 2,869-test compile/export baseline. See `crates/sql/P0.md` for scope.

- 2026-11-16: normalized BIGINT keys pass 594 paired sort/merge checks across
  eighteen synthetic configurations. Including encoding makes twelve-field
  uniform inputs 18–28% slower; common-prefix inputs benefit. These are bounded
  component measurements, not the full N1/N2 result. The parser has a
  source inventory and 31 historical refusal groups. See `crates/sql/P0.md` for limits and review requirements.

- 2026-11-16: the first paired cold-process S2 benchmark measures 1.67–1.69×
  direct compile time across three fixed workloads, with exact artifacts on all
  39 pairs. Removing unused SQL front-end initialization during replay reduces
  the measured ratio to 1.46–1.58×. Raw samples and source snapshots are archived.
  This is a Java/S2/Java
  transport experiment, not a production p95 certification; see
  `crates/sql/BENCHMARKS.md`.

- 2026-11-16: the current Gen-2 suite has 2,218/2,868 passes (77.34%; 79.30%
  excluding skips), with exact test identities against a fresh passing
  compile/export baseline. A pinned on-demand CI workflow and audited runner
  are implemented; remote execution and owner disposition remain open. Parser
  follow-up locates all remaining measured Rust errors without changing any
  verdict or accepted AST. The fold-library split has a documented dependency boundary. See
  `crates/sql/P0.md` for evidence and remaining gates.

- 2026-11-16: captured all 1,143 diagnostic assertions as original SQL
  submissions and repaired eight raw-parser over-acceptances. The expanded
  436-case adversarial set and diagnostic capture have zero Rust-only
  acceptances; positive corpus coverage remains 8,450/8,571. On the original
  adversarial population, exact error starts increased from 60/202 to 119/202
  and missing Rust positions fell from 98 to four. CI includes diagnostic
  replay. Exhaustive rejection and S1 equivalence remain open; evidence is in
  `crates/sql/evidence/parser-error-positions.json`.

- 2026-11-16: the explicit legacy Janino fold profile and corrected optimizer
  boundaries close the measured diagnostic gap. The full S3 audit matches all
  8,571 verdicts, schemas and diagnostics with zero compiler bugs, all 16,796
  comparable Java values, all 1,296 strengthened warning-policy cases and all
  5,240 actual-runtime vectors. Windows/wasm rlib builds pass. Runtime semantics
  remain a separate profile. Unsupported folds, complete site coverage, shared
  kernel extraction and the remaining P0 gates stay open; see `crates/sql/P0.md`.

- 2026-11-16: integer-to-binary casts, text conversions and bounded map folds
  reduce full-corpus diagnostic differences from 114 to 38. All 8,571 verdicts
  and schemas and all 16,738 comparable Java values match; 4,615 actual-runtime
  vectors pass. Window-bound errors now use stable, located messages instead
  of generated compiler IDs. The 1,260-case focused audit passes, but its
  unused-table prefix could mask warning promotion. A stronger audit removes
  that prefix and adds the remaining compatibility boundaries explicitly.
  These results do not close Spike 7 or P0.

- 2026-11-16: explicit legacy EXP/LOG kernels match all 16,429 comparable Java
  fold values. The complete S3 audit preserves all 8,571 verdicts/schemas,
  retaining 114 diagnostic differences. All 996 focused compilations and
  3,137 runtime vectors pass. Java comparisons use the recorded legacy profile;
  runtime checks explicitly retain runtime math. Batch translation eligibility
  and nonfinite-literal repairs remove 12 newly exposed focused regressions.
  Production compatibility negotiation, shared kernels and owner review remain
  open; the profile experiment does not claim Java/runtime math equality.

- 2026-11-16: typed recursive view and standalone LATENESS nodes, followed by
  a five-file vendored parser patch for LEFT ASOF, TABLE arguments and repeated
  ARRAY suffixes, bring coverage to 8,450 accepted and 121 rejected cases
  (1.41%). This meets the 2% parser coverage threshold. All 419 adversarial
  and 13,660 keyword probes have zero Rust-only acceptances; seven older
  overacceptances are repaired and retained as evidence. All 34,015 stock
  comparator submission results are unchanged. Forty-one Rust tests, Clippy
  and eight Java oracle tests pass. Exhaustive rejection, complete spans,
  S1 parity remained open in this run.

- 2026-11-16: typed SQL function declarations bring parser coverage to 8,320
  accepted cases, with 251 rejections (2.93%). A broader adversarial suite
  exposed 35 additional Rust-only acceptances in statement families/options;
  the grammar-based boundary repairs them. All 325 adversarial and 13,660
  keyword probes now have zero Rust-only acceptances. Thirty-five Rust tests
  and Clippy pass. Exhaustive field rejection, spans, S1 parity and the 2%
  threshold remain open.

- 2026-11-16: strict version-4 S2 replay passes all 8,571 classic cases
  (3,503,220 observations per side) and all 8,527 exported Gen-2 cases
  (3,527,714 observations per side). Both engines compare dataflow JSON;
  generated Rust, hash inputs and ids retain exact checks. A new verifier
  rechecks all fingerprints and rejects incomplete or overlapping shard
  populations, changed inputs and weaker contracts. Its tests bring the Python
  harness suite to 59 passes. These certificates use frozen compiler v26;
  later let-binding additions retain their separate v28 certificate.

- 2026-11-16: column LATENESS and aggregate FILTER recover 234 parser corpus
  cases, leaving 282 rejections (3.29%). All 234 adversarial and 13,660 keyword
  comparisons have zero Rust-only acceptances; the 2% gate remains unmet.
  Decimal arithmetic adds 160 passing runtime vectors. The full folding audit
  passes 932 focused compilations and 2,563 runtime vectors, with 142 remaining
  corpus diagnostic differences and six exact math-value differences.

- 2026-11-16: repaired the early temporal fold-site eligibility guard. All 900
  focused compilations and 2,403 actual-runtime vectors pass; the full S3 audit
  preserves all 8,571 verdicts and schemas but retains 162 diagnostic and six
  exact math-value differences. Binary search/hash and UUID byte casts now
  have runtime coverage. Both Gen-2 cases newly exported after the let-binding
  repair pass strict S2 replay, including parsed dataflow and exact hash inputs.
  The existing full version-4 replay run remains a separate frozen build.

- 2026-11-16: version-3 Gen-2 S3 replay passes the 8,527-case exported population;
  classic full S2 replay also passes all 8,571 cases and 3,503,220 prefix
  observations per side. Gen-2 full S2 replay exposed property-position loss in
  generated emit_final windows. Version 4 repairs it; new full classic and Gen-2
  audits require dataflow equality for both engines. Parser repairs
  preserve ROW fields, adjacent strings and four-digit Unicode identifiers;
  unsupported interval qualifiers now fail explicitly. The 0.64 prototype
  rejects 516 of 8,571 cases (6.02%) after interval-CAST and TRIM support,
  so the parser gate remains unmet.
  Reserved-identifier checks remove all 504 Rust-only keyword acceptances
  without changing corpus coverage; 185 adversarial probes also have zero
  Rust-only acceptances. A grammar drift check guards the reserved-word list.
  The completed OVERLAY fold audit retains 268 diagnostic and six exact value
  differences, with 528 focused compilations and 392 runtime vectors passing.
  The full interval-cast audit preserves those 268 diagnostic differences and
  passes 584 focused compilations and 483 runtime vectors. Binary folds then reduce
  diagnostic differences to 198, with 620 focused compilations and 519 runtime
  vectors passing; unsigned support extends those checks to 748 compilations
  and 619 runtime vectors without changing the remaining differences. The later full Java
  compile/export suite passes 2,849 tests with 71 skips. The separate current-tree
  Gen-2 runtime rerun passes 2,197 of 2,849 tests (77.11%, or 79.09% excluding
  skips); it remains below the 80% gate, with no regressions among prior passes.
  A later let-binding type repair removes four Gen-2 compiler crashes: two
  runtime cases pass and two reach an unsupported waterline operator. The full
  classic compile/export suite then passes 2,853 tests with 71 skips, retaining
  all 8,571 validated exports.
  Its full Gen-2 rerun passes 2,203 of 2,853 tests (77.22%, or 79.19% excluding
  skips), without regressing earlier passes. Calendar folding passes 2,347
  runtime vectors but retains 196 corpus diagnostic differences, 48 focused
  diagnostic differences and the six known math-bit differences.
  A native nullable BIGINT batch prototype now emits, links and loads shared
  libraries; 6,732 vectors match actual sqllib values and overflow messages. Dynamic
  DBSP rows, ten-program C7, performance and state-format experiments remain open.
  These experiments do not complete P0 or authorize P1.

- 2026-11-15: full classic S3 replay passes all 8,571 programs with exact graphs,
  artifacts, hash inputs, schemas and diagnostics. The broader S2 prefix audit
  exposed pending LATENESS checks emitted at the wrong time; the version-3 seam
  preserves their timing and passes the focused regression in both engine modes.
  Full audits of that repair remain in progress. The expanded constant-folding
  experiment preserves all corpus verdicts and schemas but still has 276 final
  diagnostic differences and six exact math-value differences. Its 504 focused
  compilations and 370 runtime vectors pass. The full Java compile/export suite
  passes 2,845 tests with 71 skips. A strict Rust type/source schema fragment is
  implemented; the full FRel/FRex/statement model and S1 importer remain open. P0 remains open; evidence and remaining gates
  are tracked in `crates/sql/P0.md`.

- 2026-11-13: added a disposable Rust multiset-analysis and dead-code prototype.
  Fifty predeclared programs pass in both modes at S2/S3: 200 analysis/graph
  comparisons and 200 final-result comparisons after Java resumes Rust-written
  files. Hash inputs, persistent ids, schemas, diagnostics and classic Rust
  artifacts match. The remaining optimizer still runs in Java; the full typed
  IR subset, prefix port and independent review remain open.

- 2026-11-13: implemented restricted S2/S3 file readers with schema, SET,
  diagnostic and relational-provenance restoration. S2 passes 16 focused cases
  (6,174 observations, 48 fresh JVMs); S3 passes 16 final-graph comparisons in
  another 48 JVMs, with exact workspace and hash-input regressions. All 167
  exported CastTests cases pass S2 replay in both classic and Gen-2 populations.
  The full compile/export suite passes 2,820 tests (71 skips). Broader coverage, the Rust subset port, native
  experiments and remaining P0 reviews stay open; see `crates/sql/P0.md`.

- 2026-11-13: full Gen-2 exported-population replay passes after deterministic
  parameter-type allocation: 8,527 cases, 16 fresh JVMs, three captures plus
  baseline, 3,527,714 observations per capture. Exact artifacts, hash inputs,
  schemas and diagnostics match. The original corpus still has failures/skips
  requiring disposition. Evidence: `crates/sql/evidence/gen2-determinism.json`.

- 2026-11-11: started the P0a harness in `crates/sql/sql-difftest`.
  The `sql-difftest` CLI and library compare raw circuit graphs with separate
  inner/outer identities, preserve sharing and semantic fields, and reject
  duplicate definitions and dangling references. Mutation and CLI tests cover
  topology, metadata, cycles, and input errors. This is comparator infrastructure;
  dump capture, envelopes, corpus export, S4 side data, and spike 0 remain open.
  See the crate README for commands and current limits. No phase gate is complete.
- 2026-11-12: added version-1 observation validation and complete-directory
  comparison to `sql-difftest`; 21 Rust tests, Clippy, and formatting checks pass.
  Implemented Java `--dump-circuits`, with required producer/runtime identities,
  S2/S3 capture, nested `Passes`/`Repeat` observations, and completion markers.
  Validated in the repository's devcontainer image on Linux ARM64, with JDK 21,
  Rust 1.99, and Calcite built from the pinned commit: all 5 capture tests,
  14 existing Gen-2 JSON tests, and 21 Rust tests pass, as do Clippy and formatting.
  Seven SQL programs in each engine mode passed three fresh-JVM capture runs
  and a capture-disabled baseline (56 compilations). All 28 directory comparisons
  matched, covering 6,042 observations across the 14 program/mode combinations.
  Schema, dataflow, diagnostics, generated Rust, and Gen-2 circuit output matched
  between the baseline and captured runs. This is focused validation, not the
  full corpus determinism gate; no runtime execution parity is claimed.
  Direct composite-pass substeps, corpus export, lossless S2 replay, pass side
  data, and the three-JVM determinism spike remain open. The dump envelope is
  observation-only and leaves the public Gen-2 payload unchanged.
- 2026-11-12: expanded direct composite-pass observations and made completion
  markers wait for post-compilation checks. Implemented the JUnit corpus export
  hook, strict Rust corpus validation, duplicate-key rejection, and CI export/
  validation/artifact retention. A full compile-and-export run passed 2,777 tests
  (70 skipped), exported all 8,566 registered cases, and validated 10,398 steps,
  232 expected runtime errors and 315 compile-only cases. This exposed and fixed
  duplicate type serialization on VARIANT expressions. Added opt-in Calcite rule
  production counters, correlate summaries and an operator-table exporter.
  `crates/sql/PORTING.md` records S4 state and prefix-replay requirements;
  `crates/sql/P0.md` tracks the measured results and outstanding exit checks.
  P0 is not complete: runtime execution, full determinism, seam replay and the
  remaining prototypes and owner/maintainer reviews are still required.
- 2026-11-12: ran the current compiler's full suite against pinned forge binaries
  built natively in the devcontainer. It produced 2,130 passes out of 2,779 tests
  (69 failures, 510 errors, 70 skips): 78.6% excluding skips, below the 80% gate.
  The expanded capture test passed 56 compilations and 28 comparisons covering
  6,294 observations. The Calcite census found 80 configured rule candidates,
  56 with productions, and 635 validator entries. Implemented a deterministic
  Rust scalar-probe generator and a Java batch oracle: 84,224 probes yielded
  59,456 acceptances and 24,768 rejections, including 1,384 internal compiler
  failures. The oracle-readiness check correctly fails; no deviations were
  silently allowed. These are recorded experimental results, not completed
  P0 exit gates. See `crates/sql/P0.md` and its checked-in evidence.
- 2026-11-12: repaired the two Calcite coercion inconsistencies exposed by the
  probes. The 84,224-probe matrix now has 60,840 acceptances, 23,384 ordinary
  rejections, and zero internal failures. Against a frozen production-mode Java
  baseline, 83,000 records are unchanged and 1,224 formerly failing set-operation
  probes now compile; the comparison includes canonical S2, schema, and messages.
  The full compile/export suite passes 2,791 tests (71 skipped), and the classic
  backend passes 153 generated runtime tests covering set operations and the
  coercion fixes. Added complete probe comparison and full-oracle CI wiring.
  The diagnostic inventory records 329 executed Java call sites and 281 Python
  expectation assignments, with Calcite-template candidates retained for review.
  These results remove the scalar-oracle blocker; the remaining P0 experiments
  and reviews are still tracked in `crates/sql/P0.md`.
- 2026-11-12: repaired identity-dependent ASOF GC scheduling and completed the
  classic repeated-capture run: all 8,571 cases match across three captures and a
  baseline in 24 JVMs, with 3,503,220 observations per capture. Implemented the
  Java-only compiler selector and bounded shadow worker; real-Java tests verify
  admission, comparison, child termination, cleanup, and primary-result isolation.
  The Gen-2 replay exposed a folded-point dataflow serialization failure, now
  repaired with regressions in both engine modes. The full compile/export suite
  passes 2,803 tests with zero failures/errors and 71 skips. The parser experiment
  now measures pinned sqlparser 0.62.0/0.64.0 against Java over the full corpus:
  the partial 0.64.0 Feldera dialect rejects 899 of 8,571 cases, and focused probes
  retain four silent wrong parses. These measurements do not close P0; the seam,
  native, binder, type, folding, benchmark, and review criteria remain open in
  `crates/sql/P0.md`.

- 2026-11-12: measured 13,660 keyword probes over 1,366 words in five contexts;
  both prototype dialect versions over-accept 504 probes. Gen-2 replay found
  411 exact JSON differences caused by replacement type allocation order.
  The repaired allocator is independent of identity hashes and analysis discovery
  order; regression tests pass, and all 8,527 exported cases now match their
  baseline in the first full capture. The remaining repeats are running. Replay workers
  now cancel on output-budget or free-space violations, preserving evidence.
  The rule census resolves all 80 descriptions to 71 implementation classes;
  the proposed scope retains every rule, including 24 without productions.
  Attribution inventories and repairs are recorded in `crates/sql/LICENSING.md`;
  independent maintainer and legal reviews remain open.

- 2026-11-15: the bounded fold registry matches 11,240 comparable Java values
  across the full exported corpus. Rust-assisted compilation matches all 8,571
  cases and 240 focused cases on acceptance, schemas, and complete diagnostics;
  37 edge vectors match actual sqllib execution. NULL window operands exposed
  Java internal failures, now repaired with located diagnostics. The full
  compile/export suite passes 2,828 tests, with zero failures/errors and 71 skips.
  Windows MSVC and wasm32 library builds pass after separating fxp's optional
  random support. Unsupported folds remain counted; complete constant-site
  coverage, shared-runtime extraction, and review remain open.
  `crates/sql/P0.md` and the fold evidence record the measured scope.

- 2026-11-15: inventoried S2/S3 forms across all 8,571 exported classic cases.
  Preselected form-coverage checks pass 4,025 S2 prefix observations and 14 S3
  final graphs. Expanded replay found missing type sharing, compiler-owned
  weight and aggregate-cache state, nested provenance ordering, function type
  ordering, index-sink decoding, and JSON-null encoding; repairs have regressions.
  The targeted suite passes 45 tests. The preceding full compile/export suite
  passes 2,841 tests with zero failures/errors and 71 skips; the final function
  repair is covered by targeted tests. Full-corpus boundary audits continue with
  failures retained and counted, not excluded. P0 remains in progress.

## 1. Present state

### 1.1 Size

| Part | Files | Lines | Note |
|---|---|---|---|
| Main Java, `SQL-compiler/src/main` | 853 | 127,713 | |
| Test Java, `SQL-compiler/src/test` | 223 | 87,099 | 2,478 `@Test`, 23 with `@Ignore` |
| Parser templates `G/` | 3 | 1,515 | `G/config.fmpp` 1,000, `G/includes/ddl.ftl` 411, `G/includes/parserImpls.ftl` 104 |
| SLT runner `sql-to-dbsp-compiler/slt` | 13 | 1,460 | |
| Simulator `sql-to-dbsp-compiler/simulator` | 55 | 2,535 | no SQL, not a compiler constraint |
| Rust helper crates `sql-to-dbsp-compiler/lib/{readers,sltsqlvalue,hashing}`, `temp/` | | | used by generated test code; `Cargo.toml:27,39-43,359` |
| Calcite | external | | 1.43.0 unreleased, pinned at commit `4bf922bb`, built from source; `sql-to-dbsp-compiler/calcite_version.env:1-7`. The pin moved in 28 commits in the last 12 months. |

### 1.2 Pipeline and Calcite dependency

```
SQL text
  | JavaCC parser (Calcite Parser.jj + G/ templates)        Calcite: very deep
  v
SqlNode --> validator (types, coercion, overloads)           Calcite: very deep
  | SqlToRelConverter + ConvertletTable (2,641 lines copy)   Calcite: very deep
  v
RelNode/RexNode --> 32 steps: 29 HEP programs, 3 direct      Calcite: deep
  |   calls (RejectUnsupportedPlans, CorrelateProjectExtractor,
  |   RelDecorrelator); 63 stock rules, 10 cloned, 13 custom
  v
CalciteToDBSPCompiler (3,437) + ExpressionCompiler (2,713)   reads 17 RelNode classes, 7 Rex kinds
  | + aggregates/ (2,559)
  v
DBSP circuit IR  ======= JSON form exists (246 decoders) =======
  | CircuitOptimizer: 94 steps, 81 pass classes              28 Calcite imports (IR, passes, back end)
  v
ToRust visitors (multi-crate workspace) | Gen-2 IR JSON | schema.json | dataflow.json
```

| Part | Directories | Lines | Calcite imports | Front-end classes used | Port mode |
|---|---|---|---|---|---|
| Front | `F/` (190 files), `G/` | 34,466 + 1,515 | 1,244 | | Rewrite on a new base; lowering (9.0k) ports 1:1 |
| IR | `J/ir`, `J/circuit` | 31,760 | 9 | `CalciteObject` family for provenance; static helpers of `ExpressionCompiler` and `TypeCompiler`; avatica `DateTimeUtils` and Calcite date and time string classes in the literals | Mechanical port |
| Mid-end | `J/compiler/visitors/{inner,outer,monotone,unusedFields}` | 43,440 | 17 | Same helpers (28 call sites; 34 with IR and back end); Calcite date and time string classes in `Simplify`; `SqlFunctions.stringToUuid` | Mechanical port, same order |
| Back end | `J/compiler/backend` | 10,396 | 2 | | Mechanical port |
| Driver, errors, util | `J/compiler/*.java`, `J/compiler/errors`, `J/*.java`, `J/compiler/visitors/*.java`, `org/dbsp/util` | 7,651 | 33 | | Port |

The import count understates the coupling: 271 of 588 files in IR, mid-end and back end import classes of `J/compiler/frontend` (mostly the provenance wrappers `CalciteObject`, `CalciteRelNode`, `CalciteEmptyRel`). The Rust IR crate must hold a frontend-neutral provenance type and the helper functions.

Key code locations:

| Subject | Location |
|---|---|
| Parser and validator configuration (Lex.ORACLE, lower-case fold, LENIENT, NULLs low) | `CC/SqlToRelCompiler.java:274-349`, `:689-735` |
| Type system overrides (DECIMAL 38/38, TIME 9, TIMESTAMP 6) | `CC/SqlToRelCompiler.java:447-481` |
| Documented function list: 253 Calcite entries (178 STANDARD, 75 from 6 other libraries) | `CC/functions/CalciteFunctions.java:44-724` |
| Validator operator table: the whole Calcite STANDARD table minus `JSON_*` and `ABS`, the Calcite spatial table, the 75 entries, and 61 custom functions (53 classes) | `CC/functions/CalciteFunctions.java:760-776`, `CC/functions/CustomFunctions.java:58-118` |
| Rule program, 32 steps | `CC/optimizer/CalciteOptimizer.java:253-552` |
| Decorrelation entry points | `CC/optimizer/CalciteOptimizer.java:447-462`, `CC/optimizer/InnerDecorrelator.java:30` |
| Metadata: custom row count plus the default Calcite provider | `CC/SqlToRelCompiler.java:377-379` |
| Statement dispatch of the lowering (10 statement classes in 9 branches; CREATE TABLE and DROP TABLE share one) | `F/CalciteToDBSPCompiler.java:3358-3398` |
| RelNode dispatch | `F/CalciteToDBSPCompiler.java:2832-2872` |
| Rex dispatch, one switch on SqlKind (89 labels, 110 named functions) | `F/ExpressionCompiler.java:1115-2517` |
| Hand-off of the unoptimized circuit to the optimizer | `J/compiler/DBSPCompiler.java:838-847` |
| Pass list | `J/compiler/visitors/outer/CircuitOptimizer.java:119-255` |
| Optional JSON round-trip check after each top-level step | `J/compiler/visitors/outer/CircuitOptimizer.java:257-264` |
| Circuit JSON decoder | `J/circuit/DBSPCircuit.java:315-325`, `J/compiler/backend/JsonDecoder.java` |
| Persistent id computation | `J/compiler/visitors/outer/merkle/MerkleOuter.java:90-132`, `:180-288`; `merkle/MerkleInner.java:26`, `:88-94` |

Invocation sites of the compiler:

| Caller | Location | Flags |
|---|---|---|
| pipeline-manager (the product) | `PM/compiler/sql_compiler.rs:793-826` | `-i -je --alltables --ignoreOrder -js --dataflow`, then `-o --crates --runtime` or `--gen2 -o`, plus `--enterprise` |
| felderize | `python/felderize/felderize/feldera_client.py:13-58` | `-i --ignoreOrder --alltables --noRust`; reads the plain-text stderr format |
| CI demo validation | `scripts/validate-demos.sh:7-8` | `-i --noRust -je`; fails on a warning |
| Launcher script `SQL-compiler/sql-to-dbsp` | `deploy/Dockerfile:107`, `sql-to-dbsp-compiler/using.md` | documented for users |
| JUnit and SLT | `T/sql/tools/` | Java API |

### 1.3 Facts that shape the plan

| Fact | Consequence |
|---|---|
| The optimizer is a fixed sequence of steps with no Volcano search. The only custom metadata is row count (table property `expected_size`); the default Calcite metadata (pulled-up predicates, uniqueness, selectivity) and RexSimplify stay in use. | An ordered pass list in Rust is sufficient. The rule port needs a small metadata module and a simplifier. |
| No custom RelNode classes. The lowering reads 17 RelNode classes (LogicalCorrelate for UNNEST, 16 others) and 7 Rex kinds. | A Feldera relational IR (FRel) that mirrors these kinds lets the 9.0k lines of lowering port 1:1. |
| The circuit IR has a JSON form with 246 decoders. All 73 concrete operators have one. | The IR JSON is the base of a staged migration. |
| The round-trip check is optional and only 8 tests enable it (5 in `SerializationTests`, 3 in `Gen2JsonTests`). It compares up to node ids, node sharing, source positions and program metadata (`J/compiler/visitors/outer/TestSerialize.java:19-22`). No test gives a decoded circuit to the optimizer. | The seam needs real work before it can carry a hybrid compiler, section 7.1. |
| `--gen2` changes the circuit at 7 code sites, the first in the lowering (`F/CalciteToDBSPCompiler.java:3286`). | There are two circuit variants (classic and Gen-2). Each seam and each oracle exists in the two modes. |
| A persistent operator id is a sha256 over the JSON text of an operator skeleton (`JsonStream`, indent 1, Java class tags and label order). In the skeleton, each inner node is the sha256 of its generated Rust text. For a non-input operator the hash also holds the output types as printed by `DBSPType.toString()` (`MerkleOuter.java:242-250`). The table hash holds the table metadata (`J/compiler/TableMetadata.java:101-125`). | Equal ids need three byte-equal texts, also in Gen-2 mode: the Rust printer text, the debug type text, and the skeleton JSON text. The Rust compiler does not make these texts. In the hybrid stages the Java tail computes the present ids from the S3 that Rust writes. The native form has a new id scheme (section 4.2). |
| Feldera already has two SQL dialects: compiled (Calcite) and ad-hoc (DataFusion), with documented differences (`docs.feldera.com/docs/sql/ad-hoc.md:24-45`). | Stock DataFusion planning does not give parity. |
| In the last 12 months, 603 commits added 99k Java lines and removed 26k. The 509 non-bot commits averaged 1.4 per day. In IR, passes and the back end, 277 commits added 35k lines. | The plan needs an explicit method to follow the Java code, section 7.4. |
| Java Tests is the longest CI job (41 to 52 minutes). The runtime image carries a JRE and a 29 MB jar. | Gains at M4 (gate G8), section 9. |

## 2. The contract a replacement must honor

| Output or behaviour | Consumer | Required level |
|---|---|---|
| CLI flags of the callers in section 1.2, exit code | pipeline-manager, felderize, CI and documented standalone callers | Preserve the options and exit codes during the migration. The [decision 8 audit](crates/sql/CLI-SCOPE.md) accounts for all 47 named options, including internal replay controls. |
| Messages JSON (8 fields: range, `warning`, `error_type`, `message`, `snippet`), for errors and for warnings of accepted programs (74 warning sites: 73 `reportWarning` calls and the lenient `reportProblem` at `CC/SqlToRelCompiler.java:1453`) | `PM/db/types/program.rs:105-114`, web console, Python SDK, fda | Same fields, verdict, `error_type`, start position and `snippet`. Text can change with review (decision 3). |
| Plain-text message format `file:line:col: error: ...` | felderize | Same format, or change felderize in the same release |
| Schema JSON (`inputs`, `outputs`, `columntype`, `unused`, `properties` with positions, `default` and `lateness` SQL text) | `crates/feldera-types/src/program_schema.rs:160-307`, `:737-770`; connectors | Equal after key normalization. `columntype` is the Calcite RelJson type form. The `default` and `lateness` text is Calcite Rex-to-SQL output (`F/calciteObject/CalciteRelNode.java:54-60`), not source text. |
| View column types and nullability | Connectors, Rust UDF ABI, checkpoints | Equal to the behaviour of the pinned Calcite commit, except recorded deviations |
| Dataflow JSON: `calcite_plan` (Calcite RelJson form, `CC/RelJsonWriter.java`), `mir`, `sources` | `crates/ir/src/lib.rs:36-137`, `crates/ir/src/hir.rs`, `js-packages/profiler-lib/src/dataflow.ts` | Same shape. Add a version field. |
| Multi-crate Rust workspace (`_main`, `_globals`, one crate per operator, `stubs.rs`) and its dependency list (`J/compiler/backend/rust/multi/CrateGenerator.java:112-139`) | `PM/compiler/rust_compiler.rs:1499-1717` | Java back end only. The Rust compiler does not produce it (section 4.2). |
| Generated code calls into `feldera_sqllib`, `dbsp`, `feldera_types`, `static_connectors`, `feldera_macros`, `feldera_adapterlib` | rustc | Java back end: unchanged. Native back end: the same functions through a C-ABI export table, with the same naming scheme (`J/compiler/backend/rust/RustSqlRuntimeLibrary.java:143-214`) as the table key. |
| Rust UDF stubs and type mapping; preprocessor and postprocessor factory registration (`J/compiler/backend/rust/ToRustVisitor.java:275-307`) | User code in `udf.rs` | Source contract unchanged. In the native form the UDF crate is compiled separately to a shared library (section 4.2), so the binary interface is new. |
| Circuit IR JSON (`--gen2`), consumed with `mir` and the schema as `program_ir` | Gen-2 engine in feldera/forge | Same tags (Java class names, `org/dbsp/util/JsonStream.java:61-63`) and structure |
| Persistent operator ids | Checkpoints, `Dataflow::diff`, crate names | Equal when the circuit is equal. Table ids are always equal. A different view id needs a bootstrap, which the runtime refuses for a program with LATENESS or with a non-materialized table (section 7.5.3, decision 4). |
| SET options: `FELDERA_DISABLE_OPTIMIZATIONS` (takes pass class names), `FELDERA_IGNORE_WARNING_<error_type>`, `FELDERA_WARNINGS_ARE_ERRORS`, `FELDERA_AVOID_STAR_JOINS`, `FELDERA_FLAT_VARIANT`, `FELDERA_LAZY_UPSERT`, `FELDERA_WINDOW_SHARING_THRESHOLD`, `ENFORCE_POSITIVE_INPUTS` | Users; `J/compiler/ProgramMetadata.java:188-203`, `J/compiler/DBSPCompiler.java:345-353`, `docs.feldera.com/docs/sql/grammar.md:770-860` | Same names and effect. The pass names that users give equal the Java class names (compatibility mapping, section 4.1). |
| SQL surface: 293 documented functions; CREATE TABLE, CREATE [LOCAL or MATERIALIZED] VIEW, DECLARE RECURSIVE VIEW, CREATE TYPE, CREATE FUNCTION, CREATE AGGREGATE, CREATE INDEX, DROP, LATENESS, REMOVE, INSERT, SET (11 added statement parsers, `G/config.fmpp:907-952`); PRIMARY KEY, FOREIGN KEY, DEFAULT; table and view properties; synthesized objects (ERROR_VIEW, the error table, NOW); `CONNECTOR_METADATA()`; streaming features | Users, `docs.feldera.com/docs/sql/` | Full, section 6 |
| Generated documentation: `docs.feldera.com/docs/sql/function-index.md` comes from the function registry (`CC/functions/FunctionDocumentation.java`); tests check the reserved-keyword table and the documentation links in messages | Docs | Same generator and checks in Rust |
| Per-version compiler download for old `runtime_version` values | `PM/compiler/sql_compiler.rs:452-586` | Keep the jar path for old versions (decision 5) |

## 3. Technology choice for the front end

### 3.1 Options

Scores: 1 (poor) to 5 (good). They are judgments from source reading and web research, not measurements.

| Option | Syntax coverage | Binder exists | SQL type system | Decorrelation | Lowering port mode | Extensible | Verdict |
|---|---|---|---|---|---|---|---|
| A. sqlparser-rs + Feldera binder, types, FRel, rules | 4 | 1 (absent in Rust) | 5 (own) | 1 (absent in Rust) | 1:1 | 5 | Default |
| H. Hybrid: sqlparser-rs + DataFusion `SqlToRel` as binder only (Feldera `ContextProvider`, `TypePlanner`, `ExprPlanner`, `RelationPlanner`, a Feldera analyzer rule in place of `TypeCoercion`). No DataFusion optimizer rule. Intercept incompatible transformations and typing during planning, then convert the analyzed `LogicalPlan` to FRel while preserving SQL set-operation provenance (section 3.4). | 4 | 4 | 3 (Feldera types as `Field` metadata) | 1 (same requirement as A) | 1:1 | 4 | Candidate, measured against A in spike D1 |
| I. Translate the Calcite validator and `SqlToRelConverter` to Rust, as this plan does for the decorrelator and other Calcite parts | 4 | 1 (Java implementation available as a reference) | 5 | 1 (absent in Rust) | 1:1 | 4 | An implementation mode within A. It needs a SqlNode-like AST and must meet section 4.1. Spike 5 compares one translated slice with a rewrite. |
| B. DataFusion `SqlToRel` + `LogicalPlan` + optimizer own the plan | 4 | 4 | 2 | 2 | Redesign | 4 | Rejected, section 3.2 |
| C. Keep Calcite, Rust back half | 5 | 5 | 5 | 4 | n/a | 5 | It is the interim state of this plan and the fallback if the front end fails. The JVM stays. |
| D. Calcite with GraalVM native-image | 5 | 5 | 5 | 4 | n/a | 5 | Not Rust. Removes the JVM only. Parser, validator and SqlToRel run in a native image today (isthmus-cli of substrait-java). Unproven for the HEP rules with the default Janino metadata provider and for the Janino constant reduction (CALCITE-4786 open). A variant, a native-image Calcite front end to S2 with the Rust back half, is the review item of the decision tree in section 12. |
| E. RisingWave front end (Apache-2.0) | 4 | 4 | 3 | 4 | Redesign | 2 | Not a library (14 workspace crates). PostgreSQL semantics. Design reference. |
| F. Materialize `mz-sql`, `mz-expr`, `mz-transform` | 4 | 4 | 4 | 5 | Redesign | 2 | PostgreSQL semantics, coupling to `mz-repr`, not a library. BSL 1.1 for versions younger than 4 years; v0.27.0 (change date 2026-10-12) and older are Apache-2.0, so a code copy from those is possible with attribution (confirm in the P0b legal review). Design reference. |
| G. DuckDB as planner, Databend planner, GlueSQL, Polars SQL, pg_query.rs, optd, egg, Substrait | varies | varies | 1 to 3 | varies | | 1 to 4 | Wrong dialect, wrong scope, or not a library |

No surveyed Rust project is a drop-in replacement for Calcite as a library.

Options C and D have the highest scores because Calcite stays. Their verdict comes from a criterion that the columns do not show: with C the JVM and the Calcite build stay, and D is not Rust. Decision 12 covers a stop at C.

### 3.2 DataFusion: gaps by option

| Gap | Evidence | B (owns plan) | H (binder only) |
|---|---|---|---|
| Types are Arrow types. CHAR(n), VARCHAR(n), the 13 interval qualifiers, VARIANT and user types have no Arrow form. BINARY(n), TIME and UUID have one. Extension types exist (`arrow.uuid`, `arrow.json`) but customize only value printing. | `DF/datafusion-common-55.2.0/src/types/native.rs:140-184`, `types/extension.rs:44-45`; epics apache/datafusion#12622 (open since 2024-09, closure proposed 2026-10-23) and #12644 (active) | Type properties travel as `Field` metadata through all rules | Same, but only through planning and analysis |
| Binary expressions return a field with no metadata, by design. | `DF/datafusion-expr-55.2.0/src/expr_schema.rs:492`, `:558-574` | Each operator must be routed through `ExprPlanner::plan_binary_op` to a Feldera function with `return_field_from_args` | Same |
| Literal typing is fixed: an integer literal is BIGINT (Calcite: INTEGER), a string literal is Utf8 (Calcite: CHAR(n)). No hook. | `DF/datafusion-sql-55.2.0/src/expr/value.rs:52`, `:102-104` | Preserve original literal types before planning; retype only where planning has not already rejected or coerced them | Same; D1 inventories early consumers |
| Coercion and result types follow Arrow rules. `coerce_types` sees `DataType` only. | `DF/datafusion-expr-55.2.0/src/udf.rs:389`; `datafusion-expr-common-55.2.0/src/type_coercion/binary.rs` | Replace the `TypeCoercion` rule and intercept incompatible typing during plan construction; analyzer replacement alone is insufficient | Same; D1a must reach analysis without losing SQL type information |
| Planner errors have few source positions: 15 diagnostics (12 `Diagnostic::new_*`, 3 `with_diagnostic`) for 145 `plan_err!` sites; none in type coercion. | `DF/datafusion-sql-55.2.0/src` | The messages contract needs positions added upstream or a wrapper | Same |
| Join operators have no planner hook. Table factors have one (`RelationPlanner`). | `DF/datafusion-sql-55.2.0/src/relation/join.rs:27-103`, `relation/mod.rs:115-143` | ASOF needs an AST rewrite into a nested join plus a `RelationPlanner`, or an upstream change | Same |
| Missing plan nodes: ASOF join, PIVOT, TUMBLE/HOP/SESSION, Collect. 80 `not_impl_err!` sites in query planning (241 in the crate; 123 are in `statement.rs` and 38 in the unparser, which Feldera does not use). | `DF/datafusion-sql-55.2.0/src` | Extension nodes | Extension nodes, converted to FRel |
| Decorrelation is incomplete and gives another plan shape (LeftSemi, LeftAnti, LeftMark joins, which the lowering rejects at `F/CalciteToDBSPCompiler.java:1440-1441`). A maintainer comment on apache/datafusion#5492 (2026-10-28) lists 16 open wrong-result issues and 17 open unsupported-query issues; the general framework is a prototype with no open PR. | `DF/datafusion-optimizer-55.2.0/src/decorrelate_predicate_subquery.rs:277-319` | Own decorrelator, or accept the regression | DataFusion decorrelation is not used; sub-queries are removed on FRel. Planning-time set-operation rewrites still need interception. |
| SQL planning itself lowers INTERSECT/EXCEPT to LeftSemi/LeftAnti joins, including ALL variants. Disabling optimizer rules does not prevent these shapes. | `DF/datafusion-sql-55.2.0/src/set_expr.rs:164-174`; `datafusion-expr-55.2.0/src/logical_plan/builder.rs:1362-1449` | Audit set-operation semantics and bag multiplicities | Preserve the SQL operation and quantifier in the H adapter; build FRel Intersect and Minus explicitly. Do not infer the original operation from a generic semi/anti join. |
| VALUES planning checks metadata equality, resolves common Arrow types, and inserts casts before the analyzer runs. | `DF/datafusion-expr-55.2.0/src/logical_plan/builder.rs:300-380` | Intercept incompatible early typing | A later analyzer cannot recover an early rejection or discarded type information. Use a Feldera planning path or a scoped upstream patch. |
| The optimizer rules target batch execution, and the stock per-rule schema check ignores metadata and nullability. | `DF/datafusion-optimizer-55.2.0/src/optimizer.rs`; `datafusion-common-55.2.0/src/dfschema.rs:607-618` | Rule-by-rule audit | Does not apply |
| The lowering is written against RelNode, Rex and SqlKind. | `F/CalciteToDBSPCompiler.java`, `F/ExpressionCompiler.java` | Requires redesign and replay of later Java fixes | Does not apply: FRel is unchanged. H adds a `LogicalPlan` to FRel converter. |
| Churn: a major release each 10 to 11 weeks with 44 to 57 upgrade-guide entries. The version is coupled to the delta-rs and iceberg forks. The repo made 5 DataFusion upgrades in 20 months. | Upgrade guides 55 and 56; `Cargo.toml:148,172,226` | Compiler API surface added to each upgrade | Smaller API surface (`datafusion-sql`, `datafusion-expr`), but a large dependency closure: `datafusion-sql` needs `datafusion-functions-nested`, which pulls `datafusion-functions`, `datafusion-functions-aggregate` and `datafusion-execution` (tokio, object_store, tempfile), and Arrow. An `AnalyzerRule` also needs `datafusion-optimizer`; a plain function over `LogicalPlan` does not. This affects the static binaries and the wasm item. |
| Forks: the three surveyed systems that build streaming plans on DataFusion carry a DataFusion fork (Arroyo, GreptimeDB, Denormalized; the last is inactive). Sail implements the Spark dialect with its own parser and resolver, with DataFusion `Expr` and `LogicalPlan` as the target and no fork; it does not use `SqlToRel`. No surveyed system uses `SqlToRel` as the binder of a foreign dialect. | Their `Cargo.toml` files, Appendix C | High fork risk | Unmeasured until D1 inventories required planning hooks and patches |

Shared front-end work and option-specific interfaces

| Item | Reason |
|---|---|
| Function registry (the validator table of section 1.2) | The rows hold name, operand rule, return-type rule and nullability in each option. With DataFusion each row also implements `ScalarUDFImpl`. No DataFusion built-in function is registered, to keep control of names and types. `SqlToRel` still calls `array_has`, `array_min` and `array_max` directly for a quantified comparison with no sub-query; D1a lists such paths. |
| Convertlet logic (2.6k lines of Java) | It is written against Calcite SqlNode, which exists in no Rust option. A rewrites it in the HIR to FRel builder. H has no HIR, so the same logic moves into its `ExprPlanner` hooks and its `LogicalPlan` to FRel converter. |
| Decorrelator, rules, plan builder | Written on FRel in A and H. |
| sqlparser-rs fork risk | Equal: Arroyo, GreptimeDB, RisingWave and Materialize all carry a sqlparser fork. Not equal: in H the parser must emit, or bridge to, the AST of the sqlparser version that `datafusion-sql` pins (0.61 with 53.1, 0.62 with 55.2), so a patched sqlparser needs a workspace patch or a DataFusion fork. In A the version is free, and crates.io releases are faster than the DataFusion pin. |

H would reuse some scope resolution and plan construction and would
convert its expression and plan objects to FRel. Feldera still supplies
coercion, type signatures, decorrelation and function semantics. The D1
evidence records the prototype's actual reach through these boundaries.

### 3.3 Other pieces of the DataFusion ecosystem

| Piece | Decision |
|---|---|
| sqlparser-rs 0.62 (69k lines, already in `Cargo.lock`) | Use for syntax in A and H. Limits in section 6.2. Only one crate sees its AST. On 2026-11-11 the releases 0.63.0 and 0.64.0 exist; the pinned 0.64.0 source supplies a datatype hook but no join or table-factor hook. Spike 4 runs on 0.62 and on the newest release. |
| `DFParser` (`DF/datafusion-sql-55.2.0/src/parser.rs`, used today in `crates/connectors/src/adhoc.rs`) | Pattern for the wrapper parser of the Feldera statements |
| `TreeNode`, `Transformed` (`datafusion-common`) | Copy the API design for HIR, FRel and FRex. No dependency: the trait fixes `DataFusionError`. |
| sqlparser AST `Display` | Unparser for `--format`. Not for `default` and `lateness` text, which is Rex-to-SQL output. |
| `ScalarValue`, Arrow kernels, `ExprSimplifier` | Not used: they fold with Arrow semantics; this plan folds with sqllib. The rewrite list is a reference. |
| Substrait | Not used. S1 carries statement envelopes, source positions and column metadata that Substrait does not have. Correlate and Uncollect, ASOF, the 13 interval qualifiers, VARIANT and the 61 custom functions need extensions. An importer and an exporter are necessary in each case. |
| DataFusion for ad-hoc queries | Unchanged in this project |
| One dialect for compiled and ad-hoc SQL | Later option: lower FRel to a DataFusion `LogicalPlan` for ad-hoc execution. Section 14. |

### 3.4 Spike D1: compatibility comparison of H and A in P0b

| Step | Design | Measure |
|---|---|---|
| D1a, inventory | List the early typing and coercion sites of `SqlToRel` in VALUES, nested expressions, CASE, function calls, joins and set operations. Classify each as compatible, intercepted, or patch needed. List the `SqlToRel` paths that build a DataFusion function or expression with no registry lookup and no hook. The first entry is the quantified comparison `x op ANY(...)`, which calls `array_has`, `array_min` and `array_max`. | Count per class and an inventory of interception paths |
| D1a, prototype | Prototype H for the 40 functions of spike 5: a Feldera `ContextProvider` (signature-only functions with `return_field_from_args`), `TypePlanner::plan_type_field`, `ExprPlanner::plan_binary_op`, a Feldera coercion step in place of `TypeCoercion`, a literal retyping pass, and interception of incompatible early typing. Keep the original SQL types and source spans through each conversion. Metadata added after a lossy cast is not sufficient. | C0, C1a and C9 agreement on the spike 5 probes (mixed CHAR lengths, DECIMAL precision and scale, typed NULLs, nested VALUES expressions); plans that Java accepts are built; no early rejection and no lost type |
| D1b | Run the D1a prototype on the full corpus. | C0 and C1a against Java; scope of the sqlparser AST bridge; count of Feldera forms with no stock AST form (`LEFT ASOF JOIN` and the `TABLE` argument are known); count of AST pre-rewrites; count of upstream patches |
| D1c | Informative, not a gate. Run the corpus sub-queries through the 3 DataFusion decorrelation rules. | Plans that keep a `Subquery`; plans with semi, anti or mark joins; plans whose run-time result differs from Java |
| D1d | Mandatory execution test of the H set-operation handling. Keep the AST operation and quantifier before DataFusion rewrites them, build FRel Intersect and Minus, and execute through the S1 Java importer, the existing lowering and the runtime. D1d depends on spike 11. If that path is not available, an equivalent executable oracle is necessary before H can be selected. | C7 agrees for DISTINCT and ALL, NULLs, empty inputs, nested operations and unequal duplicate counts. The lowering-boundary FRel has no semi or anti join. |

D1d test case: the left input has three copies of a row and the right input has two. INTERSECT ALL must give two copies and EXCEPT ALL must give one. Do the test again with rows that contain NULL and with reversed inputs. A schema comparison cannot tell these results from a membership join.

Decision rule, fixed before the spike. The owners of decision 9 apply it.

| Item | Rule |
|---|---|
| Parity floor | A and H use the same feature subset, the same probes and the same held-out corpus split. Each side must meet C0, C1a and C9 on positive and negative cases, and must keep types and source positions through nested expressions. H must also pass D1d. |
| Scope | Both prototypes reach FRel on the held-out statements. For H, report the adapters, literal retyping, convertlet logic, early typing interception, set-operation handling, sqlparser AST bridge, and the `LogicalPlan` to FRel converter. |
| Incomplete evidence | Extend the spike once or stop it. H is eligible only after both the parity floor and its executable oracle pass. |
| Report | Coverage and unexplained mismatches are reported separately. A named cause does not make a mismatch acceptable. |

The plan owner stopped D1 on 2026-11-18 under the incomplete-evidence rule. A remains the planning assumption. Equivalent held-out FRel and full C0/C1a remain open. [D1 evidence and decision](crates/sql/D1.md#decision-on-2026-11-18)

For binder A, the owner chose a Feldera-owned validator and typed SQL-to-FRel
lowering on 2026-11-18. Translate Calcite's coercion and return-type rules
where they determine observable behavior. Keep Rust ownership, interfaces
and control flow under section 4.1.
[Spike 5 implementation modes](crates/sql/TYPE-DERIVATION.md#accepted-implementation-modes-for-binder-a)

The parser crate, type model, function registry rows, FRel, decorrelator, test corpus and mid-end and back-end tracks keep their value.

## 4. Target architecture

```
SQL text
  | sql-parser    sqlparser-rs + Feldera parser code (section 6.2), spans, structured error location
  v
AST ------------> AST unparser (--format)
  | sql-binder    scopes, aliases, overloads, implicit casts   <-- sql-types, sql-functions, sql-catalog
  |               A: Feldera code.  H: adapter over datafusion-sql, then LogicalPlan -> FRel
  v
HIR   typed; sub-queries are expressions; correlated references carry a scope level
  |               (HIR, FRel and FRex types are in sql-rel-model)
  | sql-rel       FRel builder (convertlets, PIVOT, GROUPING SETS), plan builder and Rex utilities,
  |               ordered rule list, metadata module, sub-query removal, decorrelator, folding,
  |               writers: Rex to SQL text, RelJson form for types and plans
  v
FRel/FRex   at this boundary: only the 17 RelNode classes and 7 Rex kinds read today
  | sql-lower     1:1 port of CalciteToDBSPCompiler, ExpressionCompiler, aggregates/
  v
circuit IR (sql-ir)  ===== seam S2: unoptimized circuit JSON (classic and Gen-2 variant) =====
  | sql-opt       81 pass classes in the Java order; monotonicity and GC verbatim
  |               (uses sql-expr)
  v
circuit IR, final    ===== seam S3: optimized circuit JSON (classic and Gen-2 variant) =====
  | sql-native    row layouts, Cranelift code, object file, new persistent ids, mir (section 4.2)
  |               hybrid stages: the Java tail reads S3 and writes the Rust workspace
  | sql-compiler  output module: schema JSON, dataflow JSON (writers by stage: section 7.3)
  v
outputs
```

The diagram shows the A path. H has no HIR: it converts the analyzed LogicalPlan
to FRel, and the convertlet logic moves into the H adapter (section 3.2). Both
paths enter the same FRel rule pipeline. The model crate
is independent of both implementations, so the H adapter does not introduce a
`sql-binder`/`sql-rel` dependency cycle.

| Crate (under `crates/sql/`, package prefix `feldera-sql-`) | Content | Depends on | Replaces |
|---|---|---|---|
| `sql-parser` | Dialect, token pass, parser code for data types and FROM items, DDL AST, spans on all nodes, hints, AST unparser, reserved-word table, comment scanner | sqlparser (pinned or vendored, spike 4) | `G/`, `F/parser`, `F/SqlCommentParser.java` |
| `sql-types` | SQL type model with precision, scale, nullability, CHAR(n), 2 interval families and 13 qualifiers, ROW, MAP, VARIANT, user types; least-restrictive type; decimal derivation; cast matrix; RelJson type form for `columntype` | `feldera-types` | Calcite type factory, `F/TypeCompiler.java` |
| `sql-functions` | Declarative registry, one row per function: name, aliases, operand rule, return-type rule, nullability rule, sqllib name stem, documentation anchor, test file; generator of `function-index.md` | `sql-types` | Calcite operator tables, `CC/functions/` |
| `sql-catalog` | Tables, views, types, UDFs, UDAFs, indexes, foreign keys, properties with positions, SET options, synthesized objects; compiler connector validation over shared Rust config types, preserving warning policy, semantic checks, and JSON-to-SQL positions (section 6.3) | `sql-types`, `sql-rel-model`, `feldera-types` | `F/statements`, `F/connectors` (replace mirror types; retain compiler validation behavior) |
| `sql-rel-model` | HIR, FRel, FRex, statement envelopes, source spans, and diagnostics shared by both binder implementations; no planner or execution dependency | `sql-types`, serde | Neutral front-end contracts |
| `sql-binder` | Validator replacement; A emits HIR; H converts its analyzed LogicalPlan directly to FRel and diagnostics | `sql-parser`, `sql-types`, `sql-functions`, `sql-catalog`, `sql-rel-model`; in H also `datafusion-sql`, `datafusion-expr` and their closure (section 3.2) | Calcite validator |
| `sql-rel` | HIR to FRel for A; common FRel plan builder, pass driver, rules, metadata, simplifier, decorrelator, constant folder, serializers | `sql-rel-model`, `sql-functions`, a fold crate without dbsp that is split from `feldera-sqllib` (spike 7; today `crates/sqllib/Cargo.toml:24` depends on dbsp, which has Unix-only code) | SqlToRel, `CC/ConvertletTable.java`, `CC/optimizer/`, RelBuilder, RexUtil, RexSimplify, Janino, rel2sql, RelJson |
| `sql-lower` | FRel statements to circuit IR | `sql-rel-model`, `sql-ir` (no plan builder and no Rex utility is necessary after the Sarg expansion of S1) | `F/CalciteToDBSPCompiler.java`, `F/ExpressionCompiler.java`, `F/aggregates/` |
| `sql-ir` | Operators (73 concrete), expressions (88 concrete subclasses of `DBSPExpression`), types (45), arena graph, JSON with the Java class tags, traversal derives, provenance type (port of `F/calciteObject` without RelNode), the static builders used by passes (`makeBinaryExpression`, `makeBinaryExpressions`, `makeZSet`, `makeIndexedZSet`, `expandTuple`, `combineAggregateList`) | serde | `J/ir`, `J/circuit`. Separate from the existing `crates/ir` (`feldera-ir`: dataflow summary), which stays. |
| `sql-expr` | Shared circuit-expression transformations and traversal helpers, including struct elimination and cast error wrapping; explicit context and options, no pass scheduler or code emitter | `sql-ir` | Shared subset of `J/compiler/visitors/inner` |
| `sql-opt` | Passes with the Java class names as compatibility names, combinators (`Passes`, `Repeat`, `Conditional`). The Merkle hashes of the present id scheme stay in the Java tail. | `sql-ir`, `sql-expr` | `J/compiler/visitors` |
| `sql-native` | Native back end, section 4.2; `mir` writer | `sql-ir`, `sql-expr`, cranelift | `J/compiler/backend` (not a port) |
| `sql-compiler` | Binary `sql2dbsp` with the Java flags, which is the service entry point (section 9.1). Library API `compile(sql, options) -> CompileOutput { program: Option<ProgramInfo>, messages }` for trusted callers (warnings survive a success). Output module for the schema JSON and the dataflow JSON. | all | `J/CompilerMain.java` |
| `sql-difftest` | Corpus reader, comparators, allow-list, probe generator, SLT runner, Rust test authoring format (text tables parsed with the Rust-inferred types) | `sql-compiler` | JUnit harness, `slt/`, `temp/`, `lib/` |

Crates that section 7.2 does not name: `sql-rel-model` belongs to track F (schema v0 in P0b); `sql-compiler` gets its entry point in P2 and its front half in P5d; `sql-difftest` belongs to the harness row and starts in P0a.

The shared expression layer holds the expression transformations that the passes and the back end share. Today
`ToRustInnerVisitor.generateFromTrait` invokes `EliminateStructs` (`:1099`), and
column-default printing invokes `CreateRuntimeErrorWrappers.wrapCasts` (`:1258`).
Move these transformations and their required traversal helpers to `sql-expr`;
`sql-opt` may wrap them as passes. The dependency direction is
`sql-opt -> sql-expr -> sql-ir` and `sql-native -> sql-expr -> sql-ir`, with no reverse edge. P1 inventories
helper dependencies. The Java printer keeps its own copies until M4.

Design rules:

| Rule | Reason |
|---|---|
| FRel at the lowering boundary copies the 17 accepted Calcite node kinds (no SEMI or ANTI join, Filter above Window for TopK, Correlate only for UNNEST). Before that boundary, the model must represent correlated sub-queries and every intermediate node needed by the selected rewrite rules. | Inventory transient forms in P0b. A verifier rejects unresolved correlations and unsupported nodes before lowering. The final node list alone is insufficient to implement decorrelation. |
| Preserve traceability from Rust modules and passes to the Java behavior they replace; the mapping may be many-to-many. Design the Rust implementation for idiomatic ownership, clear interfaces, and human readability from the start. | The ledger tracks semantic coverage and upstream changes without forcing Java class or file structure into Rust. |
| Java idioms get one Rust mapping, written in `PORTING.md`: class hierarchy to enum plus property methods; identity maps to id maps; an iterated `HashMap` or `HashSet` to `IndexMap` or `IndexSet`; global id counters and the shared compiler object to an explicit context; visitor overloads to `match` plus derived traversals. Passes may depend on stable identity and recorded analysis history, but never on the numeric magnitude or allocation order of an id. Remapping ids also remaps associated pass state (section 7.1). | About 2,300 dynamic type tests and casts in IR, mid-end and back end (`.to(X.class)` and `.is(X.class)`) and 250 visitor overloads need one answer. |
| Constant folding uses an explicit registry of deterministic, side-effect-free sqllib functions with supported typed values and bounded evaluation cost. It never executes user UDFs, clock-dependent functions, or connector access. | Folds are of two classes. Verdict-relevant folds make a value constant where the compiler requires a constant (for example the TUMBLE interval, `F/CalciteToDBSPCompiler.java:583`; window bounds; LIMIT). They must match the Java result and are never disabled, or the verdict changes. Optimization-only folds can be refused, which leaves the expression for runtime. Folding is on by default today (`J/compiler/CompilerOptions.java:52`), and a changed fold changes the circuit and so the persistent ids. Thus the compiler receives the fold semantics version of the target runtime (section 7.5.1). A combination with a different version is rejected when a verdict-relevant fold is affected; it does not run with folding off. Preserve lazy CASE/COALESCE evaluation and runtime error behavior; an unreachable failing branch must not become a compile error. Panic catching is only a secondary guard. Spike 7 covers the constant-required sites, overflow, decimal rounding, NaN, casts, and temporal values. |
| In A, sqlparser types do not leave `sql-parser`. H needs a private bridge to the sqlparser AST expected by DataFusion; prototype that conversion in D1 and keep it out of shared model APIs. DataFusion types do not leave `sql-binder`. | Limits upstream coupling without assuming DataFusion can consume a Feldera AST directly. |

### 4.1 Rust quality is a delivery requirement

The compiler must use idiomatic Rust, with a clear architecture and code that people can read. Apply these acceptance criteria at each phase exit, including to code written by agents.

In this plan, "1:1", "mechanical" and "verbatim" mean that the port keeps the observable semantics, the pass order, the compatibility contracts and the invariants of the algorithms. They do not mean a copy of the Java inheritance, file layout, shared mutable compiler state or control-flow boilerplate. Changes of representation and structure are made under the differential oracle. Changes of SQL semantics and of optimization algorithms are kept in separate commits, so that a reviewer can find the cause of a mismatch.

| Rule | Detail |
|---|---|
| Modules | Cohesive modules with explicit responsibilities, acyclic dependencies and small interfaces. Parsing, semantic analysis, optimization, serialization and process control stay separate. Each boundary has documented invariants. |
| Types | Rust enums, pattern matching, typed identifiers, explicit ownership and borrowing, `Result` and `Option` where they express the domain. No copy of Java object graphs with shared mutable state or downcasts as the default. |
| Readability | Domain names that describe; simple control flow. Comments give the reason for a subtle SQL rule, a pass precondition or a compatibility exception. Macros, generic abstractions and generated helpers stay small enough that a maintainer can follow the execution. |
| Compatibility names | The wire tags and the pass names that users see (section 2, SET options) equal the Java class names through an explicit mapping. The Rust type names can differ. The port ledger records the Java origin. |

Structure that the oracles freeze. A change to an item of this list needs the same change in Java first, while the oracle of that region is in use:

| Frozen item | Oracle | Reference |
|---|---|---|
| Pass boundaries, pass names and pass order | C4 at each S4 dump point | Section 7.1 |
| Iteration behaviour of `Repeat` and of nested `Passes` | C4 | Section 7.1 |
| Operator sharing and expression sharing | Canonical-equal comparison | Section 7.1 |
| Analysis side data and cross-pass history (for example `AnalyzedSet`) | C4 with pass state | Section 7.1 |
| The S3 content that the Java tail hashes and prints | C5, C6b, C6c | Section 1.3 |

Review per phase exit:

| Check | Record |
|---|---|
| A named maintainer, who did not write the code, traces one SQL feature through the stages of each crate that the phase delivers | Name, date, feature, result per crate |
| The same maintainer explains the invariants of 3 passes or modules, selected by the reviewer, without the Java source | Result per module |
| `cargo fmt`, Clippy and the parity tests pass | CI result. These alone do not meet the requirement. |
| Exceptions | Each exception has a reason and an owner in the port ledger |

Spikes 2 and 8 exercise these quality criteria against the differential oracle.

### 4.2 Back end: native code through Cranelift

Decision of 2026-11-11: the Rust compiler does not generate Rust source for `rustc`. It generates compiled code directly with Cranelift. This section is a design proposal from source reading. Nothing in it is measured.

What changes:

| Subject | Today (Java back end) | Native back end |
|---|---|---|
| Output | Multi-crate Rust workspace; cargo and `rustc` build the pipeline binary (`PM/compiler/rust_compiler.rs`) | One object file per program from `cranelift-object`, linked to a shared library, plus the circuit IR and the row descriptors. A prebuilt generic pipeline binary loads the library at start (decision 14, taken). The interface follows the plugin pattern that Arroyo uses for its UDFs; see the table below. |
| Row types | One Rust struct per row type, with derived `Clone`, `Ord`, `Hash`, rkyv, serde and `SizeOf` | A row layout computed by the compiler, and a descriptor with C-ABI function pointers that Cranelift generates (compare, hash, clone, drop, archive, debug). The descriptor also defines archived/live ordering and hash consistency; see the storage contract below. |
| Link to dbsp | Generic operators, monomorphized by `rustc` | The dynamic layer of dbsp (`crates/dbsp/src/dynamic/`: `dyn Data`, `Factory`). Rust trait objects have no stable ABI, so the engine gets a generic row type that implements `Data` through the descriptor. |
| Circuit construction | Generated Rust calls the operator constructors | A circuit builder in the engine reads the circuit IR and creates the 73 operator kinds |
| Expressions and closures | Rust text, with inlining of `feldera_sqllib` by `rustc` | Cranelift functions that call `feldera_sqllib` through a C-ABI export layer (about 1,200 public functions today, with type-suffix variants) |
| Connector formats | serde code generated by `feldera_macros` per table struct | Schema-driven encoders and decoders in the engine for JSON, CSV, Avro, Parquet, Arrow IPC and raw (`crates/adapters/src/format/`) |
| Rust UDFs | `udf.rs` is compiled inside the pipeline crate | The UDF crate is compiled separately to a shared library with a stable ABI, as the Gen-2 path does today (`PM/compiler/gen2_udf_compiler`). cargo runs only for a program with UDFs. |
| Persistent ids | sha256 with the Rust text of each expression | A new scheme: sha256 over the canonical IR of the operator. The Rust compiler has no Rust printer. |
| Stored state | rkyv archives of the Rust structs | Depends on spike N2. If the native rows cannot give the same archive bytes, each pipeline with state needs a bootstrap or a reset to change to the native form. |

Storage contract: DBSP searches archived batches using live keys. Native rows
must preserve `OrdRepr`'s equality with live `Ord` and the decoded hash used by
membership filters (`crates/dbsp/src/dynamic/ord_repr.rs` and `hash_repr.rs`).
An archived hash may use the existing decode-and-hash fallback until equivalence
is proved; it must not claim faithful hashing without that proof. The factory
must retain the row layout when creating defaults and interpreting archived
bytes (`dynamic/factory.rs`), and keep the descriptor and its code alive for every
row, factory, and archived view that uses them. A single generic Rust row type
does not supply per-layout context through `WithFactory<T>` automatically.
Spikes N1 and N2 test this bridge with live/live, archived/archived, and
archived/live comparisons, hash consistency, spill and restore, and the required
row-type archive comparisons. These requirements do not replace their
ten-program or full-type criteria.

Rules of the dynamic link. The source of the pattern is the Arroyo article on Rust plugin systems (Appendix C). Arroyo applies it to UDF libraries that cargo builds as `cdylib`; here the library comes from Cranelift, and the same rules apply to the separate UDF library.

| Subject | Rule | Note |
|---|---|---|
| ABI | C ABI only. Each exported symbol is an `extern "C"` function; each shared data type is `#[repr(C)]`. | Rust has no stable ABI. A Rust `dylib` needs the same compiler version on the two sides. |
| Entry points | One metadata symbol and one registration symbol. The metadata gives the ABI version, the compiler build id, the fold semantics version and the list of row descriptors. The registration gives the function pointers of the closures. | The article has `plugin_metadata` and `plugin_entrypoint` |
| Version check | The engine reads the metadata first and refuses a library with another ABI version, with a clear error | The article has no version check; this plan adds it for section 7.5.1 |
| Loading | `dlopen2` in the pipeline binary (`Container::load`) | The article selects it over `libloading` for its interface and thread-safety guarantees |
| Memory | One side owns each value, and the interface states which one. Generated code allocates and frees through engine exports, so there is one allocator. | The article lets the host own all values |
| Errors and panics | No unwinding across the boundary. A sqllib export catches a panic and returns an error code; generated code returns the error to the engine. | The article uses `catch_unwind` in the plugin |
| Calls into the engine | The library imports the `feldera-sqllib-abi` symbols from the pipeline binary, which exports them | Needs the dynamic symbol table of the binary, or a table of pointers passed at registration |
| Link step | The compiler worker links the object file to a shared library with a linker (selected on 2026-11-12) | The compiler-server image carries a linker, for example `lld`. No object loader in the engine. |
| Cost | No inlining across the boundary, no LTO | Accepted (speed row below) |

Expected operating effects

| Item | Direction | Note |
|---|---|---|
| Pipeline compile time | Gain | No cargo and no `rustc` for a program without UDFs. Not measured. |
| Compiler-server image | Gain | The Rust toolchain stays only for UDFs |
| Port of `J/compiler/backend` and of the Rust printer | Not necessary | The Java back end stays until M4 for the pipelines that keep the present form |
| Speed of the generated code | Not a concern (owner position of 2026-11-11) | The generated code is a small part of the run time of the product. Cranelift does fewer optimizations than LLVM, and the calls into `feldera_sqllib` are not inlined. The row functions (compare, hash, clone, archive) are in the generated part and run per row in sort and merge, so spike N1 records the number as a check, not as a gate. |
| New unsafe surface | Cost | Generated machine code with raw layouts. Needs differential tests and fuzzing (C12). |
| Oracle of the back end | Loss | The Java compiler has no native back end. The structural oracle (token-equal workspace) is gone; run-time results (C7) and expression tests (C12) remain. |
| Engine integration | Scope | Native row storage and format handling live in `dbsp` and the adapters |

History: the repository had a Cranelift back end before. `crates/dataflow-jit` (111 Rust files, 47.4k lines, Cranelift 0.99) was added on 2022-12-09 and removed on 2023-11-21 (commit `1c061bf5a`, "Remove the JIT backend"). The commit message gives no reason. The reasons were recorded on 2026-11-12 from the maintainers; the table below compares them with this plan.

| Recorded reason for the removal | State in this plan |
|---|---|
| The JIT cannot deal with generics | Changed for the operators. The dbsp dynamic layer arrived on 2024-03-08, after the removal; the old JIT had its own operators. Not changed for `feldera_sqllib`: each type variant needs an export. |
| It must be fast enough to be worth the work | Changed by decision. The goal is to remove the cargo step; speed is recorded and is not a gate. |
| Cranelift is made for debug builds | Partly correct. Cranelift is also the production code generator of Wasmtime, but it optimizes less than LLVM. The speed design below reduces the effect. |
| It must work at the boundary with the external world (serialization) | The old JIT had CSV and JSON. PN-c covers six formats. |
| It needs much unsafe code | Not changed. C12, sanitizer runs and the descriptor verifier are the controls. |
| It must work with all of `feldera_sqllib` | The `feldera-sqllib-abi` exports and C12 expression tests cover this boundary. |
| A JIT is very hard to do well; vectorized execution is a better middle ground, and Gen-2 uses it | Not answered. This track is an experiment to see how far the approach gets. Spike N1 is its first check. |

Speed design. Vectorized execution in the full sense needs columnar batches. The dbsp engine stores and iterates rows, and its joins and aggregates work per key inside the generic operators. Thus the techniques below make only the generated part faster. None is measured.

| # | Technique | Effect | Status |
|---|---|---|---|
| 1 | Primitive operations inline | Integer and float arithmetic, comparisons, NULL checks and numeric casts are Cranelift instructions. Only strings, decimals, dates and similar functions call `feldera_sqllib`. The inline code must equal sqllib exactly, with the overflow errors (C12). | In the design from spike N1 |
| 2 | Batch interface | Each generated function takes a slice of rows and has the loop inside. One boundary call per batch, not per row. It is part of the library interface, so a later change is expensive. | In the design from spike N1 |
| 3 | Normalized keys | The compiler generates an encoder from a key to a byte string whose `memcmp` order equals the SQL order. Compare is then `memcmp`, and the hash is computed one time per batch. Nullable and variable-length fields need care. | Measured in spike N2, because it touches the state format |
| 4 | NULL handling without branches | NULL flags as bitmaps per batch | Only if a profile asks for it |
| 5 | Batch exports from sqllib | An array variant of a hot function, compiled and auto-vectorized by `rustc` | Only if a profile asks for it |
| 6 | Explicit SIMD in generated code | Cranelift has 128-bit vector types but no auto-vectorizer. It needs columnar inputs, thus a copy from rows to columns and back. | Not planned |

Relation to Gen-2: the Gen-2 engine (feldera/forge) already runs a program with no `rustc` step. It interprets the circuit IR on Arrow data. The native back end and Gen-2 thus solve the same compile-time problem in two ways. Decision 2 selects which engines stay.

New crates:

| Crate | Content | Depends on |
|---|---|---|
| `sql-native` (replaces `sql-codegen` and `sql-rustexpr`) | Row layouts and descriptors; Cranelift code for expressions, closures and aggregates; object file writer; new persistent ids | `sql-ir`, `sql-expr`, cranelift |
| `feldera-sqllib-abi` (engine side) | C-ABI exports of the `feldera_sqllib` functions, generated by a macro from one table that the compiler also reads | `feldera-sqllib` |
| Engine: native row, circuit builder, loader | A generic row type over a descriptor; the builder from circuit IR to dbsp dynamic operators; loading of the shared library; schema-driven formats | `dbsp`, `feldera-adapterlib`, `sql-ir` |

## 5. Mapping table: Java subsystem to Rust

| Java subsystem | Java kLOC | Action |
|---|---|--- |
| Grammar templates, `F/parser` | 3.2 + Calcite `Parser.jj` | Rewrite on sqlparser-rs with own code for types and FROM items |
| `F/statements`, DDL part of `CC/SqlToRelCompiler.java` | 3.8 | Rewrite in catalog and binder |
| `F/connectors` | 3.5 | Replace mirror types with shared Rust types plus compiler validation and diagnostic compatibility (section 6.3) |
| Calcite validator, scopes, coercion | external | Rewrite (smaller in option H) |
| Calcite type factory, `F/TypeCompiler.java` | 0.6 + external | Rewrite |
| Operator tables, `CalciteFunctions`, `CustomFunctions`, `FunctionDocumentation` | about 2 + external | Rewrite as a data table |
| `SqlToRelConverter`, `CC/ConvertletTable.java` | 2.6 + external | Rewrite as the FRel builder, port the convertlets |
| `CC/optimizer/` without `RexOptimize` and `ReduceExpressionsRule` (19 files; 13 custom and 10 cloned rule instances), 63 stock rules, metadata | 3.9 + external | Port the custom rules; inventory every configured stock rule; port or justify omission with targeted legality and performance tests (spike 10) |
| `RelDecorrelator` (4.2k), `SubQueryRemoveRule` (1.5k), `CorrelateProjectExtractor` (0.4k) | external, 6.1 | Port the Calcite algorithm onto FRel |
| Plan builder and Rex utilities: RelBuilder, RexBuilder, RexUtil, RelOptUtil, RexSimplify | external, about 20.8 | Port the subset that the rules, the convertlets and the decorrelator call |
| Calcite serializers: Rex to SQL text (rel2sql, 4.8k), RelJson for types and plans (1.5k), `CC/RelJsonWriter.java` | 0.1 + external | Port the used subset |
| Janino executor, `CC/optimizer/RexOptimize.java`, `CC/optimizer/ReduceExpressionsRule.java` | 1.4 + external | Folder with an explicit registry of foldable sqllib functions (section 4, design rules) |
| Lowering: `CalciteToDBSPCompiler`, `ExpressionCompiler`, `aggregates/`, `JoinConditionAnalyzer` | 9.0 | Port 1:1 |
| `J/ir`, `J/circuit`, `F/calciteObject` | 32.8 | Port; derives replace `equivalent`, `deepCopy`, JSON boilerplate |
| `J/compiler/visitors` | 43.4 | Preserve pass semantics and order; shared expression helpers belong to `sql-expr` |
| `J/compiler/backend` | 10.4 | Not ported. Stays in Java until M4. The native back end of section 4.2 replaces it (compiler and engine parts, new design). |
| `J/compiler/errors`, CLI, driver, metadata, `org/dbsp/util` | about 7.7 | Port what is used |
| `F/RustRegexChecker.java`, `F/SqlPrettyPrinter.java`, `CC/RenameIdentifiers.java` | 1.1 | Port, or replace the checker by a call to the `regex` crate |
| JUnit tests | 87 | Export as data; rewrite about 265 IR-structure tests; a Rust authoring format replaces the Java text tables before M4 |
| SLT runner, `lib/readers`, `lib/sltsqlvalue`, `lib/hashing`, `temp/` | 1.5 + Rust | Rewrite the runner; move the helper crates under `crates/sql/`. The Java runner takes the corpus from the hydromatic library and uses HSQLDB through JDBC, so the Rust runner needs a named corpus source and a statement executor. |
| Simulator, `multi/` (README only), `build.sh`, `pom.xml` files, launcher script, keyword drift check | 2.5 | Drop at M4 |
| Total | about 215 | |

## 6. Semantic compatibility policy

### 6.1 Rules

The specification is the behavior of the Feldera compiler commit, its options,
and runtime together with the Calcite commit pinned in `sql-to-dbsp-compiler/calcite_version.env` (`4bf922bb` on 2026-11-11). The golden tables record all these versions and options. A deviation needs an entry in a versioned allow-list file (entry fields in section 8.4).

| Area | Present source of truth | Strategy | Check |
|---|---|---|---|
| Result types (DECIMAL arithmetic, SUM, AVG, least restrictive, CHAR union to VARCHAR) | Calcite `RelDataTypeSystemImpl` defaults, 4 overrides at `CC/SqlToRelCompiler.java:447-481` | Transcribe the derivations into `sql-types` | Type probes (C9, section 8.2), from the generation rule of section 8.1. The golden table is the Java `-js` output or error and the S2 expression text. |
| Implicit casts | Calcite default type coercion (no custom class in the repo) | One coercion module, called by the binder only | Type probes, negative tests |
| Literal typing (integer width, DECIMAL precision, CHAR(n) strings, intervals; 3 unfolded cast cases) | Validator, `CC/FelderaRexBuilder.java:24-61` | Explicit rules | Probes |
| Nullability | Calcite return-type inference | Policy per registry row | Probes with nullable and NOT NULL inputs |
| Names: unquoted to lower case, case-sensitive catalog, case-insensitive built-in functions, alias rules, GROUP BY ordinal | `CC/SqlToRelCompiler.java:274-349`, `:689-718` | Scope rules in the binder | Corpus, `docs.feldera.com/docs/sql/identifiers.md` |
| Keyword policy in declarations, qualified references, and aliases | `G/config.fmpp` and parser extension tokens | Generated keyword table and context checks | Each keyword parsed as an identifier by both compilers; the documented table of `grammar.md` |
| Functions: the validator table of section 1.2, type-suffix dispatch into sqllib | `CC/functions/`, `F/ExpressionCompiler.java:682-713` | Registry rows; the naming scheme is ported unchanged. STANDARD and spatial operators that validate today but fail in the lowering keep their verdict and position (spike 10 lists them). | `cargo check` on generated code, run-time results |
| Sub-query semantics (NOT IN, scalar sub-query, EXISTS and NULLs) | Steps 1, 17, 19, 22; `InnerDecorrelator` retries on sub-trees and rejects a result whose row type changed | Port `RelDecorrelator`, no new algorithm. Parity includes the cases where Calcite keeps a Correlate. | Sub-query corpus, metamorphic tests, spike 6 |
| Plan shape | 32-step order | Same order; Java dumps the plan after each step | Metric per step, not a gate |
| Constant folding | Janino, Java semantics; also `Simplify` in the mid-end (ported verbatim) | sqllib semantics in `sql-rel`. Recorded deviation. Verdict-relevant folds must match Java (section 4, design rules). | Spike 7 measures the difference and lists the constant-required sites |
| Messages | Calcite wording, changed at `J/compiler/DBSPCompiler.java:900-919`, `J/compiler/errors/CompilationError.java:14-18` and `J/compiler/errors/CompilerMessages.java:23-32`. Positions of errors in generated SQL are moved at `CC/SqlToRelCompiler.java:1968`. | Require exact verdict, message count, warning flag, `error_type`, start position and snippet. Measure end position. Allow reviewed wording changes under decision 3. Use the message inventory from Spike 9 to migrate wording-only compiler assertions to structured fields. Preserve separately specified runtime and platform error contracts. | C0 |
| Unsupported features (outer UNNEST, non-linear user aggregates, DISTINCT ON, MATCH_RECOGNIZE, window limits) | `docs.feldera.com/docs/sql/unsupported-operations.md` | Keep the same limits and rejections until the cut-over | Negative tests |

### 6.2 Syntax: what sqlparser-rs 0.62 does and does not parse

A review agent parsed these forms with sqlparser 0.62.0 in a scratch crate (7 dialects). `Dialect` has 4 parse hooks only: `parse_statement`, `parse_prefix`, `parse_infix`, `parse_column_option`. There is no hook for data types, joins, table factors or the tokenizer. AST spans are partial (208 `Span::empty()` sites, 179 of them in `ast/spans.rs`; no span on data types, CREATE FUNCTION, CREATE TYPE), and a syntax error carries its location only as text.

| Form | Result | Needed |
|---|---|---|
| `INTERVAL '10 10:30' DAY TO MINUTE`, lambdas, named arguments `=>`, PIVOT, UNPIVOT, QUALIFY, `<=>`, `::`, SAFE_CAST, DATEADD with a bare unit, `* EXCLUDE`, `* REPLACE`, hint after SELECT, trailing comma in the select list, `ASOF JOIN` without LEFT | Parses | Nothing |
| `LEFT ASOF JOIN ... MATCH_CONDITION` (the only form Feldera supports) | Error | Parser code for joins |
| `TABLE(TUMBLE(TABLE t, DESCRIPTOR(c), ...))` | Error | Parser code for table factors |
| `MAP[k, v]`, `MAP<K, V>`, column type `INT ARRAY` | Error | Dialect prefix hook; parser code for data types (0.63.0 adds `MAP<K, V>` for the Hive dialect only) |
| `ROW(a INT, b VARCHAR)` type | Accepted with the structure lost | Parser code for data types |
| `(a - b) YEAR`, `U&"..."`, table hint `T /*+ ... */`, digit-leading names, adjacent string literals | Silent wrong parse | Token pass before the parser; infix hook |
| The 11 Feldera statement forms, `WITH ('k' = 'v')` properties with positions | Not present | Wrapper parser in the `DFParser` style |

The table was run with stock dialects. sqlparser selects some behaviour by the dialect type, so the Feldera dialect needs a fixed identity (an own `TypeId`, or the `TypeId` of one stock dialect), and spike 4 runs the table again with it. sqlparser also accepts much syntax outside the Feldera grammar. A reject pass after the parser is an exhaustive `match` on the AST (no `..` pattern on `Select`, `Query`, `Expr` and `DataType`), so that a new upstream field is a compile error.

Strategy, decided by spike 4: (1) a vendored sqlparser with a small patch set (the 0.64 datatype hook plus patches for table factors and joins; structured error location; spans), offered upstream; or (2) a Feldera fork from the start, as RisingWave and Materialize did; or (3) an own recursive-descent parser. For (1), the patch set is rebased over each release (79 changelog entries in 0.63.0; 186 in 0.64.0, 19 of them breaking), and the pinned 0.64.0 source already supplies `Dialect::parse_data_type` (verified against its Cargo.lock archive checksum). The P0 ROW prototype uses it; table-factor and join hooks remain absent. Upstream discussion on the remaining hooks is P0b work, not a gate.

### 6.3 Connector validation compatibility

The Rust compiler uses the runtime config types again, but it keeps a validation layer of its own in `sql-catalog`. Shared types remove a part of the duplicate definitions. They do not remove the validation work or the diagnostic work.

| Subject | Java behaviour today | Rust design | Check |
|---|---|---|---|
| Unknown fields | `F/connectors/ConnectorValidator.java:339-390` reports each unknown field as a located warning | A compiler-local decoder records unknown fields and error paths. Plain serde is not equal: `crates/feldera-types/src/transport/http.rs:11-24` accepts unknown fields for `HttpOutputConfig`. The runtime serde strictness does not change. | C0 fixtures: misspelled fields, with HTTP output `backpressure` |
| Flattened and hand-decoded types (Kafka, Delta, Iceberg, Postgres, Avro) | The Java mirror types list the fields | A serde wrapper cannot see an unknown key below `#[serde(flatten)]`. These types need an explicit field-set source: the utoipa schema, or a field table with a CI check against `feldera-types`. | P0 prototype on `PostgresReaderConfig`, `KafkaInputConfig`, `IcebergReaderConfig` and the Avro configs; the Java fixtures are the pass criterion |
| String maps | The Java Kafka map accepts each value type | The Rust map accepts strings only. Record the difference and keep the Java verdict. | C0 fixtures: wrong types |
| Missing, null and default values; scalar coercions; enums | Java rules | Same rules, in explicit validators | C0 fixtures: missing and null values, enum errors |
| Semantic constraints | Java validators report located warnings per field | The Rust `validate` functions return one string with no field path. The compiler layer adds the field path. | C0 fixtures: semantic errors |
| Error count | Java reports only the first decoding error of a config block | Same | C0 multiplicity |
| Positions | The warning points to the key in the SQL string literal | Keep the mapping from JSON offsets to SQL positions, with escaped strings and nested arrays | C0 fixtures: nested locations |
| Warning policy | Warnings can be suppressed or promoted to errors | Same, through the SET options | Each fixture with warnings enabled, suppressed and promoted |
| Other runtime versions | The jar of that version validates | The compiler reads the configuration contract of the selected runtime | G9 old-runtime case |

P0b makes the inventory of the Java validation rules, the decoder prototype and the differential fixtures. The fixtures are required for the P5a exit and for G1 and G4.

Measured on 2026-11-15: the disposable six-class decoder passes 6,544 direct
comparisons and 26,176 SQL comparisons across four warning policies, including
snippets and complete diagnostic counts. The inventory covers 29 Java classes and
239 fields; a runtime utoipa export checks field drift. Three Java null-handling
defects were repaired before recording passing evidence. Message text differences
remain a review item. See `crates/sql/connector-probe/README.md`, its validation
inventory, and `crates/sql/evidence/connector-probe.json`. This result does not
close the remaining P0 technical or independent-review criteria.

## 7. Migration strategy

### 7.1 Seams

Each seam except S1 exists in a classic variant (`--crates` mode) and a Gen-2 variant (`--gen2`).

| Seam | Content | Anchor today | Work to open it | Role |
|---|---|---|---|---|
| S0 | CLI and output files | `PM/compiler/sql_compiler.rs:793-826` | Compiler selector in pipeline-manager with the resolution order of section 7.5.1 and the bounded fallback of section 7.5.2. `/diff` and `/validate_program` must receive the `program_config` of the pipeline. Needs one experimental `ProgramConfig` field (openapi.json, Python SDK, fda). | Whole-compiler switch and rollback |
| S1 | Stream of 10 statement classes with RelNode and Rex trees, full types, positions and column metadata | `F/CalciteToDBSPCompiler.java:3358-3398` | Java exporter and test-only Java importer. Sarg expansion before export. FRel JSON schema v0 is a Phase 0 product. | Oracle for `sql-lower`. Test only. |
| S2 | Unoptimized circuit JSON | `J/compiler/DBSPCompiler.java:838-847`. Write half: the hidden option `--input_circuit` with `--jit -o file` (`J/compiler/CompilerOptions.java:231-233`), not exercised on the corpus. Read half: none. | (a) Read flag. (b) Serialize per operator the source positions and a provenance record (plan node ids and kinds); replace the RelNode tests in `FindUnboundedState.java:628-650` by the provenance kind. (c) Serialize `ProgramMetadata` variables (SET options), `viewOrigins`, column lateness, default and positions. (d) Carry the front-end messages. (e) Decode metadata without a Calcite type factory. (f) Run the optimizer on decoded circuits for the full corpus, which no test does today. (g) Carry the Java schema JSON, `calcite_plan` and `sources` beside the circuit, so that Rust can add the `unused` flags and `mir` (section 7.3). | Shippable hybrid boundary. The seam with the largest hidden work. |
| S3 | Optimized circuit JSON | `J/CompilerMain.java:245-262`. Written after all 94 steps, with the hash annotations. `--jit` is refused with `--crates` (`:246-250`). | Gen-2: none to write; the Gen-2 input is `program_ir` (mir + schema + circuit), so the schema and the `calcite_plan` part of the dataflow stay Java outputs in a hybrid (writers by stage: section 7.3). Classic: a mode that dumps the multi-crate circuit. Same provenance and position work as S2. | Shippable hybrid boundary. With the native back end, Java also needs a read flag for S3: the Java tail (Merkle hashes, Rust printer, workspace writer) then runs on the circuit that the Rust mid-end optimized. |
| S4 | Circuit JSON after each pass | `J/compiler/visitors/outer/CircuitOptimizer.java:257-264`, top-level steps only | CLI flag that dumps in `afterPass`; dump points inside nested `Passes` and `Repeat` loops (30 nested classes in 25 files, plus 5 inline `Passes` or `Repeat` instances) and after each sub-step of `MonotoneAnalyzer.java:114-162`; analysis results and cross-pass history with identity mappings; replay from S2 by default, isolated replay only at validated checkpoints (below) | Oracle per pass. Test only. |

Equality at the seams is structural. Normalize incidental ids and JSON object-key order, but preserve operator identity, edge ports, edge order, recursive references, and all semantic annotations. Compare inner expression sharing separately from operator topology; expanding the whole graph into trees can hide a duplicated stateful operator and can explode in size. Use memoized graph comparison with separate inner and outer id namespaces and explicit cycle handling. "Canonical-equal" in sections 7.2, 7.3, 8.2 and 12 means equal under this comparison.

`TestSerialize.Comparison` is a starting point, not the seam contract: it deliberately removes metadata and ignores positions. S2/S3 tests must compare the new metadata, provenance, SET options, diagnostics, and positions independently and must run downstream passes on decoded objects. P1 requires a lossless semantic decode and encode, not preservation of JSON whitespace or allocation ids. The Java tail computes the present persistent ids from the S3 that Rust writes. Thus that S3 must decode in Java to a circuit with byte-equal hash inputs (C6c) and a byte-equal workspace (C6b). Test the comparator itself with deliberate changes to ports, annotations, sharing, metadata, and recursive edges.

Every dump has an envelope recording schema version, producer commit, runtime ABI, classic or Gen-2 mode, options, and pass/iteration identity. Define required fields and reject incompatible versions before decoding. Keep existing Gen-2 consumer payloads unchanged until consumer compatibility is verified; the internal envelope is not automatically a new public wire format. Golden fixtures cover version rejection and both engine modes.

S4 is an observation point by default. P0a inventories pass inputs beyond the
circuit, including shared caches, analysis results, options, diagnostics, and
identity-indexed history. For example, `CircuitOptimizer.java:142-175` shares an
`AnalyzedSet` across `OptimizeProjections` invocations; `AnalyzedSet.done()` marks
operator identities and changes which later rewrites run. A fresh pass instance
with circuit JSON alone does not reproduce that execution.

The baseline harness replays the preceding passes from S2 in one compiler context
and compares S4 observations. To accelerate isolated tests, a checkpoint must also
serialize all required history, analysis state, diagnostics, and stable identities,
or reconstruct them by replaying the necessary prefix. Use one id mapping for the
circuit and all side data. P1 proves that Java execution resumed from each claimed
checkpoint matches uninterrupted execution, including later projection passes,
nested Repeat iterations, and diagnostics. Unsupported checkpoints stay
observation-only; do not reset hidden state to make a comparison pass. P3 retains
full S2-to-S3 comparison even when isolated replay works.

### 7.2 Implementation tracks

| Track | Content | Oracle |
|---|---|---|
| M (mid-end) | `sql-ir`, `sql-expr`, `sql-opt`, later `sql-lower` | Canonical-equal JSON at S2, S3, S4. The Java tail on the Rust S3 gives a byte-equal workspace and equal ids. |
| F (front end) | `sql-parser`, `sql-types`, `sql-functions`, `sql-catalog`, `sql-binder`, `sql-rel` | Verdict, schema, type probes, run-time results |
| N (native back end) | `sql-native`, `feldera-sqllib-abi`, engine parts (row type, circuit builder, loader, formats, UDF interface) | Run-time results against the pipelines that the Java back end built (C7 in native mode), expression tests (C12), benchmarks (C13) |

Track M ships back to front, because each back stage has a strong oracle. Track N has no structural oracle and starts in P0a with spike N1. Track F spikes run in parallel during P0b. Its implementation starts after the P0b review. The tracks meet at S2.

### 7.3 Phases

Milestones. The other sections use only these names. "Cut-over" means M2.

| Id | Name | Event | Gates |
|---|---|---|---|
| M1 | Hybrid default | End of P3. Java runs Calcite, lowers the input to S2 and consumes S3 for printing, hashes and workspace generation. Rust runs the circuit optimizer between S2 and S3. | P3 exit criteria |
| M2 | Rust default | In P6. New compilations use `sql2dbsp` with the native back end, with the resolution order of section 7.5. Remove `use_platform_compiler`. | G1 to G7, G9, G10, G11 |
| M3 | End of the fallback window | 90 days or more after M2, when the conditions of section 7.5 hold | Section 7.5 |
| M4 | Java retirement | The Java build leaves the mainline | G8 |

| Phase | Content | Exit criteria | Ships |
|---|---|---|---|
| P0a Oracle, seams, track M spikes | Corpus exporter at `BaseSQLTests.runOneTestCase` (section 8.1), beside `T/sql/tools/Gen2Runner.java`; Java flags for S2, S3, S4; S4 state inventory; selector and shadow mode; benchmark suite and baseline; `PORTING.md`; inspection of feldera/forge; spikes 0, 1, 2, 3, 10, N1, N2 | The P0a rows of the deliverable table below. Go or no-go review for Track M using Spike 2 parity and for Track N using Spikes N1 and N2. | Corpus and harness |
| P0b Track F spikes | Type-probe generator; FRel, FRex and type JSON schema v0 in `sql-rel-model`; inventory of transient FRel forms; connector-validation inventory and fixtures; licence review; spikes 4, 5, 5b, 6, 7, 9, 11, D1 | The P0b rows of the deliverable table below. Go or no-go review for track F, with decision 9. | Probe table |
| P1 `sql-ir` | IR types, arena graph, JSON, provenance type, helper builders; contract tests with forge; spike 8 at the end | Lossless semantic decode and encode of each S2, S3 and S4 dump, with topology and metadata, in the two modes. Decoded objects pass downstream validation. Each replayable S4 checkpoint passes the test of resumed against uninterrupted Java execution; other points use prefix replay. Shared expression dependencies are assigned. Spike 8 parity and review recorded. | A Rust crate for consumers of the circuit IR (forge) |
| P2 `sql-expr`, tail passes, writers | Shared expression helpers; tail passes from the S4 dump before `CheckHints` (`CircuitOptimizer.java:247`); `mir` writer; S3 read flag in Java for the Java tail; `sql2dbsp` entry point with the worker protocol of section 7.5 | S3 decoded and encoded by Rust, then given to the Java tail: workspace byte-equal and ids equal on 100% of the corpus. C10 equal. Gen-2 JSON canonical-equal. C7, and G9 and G10 in reduced form. | Hybrid plumbing; no user-visible change |
| PN Native back end | PN-a: row layout, descriptor ABI with the batch interface, native row type in dbsp, loader, after spikes N1 and N2. PN-b: Cranelift code for expressions, closures and aggregates; `feldera-sqllib-abi`; circuit builder for the 73 operators. PN-c: schema-driven formats, UDF interface, packaging, pipeline-manager integration, debug symbols. PN-d: opt-in soak. | C7 in native mode on 100% of the corpus cases with expected output. C12 with zero difference. C13 inside the budget of the P0a exit. G11. G9 and G10 in reduced form. | Opt-in engine form `native` from PN-d, first with the Java front end through S3 |
| P3 `sql-opt` | Each pass ported against S4 observations, with prefix replay or a validated state-complete checkpoint. Monotonicity and GC last and verbatim. | Per dump point: canonical-equal output on the corpus. Then S2 in gives canonical-equal S3 out and equal persistent ids. C1 and C10 equal. C7, and G9 and G10 in reduced form. 30 days of shadow runs with no difference on the minimum sample of section 8.4. Hybrid compile p95 is at most 1.2× direct Java p95. The owner confirmed the ceiling on 2026-11-18. | M1: Java front end, Rust mid-end, Java tail (printer, hashes, workspace). Reason for the default: new mid-end work is then written one time, in Rust (section 7.4). |
| P4 `sql-lower` | Port against S1. The lowering-boundary subset of FRel (17 node kinds, 7 Rex kinds) is frozen before the P4 port. The transient forms that the rules and the decorrelator use stay open until P5c ends. | S1 in gives canonical-equal S2 out on the corpus | Test-only hybrid |
| P5 Front end | P5a parser, DDL and catalog. P5b types, functions and binder. P5c FRel, rules and decorrelation. P5d integration. | P5a: the connector-validation C0 fixtures pass; 100% of the corpus statements parse or are in the allow-list; C0 agrees for syntax errors; no silent wrong parse (AST and operator checks, plus C7 on pairs of parse and unparse); zero statements that only Rust accepts, apart from the allow-list. P5b: C9 agrees 100% apart from the allow-list; C1a equal on corpus programs without sub-queries. P5c: C7 passes on the sub-query corpus. P5d: gates G1 to G5, G9 and G10 for each opt-in configuration; circuit-quality check (count of stateful operators and state size on the benchmark suite). | After P5a: syntax check and formatter without a JVM. After P5b: schema preview (C1a content) and live diagnostics. Expose these through the product integration. Show a verdict that differs from the Java compiler as advisory. After P5d: `sql2dbsp` opt-in per pipeline. |
| P6 Soak and cut-over | Shadow runs, fleet report, M2, fallback window, docs. M4 is a later milestone with its own date. | Before M2: the state and sink report of section 7.5 is complete for the Feldera-operated fleets, and the fleet report is released for the other installations. Gates G6, G7, G9, G10 in full. | M2. Java stays supported until M3, and longer for the exceptions of section 7.5. |

Each phase exit also needs the review of section 4.1.

P1 requires a Track M go at P0a. P5 requires a Track F go at P0b. A no-go
closes the track's P0 decision and keeps subsequent phases closed. Before a
go, work on each track stays within its P0 spike scope.

On 2026-11-19 the plan owner recorded go for Tracks M, N and F. The
measured mid-end scope preserves artifacts and persistent IDs. The native
prototype passes the ten frozen programs with descriptor-owned inputs and
restores both global aggregates through DBSP checkpoints. PN-a owns the
general batch factory interface. Track F proceeds with binder A and the
approved diagnostic policy and cut-over gates. The plan owner confirmed the Forge IR contract owner. P0 still needs independent maintainer
acceptance and independent licensing acceptance. [Recorded track decisions](crates/sql/P0-REBASELINE.md)

P0 deliverables. The go or no-go reviews check this table.

| Deliverable | Part | Source | Exit check |
|---|---|---|---|
| Corpus export | P0a | Section 8.1 | Each case that reaches `runOneTestCase` is exported or is in a skip list with a reason |
| S2 read flag (subset), S3 and S4 dump flags | P0a | Section 7.1 | The flags run in CI; the spike 1 criterion holds |
| S4 state inventory and replay strategy | P0a | Section 7.1 | Recorded per dump point |
| Selector with one value (Java), shadow mode | P0a | Section 7.1, S0 | One test; `/diff` and `/validate_program` receive the `program_config` |
| Benchmark suite, G7 budgets (RSS, concurrency, cancellation, throughput of the generated program), fuzzing budgets of G10 | P0a | Section 8.3 | Baseline recorded; numbers approved |
| Hybrid speed measurement and the 1.2 threshold of P3 | P0a | Risk 9 | Number recorded; threshold confirmed |
| Inspection of feldera/forge, owner on that side, contract tests planned | P0a | Section 9.1 | The "Unknown" row of Appendix A is replaced; decision 2 has a date |
| Type-probe generator | P0b | Section 8.1 | Runs in CI; the count from the generation rule is recorded |
| FRel, FRex and type JSON schema v0; inventory of transient FRel forms | P0b | Section 4 | Recorded |
| Connector-validation inventory, decoder prototype, fixtures | P0b | Section 6.3 | Fixtures pass on Java |
| Build of the fold registry without dbsp for Windows and wasm32 | P0b | Spike 7 | Build result and dependency boundary of the sqllib split |
| Licence review | P0b | Section 9.2 | Recorded result |
| Stop and descope numbers of section 11.1 | P0b | Section 11.1 | Each trigger has a number |
| Reasons for the removal of `dataflow-jit` in 2023 | P0a | Section 4.2 | Done on 2026-11-12 |
| Native prototype, speed and compile-time numbers, state-format result, C13 budget | P0a | Spikes N1, N2 | Recorded; decision 15 taken |
| Each P0 spike | both | Section 12 | A recorded result against its criterion |

Writer of each output by stage:

| Output part | Java only | P2 hybrid (Java to S3) | M1 hybrid (Java to S2) | Rust (P5d and later) |
|---|---|---|---|---|
| Schema JSON: columns, types, properties, `default` and `lateness` text | Java | Java | Java, carried as a file beside S2 | `sql-compiler` output module, with `sql-catalog` and the `sql-rel` serializers |
| Schema JSON: `unused` flags (set by a mid-end pass, `J/compiler/visitors/unusedFields/UnusedFields.java:125`) | Java | Java | Rust `sql-opt` writes the flags into the Java schema | Rust |
| Dataflow JSON: `calcite_plan`, `sources` | Java | Java | Java, carried beside S2 | `sql-rel` RelJson writer |
| Dataflow JSON: `mir` | Java | Rust `sql-compiler` | Rust | Rust |
| Messages | Java | Java, plus Rust back-end messages | Java front-end messages carried in S2 (item d), plus Rust | Rust |
| Rust workspace (present form) | Java | Java tail on S3 from Rust | Java tail on S3 from Rust | Not produced |
| Native object and descriptors | None | None | Rust `sql-native`, opt-in from PN-d | Rust `sql-native` |
| Gen-2 IR | Java | Rust | Rust | Rust |

Spike 1 confirms the two "carried" cells. One component writes each part in each stage.

### 7.4 How to follow the moving Java code

| Mechanism | Detail | Applies |
|---|---|---|
| Ledger | Record the last ported Java commit and the Rust modules and tests that cover each Java region. The mapping can be many-to-many after a structural change. | Always |
| CI check | When a Java file of a ported region changes, CI asks for a Rust change or a reviewed no-impact note. An advance of the ledger alone is not sufficient. The port team answers in 2 working days, so that the check does not stop Java work. | Always |
| Drift tests | CI makes the S4 dumps again from the Java compiler (one JVM per program). A Java change shows as a failing Rust test. | Until M1 plus 90 days |
| Rust first after M1 | From M1, new passes and mid-end changes are written in Rust only. For 90 days after M1, the Java mid-end and back end receive back-ports of correctness fixes, and the full Java compiler is the fallback of the hybrid. After these 90 days the Java mid-end is frozen. It then serves old `runtime_version` values only. | Always |
| Fallback after the M1 window | The fallback of the Rust front end is the M1 hybrid (Java front end, Rust mid-end), not the full Java compiler. A fallback to the full Java compiler can give a different circuit and thus needs the preflight of section 7.5. | Always |
| IR changes for Gen-2 | A change of the IR JSON that the Gen-2 engine needs is made in Java and in `sql-ir` in the same pull request while the Java mid-end is in use, with the forge contract tests. | Always |
| Structural notice | A structural change of the IR or of the pass order between P1 and M1 needs a notice to the port team 2 weeks before the merge. | Always |
| Corpus rule | Each Java front-end change adds a corpus case: from P5a for syntax, from P5b for types and functions, from P5c for plans. | Always |
| Calcite upgrades | Each change of `calcite_version.env` makes the type-probe table, the S1 dumps and the C0 and C1 goldens again in CI. Differences are port work. Calcite stays at one commit from the start of P5d to M2. | Always |

### 7.5 Deployment identity, fallback, and state

#### 7.5.1 Compiler identity and selection

| Subject | Present behaviour | Rule |
|---|---|---|
| Recorded identity | The program revision records `platform_version` and `runtime_version` | Each compiled program revision also records the compiler family (Java, hybrid, Rust), the back-end form (`rustc` workspace or native), the build id, the engine mode, the options and the artifact digests. The SQL compile, the generated-code caches and the artifact lookup use these values as keys. |
| Identity in the checkpoint | `ProgramIr` holds `mir`, `program_schema`, `circuit_ir`, `udf_library` (`crates/feldera-types/src/config.rs:157-173`) and no compiler identity | Add the compiler family, build id and mode to `ProgramIr`, so that each checkpoint records them |
| Resolution order of the selector | Instance default only | (1) The explicit value of the pipeline. (2) While the storage of the pipeline is not cleared: the family and build line of the last deployed revision. (3) The instance default. |
| Edit of a deployed pipeline | Each change of the code, the UDFs or the `program_config` sets the program to pending (`PM/db/operations/pipeline.rs:694-705`) | The recompile uses the resolution order. Thus a one-view edit of a Java-compiled pipeline with state stays on Java. |
| Missing artifact at start | The program goes back to pending and is compiled with the current platform compiler (`PM/runner/pipeline_automata.rs:1335-1351`) | Same order. If the recorded build line is not available, the start stops for approval with the preflight. |
| Platform upgrade during a compile | A pending or compiling program of an older `platform_version` is moved to the current version and compiled again (`PM/compiler/sql_compiler.rs:150-155`, `PM/compiler/rust_compiler.rs:190-194`) | Same order |
| `/update_runtime` | Recompiles on request (`docs.feldera.com/docs/pipelines/modifying.md:196-207`) | Same order. A family change must be requested explicitly. |
| Family change for a pipeline with state | Not applicable | Needs an explicit request and the preflight of section 7.5.3. The instance default never causes it. |
| Standby and seeded pipelines | A different pipeline, possibly on a different instance, starts from the checkpoint of a primary and is compiled separately (`docs.feldera.com/docs/pipelines/checkpoint-sync.md:9-13`, `:106-133`) | Before the start, the control plane compares the identity in the checkpoint with the identity of the standby. It compiles the standby with the family of the checkpoint, or it reports the difference. |
| Gen-2 | Compilation does not depend on the engine build | The recorded artifact is the circuit IR, and the engine build moves below it. G10 tests a matrix of stored IR (older Java, current Java, Rust) against engine builds (current, oldest supported). Additions to the Gen-2 payload for S2 and S3 are optional fields for the engine. |
| Fold semantics | The compiler has no information about the runtime that will execute the program | `feldera-sqllib` and the Gen-2 engine export a fold semantics version. The selector gives the target value to the compiler. On a mismatch the compiler rejects the combination if a verdict-relevant fold is affected. In other cases it leaves the optimization-only folds to run time; the circuit then differs and the preflight applies. |
| `use_platform_compiler` (`PM/compiler/sql_compiler.rs:702-705`) | Pairs the platform compiler with another runtime | Remove at M2 under decision 10. G9 tests the cut-over and the continued per-version selection under decision 5. |
| Newer runtime on an older platform | Each released platform holds the jar file name and the `java` command in code | The manifest-aware selection of section 9.1 ships in a platform release 90 days or more before M4. The manifest records the minimum platform version for a runtime without a jar. An older platform gets a clear error through the manifest location. |

#### 7.5.2 Worker protocol and fallback

| Subject | Present behaviour | Rule |
|---|---|---|
| Result | Exit code and a scan of stderr | The worker writes a result file with a status (ok, SQL error, internal error, resource limit, cancelled) and the messages |
| Process that ends with no exit code | Treated as transient: no status is written and the next loop sets the program to pending again. With a native worker, a stack overflow, a memory kill and a deadline kill thus cause an endless compile loop. | A supervisor kill or a crash signal is counted per program revision. After 2 attempts the result is a terminal system error. The silent retry stays only for a shutdown of the compiler server. |
| Deadline | The pipeline compile has none. The 300 s value applies only to the validation and diff calls (`PM/config.rs:184-189`). | A configurable deadline for pipeline compiles, with a status that the user sees |
| Fallback | None | One retry with the fallback family after an internal error or a resource limit, inside the deadline. Never after a SQL error, a cancellation, or for an obsolete program revision. |
| Fallback and state | Not applicable | The fallback never changes the family of a pipeline with state when the persistent ids differ. The result is then a system error with the reason. |
| Publication | Files in a temp directory | All outputs come from one successful attempt and are published atomically, with the record of the compiler that made them |
| Shadow attempts | None | Separate temp directories, bounded concurrency, resource quotas. A shadow attempt cannot write the program status or publish an artifact. |
| Counters | None | A fallback event counts as a failure of the candidate compiler |

#### 7.5.3 State

The native back end has a new id scheme and possibly a new state format (spike N2). Without a compatible state format, a change from the `rustc` form to the native form is always the reset or bootstrap case of this section, for each pipeline with state. Such pipelines stay on the Java tail until their owner selects a disposition; this sets the earliest date of M4.

A compiler rollback, a state rollback and a sink rollback are separate operations. Equal persistent ids are necessary for state reuse, but they do not prove that a checkpoint is compatible. The state-format version is compared also.

| Id class | Effect of a changed id today | Rule |
|---|---|---|
| Table ids (source operators) | The runtime clears the table and starts its input connectors from the initial position (`docs.feldera.com/docs/pipelines/modifying.md:93-96`, `crates/connectors/src/controller.rs:3431-3440`). This is data loss for a source that cannot replay. | Hard gate: table ids are equal on 100% of the corpus and of the shadow programs (C5a, G6). The inputs of the table hash that the front end makes have their own comparator cases: DEFAULT and LATENESS text, foreign keys, `expected_size`, `unused` flags with `skip_unused_columns`. The preflight reports this class separately. A script that passes `bootstrap_policy=allow` gets no approval step, so a family change with a changed table id is always refused without an explicit request. |
| View ids | The view is bootstrapped. The runtime refuses a bootstrap when the old or the new program has a relation with LATENESS (`crates/feldera-types/src/pipeline_diff.rs:566-573`) and when a needed table is not materialized (`crates/connectors/src/controller.rs:3311-3329`). | Programs that can bootstrap: published equality rate; bootstrap through the existing `bootstrap_policy` approval (`modifying.md:65-82`). Programs with LATENESS or with a non-materialized source table: a changed id means a full reset. Such a pipeline with state does not change family, unless all ids are equal or the owner accepts the reset. For these two classes, id equality on the corpus subset and in shadow runs is a hard gate for M2 (G6). |
| Other stateful operators outside all view cones | DBSP decides the backfill for each stateful operator that has no checkpoint. If this result disagrees with the diff of table and view ids, the controller stops with `UnexpectedBootstrap`. | C5, the shadow counters and the preflight compare all operator ids, in three separate counts. The corpus audit found 942 persistent delay occurrences outside all sink cones across 17,102 circuits. The G9 fixture changes only the GC-branch delay ID in a LATENESS program. It must refuse automatic migration and preserve the existing family unless the pipeline owner approves a reset. [Corpus audit and G9 fixture](crates/sql/evidence/state-outside-view-cones.json) |

Preflight before a new compiled revision is deployed on a pipeline with state:

| Step | Detail |
|---|---|
| 1 | Compare the identity (section 7.5.1) and the three id classes with the recorded program info |
| 2 | Select one disposition: reuse of compatible state; bootstrap from retained materialized state; reconstruction from replayable inputs; accepted reset. No source replay does not exclude a bootstrap from retained state: the check is which data the new circuit needs. |
| 3 | Show the changes that only the compiler caused, the blockers (LATENESS, non-materialized table, table id change, sinks) and the disposition of each sink, in the existing approval of `bootstrap_policy` |

#### 7.5.4 Sinks

A bootstrap sends the full contents of a modified view and does not retract the old sink contents. A checkpoint restore cannot undo output that was delivered. Each migration and each rollback thus records a consistent boundary of input and output progress, and a sink action, before it starts.

| Sink condition | Migration action | Rollback requirement |
|---|---|---|
| The external sink has the correct contents and the compiler change keeps the observable results | `silent_bootstrap=true`, only after this condition is shown. Verify that the output progress continues with no gap and no duplicate. | Reconcile the output delivered after the migration boundary. An older checkpoint alone is not sufficient. |
| The sink contents must be made again, or the observable results change | Stop delivery at a recorded boundary; clear, rebuild or replace the sink; bootstrap; start delivery again | Restore or rebuild the sink state, or compensate the delivered changes |
| An append-only sink, or a sink with external side effects, that cannot be reset or reconciled | Show a supported protocol for deduplication, transactions or reconciliation. If there is none, this pipeline does not switch. | No automatic rollback after delivery, until a recovery procedure exists |

| Limit of the runtime today | Consequence |
|---|---|
| `silent_bootstrap` is one flag per start (`crates/connectors/src/server.rs:747-749`) and applies to all output streams (`crates/connectors/src/controller.rs:5117-5121`). It is exclusive with a concurrent bootstrap. | Add bootstrap output control per output connector in `/start` and `/approve`, with the output progress rules of the silent mode. Until it exists, a pipeline whose sinks are in different rows of the table does not switch. |
| The paused state of an output connector is not kept when its view is modified (`crates/connectors/src/controller.rs:6447-6465`) | A pause before the migration is not a method to stop one sink |
| Delivery guarantees differ per connector | Use the real guarantee of the connector, not an assumed exactly-once contract |

Silent bootstrap is not selected only because the SQL text is the same: a reviewed semantic deviation can change results. The approval names the sink cleanup or reconciliation steps and the person who does them. It is not a general approval to discard data. G9 tests sink contents and output progress after a bootstrap, later input, a crash and restart, and a rollback, with failures between the sink preparation and the pipeline restart, and with one pipeline whose sinks are in different rows.

#### 7.5.5 Fleet report, fallback window and retirement

| Subject | Rule |
|---|---|
| Fleet report | A product function, because the project cannot see the pipelines of self-hosted installations. After an upgrade, pipeline-manager compiles each pipeline with state in shadow, compares all ids with the recorded program info, and shows the result and the blockers in the REST API, `fda` and the web console. |
| Feldera-operated fleets | Before M2, each affected pipeline has a record: state and input availability, sink handling, a tested recovery path, owner, date. If no safe path exists, the pipeline stays on a supported compiler line, or its owner accepts a reset. An owner and a date do not show that data can be recovered. |
| Other installations | The fleet report and the preflight are the mechanism. M4 depends on a published support policy for the Java line with a named last version, not on a count of pipelines. |
| No forced migration | The end of a window never causes a migration. The resolution order of section 7.5.1 keeps a pipeline with state on its family. |
| Fallback window | 90 days or more. The clock starts at M2, and only when the Feldera-operated records are complete. A new exception or a failed migration opens the retirement review again. |
| Support window of the Java line | 12 months after M2 as the maximum (decision 11). Each extension has an owner, a review date, retained artifacts, and a maintained path for builds and security fixes. A retained jar with no maintenance is not an extension. |
| Supported runtime window | `is_supported_runtime` gets a defined window before P6, so that the set of old versions that need the jar is known |
| M4 | G8. The mainline Java build stays while an exception depends on it. The person who decides the retirement is named in decision 11. |

## 8. Test strategy

### 8.1 Corpus

| Asset | Size | Use |
|---|---|---|
| JUnit cases that reach `BaseSQLTests.runOneTestCase` (`T/sql/tools/BaseSQLTests.java:428`), with the failing cases of `addFailingRustTestCase` (`:440`). Entry points: `q`, `qs`, `qst`, `qf`, `step`, `stepWeightOne`, `testQuery`, `runtimeFail`, `runtimeConstantFail`, `compileRustTestCase`. Exported as data (program, input steps, expected Z-sets in canonical JSON). | About 3,800 expected tables by grep (2,786 `(N rows)` tables, 187 `q` sites, 844 `step` sites) plus 163 runtime-failure sites (estimate) | Main run-time oracle. Export from Java, because the expected text is parsed with the types that Java inferred (`T/sql/tools/TableParser.java`). |
| Compile-time negative tests | 329 executed Java call sites (243 distinct expectations); 281 Python expectation assignments (224 distinct strings) | Verdict, position, message catalog |
| Python `python/tests/runtime_aggtest` | 2,279 views with expected rows | Black box behind pipeline-manager |
| Python `python/tests/runtime`, `python/tests/platform` | 25 files and more | Black box |
| Docs blocks, demos, Nexmark, TPC-H, TPC-DS | 323 blocks, 12 demo programs, suites | Verdict, schema |
| felderize fixtures | 1,504 query files | Spark SQL inputs of the translator. Of use only through the translated output. |
| Type probes (generated) | About 50,000 (estimate; the rule gives the count) | Type and coercion parity (C9). Generation rule: one probe per point of (operator or function, type family of each argument, nullability, literal or column, boundary values of precision, scale and length), with a stated sampling method and a coverage report. |
| SLT (hydromatic corpus) | Millions, narrow feature set | Nightly rotation with a Rust runner; one full pass before M2 (G3) |
| IR-structure tests | About 265 tests, 149 visitor sites | Rewritten against a stable text dump of `sql-ir` |
| Documentation checks | `T/DocumentationTests.java`, `T/sql/ParserTests.java:612-615`, `T/sql/OtherTests.java:856-861` | Same checks in Rust |
| Shadow compiles in pipeline-manager | Real programs | Counters only; the result never reaches the user |

### 8.2 Comparators

| Id | Compares | Strictness | Phase |
|---|---|---|---|
| C0 | Verdict; for each message (errors and warnings): warning flag, `error_type`, start position, snippet; count of messages | Exact. End position is a metric. | P0b subset for D1; P5, P6 |
| C1 | Schema JSON, full | Exact after key sort | P3 (hybrid), P5d, P6 |
| C1a | Schema JSON without the `unused` flags and without the `default` and `lateness` text: names, `columntype`, nullability, keys, properties and positions | Exact after key sort | P0b subset for D1; P5b |
| C2 | FRel JSON after each rule step | Metric | P5 |
| C3 | S2 circuit JSON | Canonical-equal in P4, metric in P5 | P4, P5 |
| C4 | S4 observations per dump point with the required pass state | Canonical-equal; isolated replay only after the test of resumed against uninterrupted execution | P3 |
| C5 | S3 JSON and persistent ids, counted in three classes: table ids (C5a), view ids, other operator ids | Canonical-equal and equal ids in P2 to P4. In P5 and P6: C5a equal on 100%; view and other ids as a rate, but equal for programs with LATENESS or with a non-materialized source table. | P2 to P6 |
| C6a | Text of the Java expression and type printer per inner node. With the native back end it is a Java-side reference only: the Rust compiler has no printer. C9 uses it for the S2 expression. | Reference | P5 |
| C6b | Workspace that the Java tail writes from the Rust S3, against the workspace of the full Java compiler | Byte-equal | P2, P3 |
| C6c | Pre-hash string per operator from the Java tail on the Rust S3, against the full Java compiler; pre-hash string of the table metadata | Byte-equal. Diagnosis aid for C5. | P2; P5 for tables |
| C7 | Output Z-sets and observable runtime errors per step | Exact typed values and signed multiplicities; narrow float exceptions (section 8.4) | P0b subset for D1d; P2 to P6 |
| C8 | Metamorphic (query partitioning) and batch against incremental | Self-check | Nightly from P5c |
| C9 | Type and coercion probes against the Java golden table. Per probe: the result type and verdict, and the unoptimized expression at S2 as C6a text (cast placement, sqllib function name and type suffix). Probes of comparison, IN, BETWEEN, CASE, join keys and set operations also carry value vectors (boundary values, NULL, strings that parse as numbers) and are compared with C7. | Exact, apart from the allow-list | P0b subset; P5, P6 |
| C11 | New persistent ids of the native form | Stable across compiles and builds; equal for equal canonical IR; different when an operator cone changes (mutation tests) | PN |
| C12 | Each expression kind and each `feldera_sqllib` export, on generated values with NULLs and boundary values: Cranelift code against the Rust function | Exact, with errors and panics mapped to the same run-time error | PN |
| C13 | Throughput, latency, memory and state size of the native form against the `rustc` form, on the benchmark suite; pipeline compile time | Budget fixed in the P0a exit from spike N1 | PN, P6 |
| C10 | Dataflow JSON | P2 to P4: canonical-equal with the Java file, with the `mir` persistent ids, positions and plan links. P5: `mir` and `sources` exact where the circuit is equal; `calcite_plan` checked against the `feldera-ir` types and by a load test of the profiler. | P2 to P6 |

### 8.3 Cut-over gates

| Gate | Criterion |
|---|---|
| G1 | C0 agrees on 100% of the corpus, apart from allow-list entries with an issue number |
| G2 | C1 and C10 are equal on 100% of the accepted programs; C9 agrees on 100% of the probes that the generation rule of section 8.1 gives |
| G3 | C7 passes on 100% of the cases with expected output. One full SLT pass of the two compilers before M2, with its machine time recorded, shows no new failure. |
| G4 | Each Java negative test and each Python `expected_error` test passes or has a reviewed new expected text |
| G5 | The IR-structure tests exist against `sql-ir`; the documentation checks and the function index generator exist in Rust; the Rust test authoring format exists and the corpus is converted to it |
| G6 | 60 days of shadow runs on the minimum sample of section 8.4 with no unexplained C0, C1 or C10 difference. C5a is equal on 100% of the shadow programs. View and other ids are equal for each shadow program with LATENESS or with a non-materialized source table. For other programs, publish the share with all view and other operator ids equal. A rate below 100% starts a review of decision 4. |
| G7 | Compile time is not worse than Java at p50 and p95 on a frozen benchmark suite and fixed hardware, for cold and warm processes and each supported mode. Peak RSS, concurrency, cancellation latency, and the throughput and state size of the generated program meet the numbers that the P0a exit fixed. |
| G8 | M4. The retirement conditions of section 7.5.5 hold. No state or sink migration and no exception depends on the mainline Java build. The manifest-aware selection was released 90 days or more before. No JDK, Maven or Calcite build in mainline CI and in `deploy/build.Dockerfile`; `sql-to-dbsp-compiler/SQL-compiler` deleted. The JRE stays in `deploy/Dockerfile` only while a supported `runtime_version` needs the jar, with a recorded end date. |
| G9 | Deployment and restart tests pass from Java to Rust and from Rust to Java, for the two modes. Named cases: compatible-state reuse; incompatible-state rejection; replay and bootstrap; a LATENESS program; a non-materialized table; a changed table id; an operator outside all view cones (if P0a finds one); UDFs; recursive state; selection of the associated Java compiler for existing runtime versions; removal of `use_platform_compiler` at M2; a missing artifact at start; a platform upgrade during a compile; `/update_runtime`; an edit of a pipeline with state; standby activation and seeding across families, with and without LATENESS. External sink contents and progress stay correct across a bootstrap, later updates, a crash and restart, and a rollback, with one pipeline whose sinks need different actions; a sink that cannot be reconciled blocks the transition. Selector persistence, fallback, cancellation, stale-result publication, and the worker cases (stack overflow, memory limit, deadline) pass. |
| G11 | Native back end: C7 in native mode passes on 100% of the cases; C12 has zero difference after the fuzzing budget; C13 is inside its budget; C11 passes; each connector format passes its tests on native rows; UDF programs pass; sanitizer runs of the engine with generated code are clean. |
| G10 | Fuzzing of the parser, binder and decoders, and adversarial resource tests, pass under the worker limits: the CPU-hours with zero crash and zero hang, and the RSS and latency limits, are the numbers that the P0a exit fixed. The matrix of stored Gen-2 IR against engine builds (section 7.5.1) passes. Forge and all supported artifact targets pass the consumer compatibility tests. Missing consumer coverage blocks the affected rollout. |

The owner approved these limits on 2026-11-18 for the measured Linux ARM64
devcontainer profile. The [benchmark record](crates/sql/P0-REBASELINE.md#approved-devcontainer-budgets)
specifies the workloads and current results. Shipping configurations still
need measurements at their phase exits.

| Gate | Approved limit |
|---|---|
| G7 compiler workers | Four parallel jobs, at most 1 GiB RSS per worker, and cancellation p95 at most 25 ms from signal to reap. The Java p50 and p95 compile-time ceilings above also apply. |
| G7 generated program and C13 native runtime | At least 85% of classic median throughput, at most 120% of classic p95 step latency and 125% of classic median peak RSS, and at most 101% of classic checkpoint bytes for stateful workloads. |
| C13 artifact build | The native emission, link, load and initialization p95 is at most the classic warm-dependency shared-library build plus initialization p95 on the same workload. |
| G10 | The G7 worker RSS limit, a two-second isolated adversarial deadline, and at least one worker CPU-hour each for the Rust parser, binder and decoder with zero crashes or hangs. |

| Rule for all gates | Detail |
|---|---|
| Allow-list cap | The initial cap is zero entries and 0% affected cases for each gate. The owner selected it on 2026-11-18. A future exception needs a recorded revision of the applicable cap and approval from the owner of decision 1. Publish the effects on users. [Versioned allow-list](crates/sql/migration-allow-list.json) |
| Reduced form | Before P6, a phase exit applies G9 and G10 only for the compiler configuration, engine modes, state transitions and artifact targets that the phase ships. The full gates apply at P6. |

### 8.4 Coverage and operational oracles

| Subject | Rule |
|---|---|
| Diagnostic coordinates | The API boundary specifies: index base, end-range convention, Unicode column units, CRLF, tabs, offsets in programs with many statements. Java is probed with non-ASCII and supplementary characters, and the Rust byte spans are converted to that contract. C0 compares the count of diagnostics and the suppression and promotion of warnings, also when SET changes in the middle of a program. |
| Corpus manifest | Each case records: source test id; compiler, Calcite and runtime commits; options; engine mode; input steps; expected outputs or errors. |
| Coverage report | Exported, executed, skipped and unsupported counts, separately by feature and mode. A case that is not implemented is not a passing case. |
| Allow-list entry (one definition for sections 6.1 and 8.3) | Exact scope, reason, owner, issue number, review or expiry date, effect on users. A skip or a named discrepancy does not waive a gate. |
| C7 scope | From the first shipped back-end replacement. Classic execution is used for the cases that Gen-2 cannot run. Spike 3 sets the share; below 80% of the corpus, the Gen-2 mode ships only with canonical-equal S3 and has no front-end switch until the share is reached. The Gen-2 tests run in CI from P0a. |
| C7 data cases | Inserts, deletes, duplicates, empty groups and groups of all NULL, cardinality errors of scalar sub-queries, recursion, UDF failures |
| C7 streaming cases | Watermarks and NOW advance; lateness boundaries are crossed; per-step deltas and materialized results are compared. Long runs check state reclamation and memory bounds. Checkpoint and restart cases check outputs and offsets. |
| C8 | Each transformation states its preconditions for bag semantics, NULLs, nondeterminism and streaming time |
| Value codec | Keeps SQL types, decimal scale, binary values, temporal units, NULLs, nested values and signed weights |
| Float exceptions | Absolute and relative bounds and the NaN and infinity behaviour are given per test. Keys, counts and branch predicates use exact comparison. A global tolerance can hide a wrong join key or grouping key. |
| Shadow population | The Feldera-operated instances, plus installations that opted in. The existing channels are the PostHog key, the opt-in Sentry reporting and the runner telemetry of the enterprise chart. P0a uses local structured pipeline-manager logs. An operator enables the shadow compiler explicitly. Use opt-in reporting and recorded consent for any external aggregation at rollout. |
| Minimum shadow sample | P3 exit: 200 distinct programs. G6: 500 distinct programs, with 20 or more programs in each feature class of the coverage report and in each engine mode. P0a confirms these numbers against the real population. Days without the minimum sample do not count. |
| Shadow mismatch | Telemetry carries counters only; SQL and connector secrets stay out. For each mismatch the instance keeps a local bundle that the administrator can export, made with the existing anonymizer (`--anonymize`). "Unexplained" means that no bundle was examined. |
| Shadow window | A semantic fix resets the window of the affected feature. More than 2 resets start a review. |
| Limits of the shadow | A compile shadow measures C0, C1, C10 and ids, not run-time correctness. G3 and G9 supply the run-time evidence. |
| Circuit benchmarks | The circuits are measured as well as the compilation: a correct rewrite can multiply join state or prevent GC |

## 9. Integration, release and licensing

### 9.1 Integration changes

| Area | Today | During the migration | After M2 (items marked M4 arrive at gate G8) |
|---|---|---|---|
| Invocation | `java -jar`, temp files, own process group, kill when the pipeline is deleted or outdated, stderr JSON scan (`PM/compiler/sql_compiler.rs:725-957`) | Selector: Java, hybrid, Rust; shadow mode | From the first shipped Rust stage (P2): `sql2dbsp` runs in a supervised worker process with a hard deadline, process-group cancellation, memory and output limits, bounded concurrency, and the result protocol of section 7.5.2. Expensive loops also have cooperative checks. `catch_unwind` cannot contain an abort, a memory kill or a stack overflow, and fuzz tests do not remove that limit. The library API is for trusted callers; an in-process service is a separate decision. |
| Pipeline build | cargo and `rustc` build the generated workspace (`PM/compiler/rust_compiler.rs`) | Unchanged for the `rustc` form. The native form is opt-in from PN-d. | Native form: no cargo step for a program without UDFs. The Rust toolchain stays in the compiler-server image for UDFs and, until M4, for the `rustc` form. |
| REST contract | `SqlCompilationInfo { exit_code, messages }`, also on success (`PM/db/types/program.rs:220-227`) | Unchanged | `exit_code` kept as 0 or 1 |
| Validation | HTTP hop to the compiler server plus a JVM start; 300 s timeout (`PM/config.rs:184-189`) | Advisory validation through the same bounded workers | Rust worker; process pooling may remove startup overhead |
| Multi-version runtime | Jar download by sha or version (`PM/compiler/sql_compiler.rs:452-586`) | Unchanged | A manifest per version selects jar or binary; old versions keep the jar |
| Release artifacts | Jar per commit sha on S3 (`.github/workflows/build-java.yml`), jar per version on S3 and the GitHub release (`ci-release.yml`), `pom.xml` version bump (`ci-post-release.yml:254-258`) | Jar and `sql2dbsp-<target>-<sha or version>` are both published: x86_64 and aarch64 Linux for the platform, macOS and Windows for felderize. The Windows binary needs the fold crate without dbsp (spike 7); until it exists, felderize on Windows keeps the jar. | Mainline jar publication and the `pom.xml` step stop only at G8; supported exception lines and runtime lines retain their own build, artifact publication, and security-fix path |
| Images | JRE in `deploy/Dockerfile:18-33`; JDK, Maven and Gradle in `deploy/build.Dockerfile`; JDK in `.devcontainer/Dockerfile:57` | Unchanged | Removed per gate G8 |
| CI | Calcite source build, Maven, Java Tests job of 41 to 52 minutes | Both test sets run | One cargo workspace (M4) |
| Cargo workspace | `sql-to-dbsp-compiler/lib/readers` is a member, `sltsqlvalue` a dependency (`Cargo.toml:27,359`); `deploy/Dockerfile:103-107` copies `lib/` and `temp/` | Unchanged | Helper crates move under `crates/sql/`; stale entries removed |
| feldera/forge | `circuit-ir` decodes compiler IR and `circuit-loader` lowers it into `circuit-runtime`. `crucible` and `ir-expander` provide the Java test protocol. | Contract tests on the JSON tags, schema and canonical circuit output on the pinned Forge build. Decision 2 limits initial Gen-2 migration to canonical S3 equality. | Can link `sql-ir` |
| Web console, fda, Python SDK | Depend on the REST schema | One experimental `ProgramConfig` field for the selector | Optional: wasm build of the front half for live diagnostics (needs the fold crate without dbsp); an LSP on the same crates |
| felderize | Downloads the jar by name, needs `java`, parses text errors | No change | Static binary; download name and the `java` check change |
| Documentation | Describes Calcite, JDK, Maven and the jar: `README.md:123-152`, `sql-to-dbsp-compiler/README.md`, `using.md`, `docs.feldera.com/docs/sql/intro.mdx:97-102`, `casts.md:15-17` (implicit casts are a link to the Calcite reference), `tutorials/testing.md:37`, `operations/required-domains.md:13`, the parallel-compilation page (JVM heap), `crates/connectors/README.md:98,347` | Unchanged | Rewritten; `casts.md` documents the implicit cast rules directly |

### 9.2 Licensing and supply chain

| Item | Licence | Action |
|---|---|---|
| Code translated from Calcite (decorrelator, sub-query rules, cloned rules, convertlets, plan builder subset, type derivation, serializers, message text) | Apache-2.0 | Keep the ASF header and a change note in each translated Rust file; add a NOTICE file with the Calcite notice to the affected crates. Eight Java files and one parser template now carry the ASF header. P0 restored missing headers in `ValuesReduceRule`, `GenericLambdaTypeChecker`, and `SqlAttributeDefinition`, and added the pinned Calcite licence and notice to compiler resources. The inventory is in `crates/sql/LICENSING.md`; independent legal review in P0b remains required. |
| sqlparser-rs | Apache-2.0 | Listed in the SBOM. A vendored patch keeps the licence file. |
| Materialize | BSL 1.1 for versions younger than 4 years; Apache-2.0 for v0.27.0 and older | Design reading. A code copy only from the Apache-2.0 versions, with attribution, after the P0b legal review. |
| RisingWave | Apache-2.0 | Design reference; code reuse possible with attribution |
| Gain | | The JRE and 26 declared Maven dependencies (22 without test and provided scope; Calcite, Avatica, Janino, Jackson, Guava and others), with their transitive dependencies, leave the image SBOM and the CVE scan at M4 |

## 10. Delivery dependencies

P0 establishes the corpus, toolchain and executable seams. The mid-end starts
from the circuit IR before porting passes. The front-end port uses the detached
S1 oracle and freezes the lowering-boundary FRel subset before P4.
The native backend follows the N1 and N2 results and uses its own C7, C12 and
C13 gates. Section 7.3 defines the phase exits and release order.

P5's schema preview and advisory diagnostics need an endpoint, CLI command
or web-console integration. The Rust test authoring format and SLT statement
executor precede Java retirement. Keep the compatibility checks and release
soak windows in sections 7.5 and 8.

## 11. Risks

| # | Risk | Probability | Impact | Owner role | Mitigation |
|---|---|---|---|---|---|
| 1 | Type derivation differs from Calcite in untested cases and changes view schemas | High | High | Track F lead | Type probes from P0b; C1 and C9 as hard gates, with C9 on the expression and on values; shadow runs on real programs; strict allow-list |
| 2 | The decorrelator covers fewer shapes or has wrong NULL semantics | High | High | Track F lead | Spike 6 reached 149/155 selected-stage C7 occurrences after the aggregate and FETCH follow-ups; [C7 evidence](crates/sql/evidence/correlation-spike6-weighted.json); port the Calcite algorithm with its plan builder; metamorphic tests; located error for the rest |
| 3 | The Java code moves faster than the port | High | Medium | Engineering owner | Section 7.4: Rust first after M1, corpus rule, CI check; decision 7 |
| 4 | Monotonicity and GC port errors cause unbounded state or wrong results | Medium | High | Track M lead | Port last and verbatim; dump points inside `MonotoneAnalyzer`; GC tests on the IR dump; longer shadow period |
| 5 | Seams omit metadata or pass history, and a faithful replay is not possible | High | High | Harness owner | S2 work list and spike 1; S4 state inventory, prefix replay, and the test of resumed against uninterrupted execution before isolated pass tests |
| 6 | Structural equality is not reachable because passes depend on Java hash-map order or node identity | Medium | Medium | Harness owner | Spike 0; `LinkedHashMap` fixes in Java; `IndexMap` rule in `PORTING.md` |
| 7 | The front-end switch changes circuits, so persistent ids change. For a table, a LATENESS program or a non-materialized table this means a reset, not a bootstrap. | High | High | Runtime owner | Section 7.5.3: table ids as a hard gate; pipelines with state stay on their family; preflight; G6 and G9 cases |
| 8 | sqlparser-rs needs a patch set or a fork for the Calcite Babel syntax, positions and spans. Version 0.64 has a datatype hook; table-factor and join hooks remain absent. Each measured release has 79 to 186 changelog entries to rebase over. | High | Medium | Track F lead | Spike 4 on 0.62 and on the newest release; budget in section 5; upstream discussion on the hooks in P0b |
| 9 | On three workloads, two fresh JVMs and S2 files cost 1.46–1.58× the direct mean after the replay repair. The same-process split costs 1.69–2.14× at empirical p95. | High for file transport | Medium | Harness owner | Compare the actual Rust mid-end and transport against the [incremental Java baseline](crates/sql/BENCHMARKS.md#eight-workload-incremental-java-baseline) before the P3 1.2× gate. |
| 10 | Message text changes break existing exact-text assertions | Medium | Low | Track F lead | Use the Spike 9 inventory to migrate compiler assertions to the C0 fields |
| 11 | The port team produces few new SQL features from P0 to M2, and the Java front end slows from P5c with the freeze option | High | Medium | Engineering owner | Product integration for the P5a and P5b deliverables; M1 moves mid-end work to Rust; go or stop review at each phase exit |
| 12 | Dual maintenance does not end because state or sinks cannot migrate safely | Medium | High | Runtime owner | Fleet report, resolution order, support window with a maximum (decision 11), conditional M4; no forced migration |
| 13 | A compiler panic, stack overflow or memory growth stops the API or the compiler server, or loops | Medium | High | Runtime owner | Bounded worker processes with the result protocol of section 7.5.2; fuzzing and corpus tests add to the isolation |
| 14 | A mechanical port brings Java idioms and complexity into Rust | High | High | Track M lead | Section 4.1 from the first port, with the review per phase exit; spikes 2 and 8 exercise the review and parity requirements |
| 15 | Calcite defects at the pinned commit become the specification | Certain | Low | Track F lead | Record each known defect in the allow-list |
| 16 | Compatibility rules and pass invariants depend on knowledge held by one maintainer | Medium | High | Compiler maintainer | Document pass invariants, use the phase review in section 4.1, and record the ownership handover before M2 |
| 17 | Gen-2 doubles each oracle while its test coverage, its consumer and its rate of change are not known | High | Medium | Track M lead | Inspection of forge and an owner on that side in P0a; Gen-2 tests in CI; spike 3 threshold; the 7.4 rule for IR changes; decision 2 before P1 |
| 20 | Native code from Cranelift is slower than the present `rustc` build. The owner position is that the generated code is a small part of the run time. | Low | Medium | Track N lead | Spike N1 and C13 record the numbers; the row functions used by sort and merge are the part to watch |
| 21 | The native rows cannot keep the present state format, so each pipeline with state needs a bootstrap or a reset to change form, and M4 moves late | High | High | Runtime owner | Spike N2; decision 15; resolution order of section 7.5.1 keeps such pipelines on the Java tail |
| 22 | Generated machine code with raw layouts corrupts memory or state | Medium | High | Track N lead | C12 with fuzzing; sanitizer runs; a verifier for the row descriptors; G11 |
| 23 | The causes that removed `dataflow-jit` in 2023 apply again | High | High | Engineering owner | The reasons are recorded in section 4.2: three of them are not changed (serialization boundary, unsafe code, sqllib cover). Spike N1 must include one connector format and non-trivial sqllib calls. |
| 24 | The native back end has no structural oracle, and it overlaps with the Gen-2 engine | High | Medium | Engineering owner | C7 in native mode on the full corpus; decision 2 before P1 |

### 11.1 Stop and descope triggers

| Trigger | Number | Action | Decider |
|---|---|---|---|
| Unexplained probe differences of spike 5 after one extension | More than zero, per the spike 5 criterion in section 12 | Stop track F; option C, or option I for the type rules | Owners of decision 9 |
| Classified deviations of spike 5 | Count and user effect above the allow-list cap of section 8.3 | Review of decision 1 | Owner of decision 1 |
| Allow-list size at a gate | Above the cap | The gate does not pass | Owner of decision 1 |
| Share of ordinary programs with all view and other operator ids equal at the P5d exit and in G6 | Below 100%, selected on 2026-11-18 | Review of decision 4 | Owner of decision 4 |
| Gen-2 share of the corpus (spike 3) | Below 80% | Gen-2 mode ships with canonical-equal S3 only | Track M lead |
| Decorrelation cover (spike 6) | Below 95% | Record unsupported shapes and raise risk 2 probability to High | Track F lead |
| Spike N1 | The prototype does not pass C7 for the 10 programs, or a needed dbsp operator has no dynamic form | Decision 16 | Owners of decision 2 |
| After M1 | Decision 12 | Continue track F, or stop at option C | Owners of decision 12 |

Spike 6 initially measured C7 equality for 9 of 155 selected
first-inner-stage Correlate occurrences. The follow-ups verify 149 of 155
(96.13%), above the 95% criterion for that population. Saved corpus
transactions verify 130 occurrences. Paired classic runs using newly
seeded inputs for compile-only corpus programs verify 19 more. The
four-stage census retains 157 related cases, and risk 2 remains High.
[Original ledger](crates/sql/evidence/correlation-coverage.json) ·
[Weighted C7 evidence](crates/sql/evidence/correlation-spike6-weighted.json)

Spike 9 recorded 252 witnessed diagnostic body roots, including 12 found
in Python compiler cases. Keep their dispositions tied to decision 3.
[Diagnostic scope](crates/sql/evidence/diagnostic-scope-extension.json)

Option C stops after M1. It retains Calcite and the Java build, with a Rust
mid-end, back end and circuit IR crate for forge.

## 12. Spikes

Spikes 0, 1, 2, 3, 10, N1 and N2 are in P0a. Spikes 4, 5, 5b, 6, 7, 9, 11 and D1 are in P0b. Spike 8 is at the end of P1.

| # | Question | Design | Pass criterion | Feeds |
|---|---|---|---|---|
| 0 | Is the Java compiler deterministic at the dump level? | Compile the corpus 3 times in separate JVMs; compare S2, S3, S4 and the Rust output. Includes the S4 cross-pass state inventory and a replay strategy for each observation point. Starts after the dump flags exist; uses the Java `TestSerialize.Comparison`, extended for metadata, until the Rust comparator exists. | Canonical-equal graphs and metadata; byte-equal hash inputs and ids; nondeterminism classified before a Java change | Precondition of spikes 1, 2, 8 |
| 1 | Is S2 a complete boundary? | Add the read flag and the work items (b) to (g) of section 7.1 for a subset; run the corpus as front end, file, mid-end; compare with the direct run | Identical outputs and messages; list of the missing data | Strangler or front-to-back plan |
| 2 | Can the Java tail give equal ids from an S3 that Rust wrote? | On an IR subset that is discarded after the spike (operators, expressions, types, JSON decode and encode for 50 programs): port `MultisetAnalysis` and the prefix passes; add the S3 read flag to Java; run the Java tail on the Rust S3. Compare with the Java comparator of spike 0. Record defects and review findings under section 4.1. | C6b and C6c byte-equal; equal ids | Track M go/no-go |
| N1 | Does a native back end work on the dbsp dynamic layer, and how fast is it? | For 10 corpus programs with joins, aggregates, string and decimal functions and one connector format, with the batch interface and inline primitive operations (speed design 1 and 2 of section 4.2): row layout and descriptor, native row type that implements `Data`, Cranelift code for the expressions, circuit built from the IR, object file linked to a shared library and loaded with `dlopen2` by a test binary. Measure throughput, memory and compile time against the `rustc` form. | C7 passes for the 10 programs; numbers recorded; list of dbsp operators with no dynamic form | Track N go; C13 budget; decision 16 |
| N2 | Can native rows keep the present state format, and what do normalized keys give? | Compare the rkyv archive bytes of the Rust structs with a native encoder for the row types of the 10 programs; restore one checkpoint of a `rustc` pipeline in the native form. Prototype normalized keys (speed design 3) for the key types of the 10 programs and measure sort and merge against the field-by-field compare. | Yes or no for the state format, with the cause; a measured gain or loss for normalized keys | Decision 15; risk 21; row layout |
| 3 | Which share of the JUnit tests passes with `GEN2_PATH`? | Run `mvn test` with the Gen-2 binaries; put this run in CI | A share; 80% or more keeps Gen-2 in the C7 oracle | Engine for C7 |
| 4 | Does sqlparser-rs accept the Feldera surface, and nothing more? | Prototype dialect and token pass on 0.62 and on the newest release (0.64.0 on 2026-11-11), with the version recorded per result. Parse about 9,000 corpus statements; classify failures; keyword test; share of corpus error positions that the spans can reproduce. A wrong parse is found by comparison with the S1 dump of the same statement. Count the statements that only Rust accepts: mutated corpus statements, and one statement for each AST field outside the Feldera grammar. | Under 2% failures with a known fix each; zero silent wrong parses; a reject pass that covers each over-accepted form | Patch set, fork or own parser |
| 5 | How far is a ported type derivation from Calcite? | Type probes from the generation rule of section 8.1 through a Java server loop; prototype decimal, CHAR and nullability rules for 40 functions, arithmetic, CASE, set operations and aggregates. Translate one slice directly from Calcite (implicit coercion and return-type inference for the 40 functions, option I) and compare semantic agreement with the rewritten slice. | Zero unexplained differences; classified deviations under the cap of section 11.1; a mode (rewrite or translate) recorded for validator, coercion and SqlToRel | Size of the binder; feasibility of the parity goal |
| 5b | Can a minimal A binder retain parity through FRel? | Name resolution and typing for the subset of spike 5, to FRel on the held-out statements | C0 and C1a measured on the corpus split of D1 | The A side of D1 |
| 6 | Which correlate shapes occur, and can a ported decorrelator handle them? | Instrument steps 1, 17, 19, 22 to dump shapes on the corpus, with the cases where Calcite keeps a Correlate; prototype on FRel for the top clusters. Reference: the Apache-2.0 lowering code of old Materialize versions (section 9.2). | Shape list. Cover means equal C7 results through the S1 importer, weighted by occurrence: 95% or more. | Risk 2 |
| 7 | How far do sqllib and Janino folding differ, and can the fold registry build without dbsp? | Collect the constants folded at level 2 on the corpus; evaluate with both; list the sites where the compiler requires a constant. Build the fold registry for `x86_64-pc-windows-msvc` and `wasm32`. | Classified list; zero verdict-relevant differences without a fix or an allow-list entry; build result and dependency boundary of the sqllib split | Allow-list and fold architecture |
| 8 | Do representative Rust passes preserve the Java contracts? | Port `CSE`, `FuseIntoArrayAgg` and a 600-line slice of `InsertLimiters` against S4. A maintainer reviews the code against section 4.1. Record defects and repairs. | S4 parity and maintainer acceptance | P1 architecture and parity review |
| 9 | How many message templates do the tests depend on? | Capture error and warning assertions, including their exact rendered targets. Three shared helpers currently cover 375 Java call sites; direct field, list, prefix, and absence assertions require separate accounting. Include all 281 Python assignments. Attribute asserted text to message bodies, classes, snippets, and rendering before clustering against emitting sources and Calcite resources; include the separate platform assertion and distinguish compiler errors from runtime errors. Incidental or suppressed diagnostics are not required templates. | Auditable count, inventory and exact-text dependencies | Message parity scope |
| 10 | Which stock Calcite rules fire, and which STANDARD and spatial operators validate but are not in the documented list? | Counter per rule in `CC/optimizer/CalciteOptimizer.java`; run the corpus; dump the operator table | Rule port list; operator list | Size of `sql-rel` and of the registry |
| 11 | Is S1 lossless? | Java exporter and importer; lowering from the import gives canonical-equal S2 | 100% of the corpus | P4 oracle; D1d |
| D1 | H or A for the binder? | Section 3.4 | Decision rule of section 3.4 | Front-end base |

Decision tree after P0:

| Result | Action |
|---|---|
| Spike 0 fails | Classify the nondeterminism, repair it in Java, and do spike 0 again before spikes 1 and 2 |
| Spikes 0, 1 and 2 pass | Track M proceeds back to front as planned |
| Spike 1 fails | Repair the seam or stop shipping the hybrid. An executable boundary is required before the migration order changes. |
| Spike 2 fails on ids | Diagnose the hash inputs with C6c. If the Java tail cannot give equal ids, the M1 hybrid needs the bootstrap rules of section 7.5.3. |
| Spike N1 passes | Track N proceeds; its speed numbers set the C13 budget |
| Spike N1 fails | Decision 16 |
| Spike N2 says no | Each pipeline with state needs a disposition to change form (section 7.5.3); M4 gets a later date |
| Spike 5 passes; D1 is complete and H fails the parity floor | Track F proceeds with the own binder (A) |
| Spike 5 passes; H meets the parity floor, D1d and the FRel contract | The owner can reconsider decision 9 with that evidence |
| D1 evidence or its execution oracle is incomplete | One extension, then A |
| The type rules of spike 5 fail (trigger of section 11.1) | Stop track F. Keep Calcite as the front end and ship track M only (option C). Or review option D with a native-image trial of the Java front end to S2 (blockers, start time, peak RSS, binary size). |
| Spike 4 fails | Feldera fork of sqlparser or an own parser |
| Spike 6 covers under 95% | Record uncovered Correlate shapes and raise risk 2 probability to High |
| Spike 11 fails | P4 uses C3 as a metric and C7 as the oracle. D1d needs another executable oracle. |

## 13. Decisions needed from the owners

Decisions 1, 2, 7 and 8 are preconditions of P0. Each decision needs a named owner before P0 starts.

| # | Decision | Options | Effect | Decide by |
|---|---|---|---|---|
| 1 | Observable type and schema parity | Require exact parity with Calcite. A future exception needs an approved revision of the zero-exception cap in §11.1. | Keep C1a and C9 as exact compatibility contracts. | Taken on 2026-11-18 |
| 2 | Which engines stay? | Keep native DBSP and Gen-2 | The 77.34% measured Gen-2 share activates the canonical-equal S3 scope in section 11.1. Track N continues with its separate C7 and state gates. | Taken on 2026-11-18 |
| 3 | Compiler diagnostics | Keep exact verdict, count, warning flag, error class, start position and snippet. Allow reviewed wording changes. | Use C0 for parity and keep end position as a metric. | Taken on 2026-11-18 |
| 4 | Persistent ids at the front-end switch | Require equal table ids. For eligible pipelines, allow bootstrap after preflight. For pipelines with LATENESS or a non-materialized source, require equal ids, keep the current compiler family, or obtain the pipeline owner's approval for a reset. | Apply the state policy in §7.5.3 and the 100% review threshold for ordinary pipelines in §11.1. | Taken on 2026-11-18 |
| 5 | Compiler for existing supported `runtime_version` values | Keep each version's associated Java compiler and jar path. | Retain the JRE while those runtime versions remain supported. | Taken on 2026-11-18 |
| 7 | Java policy during the port | Continue normal Java development under the compatibility and corpus rules in §7.4. | Apply the structural notice, corpus and Gen-2 compatibility rules during development. | Taken on 2026-11-18 |
| 8 | CLI options and side features | Keep features used by product paths or documented user paths. Audit the callers of the remaining features before assigning test-only or removable status. Keep `--anonymize` under §8.4. | The [CLI scope audit](crates/sql/CLI-SCOPE.md) records the existing dispositions. | Taken on 2026-11-18 |
| 9 | Binder base | Stop D1 and keep A as the planning assumption | D1 stopped under the incomplete-evidence rule of section 3.4 | Taken on 2026-11-18 |
| 10 | `use_platform_compiler` after M2 | Remove the flag at cut-over. | Prepare the transition for installations that use the flag today. Existing supported runtime versions keep their associated Java compiler under decision 5. | Taken on 2026-11-18 |
| 11 | Support window of the Java line, and who decides M4 | 12 months after M2 as the maximum (assumed) / other length | Sets the end of dual maintenance and the extension policy | Before P6 |
| 12 | After M1: continue track F, or stop at option C | Continue (assumed) / stop | Use the measured effect on image size, JVM startup, CI and dependencies, along with front-end parity and scope. | M1 |
| 14 | Output form of the native back end | Shared library, loaded dynamically by a prebuilt pipeline binary through a C ABI (selected on 2026-11-12) / JIT from the circuit IR at pipeline start | Section 4.2, rules of the dynamic link | Taken |
| 15 | State at the change to the native form | For incompatible checkpoints, allow a documented per-pipeline bootstrap after preflight or an owner-approved reset | Section 7.5.3 governs eligibility and explicit requests for stateful pipelines. Compatible checkpoints can restore directly when verified. | Taken on 2026-11-18 |
| 16 | If spike N1 fails | Port the Java back end to Rust and keep `rustc` (the plan before 2026-11-11) / Gen-2 only / Cranelift for expressions inside Gen-2 | Sets the back end of the Rust compiler | P0a review |

## 14. Out of scope, and later gains

| Item | Note |
|---|---|
| New IR semantics and new optimization algorithms | After the cut-over. Idiomatic Rust representation, sound architecture, and human readability are required during the port (section 4.1). |
| New SQL features (MATCH_RECOGNIZE, NTILE, outer UNNEST, native SEMI and ANTI joins) | After the cut-over; each is then Feldera work with no upstream help in option A |
| One dialect for compiled and ad-hoc SQL (lower FRel to a DataFusion plan) | Later project; removes the differences listed in `docs.feldera.com/docs/sql/ad-hoc.md` |
| wasm build, LSP, completion in the web console | Enabled by the front-half crates and the fold crate without dbsp; not planned here |
| SQL compile cache in pipeline-manager | Possible once the compiler identity of section 7.5 is part of the cache key |
| Cost-based join ordering | Inventory all configured rules, including MultiJoin and hypergraph steps. Non-firing on the corpus is not proof a rule is unnecessary; add targeted activation cases and classify each omission for legality, semantics, and performance before approval |

## Appendix A. Evidence and its limits

The investigation used 21 agents in two rounds. Round 1: 9 readers of the repository, 3 web researchers, 3 independent architects (DataFusion-first, own planner, strangler). Round 2: 3 fact-checkers and 3 critics (feasibility, DataFusion steelman, completeness) reviewed the first draft; this version includes their 95 findings. A later review revision (see the header) checked contracts, dependencies, rollout and gates against local source, and a follow-up review of that revision added 12 corrections. The subsequent adversarial review identified seven further gaps in DataFusion planning, S4 replay, crate dependencies, connector diagnostics, and state and sink migration. That revision addressed them in the design and the acceptance gates. A further adversarial review (5 reviewers, each followed by a skeptic who read the cited code) gave 69 findings, none refuted. The plan incorporated those findings in sections 7.5, 8.2 to 8.4, and 11 to 13. The owner accepted the shadow sample sizes, the 80% Gen-2 threshold, and the 12-month support window on 2026-11-11. The back-end revision in section 4.2 drew on `crates/dbsp/src/dynamic/`, the former `dataflow-jit` crate and the forge README. The original investigation did not run the runtime suite. The P0 evidence records later measurements.

The DataFusion-first architect recommended planning with type checks as a
gate. The other two recommended an own binder. All three named sqlparser-rs
with an own binder as the fallback. The DataFusion source audit led to
option H and the comparison in section 3.4. Both prototypes match Java on
59,264 generated function and arithmetic cases, and H passes the focused
D1d set fixtures. The frozen held-out baseline measures the current prototypes'
reach. Equivalent FRel and complete C0/C1a remain open. [D1 evidence](crates/sql/D1.md)

| Statement | Basis | Confidence |
|---|---|---|
| Sizes, counts, code locations in the repo | `wc`, `grep`, `git log`, source reading; checked again in round 2 | High |
| Calcite usage (32 steps, rule lists, 17 RelNode classes) | Source reading | High |
| DataFusion 55.2.0 and sqlparser 0.62.0 structure, hooks and gaps | Source reading in the cargo registry; one scratch parse test of sqlparser | High for structure; medium for issue status |
| Sizes of Calcite classes (RelDecorrelator, RelBuilder and others) | A Calcite checkout near, not at, the pinned commit | Indicative |
| No drop-in Rust alternative | Web research; several candidates from search summaries only | Medium |
| Corpus sizes (about 3,800 expected tables, 163 runtime-failure sites, 265 IR tests) | Estimates from grep counts | Medium |
| SQL compile-time baseline | Eight workloads each ran in cold and warm JVMs in classic and Gen-2 modes, both with standalone defaults and with incremental pipeline flags. The four-workload cold worker pilot also measured concurrent RSS and cancellation. See `crates/sql/BENCHMARKS.md`. | Java CLI baseline measured; deployment and generated-circuit budgets open |
| Gen-2 test coverage | Measured compiler against forge `d4879df7`: 2,218 passes, 69 failures, 510 errors and 71 skips out of 2,868 JUnit tests; 77.34% overall and 79.30% excluding skips. `crates/sql/evidence/gen2-ci-runner.json` pins that build. | Measured in the Linux ARM64 devcontainer; below the 80% gate |
| feldera/forge coupling | `circuit-ir` decodes compiler IR and `circuit-loader` lowers it to `circuit-runtime`. `crucible` and `ir-expander` provide the Java test protocol. See `crates/sql/P0.md` for pins and contract-test requirements. | Decision 2 was taken on 2026-11-18. The plan owner confirmed the Forge IR contract owner. The consumer compatibility matrix remains open. |

## Appendix B. Prior art

| Project | What it did | Lesson used here |
|---|---|---|
| RisingWave | Removed its Java/Calcite front end in 2022 for a Rust binder and optimizer on a sqlparser fork; ran both front ends for a time | Own planner for incremental streaming SQL; dual run |
| Materialize | Own Rust planner (HIR, MIR); decorrelates almost all sub-queries | Decorrelation is the central planner problem when no nested-loop fallback exists |
| Arroyo | Plans with DataFusion on forks of DataFusion and sqlparser; left generated Rust for an interpreter, with compile time as a reason | Fork maintenance when DataFusion owns a streaming plan; relevant to decision 2. Its UDFs are shared libraries with a C ABI, loaded with `dlopen2`: the pattern of the dynamic link in section 4.2. |
| Sail | Spark dialect on stock DataFusion with its own parser and resolver, no fork; runs the PySpark suite as oracle | A foreign dialect needs its own parser and resolver. DataFusion `Expr` and `LogicalPlan` can be the target without a fork. Sail gives no data on `SqlToRel` reuse (option H). |
| Flink SQL | Stays on Calcite; one Calcite change can cause hundreds of plan changes | Test the upstream planner boundary on each upgrade |
| Comet | Per-expression compatibility guide against Spark | Publish a compatibility matrix |
| fish (C++ to Rust) | Kept the structure and a working program at each step (hybrid build) | Model for the staged port |
| TypeScript (to Go), Bun (Zig to Rust) | Translated the whole tree with the structure near 1:1, then converged on the existing test suite | Port the behaviour first; new semantics and algorithms later. Section 4.1 overrides the 1:1 structure of these ports. `PORTING.md`; language-neutral tests. |
| `dataflow-jit` (this repository, 2022-12 to 2023-11) | A Cranelift back end with its own row layout and formats, 47.4k lines, removed in commit `1c061bf5a` | The reasons are recorded in section 4.2. Spikes N1/N2 must test the risks that remain. |
| SQLancer | Metamorphic oracles (query partitioning, NoREC) found logic bugs that cross-engine comparison missed | Comparator C8 |

## Appendix C. Sources

| Topic | Link |
|---|---|
| DataFusion sub-query epic | https://github.com/apache/datafusion/issues/5492 |
| DataFusion logical types epic | https://github.com/apache/datafusion/issues/12622 |
| DataFusion extension types epic | https://github.com/apache/datafusion/issues/12644 |
| DataFusion "Extending SQL" guide | https://datafusion.apache.org/library-user-guide/extending-sql.html |
| DataFusion data types | https://datafusion.apache.org/user-guide/sql/data_types.html |
| DataFusion upgrade guides 55, 56 | https://datafusion.apache.org/library-user-guide/upgrading/55.0.0.html , https://datafusion.apache.org/library-user-guide/upgrading/56.0.0.html |
| sqlparser-rs, span epic | https://github.com/apache/datafusion-sqlparser-rs , https://github.com/apache/datafusion-sqlparser-rs/issues/1548 |
| Arroyo plugin system (dynamic link of UDFs, C ABI, `dlopen2`) | https://www.arroyo.dev/blog/rust-plugin-systems/ |
| Arroyo forks; blog | https://raw.githubusercontent.com/ArroyoSystems/arroyo/master/Cargo.toml , https://www.arroyo.dev/blog/why-arrow-and-datafusion |
| GreptimeDB forks | https://raw.githubusercontent.com/GreptimeTeam/greptimedb/main/Cargo.toml |
| Denormalized fork | https://raw.githubusercontent.com/probably-nothing-labs/denormalized/main/Cargo.toml |
| Sail on stock DataFusion | https://raw.githubusercontent.com/lakehq/sail/main/Cargo.toml |
| Comet compatibility | https://datafusion.apache.org/comet/user-guide/latest/compatibility/index.html |
| Materialize decorrelation; licence | https://scattered-thoughts.net/writing/materialize-decorrelation , https://raw.githubusercontent.com/MaterializeInc/materialize/main/LICENSE |
| RisingWave front end; removal of the Java front end | https://raw.githubusercontent.com/risingwavelabs/risingwave/main/src/frontend/Cargo.toml , https://github.com/risingwavelabs/risingwave/pull/2519 |
| Flink and Calcite | https://go.mail-archive.com/dev@flink.apache.org/msg56149.html |
| Calcite native-image | https://issues.apache.org/jira/browse/CALCITE-4786 |
| Bun port | https://bun.com/blog/bun-in-rust |
| fish port | https://fishshell.com/blog/rustport/ |
| TypeScript native port | https://devblogs.microsoft.com/typescript/typescript-native-port/ |
| SQLancer query partitioning (TLP); NoREC | https://doi.org/10.1145/3428279 , https://arxiv.org/abs/2007.08292 |
