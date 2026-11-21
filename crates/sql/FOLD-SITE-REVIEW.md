# Independent constant-site review

This automated source review assesses PLAN.md spike 7 against the existing
prototype and evidence. It is not maintainer acceptance or approval of a semantic
deviation. The reviewed evidence establishes equality on measured populations;
it does not establish complete coverage of every verdict-relevant constant site.

The review found and repaired one concrete fixture gap: folded regular-expression
patterns can change compiler warnings and hence acceptance under warning
promotion. It also corrected the inventory's omissions and distinguished hard
literal requirements from type checks and parser restrictions. The follow-up also repaired two demonstrated Java ROW-selector defects. The
Rust evaluator is unchanged.

## Findings and disposition

### FS1: folded regex warnings were absent from the focused population — repaired

`ExpressionCompiler.checkRegexLiteral` examines only non-null
`DBSPStringLiteral` values, then reports the warning returned by
`RustRegexChecker.unsupportedConstruct`. `makeRegex` invokes the check for RLIKE
and REGEXP_REPLACE. Thus replacing `CONCAT('a', '(?=b)')` with a literal can
produce a warning that an unreduced expression would avoid. With
`FELDERA_WARNINGS_ARE_ERRORS`, this is a verdict-relevant fold even though the
regex function itself accepts a dynamic pattern. Existing `RegexWarningTests`
cover literal, dynamic, and NULL patterns but do not establish folded-pattern
equivalence under all warning policies.

The focused generator now includes 80 cases: two functions, ten pattern shapes,
and four promotion/suppression combinations. Shapes include CONCAT,
concatenation, cast, both lazy-branch directions, NULL, dynamic patterns, and
supported patterns. The searched value remains dynamic. Both input columns are
projected so unrelated unused-column warnings cannot mask a changed verdict.
The regression test checks those properties and the actual regex suppression
setting, rather than merely counting fixtures.

The targeted S3 run used the frozen v46 compiler and the frozen legacy Janino
Rust worker from the window-default certificate. Baseline, traced Java, and Rust
match all **80 cases: 70 accepted, 10 rejected, zero diagnostic/schema/verdict
differences, zero compiler bugs**. Independent expected-warning and verdict
assertions pass for every case. The trace records 20 changed reductions to the
unsupported pattern, demonstrating that computed patterns reached the observed
executor. Other cases need not traverse that executor to test their final
diagnostics. All 12 Python fold-probe tests pass.

The certificate is [`evidence/fold-regex-sites-s3.json`](evidence/fold-regex-sites-s3.json).
It records classpath, input, worker, source, script, and artifact hashes. Raw
inputs, runner, snapshots, logs, and output records remain under
`$SCRATCH/p0-fold-regex-review-v2` in the devcontainer. The initial fixture run is
preserved separately in `v1`: comparisons agreed, but stronger policy assertions
exposed unrelated unused-column warnings. No original corpus evidence was
overwritten. This supplement does not execute regex runtime functions or rerun
the full corpus, and does not turn prior certificates into 1,472-case results.

### FS2: site inventory omitted important classifications — documented

The updated [`fold-probe/CONSTANTS.md`](fold-probe/CONSTANTS.md) distinguishes:

| Consumer | Source-backed classification |
|---|---|
| `CalciteToDBSPCompiler.compileTumble` size | Hard non-null, positive short-interval literal requirement after DBSP simplification. |
| `LeadLagAggregates.getLeadLagAmount` | Hard non-null i32 literal offset requirement. |
| `LeadLagAggregates.implement` default | Requires an entry in `Window.constants`; a remaining projected expression is rejected. The separate 96-case window-default supplement addresses this consumer. |
| `RangeAggregates.compileWindowBound` | Numeric/interval literal requirement after simplification; also reached by ROWS lowering. |
| `CalciteToDBSPCompiler.compileHop` | Checks short-interval types for size/interval, then constructs the operator; no analogous hard-literal guard. Optional TUMBLE start is also distinct from its size check. |
| ROUND/TRUNC/TRUNCATE in `ExpressionCompiler` | Error text says “constant integer”, but the guarded condition checks integer type only. It then casts width and emits a runtime call. NULL and integer-input branches have separate warning behavior. |
| `SqlToRelCompiler.compileSetOption`, `ProgramMetadata.set` | Literal option interpretation, preceded by restricted SET grammar. Arithmetic SET values are not missing executor tests. |
| DEFAULT, LATENESS, LIMIT/OFFSET/FETCH | Determinism, type, or validator rules must be distinguished from a hard lowering requirement for a Rex literal. Existing fixtures retain parser/validator failures, which are not successful fold evaluations. |

The SET classification is supported by `src/main/codegen/config.fmpp`, which
selects `SqlSetOption`, and the inspected Calcite parser template: its value
alternatives are `Literal()`, `SimpleIdentifier()`, and ON. The template is
`$SCRATCH/p0-parser-calcite/core/src/main/codegen/templates/Parser.jj`, SHA-256
`90fb231cc86c4b3c4d07d45eb1df39d568c12e5cf414ca6fedbbd38aa5b92650`.
No Calcite Java source archive was available in the checked local Maven cache;
this review does not independently certify every validator implementation.

### FS3: keyword and ROW consumer reachability — resolved on the measured snapshot

The follow-up source audit resolves keyword grammar for DATEDIFF,
TIMESTAMPDIFF, TIMESTAMP_DIFF, TIMESTAMP_TRUNC, temporal FLOOR/CEIL, and EXTRACT:
`TimeUnitOrName()` constructs a `SqlIntervalQualifier`, not an arbitrary scalar
expression. TRIM constructs its flag from a grammar token. DATE_TRUNC is an
exception: its grammar admits expression arguments for two dialect shapes, so
its validator restrictions require separate checks.

A 140-case follow-up matrix covers those controls and ROW selectors under all
warning policies. The initial frozen-v46 run found 28 compiler-bug cases: literal
and cast string selectors on an anonymously cast ROW lost field metadata in
lowering; computed, dynamic, and NULL string selectors hit Calcite's non-null
assertion. The latter diagnostics include a `SqlCallBinding` identity string,
causing 20 differences between independent runs. These are failed oracle cases,
not valid SQL rejection or approved deviations. The failed raw inputs and all
three outputs remain in `$SCRATCH/p0-fold-consumer-review-v1`.

Disassembly of the pinned dependency jar confirms the asymmetry:
`SqlItemOperator.checkOperandTypes` calls `getOperandLiteralValueOrThrow` for
string ROW selectors, but diagnoses a null literal value with `illegalRowIndex`
for integer ROW selectors. `SqlCallBinding.getOperandLiteralValue` inspects the
SQL operand; the normal optimizer executor has not yet run. Integer `1 + 0`,
NULL, and dynamic selectors are rejected before folding. A string literal and a
literal cast pass this earlier check. Therefore the missing string diagnostic
must preserve the existing literal restriction, rather than introduce general
SQL expression evaluation in validation. `FelderaSqlValidator` now reports that
existing error for nonliteral/NULL
string selectors. `ConvertletTable.convertItem` resolves validated named fields
to RexFieldAccess before cast simplification can erase their names.

The repaired frozen overlay matches all **140 cases: 44 accepted, 96 rejected,
zero compiler bugs, and zero baseline/Java/Rust differences**. Five targeted Java
tests cover optimization levels 0 and 2. Independently specified final circuit
constants are exactly `(17, 29, 17, 29)` for named and positional selectors,
including quoted mixed-case field names. Missing fields remain located errors;
MAP and ARRAY still accept dynamic selectors. Repeated invalid-selector
diagnostics compare exactly. Raw repaired evidence is retained in
`$SCRATCH/p0-fold-consumer-review-v2`.

The final full Maven compilation rebuilt 873 main and 355 test sources. All 12
selected Java tests pass (five ROW tests and seven existing constant-required
tests); all 13 Python fold-probe tests pass. A new frozen snapshot then ran the
entire expanded focused population: **1,612 cases, 744 accepted, 868 rejected,
zero compiler bugs, zero baseline/Java/Rust differences**. This run includes the
earlier focused sites and both new supplements. It is not a full-corpus replay
or Rust runtime test. Its certificate and verified archive index are
[`fold-consumer-sites-s3.json`](evidence/fold-consumer-sites-s3.json) and
[`fold-site-review-archive.json`](evidence/fold-site-review-archive.json).

The keyword controls verify parser rejection before folding. DATE_TRUNC
expression forms reach validation but are rejected for unsupported argument
types in this compiler configuration. The current TIMESTAMP_DIFF alias is also
rejected during overload resolution; that is not a successful fold witness.
These consumers now have a source/bytecode-backed owner and measured boundary
classification.

### Remaining observation scope

More broadly, `FoldTrace.wrap` observes planner-executor requests and results;
`RexOptimize` records its changed temporal results. The Rust replacement follows
those boundaries, including eligibility and batch behavior. It does not replace
all literal construction in the Calcite validator, `RexBuilder`, convertlets, or
the DBSP `Simplify` used by the hard window guards. `FelderaRexBuilder.makeCast`
still delegates eligible conversions to the inherited builder. Therefore a
successful comparison with the Rust executor can still rely on Java folding
before or after that executor.

Before claiming complete site coverage, attach each inventoried hard guard and
warning consumer to a source-backed classification and a witness that reaches
it. Retain parser-negative cases separately. For a verdict-dependent fold,
compare the expression/result and final diagnostic/verdict with the replacement
enabled, or record the earlier/later folding owner explicitly. Existing trace
records identify evaluator and case, but do not themselves provide that complete
consumer map. Unsupported observations and non-literal Java results must remain
visible rather than becoming equality passes.

### Explicit pre/post-executor consumer audit

[`fold_site_inventory.py`](scripts/fold_site_inventory.py) now audits the explicit
`new Simplify(...)` and `.reduce(...)` calls in Feldera's frontend. All ten
current calls have a source-pinned classification. A new unclassified call or
missing/ambiguous source anchor fails the audit. Three additional pre-executor
conversions are recorded separately. The generated
[`fold-site-source-inventory.json`](evidence/fold-site-source-inventory.json)
includes fixture identities and source hashes; fixture presence is explicitly
not reported as branch coverage.

| Owner outside the observed executor | Verdict relevance and measured scope |
|---|---|
| `FelderaRexBuilder.makeCast` | Delegates literal conversions to Calcite except the explicitly retained runtime-sensitive casts. Existing interval/temporal/unsigned fixtures compare final behavior, but inherited conversion logic has no independent Rust replacement in this experiment. |
| `ConvertletTable.convertCast` numeric-to-interval branch | Truncates numeric SQL literals toward zero for non-second units before constructing the interval. Six new TUMBLE shapes check positive, negative, fractional-to-zero, second fractions, long intervals, and zero under all policies. |
| `ConvertletTable.convertLiteralChain` | Concatenates adjacent SQL string literals before the executor. New multiline literal-chain RLIKE/REGEXP_REPLACE cases check both unsupported and valid patterns with warning promotion/suppression. |
| TUMBLE and RANGE/ROWS bound `Simplify` calls | Feed the already inventoried hard literal guards. Existing focused window cases and the numeric-interval supplement compare final acceptance. The exact simplifier invocation is not traced. |
| `DBSPExpression.reduce` | Performs beta reduction followed by up to ten DBSP Simplify iterations. Output casts and generated signed window conversions use it without imposing an additional hard literal guard. |
| `TableContents.addToTable` | Wraps casts and simplifies stored INSERT/REMOVE contents. `compileModifyTable` emits “Statement ignored” outside testing; `compileConstantProject` does not itself assert constant expressions despite its name/comment. Stored-content value equivalence is not covered by the executor trace or the new supplement. |
| Linear SUM and the mid-end `CircuitOptimizer` | Simplify generated expressions and whole circuits; no additional hard constant requirement is established by these call sites. Their simplifier can emit verdict-relevant warnings. New dynamic-numerator division/modulo cases check computed zero, safe controls, both lazy directions, and the actual division-warning suppression option. |

The new **64-case** supplement matches baseline, traced Java, and Rust: **43
accepted, 21 rejected, zero compiler bugs, zero differences**. Independently
predicted warnings and acceptance pass for every case. Only the post-simplifier
family produces executor records (12 Janino observations); numeric-interval and
literal-chain cases produce none. This demonstrates why an executor-only census
cannot stand in for a consumer inventory. It does not prove a Rust implementation
of the unobserved conversions: the current Java frontend still owns them.

The supplement's certificate and verified raw archive are
[`fold-outside-executor-s3.json`](evidence/fold-outside-executor-s3.json) and
[`fold-outside-executor-archive.json`](evidence/fold-outside-executor-archive.json).
The Java workflow runs the source audit explicitly and uploads its JSON even
when the audit fails. Existing Python test discovery also executes the audit's
current-source test. All 16 fold/audit Python tests and actionlint pass.

This closes the identified literal-chain, numeric-interval, and simplifier-warning
fixture gaps and makes new explicit local call sites reviewable. Inherited
Calcite validation/builders, hidden dynamic dispatch, stored table-content
values, and exact branch attribution remain outside the audit's completeness
claim. The generator now contains 1,676 cases; the 64-case supplement does not
retroactively change the earlier 1,612-case full focused certificate.

## What the existing measurements establish

`fold-legacy-final-s3.json` establishes equality for 8,571 classic S3 programs,
16,796 comparable Java values, and 1,296 focused compilations under the explicit
`legacy-janino-v1` compiler profile. It retains unsupported observations and
non-literal Java results. The later `fold-window-default-s3.json` covers 1,392
focused compilations, including 96 LEAD/LAG defaults, with one corpus smoke case;
it is not another full-corpus run. Both retain an open coverage flag.

The 5,240 runtime vectors use `runtime-v1`. Equality under the legacy compiler
profile does not make these profiles interchangeable or approve their semantic
differences. Windows and wasm evidence demonstrates rlib compilation, not linked
execution on those targets. The 25–44 engineer-day split price remains a
provisional estimate, not an approved staffing commitment. Full runtime
extraction and production version negotiation are later implementation work;
they should not be silently added to spike 7's P0 acceptance criterion.

The regex omission and the identified keyword/ROW reachability gaps are closed
on the measured snapshots. Complete coverage of other pre/post-executor folding
remains a limit on extrapolating the results to an independent frontend, not an
additional gate in PLAN.md §12. The following adjudication supersedes earlier
open-ended statements that treated this broader coverage as a spike-7 blocker.

## Spike 7 criterion adjudication

**The measured engineering experiment is complete against the actual §12
criterion.** The criterion is: “Classified list; zero verdict-relevant differences
without a fix or an allow-list entry; build result and price of the sqllib split.”
It does not require exhaustive branch/type coverage, a complete runtime-kernel
extraction, replacement of every inherited Calcite/DBSP helper, execution on
Windows/wasm, production negotiation, or approval of the engineering estimate.
Those remain implementation or acceptance decisions in their respective scope.

The source/consumer inventories now classify the demonstrated constant-required
and warning-sensitive sites, including pre/post-executor ownership. Full-corpus
replacement preserves all 8,571 verdicts, schemas, and diagnostics, and all
16,796 comparable Java values match. Known measured verdict differences were
fixed, including warning eligibility/batch behavior and the later NULL/ROW
oracle defects. No observed verdict mismatch needs an unrecorded allow-list
entry. The stronger 1,612-case focused run and subsequent 64-case supplement
preserve this result on their separately pinned snapshots. Windows MSVC and
wasm rlib builds provide the requested build result, and SPLIT.md supplies the
25–44 engineer-day price with work packages and assumptions.

The adjudication rechecks the raw full-corpus journal and evaluation hashes,
rather than interpreting `zero_comparable_differences` as universal value parity:

| Remaining observation class | Count | Disposition |
|---|---:|---|
| Offline Rust unsupported; Java left the expression unchanged | 154 | Explicit refused/unimplemented forms. No value equality is inferred. |
| Offline Rust unsupported; Java changed the expression | 106 | Unimplemented/no-fold limits. Of these, 99 have a canonical Java value; seven are ROW results outside the comparator's value representation. These are **not** measured Java/Rust value agreements. Examples include hyperbolic functions, SIGN, decimal string casts, typed collection casts, interval casts, and row constructors. |
| Offline Rust runtime error | 666 | Every corresponding Java journal result is unchanged and has no canonical literal value. These are refusals, not equal-value passes. |
| Offline Rust value without a canonical Java reference | 410 | Excluded from value agreement; final compilation parity remains separately measured. |

The 260 unsupported rows above belong to offline evaluation of the Java journal.
They are not the active worker's call count. The replacement worker journal has
16,720 value replies, 676 runtime-error replies, and 2,178 unsupported replies;
the latter include dynamic expressions and repeated temporal requests created
by the replacement's traversal. All reasons remain classified in
[`fold-criterion-adjudication.json`](evidence/fold-criterion-adjudication.json),
alongside representative offline expressions, source hashes, and input hashes.

`RustFoldExecutor` and `JaninoFoldEligibility` match the source hashes of the
full-corpus certificate. The replacement makes **no Java value-evaluation
fallback**: an eligibility failure retains the original executor batch;
unsupported/budget replies retain the original Rex expression; runtime errors
restore the entire original planner-executor batch. Java still performs
validation, literal construction, conversion, and DBSP simplification. Thus the
unsupported classes are bounded no-fold behavior with no measured final verdict
impact, not proof that Rust implements their value semantics or could replace
the remaining Java owners today.

This is an independent engineering disposition of the measured experiment.
Historical certificates keep their original conservative coverage/review flags.
The explicit legacy profile preserves the measured Java behavior; it does not
approve a production semantic change or make legacy and runtime profiles
interchangeable. Maintainer acceptance, adoption of the split estimate, and
production version/platform policy remain separate. None of those statements
asserts that P0 as a whole is finished.
