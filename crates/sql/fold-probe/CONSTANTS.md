# Constant-required sites

The measured engineering experiment meets PLAN.md §12 spike 7's criterion:
a classified list, no unhandled measured verdict-relevant differences, target
build results, and a split price. This is not a claim of exhaustive branch
coverage or a complete independent Rust frontend. The precise adjudication and
remaining implementation/approval boundaries are in
[`../FOLD-SITE-REVIEW.md`](../FOLD-SITE-REVIEW.md#spike-7-criterion-adjudication).

This inventory distinguishes a literal requirement from a deterministic
expression requirement. All paths below are relative to the compiler's
`src/main/java/org/dbsp/sqlCompiler/compiler` directory unless stated otherwise.
The focused SQL population includes positive and negative forms and all four
combinations of warning promotion and suppression. It supplements, rather than
replaces, the exported positive corpus.

Expression-only fixtures must not introduce an unused input table. Such a table
produces an unrelated warning that can reject both compilers under promotion,
masking a fold-induced verdict difference. The stronger generator introduces
`t` only for fixtures that read it, and tests this invariant explicitly.

| Site | Requirement and consequence | Coverage |
|---|---|---|
| `frontend/CalciteToDBSPCompiler.compileTumble` | After inner simplification, the size must be a non-null, positive short-interval literal. A refused fold can reject the program. | Literal, multiply/divide, zero, negative, month interval, column reference, lazy CASE, NULL. |
| `frontend/aggregates/LeadLagAggregates.getLeadLagAmount` | The offset must lower to a non-null i32 literal. It is read while building the lag operator. | Literal, arithmetic, string cast, ABS, column, NULL, division by zero, unreachable failing branch; LEAD and LAG. |
| `frontend/aggregates/LeadLagAggregates.implement` (third argument) | The default must be an entry in `Window.constants`, which contains Rex literals. A deterministic expression that remains an input projection is rejected, independently of offset validation. NULL is allowed here. | Literal, arithmetic, cast, ABS, column reference, NULL, failing arithmetic, both lazy-branch directions, invalid cast, overflow and decimal-to-integer conversion; LEAD and LAG under all four warning policies. |
| `frontend/aggregates/RangeAggregates.compileWindowBound` | The bound must simplify to a non-null positive numeric/interval literal before constructing a window. ROWS lowering also reaches this check. | ROWS and RANGE: literal, arithmetic, ABS, negative, NULL, failing arithmetic, column. |
| LIMIT/OFFSET/FETCH | Calcite's `SqlValidatorImpl.validateOffsetFetchExpression` rejects NULL literals, negative numeric literals, column references, and aggregates before the observed executor. `CalciteToDBSPCompiler.visitSort` compiles the remaining expression and casts it to usize; it does not require a Rex literal. | Positive/negative literal, expression, cast, NULL, and column forms. Rejection during parsing/validation is retained. |
| Column DEFAULT | `SqlToRelCompiler.validateConstantExpression` validates a synthetic expression-only query. `visitors/outer/DeterministicFunctions` checks nondeterminism later in the optimizer; its message says “compile-time constant”, but it does not require a literal. S2 mode stops before that check; S3 includes it. | Integer arithmetic, casts, overflow, NULL, column reference, failing arithmetic, lazy branches, timestamp arithmetic, and NOW, under all four warning policies. S2 and S3 results are recorded separately. |
| LATENESS | `SqlToRelCompiler.validateLateness` validates subtraction from a synthetic column. `CalciteToDBSPCompiler.convertMetadata` requires a type implementing `IHasZero`; `InsertLimiters` later builds the subtraction into a waterline expression. Neither check requires the metadata expression itself to be a literal. | Zero, negative, arithmetic, casts, NULL, overflow, column reference, invalid type, lazy branches, and short/month intervals, under all four warning policies. NULL timestamp intervals exposed an internal conversion error; validation now rejects NULL before using an unrelated converter. |
| WATERMARK column annotation | The current grammar rejects this syntax. Runtime waterlines are not a separate SQL constant-required site. | Explicit parser-negative integer and timestamp forms are retained; these are not fold evaluations or runtime watermark tests. |
| Warnings | A refused optimization can expose a warning previously eliminated by folding. With `FELDERA_WARNINGS_ARE_ERRORS`, that becomes a verdict change. Source positions can also change if only part of a failing Janino batch is reduced. | CEIL/FLOOR integer calls, ARRAY_COMPACT, ROUND with NULL, and incorrect FORMAT_DATE argument order, with suppression and promotion. |
| Regex pattern warnings | `frontend/ExpressionCompiler.checkRegexLiteral` checks only non-null string literals. Folding a computed pattern can therefore introduce an unsupported-regex warning; promotion makes this verdict-relevant even with a dynamic searched value. | RLIKE and REGEXP_REPLACE: literal, CONCAT, concatenation, cast, both lazy-branch directions, NULL, dynamic pattern, and supported pattern; all four warning policies. The 80-case S3 supplement matches baseline, traced Java, and Rust, including explicit expected warning/verdict checks. See [`../evidence/fold-regex-sites-s3.json`](../evidence/fold-regex-sites-s3.json). |
| SET values | The configured Calcite `SqlSetOption` grammar accepts a literal, simple identifier, or ON, not an arbitrary expression. `SqlToRelCompiler.compileSetOption` converts that value directly; `ProgramMetadata.set` interprets literals for thresholds and disabled optimizations. | Existing warning-policy SET statements exercise literal options. They do not demonstrate expression folding, and arithmetic SET expressions must not be counted as missing executor coverage. |
| HOP and TUMBLE start | `CalciteToDBSPCompiler.compileHop` checks short-interval types for interval and size, then constructs the operator. It does not demand literal values. TUMBLE's size check above does not apply to its optional start. | No additional hard-literal requirement established by these lowering methods. This classification is not a runtime window-semantics test. |
| ROUND/TRUNC/TRUNCATE precision | `ExpressionCompiler` checks an integer type, casts its width, and emits a runtime call. Its error text says “constant integer”, but the branch does not demand a literal. NULL and integer-input branches can emit warnings separately. | Existing rounding values and warning cases apply; no separate hard-literal site is established by that error text. |
| Keyword operands | Timestamp difference/truncation, EXTRACT, and temporal FLOOR/CEIL units come from grammar-produced interval qualifiers; TRIM produces a symbol flag. DATE_TRUNC also admits expression syntax, but the current validator rejects string-based dialect forms before folding. | A 140-case selector supplement includes accepted keyword controls and parser/type-negative expressions. These are not executor-dependent constant computations. |
| ROW selectors | Calcite validates an existing non-null literal field name or 1-based integer ordinal before the planner executor runs. Literal casts are accepted; arbitrary computed selectors are not. `FelderaSqlValidator` reports the same located invalid-index error for string selectors as Calcite does for integers; `ConvertletTable.convertItem` resolves both validated forms to field accesses before cast names can disappear. | The 140-case supplement exposed and repaired NULL/nonliteral string-selector exceptions and literal-selector assertions. All cases now match baseline/Java/Rust; independent circuit-value assertions verify selected fields at levels 0/2, including quoted mixed-case names. Dynamic MAP/ARRAY selectors remain accepted. |

The first focused run found NULL dereferences for LEAD/LAG and assertions for
NULL window bounds. These are Java oracle defects, not valid SQL rejections or
approved deviations. Explicit checks now produce located diagnostics; the same
guard covers TUMBLE. Regression tests run at optimization levels 0 and 2.

The corpus capture includes the planner executor and the changed results from
Feldera's temporal `RexOptimize`. It does not intercept every literal conversion
in Calcite's validator or RexBuilder, or the DBSP inner simplifier. Consequently,
zero differences in this experiment cannot certify a fully independent Rust
front end. Historical completion records retain conservative coverage and
review flags; the independent §12 adjudication does not rewrite those records
or turn these broader implementation limits into new spike acceptance gates.

The regex supplement keeps both input columns in the output, including when a
NULL pattern makes the regex result constant. This prevents unused-column
warnings from masking the promoted-regex verdict. It added 80 fixtures to the
generator (1,472 at that point); the later ROW/keyword supplement adds 140
more (1,612 total). Neither supplement retrospectively enlarges the earlier
1,296- or 1,392-case certificates. A subsequent full focused replay on the rebuilt
compiler matches all 1,612 cases, with zero compiler bugs; see
[`../evidence/fold-consumer-sites-s3.json`](../evidence/fold-consumer-sites-s3.json).
The source audit and its limits are recorded in
[`../FOLD-SITE-REVIEW.md`](../FOLD-SITE-REVIEW.md).

The later pre/post-executor supplement adds 64 cases (1,676 in the generator):
numeric-to-interval casts at TUMBLE's size guard, adjacent SQL string literals
feeding regex warnings, and dynamic division/modulo with computed constants and
lazy branches. All 64 final comparisons and independently specified
warning/verdict expectations pass. Numeric-interval and literal-chain cases have
no executor trace entries; their owner is Java conversion before the executor.
The explicit local-call audit and its deliberately limited coverage are in
[`../evidence/fold-site-source-inventory.json`](../evidence/fold-site-source-inventory.json).
