# Independent review of the spike 9 template count

The latest review establishes **252 compiler message-body roots** from the Java
and Python captures. The >150 estimate trigger is settled. The Python runtime
contracts and exact platform SDK rendering are accounted for separately; 232
exported Java runtime contracts still need emitting-template attribution. See
the final section and [estimate proposal](MESSAGE-PARITY-ESTIMATE.md) for current
scope. This is an independent agent review, not human maintainer acceptance.

The sections below preserve the earlier findings and the subsequent repairs in
order. Counts and pending work in historical sections describe those snapshots.

## Historical initial review

An independent Codex review agent examined the captured diagnostic assertions
on 2026-11-18. **The available evidence does not establish the number of message
templates the tests require.** The existing reports correctly leave that count
unset. Neither the 472 source-expression candidates nor the 1,088 distinct Java
diagnostics can decide the plan's 150-template trigger.

The new [assertion-attribution certificate](evidence/diagnostic-expectation-attribution.json)
narrows the review to text that assertions actually examine. Its archive retains
the full report, all six input artifacts, and nine source files. All 16 archive
members were checked against their source hashes. This is an independent agent
review, not maintainer acceptance or approval of a final catalog.

## Findings

1. **An emitted diagnostic is not necessarily a test dependency.**
   `CatalogTests.issue3056` expects two initializer diagnostics in one regular
   expression. The same compilation also emits a non-deterministic-UDF error.
   Counting every message in that record would include an incidental error.
   Conversely, `CatalogTests.issue3219` asserts source text and carets without
   asserting the associated message body. Its body cannot be counted merely
   because the same record contains it.

2. **A single assertion can span several rendering fields or diagnostics.**
   For example, `ConnectorTests` checks the error class, message, and snippet
   together. The historical Java capture retained structured diagnostics but
   omitted the exact string passed to `shouldMatch`. The new report leaves
   cross-field matches unresolved and does not evaluate Java regexes with
   Python's different regex semantics. A later capture that retains the actual
   assertion target can support more precise attribution.

3. **The historical Java population omits explicit warning assertions.**
   `BaseSQLTests.shouldWarn` performed a regex assertion but did not call
   `DiagnosticExporter.record` in the reviewed snapshot. Six call sites use
   that helper: one in `MetadataTests`, two in `RecursiveTests`, two in
   `FunctionsTest`, and one in `CatalogTests`. Therefore the 329 captured
   negative-test call sites are not an exhaustive inventory of message
   assertions. The helper needs capture, followed by a new result; the old
   result must remain scoped to its original population.

4. **Compiler acceptance does not classify the required runtime template.**
   Of the 278 registered Python cases, 61 compile successfully. Their
   expectations include numeric overflow, UUID conversion, and temporal
   conversion errors. The compiler-only run does not observe those errors.
   Those cases need runtime evidence and a source mapping before entering the
   catalog count. The three disabled examples remain separate dispositions.

5. **The platform assertion has a composite contract.**
   `NegativeCompilationTests.test_sql_error` checks SDK exception framing,
   compiler error class and body, source lines, and carets in one exact string.
   Counting it as one compiler template would conflate separate requirements;
   omitting it because its body resembles an existing Calcite message would
   discard the rendering requirement. Its execution and attribution remain
   open in the supplied evidence.

6. **Candidate matching does not prove which source emitted a message.**
   Source expressions with dynamic arguments can match unrelated strings.
   Adjacent dynamic expressions are deliberately merged into one hole by the
   clustering tool. A variable-built message may have no literal source
   candidate at all: `RejectUnsupportedPlans` builds ROW-comparison text in a
   switch before reporting it. Constructor-prefix matches also retain
   unresolved overload behavior. These are useful search candidates, not
   approved template identities.

## Measured attribution

The new tool compares literal expectations with each diagnostic's message body,
error class, and snippet separately. It preserves overlaps and all candidate
templates. For Python it applies the test helper's case-insensitive substring
contract and retains the compiler-only classification.

| Population | Disposition | Count |
|---|---|---:|
| Java | Expectation occurs within a message body | 1,088 |
| Java | Expectation occurs only in another individual field | 10 |
| Java | Rendered boundary or another field remains unresolved | 44 |
| Java | Regex across diagnostics requires Java attribution | 1 |
| Python | Expectation occurs within a compiler message body | 212 |
| Python | Expectation matches only the compiler error class | 5 |
| Python | Runtime evaluation pending | 61 |
| Python | Not registered | 3 |
| Platform | Exact composite assertion pending attribution | 1 |

The selected Java and Python bodies have 69 source-expression candidates and
39 Calcite-resource candidates. Twenty-two distinct bodies have neither kind
of candidate. These counts are not additive: candidate families can overlap,
and unresolved assertions can require further templates. They establish neither
an exact count nor a bound sufficient to decide the 150-template trigger.
The 1,088 Java body-matching assertions also happen to equal the earlier number
of distinct emitted Java diagnostics; they count different things.

Thirteen focused Python tests pass in the devcontainer, including three new
tests for incidental messages, snippet-only assertions, regex preservation,
runtime-pending cases, and population drift. The report rejects a changed
Java identity or message sequence and reuses the checked Python outcome reader.
The historical reports and source-template extraction remain unchanged.

## Work needed for the count

Capture the exact assertion target and assertion kind for error and warning
helpers, then account for the full population in a new snapshot. Resolve the
cross-field and regex cases against those strings. Run or explicitly dispose of
the 61 runtime cases and the platform assertion; a compiler-only pass does not
dispose of them. Map the remaining asserted bodies to pinned emitting sources,
including variable-built strings and parser-generated messages. Review the
template identities and ambiguity before counting them and applying the trigger.

These repairs improve the evidence; they do not authorize replacing an unknown
count with zero, the number of expectation strings, or the number of emitted
diagnostics. Spike 9 remains open.

## Source-backed assertions outside the two capture helpers

The follow-up source audit found the concrete contracts below outside
`statementsFailingInCompilation` and `shouldWarn`. Therefore the newer capture's
335 call sites are a population of those two helpers, **not a complete Java
diagnostic-assertion inventory**. The tables identify known missing contracts;
they are not a claim that a text search proves exhaustiveness. Line references
refer to the working source reviewed on 2026-11-18. This audit changed no tests
or compiler behavior and did not execute the additional tests.

### The additional shared substring helper

[`TestUtil.assertMessagesContain`](../../sql-to-dbsp-compiler/SQL-compiler/src/test/java/org/dbsp/sqlCompiler/compiler/TestUtil.java#L58)
checks a case-sensitive substring in `CompilerMessages.toString()`. Its
`DBSPCompiler` overload first calls `runAllCompilerStages()`. Neither overload
records the assertion. Source search found **40 caller sites** in the following
eleven files; a site can execute repeatedly, and several sites can share one
compiler. Capture must preserve each evaluated expectation, actual rendered
target, visible diagnostics, compiler options, SQL submissions, and calling test.
Do not recompile merely to record the assertion or count each call as a distinct
template. Some expectations below test only snippets or diagnostic classes.

| Caller source | Call-site lines | Contracts covered |
|---|---|---|
| [MetadataTests](../../sql-to-dbsp-compiler/SQL-compiler/src/test/java/org/dbsp/sqlCompiler/compiler/sql/MetadataTests.java#L230) | 230, 241, 257, 263, 271, 497, 498, 506, 516, 720, 725, 738, 739, 740, 832, 845, 866, 883, 1637 | Warnings for functions, format strings, properties, unused columns, settings, pass names, reordered fields, identical views, nullable keys, and schema generation; some checks include source context/carets or class-only fragments. |
| [ConnectorTests](../../sql-to-dbsp-compiler/SQL-compiler/src/test/java/org/dbsp/sqlCompiler/compiler/sql/ConnectorTests.java#L44) | 44, 245 | `runConnectorTest` loops over caller-supplied warning fragments; `issue4896` checks unnamed-connector guidance. Inventory the helper's evaluated arguments, not only its one source location. |
| [NegativeParserTests](../../sql-to-dbsp-compiler/SQL-compiler/src/test/java/org/dbsp/sqlCompiler/compiler/sql/NegativeParserTests.java#L161) | 161, 210, 218, 228 | Parser/validator and geometry-literal diagnostics, including rendered source locations. |
| [OtherTests](../../sql-to-dbsp-compiler/SQL-compiler/src/test/java/org/dbsp/sqlCompiler/compiler/sql/OtherTests.java#L156) | 156, 359 | Invalid numeric-string conversion and ignored `ORDER BY` warnings. |
| [WindowTests](../../sql-to-dbsp-compiler/SQL-compiler/src/test/java/org/dbsp/sqlCompiler/compiler/sql/WindowTests.java#L919) | 919, 922, 925, 955 | `issue7677` checks four position-bearing constant-rank warnings for `RANK` and `DENSE_RANK`. |
| [MapTests](../../sql-to-dbsp-compiler/SQL-compiler/src/test/java/org/dbsp/sqlCompiler/compiler/sql/simple/MapTests.java#L49) | 49 | Duplicate MAP-key warning. |
| [OuterJoinTests](../../sql-to-dbsp-compiler/SQL-compiler/src/test/java/org/dbsp/sqlCompiler/compiler/sql/simple/OuterJoinTests.java#L78) | 78, 79 | Two unused-column warnings after join optimization. |
| [Regression2Tests](../../sql-to-dbsp-compiler/SQL-compiler/src/test/java/org/dbsp/sqlCompiler/compiler/sql/simple/Regression2Tests.java#L444) | 444, 470, 509 | Source-bearing warnings in `issue6658`, `issue6658a`, and unused aggregate-input elimination. |
| [Regression3Tests](../../sql-to-dbsp-compiler/SQL-compiler/src/test/java/org/dbsp/sqlCompiler/compiler/sql/simple/Regression3Tests.java#L31) | 31 | A rendered invalid-BOOLEAN-string warning in `issue6400`. |
| [RegressionTests](../../sql-to-dbsp-compiler/SQL-compiler/src/test/java/org/dbsp/sqlCompiler/compiler/sql/simple/RegressionTests.java#L1563) | 1563 | `OVER must be applied to aggregate function`. |
| [StreamingTests](../../sql-to-dbsp-compiler/SQL-compiler/src/test/java/org/dbsp/sqlCompiler/compiler/sql/streaming/StreamingTests.java#L1998) | 1998 | Missing view named in a `LATENESS` statement. |

### Direct user-facing text and rendering assertions

These contracts require capture in addition to the shared helper above. The
comparison operation is part of the contract: exact equality, substring,
prefix, absence, and ordered-list equality are not interchangeable.

| Source and test/helper | Asserted contract | Required capture |
|---|---|---|
| [ConstantRequiredTests](../../sql-to-dbsp-compiler/SQL-compiler/src/test/java/org/dbsp/sqlCompiler/compiler/sql/ConstantRequiredTests.java#L10): `windowErrorsDoNotExposeGeneratedIds`, `rejectsNull`, `nullTimestampLateness` | Exact JSON message `Currently window bounds must be constant values`; class equality plus substrings for NULL lead/lag offsets, frame bounds, tumble intervals, and LATENESS; positive source location; repeated whole-diagnostic equality. | Record structured-field equality/substring checks, optimization level, and repeated-snapshot comparison separately. These are seven test methods and 24 compilations by the current loop bounds, not 24 templates. |
| [CatalogTests.issue2949](../../sql-to-dbsp-compiler/SQL-compiler/src/test/java/org/dbsp/sqlCompiler/compiler/sql/CatalogTests.java#L434) | Raw diagnostic-list rendering contains `Column 'c1' of table 't4' is unused`, even when quiet mode suppresses normal warning rendering. | Preserve the list-rendered target and its visibility policy; normal `CompilerMessages.toString()` is not equivalent. |
| [MetadataTests.issue4183](../../sql-to-dbsp-compiler/SQL-compiler/src/test/java/org/dbsp/sqlCompiler/compiler/sql/MetadataTests.java#L153) | The CLI error file contains `Error parsing SQL`. | Record the file content and class-fragment assertion. It does not establish a particular parser message-body template. |
| [MetadataTests.lazyUpsertDecidesForEveryKeyedTable](../../sql-to-dbsp-compiler/SQL-compiler/src/test/java/org/dbsp/sqlCompiler/compiler/sql/MetadataTests.java#L663) | Every individual diagnostic rendering must omit `Unknown setting`, for five setting variants. | Capture a negative substring assertion and all inspected diagnostics, including quiet warnings. Do not infer a required positive body template. |
| [MetadataTests.disableUnknownOptimization](../../sql-to-dbsp-compiler/SQL-compiler/src/test/java/org/dbsp/sqlCompiler/compiler/sql/MetadataTests.java#L730) | Rendered pass list contains ` RemoveNoops,` and excludes four wrapper names using Java word-boundary regexes. | Record list-content inclusion and regex exclusions in addition to the three shared-helper assertions at 738–740. |
| [MetadataTests.issue5436](../../sql-to-dbsp-compiler/SQL-compiler/src/test/java/org/dbsp/sqlCompiler/compiler/sql/MetadataTests.java#L753) | Contains the per-connector `skip_unused_columns` deprecation text. | Capture the warning substring against the actual rendered stream. |
| [MetadataTests.testUDFTypeError](../../sql-to-dbsp-compiler/SQL-compiler/src/test/java/org/dbsp/sqlCompiler/compiler/sql/MetadataTests.java#L914) | CLI compilation produces one error and contains the rejected UDF argument types and supported signature. | Record the body fragment separately from the error-count assertion and CLI options. |
| [WindowTests](../../sql-to-dbsp-compiler/SQL-compiler/src/test/java/org/dbsp/sqlCompiler/compiler/sql/WindowTests.java#L881): DISTINCT-window rejection | One error and rendered `Window aggregates do not support DISTINCT`, after extra dependent/independent views and indexes. | Capture this separate compilation. A later assertion through the captured helper in the same test does not cover it. |
| [RangeFrameTests.isRejected](../../sql-to-dbsp-compiler/SQL-compiler/src/test/java/org/dbsp/sqlCompiler/compiler/sql/simple/RangeFrameTests.java#L261) | Every failed generated program must contain the nullable-RANGE-frame explanation; successful compilations return `false`. | Capture each rejected program and substring, and retain accepted/rejected verdicts separately. One helper location covers a generated matrix. |
| [NegativeParserTests.errorTest](../../sql-to-dbsp-compiler/SQL-compiler/src/test/java/org/dbsp/sqlCompiler/compiler/sql/NegativeParserTests.java#L194), [EOF check](../../sql-to-dbsp-compiler/SQL-compiler/src/test/java/org/dbsp/sqlCompiler/compiler/sql/NegativeParserTests.java#L253), [warningTest](../../sql-to-dbsp-compiler/SQL-compiler/src/test/java/org/dbsp/sqlCompiler/compiler/sql/NegativeParserTests.java#L257) | Message prefixes `Encountered "not SQL".` and `Encountered "<EOF>"`; unused-table body substring; rendered source includes `CREATE TABLE T`; severity/count checks. | Capture body prefix, body substring, source-text substring, and verdict/severity contracts as separate assertions. |
| [Regression2Tests](../../sql-to-dbsp-compiler/SQL-compiler/src/test/java/org/dbsp/sqlCompiler/compiler/sql/simple/Regression2Tests.java#L196), [testSet](../../sql-to-dbsp-compiler/SQL-compiler/src/test/java/org/dbsp/sqlCompiler/compiler/sql/simple/Regression2Tests.java#L305) | Two unused-column fragments in the rendered stream; an individual warning contains the unknown-setting explanation. | Preserve whole-stream versus individual-message targets and the original warning visibility. |
| [FloatingPointEqualityTests](../../sql-to-dbsp-compiler/SQL-compiler/src/test/java/org/dbsp/sqlCompiler/compiler/sql/simple/FloatingPointEqualityTests.java#L20): `compileAndGetFPWarnings`, `assertWarnings`, `equalsRendered` | Filters by warning class; checks ordered lists of position-bearing warning lines and an exact full rendered warning, including source context and documentation link. Empty expected lists assert absence. | Capture the filter, ordered expected/actual strings, and the removal of `(no input file):`; full-rendering equality needs its own contract. Do not reduce this to a substring helper. |
| [RegexWarningTests](../../sql-to-dbsp-compiler/SQL-compiler/src/test/java/org/dbsp/sqlCompiler/compiler/sql/simple/RegexWarningTests.java#L117): `warnings`, `warningRendered`, `everyRegexFunction`, `noWarnings` | Exact rendered regex warning and ordered warning-line list; empty list for accepted/dynamic/NULL patterns. `rejectedPatterns` also checks the checker-produced explanation fragment directly at 106. | Capture both compiler-rendered assertions and checker-fragment expectations; connect them to the same source templates without double-counting them. Accepted-pattern `assertNull` checks are verdict-only. |
| [IncrementalUnboundedStateTests](../../sql-to-dbsp-compiler/SQL-compiler/src/test/java/org/dbsp/sqlCompiler/compiler/sql/streaming/IncrementalUnboundedStateTests.java#L41): `assertUnboundedStateWarnings`, Gen-2 variant, `fallbackWarningsDoNotContainAllocationIds` | Exact concatenation of class-filtered warning renderings, including snippets and suppression hint; empty string asserts absence. Fallback test checks `; source_map` and repeated full JSON equality. | Capture engine mode, class filter, exact expected text, absence checks, and snapshot equality. Direct message rendering can include warnings hidden from the compiler's normal quiet stream. |
| [DedupKeysTests.latenessOnPrimaryKeyTableRejectedUnderGen2](../../sql-to-dbsp-compiler/SQL-compiler/src/test/java/org/dbsp/sqlCompiler/compiler/visitors/outer/DedupKeysTests.java#L237) | SQL compilation exception must contain `not yet supported by the Gen-2 engine` and omit `--gen2`. | Capture exception text and both positive/negative predicates. Despite its exception-based test style, this is a user-facing SQL limitation. |
| [ConfigSizeTests](../../sql-to-dbsp-compiler/SQL-compiler/src/test/java/org/dbsp/sqlCompiler/compiler/frontend/connectors/config/ConfigSizeTests.java#L25), [ConfigDurationTests](../../sql-to-dbsp-compiler/SQL-compiler/src/test/java/org/dbsp/sqlCompiler/compiler/frontend/connectors/config/ConfigDurationTests.java#L25) | Parameterized `ParseException` substring checks for malformed quantities; size test also checks every listed unit and `trailing "B"`. | Capture evaluated parser input, expected fragment, and exception text. These are user configuration fragments: [ConfigQuantity.reportMalformed](../../sql-to-dbsp-compiler/SQL-compiler/src/main/java/org/dbsp/sqlCompiler/compiler/frontend/connectors/config/ConfigQuantity.java#L251) embeds them in compiler `Invalid configuration` warnings. Record the wrapper/body relationship rather than counting them as internal test failures. |

### Class, verdict, and internal-test distinctions

The following source contracts should not be mistaken for additional user-message
body templates. They still need an explicit disposition in the assertion census.

| Source | Classification and treatment |
|---|---|
| [MetadataTests.disableOptimization](../../sql-to-dbsp-compiler/SQL-compiler/src/test/java/org/dbsp/sqlCompiler/compiler/sql/MetadataTests.java#L694), [RemoveUselessLeftJoinsTests.passRemovedAJoin](../../sql-to-dbsp-compiler/SQL-compiler/src/test/java/org/dbsp/sqlCompiler/compiler/visitors/outer/RemoveUselessLeftJoinsTests.java#L40) | Presence/absence of named diagnostic classes identifies whether a pass ran. Preserve class and warning-bit predicates; these checks do not require a particular explanatory body. |
| [AsofTests.issue3435](../../sql-to-dbsp-compiler/SQL-compiler/src/test/java/org/dbsp/sqlCompiler/compiler/sql/quidem/AsofTests.java#L196), [OtherTests.testUnionWarning](../../sql-to-dbsp-compiler/SQL-compiler/src/test/java/org/dbsp/sqlCompiler/compiler/sql/OtherTests.java#L314), [CircuitReplayTests](../../sql-to-dbsp-compiler/SQL-compiler/src/test/java/org/dbsp/sqlCompiler/compiler/sql/CircuitReplayTests.java#L230) | Exact `Unused column` class or forbidden `Fields reordered` class. Replay tests may separately require whole-diagnostic equality. Record those contracts separately from message-body templates. |
| [TypeCoercionTests.explicitBooleanNumericCastsKeepExistingRejection](../../sql-to-dbsp-compiler/SQL-compiler/src/test/java/org/dbsp/sqlCompiler/compiler/sql/TypeCoercionTests.java#L45), [TypeProbeTests](../../sql-to-dbsp-compiler/SQL-compiler/src/test/java/org/dbsp/sqlCompiler/compiler/sql/TypeProbeTests.java#L43) | Rejection and absence of compiler-bug classes. Do not infer a required user-message template from text used only to detect an internal failure. |
| [ConnectorTests.runCleanConnectorTest](../../sql-to-dbsp-compiler/SQL-compiler/src/test/java/org/dbsp/sqlCompiler/compiler/sql/ConnectorTests.java#L57), [RecursiveTests](../../sql-to-dbsp-compiler/SQL-compiler/src/test/java/org/dbsp/sqlCompiler/compiler/sql/recursive/RecursiveTests.java#L621), [StringCastTrimTests](../../sql-to-dbsp-compiler/SQL-compiler/src/test/java/org/dbsp/sqlCompiler/compiler/sql/simple/StringCastTrimTests.java#L115), [RustSourcePositionTests](../../sql-to-dbsp-compiler/SQL-compiler/src/test/java/org/dbsp/sqlCompiler/compiler/sql/RustSourcePositionTests.java#L22) | Warning counts, warning absence, or successful exit. The first `Assert` argument supplies failure context; printing `messages.toString()` there is not a text assertion. |
| [Gen2JsonTests.removeTypedBoxRejectsIndexedInputWaterline](../../sql-to-dbsp-compiler/SQL-compiler/src/test/java/org/dbsp/sqlCompiler/compiler/sql/Gen2JsonTests.java#L324), [StreamKindTests.expectRejected](../../sql-to-dbsp-compiler/SQL-compiler/src/test/java/org/dbsp/sqlCompiler/compiler/visitors/outer/StreamKindTests.java#L45) | Deliberately call a pass or construct an invalid operator configuration and inspect `InternalCompilerError` text. Retain as compiler-invariant test contracts, outside the user SQL diagnostic catalog unless a production path is shown to expose them. |
| [CircuitReplayTests](../../sql-to-dbsp-compiler/SQL-compiler/src/test/java/org/dbsp/sqlCompiler/compiler/sql/CircuitReplayTests.java#L57), [S1FunctionObjectsTests](../../sql-to-dbsp-compiler/SQL-compiler/src/test/java/org/dbsp/sqlCompiler/compiler/sql/tools/S1FunctionObjectsTests.java#L63), [S1LiteralPayloadTests](../../sql-to-dbsp-compiler/SQL-compiler/src/test/java/org/dbsp/sqlCompiler/compiler/sql/tools/S1LiteralPayloadTests.java#L36), [S1ExpressionTableTests](../../sql-to-dbsp-compiler/SQL-compiler/src/test/java/org/dbsp/sqlCompiler/compiler/sql/tools/S1ExpressionTableTests.java#L231) | Malformed replay or transport input checks (`circuit replay`, `Cyclic`, `expanded`). Record under decoder/API error contracts rather than treating them as negative SQL queries. |
| [DocumentationTests](../../sql-to-dbsp-compiler/SQL-compiler/src/test/java/org/dbsp/sqlCompiler/compiler/DocumentationTests.java#L65), [MetadataTests.issue4626](../../sql-to-dbsp-compiler/SQL-compiler/src/test/java/org/dbsp/sqlCompiler/compiler/sql/MetadataTests.java#L166) | Missing documentation anchor/page errors and CLI version-banner text. These are documentation/build or CLI-output contracts, not compiler SQL message templates. |

A complete follow-up capture must account for these concrete contracts and search
for further direct assertions. It must retain negative predicates and exact
list/snapshot comparisons, not merely route every assertion through a positive
substring helper. The table also prevents inflating the catalog with assertion
failure context, deliberately malformed IR, or version-banner text.

## Source resolution and wrapper extraction against capture v7

The subsequent review uses the immutable v7 population: 1,333 Java assertions
at 375 sites, including 184 assertions from `TestUtil.assertMessagesContain`.
That capture closes the shared-helper omission identified above. Direct
assertions and specialized warning-list helpers remain separate work; 375 is
not an exhaustive assertion count. Earlier measurements in this document retain
their original populations.

### Rendering boundaries

[`diagnostic_cross_fields.py`](scripts/diagnostic_cross_fields.py) resolves the
72 v7 assertions previously classified as crossing rendering boundaries. Of
these, 71 assert text in 76 message-body occurrences. The remaining assertion,
`MapTests.testDuplicateKeys`, requires `warning: Duplicate MAP key`, which checks
the warning label and class rather than its explanatory body.

The resolver uses the exact captured rendering and visible diagnostic order.
It assigns a repeated body only when its occurrence count equals the number of
visible diagnostics with that body; extra occurrences in snippets or headers
leave the result ambiguous. The two Java regexes have explicit reviewed
dispositions: `CatalogTests.issue3056` requires its two initializer fragments,
and `FunctionsTest.timePrecision` requires the class and body with literal
parentheses. Their incidental UDF and constant-view messages are excluded.
No Python regex interpretation substitutes for Java behavior.

Five focused tests cover partial body matches, incidental diagnostics,
snippet/context matches, repeated text, and the two-stage regex review contract.
Resolving a body field identifies what text is asserted; it does not identify
the template that emitted it.

### Twenty-two previously unmapped bodies

[`diagnostic-template-dispositions.json`](diagnostic-template-dispositions.json)
records eight source-backed outer-template identities for the 22 unmapped bodies
from v5, all still present in v7. Each body retains its exact text, digest,
observed arguments, source digests, and source locations. Reconstructing each
body from its proposed format and arguments produces exact equality.

| Proposed identity | Distinct recorded bodies | Source and retained dependencies |
|---|---:|---|
| ROW equality | 1 | `RejectUnsupportedPlans.checkComparison`, `EQUALS` branch; quoted operator and ROW documentation citation |
| ROW inequality | 1 | The `NOT_EQUALS` branch; different recommended replacement text |
| ROW `NULLIF` | 1 | The `NULLIF` branch; explicit CASE-expression recommendation |
| ROW ordering/minimum/maximum | 8 | The `isRow` ordering branch; operator name remains a parameter |
| Collection ordering with nested ROW | 4 | The `hasRow && !isRow` branch; collection-specific wording remains distinct |
| Floating-point equality | 1 | `WarnFloatingPointEquality.message`; `describeValues`, `describeColumns`, suffixes, and citation remain formatter dependencies |
| Indexing diagnostic rewrite | 3 | `DBSPCompiler.improveErrorMessage`; `ITEM_ERROR` extracts source/index types and retains the `CanNotApplyOp2Type` resource's supported-signature tail |
| Parser keyword diagnostic | 3 | Generated `DbspParserImpl.normalizeException`, generated `ParseException`, and `CompilerMessages.messageWithoutPosition`; expected-token text remains intact |

These eight identities are a reviewed subset, not eight templates for the whole
catalog. Nested formatters still need catalog treatment; classifying an outer
template does not authorize hiding its dynamic text behind an unrestricted hole.
The generated parser sources are retained as evidence, not added to production
source. This review records agent findings, not maintainer acceptance.

### Connector wrappers

The source extractor now recognizes the verified forwarding signatures of
`ConfigReporter.warn`, `ConfigReporter.warnPath`, and
`ConnectorValidator.warnAt`. The first two require an explicitly declared
`ConfigReporter` receiver parameter; the third requires its owning class.
Unrelated methods with the same names and shadowed receiver scopes are excluded.
Aliased or inferred receiver types remain outside this syntactic extraction.
Each added site records the wrapper contract, category, and message expression.

Four Java tests pass in an isolated class overlay against v7; the Maven build
tree and v7 snapshot were not changed. Re-extracting v7's 865 source files finds
705 emission sites and 551 literal-expression candidates. The added 88 wrapper
sites give candidate matches to 81 previously unmapped asserted bodies, reducing
that population from 133 to 52. Eighteen focused Python tests pass, including the
cross-field resolver and existing census, clustering, and attribution checks.

The 52 bodies consist of the 22 source-mapped bodies above, 29 configuration
formatter chains, and one geometry-conversion exception chain. The configuration
chains include variable-built unknown-field messages, enum-value descriptions,
Jackson conversion text, and nested quantity-parser errors. Those dependencies
remain explicit. The resulting 141 source candidates and 42 Calcite candidates
are not additive and are not an approved required-template count.

The [resolution certificate](evidence/diagnostic-template-resolution.json)
records the inputs, validation, remaining work, and verified archive. That
archive retains the extraction overlay, frozen source, generated parser source,
reports, and review tools. Replaying its Java extractor also requires the frozen
classpaths retained in the linked v7 capture archive.


## Shared-helper repair and full-suite capture

The subsequent [v7 capture](evidence/diagnostic-shared-helper.json) records all
three shared helpers, including the 40 `TestUtil.assertMessagesContain` caller
sites identified above. It preserves all 1,149 earlier assertions and adds 184
evaluated substring assertions, for 1,333 records at 375 sites. The current JUnit
test identity is retained without recompilation. The full Java run reports
3,021 tests with zero failures/errors and 71 skips; 129 Python harness tests pass.
The [verified archive](evidence/diagnostic-shared-helper-archive.json) retains
source, compiled classes, reports, and the initial environment failure.

This repairs the additional shared-helper omission. The direct field, list,
prefix, absence, checker-fragment, and renderer contracts in the remaining
tables still need accounting. The initial v7 attribution left 72 cross-field
assertions unresolved; the rendering-boundary review above now assigns 71 to
body text and one to the warning label and class. Their complete template
identities and the required-template count remain unsettled. The earlier
numerical findings describe their frozen historical populations.

## Follow-up formatter and ambiguous-source review

The [formatter dispositions](diagnostic-formatter-dispositions.json) account for
the remaining 30 bodies from the wrapper-extraction report. Together with the
earlier 22-body review, all 52 have proposed source identities. This classification
does not replace the frozen report or establish the complete required catalog.

| Formatter chain | Distinct bodies | Evidence |
|---|---:|---|
| Unknown configuration field | 10 | `ConnectorValidator.validateConfig`, `UnrecognizedPropertyException` branch |
| Invalid enum value | 6 | The enum branch, ordered `@JsonProperty` declarations, and the captured connector SQL |
| Primitive and boxed Boolean conversion | 2 | Separate Jackson `StdDeserializer` branches, its error envelope, and the declared configuration field types |
| Unknown quantity unit | 7 | `MeasurementUnits.unknown`, `ConfigReporter.partOf`, duration/size unit lists, and `reportMalformed` |
| Missing duration unit | 2 | `ConfigDuration.parse`, `partOf`, and `reportMalformed` |
| Negative or empty duration | 2 | The two distinct `ConfigDuration.parse` branches and `reportMalformed` |
| Geometry conversion | 1 | `RexLiteral.getValueAs`, `ExpressionCompiler`, `CalciteRexNode.toString`, and `UnimplementedException` |

Thirty bodies reconstruct exactly from nine proposed formatter identities.
Every example retains its capture record, test name, assertion, SQL digest,
arguments, and body digest. The six enum lists match their frozen declarations.
The invalid `mode = batch` example selects Iceberg, whose three variants differ
from Delta's four; shared wording alone would have selected the wrong enum.

The geometry cause and Boolean conversion details come from pinned dependency
bytecode, retained with class digests and disassembly. This is explicit dependency
evidence, not a claim to have reviewed unavailable upstream source. Primitive
Boolean errors list mixed-case spellings; boxed Boolean errors use a different
suffix. The geometry node display remains a recorded SQL-rendering argument
with its actual formatter dependency identified.

All 12 bodies with ambiguous source candidates also have dispositions. Literal
constants, exception classes, connector selections, and branch-specific suffixes
exclude false candidates. The CSV delimiter body selects two separate emitting
sites because parser and encoder tests both assert it. For the S2 stream body,
the captured SQL selects the input connector; the identically worded output site
is retained as an excluded site. These are 13 selected sites across 12 observed
bodies, not 13 additional required templates.

Six focused regression tests validate exact reconstruction, altered evidence,
Boolean distinctions, quantity nesting, enum selection, and complete candidate
accounting. The [follow-up certificate](evidence/diagnostic-formatter-review.json)
records the archive and validation. Earlier certificates remain unchanged.

The remaining catalog work is to combine reviewed identities with all other
asserted bodies, resolve nested dependencies consistently, account for the
direct and specialized assertions listed above and Python/runtime/platform
contracts, and obtain maintainer acceptance of a deduplicated required-template
count. Adding the candidate counts or the proposed-identity counts would not
perform that review.

## Consolidated catalog for the v9 Java population

The covered Java population requires **240 diagnostic-body roots** under the
catalog's counting rule. Every root has a captured assertion witness. This is
a definitive count for these captured contracts, not the whole-suite total or
a claim that the eventual Rust implementation must use 240 separate functions.

| Covered population or dependency | Count | Treatment |
|---|---:|---|
| Shared-helper assertions | 1,333 | 228 body roots; 1,211 distinct selected bodies |
| Direct assertions | 382 | 12 additional body roots after deduplication |
| Combined body roots | **240** | 186 source expressions, 32 Calcite resources, 22 reviewed formatter roots |
| Direct helper-fragment templates | 26 | Ten quantity-parser and 16 regex-checker templates; reported separately |
| Direct assertions of those fragments | 116 | Exact reconstruction, with pinned formatter sources |
| Shared constructor, wrapper, formatter, and parameter-expression nodes | 174 | Dependencies, not additional body roots |

The counting rule admits a source literal/parameter sequence, a named Calcite
resource, or a reviewed formatter branch as a body root. Repeated emissions,
repeated tests, and repeated values count once. A resource used both directly
and through a rewrite retains one identity. Forwarding wrappers and constructor
continuations remain dependencies. Component fragments are not added to the
body-root total. This measures the reviewed Java message formats; alternative
ways of factoring a future Rust catalog are implementation choices.

The shared-helper review excludes 23 assertions that require no body text.
Three `TYPEOF` assertions also match SQL snippets, so they cannot independently
justify a body root; other assertions establish that root. Four warning regexes
have explicit reviews of their necessary literal fragments. The tool does not
substitute Python regex behavior for Java behavior.

All 382 direct assertions have dispositions: 176 require body text, 116 require
helper fragments, 36 check class/source text, 19 check snapshot stability, and
35 check absence. The rendered assertion of `Error parsing SQL` checks a class
name and adds no body root. Empty warning lists, forbidden allocation-ID regexes,
and equality between repeated compilations do not create positive body-template
requirements. Exact ordered warning lists retain their order and first-line
projection. JSON-string assertions are attributed to decoded message fields.

The review manifests are
[`diagnostic-catalog-review.json`](diagnostic-catalog-review.json),
[`diagnostic-catalog-review-v9.json`](diagnostic-catalog-review-v9.json), and
[`diagnostic-direct-catalog-review.json`](diagnostic-direct-catalog-review.json).
The [catalog certificate](evidence/diagnostic-reviewed-catalog.json) links the
verified archive containing complete root definitions, assertion witnesses,
dependency edges, source snapshots, and the reconstruction tools. Twenty-four
focused tests pass. They cover source/capture drift, witness attribution,
shared-resource deduplication, fragment reconstruction, non-body alternatives,
and rejection of positive dependencies inferred from snapshots.

The v7-to-v9 comparison preserves one body drift: the calendar `RANGE` rejection
in `RegressionTests.issue2027negative` included relational allocation IDs from
an `UnsupportedException` continuation. Its required substring and root identity
are unchanged. Thirty-three source emission records moved by line/offset; their
expressions and remaining metadata are identical. Neither difference is hidden
by declaring the captures byte-identical. Later source-position repairs require
their own validation and do not rewrite the frozen v9 evidence.

The covered count already exceeds the plan's 150-template threshold. Under this
counting rule, adding the remaining populations cannot lower that count, so the
phase-estimate revision is required now. The uncounted populations are the
Python compiler assertions, Python runtime assertions, and platform SDK contract;
this capture also does not prove a source-wide absence of other assertion forms.
The independent agent review supplies the catalog evidence. Human maintainer
acceptance and the revised phase estimate remain separate decisions.

## Compiler/Python/platform extension and remaining runtime inventory

The [scope extension certificate](evidence/diagnostic-scope-extension.json)
raises the compiler body-root count from 240 to **252**. All 281 Python
expectation assignments have dispositions: 212 compiler-body contracts, five
class-only contracts, 61 runtime contracts accounted for by the separate runtime
report, and three unregistered assignments. The normal Python negative-test
runner remains dormant; these results come from the documented probes.

The 12 additional compiler roots are one source expression in
`LeadLagAggregates.java:58` and 11 Calcite resource formats. The source review
handles two ambiguities explicitly. `TableNameNotFound` and `TableNotFound`
have identical complete formats and contribute one root, with both keys retained
as aliases. The `illarg_dot_illegal` SELECT expectation maps to
`ColumnNotFoundInTable`; the superficially matching source in
`ModifyTableTranslation.java:78` handles INSERT/REMOVE target-column lookup and
cannot emit this fixture's SELECT error. The archived review binds every input,
result, asserted body, selected root, and source hash.

The platform compiler probe and actual SDK method replay satisfy the exact
`NegativeCompilationTests.test_sql_error` string after `.strip()`. The compiler
input uses the SDK constructor's own SQL normalization. The replay executes the
unchanged constructor and `_wait_for_compilation` method with local responses;
it verifies the STATUS/ALL request sequence and the exception's class, body,
source lines, and carets. Its body reuses `calcite:ObjectNotFound`, so the count
stays 252. HTTP transport and pipeline provisioning were not executed.

Five catalog tests cover provenance drift, case-insensitive body matching,
non-body alternatives, missing evidence, and separate runtime populations.
Four SDK replay tests cover actual response sequencing, changed body/class/carets,
SQL normalization, and missing source methods. All nine pass with Python 3.14.
The SDK source uses syntax unavailable in the host's default Python 3.9; the
replay uses the installed Python 3.14 interpreter.

### Bounded follow-up assertion audit

The follow-up search examines Java assertions containing `getMessage`,
`messages.toString`, `.message`, `error_type`, `errorType`, `getMessages`, or
`sql_error`. The archive retains the search result. Existing captured direct
assertions are already in the v9 catalog. The remaining matches have these
source-backed dispositions; this search is not proof of all possible assertion
styles.

| Source/test anchor | Contract | Body-root consequence |
|---|---|---|
| `MetadataTests.disableOptimization`; `OtherTests.testUnionWarning`; `CircuitReplayTests.java:230`; `S1DetachedLoweringTests.java:69` | Class presence/absence | No positive body root |
| `TypeCoercionTests.java:51`; `DiagnosticTemplatesTests.java:73`; `CorpusExporterTests.java:197` | Internal-error class absence or exporter/extractor fixture class | No positive SQL body root |
| `CircuitReplayTests.java:458`; `ProgramMetadataReplayTests.java:201`; `S1DetachedLoweringTests.java:28,63,68`; `S1DiagnosticTimelineTests.java:35,42` | Snapshot/replay equality | Stability obligation, no independent positive body identity |
| `CircuitReplayTests.java:57`; `S1FunctionObjectsTests.java:63`; `S1ExpressionTableTests.java:231`; `S1LiteralPayloadTests.java:36` | Internal decoder/reference rejection | Outside user SQL diagnostic-body scope |
| `Gen2JsonTests.java:325`; `StreamKindTests.java:50` | Compiler invariants | Internal failure contract, separately scoped |
| `DocumentationTests.java:65,68` | Missing documentation page/anchor | Documentation validation, outside SQL message scope |
| Assertions that pass `messages.toString()` as JUnit's failure-context argument | Count/verdict check | Context is not asserted text |

These line anchors describe the archived source-search snapshot; v9 capture
source hashes remain separate. Changes after v9, including the calendar RANGE
location repair, are not retroactively attributed to that capture.

The retained v9 Surefire reports contain 71 skips: 47 explicitly say corpus-only
export does not execute Rust, 23 are ignored tests, and one is the configured
TypeProbe generator. These explain the difference from the 23 JUnit ignored
count. They are not the population of Java runtime message contracts.

### Java runtime contracts: precise remaining population

The v9 producer `c6ede756d-working-tree-direct-assertions-v9` exports 8,573 cases.
Its selected runtime subset contains **232 contracts in 71 tests**, with 95
unique expected substrings. All selected cases have `runtime_verified: false`.
The new source inventory validates all selected case hashes and producer IDs,
locates every test method in the frozen Java source (including inherited tests),
and retains each exact `should_panic(expected)` substring. The selection total
comes from the original full scan; the inventory replay checks the selected
232 files and does not pretend to rescan omitted cases.

The predicate is explicit in `TestCase.java:260`, the export in
`CorpusExporter.java:151`, and registration in `SqlIoTest.qf` and
`BaseSQLTests.runtimeFail/runtimeConstantFail`. The selected artifact retains SQL,
typed circuit, compilation options, commands, and input order, so further source
analysis can choose actual runtime helpers without requiring a new runtime run.
No template identity is inferred solely from registration or compilation.

This subset includes casts, UUID conversion, arithmetic and interval overflow,
array bounds, logarithm domains, regex/JSON parsing, decompression, scalar
subqueries, and negative table weights. Broad fragments (`overflow`, `out of
range`, `Cannot convert`) can refer to several formatter roots. The 95 strings
therefore are not a 95-template count. All 232 contract-to-root assignments
remain explicitly unresolved in this inventory; the already attributed Python
runtime roots may be reused only with source-path evidence for each Java case.
This is accounting work, not a newly imposed requirement to execute all 232.

The compiler count already decides the plan's >150 trigger. The proposed
[message-parity estimate](MESSAGE-PARITY-ESTIMATE.md) preserves the accepted
baseline, prices the known compiler scope under stated assumptions, and leaves
the unresolved remainder unpriced. Neither an exhaustive whole-suite template
count nor human acceptance is claimed.
