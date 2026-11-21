# Independent review of the type-derivation experiment

The two Rust strategies independently execute a bounded type-rule model; they
do not read Java answers or call each other. Their result comparison is useful
but **does not establish C9**. Family-coerced operand proposals are not the
compiler's inserted-cast graph, and the strategies do not yet emit selected
operator identities or evaluate boundary values. This is an independent agent
review, not human maintainer acceptance.

## Independence and measured scope

`translated` composes operand families, implicit conversions and return-type
rules. `rewritten` uses separate unary/binary control flow. They share the scalar
representation, its validation, and the declared operation/arity domain. The
common representation and identical source specification are correlated risks;
agreement between the strategies is not an independent oracle. Neither path
consults probe IDs, SQL text, boundary values, the Java output, or the other's
inference implementation.

I independently recomputed the historical v1 comparison: both strategies match
Java acceptance and projected result types for **42,816 of 42,880** records in
the 40-function slice. All 42,880 Rust outcomes agree with one another. The 64
shared differences are DATE/TIMESTAMP pairs in NULLIF and COALESCE, where a
common-type formula bypassed the configured validator's operand checker. Those
differences are retained; later source repairs do not rewrite the v1 result.
The separate arithmetic/aggregate/CASE/set-operation extension is not evidence
of a second independent implementation of those additional rules.

The compact Java input used for this review has SHA-256
`0b62cd4fdef58153e6d59ae559048be4a119efdb65e3075930d39a59697c91fb`.
It is a projection of the older full Java golden, whose source identity lacks a
complete frozen source/class index. A new source-pinned observer capture is
therefore compared with that golden explicitly, rather than inventing a
retrospective source certificate for the older run.

The declared scalar input slice has sixteen types: BOOLEAN, four signed integer
widths, REAL, DOUBLE, three DECIMAL parameter pairs, four CHAR/VARCHAR widths,
DATE, and default-precision TIMESTAMP. Each unary input has nullable/nonnullable
and literal/column variants; each binary input combines those variants. Origin
and boundary text remain in the protocol but do not affect inference. These
records therefore test more compilation contexts than distinct type-rule inputs.
They do not test raw untyped NULL literals, overload resolution in arbitrary
scopes, intervals, nested types, nondefault collations, arbitrary timestamp
precision, or grouped-aggregate nullability.

## Source fidelity and attribution

The reference is Calcite commit
`4bf922bbbc1cbd570a2f201339a46a2d8f7a763b`, together with the actual Feldera
`TYPE_SYSTEM`, function registrations, and clones. The crate retains Apache-2.0
LICENSE/NOTICE, and translated/formula files identify their modifications.
The source-method table in [the crate README](type-derivation-probe/README.md)
records the bounded source paths. This source review does not replace legal
acceptance of the eventual distribution.

| Rule family | Source obligation checked |
|---|---|
| ABS, SIGN, unary ROUND/TRUNCATE | Preserve numeric family and ARG0 result; omit only explicitly excluded arities/interval inputs |
| CEIL/FLOOR | `DECIMAL_SCALE0` changes scale, keeping precision and nullability |
| SQRT/CBRT/EXP/LN/LOG10, trigonometric functions, degrees/radians; POWER/LOG/ATAN2 | NUMERIC operand acceptance is separate from DOUBLE result inference and later runtime conversions |
| MOD | `implicitCast`/`decimalOf`, `deriveDecimalModType`, and the zero-scale ARG1 fallback; a wider integer operand must not change the divisor result type |
| LENGTH/CHAR_LENGTH/ASCII/OCTET_LENGTH | Character versus binary checker choice and INTEGER result, including permitted character-to-binary conversion |
| UPPER/LOWER/INITCAP, TRIM/REVERSE | Keep fixed CHAR where the source does; apply varying-width conversion only for its corresponding return transform |
| MD5/REPEAT, CONCAT, LEFT/RIGHT | Unspecified VARCHAR versus preserved/summed width, maximum precision, and integer argument family |
| NULLIF/COALESCE | Preserve operand-checker rejection separately from common-type inference; NULLIF returns nullable ARG0, COALESCE uses least-nullable common result |

A concrete identity matters here: observed ABS resolves to Feldera's
`CustomFunctions$AbsFunction#abs`, not the Calcite ABS singleton. Its
`CalciteFunctionClone` inherits the original type rules while changing folding
behavior. A source citation to `SqlStdOperatorTable.ABS` supports those inherited
rules but does not identify the selected operator. Similar display names must
not merge distinct registry objects.

Twelve focused Rust review tests cover decimal precision exhaustion, integral
MOD asymmetry, REAL-to-DECIMAL coercion, NUMERIC operands with DOUBLE results,
character-to-numeric defaults, string-width overflow, ragged CHAR promotion,
NULLIF nullability, invalid parameter bounds, unknown operations, the historical
DATE/TIMESTAMP checker defect, and valid omitted arities. All twelve pass in the final v4 devcontainer source snapshot; the earlier ten
also passed in an isolated review snapshot. These tests check source-derived boundary behavior,
not merely equality between implementations.

The omitted valid signatures initially returned ordinary rejection, including
one-argument COALESCE/LOG and two-argument ROUND. They now remain explicit
prototype limitations. Binary is an inferred coercion target, not a declared
input type in this experiment. This avoids presenting a sample-domain limit as
a SQL compatibility decision.

## What S2 can and cannot establish

Twelve original S2 records were independently inspected without rerunning Java.
Their original probe IDs and programs remain in the retained review input.

| Probe | Observed S2 behavior | Consequence |
|---|---|---|
| 52/53, ABS(VARCHAR(5)) | Character-to-DECIMAL(38,19) cast and `abs_SqlDecimal` application | A final runtime helper and cast can be inspected, but this is not the selected SQL singleton identity |
| 268/269, SQRT(INTEGER) | INTEGER-to-DOUBLE cast and `sqrt_d` application | The Rust NUMERIC family operand remains INTEGER; equal result types do not certify this later cast |
| 18712/20012, NULLIF(INTEGER,VARCHAR(5)) | IF/EQ, character-to-INTEGER comparison cast, nullable return cast | The original Rust operand vector omits comparison and branch conversion structure |
| 28145/29445, MOD(REAL,INTEGER) | Both operands convert to DECIMAL(14,7); internal MOD has DECIMAL(38,10), followed by a result cast | A flat proposed family vector cannot describe the full lowered expression |
| 23513/24813 and 54393/55693, nonnullable-first COALESCE | Only the first operand and its result cast survive | The removed branch's bindings cannot be reconstructed from S2 alone |

S2 can supply a separate oracle for final lowered cast/helper graphs. It cannot
recover erased validated syntax or original overload identity. Literal and
column origins may also have different surviving cast chains. A comparison
must name its boundary and retain those distinctions.

## Read-only binding observation

`SqlToRelCompiler.bindingObserver` exposes two optional per-compiler callbacks:
after successful validation, and immediately after SQL-to-Rel conversion.
The null path retains the previous compiler behavior. The helper
`TypeBindingProbe` only reads the supplied nodes and the validator's already
known types; it does not call `deriveType`, mutate nodes, or substitute results.

The capture retains separate validated-SQL and immediate-Rel graphs, operand
edges, actual types, nullability, numeric precision/scale, fixed/varying SQL
kinds, charset/collation, source positions, and singleton-backed operator
registry identities. The input manifest retains the original types, origins,
boundary text and SQL. Unknown capture forms and unregistered identities remain
explicit errors, never display-name matches. System-view observations remain
visible alongside the user query, rather than silently contributing user-call
coverage.

Configured behavior must be observed rather than inferred from default Calcite
comments: the current compiler retains COALESCE in validated SQL and immediate
Rex, with INTEGER/BIGINT operands and a BIGINT result. It has not yet inserted
the later lowering's CASE or uniform casts. Initial test assumptions expecting
CASE and the standard ABS singleton failed; those attempts are retained and the
tests now assert actual pinned behavior.

Six focused Java tests pass, including exact operator/coercion observation,
character metadata, rejection classification, unchanged schemas/diagnostics,
and strict S2 canonical equality for four independent expressions with the
existing Rust comparator. A 448-record sample spanning all 56 operations also
captures without compiler bugs or capture errors. Full-corpus capture and its
old-golden comparison have their own completion records; these focused checks
alone do not imply a full observer invariance result.

Remaining C9 work is a Rust binding/cast/operator model compared at the declared
Java boundary, with emitted runtime values and errors tested independently.
An all-equal projected result table cannot discharge those obligations.

## Full binding capture and selected call projection

The full source-pinned capture completes **84,224 records: 60,840 acceptances,
23,384 rejections, zero compiler bugs, and zero capture errors**. All verdicts,
schemas, diagnostics, and original input records equal the historical golden.
The strict options comparison retains 84,224 differences: the newer option
schema adds only `ioOptions.readS2=false`, `readS3=false`, `writeS2=""`, and
`writeS3=""`. Existing option values are unchanged. These additions are recorded
as a separate schema extension; the strict all-fields comparison is not marked
equal.

The observation ledger contains 145,064 validated-SQL/converted-Rel pairs,
including the system error view. It records 108,900 validated-SQL and 43,806
converted-Rel CAST nodes. Those counts include literal casts and system syntax;
they are not counts of independently verified inferred conversions.

The strict selected-call extractor retains all **28,960 accepted calls** from
the 40-function slice. Its 13,920 rejected records remain counted; the other
41,344 records belong to the additional-form experiment. It requires exactly
one validated SELECT with a sole `result` field, the exact `AS result` wrapper,
and the original operation's selected call. It retains the full result type,
registered operator identity, ordered operand/cast subtrees, metadata, source
positions, and original manifest. Unknown or ambiguous layouts fail extraction.

Column arguments must resolve to their original `t.a`/`t.b` identities. Literal
arguments retain their original innermost CAST with a source-positioned target;
inserted wrappers retain generated target positions and are marked separately.
TRIM's BOTH and space defaults stay explicit before the original argument.
This is a named logical subtree projection, not a cast or type normalization.

The Java scope is a frozen composite: the complete correlation-tools-v2 source
and class snapshot plus the new SqlToRelCompiler observer and capture/test
classes. At freezing, the only production source difference from that baseline
was the captured observer hook. Later working-tree changes are outside this
claim. Original failed classpath/test-assumption attempts remain preserved.

PLAN §8.2's C9 requires unoptimized S2 comparison under C6a, including actual
inserted casts and sqllib function names/type suffixes, as well as values.
Validated-call equality would be an additional useful boundary result; it would
not replace that required S2 and runtime evidence.


The [binding certificate](evidence/type-bindings.json) and
[verified archive index](evidence/type-bindings-archive.json) preserve the
source/class overlays, raw records, compact call projection, strict comparison
reports, tests, and the original failed attempts. Sixteen Python audit/extractor
tests cover ambiguous query selection, missing or cyclic references, literal
cast provenance, retained TRIM defaults, operator mismatches, exact JSON type
identity, and explicit option-schema extensions.
