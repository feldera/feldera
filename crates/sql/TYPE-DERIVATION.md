# P0 type-derivation comparison

The two Rust scalar strategies agree with the Java oracle on acceptance and
result type for all **42,880 probes across 40 functions**. The additional Rust
rules agree on all **41,344 arithmetic, comparison, CASE, set-operation and
aggregate probes**. Together they reproduce 60,840 acceptances and 23,384 SQL
rejections, with no unexplained difference in those two projections.

This is a completed component measurement, not completion of spike 5 or C9.
The [certificate](evidence/type-derivation.json) records the exact scope.

The subsequent [validated-binding comparison](evidence/type-binding-predictions.json)
also matches all 42,880 function records: 28,960 accepted calls and 13,920
rejections. It checks selected operator registry identities, complete result and
operand types, added cast chains and operand positions, and TRIM defaults. Both
inference strategies feed a shared binding encoder; this does not establish two
independent lowering implementations.

## Implementations and corrections

The [disposable crate](type-derivation-probe/README.md) separates the type
representation, translated Calcite family/return rules, direct Rust rules, and
file protocol. Both strategies consume declared types; neither reads SQL text,
Java answers, or the other implementation. Original literal/column origin and
boundary values remain in the input for later binding and value experiments.

The first scalar comparison found 64 acceptance differences. DATE and TIMESTAMP
can acquire a common type in CASE, but NULLIF and COALESCE reject their different
operand families before that return inference. The original checker order now
has an explicit rule and an independent regression.

The first additional-form comparison found 1,408 acceptance and 2,408 type
differences. Repairs preserve three source-backed behaviors: the directional
numeric/datetime common-type rule, INTERSECT/EXCEPT nullability refinements, and
ARRAY_AGG's outer nullability. In particular, matching DECIMAL→DATE does not
authorize inventing a symmetric DATE→DECIMAL conversion.

The final devcontainer run passes 23 Rust tests, including 12 independent review
tests and four protocol tests; all-target Clippy with warnings denied and
formatting pass. Seven comparator tests reject missing populations, changed type
attributes, unknown metadata, duplicate JSON keys and compiler-bug goldens. CI
now runs both inference strategies against its generated Java oracle and retains
all mismatches. The workflow was linted locally; remote CI was not dispatched.

## Evidence and limits

The [verified archive](evidence/type-derivation-archive.json) retains 106 files:
all four inference attempts, frozen Rust source and executable, test logs, the
full original probe/Java streams, the comparator, pinned Calcite reference
sources, and current Java reference sources. Every archive member's hash and
size was checked. The archive is 56,788,971 bytes, SHA-256
`52363956e14772b06d443bbd3382e33567d781de027ec4f1634fef3087a8fc70`.

The old Java golden has a completion manifest and immutable artifact hash, but
its original exact classpath/source snapshot was not recorded. Current Java
source in this archive is labeled separately and does not replace that missing
provenance. The subsequent [source-bound Java capture](evidence/type-bindings.json)
completed all 84,224 probes without capture errors or compiler failures. Its
verdicts, schemas, diagnostics and exact inputs match the historical golden.
Four new disabled/default seam options remain explicit differences in every
record. No existing option changed. Rust's full acceptance/result comparison
also passes against this newly frozen Java capture.

Result-type equality does not prove the actual cast tree or selected function.
For example, NUMERIC-family inference leaves SQRT's INTEGER operand intact while
lowering inserts a DOUBLE cast. MOD introduces additional decimal casts; NULLIF
introduces a comparison and nullable result. Feldera's ABS uses a custom operator
clone that retains Calcite's type rules while changing folding policy. The
initial prototype's operand arrays were proposed family conversions. The new
validated-binding comparison exposed 3,232 COALESCE operand differences:
successful common-result inference leaves numeric operands intact, and promotion
to unbounded VARCHAR preserves an existing CHAR operand. Both strategies now
match that validator behavior. The later S2 conversions remain separate.

The final binding run passes 28 Rust tests, 28 Python tests, Clippy and formatting.
Independent review also prompted strict checks for boolean IDs/schema versions,
false runtime claims and integer/boolean metadata substitutions. Its
[77-file archive](evidence/type-binding-predictions-archive.json) retains both
prediction runs, the executable, source, scripts and logs, and references the
separately verified Java source/class archive by hash. The archive is 4,512,850
bytes, SHA-256
`57d4c473cfd08a8a261015ab80e8b13e0800bad7fde36d1e1e6b590069b36951`.
Original argument subtrees remain in the Java capture; this comparison does not
claim to reimplement literal parsing or source positions.

Full C9 coverage remains open. D1 stopped with A as the planning assumption.

## Accepted implementation modes for binder A

| Component | Accepted mode | Basis and limit |
|---|---|---|
| Validator | Write a Feldera validator around an explicit scope model and operator registry. | Both prototypes agree on the tested operand families and validated bindings. Their inputs already provide argument types. Name resolution, scopes, overload ranking and diagnostics need their own oracle cases. |
| Implicit coercion and return inference | Translate Calcite rule semantics where they determine user-visible types. Keep the Rust representation and control flow idiomatic. | The independently written model can check the translated one. The original mismatches in NULLIF, COALESCE, datetime and set operations show where directional and ordered rules matter. |
| SQL-to-FRel conversion | Write a typed lowering layer against the Feldera relational model. Translate individual rules where their ordering or results are part of the observed contract. | Full detached replay gives a boundary oracle. The current binding and S2 expression checks do not cover full Rust statement-to-FRel lowering or the transient forms. |

The plan owner accepted these implementation modes on 2026-11-18. The
measured scope remains the bounded type, binding and S2 matrices.

## S2 expressions

A second Java capture prints the unoptimized scalar expression and its type
from the map immediately before view `v`. It retains the raw inner node
alongside the exact compact printer text. The full 40-function input yields
28,960 captured projections, 13,920 rejections, and zero compiler failures.
Both Rust inference paths feed a shared typed lowerer. All **28,960 accepted
function probes** match both Java text fields exactly.
This includes 7,648 column-only and 21,312 literal-bearing calls.

The capture checks late conversions that validated bindings miss. SQRT adds a
DOUBLE cast after binding, mixed integral and decimal MOD can widen the shared
operand precision and use DECIMAL(38,10) before the result cast, and NULLIF
casts its comparison separately from its returned operand. COALESCE removes
the second branch when its first argument is nonnullable.

The additional rules match the S2 printer on 20,960 accepted arithmetic,
comparison and CASE probes, and match all ordered map and filter expressions
in 10,560 accepted UNION ALL, INTERSECT ALL and EXCEPT ALL probes. The set
comparison checks input casts, output casts and nullable-field filters.

All 360 accepted COUNT, MIN, MAX, ARRAY_AGG, SUM and AVG probes match the
aggregate component expressions and types. The comparison covers the
applicable map, zero, increment, postprocess and empty-set expressions,
semigroup types, and MIN/MAX comparisons.

One devcontainer run compared all 84,224 inputs. It matched 60,840 accepted
inputs (15,708 column-only and 45,132 literal-bearing) and 23,384 rejections,
with no compiler failures or accepted expressions outside the capture scope.
The result covers the generated matrix. The [completion record](evidence/type-s2-complete.json)
links the exact source, input, outputs and comparison reports.

## Runtime value vectors

Nine Java-generated Rust circuits matched independent Rust bag predictions
over 36 transactions. They check equality and ordering with implicit
BIGINT/VARCHAR coercion, IN, BETWEEN, CASE, an explicitly cast join key, and
UNION ALL, INTERSECT ALL and EXCEPT ALL. The fixtures include signed BIGINT
limits, numeric strings, NULL, duplicates and retractions. They exercise
these runtime cases separately from the generated S2 matrix.
See the [runtime result](evidence/type-values.json).
