# Independent P0 architecture review

Reviewed on 2026-11-18 by the Codex `architecture_review` agent, independently
of the implementation agent. After reporting the findings, this reviewer also
implemented the AR3 repair at the implementation agent's request; that repair
requires a separate code review. This is an automated technical review, not the
named-maintainer acceptance required by PLAN.md §4.1. No legal approval,
staffing commitment, or P0 phase decision follows from this review.

Three decoder defects were reproduced and repaired as recorded below. The
representation and module boundaries are suitable for continued P0 experiments;
the review does not accept them as a production compiler interface.

## Findings

### AR1 — Duplicate logging keys survive the supposedly strict Rust decoder

Priority: P2. Status at review: reproduced, open.

[`IoOptions::logging_level`](sql-rel-model/src/compilation_context.rs) is a
`BTreeMap<String, String>` with Serde's default map deserializer. Duplicate map
keys overwrite earlier values. `deny_unknown_fields` on `IoOptions` does not
change that behavior inside its map. The whole-program transport calls
`serde_json::from_slice` directly, then validates the already-collapsed map.

Starting with `sql-rel-model/tests/fixtures/statement-program.json`, replace
`"loggingLevel": {}` with
`"loggingLevel": {"example": "1", "example": "2"}` and put the program on
one JSONL line. The existing devcontainer `program_roundtrip` executable exits
successfully and emits only `"example": "2"`. Direct Java `S1Program import`
rejects the same bytes with `Duplicate field 'example'`.

This contradicts the README's duplicate-field rejection claim and permits Rust
transport to turn a rejected Java input into an accepted one. Reject duplicate
map keys during deserialization, before they can be lost. Keep the fix in the
model's map decoder or its common decoding entry point; fixing only this one
example executable would leave ordinary model consumers exposed. Test duplicate
equal values, different values, and escaped spellings of the same key.

### AR2 — Catalog column indices exceed the Java adapter's domain

Priority: P2. Status at review: reproduced, open.

[`Column::index`](sql-rel-model/src/catalog.rs) is `u32`, and
`CatalogSchema::validate_metadata` never inspects it. Changing
`/catalog/schemas/0/columns/0/index` in the same fixture to `4294967295` passes
Rust deserialization and `Program::validate`. Java `S1Program import` fails in
`S1CatalogSchema.decode` with `Invalid metadata index`: `S1MetadataWire.unsigned`
requires a nonnegative Java `int`.

Bound the field to the actual adapter domain during validation and reflect the
bound in the generated schema. Audit other unbounded `u32` metadata that flows
into Java `int`; arena IDs already bounded by an arena length have a stronger
check. The same mutation to `999` also passes Rust. Decide from the captured
catalog contract whether an index must equal its column position, or whether
nonsequential values are meaningful. Do not normalize indices silently.

Regression coverage should include the signed-integer boundary and the chosen
column-position invariant. Existing valid-corpus parity does not test either
malformed-input case.

### AR3 — Precision and coordinates exceed the Java adapter's domain

Priority: P2. Status at review: reproduced; repair and verification below.

The wider integer-domain audit found three additional whole-program cases.
Changing the fixture's character length, timestamp precision, or statement
source line/end line to `2147483648` passes the original Rust validator. Direct
Java import rejects the two precisions with `Expected nonnegative int field:
value`, and the source coordinates with `Invalid metadata integer`.

`TypeTable::kind` checked SQL precision relationships but not the Java signed
integer domain. `TypeTable::origin` similarly checked ordering and source IDs
without that bound. Relational hint coordinates and inheritance paths also
reach the signed adapter, and the hint validator did not inspect their values.
These are transport-domain checks, separate from runtime precision caps or a
claim that every structurally valid type is supported by lowering.

The repair bounds specified scalar precision, DECIMAL precision and scale,
source coordinates, and hint coordinates and inheritance paths to `i32::MAX`.
Generated schemas now carry the bounds for `Precision`, DECIMAL parameters,
`Position`, and `Coordinates`. The existing large-DECIMAL behavior remains;
the patch does not apply the runtime precision-38 cap. Three focused Rust
tests check the signed boundary across all scalar precision forms, source
positions, and whole-program statement/hint use.

## Scope and feature traces

The primary trace follows the ordered-statement fixture through typed
`Program` decoding, expression and producer validation, catalog ownership,
statement ordering, source context, and the Java S1 reconstruction/lowering
adapter. The fixture includes table declarations, a function, views, DML, and an
index. This review inspected the trace in source and ran the targeted malformed
inputs above; it did not independently repeat the full-corpus lowering run.

The review also inspected the flat multiset/dead-code prototype and the native
BIGINT batch path from a validated expression program through object generation,
library loading, bounded input buffers, and DBSP projection. Those inspections
were limited to the named slices. Parser, folding, connector, binder, and every
crate's end-to-end SQL feature traces were not reviewed here.

Three invariants are understandable from Rust source without consulting Java:

| Module | Invariant and enforcement |
|---|---|
| `sql-rel-model/src/graph.rs` | Iterative DFS rejects cycles and bounds the longest path, including paths through previously visited shared subgraphs. Callers validate edge indices before entering this helper. |
| `sql-rel-model/src/typed_literals.rs` | `ValidatedTypes` immutably borrows a checked type arena. Literal checking retains exact nested type IDs and follows named representations without losing the outer declaration's identity. |
| `native-probe/src/model.rs` and `runtime.rs` | Private `Program` and `Batch` fields preserve graph, row-width, bitmap, and allocation bounds. Generated function pointers remain owned by the loaded library. The call's safety explanation connects validation, live buffers, output sizes, and overflow records. |

These are technical observations by an agent. They do not satisfy the plan's
requirement for a named maintainer to explain three modules and trace a feature
through each delivered crate.

## Architecture assessment

The S1 Rust model separates type, source, expression, relation, catalog, and
statement concerns. It has no planner, database, filesystem, or process
dependency. Enums, distinct type/source/expression IDs, explicit ownership,
bounded traversals, and `Result` make the invariants visible. The native
prototype uses private validated representations around its unsafe call;
the DBSP adapter stays outside that crate. These are sound directions under
PLAN.md §4.1.

The experimental whole-program API still exposes mutable raw wire data and
returns `()` from validation. A future compiler consumer could validate a
program, mutate it, then use unchecked indexing under stale assumptions. Before
using this model as a production phase boundary, introduce a borrowed validated
program view or an owned validated representation, following `ValidatedTypes`.
That is an API recommendation, not a demonstrated defect in today's transport,
which validates immediately before serialization.

Several relation, schema, and value-use references remain plain `u32` even
though they denote different arenas. Distinct IDs would prevent accidental
cross-arena assignment during a Rust binder implementation. This can follow
the existing `TypeId` and `ExprId` pattern without adding generic arena machinery.
Keep the wire representation unchanged and use parity checks for the refactor.

For S4, complete-prefix replay is a coherent current boundary: it recreates the
pass instances, analyses, compiler context, and history together. The declaration
inventory and `S4-STATE.md` explicitly avoid claiming resumable isolated
checkpoints. Reflection over declared fields cannot prove the lifetimes or
dynamic objects reachable through those fields. Retain that distinction; a
future isolated checkpoint needs explicit state ownership and reconstruction
contracts. This review does not close the existing field-lifetime review item.

## Reproduction and snapshot

The initial review ran three decisive Rust transport mutations and two Java
import commands. Raw inputs, transport outputs, and Java failures are in the
devcontainer at `$SCRATCH/p0-architecture-review-v1`. One additional presentation-kind mutation
was accepted; it is not reported as a defect because that field is explicitly
presentation metadata.

The wider integer audit and its three Rust/Java comparisons are retained at
`$SCRATCH/p0-architecture-review-v2`. After the repairs, all six decisive malformed
inputs were rerun against rebuilt Rust transport executables: each now fails
with its intended diagnostic. The initial and repaired results remain separate.
No benchmark or runtime correctness claim follows from these checks.

The findings refer to these source SHA-256 hashes; later repairs must retain the
original finding and append their validation result.

| Source | SHA-256 |
|---|---|
| `sql-rel-model/src/compilation_context.rs` | `d7af57fbc517e70b61862ec7ae94fe12704e74348cebda85a1df3b784aa6bd4c` |
| `sql-rel-model/src/catalog.rs` | `93e6b332318b0750b098397a7cd93d2144cdf0edcb00809a49b4b1515262a86e` |
| `sql-rel-model/src/program.rs` | `89250c8e126f091d156df55fb7476f78b15a5b0a03ddc334c88e7ac4ba89aac9` |
| `sql-rel-model/examples/program_roundtrip.rs` | `d0f08b9da27292d9553492ce28cc6fcf85e9d77a0ed5899cecb8da3ec248e678` |
| Java `S1MetadataWire.java` | `deade05875a09ab87d451ba0bbfb9ff6f413c419cd51504d92310b2f340527cd` |

## Repair review

AR1: the implementation agent added a model-level `logging_levels` Serde
visitor. Inspection confirms that duplicate keys are rejected before a map can
escape, including when values are equal or key spellings decode identically.
Independent replay of the original malformed bytes now fails with `duplicate
logging level key`. The regression starts from raw JSON, avoiding a preparse
that would itself erase duplicates.

AR2: the implementation agent now requires each catalog column index to equal
its ordered position and fit a signed integer. Java applies the same positional
check. Independent replay rejects both `999` and `4294967295`. The generated
column schema records the signed upper bound; positional equality remains a
semantic validator check.

AR3: the reviewer implemented the range checks after the implementation agent
authorized the repair. The original character, timestamp, and statement-position
mutations now fail with `signed-integer range` errors. The implementation agent
must review this patch separately. All 103 Rust tests and Clippy with warnings
denied pass in the devcontainer, recorded in
`$SCRATCH/p0-rel-model-architecture-review-v1.log`. Formatting checks pass. These
repairs do not alter the scope of earlier frozen corpus certificates.

The implementation agent subsequently inspected AR3's checks against the Java
scalar and metadata decoders. The checks preserve the distinction between an
unspecified precision and a specified nonnegative signed integer, keep the
runtime decimal cap separate, and cover both source-origin and hint coordinates.
The schema bounds and focused tests agree with those checks. The full-corpus
acceptance result below confirms that the stricter validation preserves all
recorded valid inputs. This is a second automated review, not maintainer approval.

## Full-corpus acceptance after the repairs

The repaired release-mode Rust transport validates all **17,098** frozen
`statement-program-v5` inputs: 8,571 classic and 8,527 Gen-2 programs, containing
254,860 statements across ten disjoint shards. Every parsed output record and
its ordered identity exactly matches its input. No previously exported program
is rejected by AR1–AR3's stricter checks.

The run freezes the executable and current Rust crate sources, hashes every
input and output, verifies the complete producer shard population, rejects
duplicate per-engine identities, and rechecks input, executable, and source
hashes before writing `complete: true`. Artifacts are in the devcontainer at
`$SCRATCH/p0-s1-validation-v1`, including `complete.json`, source and input manifests,
per-shard comparisons, and the frozen runner script. The execution log is
`$SCRATCH/p0-s1-validation-v1.log`.

This follow-up proves acceptance and lossless typed Rust transport of the frozen
v9 exports under the repaired decoder. It does not rerun Java reconstruction,
optimizer prefixes, generated artifacts, or runtime execution. Those results
remain attached to their separately recorded compiler snapshots.

## Independent review of relational SQL renderer lifetime

The conversion-lifetime repair has no blocking finding in this source review.
Both `CalciteRelNode.toSqlString(RelNode)` and `RelReference.Live.sql()` now
create one `RelToSqlConverter` for each complete root conversion. That keeps
alias allocation consistent within a query while preventing prior renderings
from changing aliases or enlarging the retained alias set. The shared helper
centralizes lifetime without merging the two existing formatting or fallback
behaviors. No converter escapes the method.

`RelSqlRenderingTests` interleaves both rendering APIs on distinct join plans,
then requires repeated output to equal the original string. Checking that the
result contains JOIN prevents fallback text from satisfying the comparison.
This is an appropriate regression for history-dependent aliases. The root agent
reports that the original implementation fails both tests and the repair passes
them; this independent review inspected the source and test assertions, and did
not rerun or independently verify its profiling/benchmark measurements. It does
not certify global compiler determinism or concurrent use of the remaining
compiler state.

Reviewed SHA-256 snapshots:

- `sql-to-dbsp-compiler/SQL-compiler/src/main/java/org/dbsp/sqlCompiler/compiler/frontend/calciteObject/CalciteRelNode.java`: `799e7b83517e621737f38199674d7f83962bf02d0de7007385725f256d3764a3`.
- `sql-to-dbsp-compiler/SQL-compiler/src/main/java/org/dbsp/sqlCompiler/compiler/frontend/calciteObject/RelReference.java`: `0f75fe73a2f939ea10debb6a42f34f9bdb9b749a5351505a885615ae11654e4c`.
- `sql-to-dbsp-compiler/SQL-compiler/src/test/java/org/dbsp/sqlCompiler/compiler/sql/RelSqlRenderingTests.java`: `53d46c9a8ec0ab52a9f7c27ded70a86f32713dcf26c2812aafba3a3a99021402`.

## Optional optimizer observer review

Independent source review found no blocking behavior or lifetime issue in the
optional per-compiler observer. Production leaves the hook null. The existing
rule selection, order, fallback and plan replacement logic remain unchanged.
The before callback runs outside the optimizer exception handler; the after
callback runs in finally, so callback failures propagate rather than becoming
failed-rule skips. Each optimizer is fresh, and CorrelationProbe snapshots the
plan immediately to JSON rather than retaining mutable RelNode references.

If the existing trace writer throws, after is not called; compilation already
aborts. A failing after callback can mask the original optimizer exception,
but also aborts instead of yielding a successful observation. A future caller
that changes the observer during a callback should capture one observer per
step to preserve callback pairing; the current caller never replaces it.
These limits do not block this test-only observation hook. This was source
review; the parent agent reported targeted tests and corpus checks separately.

Reviewed source hashes:

- `sql-to-dbsp-compiler/SQL-compiler/src/main/java/org/dbsp/sqlCompiler/compiler/frontend/calciteCompiler/optimizer/CalciteOptimizer.java`: `c0d0e667b41de2a51502bbea53dd524716ff74b421b7f4d48f24a2c62124b8ac`
- `sql-to-dbsp-compiler/SQL-compiler/src/main/java/org/dbsp/sqlCompiler/compiler/frontend/calciteCompiler/SqlToRelCompiler.java`: `04bafc78d95bf14d2cfd70c04a22110f9ca367a592a3d7a3de2146e0c3b56e13`
- `sql-to-dbsp-compiler/SQL-compiler/src/test/java/org/dbsp/sqlCompiler/compiler/sql/tools/CorrelationProbe.java`: `b42ec1846bfe7990eed8c05d7d704fd9ab7a69d4133bbebc5b3521f39d66587a`

## ROW selector intermediate representation change

An independent controlled rollback explains the reviewed compiler's S2 change
for `RegressionTests#issue3326/0` (`scopeSpan.scope['name']`). Removing only
the new character-index ROW branch from the frozen reviewed ConvertletTable
restores exact canonical equality with the old S2 dump. All other reviewed
classes remain identical. The repaired dump differs by one clone expression
and the consequent graph references; the difference is not normalized away.

The cause is visible in ExpressionCompiler: the ITEM path strips a clone from
its collection operand, while visitFieldAccess retains the source and clones
the selected field when needed. The repaired convertlet intentionally resolves
the field before cast simplification can lose names. This is an expected
intermediate representation change, not an unexplained semantic regression.
The parent independently reports final S3/backend equality; this rollback
experiment establishes S2 causality only. Old S1 inputs containing the previous
ITEM representation must be re-exported for comparisons against the repaired
compiler, or retained as explicitly different historical-version inputs.

Evidence: `evidence/row-selector-intermediate-change.json` and
`$SCRATCH/p0-issue3326-convertlet-rollback-v1` retain the exact removed block,
reviewed/rollback sources, overlay class, commands, logs and raw/canonical dump.

## Quoted function parameters at the SQL/Rust boundary

The parser semantic matrix found a distinct compiler assertion: SQL parameter
`"a""b"` was passed unchanged to DBSPParameter and then DBSPVariablePath, whose
constructor requires a Rust identifier. ExternalFunction now shares parameter
construction between SQL implementations and external declarations. Names that
fail the existing identifier predicate receive deterministic local names;
reserving every original name prevents capture by a legal parameter named
`__feldera_arg_0`. SQL names in the function signature remain unchanged.

The regression checks both argument positions with different values, across
optimization levels 0 and 2, and proves declaration/implementation consistency.
Both tests fail with the old frozen production classes and pass with the repair.
Six generated integer functions independently compile with rustc and execute
all expected-value assertions. The full Maven source compilation and 29
focused tests pass. This repair covers names rejected by the existing predicate;
it does not assert that the predicate implements Rust's complete Unicode or
reserved-word rules. Raw failed and repaired observations are retained in
`evidence/parser-semantic.json` and its archive.
