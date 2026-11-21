# Relational model contract work

This isolated P0 crate models typed source, type, literal, expression, relation,
catalog, and ordered statement arenas. The current experimental whole-program
format is `statement-program-v5`; it also carries compiler options, source state,
frontend diagnostic boundaries, pending LATENESS checks, and relative table-scan
creation order. It has no planner, parser, engine, filesystem or process dependency.
It is not an accepted production FRel/FRex schema and does not close P0.

After deserializing a complete program, call `Program::validate`. Validating only
its `TypeTable` does not check statement/catalog ownership, expression contexts,
diagnostic boundaries, or scan order. The Java adapter reconstructs typed objects
and lowers them without a SQL frontend. The frozen version-2 certificate passes
S2, schema and diagnostic comparison for all 17,098 exported cases. Version 5
passes the complete optimizer prefix for the same exported population, including
byte-equal backend artifacts and persistent IDs.
[Version-5 certificate](../evidence/s1-prefix.json) · [P0 acceptance status](../P0.md)

The evidence entries below describe their named, frozen experiments. Limitations
in an earlier entry describe that snapshot; they do not override later results.

`types.rs` owns ordered type arenas and distinct SQL forms. `source.rs` owns
identifiers and source coordinates. `validate.rs` checks references, cycles,
limits, decimal parameters, interval qualifiers and basic source ranges. JSON
Schema is generated from these Rust definitions, not maintained separately.
Fragment versions such as `types-v0` and `expressions-v2` remain distinct from
the whole-program envelope; each is checked independently.

Decoding rejects unknown variants, unknown fields, duplicate fields, missing
required fields and unknown versions. Zero-field tagged variants use empty
struct variants: Serde's internally tagged unit variants otherwise accept extra
fields even with `deny_unknown_fields`. Regression tests exercise this behavior.
After decoding a standalone type fragment, callers must run `TypeTable::validate`;
JSON Schema cannot prove that graph references resolve or that the graph is acyclic.

Validation visits unreachable entries too. Its iterative traversal bounds depth
without consuming the call stack and memoizes shared-subtree heights. It neither
interns equal nodes nor sorts fields or roots. Repeated names and references stay
ordered. Type precision is preserved before Java lowering's narrowing rules;
the model does not silently cap DECIMAL precision at 38 or merge FLOAT and DOUBLE.

Source positions use one-based UTF-16 columns and inclusive ends, matching
Calcite with tab size one. Absent origins have a separate variant. The current
verifier checks coordinates and source references, not correspondence between a
range and a submission's text; the submission adapter must establish that.
Byte-oriented parsers need an explicit conversion before producing this format.

The limits bound verification work after decoding. A caller must separately bound
input bytes and deserialization. The modeled planner types are not a lowering
allow-list: MULTISET, local-time-zone types, ANY, UNKNOWN and FUNCTION need phase
rules. Unsupported external types must produce an error, not an opaque JSON bag
or SQL string. The complete type projection still needs measurement against the
corpus and review alongside the expression and statement contracts.

Run locally or in the devcontainer:

```sh
cargo test --locked --manifest-path crates/sql/sql-rel-model/Cargo.toml
cargo clippy --locked --manifest-path crates/sql/sql-rel-model/Cargo.toml --all-targets -- -D warnings
cargo run --locked --manifest-path crates/sql/sql-rel-model/Cargo.toml --example type_schema > crates/sql/sql-rel-model/type-schema.json
```

Tests cover graph sharing, duplicate output names, orphan cycles, dangling
references, a 20,000-node chain, shared-subtree depth limits, source absence,
strict decoding and schema drift. CI runs the tests and Clippy. Independent
schema review, transient-form dispositions, multiple compilation batches, JDBC
adapters, and complete join system fields remain open. See
[the source inventory](../FREL.md).

## Draft literal payloads

`literals.rs` adds typed literal payloads for the developing relational boundary.
Integers use decimal text; exact decimals retain a coefficient and signed scale.
Floating-point values use integer bit patterns, preserving signed zero, infinities,
and NaN payloads. Nested arrays, rows, maps, and multisets keep types and order;
map duplicates are not collapsed. Date/time fields and interval storage units
are explicit. Geometry now has an exact Point subset; other shapes and VARIANT remain
unsupported. Unknown wire variants fail decoding.

`Literal::validate_payload` bounds nodes, depth, and payload bytes through an
iterative traversal. It checks coefficient spelling, time bounds, symbol fields,
and multiset multiplicities. This is payload validation: it does not resolve
`TypeId`/source references, check SQL type/value compatibility, or establish
Calcite round-trip parity. Those checks belong to the complete S1 adapter and
scoped verifier. The generated `literal-schema.json` is a draft fragment, not a
new S1 protocol version. A regression checks schema drift.

The first measured adapter slice transports 94 scalar fixture payloads through
Java export, strict Rust decoding/encoding, and Java import. It preserves the
measured RexLiteral values and types, including unsigned integer boundaries,
NaN payload bits, signed zero, exact decimals, Unicode, binary data, temporal
values, typed NULLs, and all thirteen interval qualifiers. Missing cases,
duplicate identities, extra fields, invalid coefficients, and a changed NaN
payload are detected. See [the certificate](../evidence/s1-literal-payloads.json).

The original payload-only mode supplies SQL types from its fixture registry.
Typed mode (`export-typed` / `verify-typed`) additionally transports a one-entry
scalar type table. Rust validates the declared type against the payload, and
Java reconstructs the type from the table before decoding the value. All 94
fixtures preserve exact Calcite type equality and full type digests. Nine
negative mutations fail. The wire's `type_description` remains diagnostic text.
See [the typed certificate](../evidence/s1-scalar-types.json).

`TypeTable::validate_literal` checks the arena and payload limits, all nested
source/type references, declared nullability, integer ranges, exact decimal
representability, float width, character/binary lengths, fractional temporal
precision, interval storage units and collection shape/cardinality. Nested types
must use the exact declared IDs. Named types retain outer identity and nullability;
their representation controls payload shape. Non-null unsupported types fail.
Character lengths use UTF-16 code units, matching the measured Calcite constructor.
This does not validate character encodability, collation semantics, interval value
qualifiers or timezone names. Local-timezone payloads remain unsupported.

The typed experiment exposed a missing distinction in the draft type schema:
Calcite's built-in IMPLICIT/COERCIBLE collation singletons affect type digests
differently from equal-named, separately constructed collations. Character metadata
now records that identity explicitly. The older certificates retain the earlier
draft's sources; `types-v0` is experimental and has no production consumers.

Compound/named type transport, shared arena identity, real source origins,
default-versus-explicit precision beyond the fixtures, relational/statement
transport and complete S1 parity remain open. The existing Java workflow runs
the typed slice and retains all three transport stages. All 24 Rust tests, eight
Java tests, formatting and Clippy pass for the measured implementation.

Before typed export, the Java adapter reconstructs the type and checks exact type
identity and the full digest. Unsupported precision distinctions, such as explicit
FLOAT precision, fail before export. The regression also checks the measured
default TIME form, which the type factory preserves.

The [point extension](../evidence/s1-point-literals.json) expands typed transport
to 105 fixtures: the original 94 scalars plus ten points and a geometry NULL.
`GeometryPoint` retains signed SRID, coordinate dimension, measure count and raw
IEEE ordinate bits. Empty points have no ordinates. Supported layouts are XY,
XYZ, XYM and XYZM; validation checks dimensions and accounts for coordinate bytes.
The adapter rejects non-Point shapes, non-floating precision models and user data.
Coordinate-sequence factory identity is not transported; the measured point fields
are preserved exactly. This is still a draft S1 fragment, with 25 Rust tests,
ten Java tests and fifteen negative transport checks.

## Ordered compound type transport

`S1TypeTable` now exports and imports row, array, multiset and map type arenas
alongside scalar types. The importer bypasses Calcite's global interning caches:
shared references remain shared, while distinct equal entries remain distinct.
Root/field order, duplicate field names, struct kind and nested nullability survive.
Resolved field names use `quoted=true` and absent origins; this is an exact resolved
name, not a recovered SQL token or original quoting decision.

The expanded wire preserves raw DECIMAL scale omission separately from its resolved
scale, raw unspecified CHAR/BINARY length, and SQL versus host geometry types.
QUARTER and WEEK join the interval units. SYMBOL, VARIANT, UUID, ANY, UNKNOWN,
COLUMN_LIST, CURSOR and all six temporal zone flavors have explicit type tags.
Descriptor types are planner forms; representing them does not admit arbitrary
runtime literal values. Named catalog identity and function types remain separate
work. Unsupported forms fail export/import rather than becoming JSON extensions.

[The measured certificate](../evidence/s1-type-arenas.json) covers 110 arena entries
and 161 fixture roots, plus all optimized-view relation/expression type roots in
8,571 classic and 8,527 exported Gen-2 cases. Rust validates and rewrites the arenas;
Java reconstructs every entry and checks exact wire fields and sharing. Both
languages reject fifteen malformed graphs and metadata mutations. The bounded
streaming corpus example refuses partial type support and duplicate case IDs.

These are type observations from optimized view plans. The collector does not
capture every statement/catalog type, binding, expression value or metadata field.
This result does not certify full FRel/FRex/statement v0, SQL lowering, C7 or S1
parity. The version remains experimental `types-v0`; earlier certificates retain
the earlier draft and its frozen tools.

## Corpus literal transport

[The full literal certificate](../evidence/s1-literals-full.json) records 21,471
classic and 21,427 Gen-2 literals across their complete exported case populations.
Collection expands SEARCH using the same `RexUtil.expandSearch` call as lowering;
unexpanded SARG values are rejected by both transport readers. This is an
observational collection of expanded expressions, not a rewritten relational plan.

The adapter now covers the observed symbol domains, UUID bytes and local zoned
timestamp fields. Zoned local values retain calendar fields and the zone label
before any instant conversion; invalid DST-gap clock values remain explicit errors.
The existing instant-based timestamp variant remains distinct. A test-only bridge
uses Calcite's exact constructor because its builders can infer CHAR from VARCHAR
and clone NULL collection types. The bridge preserves resolved types and their
arena identity, without changing the production Calcite dependency.

`TypeTable::validated` returns an immutable borrowed proof of structural validity.
Reuse its `validate_literal` method for a corpus batch; the borrow prevents graph
mutation and avoids validating the same type arena for each literal. Payload limits
remain explicit. The streaming example bounds record bytes before decoding and
checks case identity, literal order, root type IDs and every payload.

All 146 literal fixtures, 26 Rust tests, twenty Java tests and fifteen malformed
records rejected independently by both languages pass for this experiment. The
certificate retains the earlier builder-normalization, NULL identity and temporary
filter-construction failures. Statement/catalog literals and complete expression,
relation, binding and statement transport remain open, as do full S1-to-S2 and C7
parity. This result does not complete spike 11.

## Resolved expression draft

`ExpressionTable` adds ordered expression, operator, binding and scope arenas for
InputRef, Correlation, Literal, FieldAccess, Call, LambdaRef and Lambda. Resolved
operator identity contains an explicit registry, registry version, key and exact
parameter/result type IDs. Adapters must check registry membership; the structural
verifier does not infer SQL types or authorize arbitrary functions.

The verifier checks every entry, including orphans, before following graph edges.
It rejects cycles, excessive shared-subtree depth, dangling references, duplicate
or absent binding declarations, scope escapes, local inputs substituted for outer
correlations, exact type-ID mismatches and inconsistent call signatures. Lambda
bodies have child scopes that declare exactly their ordered parameters. Names and
slot indices survive unchanged. As in Calcite, a lambda expression's type is its
body's result type; its parameters supply the separate signature. A root scope
spells its required parent as `null` in both Rust and JSON Schema.

[The fixture certificate](../evidence/s1-expressions.json) records seven real Rex
nodes crossing Java, Rust and Java, including sharing and distinct equal type
entries. Both readers reject fifteen malformed variants. The Java reader is a
fixed fixture oracle with one PLUS operator, child-first edges and known binding
layout. It is not the general expression importer. The draft still needs transient
planner forms, relation/statement attachment, a complete operator registry and
corpus coverage before adoption as FRex v0 or full S1. It proves no lowering or C7
parity. Earlier certificates retain their historical schemas and source snapshots.

The [static operator registry](../evidence/s1-operator-registry.json) resolves 603
public constants from two fixed trusted Calcite tables to their exact objects.
Registry keys and versions drive a frozen lookup map; wire input never chooses a
Java class or field for reflection. An equal-named operator object cannot impersonate
a registered one. Per-call signatures remain explicit in the Rust model and need
independent validation by an importer. Catalog, custom, spatial and dynamically
created operators remain outside this registry. This is test infrastructure, not
the complete function catalog or corpus expression importer.

[Symbol validation hardening](../evidence/s1-symbol-validation.json) rejects unknown
symbol domains and invalid or mismatched members in Rust before import. The
supported members match Java's TimeUnitRange and TRIM mode enums. Three retained
counterexamples show that the previous verifier accepted unsupported symbols;
both readers now reject them. Revalidation preserves every one of the 42,898
captured corpus literals and all 146 fixtures, including all 29 supported enum
members. All 40 Rust tests and Clippy pass. This tightens the draft verifier; it
adds no whole-program S1 or runtime parity claim.

[The Java storage guard](../evidence/s1-literal-storage.json) closes a separate
NaN comparison hole: `Double.equals` and RexLiteral equality canonicalize NaNs,
so they could accept a REAL payload that lost bits from its underlying DOUBLE
storage. Export now reconstructs the supported payload and checks the type object,
storage tag/class and raw floating bits. It also rejects unrepresentable REAL
precision and integer storage scale. Representable REAL NaN payloads, subnormals,
signed zero and arbitrary DOUBLE NaN payloads still pass. The intentionally failing
pre-fix regression remains in the evidence. Fresh capture preserves all 42,898
literal records and 146 fixtures exactly; 29 focused Java tests pass. This remains
an optimized-view literal fragment, not full S1 or C7.


The corrected local Rex-field walker explicitly covers the 17 audited lowering
relation classes, including VALUES rows, window constants and frame offsets,
aggregate direct arguments and ASOF match conditions. Generic
`RelNode.accept(RexShuttle)` omitted several of these fields. The expanded
Java/Rust/Java run transports 58,661 literals and all collected type roots across
8,571 classic and 8,527 Gen-2 cases. This supersedes the coverage counts of the
older captures; their artifacts remain historical evidence. See
[the field-walk certificate](../evidence/s1-rel-field-walk.json).

`expressions-v1` adds an explicit expression ID for each lambda declaration.
The declaration and body reference may be equal but distinct Java objects.
Binding-free literal/call subgraphs can be shared between evaluation scopes;
that permits transport of existing sharing and conveys no permission to cache,
hoist or merge evaluations. Calls that depend on bindings retain scope checks.


The [general expression-fragment codec](../evidence/s1-expression-codec.json)
exports contextual Rex graphs and imports arbitrary arena ordering with bounded
iterative graph checks. Six constructor graphs (25 expressions and 15 roots)
pass Java/Rust/Java transport; a separate seven-node oracle also passes. Tests
cover nested lambda capture, separate equal declarations/references, shared
closed calls and distinct equal types. Twenty malformed variants fail both
readers; the unknown registry key is rejected by Java independently of Rust's
structural verifier. All 42 Rust and 48 focused Java tests pass.

The versioned registries resolve 603 Calcite keys and 61 Feldera builtin objects.
They distinguish Feldera's cloned `abs` from Calcite's `ABS`. Unknown keys and
versions fail; input never directs class loading. Correlation IDs become fragment
binding IDs, so a future relation adapter must connect declarations consistently.
Source-bearing calls and specialized Rex subclasses are explicitly unsupported.
This is a constructor-graph experiment, not a corpus FRex or whole-program S1
certificate. CI runs the graph transport and uploads its outputs and inventory.


The [source-origin extension](../evidence/s1-expression-sources.json) adds explicit
source records for calls. Seven constructor graphs (28 expressions, 16 roots)
pass Java/Rust/Java transport, and 52 Java tests include actual optimized SQL
project expressions. Source text and UTF-16/inclusive positions survive exactly;
the Java adapter checks line/column bounds and one-past-line EOF anchors. Ten
malformed source cases are retained. Rust rejects structural/range-order errors;
two text-bound violations remain structurally valid there and fail the Java
submission adapter, matching the verifier's documented responsibility.

Callers must supply the text to which coordinates refer. This does not yet map
whole-program submission boundaries or preserve nullable source-file metadata.
Type/field and non-call source origins, quoted parser positions and specialized
Rex subclasses remain unsupported by the Java codec. The
[source archive](../evidence/s1-expression-sources-archive.json) pins 188 artifacts,
66 sources and five classpath records. Whole-program S1 remains open.


The [corpus-driven context repairs](../evidence/s1-expression-context.json) add
`Origin::Generated` for partially unknown positions. An actual row-comparison
call has coordinates `(0, 0, 2, 31)`: replacing its unknown start with an invented
one-based point loses information. Generated ranges retain zero components,
source identity and known endpoints. Fully unknown positions remain absent;
fully known ranges use the ordinary source variant. Rust checks references,
partiality and ordering; Java also checks known text bounds. All three generated
schemas and their drift checks include this experimental extension.

Import now constructs input references directly. `RexBuilder.makeInputRef` adds
character metadata and can clone an already resolved type object, which the
corpus exposed. A retained pre-fix regression and character-input transport graph
cover the repair. The literal adapter rejects SARG storage explicitly, and the
expression reader rejects SEARCH even when its operator key is registered.
Project capture applies the exact SEARCH expansion used by `ExpressionCompiler`.

All **43 Rust and 56 focused Java tests** pass. Nine constructor graphs (32 nodes,
19 roots), a separate seven-node oracle and a **240-case optimized-project
prefix** pass Java/Rust/Java transport. The prefix contains **389 projects,
5,217 nodes and 1,002 roots**. The attempted 1,429-case shard stops at case 241,
`StructTests#structConstructorTest/0`: its `address_typ` operator is a
`SqlTypeConstructorFunction` requiring catalog/type declarations. This failure
is retained; no case is excluded and no full-shard pass is claimed. The full
FRel/FRex/statement model, catalog bindings and detached S1-to-S2 remain open.

The [context archive](../evidence/s1-expression-context-archive.json) verifies
362 artifacts, 131 sources and eleven classpath records, including earlier
failures and the current successful prefix. The corpus tools measure project
fragments only; they do not transport every local Rex-bearing field or preserve
whole-program relational/catalog sharing.


The [constructor-object extension](../evidence/s1-constructor-objects.json) uses
`expressions-v2`. `operators.rs` owns an operator-object arena, separate from
instantiated call signatures. Builtins retain registry identity. Type constructors
carry their declared type and qualified name, including quoting and the optional
component-position list. Row constructors retain their name and absent/present
field-name lists, including null entries. Equal distinct constructor objects
remain separate; multiple signatures can reference one shared object. The Java
adapter instantiates only fixed trusted classes; wire data cannot name a class.

Field references now permit equal distinct unnamed type objects and safe outer
nullability widening. The declared kind and nested IDs must still match, narrowing
is rejected, and named-type identity stays nominal. Neither type objects nor
expression headers are rewritten. The actual SQL regression and focused tests
cover the nullable reference to a non-nullable aggregate output.

All 49 Rust and 63 Java tests pass. Eleven constructor graphs (41 expressions,
26 roots) and the separate seven-node fixture pass Java/Rust/Java transport.
The corpus run now transports a **527-case project-only prefix**, with **900
projects, 9,876 expressions and 3,017 roots**. The 1,429-case shard remains failed:
its next case calls SQL-defined `ExternalFunction` `expensive`, whose body and
parameter scope are not yet transported. This is not a full-shard, full FRex,
statement/catalog or detached S1-to-S2 result.


The SQL-function extension uses `expressions-v3`. Function objects retain their
ordered parameters, declared result type, generated flag, diagnostics, and either
an explicit external body or a root in the shared expression arena. SQL bodies
have isolated parameter scopes. A combined expression/operator construction graph
rejects cycles that cross the two arenas; ordinary expression-cycle checks alone
would miss these dependencies. Shared function objects and closed expression
nodes retain their identity across callers and bodies.

Function bodies compiled from synthetic queries retain those queries as source
records for diagnostic coordinates. The adapter reconstructs the typed body
directly; it never parses the captured text. Function declaration positions still
refer to the caller's source. Type-constructor name positions inside bodies use
the body's source as well. The compiler's identity-function extractor now gives
the body input reference its sole field's type, rather than its containing row
type. Multi-parameter functions retain their project extraction path.

The builtin registry now includes the compiler's Unicode-wrapper singleton,
spatial overloads identified by their exact trusted Java method signatures, and
Calcite's internal operator table, including datetime subtraction. Wire keys
select from fixed trusted maps; they cannot load a class or invoke reflection.

All **54 Rust and 74 focused Java tests** pass, with Clippy and schema drift
checks. Fourteen constructor graphs (53 nodes, 31 roots) and the separate
seven-node oracle pass Java/Rust/Java transport. The full exported project
populations now pass all ten shards: **8,571 classic cases** contain **12,744
projects, 73,084 expressions, and 36,607 roots**; **8,527 Gen-2 cases** contain
**12,686 projects, 72,843 expressions, and 36,456 roots**. CI now runs project
transport on its prepared classic corpus as well.

These counts cover optimized projects, not all local Rex fields, relational
nodes, catalog state, or statements. Whole-program S1 and detached S1-to-S2 remain
open. Historical failed runs retain the unsupported operators, incorrect body
source contexts, and extractor regression that drove these repairs.

The [function-body certificate](../evidence/s1-function-bodies.json) and
[archive index](../evidence/s1-function-bodies-archive.json) pin the sources,
classpaths, full project results, two generated Rust runtime tests, and retained
failures. Integrity verification checks 1,000 artifacts, 82 sources, and five
classpath records.


The `expressions-v4` extension adds extracted window aggregate calls with their
ordinal, DISTINCT, IGNORE NULLS, exact class, and source position. Their evaluation
depends on the window group even when their arguments are constant; the verifier
therefore retains scope dependence. The Java adapter also checks that the
resolved operator is an aggregate. Feldera's replacement ARRAY_AGG retains its
own trusted identity.

The all-local-field corpus passes all ten shards: 8,571 classic cases have 22,774
fragments, 98,025 expression nodes and 49,127 roots; 8,527 Gen-2 cases have 22,685
fragments, 97,629 nodes and 48,934 roots. The field walker includes Values,
filters, joins/ASOF, aggregate direct arguments, Window constants/bounds/calls,
Sort bounds and table-function calls. Window contexts include their separate
constant slots. Both readers reject unknown or mixed capture scopes; the
experimental corpus envelope names its collection `fragments`.

All 56 Rust and 80 focused Java tests pass, as do Clippy, schema drift, fifteen
constructor graphs (56 nodes, 34 roots), the seven-node oracle, and four malformed
window variants in both readers. CI runs both capture scopes. These are local
expression fragments: cross-relation sharing, relation edges, aggregate
descriptors, window groups/frames/collations, traits, hints, statements and
catalog effects still need the full relation/statement adapter.

The [local-expression certificate](../evidence/s1-local-expression-fields.json)
and [verified archive index](../evidence/s1-local-expression-fields-archive.json)
retain 984 artifact hashes, 82 source hashes and five classpath records. The
123,439,841-byte archive includes the earlier window-subclass and ARRAY_AGG
failures alongside the successful full audit.


The [whole-program sharing census](../evidence/s1-expression-sharing.json) finds
725 input-reference objects shared across distinct input environments in 117 cases
per engine. It observes original Java objects before SEARCH expansion. The
fragment model's single scope per expression is therefore an experimental local
contract, not the full model's object-ownership rule. The full model must separate
expression identity from evaluation context and validate each bounded
expression/context state; see [FREL.md](../FREL.md). Two constructor regressions
distinguish different inputs from two consumers of one input. All 82 focused Java
tests pass. The [sharing archive](../evidence/s1-expression-sharing-archive.json)
verifies 82 artifacts, 46 sources and three classpath records.


The `input-uses-v0` contract in `input_uses.rs` separates shared input-reference
objects from evaluation uses. Distinct typed object/environment IDs prevent
accidental interchange, and the verifier bounds and validates each use rather
than memoizing only by object. The fragment and input-use validators share one
resolved-reference type compatibility rule. Java reconstructs one RexInputRef per
object ID, preserving both sharing and equal distinct objects.

All 8,571 classic and 8,527 Gen-2 cases pass this transport; see the
[input-use certificate](../evidence/s1-input-uses.json). The populations retain
34,413/34,192 objects and 35,231/35,010 uses respectively. All 61 Rust and 85 Java
tests pass with Clippy and schema drift. This contract assumes the producer rows
supplied by the capture; their relationship to the real relation graph remains
the full adapter's responsibility. Other expression objects and statement/catalog
state are outside this input-reference contract.

`input-producers-v0` checks those environments against an explicit producer DAG:
arity, cycles, depth, ordered row fields, function parameters and Window constant
slots. `relation-properties-v0` adds typed operator descriptors, aggregate operator
identity, and common traits/hints. The metadata validators share a cumulative
reference and string budget. Unknown tags and fields fail; these contracts have
no arbitrary JSON extension bag or wire-directed Java class loading.

The [descriptor certificate](../evidence/s1-relation-properties.json) covers all
17,098 cases, 74 Rust tests and 90 Java tests. Java reconstructs operators, traits
and hints; full relation nodes, expression-use linkage, statements and catalog
ownership remain open. The crate includes Calcite's LICENSE and NOTICE for the
adapted trait-ordering rules and declares Apache-2.0. See
[the licensing inventory](../LICENSING.md) for the pending independent review.

`expression-objects-v0` links the local fields to one expression arena spanning
relations and SQL function bodies. Source/input contexts belong to uses; lambda
references resolve in each lexical frame. The validator checks every context,
combined expression/function cycles, relational slot roles, and separate limits
for distinct states and repeated traversal work. Memoized SEARCH expansion runs
before input-use collection. Java reconstructs exact expression classes and
object sharing from the wire.

The [expression-object certificate](../evidence/s1-expression-objects.json)
covers all 17,098 cases, 81 Rust tests, and 95 Java tests. Complete relation nodes,
statement/catalog ownership, cross-arena aggregate-operator identity, and
detached S1-to-S2 remain open. Java independently validates intrinsic object
construction; the Rust reader validates contextual evaluation uses.

The [relation constructor certificate](../evidence/s1-rel-construction.json)
extends this transport to all 79,336 relations in the same corpus. It preserves
declared row objects and output names, checks constructor-derived field types,
and re-encodes properties and expression links exactly. Memoized SEARCH
normalization also flattens Boolean calls for Filter construction. Scan tables
remain placeholders: catalog/statement replay and S1-to-S2 are still open.

Catalog transport now models schema and table-wrapper identities separately,
with explicit scan edges. `catalog.rs` preserves column types, quoting, defaults,
lateness, diagnostics, keys, unused flags, and optional positioned properties.
The expression-object and input-use experiments use version 1 to add constant
value contexts. Their required nullable metadata fields reject missing fields
while accepting explicit JSON null. Uncalled function declarations can also
register SQL bodies independently of relation roots.

The [catalog certificate](../evidence/s1-catalog-transport.json) records all
17,098 cases passing Java/Rust/Java transport and catalog-backed relational
construction, 85 Rust tests, 111 Java tests and Clippy. The independently checked
[archive](../evidence/s1-catalog-transport-archive.json) retains the exact tools,
sources and failed diagnostic-printer run. These are optimized-view catalog
snapshots, not the full ordered statement stream or detached S1-to-S2.

`statement-program-v1` composes ordered typed declarations and DML with the
expression, relation, and catalog arenas. Its source identities distinguish the
user program from exact compiler-generated parser inputs. `catalog-v1` also
records the source of each schema and its constant-value use. Required nullable
fields remain distinct from missing fields.

The [statement transport certificate](../evidence/s1-statement-program.json)
records 254,860 statements across all 17,098 cases, 117 Java tests, 90 Rust tests,
and Clippy. Import reconstructs typed statements without parsing SQL. Detached
S1-to-S2 and the remaining compiler context are still open.

The newer `relation-properties-v1` can reference the expression operator arena;
standalone fragments can still own their aggregate operators. Whole-program
statements, scalar calls, and relational aggregate calls therefore share exact
operator identities. External aggregate declarations retain qualified-name
metadata, parameter and return types, and the linear flag. This newer format is
under separate validation and is not covered by the preceding certificate.

The [shared-operator certificate](../evidence/s1-statement-program-shared.json)
now covers that newer format over all 17,098 cases and 254,860 statements, with
119 Java tests and 91 Rust tests. Both historical statement archives remain
available. A later Java construction test lowers five imported programs without
initializing a SQL frontend; complete S1 context and observational parity remain
open.

The [detached lowering certificate](../evidence/s1-detached-lowering.json) now
compares imported S1 against direct compilation for all 17,098 cases: canonical
S2 circuits, schemas, and diagnostic lists match. `expression-objects-v2`
preserves internal parser positions and typed SEARCH ranges; whole-program
transport leaves expansion to lowering. `statement-program-v2` adds frontend
warning interleaving, with lowerer diagnostics independently regenerated.
The earlier fragments and their SEARCH-expansion measurements remain historical
contracts. This snapshot passes 125 Java tests, 96 Rust tests, and Clippy.

`statement-program-v3` additionally carries typed compiler options,
source text and diagnostic line boundaries, input kind, and pending LATENESS
checks. Unknown or missing option fields fail instead of acquiring defaults.
That context extension has focused coverage and is undergoing full-corpus and
optimizer-prefix validation; the preceding certificate does not cover it.

Current `statement-program-v5` also retains relative table-scan creation order
as a permutation of scan relation IDs. Java's combined table-scan provenance is
ordered by creation ID; arbitrary topological reconstruction changed two TPC-H
`dataflow.json` arrays despite matching circuit observations and hash inputs.
Both readers reject repeated, missing, and non-scan identities. Scans are built
in the transported order, then other relations follow dependency order. Calcite
rewrites can replace a node's inputs with newer nodes, so original creation
order is not generally topological. Full-prefix validation remains ongoing.
