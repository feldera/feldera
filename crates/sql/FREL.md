# Relational model boundary inventory

This source audit supplies requirements for the P0b FRel/FRex/type schema.
The [type/source contract](sql-rel-model/README.md) has Rust definitions, generated
strict schemas and bounded graph/literal verifiers. Ordered compound type transport
passes every collected optimized-view type root in both exported corpus populations.
Literal transport now also covers their values after the lowering's SEARCH expansion.
The [local-expression transport evidence](evidence/s1-local-expression-fields.json)
covers all local Rex roots in both exported populations, including extracted
window aggregate calls and typed SQL function bodies. The later version-5
whole-program adapter transports those contracts through the complete optimizer
prefix on the exported corpus. The accepted production format and the
out-of-corpus forms remain open. The
[inventory](evidence/frel-source-inventory.json) pins 66 source files: every
configured rule implementation's source, the lowering visitors, and the main
converter, decorrelator, field trimmer, builder and simplifier. Its 85 referenced
classes include utilities and visitors; that number is not a count of IR variants.
Imports establish a dependency to inspect, not proof that a rule produces a node.

## Ownership and representation

`sql-rel-model` owns data shared by both binder candidates, rule passes and
lowering. It may depend on `sql-types` and serialization support. It must not
import Calcite, sqlparser, DataFusion, DBSP, files, processes, or a mutable compiler
service. Adapters translate external representations at their own boundaries.
Do not use SQL strings or an untyped JSON extension bag as a substitute for a
missing model variant.

Use distinct IDs for relational nodes, expressions, SQL types, bindings,
catalog objects and source records. Their owning arenas make lifetime and
sharing explicit. IDs identify objects within one compilation; their numeric
values do not determine semantic equality or persistent pipeline IDs. Traversal
order and ordered fields must survive serialization. Rebuilding a shared node
as two equal nodes is observable in the current oracle.

Keep bound input slots separate from names used in diagnostics. Each expression
object needs its complete result type and intrinsic parser coordinates, even
when a parent could infer them; each use supplies its source and evaluation
context. Each relation needs its ordered output row type and the correlation scope
in which its expressions are valid. Source absence is explicit; an invented
zero-width position is not equivalent to an absent position.

## Expression objects and evaluation environments

The fragment codec's per-expression scope is not a whole-program ownership
rule. A census of both exported optimized-view populations finds **725 original
RexInputRef objects shared across distinct input environments in 117 cases per
engine**. Two consumers of the same ordered input producers count as one
environment; Window's appended constant slots and SQL-function parameters have
separate environments. The [sharing census](evidence/s1-expression-sharing.json) observes original Java
objects before SEARCH expansion. It does not certify transport or whole-program S1.

The full model must separate an expression object's intrinsic data from each
place that evaluates it. An input reference stores its field ordinal and exact
type; its use supplies the ordered input environment. A shared expression may
have several uses under different environments without being cloned or interned.
Row-type equality cannot establish environment identity: two different table or
relation inputs can have equal row types. Lambda declaration/capture identity,
correlation bindings, function parameters and Window constant slots also need
explicit environments.

Source coordinates and source context require the same distinction. A RexCall
stores parser coordinates; the source text may come from a statement or from a
function's generated query. Reusing an expression object must preserve its
coordinates and each use's source context rather than choosing the first source
encountered.

Validation must check each distinct expression/environment pair. Bound the number
of validation states and context depth explicitly; otherwise two individually
bounded arenas can force a Cartesian-product traversal beyond the verifier's
resource budget. The eventual
Java adapter must construct one Rex object per expression ID while checking all
its uses against their environments. The existing fragment codec remains useful
within its measured boundary; attaching its single-scope header unchanged to a
whole-program arena would violate this requirement.

## Lowering input

The current relational dispatch accepts these 17 Java classes. Equivalent Rust
variants can share an implementation, but must retain the distinctions below.

| Java forms | Data and constraints that the model must preserve |
|---|---|
| `LogicalTableScan`, `JdbcTableScan` | Resolved catalog object, ordered row type, and scan origin. The lowering distinguishes JDBC scans. |
| `LogicalProject` | Input, ordered expressions, output field names and types. Duplicate expressions remain duplicate output fields. |
| `LogicalFilter` | Input and typed predicate. Its placement above a window matters for TopK recognition. |
| `LogicalValues` | Complete row type and ordered rows, including an empty relation and typed NULLs. |
| `LogicalUnion`, `LogicalIntersect`, `LogicalMinus` | Ordered inputs and ALL/DISTINCT. Preserve the original set operation; a membership join does not preserve duplicate counts. |
| `LogicalJoin` | Ordered left/right inputs, join kind and condition. SEMI/ANTI are transient and cannot cross this boundary. |
| `LogicalAsofJoin` | Ordinary join condition and match condition, their types and operand order, and the ASOF join kind. |
| `LogicalAggregate` | Group keys, grouping sets, ordered calls, DISTINCT, FILTER, arguments, orderings, output types and names. |
| `LogicalWindow` | Input, constants, ordered groups and calls, partition/order keys, frame mode, bounds and exclusions. Constants participate in input-slot numbering. |
| `LogicalSort` | Ordered collation fields with direction and NULL ordering, optional offset and fetch expressions. |
| `Uncollect`, `Collect` | Input, collection shape and element type, field mapping, and ordinality where present. |
| `LogicalTableFunctionScan` | Resolved function identity, typed call, ordered relational inputs and output row type. |
| `LogicalCorrelate` | Correlation binding and required fields. Lowering accepts only INNER UNNEST shapes recognized by `isImplementableCorrelate`, including its optional outer Project or Filter. Other correlations must be eliminated or rejected. |

The seven expression visitor entry points are `RexInputRef`,
`RexCorrelVariable`, `RexLiteral`, `RexFieldAccess`, `RexCall`, `RexLambdaRef`
and `RexLambda`. A call requires resolved operator identity and overload
information, not just its display name. Lambda and correlation references need
distinct binding identities. A field access retains field index and diagnostic
name. A window input reference can address its separate constant list. Extracted
`Window.RexWinAggCall` objects additionally retain ordinal, DISTINCT and IGNORE
NULLS. They are owned by Window group descriptors and cannot be flattened into
ordinary calls without retaining their specialized fields.

Whole-program S1 retains Sarg as typed, ordered disjoint ranges, including open
and closed endpoints, unbounded endpoints, and its three-way null policy.
Lowering expands SEARCH at its original point: eager expansion and Boolean
flattening during transport changed the S2 expression structure in both TPC-H
corpus cases. The earlier scoped fragments still require expansion. Neither
format may stringify a Sarg. Literal encodings must preserve exact
decimal digits and scale, binary data, non-finite floats and signed zero,
interval qualifiers, temporal precision, typed NULLs, collections, UUIDs,
VARIANT representation, and supported geometry data. JSON numbers alone cannot
preserve those distinctions.

`TypeCompiler` handles signed and unsigned integer families, decimal, floating
point, CHAR/VARCHAR, BINARY/VARBINARY, NULL, ARRAY, MAP, interval qualifiers,
GEOMETRY, TIMESTAMP, TIMESTAMP_TZ, DATE, TIME, UUID and VARIANT. Struct types also
take an earlier conversion path. Preserve nested nullability, ordered field
names, named-type identity, precision/scale, character metadata, interval bounds
and temporal flavor. A common runtime representation does not make two SQL
types interchangeable. SYMBOL, ANY and UNKNOWN require explicit phase rules;
their presence in a switch is not evidence that arbitrary values can lower.

## Transient forms

The lowering list is insufficient for the rule pipeline. The audited sources
also reference the following forms and associated state:

| Family | Required representation or disposition |
|---|---|
| `LogicalCalc`, `RexProgram`, `RexLocalRef` | Ordered expression program, local slots, projection and optional condition. Local-reference expansion must preserve types and scope. |
| `RexSubQuery`, general `LogicalCorrelate`, SEMI/ANTI joins | Subquery kind, relational input, operands, correlation bindings, required columns, cardinality and NULL semantics. Rewrites must retain ALL/DISTINCT set-operation provenance. |
| `RexOver`, `RexWindow`, bounds, exclusions and field collations | Window expressions before extraction into `LogicalWindow`, including unbounded/current/offset bounds and NULL ordering. |
| `RexDynamicParam`, `RexRangeRef`, `RexTableInputRef` | Separate parameter/range/table provenance identities. They require elimination or an explicit rejection before lowering if no lowering visitor consumes them. |
| `LogicalSnapshot`, `LogicalMatch`, `RexPatternFieldRef` | Converter references exist. Inventory the accepted grammar paths and reject unsupported forms explicitly; do not silently omit them from the schema because they are absent from positive tests. |
| `LogicalTableModify` | Intermediate DML representation before a `TableModifyStatement`; retain target columns, operation and source mapping. |
| Trait sets, hints, correlation sets and builder stack state | Classify each field as semantic, derivable, or deliberately unsupported. Diagnostic `RelJsonWriter` drops hints and cannot serve as a lossless S1 exporter. |

A lowering verifier must check node kinds, binding scopes, input-slot bounds,
row and expression types, set-operation provenance and allowed correlate shape.
Unknown variants, unresolved references, cycles outside an explicitly modeled
recursive construct, and incomplete metadata fail before circuit construction.
Rule passes need a less restrictive verifier for their declared transient forms.

## Ordered statements and remaining validation

The dispatch consumes ten statement classes: CreateView, CreateTable, DropTable,
TableModify, CreateType, CreateFunction, DeclareView, CreateIndex,
CreateAggregate and SetOption. The experimental program schema carries ordered
catalog effects, resolved identifiers, source ranges and declarations. Table and
view metadata includes defaults, lateness, keys, properties, original view SQL
and column positions. Statement order and submission boundaries affect name
resolution and diagnostics and cannot be sorted during comparison.

Table modifications now capture ordered target selectors as immutable statement
metadata before lowering, including resolved spelling/quoting, source ranges and
diagnostic spelling. The lowering no longer rereads INSERT/REMOVE parser nodes to
recover column order. Null selectors mean implicit table order; explicit selectors
retain their order. The test-only relational observer runs synchronously at each
statement's actual lowering entry. Type declarations can precede earlier source
statements because the compiler performs a declaration pass; a replay stream must
preserve that observed execution order as well as original submission boundaries.
The current statement adapter serializes all ten forms and reconstructs their
typed objects without parsing the diagnostic SQL text. Catalog snapshots freeze
mutable metadata at each actual lowering entry; declarations, scalar calls and
aggregate calls share an operator arena. Frontend diagnostics are replayed at
typed boundaries while the lowerer regenerates its own messages.

The tagged machine-readable schema and bounded Rust validator reject unknown
fields and variants. The [detached lowering certificate](evidence/s1-detached-lowering.json)
matches S2 graphs, schemas and diagnostics across all 17,098 exported positive
cases, with options and source supplied by a companion input in that frozen
version. Current version 5 transports those fields and relative scan creation
order too. Its [complete-prefix certificate](evidence/s1-prefix.json) matches
all 17,098 exported programs and 7,030,934 observations, including byte-equal
backend artifacts, hash inputs and persistent IDs. Independent maintainer review,
transient-form dispositions, multiple compilation batches, JDBC adapters and
complete join system fields remain open. Failed binding is not an accepted S1 program;
negative parser/binder behavior needs its separate C0 oracle.

The entries below retain the scope and limitations of earlier fragment snapshots.


The [local-field traversal correction](evidence/s1-rel-field-walk.json) replaces
`RelNode.accept(RexShuttle)` with an explicit allow-list for the seventeen classes
above. Generic visitation skips VALUES and several aggregate/window fields,
including ASOF match conditions. The widened captures preserve all previous
literal value/type signatures and add 15,763 literal objects. This improves the
fragment census; it does not transport relational descriptors or statements.


The [general expression-fragment codec](evidence/s1-expression-codec.json) adds
arbitrary arena ordering, explicit lambda declaration nodes and contextual
export. It has constructor-graph parity, not full-corpus FRex parity. Source-bearing
calls and specialized Rex subclasses fail explicitly. Correlation binding IDs
must be connected to relational declarations before whole-program S1 can pass.


The [context run](evidence/s1-expression-context.json) preserves generated partial
positions and exact character input types. A project-only 240-case prefix passes
cross-language transport. The attempted full shard stops at a named type
constructor, `SqlTypeConstructorFunction`, demonstrating the need for explicit
catalog/declaration identity rather than a builtin display-name fallback. The
prefix is not a full FRex, relation or statement transport certificate.


The [operator-object extension](evidence/s1-constructor-objects.json) transports
`SqlTypeConstructorFunction` and fresh `SqlRowOperator` instances explicitly,
including declared types and private field-name lists. Operator-object IDs are
separate from instantiated call signatures. The 527-case project-only prefix
passes cross-language transport; the full shard stops next on a SQL-defined
`ExternalFunction` body. Supporting that body requires a combined operator and
expression dependency graph with isolated parameter scopes. Independent nested
expression tables would lose sharing and cannot certify whole-program S1.
