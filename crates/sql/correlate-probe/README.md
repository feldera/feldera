# P0 decorrelation experiment

This disposable crate ports a bounded subset of Calcite's `RelDecorrelator` at
`4bf922bbbc1cbd570a2f201339a46a2d8f7a763b`. It consumes validated catalog-backed
FRel fragments captured before `Decorrelate inner queries 1`. It emits explicit
relational constructors and expression remappings for the Java S1 adapter.
The Java adapter must not run a decorrelator to produce a successful recipe.

The implementation preserves Calcite's distinct value-generator domain, payload
field maps and hidden correlation fields through Project, Filter and set
operations. The final INNER or LEFT join matches domain fields with `IS NOT
DISTINCT FROM`; original SQL predicates keep their original operators. LEFT
padding happens after the right projection. Scalar COUNT, MIN, MAX, SUM, AVG and SINGLE_VALUE group by
the complete correlation domain, then restore missing domain rows before any
parent HAVING or projection. COUNT receives a typed zero. The other supported scalar aggregates retain
NULL. A single ordinary GROUP BY set adds the correlation domain to its
keys and preserves empty groups for the final join. An ordered FETCH with a positive integral limit uses per-domain
ROW_NUMBER, preserving sort direction and NULL ordering. The bounded
port also handles unordered LIMIT 1 and ARRAY_AGG over ordered FETCH.
Multiple grouping sets, other aggregate functions or modifiers, general
windows and sorts, nested right
correlations, unknown function determinism and shared relations with multiple
evaluation environments are explicit refusals.

`rewrite.rs` owns the frame and expression transformations;
`rewrite/aggregate.rs` handles scalar empty-group semantics; `recipe.rs` defines
the constructor contract.
The library validates input before accessing arenas. The CLI handles bounded
files, identities and reports. An unsuccessful fragment has no partial recipe.
Constructing a recipe is not evidence of runtime equivalence. Spike 6 coverage
must include the complete case/stage census, failed transports and refusals,
and count only cases whose Rust transformation passes C7 through the S1 importer.

Schema expansion has separate limits for field slots and copied field-name
bytes, including cached frame copies. Internal hidden names avoid visible
payload names. Set branches share one schema and use explicit nullable widening
instead of retagging an input expression. Only alias hints with a preserved
original relation are accepted; optimizer hints remain unsupported.

The source reference is Apache Calcite's
`core/src/main/java/org/apache/calcite/sql2rel/RelDecorrelator.java`: the
value-generator, Project, Filter, SetOp, Correlate, scalar Aggregate repair and
field-access rules.
The Apache header, license and notice accompany this adaptation. This is not a
production optimizer or the full port required by the later front-end phase.
