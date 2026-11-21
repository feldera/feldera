# Composite optimizer state

This source audit supplements [PORTING.md](PORTING.md). It records state passed
between nested transforms in the current Java optimizer. It does not authorize
an isolated checkpoint. Every listed observation replays from S2 through the
complete prefix, with one compiler and the original object lifetimes. Independent
review and full-corpus replay evidence remain separate requirements.

Paths below are relative to
`sql-to-dbsp-compiler/SQL-compiler/src/main/java/org/dbsp/sqlCompiler/compiler/visitors/outer`.
The inventory includes subclasses of `Passes` and `Repeat`, their inline
composites, and the three manually instrumented composite transforms. A newly
added transform requires inspection; inheritance alone does not prove that its
state is covered by a circuit dump.

All rows also depend on compiler options, SET state, declarations, source and
diagnostic state. Ordinary `CircuitVisitor` traversal context and
`CircuitCloneVisitor` replacement maps belong to the active visitor. A checkpoint
inside a traversal would need those too; current observations occur after passes.

| Composite / source | State and required lifetime |
|---|---|
| `CircuitOptimizer.java` | One `AnalyzedSet` survives all three `OptimizeProjections` placements. `addWithMultisets` binds an analysis to its consumer. Disabled-pass names and conditional predicates depend on compiler metadata; preserve construction and execution order. |
| `Passes.java`, `Conditional.java` | Ordered pass instances and predicates captured from other analyses. `afterPass` validation and stopping on diagnostics are part of execution. Skipped conditional bodies have no inner observation. |
| `Repeat.java` | Reuses the same transform instance, tracks iteration and convergence against the preceding circuit, and bounds iterations using the original component size. Recreating only the final iteration loses this state. |
| `OptimizeWithGraph.java` | Each iteration runs `Graph`, the consumer bound to its `CircuitGraphs`, and `DeadCode`. A saved circuit cannot substitute for the paired graph object. |
| `DeadCode.java` | `FindDeadCode.toKeep` is consumed by `RemoveOperators`; both use the same operator identities and `keepAllSources` policy. |
| `CSE.java` / `OneCSEPass` | `Graph`, `FindCSE`, and `RemoveCSE` share a canonical-operator map. The enclosing repeat retains their instances and convergence history. |
| `CreateStarJoins.java` | `FindJoinTrees` reads the graph and populates the join-tree map consumed by `ReplaceStarJoins`. |
| `RemoveStarJoins.java` | Repeated `RemoveStarJoinsPass` with the common repeat contract; SET state selects its top-level execution. |
| `FuseExpensiveMaps.java` | `FindFusableMaps` supplies the analysis object to `Fuse`; its operator/function identities must agree with the input circuit. |
| `FuseIntoArrayAgg.java` | Each repeat iteration runs graph analysis, `FindFusable`, `Fuse`, and dead-code removal. Fusion analysis includes closure parameter/reference mappings. |
| `ExpandAggregates.java` | `AppendOnly` supplies the predicate used by `ExpandStreamAggregates`. The compiler weight variable and aggregate cached empty tuples must retain identity; the S2 envelope explicitly restores them. |
| `ExpandAggregateZero.java` | `CollectZeros` and `ExpandAggregateZeros` share the zero-aggregate collection. |
| `keys/LeftJoinChains.java` | `KeyAnalysis` feeds `LeftJoinChainsVisitor`; `DeferCarriedColumns` consumes its `chainEnd` map. |
| `RemoveUselessLeftJoins.java` | Key analysis and its consumer run before dead-code removal on the same identities. |
| `recursive/RecursiveComponents.java` | The first graph feeds view-reference fixup; a fresh second graph feeds nested-operator construction. Validation and left-join substitution follow. The two graphs are not interchangeable. |
| `indexSharing/ShareIndexes.java` | Map-chain discovery feeds collapse. A graph then feeds index duplication; a later rebuilt graph feeds shared-index discovery and replacement. Each phase has its own dead-code cleanup. |
| `windowSharing/ShareWindowIntegrals.java` | Window-group discovery feeds `ImplementWindowGroups`, then dead-code removal. |
| `ExpandIndexedInputs.java` | Indexed-input rewriting precedes graph construction. `FindCommonProjections` consumes that graph and passes its results to replacement, then dead-code removal. |
| `RemoveIdentityOperators.java` | `Graph` feeds `Decide`, whose removal map feeds `RemoveOrReplace`. |
| `FindUnboundedState.java` | Streaming detection, GCed streams, key analysis, bounded ports, and unbounded operators are shared. The same `FindBounded` visitor runs twice for recursive circuits. A final graph feeds reporting. `apply` clears the shared collections only at the composite boundary. |
| `temporal/ImplementNow.java` | `CircuitContainsNow.found` gates subsequent transforms and table removal. Filter breaking repeats. A distinct final NOW scan gates failure reporting; its predicate must not use the earlier scan. |
| `temporal/ReorderTemporalFilters.java` | A graph feeds `Scan`; `Reorder` reads its chosen-conjunct map. Both conditional bodies use the same window-sharing SET policy. |
| `monotonicity/MonotoneAnalyzer.java` | Original/expanded circuits, expansion maps, monotonicity facts, error reachability, and graph rebuilds are detailed in PORTING.md. Limiter insertion returns to the original circuit. |
| `monotonicity/MergeGC.java` | The first graph and equivalent-noop scan feed canonical/removal maps. After noop merging, a second graph and retain scan feed `MergeRetain`. |
| `LowerAsof.java` | Left/right GC maps and ordered original-to-replacement mappings survive lowering, graph rebuilding, dependency sorting, and GC movement. Edge insertion order is observable. |
| `LinearPostprocessRetainKeys.java` | The initial graph discovers artificial waterline edges. Sorting occurs only when edges exist; a rebuilt graph feeds replacement. Observation indices are 0 graph, 1 sort, 2 graph, 3 replacement. |
| `expandCasts/ExpandCasts.java` | Separate safe- and unsafe-cast repeats retain their own visitors and convergence state; metadata-cast expansion follows both. |
| `Gen2Passes.java` | Ordered typed-box removal followed by clone removal; selected only for Gen-2. No additional shared analysis is wired by this constructor. |
| `merkle/MerkleHashes.java` | Multiset facts, unprefixed hashes, delta/GC prefix maps, global hashes, and local hashes share operator identity. Global persistent-id validation precedes local hash assignment. Restore the whole sequence, not serialized hash annotations. |
| `TagRegions.java` | Conflict detection and region removal share the annotation-removal set. |

This is an audit of composite wiring and its state dependencies, not an exhaustive
field-level proof for every visitor. No row claims that an unobserved conditional
branch was tested, that a graph-only S4 file is resumable, or that runtime outputs
were compared. Full-prefix replay and targeted activation cases are needed to
establish those separate properties.

## Executed pass-path evidence

The corpus runner accepts `-Dfeldera.determinism.passCoverage=true` for `capture`
and `replay-s2`. It writes `s4-paths.jsonl` beside the normal case records, with
one record for each successful case. Each record retains the complete nested
container/pass/index path, observation count and largest observed iteration.
Repeat iterations aggregate at the same structural placement; separate placements
remain distinct. The option leaves normal fingerprint records unchanged.

A sidecar alone is incomplete evidence. Require the run's `complete.json`, zero
failed cases, the exact input identities, one sidecar record per case and agreement
between each sidecar count and its S4 fingerprint count. Match complete paths
between direct capture and replay. A path absent from this report is unobserved;
no conditional branch or field-level state coverage is inferred from its absence.
Coverage reports supplement the source inventory and independent review.

The machine-readable [inventory](s4-inventory.json) pins the thirty source-state
rows above. Its selectors link runtime container names to source contracts; the
three manually instrumented composites additionally list exact pass/index points
and activation conditions. Wrapper selectors such as `Repeat` identify shared
machinery and do not prove a particular consumer's state contract. The file marks
its per-point enumeration as incomplete; the remaining composites still need that
level of expansion and independent review.

Audit a completed capture/replay pair and link it to the source inventory:

```sh
python3 crates/sql/scripts/s4_coverage.py DIRECT_DIR REPLAY_S2_DIR coverage.json
python3 crates/sql/scripts/s4_inventory.py crates/sql/s4-inventory.json coverage.json inventory.json
```

`summarize_s4_coverage.py` combines disjoint shards only after revalidating the exact prepared population,
build identity, normal replay results and every sidecar count. The CastTests CI
replay enables path recording and runs both audits. Missing conditional points
remain in the inventory output; matching paths do not certify field-level state.

The [full-population certificate](evidence/s4-full-paths.json) records 8,571
classic and 8,527 exported Gen-2 cases: 3,486,078 and 3,510,660 S4 observations
per side, respectively, over twenty JVMs. Thirty-two separately labeled option
activations add 13,328 observations. Their combined 1,115 structural paths cover
all thirty source-state rows and every explicit manual-composite point.

`S4PassInventory` enumerates the constructed `Passes`/`Repeat`/`Conditional` tree
for sixteen option configurations without running conditional predicates. It
finds 6,008 placements and 1,050 distinct paths under default metadata. All
1,049 returning constructed paths are exercised by the combined corpus and
activation runs. The remaining `Fail` path always throws and cannot emit a
successful post-pass observation. The inventory keeps it in scope, and a Java
regression checks both the throw and observation-path cleanup. Other unobserved
implementations continue to require activation; this is an explicit disposition
for one source-pinned nonreturning implementation.

These counts do not enumerate every metadata-dependent configuration or prove
field-level state completeness. The certificate retains raw Gen-2 byte differences
under seam-v1 while requiring canonical Gen-2 and every artifact-contract field
to match. Independent review and exhaustive per-point field enumeration remain
open.

## Declared fields and manual children, 2026-11-18

The [declaration audit](evidence/s4-fields.json) supplements execution coverage
with 321 distinct field declarations, including 32 static declarations. It walks
the 141 constructed pass implementations, their superclasses and interfaces,
nine ambient compiler classes, and the implementations of all 20 explicit manual
observation points. The six children constructed by `MergeGC` are included too.
Hidden inherited fields retain their declaring class; reflection reads no values
and does not initialize classes or evaluate predicates.

Manual records distinguish transform objects from in-place ordering operations.
They record the error-reachable set, original and expanded circuits, monotonicity
and expansion maps, shared ASOF GC maps, and waterline dependency list. The audit
checks their observation indices against source and rejects an omitted manual
composite. Every implementation and declaration source is pinned, including
implementations with no declared fields. Five Java and five Python tests pass;
the [verified archive](evidence/s4-fields-archive.json) retains 135 source hashes,
five artifact hashes, and three classpath hashes.

This enumerates declarations, not all objects reachable through those fields.
Field lifetimes and metadata-dependent construction still require review. The
records preserve that distinction and authorize only complete-prefix replay.

## Lifetime audit

The [contract ledger](s4-lifetime-contracts.json) classifies all 321 declarations
from the historical field inventory. It links the contracts to 6,008 placements
across 16 constructed configurations, 20 manual points and six nested MergeGC
transforms. The audit verifies the historical source and referenced lines. The
test observer is recorded separately from that declaration population.

Across 32 activation programs, the observer records 13,296 before/after pairs
and 32 in-place points. Baseline and observed runs match all 13,392 prefix
observations per side, including S2 and S3. The sample includes 260 historical
declarations and 111 additional declaring fields reachable through direct
referents or the observer configuration. The remaining 61 historical declarations
retain their source contracts. No sample reached the 32-entry collection limit
or encountered an inaccessible inspected field.

These contracts preserve shared analysis instances, operator identity history
and deferred callables by replaying the complete S2 prefix inside one compiler
context. The [report](evidence/s4-lifetime.json) includes the exact sampling
limits and all declaration IDs absent from the activation run. The
[verified archive](evidence/s4-lifetime-archive.json) retains the raw events,
source, class overlay, commands and comparison summaries.
