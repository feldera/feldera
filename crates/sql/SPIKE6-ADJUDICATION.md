# Independent adjudication of spike 6

**The bounded spike experiment is complete with a negative result: its 95%
success criterion is not met.** The declared first-inner-stage measurement has
**9 C7-equal occurrences out of 155 (5.81%)**. The complete four-stage shape
census remains distinct from this selected-stage execution measurement. No
single global four-stage C7 percentage is claimed.

PLAN §12 asks for a shape list and an FRel prototype on the top clusters, then
defines success as at least 95% occurrence-weighted C7 equivalence through the
S1 importer. Its outcome table and §11.1 explicitly allow a negative result:
raise the decorrelation estimate and risk 2's probability to High. The
[source-bound ledger](evidence/correlation-coverage.json) and
[estimate proposal](DECORRELATION-ESTIMATE.md) record that result without changing
the threshold or the accepted budget. This is independent agent adjudication,
not human maintainer acceptance or approval of a new estimate.

## Measured populations

| Population | Count | Disposition |
|---|---:|---|
| Full classic census | 8,571 cases | Four optimizer stages observed; schemas and diagnostics checked |
| Related cases across those stages | 157 | All identities and retained-stage summaries preserved |
| First-inner-stage selection | 60 cases, 75 invocations | Fixed boundary before `Decorrelate inner queries 1` |
| Correlates at that boundary | 155 occurrences | The weighted denominator, including failures and refusals |
| C7-equal | 9 occurrences, 7 original cases | Exact typed input and final recipe match runtime witnesses apart from labels |
| Rewritten without credited corpus C7 | 6 occurrences | Reconstructed successfully; not counted as equal |
| Refused | 138 occurrences in 59 invocations | Exact reasons retained |
| Failed transport | 2 occurrences in one invocation | Unsupported transient RexSubQuery retained |
| Other related cases | 97 | Outside the selected boundary; no positive execution credit inferred |

The correspondence between the census and exported invocations is checked using
case identity and the frozen four-callback order. Typed Correlate signatures
(join kind, correlation identity, required fields, and child kinds) match each
transported plan. Each occurrence retains its case, stage sequence, relation
identity, row type, disposition, and runtime witness. All 75 invocations remain
in the ledger; the denominator is not the 74 transportable fragments.

The seven credited original cases are the two `PivotTests.unpivotTests/4,5`
variants; `ReduceExpressionsTests.correlatedFilterOverValues/0`, with all three
correlated views; and `FoodmartTests.testScalar/0..3`, the original MIN and COUNT
queries at both optimization settings. The source wrappers preserve their SQL,
options, input preparation, and expected results. Every selected invocation of
each credited case executes through the Rust replacement. No partly supported
case is credited as a complete success.

Both the typed fragments and emitted recipes used in those runtime runs match
the final measured corpus versions exactly except for their `id` labels. The
Java adapter reconstructs explicit Rust-produced nodes, verifies the root
contract, and rejects surviving Correlates/free correlation variables. Unsupported
Rust input fails the test; Java does not supply the missing decorrelation.

## Why the prototype obligation is now substantively tested

The initial bounded port covered Project, Filter, and set operations but had no
aggregate rule. The later COUNT/MIN/MAX port implements the pinned Calcite
scalar-aggregate handling and exercises the dominant scalar family, rather than
merely labeling it unimplemented. It preserves distinct domains, original bag
multiplicity, hidden correlation fields, nullable keys, empty-group COUNT
correction, HAVING, and LEFT padding. Grouped or richer aggregate calls remain
explicitly unsupported.

The independent scalar review checks eleven expected SQL bags and five
incremental deltas with a separate SQLite scalar-subquery oracle. That validates
the expected data; the actual Feldera C7 runs separately execute Java and Rust
transformations. The domain fixture run executes 24 generated Rust tests and
74 transactions; the scalar fixture run executes 24 and 80. Original unpivot
runs execute 44 tests/44 transactions, and the selected scalar corpus runs
execute 10/10. The fixtures add no original-corpus weight. Test-only reference
oracles do not replace the actual S1/runtime path.

The final source snapshot passes 23 Rust tests, Clippy with warnings denied, and
format checking. Runtime runs pin their actual binary; later copied source
snapshots are not silently labeled as different binaries that were executed.
The archive retains earlier failed reconstruction/runtime attempts and the
independent reviews, so repairs do not disappear from the evidence.

## Negative result and remaining work

The confirmed fraction is 9/155. Giving every remaining successfully rewritten
occurrence C7 credit would reach only 15/155 (9.68%). Therefore the current
prototype cannot reach 95%, independent of the six missing runtime witnesses.
This supports the negative estimate/risk response now. It does not mean that
those witnesses, the 138 refused occurrences, or the two transport-failed
occurrences are correct by assumption.

The original plan leaves “top clusters” without a numerical cutoff. This review
judges the now-implemented domain and substantive scalar-aggregate experiments,
with representative empty-group/NULL/duplicate/HAVING/retraction C7 results and
original corpus witnesses, sufficient to finish the bounded negative experiment.
A successful spike would still need the stated 95%; the negative path does not
require implementing the complete production decorrelator.

The other 97 related cases and later-stage repeats retain their own identities
and shape evidence. The 5.81% result must not be described as a global rate over
all four stages, and uncorrelated cases must not inflate it. Future work should
retain the same ledger discipline while extending grouped aggregates, call
metadata, nested/join/window/sort forms, hints, and transport support.

The remaining production capability and other P0 obligations are tracked in
the [P0 checklist](P0.md#exit-checklist).
