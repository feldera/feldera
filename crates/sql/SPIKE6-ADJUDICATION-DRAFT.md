> Historical pre-scalar assessment. Superseded by [the final spike 6 adjudication](SPIKE6-ADJUDICATION.md) after the scalar port, C7 runs and complete occurrence ledger. Its original findings remain below.

# Draft independent adjudication of spike 6

**Spike 6 is not complete.** The shape census and bounded prototype provide
useful negative evidence, and the current prototype's coverage ceiling is
already below 95%. The unfinished C7 accounting and dominant-cluster prototype
work must remain distinct from that estimate warning. This is a read-only,
independent agent assessment, not approval to weaken the threshold or replace
the existing spike design.

## Exact criterion and negative outcome

PLAN §12 requires both a shape list and a prototype on FRel for the top clusters.
It defines coverage as equal C7 results through the S1 importer, weighted by
occurrence, with 95% or more as the success threshold. Sections 11.1 and 12's
outcome table explicitly provide a negative path: coverage below 95% raises the
decorrelation estimate and risk 2's probability to High. They do not require
implementing a production decorrelator before a negative spike can be recorded.

A **completed negative measurement** would therefore consist of the declared
shape population, a substantive prototype of its dominant shapes, an auditable
occurrence-to-C7 outcome table including refusals and missing oracles, and the
recorded estimate/risk response. It would say that the experiment is complete
and the 95% success criterion failed. It would not say the decorrelator passed.
Merely selecting a small convenient subset, refusing the dominant family without
attempting its rewrite, and running its fixtures is not the promised experiment
on the top clusters.

## Evidence read

The existing reports establish:

| Unit | Count | Meaning |
|---|---:|---|
| Classic corpus cases | 8,571 | Full captured input population |
| Cases with retained correlation/subquery observations | 157 | Across the four observed stages |
| First-inner-decorrelation invocations | 75 | In 60 cases; includes one incomplete stage |
| Correlate occurrences before that stage | 155 | Candidate occurrence denominator for this fixed stage |
| Transported plan fragments | 74 | One further fragment fails transport on RexSubQuery |
| Rust-rewritten fragments | 5 | Four cases; one Correlate removed in each fragment |
| Refused fragments | 69 | 65 structural-family refusals, three hint refusals, one foreign/nested correlation refusal |
| Reconstructed accepted fragments | 5 | Decoder acceptance, not C7 |

The five accepted identities are `ComplexQueriesTest.testCrossApply/0`,
`PivotTests.unpivotTests/4`, `PivotTests.unpivotTests/5`, and two invocations of
`ReduceExpressionsTests.correlatedFilterOverValues/0`. The last case also has a
refused aggregate fragment. A passing fragment does not make that whole case a
passing Rust transformation. The compile-only case has no original C7 oracle.
The eleven adversarial fixtures exercise correctness, but add no original
corpus weight to the numerator.

The raw report files read are `$SCRATCH/p0-correlation-cases-v1.json`,
`$SCRATCH/p0-correlation-plans-v2/selection.json`,
`$SCRATCH/p0-correlation-plans-v2/java/complete.json`,
`$SCRATCH/p0-correlate-corpus-rewrite-v3/{recipes.jsonl,complete.json}`, and
`$SCRATCH/p0-correlate-corpus-reconstruction-v3/complete.json`. Runtime work is still
in progress; this assessment does not assign passes to it.

## What the current numbers decide

For the declared first-inner-stage population, the current implementation can
cover at most **5/155 = 3.23%** of occurrences, even if every accepted fragment
later gets suitable passing C7 evidence. Actual credited coverage may be lower:
missing runtime oracles, partial-case refusals, and multiple invocations must be
resolved. This is a conservative ceiling for the current bounded implementation,
not a measured passing C7 rate or a forecast of a fuller port.

That ceiling suffices to start the §11.1 estimate/risk response now. Waiting for
all fixture results cannot make this implementation reach 95%. It does not,
however, discharge the separate obligation to prototype the top clusters and
finish the occurrence accounting. The 95% threshold remains unchanged. Do not
use 5/74 (fragments), 4/60 (cases), or a ratio containing the thousands of
uncorrelated cases as the required weighted C7 result.

The three largest first-inner-stage coarse clusters are:

| Correlate shape | Occurrences |
|---|---:|
| INNER, Project / Project | 55 |
| LEFT, TableScan / Aggregate | 51 |
| LEFT, Project / Project | 23 |

Together these represent 129/155 occurrences (83.23%). Coarse root kinds hide
right-subtree requirements, so the 55 Project/Project occurrences must not all
be credited to the five accepted recipes. The scalar-aggregate family accounts
for at least the 51 TableScan/Aggregate occurrences and is explicitly refused by
the current port. Its empty-group/COUNT and LEFT-padding semantics are central
to the stated risk; refusal by an implementation that has no aggregate rule is
not a measurement of that rule's feasibility.

The plan does not specify how many clusters “top clusters” means. Requiring a
substantive attempt on this 51-occurrence family is this reviewer's application
of that design requirement, not a new numerical gate stated in the plan. A
documented alternative that addresses the dominant observed requirements could
also satisfy it; silently treating every missing rule as an exhausted experiment
would not.

## Work needed to finish the measurement

1. Freeze an occurrence ledger keyed by case, stage invocation, and relation
   identity. Reconcile all 75 selected invocations and 155 occurrences, including
   the failed transport, with the richer four-stage census. Keep intentional
   retained UNNEST forms and later-stage repeats explicit. Do not sum the same
   correlation's appearances across stages into independent coverage.
2. Complete and retain C7 results for the accepted original corpus cases and
   the adversarial fixtures, including duplicate retractions. Credit only
   matching typed results/errors produced by Rust recipes through the S1
   importer; Java must not supply missing decorrelation. Mark compile-only and
   partial-refusal cases unverified or unsupported instead of successful.
   A compile-only case can acquire independently specified C7 vectors; the
   absence of an original runtime oracle is not a permanent exclusion.
3. Refine the dominant Project/Project and Project/Aggregate populations by
   actual subtree requirements. Make a substantive prototype attempt for the
   dominant scalar-aggregate family, with at least COUNT empty-group, NULL-key,
   duplicate-key, and LEFT-padding cases. A bounded failure or concrete source/
   transport obstacle can be a measured negative outcome; an unimplemented
   aggregate dispatch arm alone is still unfinished prototype work. The plan
   does not require filling every refusal or reaching 95% to record a negative
   result.
4. Publish the occurrence-weighted result or explicit interval with each
   unresolved oracle separated, and record a concrete revised decorrelation
   scope/estimate proposal plus risk 2 = High. Estimate acceptance and staffing
   remain separate decisions. If top-cluster work is deliberately dropped,
   record that as a scope decision, not as an already satisfied original design.

No extra full-production runtime gate is proposed. Existing controlled fixtures,
original corpus cases with usable C7 oracles, explicit refusal outcomes, and
source-backed prototype findings can finish the experiment. The outstanding
work is the experiment already described in the plan.

Reviewed report SHA-256 values:

- `p0-correlation-cases-v1.json`: `129965ca9971198a08601c80fb18b517fbe361e712e8c5693a983ba562476f4a`.
- `p0-correlation-plans-v2/selection.json`: `2d62e40f9cd27ca7e1f6f35f29c6bad3b96d875b326dd6a0dd761230ef0cea5f`.
- `p0-correlation-plans-v2/java/complete.json`: `f50ae3d3c10d66e3f3b2831a54a3bd8265999937b0a814c9d6a36bf44c0e5685`.
- `p0-correlate-corpus-rewrite-v3/recipes.jsonl`: `9cd7e98f1c631913782835bf410aa3901288fc97d65b4959c9a1c0a07cf0a877`.
- `p0-correlate-corpus-rewrite-v3/complete.json`: `2ee668aaf95a2f3d0ffc89ae193bde8aec3104775d54366b8b2085d381967093`.
- `p0-correlate-corpus-reconstruction-v3/complete.json`: `359d1803b9fe9c15381f6103ded1b9c1997921bffce756a4e8b816d7edf7a524`.
