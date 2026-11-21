# Independent review of the bounded domain decorrelator

The proposed distinct-domain rewrite matches the structure of the pinned
Calcite `RelDecorrelator` for Project, Filter, SetOp and INNER/LEFT Correlate.
Its correctness depends on the invariants below. This review does not establish
C7 coverage or the 95% spike-6 threshold. The current transported population is
74 fragments from 60 selected cases, with one separately retained RexSubQuery
transport failure; transport is not decorrelation or execution.

Source reviewed: `RelDecorrelator.java`, SHA-256
`111b0ab303f7d636452a1500ac854d99ed7a496f6fd01d4c14365cd1b3f42b2f`.
Relevant methods are `createValueGenerator` (line 1439), Project (1372), Filter
(1841), SetOp (1278), Correlate (1890), and `buildCorDefJoinConditions` (4199).
Line numbers identify this exact source snapshot, not a moving upstream file.
The port should retain its ASF header/change note and the pinned NOTICE.

## Required semantic invariants

1. **Domain identity is not the SQL predicate.** The generator is DISTINCT over
   exactly the supplied correlated left fields, and generator alignment uses
   `IS NOT DISTINCT FROM`. Keep the original right predicate intact after
   rewriting its references. Replacing `R.k = L.k` with null-safe equality makes
   NULL rows match incorrectly. Conversely, dropping NULL domain keys loses
   valid constant or `IS NULL` projections.

2. **Preserve bag multiplicity.** Only the generator is deduplicated. The final
   join reads the original left bag; the transformed right is evaluated once per
   distinct domain, and each original left occurrence multiplies its right bag.
   The incremental domain must remain present until the last duplicate left row
   is retracted. The generator and final left must use the same rewritten left
   relation, with the same field mapping.

3. **A frame must retain every supplied domain distinction.** Each frame tracks
   original payload positions separately from correlation-field positions.
   Project appends those hidden fields even when every visible expression is a
   constant, duplicates another expression, or projects a correlation field.
   If a common domain `D(k,p)` feeds a branch using only `k`, it must still carry
   both hidden fields, or begin with a separately deduplicated `D(k)`.
   Dropping `p` from a two-row domain `(1,10),(1,20)` while retaining both `k=1`
   rows, then joining back on `k`, squares the multiplicity.

4. **Set operations partition by the domain.** Align every branch to a common
   ordered domain and payload schema, including branches with no correlation or
   only a subset of the fields. Project exactly common correlation fields plus
   original payload before UNION/INTERSECT/EXCEPT. Preserve `all`; duplicate and
   NULL equality follow each original set operator. A branch-specific hidden
   field must not accidentally participate in DISTINCT or bag subtraction.
   Calcite's SetOp method explicitly uses the original payload width and each
   child's `oldToNewOutputs` mapping.

5. **LEFT padding happens after right projections.** Keep constant and non-strict
   right expressions below the final LEFT join. For `LEFT JOIN LATERAL
   (SELECT 7 FROM R WHERE R.k=L.k)`, an absent right match produces NULL, not 7.
   Final right fields have the original Correlate's nullable output types. A
   nullable visible payload is not an existence marker. If future support
   introduces scalar aggregates, Calcite's extra empty-group/LEFT handling must
   be ported; grouping by domain alone loses empty groups and COUNT zeroes.

6. **Correlation identity and use context are part of every mapping.** Map
   `(correlation identity, field)` to positions. Do not infer identity from field
   indices or names. `ExpressionObjects::RelationUse` explicitly permits one
   shared relation to have multiple environments; shared expression IDs also
   retain distinct input/source/correlation contexts. Memoization keyed only by
   RelId or ExprId is insufficient. Include the environment and frame mapping,
   or reject such sharing in this bounded prototype.

7. **Reject unsupported scopes before treating a subtree as opaque.** Scan every
   expression and descendant, including uses under unknown operators, for free
   correlations. Reject foreign enclosing correlations, correlated Aggregate,
   Window, Sort/limit, nested-right Correlate and unsupported expression forms
   until their rules are ported. A correlated reference hidden below an otherwise
   independent-looking node is not an independent subtree. A zero-column Values
   containing one tuple differs from Values containing no tuples. Never silently
   leave a correlation for Java to repair.

8. **Factorization requires a repeatability policy.** An external/volatile call
   evaluated once per distinct key can differ from evaluation once per original
   left row. The current `OperatorObject` model does not encode determinism.
   Establish determinism from the pinned builtin registry and refuse unknown
   external functions or unsupported calls; do not assume that a validated
   operator object is deterministic. This is a conservative prototype boundary,
   not a claim that Calcite applies this particular guard in the cited methods.

The rule output must also preserve names, types, nullability and expression
origins, validate all new ordinals and environments, and obey a bounded output
node/expression budget. Reusing the original plan's input validation does not
validate newly generated recipe indices. Domain reuse should be a shared DAG,
not unbounded subtree duplication at every SetOp branch.

## Adversarial execution fixtures

[`decorrelation-domain-review-fixtures.json`](decorrelation-domain-review-fixtures.json)
contains eleven SQL fixtures with explicit weighted inputs and outputs:
ordinary versus null-safe equality, duplicates on both sides, LEFT constant
projection, completely empty right/left inputs, projection without FROM,
branch-specific key subsets, UNION DISTINCT, INTERSECT ALL, EXCEPT ALL and an
uncorrelated UNION branch. It also specifies two retractions of one duplicate
left row to exercise the distinct-domain threshold. All eleven now compile and
export one Correlate fragment each. The v3 Rust recipes reconstruct through the
strict Java adapter. These structural checks do not establish C7 execution; the
expected bags remain independently specified oracles.

The branch-subset and LEFT-constant examples should be run early. Then check
reordering/duplicating visible columns, sparse required-column positions, at
least two correlation fields with different NULL masks, and shared expression
objects used under different environments. Unsupported tests should assert a
located refusal for an aggregate, per-domain LIMIT, nested enclosing reference,
volatile/external function and the retained transient RexSubQuery form.

## Coverage accounting

Java may reconstruct and lower a Rust-produced recipe through the S1 importer.
It must not run its decorrelator to supply the missing transformation and count
that as Rust coverage. Inspect the resulting recipe for residual free
correlations before import, retain exact case/stage/occurrence identities, and
compare full ordered transaction deltas. A partially transformed plan with an
unsupported remaining Correlate is not a passing Rust case merely because one
node was removed. Keep intentional UNNEST forms and failed transports in their
explicit populations; do not silently shrink the denominator to the subset the
prototype accepts. The 95% claim requires the defined occurrence weighting and
C7 execution, not structural removal counts or successful Java reconstruction.

## Concrete prototype review and repairs

The first `correlate-probe/src/rewrite.rs` implementation keeps every supplied
domain column through Project and Set frames, satisfying the branch-subset
multiplicity invariant. It rejects multiple relation-use environments and uses
an explicit standard-operator repeatability allow-list. Its first version refused correlated aggregate/window/sort/nested rules. The
subsequent bounded scalar aggregate port and its evidence are recorded below;
other aggregate forms remain refused.

The review repaired these concrete boundaries:

- A structurally validated FRel fragment can still declare inconsistent Filter,
  SetOp or Correlate output widths. The model leaves relational output coercions
  to construction. Zipping payload positions with declared columns silently
  truncated projections. Explicit width checks and checked input lookups now
  refuse these fragments.
- An expression environment can bind a correlation without its selected root
  having a supplying Correlate ancestor. Root traversal now refuses such free
  correlations; only dependent traversal can rewrite them.
- Rebuilding a parent emitted an unused Original subtree containing a Correlate.
  Rebuilt parents now emit only the required Rebuild node.
- Blanket hint refusal excluded normal SQL aliases. Alias-only hints are now
  preserved on Scan/Values leaves and Project/Filter/Set nodes. Project records
  its original relation when one exists; Filter always records its original
  relation. Generated projections carry no original relation. Correlate aliases,
  unsupported-node hints and optimizer hints remain refused. The source basis is
  `FelderaSqlToRelConverter.convertFrom`, which attaches FROM aliases, and
  `RewriteJoinAnnotations`, which consumes aliases with actual join hints.
- Traversal limits alone did not bound repeated cloning of wide schemas and
  mappings. A cumulative expansion budget now charges schema fields, copied
  payload/correlation maps, cached frames, dependency sets and expression slots
  before allocation: at most 100,000 logical entries and 8 MiB of copied field
  names. Traversal separately allows 100,000 visits and depth 128. These limits
  bound logical expansion, not measured resident memory; allocation overhead is
  not an RSS guarantee.
- The v2 Java reconstruction attempt caught duplicate hidden projection names in
  a real corpus fragment. Internal names are now deterministic and unique after
  reserving visible payload names; Set branches share one common naming policy.
  Final root names retain the original contract. Duplicate visible payload names
  that this policy cannot preserve are refused.
- The same attempt caught a nonnullable integer UNION branch feeding a nullable
  output. Input expressions now retain their actual child types; an explicit
  `WidenNullability` expression permits only identical value types with top-level
  nullability changing from false to true. Narrowing and value-type changes are
  refused. The strict Java decoder independently validates this operation.

Sixteen [focused regressions](correlate-probe/tests/review.rs) pass in the Linux
devcontainer, with offline locked dependencies and two build jobs. Clippy passes
for all targets with warnings denied. The tests include validated-but-malformed
widths, unsupported/free correlations, preserved alias metadata, shared wide
schemas and cached frames exhausting the expansion budget, long field names,
hidden-name collisions, the exact nullable UNION fragment, and forbidden casts.
At this stage, the original MAX-containing `correlatedFilterOverValues` fixture
was unsupported; its explicitly modified Project variant supplied only a
unit-test fixture. The later scalar port executes the original MAX case, as
described below.

The final algorithm and recipe source were formatted before these checks. A
later workspace `cargo fmt --all` also formatted CLI/library files; the runtime
run pins the earlier frozen v3 binary and its exact preformat source snapshot.
No throughput or memory measurements were performed for this review.

## Initial frozen structural result and runtime adjudication

The v3 evidence is in `$SCRATCH/p0-correlate-shape-review-v3` in the devcontainer:
`tests.log`, `clippy.log`, `build.log`, the frozen executable and a 50-source
`snapshot.json`. Executable SHA-256:
`51784e2b0cd09c0123a5653cae66141235b9e1c234bb976a44f36bc56675b50b`.
The earlier v1/v2 attempts and their failed reconstruction logs remain evidence;
they are not retrospectively relabeled as passing.

All 11 synthetic fragments produce recipes. Of the 74 transported corpus
fragments, five produce recipes and 69 are refused: 65 unsupported correlated
forms, three unsupported-node hints, and one foreign correlation. There are no
budget refusals in these selected inputs. Strict Java reconstruction passes all
11 synthetic and five accepted corpus recipes, recorded in
`$SCRATCH/p0-correlate-fixture-reconstruction-v3/complete.json` and
`$SCRATCH/p0-correlate-corpus-reconstruction-v3/complete.json`.

The five accepted corpus fragments are not five passing C7 cases:

- `ComplexQueriesTest#testCrossApply/0/correlation-stage/1` belongs to a
  compile-only test and has no execution oracle.
- `PivotTests#unpivotTests/4/correlation-stage/24` and `/5/correlation-stage/24`
  are the optimized and unoptimized variants of the same third SQL query in
  `unpivotTests`. They are two corpus case IDs, one SQL query.
- `ReduceExpressionsTests#correlatedFilterOverValues/0/correlation-stage/1` and
  `/3` belong to one case with additional refused aggregate-bearing fragments.
  They did not establish complete coverage of that case at this stage; the later
  scalar port executes all three views together.

The runtime harness executes the frozen Rust process on the exact pre-stage S1
fragment, requires one matching successful recipe, reconstructs through the
strict adapter, and preserves the original root type. A refusal throws before
Calcite's optimizer-stage error recovery. The adapter rejects residual Correlate
nodes and free correlation expressions, so the subsequent ordinary Java stage
cannot supply a missing correlated transformation and conceal it as Rust work.
The baseline uses the unchanged Java compiler. Both sides compare against the
specified expected bags, not merely against each other.

The synthetic suite registers 22 baseline/Rust circuits with three transactions
each (empty, insertion, full retraction), plus two duplicate-retraction circuits
with four transactions each. The corpus suite invokes the original
`PivotTests.unpivotTests` method, preserving its inputs, expected outputs and
compiler options; only its two correlated variants can contribute Rust C7
coverage. Uncorrelated queries executed by the same method are additional checks,
not newly covered Correlate cases.

The first runtime attempt, `$SCRATCH/p0-correlation-runtime-v1/tests.log`, failed:
empty expected tables included a trailing blank row rejected by the table parser,
and the combined test queue mixed incremental/ignore-order settings. The printed
`Executed 44` counter increments while generating tests and is not evidence that
44 Rust tests ran successfully. The repair strips trailing whitespace from the
expected-table string and separates corpus and synthetic classes, retaining the
original corpus settings. The corrected runs pass:

- `$SCRATCH/p0-correlation-runtime-v2/tests.log`: two JUnit tests, zero failures,
  errors or skips; 24 generated Rust tests. Independent inspection of preserved
  `generated/lib.rs` confirms 74 runtime `transaction().unwrap()` calls and 74
  `must_equal` output assertions, using three workers per circuit. Twelve retained
  successful Rust recipes cover the eleven fixtures plus the duplicate-retraction
  extension, each removing one Correlate.
- `$SCRATCH/p0-correlation-corpus-runtime-v1/tests.log`: one JUnit test, zero failures,
  errors or skips; 44 generated Rust tests with 44 transactions and 44 output
  assertions. These are 22 original query/optimization cases run once through
  the baseline and once with the Rust hook. Exactly two Rust recipes occur,
  `unpivot/compiler-4/invocation-24` and `compiler-5/invocation-24`, matching the
  two previously identified corpus variants. Only those two variants contribute
  complete selected-corpus C7 cases.

Unlike the failed first attempt, both successful Maven runs reach and return
from `Utilities.compileAndTestRust`; `skipRust` and automatic retry are false.
The utility throws for a nonzero Cargo child exit, so success here establishes
actual Cargo execution. Successful child output is redirected to a temporary file
and deleted by the existing harness; the retained Maven/Surefire results and
exact generated Rust establish the execution path and checks. No claim is made
that a raw successful Cargo transcript was retained.

This establishes C7 for the eleven specified synthetic queries and their
retraction extension, and two selected corpus variants of one SQL query. It does
not establish the spike-6 95% occurrence-weighted threshold: the broader
correlated population, refused forms, failed transport and partially transformed
case remained in scope for this version.


## Bounded scalar aggregate port and independent review

The next increment ports the scalar subset of the same pinned Calcite source:
`decorrelateRel(Aggregate)` (line 644) and `rewriteScalarAggregate` (line 947).
The implementation is in
[`rewrite/aggregate.rs`](correlate-probe/src/rewrite/aggregate.rs), separate from
the traversal and expression remapping code. It accepts exactly one empty
grouping set and ordinary COUNT with zero or one argument, or MIN/MAX with one
argument. DISTINCT, filters, direct arguments, ordering, approximate calls,
IGNORE NULLS, distinct keys, other aggregates and nonempty grouping keys remain
refused. General grouped and scalar aggregation is not claimed.

The child projection places the complete domain first, followed by payload
fields. This makes Calcite's sorted group positions and the Rust correlation
mapping agree. Original aggregate arguments map through the child's payload;
operator identities and accepted call metadata remain original. Java clones the
original calls with `adaptTo`, checking the SQL result type independently while
allowing its inferred top-level nullability to change with grouping.

Grouping by the full hidden domain loses empty right groups. A LEFT join from the
complete domain restores one aggregate result per domain before any parent
Project or HAVING filter. Hidden keys come from that left domain, including
unmatched NULL keys. An explicit outer-field expression verifies the actual
nullable join field. Only COUNT defaults to a typed, nonnullable signed BIGINT
zero; MIN/MAX retain NULL. The final Correlate replacement joins to the original
left bag, preserving duplicate multiplicity. The independent review found no
substantive defect in these mappings, type checks or charged allocations.

Four additional [independent refusal tests](correlate-probe/tests/scalar_review.rs)
first validate their FRel inputs, then check rejection of a COUNT declared as
INTEGER rather than BIGINT, excess MIN/MAX arguments, zero grouping sets, and
unsupported SUM. The frozen `$SCRATCH/p0-correlate-final-v3` source run passes all
23 Rust tests (19 existing plus four independent), Clippy with warnings denied,
and formatting checks. `tests.log`, `clippy.log` and `fmt.log` record these
results. This is the final source check; runtime runs retain their own exact
producer snapshots instead of being attributed to a later rebuild.

[`decorrelation-scalar-review-fixtures.json`](decorrelation-scalar-review-fixtures.json)
adds eleven independent weighted integer fixtures: scalar INNER/LEFT behavior,
empty right/left, COUNT versus COUNT(arg), all-NULL groups, nullable and
nonnullable MIN/MAX inputs, NULL-safe keys, two correlated fields with different
NULL masks, HAVING zero/positive predicates, and duplicates. Its extension removes
the last right row while two duplicate left rows remain, verifies replacement by
a zero/NULL aggregate result, retracts both left copies, inserts a right row
while the domain is absent, and reintroduces the left row. Equivalent SQLite
3.54.0 scalar subqueries independently confirm all eleven expected bags and five
incremental deltas. That oracle check is separate from Feldera execution.

Both scalar runs pin `$SCRATCH/p0-correlate-scalar-v1/feldera-correlate-probe`,
SHA-256 `b72133d23ece2327115948713d80dd7f9978704b040acb7ad60fcd65365e8524`.
The independent `$SCRATCH/p0-scalar-independent-review.json` records the matching
Surefire outcomes, runtime binary and generated-source hashes. The scalar runtime
evidence passes independently inspected generated checks:

- `$SCRATCH/p0-correlation-scalar-runtime-v1`: two JUnit tests with no failures,
  errors or skips; 24 Rust tests, 80 runtime transactions and 80 output
  assertions. Eleven paired circuits run three transactions each; the paired
  extension runs seven each. Twelve successful Rust recipes remove one
  Correlate each. The preserved generated Rust SHA-256 is
  `bbd987cc00542d2883111d9ec771f26c2500cd9ab3b268eb972b1535e78e9d5d`.
- `$SCRATCH/p0-correlation-scalar-corpus-runtime-v1`: two JUnit tests with no
  failures, errors or skips; ten Rust tests, ten transactions and fourteen output
  assertions. The preserved generated Rust SHA-256 is
  `227ff7bffd156841c42bd913445fac88c40b0cb25687e51dc2ecaade95325231`.
  Only the matching `CorrelationScalarCorpusRuntimeTests` Surefire report
  belongs to this run; the copied directory also contains older unrelated
  reports, which do not increase these counts.

`CorrelationScalarCorpusRuntimeTests` calls the original
`ReduceExpressionsTests.correlatedFilterOverValues` method unchanged. Its Rust
compiler setup mirrors `BaseSQLTests.getCCS(String)` and inserts the hook before
compilation. Both baseline and Rust executions use all three original view
oracles in one transaction, and all three correlation stages are rewritten.
The earlier partially supported case is therefore now a complete selected
corpus C7 case.

The same harness calls the original `FoodmartTests.testScalar` and forwards only
its first four `compare` invocations, preserving their SQL, expected strings,
input preparation, row count, whitespace policy and optimization flags. Because
`qst` uses both optimization modes, these are the first two SQL queries (MIN and
COUNT), each optimized and unoptimized: original case IDs `/0` through `/3`.
Subsequent grouped/count/SUM cases in that method are explicitly not executed or
credited by this selection. The seven successful scalar corpus recipes comprise
three ReduceExpressions views and four Foodmart variants.

With the scalar port, the same transported population produces 15 recipes out of
74 fragments, all accepted recipes reconstructing through strict Java checks.
The remaining 59 refusals are 30 non-scalar grouping forms, 15 unsupported scalar
call forms, ten unsupported correlated relation forms, three hint-policy
refusals and one foreign correlation. Results are retained in
`$SCRATCH/p0-correlate-scalar-corpus-v1` and
`$SCRATCH/p0-correlate-scalar-corpus-reconstruction-v1`. Successful reconstruction of
other accepted fragments adds no runtime coverage by itself.

Together, the executed selected corpus now contains seven case IDs: two Pivot
variants, one complete ReduceExpressions case, and four Foodmart variants. They
exercise nine correlated occurrences. Synthetic fixtures remain outside the
corpus occurrence denominator. The occurrence ledger determines the weighted
spike-6 percentage against the complete retained population; this bounded port
still does not establish the required 95% threshold.
