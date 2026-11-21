# Independent source review of the correlation exporter

The optional observer and catalog-backed exporter fit the disposable spike 6
architecture. Production leaves the observer null; the optimizer owns the plan,
and the test helper snapshots it synchronously before mutation. The exporter
reuses the existing S1 expression, relation, and catalog encoders and immediately
checks decoding with a fresh decoder cluster. It does not introduce a second
production IR or retain planner-owned relational objects across stages.

This is an independent agent source review using the good-architecture skill.
It does not certify a Rust decorrelator, runtime equivalence, or measured
coverage. The reviewed tests include a correlated scalar aggregate with catalog
transport and an uncorrelated control. The broader observation tests cover
nested subqueries, stage ordering, UNNEST, and schema/diagnostic checks; this
review did not rerun Maven.

Two boundaries matter for the upcoming weighted coverage result:

1. `CorrelationPlanProbe` selects plans before `Decorrelate inner queries 1`
   using `CalciteOptimizer.containsCorrelate`. That predicate follows RelNode
   inputs, not relational plans nested inside `RexSubQuery`. It also does not
   select correlations created only at a later stage. The richer
   `CorrelationProbe` census follows nested subqueries and must supply the
   denominator. Every relevant source case and stage occurrence needs an
   exported, unsupported, intentionally retained, or otherwise explained
   disposition. A percentage computed only over successfully exported fragments
   would omit failed or ineligible shapes.
2. `all_inputs_transported` currently means that no selected encode/decode call
   reported an `IOException`. A case with no eligible stage or fragment can
   still satisfy that flag. It does not establish that every source case had a
   transportable correlate plan. Coverage should retain observed stage and
   eligible-plan counts, then reconcile them with the census. An explicit name
   such as `all_selected_plans_transported` would describe the existing flag.

The exporter correctly retains encode/decode failures with invocation indices
and continues collecting later invocations. Other unexpected runtime exceptions
abort the batch before its completion record. These are preferable to silently
removing failed inputs from the evidence. Decoding establishes that the selected
wire form is accepted; it does not alone prove semantic equality or C7.

One additional input-integrity gap should be closed before consuming export
IDs as coverage identities: the command does not reject empty or duplicate
input `id` values. The shared compile helper validates SQL submissions but not
IDs. A controlled input manifest can establish uniqueness externally, but the
consumer must check that invariant; otherwise fragments from repeated IDs can
collide. A focused rejection test would be useful alongside the positive
transport tests.

The existing optional-observer review in `ARCHITECTURE-REVIEW.md` already
covers callback lifetime and exception behavior. This review adds the exporter
and denominator limitations; it does not reopen the production rule order or
require a production-scale execution gate.

Reviewed source hashes:

- `sql-to-dbsp-compiler/SQL-compiler/src/main/java/org/dbsp/sqlCompiler/compiler/frontend/calciteCompiler/optimizer/CalciteOptimizer.java`: `c0d0e667b41de2a51502bbea53dd524716ff74b421b7f4d48f24a2c62124b8ac`.
- `sql-to-dbsp-compiler/SQL-compiler/src/test/java/org/dbsp/sqlCompiler/compiler/sql/tools/CorrelationPlanProbe.java`: `2761c1555578a125e7cbc65647218cf44df5f1aca6082b8f8acc2d0bdc740bbb`.
- `sql-to-dbsp-compiler/SQL-compiler/src/test/java/org/dbsp/sqlCompiler/compiler/sql/tools/CorrelationPlanProbeTests.java`: `dfbf5f1cc116573641e67d9fea7524fd134fd0ab5a0d990e7c2b22d21c60d327`.
- `sql-to-dbsp-compiler/SQL-compiler/src/test/java/org/dbsp/sqlCompiler/compiler/sql/tools/CorrelationProbe.java`: `b42ec1846bfe7990eed8c05d7d704fd9ab7a69d4133bbebc5b3521f39d66587a`.
- `sql-to-dbsp-compiler/SQL-compiler/src/test/java/org/dbsp/sqlCompiler/compiler/sql/tools/CorrelationProbeTests.java`: `5d1c840609e47344d2642923f4303df018b5608fa9a8d1aa8988f30a438e369f`.
- `sql-to-dbsp-compiler/SQL-compiler/src/test/java/org/dbsp/sqlCompiler/compiler/sql/tools/S1RelNodes.java`: `3a240d1f9decd2c1dc264e8e7e8446eca71401903ccbeadae8f032e9f3eb0d2f`.
- `sql-to-dbsp-compiler/SQL-compiler/src/test/java/org/dbsp/sqlCompiler/compiler/sql/tools/S1ExpressionCorpusProbe.java`: `026d78c6d8b69dfb67cafb263810adf83aafe45b0127d4c49821a2f4af83d88a`.
