# Vendored-parser maintenance estimate

The current reviewed surface has **478 added/deleted vendor lines across eleven
files**, plus **6,220 physical Rust lines in 27 adapter/probe files**, including
tests and the unchanged 0.62 comparison adapter. The full unified patch has
1,019 lines including context. The patch, original and modified hashes are in
`vendor/sqlparser/FELDERA.patch` and `FELDERA-PROVENANCE.json`; the frozen build
and sources are in [the grammar review certificate](../evidence/parser-grammar-review.json).

The historical [inventory](../evidence/parser-maintenance-inventory.json)
retains its earlier five-file, 181-line patch and 3,227-line adapter baseline.
The later work adds typed DDL/expressions, precise field rejection, charset and
literal repair, AST-derived SQL, and the independent Java AST/binding/S1 oracle.
The current candidate accepts 8,470/8,571 programs, with 101 explicit refusals.
All 8,470 accepted programs pass the semantic/S1 oracle; later repairs preserve
the full 8,571 parser records exactly.

## Provisional recurring cost

Budget **4–8 engineer-days per routine upstream upgrade** and **8–15 days when
upstream changes affect our AST forms, parser entry points or span handling**.
These revised ranges are engineering judgment from the larger measured source
surface and added comparison requirements. They are not measured rebase hours,
human staffing commitments or an approved budget.

| Routine upgrade work | Engineer-days |
|---|---:|
| Inspect upstream changes and identify affected grammar/AST fields | 0.5–1 |
| Carry the explicit patch and update the version adapter | 1–2 |
| Audit new AST forms, rejection policy and source positions | 1–2 |
| Run paired populations and classify regressions | 0.5–1 |
| Independent review, documentation, attribution and pin update | 1–2 |
| Total | 4–8 |

For `U` routine upgrades, reserve `4U–8U` engineer-days; price disruptive
upgrades separately. Release cadence is not measured here. The estimate excludes
new grammar features, completing currently unsupported forms, and a DataFusion
AST bridge. D1 must charge that bridge to option H. It also excludes unplanned
upstream security fixes and does not approve a permanent fork.

## Remaining positive-corpus refusals

The baseline's **121 refusals in 8,571 cases (1.41%)** map to 31 diagnostic groups. Source
inspection and a representative failing submission from each group give these
repair directions. They are proposed repairs, not verified implementations.
The inventory retains every group, count and example identity.

| Grammar/representation gap | Cases | Repair direction |
|---|---:|---|
| Qualified datetime differences | 32 | Add an expression node retaining its unit and span in casts, window ordering and projections. |
| Scalar VALUES rows | 29 | Parse unparenthesized rows while retaining row boundaries and distinguishing ROW-valued scalar expressions. |
| Multi-column UNPIVOT | 9 | Preserve grouped inputs, aliases and FOR columns. |
| Unicode escape rules | 8 | Decode after UESCAPE selection; join valid surrogate pairs and handle unrepresentable lone surrogates explicitly. |
| Named ROW fields | 7 | Retain constructor-field aliases, including nested array/subscript contexts. |
| ROW-field nullability | 6 | Preserve field NOT NULL separately from outer-row nullability. |
| CREATE TYPE field nullability | 5 | Preserve both explicit NULL and NOT NULL. |
| BETWEEN SYMMETRIC | 4 | Retain the symmetry modifier as syntax. |
| RLIKE NULL | 4 | Keep the NULL operand distinct from aggregate null-treatment syntax. |
| LIMIT offset,count | 4 | Preserve operand roles when normalizing. |
| GROUP BY DISTINCT | 4 | Retain the grouping quantifier. |
| Nested GROUPING SETS | 2 | Preserve nested, empty and duplicate groups. |
| Wildcard EXCEPT/EXCLUDE | 2 | Disambiguate exclusions from set operators and retain qualified names. |
| Inline foreign key, wildcard exclusion inside ROW, MAP subquery, scalar type alias, double-slash comment | 5 | Add the corresponding scoped grammar and typed representation; preserve original coordinates. |

The current [101-refusal ledger](../evidence/parser-refusal-dispositions.json)
assigns each first refusal to one of 16 grammar gaps with its proposed repair.
The largest group is qualified datetime subtraction, with 42 cases across
projections, casts and window ordering. The ledger pins the exact source
submission and diagnostic. Each repair still needs an AST and binding replay
to find any additional errors in the same submission.

The negative population remains equally important. The current candidate has
zero Rust-only acceptances in 2,136 adversarial cases, and all 988 accepted
adversarial cases pass independent Java AST comparison. Earlier source-bound
results retain 13,660 keyword probes and 1,143 diagnostic assertions. That does not exhaust fields within
accepted AST variants. New upstream fields must be classified explicitly;
successful compilation alone cannot establish a safe rebase.

## Reviewable upgrade procedure

1. Start from the exact upstream archive and retain its checksum, licence and
   notice. Regenerate the complete vendor diff and modified-file hashes.
2. Review every touched parser path and added AST field, including display,
   visitors and spans. Keep the stock comparator on the unmodified registry
   crate so the candidate cannot alter its own baseline.
3. Run Rust tests, Clippy and formatting, then the full positive, adversarial,
   keyword and captured-diagnostic populations against the pinned Java parser.
   Compare previously accepted ASTs as well as acceptance counts.
4. Record parser overacceptance, changed error positions and unsupported forms
   separately. Add minimal regressions for each repaired difference.
5. Run the full independent-JVM S1 oracle with strict Java/Rust transport, plus
   the AST-only accepted-adversarial oracle. Preserve exact type identities and
   all raw evidence. Obtain independent review before changing the pin.

No upstream discussion or submission has occurred. The exhaustive outer AST
policies require review for new fields; they do not prove all nested grammar
contexts. Source inventory, regression populations and independent semantic
comparisons are the measured basis for the candidate recommendation. The cost
estimate and staffing acceptance remain distinct from those measurements.
