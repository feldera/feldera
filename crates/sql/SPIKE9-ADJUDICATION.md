# Independent adjudication of spike 9

**The spike 9 measurement criterion is satisfied, and the required estimate
review has started.** This is an independent agent adjudication of the evidence
against the existing plan. It is not human maintainer acceptance, legal review,
or approval of a replacement effort baseline.

PLAN §12 asks for a count and says that more than 150 starts a new estimate of
row 9. The witnessed compiler catalog contains **252 body roots**, not a count
of incidental messages or unresolved source candidates. The
[scope certificate](evidence/diagnostic-scope-extension.json) preserves that
count, the declared population, source attribution, component dependencies,
negative dispositions, raw evidence, and reproducible validation. The
[estimate proposal](MESSAGE-PARITY-ESTIMATE.md) records a concrete range and its
assumptions. The accepted §10 baseline remains unchanged pending its separate
review.

## Scope interpretation

The plan's first sentence, “How many message templates do the tests depend on?”,
is broad. The surrounding requirements identify the purpose and boundary:

- The §6 “Messages” row describes preserving Calcite/compiler wording in a
  compiler message catalog, citing Java negative tests, Python `expected_error`
  assignments, and the platform SQL-error assertion.
- Spike 9 requires distinguishing compiler errors from runtime errors. A
  runtime expectation must receive a disposition; it must not be counted as a
  compiler message merely because its program compiled.
- Section 8.1 lists `qf`, `runtimeFail`, and related exports as the **runtime
  oracle**, separately from compile-time negative tests and their message
  catalog. C7 compares observable runtime errors; C12 covers native runtime
  errors and panic translation.
- The dependency inventory retains `feldera_sqllib` and the runtime libraries.
  Reimplementing every retained runtime formatter is not part of the compiler
  message-catalog task.

Accordingly, the indispensable result is a measured compiler catalog with all
specified populations accounted for, plus separation of retained-runtime
contracts and renderer obligations. A single number that sums compiler roots,
Rust panic bodies, SDK framing, and formatting dependencies would mix counting
units and would be less informative for row 9.

## Required populations and dispositions

| Requirement | Evidence and disposition |
|---|---|
| Java errors and warnings, exact targets | 1,333 shared-helper assertions at 375 sites, including warning assertions; rendered targets and field attribution retained |
| Direct field, list, prefix, absence, and snapshot assertions | 382 assertions at 42 sites; positive body requirements separated from component fragments, non-body fields, absence, and stability |
| Source-backed compiler roots | 240 Java roots, extended by 12 Python roots to 252; 187 source expressions, 43 Calcite resource formats, and 22 reviewed formatter roots |
| All 281 Python assignments | 212 compiler-body contracts, five compiler-class contracts, 61 separately executed and attributed runtime contracts, and three unregistered assignments |
| Separate platform assertion | Frozen compiler probe plus the actual SDK formatting methods reproduce the exact expected exception, including snippets and carets; existing `ObjectNotFound` body adds no new root |
| Suppressed and incidental diagnostics | Excluded from required roots unless an assertion independently witnesses the body; visibility and rendered-target metadata checked |
| Disabled and skipped tests | Three unregistered Python assignments excluded explicitly; v9 Surefire's 71 skips retain reasons (47 corpus-only Rust skips, 23 ignored tests, one configured generator) |
| Follow-up source search | Remaining matches classified as class/verdict checks, snapshot equality, decoder errors, compiler invariants, or documentation errors; no additional positive SQL body contract found within the stated search |
| Runtime corpus exports | 232 contracts in 71 tests, 95 distinct expected substrings; source methods, predicates, typed circuits, producer IDs, and execution status retained separately |

The catalog does not claim that a bounded source search proves the absence of
all conceivable assertion styles. Its inventory, captured sites, exclusions,
and follow-up search are concrete and reviewable; no known unaccounted positive
compiler-body assertion remains in that reviewed population. Future newly found
assertions should extend the catalog rather than retroactively change its
recorded capture population.

## What remains, and why it does not reopen the measured criterion

The 232 Java runtime contracts still lack per-contract emitting-template
identities. They are a precise remaining source population: the v9
`expected_runtime_error` exports, with `runtime_verified: false` in that capture.
Their 95 substrings must not be reported as 95 templates. Source analysis of the
retained typed circuits and selected sqllib helpers can deduplicate their roots
without first executing all 232 cases.

That work can refine runtime-wrapper costs and later C7/C12 coverage. It is
**not indispensable to spike 9's compiler template count or its >150 decision**.
The retained-runtime bodies have not been silently counted as zero, removed
from the corpus, or declared parity-complete. The Python runtime attribution
also keeps its ten repository body roots, one delegated formatter, and six
wrappers separate from the 252 compiler roots.

The platform formatting contract is measured; HTTP provisioning and deployed
platform execution remain later integration work, not an added spike 9 gate.
Human review of the estimate, ownership/capacity decisions, and licensing
acceptance remain separate phase-exit obligations. Marking spike 9's measurement
complete does not close those obligations or P0 as a whole.
