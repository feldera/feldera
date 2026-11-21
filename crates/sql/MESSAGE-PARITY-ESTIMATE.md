# Message parity: measured scope and proposed estimate

The measured compiler catalog contains **252 required message-body roots**:
240 from the Java capture and 12 additional roots from the Python compiler
expectations. Either count exceeds PLAN §11.1's 150-template trigger. Row 9
therefore needs a new estimate. This document proposes a transparent adjustment;
it does not replace the accepted baseline or record human approval.

The independent agent review is supported by
[the Java certificate](evidence/diagnostic-reviewed-catalog.json) and
[the compiler/Python/platform extension](evidence/diagnostic-scope-extension.json).
The counting rule separates message-body roots from formatter dependencies,
fragments, class names, source snippets, absence checks, and stability checks.
It counts required roots with assertion witnesses, rather than every emitted
message or every plausible source candidate.

## Scope measured

| Population | Observed contract | Accounting |
|---|---|---|
| Java shared helpers | 1,333 assertions at 375 sites | 228 body roots |
| Java direct assertions | 382 assertions at 42 sites | 12 additional roots; 26 formatter fragments remain components |
| Python compiler | 212 body assertions and 5 class assertions | 12 additional roots; compiler total 252 |
| Python runtime | 61 registered expectations, independently executed | 10 repository body roots and one delegated Rust remainder formatter; six shared wrappers and dependency formatters separate |
| Python unregistered | 3 assignments | Explicitly excluded from active requirements |
| Platform SQL error | Exact SDK exception contract | Existing ObjectNotFound root; actual SDK formatting replay passes, including class, snippet, and carets |
| Java runtime | 232 exported contracts in 71 tests, with 95 distinct substrings | Source test/predicate inventory complete for the selected v9 export; emitting template identities unresolved |

The compiler total comprises 187 source-expression roots, 43 Calcite resource
formats, and 22 reviewed formatter roots. The Python runtime report is
[separate](PYTHON-RUNTIME-TEMPLATES.md): adding all of its wrappers and formatter
dependencies to 252 would mix counting units. The platform replay executes the
actual SDK constructor and compilation-wait method against captured Java output
and local responses. It does not claim HTTP transport, provisioning, or a
successful deployed platform test.

## Proposed adjustment

PLAN §10 currently assigns message parity 5–8 engineer-months by hand or 3–4
with LLM assistance. It does not state a template count or measured productivity
behind those ranges. In particular, **150 is a review trigger, not a measured
baseline scope**. The following calculation is a sensitivity analysis, with an
explicitly chosen 150-root reference, rather than measured engineer throughput.

Let `f` be the share of the original effort that scales with template count.
For the 252 measured compiler roots, the multiplier is
`(1 - f) + f * (252 / 150)`.

| Assumed variable effort share | Multiplier | By hand, engineer-months | LLM assisted, engineer-months |
|---|---:|---:|---:|
| 50% | 1.34 | 6.70–10.72 | 4.02–5.36 |
| 75% | 1.51 | 7.55–12.08 | 4.53–6.04 |
| 100% | 1.68 | 8.40–13.44 | 5.04–6.72 |

**Proposal:** use 8–14 engineer-months by hand or 5–7 with LLM assistance as a
provisional planning range for the currently identified compiler-message scope.
This rounds the fully variable sensitivity to whole months. It leaves the
unresolved Java runtime attribution and any resulting additional message-parity
work explicitly unpriced. These are not complete-phase upper bounds. No actual
human hours, implementation rate, staffing capacity, or acceptance is inferred
from agent execution time or assertion counts.

Before accepting a replacement for row 9, the Track F lead should calibrate the
fixed renderer/transport work separately from template-specific work and price
any additional roots selected by the Java runtime source analysis. Existing
runtime implementations may be retained, but their source-location and panic
wrappers still need parity accounting; neither zero extra work nor a complete
runtime rewrite follows from the current evidence. Rows 17, 19, and 20 already
cover reviews, corpus upkeep, and upstream drift; their effort must not be added
again under row 9 without an explicit scope change.

## Exact remaining accounting work

The v9 corpus exporter records 232 `expected_runtime_error` contracts. The
source inventory retains each SQL program, typed circuit, test method, options,
input order, exact expected substring, and original producer identifiers.
`TestCase.createTesterCode` turns these strings into Rust `should_panic(expected)`
assertions. `SqlIoTest.qf` commonly registers optimized and unoptimized cases,
which explains why cases, tests, and distinct strings differ.

All 232 exports say `runtime_verified: false`. That is an execution status,
not a demand to execute all 232 before counting. Source analysis can resolve a
contract by following its typed circuit to the selected runtime helper and its
actual error formatter, retaining delegated formatting and wrapper dependencies.
Broad fragments such as `overflow`, `Cannot convert`, and `out of range` do not
identify a unique root. The inventory therefore leaves 232 contract identities
unresolved instead of reporting 95 templates or silently excluding them.

A targeted follow-up source search also separates class-only assertions,
snapshot equality, internal decoder failures, compiler invariant failures, and
documentation checks from SQL diagnostic bodies; see
[the independent review](DIAGNOSTIC-REVIEW.md). This search is a bounded audit,
not proof that every possible assertion style has been discovered.

The concrete spike 9 result is consequently a **252-root compiler count**, a
separate accounted Python runtime scope, an exact platform formatting replay,
and a finite unresolved Java runtime contract inventory. The >150 decision is
settled. A single whole-suite template count and approval of a replacement
estimate remain unclaimed.
