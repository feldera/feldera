# Independent P0 attribution source review

An independent Codex review agent inspected the local P0 source and attribution
evidence on 2026-11-18. This review found one stale inventory and repaired it.
The checks below found no missing licence header or notice in the explicitly
identified translations and vendored parser. This is a source audit, not legal
approval, maintainer acceptance, or a complete provenance audit. PLAN.md section
9.2's independent legal review remains open.

## Finding and repair

**The parser inventory described an obsolete dependency graph.** `LICENSING.md`
said no parser implementation was vendored. The current
[`parser-probe/Cargo.toml`](parser-probe/Cargo.toml) depends on
`vendor/sqlparser`, and the locked graph also adds `sqlparser_derive` through
the visitor feature. The earlier 25-package evidence omitted these two packages.
An approving reviewer relying on that document would miss modified third-party
source and its additional dependency.

`LICENSING.md` now describes the vendored package and links its provenance,
patch description, and current 27-package inventory. The earlier inventory is
retained as historical evidence. No source licence, attribution, or compiler
behavior changed during this review.

## Verified scope

The [machine-readable inventory](evidence/licensing-source-review.json) pins
the current manifests, lockfile, notices, translated Rust source, and vendor
patch. The checks used `cargo metadata --offline --locked --format-version 1`
in the devcontainer and read local package files without compiling code.

| Check | Result |
|---|---|
| Registry sqlparser 0.64.0 archive | SHA-256 `92e0d273b4f8ddd14a35ed36862acbc9cc96cec578e36ee1881a9d42e0e0f2c8`, equal to the lockfile checksum |
| All 50 files in that archive against the vendored tree | 41 byte-identical; nine modified; none missing |
| All nine modified parser files | Original and patched digests equal `FELDERA-PROVENANCE.json`; retained Apache header and explicit modification notice |
| Vendored `LICENSE.TXT` and `NOTICE.TXT` | Byte-identical to the registry archive |
| Parser dependency graph | All 27 packages declare a licence; all 26 third-party package entries have licence or notice files inventoried |
| `unicode-ident` | Full `(MIT OR Apache-2.0) AND Unicode-3.0` declaration retained |
| Eight Java files and parser template in `calcite-attribution.json` | All nine current digests match the inventory and retain ASF headers |
| Calcite resource `LICENSE` and `NOTICE` | Digests match the pinned inventory; the copies in `sql-rel-model` are byte-identical |
| `sql-rel-model/src/relation_metadata.rs` | ASF header, pinned Calcite revision, source class names, and Feldera adaptation note present |
| `multiset-probe/src/prune.rs` | VMware copyright and MIT marker identify the port; full permission notice retained in `LICENSE-VMWARE` |

The review also searched Java and Rust sources for explicit copied, adapted,
cloned, and translated-source descriptions. For example,
`CustomFunctions.CalciteFunctionClone` delegates to an existing operator's public
inference and checker methods; the word “clone” alone does not establish a source
copy. This search cannot establish the provenance of every implementation.

## Remaining review obligations

The current source inventory does not validate a final production distribution.
The compiler-JAR notice check in `calcite-attribution.json` is historical;
this review did not assemble a new JAR, Rust package, or container image.
The final distributable must be checked against the dependencies and translated
files it actually contains. Package metadata alone does not establish licence
compatibility or fulfil distribution obligations.

The appointed licensing reviewer still needs to assess source provenance beyond
explicit attribution comments, the chosen distribution artifacts, and future
Calcite or other third-party translations. No Materialize source was identified
in the reviewed experiment, but keyword searches cannot prove absence of an
unattributed copy. The plan's exact-revision and review requirements continue to
apply before such reuse. These open obligations prevent this source review from
closing the P0b legal gate.
