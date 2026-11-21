# P0 attribution inventory

This records source and dependency evidence for the independent P0b review.
It is not that review's approval. The reference is Calcite commit
`4bf922bbbc1cbd570a2f201339a46a2d8f7a763b`.

## Existing Calcite-derived source

Nine files now retain an ASF header: eight Java files and the parser extension
template. Their paths and digests are recorded in
[`evidence/calcite-attribution.json`](evidence/calcite-attribution.json).

Three files explicitly identified their copied source but lacked its header:

| Feldera file | Pinned Calcite source | Feldera adaptation |
|---|---|---|
| `ValuesReduceRule.java` | `rel/rules/ValuesReduceRule.java` | Uses Feldera expression reduction and rule configuration |
| `GenericLambdaTypeChecker.java` | `sql/type/OperandTypes.java`, `LambdaRelOperandTypeChecker` | Accepts caller-supplied argument types and revalidates inferred expressions |
| `SqlAttributeDefinition.java` | `sql/ddl/SqlAttributeDefinition.java` | Implements three-operand construction without collation operands |

The missing headers are restored, with the adaptations described in each file.
This changes attribution comments, not compiler behavior.

The pinned Calcite `LICENSE` and `NOTICE` are copied unchanged into
`SQL-compiler/src/main/resources/META-INF/feldera-calcite/`. Maven's resource
copy preserves their bytes in `target/classes`. Inspection of the assembled
compiler JAR on 2026-11-13 also confirms byte-identical LICENSE and NOTICE entries;
the JAR and entry digests are retained in the attribution evidence.
The November 19 offline assembly verifies that the latest compiler JAR
contains one byte-identical entry for each file. The certificate records the
compiler source and JAR hashes. Other dependency notices remain separate.
[Packaged attribution evidence](evidence/p0-java-packaging.json)

## Parser experiment

The standalone experiment pins registry sqlparser 0.62.0 and 0.64.0 and a
separate vendored 0.64.0 package. All three manifests declare Apache-2.0.
The modified parser sources retain their licence headers and carry Feldera
modification notices. The package retains
the upstream `LICENSE.TXT` and `NOTICE.TXT`. Its
[provenance](parser-probe/vendor/sqlparser/FELDERA-PROVENANCE.json) and
[patch description](parser-probe/vendor/sqlparser/FELDERA-PATCHES.md) record
the source changes.

[`evidence/parser-dependency-licenses.json`](evidence/parser-dependency-licenses.json)
records all 25 packages in the earlier locked metadata graph, their declared licences,
manifest hashes, and distributed licence-file hashes. Every package has a
declaration. This includes the Unicode-3.0 component of `unicode-ident`'s
declaration; it must not be flattened to an MIT/Apache choice. The inventory is
for the experiment, not a production SBOM. The
[2026-11-18 source review](LICENSING-REVIEW.md) adds a
[source inventory](evidence/licensing-source-review.json) of 27 packages,
including the vendored parser and `sqlparser_derive`. That review compared all
50 files in the pinned upstream archive. At the time, 41 were byte-identical
and nine matched the recorded modifications. The current provenance includes
the later grammar changes and documentation edits.

The parser-only profile still resolves 27 packages. Enabling the binder
feature resolves 44, including the shared FRel model and fold evaluator.
The updated inventory records the manifest and licence-file hashes for each
package and retains the Apache-2.0 or BSL-1.0 declaration for `ryu`.
[Parser and binder dependency inventory](evidence/parser-binder-license-inventory.json)

The dead-code pass now includes recursive regions in
[`multiset-probe/src/decode/reachability.rs`](multiset-probe/src/decode/reachability.rs).
The port retains the VMware copyright and MIT notice in the crate.
The [Rust optimizer](sql-opt/src/dead_code.rs) implements the reachability
rules of Java's `FindDeadCode` for passes that preserve table sources.
The optimizer crate carries the [VMware notice](sql-opt/LICENSE-VMWARE).

## Native DBSP test dependencies

The standalone native DBSP probe tests storage and checkpoint compatibility
against DBSP. DBSP requires `tokio-rustls`. Its storage crate enables the
HTTP and cloud features of `object_store`, which depends on `reqwest`.
The lockfile therefore includes `rustls` and records `ring` as a dependency
of `rustls-webpki` and `quinn-proto`. The lockfile also covers optional and
target-specific dependencies. These entries alone do not show which provider
a test binary links.

The exception applies only to this test workspace and the recorded direct
parents of each crate. A probe feature cannot disable the required DBSP
dependencies. The crypto check rejects additional parents and checks all
other SQL lockfiles, including the vendored parser.

## Type-derivation experiment

The disposable `type-derivation-probe` crate implements bounded rules from
Calcite commit `4bf922bbbc1cbd570a2f201339a46a2d8f7a763b`. Its
[`translated.rs`](type-derivation-probe/src/translated.rs) and
[`extras.rs`](type-derivation-probe/src/extras.rs) retain ASF headers and
Feldera change notices. The crate carries the pinned Apache-2.0
[`LICENSE`](type-derivation-probe/LICENSE) and [`NOTICE`](type-derivation-probe/NOTICE).
Both files are byte-identical to the pinned Calcite resources retained in the
compiler distribution inventory.
The [method-level source inventory](type-derivation-probe/README.md) identifies
operand, return-type, decimal, and common-type rules and the omitted context.

Feldera's `SqlToRelCompiler.TYPE_SYSTEM`, `CalciteFunctions`, and
`CustomFunctions.CalciteFunctionClone` also determine the behavior. The latter
includes the selected ABS clone; matching Calcite's type formula does not make
that clone the same operator object. These sources and exact hashes belong in
the frozen evidence alongside Calcite references. The independent
[type-rule review](TYPE-DERIVATION-REVIEW.md) distinguishes inherited formulas,
selected operators, and the actual binding boundary. This inventory does not
supply legal approval or approve a production distribution.

## Review still required

The reviewer must assess the provenance inventory beyond files with explicit
copy comments, the assembled distribution and notices, and the planned Calcite
translations. The relational metadata validator now implements Calcite's trait-alternative
ordering rules in `sql-rel-model/src/relation_metadata.rs`. It retains an ASF
header and a change note identifying the pinned revision and source classes
(`RelCollationImpl` and `RelDistributions`). The crate includes byte-identical
copies of that revision's LICENSE and NOTICE. Other future translations must
retain the attribution and change notes required by `PLAN.md` section 9.2.

This work has copied no Materialize implementation. Any proposed copy still
needs an exact source revision, its licence, and the plan's review before use.
An independent agent source review is recorded in `LICENSING-REVIEW.md`.
It does not supply the independent legal review required by the plan.
No legal approval or licence exception is recorded here.
