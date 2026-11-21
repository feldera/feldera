# Independent review of parser spike 4

This review checks the engineering criterion in PLAN §12 against the current
prototype. It is an agent review, not maintainer acceptance. The 0.64 Feldera
candidate still lacks a demonstrated S1 semantic comparison and a complete
inventory of AST fields outside Feldera's grammar. Its positive-corpus failure
rate passes the numeric threshold. Matching raw parse verdicts alone does not
establish the remaining criterion.

## Confirmed defects and repairs

The independent 60-case field matrix found **32 Rust-only acceptances**. A
12-case follow-up found **four more**. The failures and their original debug
ASTs remain in `$SCRATCH/p0-parser-review-v1` and
`$SCRATCH/p0-parser-review-controls-v1`; they were not overwritten by the repair.

| Area | Demonstrated problem | Repair |
|---|---|---|
| Function arguments | Inline WHERE/LIMIT, ON OVERFLOW, and a JSON null clause on an ordinary function | Recursive clause checks retain ordinary ORDER BY and JSON-constructor null clauses. |
| Relations | SQL Server WITH hints; WITH ORDINALITY on an ordinary table/function | Scoped rejection retains UNNEST WITH ORDINALITY. |
| CREATE INDEX | UNIQUE, concurrent/async creation, IF NOT EXISTS, access method, INCLUDE, NULLS policy, predicate, ordering, expression/qualified columns, absent index name | Explicit policy destructures every CreateIndex field and permits only the name/table/simple-column grammar from `ddl.ftl`. |
| CREATE TABLE/VIEW | LIKE, CLONE, temporary/unlogged tables, query-backed tables, IF NOT EXISTS on views | Explicit scoped checks reject the demonstrated fields. |
| MERGE | DELETE, BY SOURCE/TARGET, conditional WHEN, repeated matched clauses | Policy follows Calcite's `SqlMerge`, `WhenMatchedClause` and `WhenNotMatchedClause`: at most one update followed by one insert. |
| Expressions/windows | GROUPS frames, array slices, LIKE/ILIKE ANY | Recursive rejection includes nested queries and named window specifications. |

The adapter changes are in `crates/sql/sql-parser/src/ast_fields.rs`, `grammar.rs` and
`reject.rs`. The vendor grammar and three control parsers were not changed.
All 72 inputs now belong to the CI adversarial population, which has **817
cases**. Tests independently cover neighboring accepted forms, nested contexts,
index column shape and MERGE clause ordering.

[The certificate](evidence/parser-review-fields.json) contains the exact
identity-level changes, raw output/source/binary hashes and comparisons against
frozen Java outputs. Cargo tests, Clippy with denied warnings and the release
build pass in the devcontainer. Replaying the repaired binary gives:

| Population | Cases | Result |
|---|---:|---|
| Positive corpus | 8,571 | 8,479 accepted, 92 refused; all measurements unchanged. |
| Keywords | 13,660 | All measurements unchanged. |
| Captured diagnostics | 1,143 | All measurements unchanged. |
| Previous adversarial set | 745 | All measurements unchanged. |
| Mutation/control set | 27,760 | All measurements unchanged. |
| Previous field-policy set | 48 | All measurements unchanged. |
| New review matrix and controls | 72 | Exactly 36 previously overaccepted submissions now rejected. |

All populations have zero Rust-only acceptances after the repair. All **235,038
control-parser measurements** are identical, including their errors and debug
ASTs. All previous candidate measurements are also identical. The new matrix
has four existing Java-only refusals, not new losses of valid coverage. No
silent semantic misparse was demonstrated by this matrix.

## Remaining field audit

The explicit policies for Query, Select, Fetch, Insert, Update, Delete and
CreateIndex are useful compile-time guards against upstream field additions.
They do not cover every reachable AST field. Concrete remaining audit surfaces
include:

- `CreateTable` storage/distribution/version/comment/constraint options and
  nested column-option variants. The new five-field rejection is not a policy
  for that entire struct.
- `CreateView` security, temporary/materialized/alter modes, options, output
  target, column definitions and comments. Rejecting IF NOT EXISTS alone does
  not classify these fields.
- `TableFactor` versions, sampling variants, aliases, JSON paths, index
  directives, inheritance and its other variants. `WITH ORDINALITY` is only
  one field and is grammar-dependent.
- Function clause contexts, argument-name operators, window/order modifiers,
  expression variants, type options, CREATE TYPE/AGGREGATE and remaining
  accepted statement payloads.

These are source-audit gaps, not claims that every listed option is reachable
or wrong. Some are rejected by the dialect parser before the AST exists;
others are legitimate Feldera syntax. PLAN asks for a statement for each
outside-grammar AST field. A reviewable inventory must distinguish tested
rejection, accepted-and-represented syntax, and unreachable upstream syntax,
with the grammar/dialect gate for the latter. Listing only successful negative
fixtures does not establish that denominator.

An initially suspicious `SELECT * FROM ONLY t` control is correct: both ASTs
refer to the table named `only` aliased `t`. Rust's `inheritance` field is None.
Its terse display output was ambiguous to a human; inspection of the actual
AST resolved the question. It remains a positive regression control.

## Semantic oracle

The current output's `statements` strings come from upstream Display. They are
explicitly not a Feldera unparser: structured ROW types display as STRUCT,
MAP constructors display as `MAP {key: value}`, and internal nullable wrappers
have a different spelling. Replaying those strings cannot certify all accepted
cases without a typed unparser that covers those forms.

A Java-assisted oracle is a viable next step without implementing a Rust
binder. Emit Feldera SQL from the Rust AST; preserve original submission modes,
options and catalog evolution; then independently parse/bind both the original
and emitted programs with Java. Compare the S1 statements and S2 plans using
explicit field policies. Keep names, types, literal values, clause ordering,
options and relation identities exact. Record source positions separately;
rewriting SQL necessarily moves them. A failure to emit or reparse is an
unsupported observation, not semantic equality. Java canonical parsed AST
comparison can detect earlier defects but does not replace the bound S1 check.

This oracle checks the combined parser/unparser behavior. An unparser that
copies the original source, or guesses the intended meaning from the original
tokens, would hide parser defects. It must emit from represented AST fields,
with independent shape/value tests for every adapter-specific node. Do not
repair mismatches by broadly normalizing away structural fields.

## Source positions

The existing comparison measures reported raw-parser error starts after Java's
submission-line adjustment. It deliberately does not convert UTF-16 columns to
Unicode scalar columns. On the captured-diagnostic input, Java rejects only
14 submissions: **10 equal starts, two different starts, two absent Java
positions**. Thus the measured shares are 10/14 of raw-parser rejections or
10/12 of those with Java locations. They are not 1,143 matched compiler
assertions: most captured compiler failures occur after parsing.

The latest replay also preserves the previous 745-case adversarial starts
(159 equal, 190 different, three absent Java positions among 352 Java
rejections) and mutation starts (10,370 equal, 6,856 different). No source-span
regression was hidden by the field repair.

Field-level AST positions need a separate metric. Concrete representation
limitations are visible in `vendor/sqlparser/src/ast/spans.rs`: MAP, lambda,
struct/named expressions and several column options return empty spans;
interval expressions use only the value span; many unary predicates omit the
operator token. Built-in datatype variants do not retain an independent token
span. Existing fallback diagnostics sometimes use the column or statement
start, which must not count as reproducing the original field's range.

The experiment should report absent, partial and exact field ranges separately
from parse acceptance and semantic equality. PLAN requires measuring the share
of corpus error positions that can be reproduced; it does not specify 100%
span agreement as a separate numeric pass gate. The present raw-parser metric
is valid within its scope, while a claim about all binding/type diagnostic
positions remains unsupported.

## Follow-up: complete DDL fields and LOCAL semantics

The next 98-case DDL matrix found **48 further overacceptances** and a silent
semantic loss: upstream consumes LOCAL before CREATE VIEW but does not retain
it in CreateView. This affected **591 accepted view statements** in the
positive corpus. The Java AST retains LOCAL; the previous Rust AST and Display
converted them to ordinary views. The earlier review's absence of demonstrated
silent misparses applied only to its original 72-case matrix.

CreateView now retains an optional LOCAL modifier span, set by the Feldera
statement adapter before upstream can discard it. Its Display retains LOCAL.
Explicit field policies now cover every top-level CreateTable/CreateView field,
view columns, table column options and primary/foreign-key payloads. CREATE
prefix checks reject modifiers that upstream otherwise silently drops. These
repairs retain legitimate replacement/materialized views and key/default/
LATENESS/property forms. The raw failed matrix and original ASTs are preserved.

The repaired parser still accepts **8,479/8,571** positive cases. No previously
accepted case is lost. All control-parser observations remain unchanged. The
accepted AST changes are limited to the explicit CreateView.local field and
the restored LOCAL semantics; the evidence checks this exact field addition
and records every affected identity. Existing negative cases can report a
different refusal because the new CREATE/DDL checks run earlier. Their changed
diagnostics remain explicit; this follow-up does not claim unchanged errors.

Both parsers now export CREATE VIEW kind and replacement directly from typed
AST fields. The comparison checks these fields independently of SQL Display.
All **19 views in the expanded 922-case adversarial population** and eight
views in seven dedicated kind/multistatement controls match. CI requires this
comparison on positive and adversarial inputs. The comparison deliberately
retains `semantic_equivalence_checked=false`: these are selected field checks,
not the complete S1 semantic oracle.

The source audit enumerates **576 public AST types, 2,606 named/tuple fields and
1,838 enum variants**, including helper types, with explicit exhaustive field
patterns in 16 types. It marks remaining context review openly. Enumeration
does not prove reachability or classify every outside-grammar field. See
`evidence/parser-ast-field-inventory.json` and `scripts/parser_ast_inventory.py`.
The audit is executed in CI and its output uploaded.

Validation: 60 Rust library tests, two binary tests, Clippy with denied warnings,
release build, 20 Java parser tests and 12 Python tests pass. Full raw-parser
populations and the new matrices have zero Rust-only acceptances. Host Python
3.9 cannot run the existing comparator's `hashlib.file_digest`; the passing
Python tests used installed Python 3.14. See `evidence/parser-ddl-fields.json`.

## AST-derived SQL and independent Java semantic checks

The candidate now emits Feldera SQL from represented AST fields, without access
or fallback to the input text. ROW types retain nested field nullability, MAP
expressions retain ordered duplicate keys and NULL values, function declarations
recover their nullable/non-null type wrappers, and LOCAL remains explicit.
An unsupported emission is a refusal to run the semantic comparison, never a
successful comparison using the original SQL. Compiler options, submission
boundaries and catalog history are preserved by the pairing tool.

The focused 17-case matrix passes exact Java `SqlNode.equalsDeep` comparisons
for original and emitted SQL. Sixteen cases also agree on accepted binding,
complete bound metadata schemas and diagnostic content. One OR REPLACE LOCAL
VIEW case is an explicit matching user rejection; its schema-agreement value
is null. Original and emitted statement coordinates remain separate. This is
an independent semantic check of the listed forms, not full-corpus S1 equality
or evidence that all field-level diagnostic spans are available. Mutation
controls prove that changing a MAP value, column nullability or LOCAL view kind
cannot pass because acceptance or an output type happens to match.

The experiment also exposed a Java compiler bug: a quoted SQL function
parameter such as `"a""b"` became an illegal Rust variable name. The repair
preserves names accepted by the existing Rust-identifier predicate and gives
other parameters collision-free local names. SQL binding names remain intact;
RexInputRef positions select the same parameter in the declaration and body.
Both regressions fail against the frozen old classes and pass after repair.
They check return values 17/29 at optimization levels 0/2, declaration/body
consistency and collisions with an existing legal generated-looking name.
Six separately emitted integer functions also compile with rustc and execute
all expected-value assertions. This targeted repair does not expand the
existing identifier predicate's Unicode or reserved-word policy.

A fresh raw-parser replay preserves every candidate verdict, diagnostic and
typed-view observation in 8,571 positive and 922 adversarial cases, plus all
104,811 stock-control measurements. Positive coverage remains 8,479/8,571;
there are zero Rust-only acceptances. Display SQL changes for 172 positive
submissions and 17 adversarial submissions are retained explicitly. These
rendering changes are not normalized into an AST or full-corpus semantic claim.
All 34,325 accepted submissions in these two populations produce AST-derived
SQL; only the focused matrix has run the Java parse/bind comparison so far.

The original invalid MAP-return fixture, the first unclassified OR REPLACE
binding refusal and the quoted-parameter compiler failure remain in the raw
failed runs. None counts as a passing semantic observation. See
`evidence/parser-semantic.json` and its archive index.

## Full-corpus AST emission and S1 semantic oracle

The next frozen experiment checks every candidate-accepted program in the
8,571-program corpus. **8,470 programs pass** independent Java parsed-AST,
binding-verdict, schema-content and diagnostic-content comparisons after
Rust AST emission. The other **101 programs (1.18%)** are explicit parser
refusals. No accepted program is excluded because emission or comparison failed.
The new refusals are qualified datetime differences whose plural unit was
previously misread as a projection alias; the known fix is a typed interval
qualifier on that expression. Existing refusal families retain their earlier
classifications.

The full S1 oracle captures original SQL and AST-derived SQL in matching,
independent JVMs for each of 67 ordered shards. All **8,470 program pairs** have
identical semantic S1 content, including type identities and sharing, expression
objects, operator references, catalogs, options and statement metadata. All
**16,940 raw programs** also pass strict Java decoding and strict Rust decoding;
Rust re-encoding preserves each complete JSON record exactly. This is a parser
semantic oracle through the existing Java binder, not a Rust binder or runtime
parity claim.

The comparison is schema-specific. It separates coordinates only in the named
`Position` and `Coordinates` contracts, SQL text only in `Source`, and program
text/lines only in `SourceState`. Source names/IDs, quote flags, diagnostic text,
statement text, identities, ordering and every other field still compare
exactly. Raw S1, separated coordinates and source text remain archived. Unknown
schema fields or constructs fail closed. The metadata comparison likewise
separates only the two source ranges on each relation property. Its full raw
schema comparison remains visible: 159 programs have different property source
coordinates after formatting, while their property content agrees.

The first full pass exposed decoded quote loss, merged continued-string
fragments, and nine plural interval qualifiers misread as aliases. Repairs
escape every decoded delimiter, preserve each string fragment in a typed AST
node, and refuse ambiguous datetime qualifiers. Separate context matrices found
20 overaccepted forms involving TABLESAMPLE, COLLATE, foreign operators, lambda
placement and dynamic PIVOT. Those are now rejected, with accepted neighboring
forms retained as controls. The final release preserves every full-corpus parser
record from the passing semantic input snapshot exactly. The expanded **1,108
adversarial cases** contain **zero Rust-only acceptances**: 487 both accept, 555
both reject and 66 Java-only.

An initial S1 experiment interleaved original/emitted compilation in one JVM and
reported 29 type-sharing differences. A 128-case same-input control reproduces
one of these differences without changing SQL. Calcite's process-wide type
caches therefore make that experiment unsuitable for exact identity comparison.
The corrected independent JVM protocol preserves matching case order, compiler
configuration and capture/decode operations. It requires no type deduplication
or identity normalization. Both failed runs and the same-input control remain
archived.

CI now runs the full AST/binding oracle and the separate-JVM S1 oracle, with
strict Rust transport and input hash/count/identity checks. Current verification
passes 65 Rust library tests and two binary tests, Clippy with warnings denied,
a release build, nine Java semantic-probe tests, 18 Python parser tests, and
workflow lint. Both reusable corpus runners also pass a 128-case frozen shard.
See [the full-corpus certificate](evidence/parser-semantic-corpus.json).

The measured semantic gap is closed for these 8,470 programs. This is a P0
experiment, not a proof of parity for every possible SQL program. Error-position
measurement remains separate: among 1,143 captured diagnostic assertions, 14
submissions are raw Java syntax failures; 10 starts agree, two differ and two
have no Java position. That is 10/12 comparable positions (83.33%), or 10/14
including missing positions (71.43%). The other assertions exercise later
compiler stages and do not enter this raw-parser denominator. These measurements
meet the requested reporting scope; they do not establish complete field spans.

## Type, expression and accepted-adversarial follow-up

The outer rejection policies now enumerate all 119 `DataType` variants and all
63 `Expr` variants, without wildcard arms or omitted named fields. Together with
`Query` and `Select`, this implements the upstream-change compile-time review
requirement in PLAN section 6.2. Ten nested enums also have exhaustive policies.
The source inventory still distinguishes those policies from mere declarations:
576 public types include helper types and entire statement families that the
adapter rejects. It does not label all declarations reachable or all nested
policies complete.

A 732-case type matrix, spanning column declarations, CAST and nested ROW,
exposed 225 overacceptances. The repaired policy rejects foreign type parameters
and shapes, signed integer overflow in precisions, and unsupported array forms.
Forty boundary controls retain Java's signed magnitude limits. A 210-case
expression matrix exposed 32 overacceptances involving foreign expression
options and reserved-keyword fallback. These are rejected recursively, while
neighboring accepted forms remain controls. Typed datetime literals retain their
literal-specific grammar rather than inheriting type-declaration restrictions.

The final **2,136 adversarial fixtures** produce **988 both-accepted**, **1,061
both-rejected**, **87 Java-only**, and **zero Rust-only** results. Every one of
the 988 accepted fixtures passes a separate Java AST comparison after Rust AST
emission. Binding is deliberately absent from this grammar-only oracle: many
fixtures refer to undefined tables or unsupported compiler constructs. An earlier
experiment that attempted binding is retained as failed evidence and contributes
no binding-parity claim.

This oracle found charset introducers emitted with a meaning-changing space,
hyphenated introducers parsed as subtraction and aliases, and escaped strings
parsed as identifiers. The repair preserves lexical adjacency, decoded literal
values and source spans. Unknown E-string escapes retain their backslash as
Calcite requires. Both failures and controls remain in the evidence. The repaired
release produces exactly the same records for all **8,571 positive programs**
as the archived full semantic/S1 run; thus its 8,470 accepted programs and 101
refusals retain that evidence unchanged.

Two oracle defects are recorded separately. Calcite's TABLESAMPLE literal uses
object identity for its four-field value, so even unchanged SQL failed the old
comparison. A narrow comparator checks the exact class, complete instance-field
set, Bernoulli flag, BigDecimal rate including scale, repeatable flag and seed
including when inactive. It preserves the original AST; changed fields still
fail. Two hint string-chain fixtures make Calcite's diagnostic pretty printer
throw. Their exact AST comparisons still run; rendering failures are recorded
with class, message and stack rather than replacing structural equality.

Verification passes 69 Rust library tests, two binary tests, Clippy, release
build, 14 Java semantic-probe tests, 22 parser diagnostic-classification tests,
18 Python tests and workflow lint. CI repeats the accepted-adversarial AST-only
oracle alongside the full positive binding/S1 oracles. Source, binaries,
classpath hashes, failed witnesses and compiled Calcite field/charset proofs
are retained in `evidence/parser-grammar-review.json` and its archive.

Remaining scope is explicit: the finite context matrices cover the observed
foreign forms, while delegated function/operator/relation policies require
continued grammar review during production implementation. The exhaustive
outer matches force review when upstream fields change; they do not replace
that review. Maintenance pricing and maintainer acceptance remain separate
from these engineering observations.

The [refusal ledger](evidence/parser-refusal-dispositions.json) links all 101
remaining first failures to the exact frozen submissions and 16 proposed
grammar repairs. Qualified datetime subtraction accounts for 42 cases.
