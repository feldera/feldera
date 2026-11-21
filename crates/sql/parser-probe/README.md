# Parser coverage experiment

This standalone P0 prototype compares **pinned sqlparser 0.62.0 and 0.64.0**.
It uses its own Cargo workspace so the experiment cannot change DataFusion's
production parser. Each version parses the same input with `GenericDialect`
and an incomplete Feldera dialect with its own `TypeId`. The 0.64 generic
comparator uses the unmodified registry crate; the Feldera adapter now uses a
separate vendored copy with an eleven-file patch (see
[vendor provenance](vendor/sqlparser/FELDERA-PATCHES.md)). The prototype accepts
double-quoted identifiers and enables the available hooks for Unicode strings,
lambdas, numeric-prefix identifiers, trailing projection commas, and wildcard
EXCLUDE/REPLACE. The 0.64 adapter also uses its datatype hook to parse ROW
fields into structured AST fields, including nested ROWs, explicit field NULL
modifiers, and array suffixes. MAP types use the upstream angle-bracket hook;
MAP constructors retain ordered key/value expressions, duplicates, NULLs and
nested collections in structured entries. Its prefix hook merges adjacent string fragments
while preserving their span. A token pass decodes Unicode identifiers in all
identifier contexts, and a scoped AST check rejects qualified datetime
differences that upstream misparses as aliases. The CAST adapter preserves all
13 standard interval ranges, including plural unit spellings, and refuses
precision qualifiers that the upstream type cannot represent faithfully.
Interval qualifiers remain forbidden in columns and nested ROW/array types,
as in Calcite. A scoped TRIM parser supports omitted trim characters after a
direction without admitting comma-separated forms. The 0.62 adapter remains
unchanged. The 0.64 rejection pass exhaustively enumerates Select, Query, Expr and
DataType fields, plus selected nested enums; delegated nested grammar policies
remain separately reviewable.

Use the JSONL input produced by `../scripts/determinism.py prepare`, which
preserves Java submission boundaries. Run inside the devcontainer:

```sh
cargo test --locked --manifest-path crates/sql/parser-probe/Cargo.toml
cargo run --locked --release --manifest-path crates/sql/parser-probe/Cargo.toml -- \
    $SCRATCH/sql-determinism-input.jsonl $SCRATCH/parser-coverage
```

The output directory must not exist. `cases.jsonl` retains original SQL,
submission modes, per-version verdicts, parser errors, and normalized SQL.
`complete.json` is written only after every input record succeeds and the input
digest is rechecked. Duplicate identities, malformed records, and empty input
fail. Add `--include-ast` for focused syntax probes to retain debug ASTs; these
are version-specific evidence, not a stable interchange format.

Counts distinguish cases, submissions, and statements. One rejected submission
rejects the case. A submission can contain several SQL statements; a failed
submission contributes no parsed-statement count. This prevents statements
before an error from being reported as a successful program.
A submission with `many: false` must contain exactly one statement.

`syntax.jsonl` contains 39 focused probes. `adversarial.jsonl` adds 2,136 probes
for string fragments, Unicode identifiers, interval/alias ambiguity, and MAP
constructors and types, grouping sets, aggregate null treatment, interval CAST
contexts, TRIM forms, reserved identifiers, LATENESS, aggregate FILTER, SQL
function declarations, statement families and options, recursive declarations,
standalone LATENESS, ASOF joins, TABLE arguments, repeated ARRAY layers,
invalid literals, duplicate column attributes, connector property syntax and
scalar/structured VALUES rows, malformed nested types, FROM-dependent clauses,
implicit table-call lookahead and hint delimiters. Both
sets run in CI. The Java raw-parser oracle and the
comparison tool use the same input:

```sh
java -ea -Dfeldera.parser.producer=WORKTREE -Dfeldera.parser.calcite=CALCITE_REV \
    -cp "$JAVA_TEST_CLASSPATH" \
    org.dbsp.sqlCompiler.compiler.sql.tools.ParserProbe \
    $SCRATCH/sql-determinism-input.jsonl $SCRATCH/parser-java
python3 crates/sql/scripts/compare_parsers.py \
    $SCRATCH/parser-coverage $SCRATCH/parser-java $SCRATCH/parser-comparison.json
```

`JAVA_TEST_CLASSPATH` contains the Java compiler's `target/test-classes`,
`target/classes`, and dependency JAR. The Java oracle stops before post-parse
validation; internal exceptions abort the run instead of becoming rejections.
The comparison requires completed matching inputs and retains error groups and
case references. CI runs corpus, focused syntax, and keyword probes.

CI also prepares parser inputs from completed diagnostic captures using
`../scripts/parser_diagnostic_inputs.py CAPTURE OUTPUT`. This preserves original
API submission boundaries; it does not replay the original compiler's binding
or diagnostic expectations. The comparison reports error starts after accounting
for Java's cumulative submission lines. Missing positions and different column
units remain visible; this is not a complete AST-span comparison. The current
capture covers 1,143 assertions and has no Rust-only acceptances after eight
rejection repairs. See [the evidence](../evidence/parser-error-positions.json).

Generate the keyword matrix from the pinned Calcite checkout and both Rust
keyword tables, then run it through the same Rust and Java tools:

```sh
python3 crates/sql/scripts/parser_keywords.py sql-to-dbsp-compiler/calcite \
    /path/to/feldera-parser-probe $SCRATCH/keywords.jsonl $SCRATCH/keywords-inventory.json
```

The matrix covers 1,366 words, quoted and unquoted in five identifier contexts.
The original 13,660 probes exposed 504 Rust-only acceptances and three Java-only
acceptances in each custom dialect. The repaired 0.64 adapter rejects all 504
overacceptances while retaining the same full-corpus coverage. Its scoped
identifier checks distinguish UNNEST table calls and SET ON values, and reject
contiguous END-EXEC tokens that upstream reads as subtraction. The three
Java-only column-name forms remain. See
`../evidence/parser-reserved-identifiers.json` for the passing matrix and the
retained earlier overrejection. The generator includes Feldera's extension
tokens and checks `--reserved-keywords` against the pinned grammar before
emitting probes, so a stale policy fails CI.

The mutation experiment derives four deterministic Unicode edits and an unchanged
control from every distinct `(sql, many)` submission. It preserves source-case
provenance and treats each submission as an isolated raw parse; compiler options
and catalog state are not applied. On the recorded corpus this gives 5,552
controls and 22,208 mutations. The first completed comparison exposed 342
Rust-only acceptances. Recursive grammar checks repair all 342 without adding
Java-only refusals; the 274 existing Java-only cases remain. CI saves the complete
comparison and fails on any Rust-only acceptance for the 0.64 Feldera candidate.
The three comparison parsers remain observational controls.

```sh
python3 crates/sql/scripts/parser_mutations.py \
    $SCRATCH/sql-determinism-input.jsonl $SCRATCH/parser-mutations
/path/to/feldera-parser-probe $SCRATCH/parser-mutations/input.jsonl $SCRATCH/parser-mutations/rust
# Run ParserProbe on the same input into $SCRATCH/parser-mutations/java, then:
python3 crates/sql/scripts/compare_parsers.py \
    $SCRATCH/parser-mutations/rust $SCRATCH/parser-mutations/java $SCRATCH/parser-mutations/comparison.json \
    --reject-overacceptance 0.64.0/feldera_prototype
```

[Mutation evidence](../evidence/parser-mutations.json) retains the failed baseline,
all repair attempts, oracle classification tests and AST preservation checks.
The Java oracle recognizes specific grammar resource errors, including missing
BETWEEN separators and empty intervals; unrelated internal exceptions still abort.
Typed optimizer hints now retain ordered options, duplicate keys, string chains,
quoting and original spans on SELECT and named table nodes, including INSERT
targets. All 75 focused cases match Java. The hint lexer respects quoted
separators and comments inside hints without enabling nested ordinary comments.
The oracle records two known table-hint pretty-printer failures separately from
parse acceptance, with `rendering_complete=false`; unknown failures abort.
See [hint evidence](../evidence/parser-hints.json). Complete syntax/span coverage
and S1 comparison remain open.

Acceptance is **not semantic equivalence**. Corpus compiler options are not
translated into dialect configuration. The full spike still needs more identifier contexts and
negative-input coverage, token handling, failure classification, an exhaustive
AST reject pass, source-span measurements, and comparison against the S1 oracle.
The original ROW counterexample loses field shape in both stock dialects and
in the 0.62 prototype; the 0.64 prototype now preserves that shape. Its AST uses
upstream Struct fields, so display output says STRUCT rather than ROW. This
normalized SQL is not a Feldera unparser or an S1 parity certificate. ROW-field
NOT NULL modifiers remain explicitly unsupported instead of being dropped.
The recorded adjacent-string and four-digit Unicode identifier examples now
retain their values. Six-digit Unicode escapes and lone surrogates are explicitly
unsupported: the pinned Java oracle misdecodes the former and accepts the latter.
The Java result writer escapes UTF-16 code units so those observations remain
readable JSON. Qualified datetime differences are rejected because the upstream
AST cannot represent them; this closes that silent misparse without adding
interval support. Explicit aliases and function-call aliases remain accepted.
Same-line string fragments are rejected during parsing; Java rejects them later
in validation, so raw-parser verdict parity is deliberately not claimed.
Neither a low rejection rate nor matching pretty-printed SQL closes that gate.

The MAP extension reduces full-corpus rejections from 908 to 739 of 8,571 cases
(8.62%). Its 54 adversarial probes have 34 mutual acceptances, ten mutual
rejections, ten Java-only acceptances and no Rust-only acceptances. Two new
Java-only cases have odd MAP argument counts: Calcite rejects these during
validation; the paired Rust AST cannot retain an unpaired key, so the adapter
rejects them during parsing. MAP display uses upstream `MAP {key: value}` syntax,
not Feldera's `MAP[key, value]`. This representation is not a lossless SQL unparser.

Grouping sets, ROLLUP and CUBE retain ordered expression groups, including empty
and duplicate sets. Inline IGNORE/RESPECT NULLS is limited to unquoted
ARRAY_AGG, ARRAY_CONCAT_AGG, STRING_AGG and GROUP_CONCAT. Other functions use
suffix syntax; inline PERCENTILE syntax remains unsupported. A scoped visitor
checks nested queries and prevents the generic hook from admitting inline null
treatment on arbitrary functions. The first broad-hook run's five over-accepted
forms remain recorded in `../evidence/parser-aggregates-overacceptance.json`.
The repaired run rejects 667 of 8,571 corpus cases (7.78%); all 83 adversarial
probes have zero Rust-only acceptances. S1 equivalence remains unproven.

Column LATENESS now uses a scoped dialect-option hook. It parses and validates
an expression, then retains the original tokens because upstream has no typed
LATENESS node. Queries, duplicate/named attributes, untyped view columns and
ALTER TABLE use are rejected. All 216 adversarial and 13,660 keyword probes
have zero Rust-only acceptances; full-corpus rejections fall to 333 (3.89%).
See `../evidence/parser-lateness.json`. This representation does not satisfy S1.

Aggregate FILTER preserves its predicate, DISTINCT and window clauses. The
original probe rejected suffix null treatment after FILTER and bare FILTER
aliases. The current parser preserves bare aliases, including after a function
call. The original full corpus run had 282 rejections (3.29%), with zero Rust-only acceptances in 234
adversarial and 13,660 keyword probes. Thirty-one Rust tests and Clippy pass;
see `../evidence/parser-aggregate-filter.json`.

SQL function declarations retain typed arguments, defaults, return types and
expression bodies. Nullable wrappers distinguish nullable from NOT NULL types;
their Display output is not canonical Feldera SQL. Query bodies remain a scoped
refusal. Full-corpus rejections fall to 251 (2.93%); 258 adversarial and 13,660
keyword probes have zero Rust-only acceptances. Thirty-three Rust tests and
Clippy pass; see `../evidence/parser-function-declarations.json`.

The statement-family allowlist now rejects transaction controls, unsupported DDL
and extra DROP/SET/description forms admitted by upstream. This closes 35 newly
measured overacceptances. All 325 adversarial and 13,660 keyword probes have
zero Rust-only acceptances, and the 251 corpus rejections remain unchanged.
Thirty-five Rust tests and Clippy pass; see `../evidence/parser-statement-boundary.json`.
The allowlist does not exhaustively validate fields within allowed families.


Typed wrapper nodes now preserve recursive view declarations and standalone
LATENESS statements. This recovers 49 corpus cases, leaving 202 refusals (2.36%),
with zero Rust-only acceptances across 367 adversarial probes. The wrapper also
requires complete top-level input consumption; END cannot hide a trailing
statement. See [statement evidence](../evidence/parser-feldera-statements.json).

The small vendor patch adds join and argument hooks, typed LEFT ASOF and TABLE
argument nodes, and flags for repeated ARRAY suffixes and cardinalities. The
Feldera adapter retains both ASOF conditions, distinguishes left-preserving
joins, and retains named relational arguments with quoted names and spans.
It rejects seven previously overaccepted forms involving ASOF USING and ROW
array cardinalities. The original failures are retained.

The [vendor audit](../evidence/parser-vendor-relations.json) accepts **8,450 of
8,571 cases**, with **121 refusals (1.41%)**. All **419 adversarial** and
**13,660 keyword** probes have zero Rust-only acceptances. Every previously
accepted case remains accepted; all 34,015 stock-parser submission results are
unchanged. Forty-one Rust tests, Clippy and eight Java oracle tests pass.
The raw-parser 2% threshold is met. Complete AST field rejection, source spans,
S1 equivalence and the vendor maintenance estimate remain open.

[MAINTENANCE.md](MAINTENANCE.md) records the 121-refusal baseline and a
provisional 3–6 engineer-day routine-upgrade estimate (6–12 for disruptive
changes). The source inventory and repair directions are review inputs, not a
completed rebase or proof of S1 equivalence.

The [VALUES extension](../evidence/parser-values.json) reduces refusals to
**92/8,571 (1.07%)** by accepting 29 scalar-row cases. A dialect hook retains
row boundaries, expressions and real token spans. It also repairs eight
previously accepted programs: `(ROW(...))` is one row, while `ROW(ROW(...))`
contains a ROW-valued column. The old parser introduced an extra layer in the
first form. AST-level tests and the pinned Java parsed SQL establish this
specific repair; full S1 equivalence remains open. All 46 Rust tests and Clippy
pass. The expanded 488 adversarial cases, 13,660 keyword probes and 1,143
diagnostic assertions have zero Rust-only acceptances. Five new Java-only
forms remain explicit refusals: qualified wildcards, DEFAULT fields and named
fields within VALUES.

Query and DML options have explicit field policies for Query, Select, Fetch,
Insert, Update and Delete. Destructuring without `..` forces review of new
upstream fields. The measured field-option set repairs 27 further Rust-only
acceptances while retaining all prior positive-corpus records and accepted ASTs.
This policy does not enumerate every AST family or every nested option. See
[the field-policy certificate](../evidence/parser-ast-fields.json).

An [independent field review](../PARSER-REVIEW.md) found and repaired **36 more
Rust-only acceptances** in a 72-case matrix, covering function clauses, relation
options, CREATE INDEX, table/view flags, MERGE and expression/window options.
The adversarial input now contains **817 cases**. All previous candidate results
(including ASTs and errors) and all three control parsers remain identical across
the positive, keyword, diagnostic, adversarial, mutation and field-policy
populations. The positive count remains **8,479/8,571**. See
[the certificate](../evidence/parser-review-fields.json). This closes the
measured field defects; the complete AST-field inventory and S1 semantic oracle
remain open.

A subsequent [DDL audit](../PARSER-REVIEW.md) repairs another **48** overaccepted
forms and preserves LOCAL view kind, previously lost in **591** accepted corpus
view statements. All **922 adversarial cases** have zero Rust-only acceptances;
positive coverage remains 8,479/8,571. The `views` observation records typed view
kind/replacement independently of Display. `compare_parsers.py
--compare-view-fields` checks it against Java; CI requires that check on the
positive and adversarial populations. The complete source-field inventory is
explicit about remaining context classifications. These selected typed checks
do not replace the full S1 semantic oracle.

`semantic.jsonl` exercises AST-derived Feldera SQL emission and independent Java
parse/bind comparison for ROW/MAP types and values, nullability, function
parameters, LOCAL views and submission boundaries. The emitter never reads the
original source. Sixteen accepted programs agree on parsed ASTs, bound schemas
and diagnostic content; one explicit user rejection agrees without counting as
schema equality. Statement coordinates are retained separately. These focused
checks do not establish full-corpus S1 equivalence. The experiment also repairs
quoted function parameters that previously caused Java's Rust-variable-name
assertion. See [semantic evidence](../evidence/parser-semantic.json).

The subsequent [full-corpus oracle](../evidence/parser-semantic-corpus.json)
checks every accepted program: 8,470/8,571 parse, emit from their AST, and agree
with Java on ASTs, binding, schema/diagnostic content and typed S1 semantics.
All 16,940 raw S1 programs survive strict Rust transport exactly. Coordinates
and source text remain separate, schema-specific observations. The final release
also rejects every measured overacceptance in 1,108 adversarial cases. The
remaining 101 positive-corpus refusals are explicit (1.18%); no accepted program
is dropped for an emission/comparison failure. Complete upstream-field/context
classification and the field-span metric remain separate open work.

## Detached constant-view programs

With the `binder-probe` feature, build `a-binder-probe` and invoke
`a-binder-probe --constant-program PRELUDE.json INPUT.jsonl OUTPUT.jsonl`.
The prelude is one `statement-program-v5` object produced by the Java S1
exporter with empty user SQL and the selected compilation options. Each input
line contains a string ID and one API submission with its original SQL and
submission mode. The Rust binder builds the user statement, catalog metadata,
typed relation and expression objects. The prelude fixes the compilation
options for this experiment.

The accepted statement shape is one ASCII line defining a standard view over
a single constant projection. The probe supplies an executable whole-program
boundary for the measured expressions. The broader front end is tracked in
the P0 checklist.
[Detached replay evidence](../evidence/a-binder-constant-program.json)

## Detached table projections

Use `a-binder-probe --table-program PRELUDE.json INPUT.jsonl OUTPUT.jsonl`
for a plain single-column table and a standard view that reads that column.
The input accepts a single SQL batch or two single-statement submissions in
the same batch. The binder emits the table and view catalog entries, the
typed table scan, and the original positions for declarations and aliases.
The tested types are all four integer widths, BOOLEAN, and TIMESTAMP at its
default precision. Both nullable and non-nullable declarations are covered.

Eleven programs per backend match Java, including a timestamp-table program
from the development corpus. The checks cover typed user statements, source
and diagnostic metadata, schemas, the optimizer prefix and artifact bytes.
[Detached table evidence](../evidence/a-binder-table-program.json)
