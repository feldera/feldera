# Feldera SQL parser patches

This directory contains the unmodified crate archive for Apache DataFusion
sqlparser-rs 0.64.0 except for the files listed in `FELDERA-PROVENANCE.json`.
The Apache licence, notice, and manifests retain their upstream contents.
The registry 0.64.0 entry in the parent Cargo.lock pins the archive checksum.
To reproduce the vendored files, apply `FELDERA.patch` to the upstream archive
with `patch -p1`. From the repository root, run
`python3 crates/sql/parser-probe/normalize_vendor_docs.py PATH_TO_PATCHED_TREE`.
This step removes personal references and cites the SQL standard in the
documentation. The provenance file records the upstream and published digests.

The Feldera parser adds dialect extension points, type visitor hooks and additional AST forms:

- `Dialect::parse_join` and `Dialect::parse_function_arg` delegate syntax to the
  Feldera adapter before the stock parser consumes it.
- `JoinOperator::LeftAsOf` retains left-preserving semantics separately from
  the existing inner ASOF node. `FunctionArgExpr::Table` retains a relational
  argument, its qualified name and its source span. Visitors traverse both.
- The explicit `TABLE` query carries its complete relation name. The Feldera
  dialect reads compound identifiers and qualified wildcards through the same
  grammar as relational function arguments and plan inspection.
- `CreateView::local` retains Feldera LOCAL semantics and the modifier span. The
  adapter recognizes the exact CREATE [OR REPLACE] LOCAL VIEW prefix before the
  upstream parser can drop it; Display retains the LOCAL keyword. Stock parsing
  initializes this field to None.
- Candidate formatting emits parenthesized structured types as ROW, preserves
  explicit ROW field nullability as NULL or NOT NULL, and emits ordered MAP entries as MAP[key, value]. An unpaired trailing key survives parsing and formatting
  so validation can report an odd argument count.
  The independent stock comparator still uses the registry crate. Function
  nullability emission is implemented in the adapter from its typed wrappers.
- Candidate quote formatting treats every decoded quote as data. It does not
  guess that doubled or backslash-adjacent quotes were already escaped. This
  candidate always parses with unescape enabled; upstream's unescape-disabled
  formatting behavior is not retained. `Expr::StringLiteralChain` preserves
  continued string fragments and each token span. Formatting retains whether adjacent
  fragments share a line so Calcite can validate continuation after replay.
- Two dialect flags control repeated ARRAY suffixes and optional cardinalities.
  Their defaults preserve the stock parser's behavior.
- `Dialect::parse_values` delegates the VALUES body to the adapter. It preserves
  scalar row boundaries and Calcite's parenthesized ROW semantics without
  changing the default parser or rewriting tokens.
- `Visitor` and `VisitorMut` receive data-type hooks, including every nested
  element and field type. Their default implementations preserve traversal. The
  adapter uses them to reject malformed custom types in declarations and casts.
- SQL hint comments have an opt-in tokenizer mode. It respects SQL strings and
  comments within the outer hint, retaining original bytes and locations.
  Ordinary comments keep their existing termination rules.
- Typed hint lists retain names, homogeneous option forms, ordered duplicate
  key/value pairs and string-fragment chains. SELECT, table references and INSERT
  targets retain attachment. Stock dialects keep their raw hint representation.
- Malformed LEFT/RIGHT joins and EXTRACT separators report the current token's location. Upstream
  omitted the location from these parser errors; no accepted syntax
  or AST form changes in this repair.

- A dialect hook retains the trailing interval qualifier on a parenthesized
  datetime difference. Its AST node keeps the interval units, precision, operands
  and source span. An explicit INTERVAL expression follows its own grammar.
- The tokenizer accepts double-slash line comments for dialects that enable them.
- RLIKE and NOT RLIKE retain a NULL right operand. The parser consumes the NULL
  token in the postfix NOT NULL branch only after matching the negation.

The dialect also retains the symmetry qualifier on BETWEEN and DISTINCT on a
grouping list. Its LIMIT clauses preserve the order of the offset and count,
including an explicit ALL after a comma and the position of that keyword.

Unicode strings retain escapes until the Feldera parser has read UESCAPE after the
last fragment. Rendering non-BMP characters uses paired UTF-16 escapes so the
pinned Calcite reader recovers the original scalar. MAP subqueries and inline
foreign keys have dedicated AST forms and source ranges. The parser also
retains aliases on ROW fields, compound column names in wildcard exclusions,
nested grouping sets, and the measure, axis, and mapping lists of UNPIVOT.

Four expression forms retain their source ranges: the UNIQUE predicate,
`TRANSLATE(value USING charset)`, the SQL/JSON object constructor with its
ordered entries and null clause, and the postfix `FORMAT JSON` clause with its
optional encoding. Two dialect capabilities, off in stock dialects, permit a
qualified wildcard as a scalar operand and the `FORMAT JSON` clause.
`Parser::parse_json_null_clause` is public so that the adapter can call it.

MULTISET stores either an element list or a query primary. The adapter uses
`Parser::parse_query_primary` so set operations and ordering inside the
constructor require parentheses. Column LATENESS stores an expression and
its span so visitors can validate nested queries and preserve their hints.

Table and view properties retain string-literal keys and values.
`SqlOption::LiteralKeyValue` preserves a key's introducer, continuation
fragments and source span. Ordinary single-quoted keys use `KeyValue`.

The join and TABLE-argument grammar lives in `crates/sql/sql-parser/src/relations.rs`; VALUES
grammar lives in `crates/sql/sql-parser/src/values.rs`. The adapter requires both
MATCH_CONDITION and ON for ASOF joins and preserves named TABLE arguments.
The Feldera dialect enables repeated ARRAY layers and refuses bracketed sizes,
which the Java parser rejects. No token rewrite simulates these constructs.

The probe separately depends on the **unmodified registry** 0.64.0 crate for its
GenericDialect comparator. The workspace parser uses the vendored dialect.
This is a measured candidate for PLAN.md section 6.2's vendored-parser option;
it does not establish a full parser, exhaustive rejection, complete source
spans, S1 equivalence, or an approved long-term maintenance estimate.

To rebase, start with the new upstream archive, port this explicit patch, and
review every changed AST field and hook. Run the probe's unit tests and Clippy,
then repeat corpus, keyword and adversarial comparisons against the pinned Java
oracle. Preserve failed runs. Refresh the provenance and patch only after that
review; do not overwrite the evidence for the previous parser build.

The AST-emission audit also requires charset introducers to remain adjacent to
their string. `Expr::Prefixed` emits `_charset'value'`; the parser requires that
same token adjacency, preserving `_charset 'alias'` as an identifier and alias.
The adapter joins adjacent hyphenated charset tokens while retaining their
original span. Escaped `E'...'` literals use the dialect capability during both
tokenization and parsing; the old parser branch checked only concrete dialect
identities. These changes are confined to the vendored candidate.

The escape decoder also has an opt-in unknown-escape preservation capability.
Calcite keeps the backslash in E'\\q', unlike PostgreSQL. Stock dialects retain
their original decoding behavior; the Feldera dialect and its hint dialect
enable this capability.
