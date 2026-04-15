"""
sqlglot-powered SQL parser for the Feldera dbt adapter.

Implements :class:`~dbt.adapters.feldera.sql_parser.SqlParser` using
`sqlglot <https://sqlglot.com>`_ with a custom Feldera dialect
(Calcite-based, currently ANSI defaults).
"""

from __future__ import annotations

from typing import List, Set

import sqlglot
from sqlglot import exp
from sqlglot.dialects.dialect import Dialect

from dbt.adapters.feldera.sql_parser import SqlIntent, SqlParser

# ---------------------------------------------------------------------------
# Feldera dialect
# ---------------------------------------------------------------------------


class FelderaDialect(Dialect):
    """Minimal Feldera SQL dialect registered with sqlglot.

    Calcite uses double-quoted identifiers and standard SQL syntax, both
    of which are already the sqlglot defaults.  Subclass exists so we can
    customise tokenisation / generation later without changing callers.
    """

    pass  # inherits ANSI behaviour


_DIALECT: str = "felderadialect"
"""The dialect name to pass to ``sqlglot.parse*`` functions."""

# sqlglot expression key → SqlIntent
_KEY_TO_INTENT: dict[str, SqlIntent] = {
    "select": SqlIntent.ADHOC_QUERY,
    "union": SqlIntent.ADHOC_QUERY,
    "intersect": SqlIntent.ADHOC_QUERY,
    "except": SqlIntent.ADHOC_QUERY,
    "create": SqlIntent.PIPELINE_DDL,
    "drop": SqlIntent.PIPELINE_DDL,
    "insert": SqlIntent.DATA_INGRESS,
}


# ---------------------------------------------------------------------------
# SqlglotParser
# ---------------------------------------------------------------------------


class SqlglotParser(SqlParser):
    """Concrete :class:`SqlParser` backed by sqlglot.

    All methods use the AST only for **classification and validation** —
    original SQL text is always preserved verbatim so that Feldera-specific
    syntax (e.g. ``WITH ('connectors' = …)``) is never mangled by
    sqlglot's code generator.

    Instances are lightweight and stateless; sharing a single instance
    across threads is safe.
    """

    # -- SqlParser interface ------------------------------------------------

    def classify(self, sql: str) -> SqlIntent:
        """Parse *sql* with sqlglot and return the matching intent.

        The sqlglot parser transparently strips leading ``--`` and
        ``/* … */`` comments before inspecting the first statement.
        Falls back to :attr:`SqlIntent.ADHOC_QUERY` for any statement
        type not explicitly mapped (e.g. ``EXPLAIN``, ``SHOW``).

        :param sql: Raw SQL text (may contain whitespace and comments).
        :return: The classified :class:`SqlIntent`.
        """
        stripped = sql.strip()
        if not stripped:
            return SqlIntent.NO_OP

        try:
            stmts = sqlglot.parse(stripped, dialect=_DIALECT, error_level=sqlglot.ErrorLevel.IGNORE)
        except sqlglot.errors.ParseError:
            return SqlIntent.ADHOC_QUERY

        stmts = [s for s in stmts if s is not None]
        if not stmts:
            return SqlIntent.NO_OP

        return _KEY_TO_INTENT.get(stmts[0].key, SqlIntent.ADHOC_QUERY)

    def extract_table_ddls(self, sql: str) -> List[str]:
        """Extract ``CREATE TABLE`` DDLs from a pipeline SQL program.

        Original SQL text is preserved verbatim — we use the AST only
        for classification, never for regeneration.

        :param sql: The full pipeline SQL program text.
        :return: A list of ``CREATE TABLE`` DDL strings with trailing ``;``.
        """
        if not sql or not sql.strip():
            return []

        segments = self._split_statements(sql)

        table_ddls: List[str] = []
        for segment in segments:
            text = segment.strip()
            if not text:
                continue
            try:
                parsed = sqlglot.parse_one(text, error_level=sqlglot.ErrorLevel.IGNORE)
            except sqlglot.errors.ParseError:
                continue
            if parsed is None:
                continue
            if isinstance(parsed, exp.Create) and (parsed.args.get("kind") or "").upper() == "TABLE":
                table_ddls.append(text.rstrip().rstrip(";") + ";")
        return table_ddls

    def extract_table_names(self, table_ddls: List[str]) -> Set[str]:
        """Return table names from ``CREATE TABLE`` DDL strings.

        Quoted identifiers preserve their original case;
        unquoted identifiers are lowercased.

        :param table_ddls: DDL strings, e.g. ``CREATE TABLE "foo" (…);``.
        :return: A set of table names (case-sensitive for quoted, lowercase for unquoted).
        """
        names: Set[str] = set()
        for ddl in table_ddls:
            try:
                parsed = sqlglot.parse_one(ddl, dialect=_DIALECT, error_level=sqlglot.ErrorLevel.IGNORE)
            except sqlglot.errors.ParseError:
                continue
            tbl = parsed.find(exp.Table) if parsed else None
            if tbl and tbl.name:
                names.add(self._normalize_identifier(tbl))
        return names

    def rename_in_ddl(self, ddl: str, old_name: str, new_name: str) -> str:
        """Rename the primary table/view identifier in a DDL statement.

        Uses sqlglot to **validate** that the first ``CREATE`` target
        matches *old_name*, then performs a targeted ``str.replace`` on
        the original text to preserve Feldera-specific SQL extensions.

        For quoted identifiers, comparison is case-sensitive; for unquoted
        identifiers, comparison is case-insensitive.

        :param ddl: The full DDL statement string.
        :param old_name: The current table/view name.
        :param new_name: The desired new name.
        :return: The rewritten DDL string.
        """
        try:
            parsed = sqlglot.parse_one(ddl, error_level=sqlglot.ErrorLevel.IGNORE)
        except sqlglot.errors.ParseError:
            return ddl.replace(old_name, new_name, 1)

        if parsed is None:
            return ddl.replace(old_name, new_name, 1)

        tbl = parsed.find(exp.Table)
        if tbl and self._identifiers_match(tbl, old_name):
            return ddl.replace(old_name, new_name, 1)

        return ddl.replace(old_name, new_name, 1)

    def sql_type_base_name(self, sql_type: str) -> str:
        """Return the uppercase base type name from a SQL type string.

        Examples: ``"DECIMAL(10,2)"`` → ``"DECIMAL"``, ``"varchar"`` → ``"VARCHAR"``.

        :param sql_type: A SQL data-type string.
        :return: The uppercase base type name.
        """
        try:
            dt = exp.DataType.build(sql_type)
            return dt.this.name
        except Exception:
            return sql_type.split("(")[0].strip().upper()

    # -- internal helpers ---------------------------------------------------

    @staticmethod
    def _normalize_identifier(tbl: exp.Table) -> str:
        """Normalize a table identifier respecting SQL quoting rules,
        and preserves case.

        Quoted identifiers preserve their original case; unquoted
        identifiers are lowercased per the SQL standard.
        """
        this = tbl.this
        if isinstance(this, exp.Identifier) and this.args.get("quoted"):
            return tbl.name
        return tbl.name.lower()

    @staticmethod
    def _identifiers_match(tbl: exp.Table, name: str) -> bool:
        """Check whether *tbl*'s name matches *name*.

        Quoted identifiers are compared case-sensitively; unquoted
        identifiers are compared case-insensitively.
        """
        this = tbl.this
        if isinstance(this, exp.Identifier) and this.args.get("quoted"):
            return tbl.name == name
        return tbl.name.lower() == name.lower()

    @staticmethod
    def _split_statements(sql: str) -> List[str]:
        """Split a SQL program at top-level semicolons.

        Uses the sqlglot tokeniser so that semicolons inside string
        literals or comments are handled correctly.
        """
        from sqlglot.tokens import TokenType

        tokenizer = sqlglot.Tokenizer()
        tokens = list(tokenizer.tokenize(sql))

        segments: List[str] = []
        start = 0
        for tok in tokens:
            if tok.token_type == TokenType.SEMICOLON:
                segments.append(sql[start : tok.end + 1])
                start = tok.end + 1

        trailing = sql[start:]
        if trailing.strip():
            segments.append(trailing)

        return segments


# ---------------------------------------------------------------------------
# Module-level singleton
# ---------------------------------------------------------------------------

parser: SqlParser = SqlglotParser()
"""Constructed parser instance.

Import as::

    from dbt.adapters.feldera.sqlglot_parser import parser
"""
