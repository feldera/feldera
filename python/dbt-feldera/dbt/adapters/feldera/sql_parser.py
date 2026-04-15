"""
Abstract SQL parser for the Feldera dbt adapter.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from enum import Enum
from typing import List, Set


class SqlIntent(Enum):
    """
    How a SQL statement should be routed inside the Feldera dbt adapter,
    via the ad-hoc DataFusion query endpoint, the pipeline API.

    Members
    -------
    ADHOC_QUERY
        ``SELECT``, ``WITH``, ``UNION``, set operations, and anything
        the parser does not recognise.  Routed to Feldera's ad-hoc query
        endpoint (DataFusion-backed) - the pipeline endpoint does not support
        these statements.
    PIPELINE_DDL
        ``CREATE``, ``DROP`` — structural changes collected by
        the pipeline state manager and compiled as a single program.
    DATA_INGRESS
        ``INSERT`` — row-level writes pushed via HTTP ingress. Both the ad-hoc
        engine and the ingress API support ``INSERT``.
    METADATA
        Reserved for future catalog / ``INFORMATION_SCHEMA`` queries.
        The Feldera Python SDK's Pipeline object exposes table and view
        metadata (via ``tables()`` and ``views()``) which could serve as
        a catalog source.  See
        https://docs.feldera.com/python/feldera.rest.html#module-feldera.rest.sql_table
    NO_OP
        Empty input, whitespace-only, or comment-only SQL.
    """

    ADHOC_QUERY = "adhoc_query"
    PIPELINE_DDL = "pipeline_ddl"
    DATA_INGRESS = "data_ingress"
    METADATA = "metadata"
    NO_OP = "no_op"


class SqlParser(ABC):
    """Interface that every SQL parser backend must satisfy.

    The adapter code programs against this ABC so that the concrete
    parser (currently sqlglot) can be swapped without touching any
    call-sites.
    """

    @abstractmethod
    def classify(self, sql: str) -> SqlIntent:
        """Classify *sql* and return its execution intent.

        Must handle leading comments, empty strings, and whitespace
        gracefully.

        :param sql: Raw SQL text.
        :return: The classified :class:`SqlIntent`.
        """

    @abstractmethod
    def extract_table_ddls(self, sql: str) -> List[str]:
        """Extract ``CREATE TABLE`` statements from a pipeline SQL program.

        Must preserve the original SQL text verbatim (including any
        dialect-specific extensions such as Feldera ``WITH`` clauses).
        Returns each DDL as a string with a trailing semicolon.

        :param sql: A full pipeline SQL program.
        :return: A list of ``CREATE TABLE`` DDL strings.
        """

    @abstractmethod
    def extract_table_names(self, table_ddls: List[str]) -> Set[str]:
        """Return lowercase table names from a list of ``CREATE TABLE`` DDLs.

        :param table_ddls: DDL strings, e.g. ``CREATE TABLE "foo" (…);``.
        :return: A set of lowercase table names.
        """

    @abstractmethod
    def rename_in_ddl(self, ddl: str, old_name: str, new_name: str) -> str:
        """Rename the primary table/view identifier in a DDL statement.

        Must preserve the original SQL text (no regeneration) to avoid
        mangling dialect-specific syntax.

        :param ddl: The full DDL statement string.
        :param old_name: The current table/view name.
        :param new_name: The desired new name.
        :return: The rewritten DDL string.
        """

    @abstractmethod
    def sql_type_base_name(self, sql_type: str) -> str:
        """Return the uppercase base type name from a SQL type string.

        For example ``"DECIMAL(10,2)"`` → ``"DECIMAL"``.

        :param sql_type: A SQL data-type string.
        :return: The uppercase base type name.
        """
