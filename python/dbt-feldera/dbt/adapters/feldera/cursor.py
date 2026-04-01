import logging
from typing import Any, Dict, List, Optional, Sequence, Tuple

from dbt.adapters.feldera.sql_parser import SqlIntent
from dbt.adapters.feldera.sqlglot_parser import parser as _parser
from feldera.pipeline import Pipeline
from feldera.rest.feldera_client import FelderaClient

logger = logging.getLogger(__name__)


class FelderaCursor:
    """
    A DB-API 2.0 cursor that wraps Feldera REST API responses.

    Routes SQL execution to the appropriate Feldera API endpoint based on
    the classified intent of the SQL statement.

    The cursor lazily resolves a :class:`Pipeline` object on the first
    ad-hoc query, avoiding unnecessary API calls for DDL-only cursors.
    """

    def __init__(self, client: FelderaClient, pipeline_name: str) -> None:
        self._client = client
        self._pipeline_name = pipeline_name
        self._pipeline: Optional[Pipeline] = None
        self._results: Optional[List[Dict[str, Any]]] = None
        self._columns: Optional[List[str]] = None
        self._column_types: Optional[List[Optional[str]]] = None
        self._rowcount: int = -1
        self._closed: bool = False

    def _get_pipeline(self) -> Pipeline:
        """
        Lazily resolve and cache the Pipeline object for ad-hoc queries.

        :return: The Pipeline instance for the configured pipeline name.
        """
        if self._pipeline is None:
            self._pipeline = Pipeline.get(self._pipeline_name, self._client)
        return self._pipeline

    @property
    def rowcount(self) -> int:
        """Return the number of rows affected by the last operation."""
        return self._rowcount

    @property
    def description(self) -> Optional[List[Tuple]]:
        """
        Return column descriptions for DB-API 2.0 compatibility.

        Each description is a 7-tuple: (name, type_code, display_size,
        internal_size, precision, scale, null_ok).

        Type codes are inferred from the Python types in the first result row.
        """
        if not self._columns:
            return None
        types = self._column_types or [None] * len(self._columns)
        return [
            (col, types[i] if i < len(types) else None, None, None, None, None, None)
            for i, col in enumerate(self._columns)
        ]

    def execute(self, sql: str, bindings: Optional[Sequence] = None) -> None:
        """
        Execute a SQL statement by routing to the appropriate Feldera endpoint.

        :param sql: The SQL statement to execute.
        :param bindings: Optional parameter bindings (not supported by Feldera).
        """
        if self._closed:
            raise RuntimeError("Cursor is closed")

        intent = _parser.classify(sql)
        logger.debug("Executing SQL (intent=%s): %s", intent.value, sql[:200])

        if intent == SqlIntent.NO_OP:
            self._results = []
            self._columns = []
            self._column_types = None
            self._rowcount = 0
            return

        if intent == SqlIntent.ADHOC_QUERY:
            self._execute_adhoc_query(sql)
        elif intent == SqlIntent.PIPELINE_DDL:
            self._results = []
            self._columns = []
            self._column_types = None
            self._rowcount = 0
            logger.debug("Pipeline DDL captured (not executed directly): %s", sql[:200])
        elif intent == SqlIntent.DATA_INGRESS:
            self._results = []
            self._columns = []
            self._column_types = None
            self._rowcount = 0
            logger.debug("Data ingress captured: %s", sql[:200])
        elif intent == SqlIntent.METADATA:
            self._execute_adhoc_query(sql)

    def _infer_column_types(self) -> List[Optional[str]]:
        """Infer SQL type codes from the first result row's Python types."""
        if not self._results or not self._columns:
            return [None] * len(self._columns) if self._columns else []

        first_row = self._results[0]
        types: List[Optional[str]] = []
        for col in self._columns:
            val = first_row.get(col)
            if val is None:
                types.append(None)
            elif isinstance(val, bool):
                types.append("BOOLEAN")
            elif isinstance(val, int):
                types.append("INTEGER")
            elif isinstance(val, float):
                types.append("DOUBLE")
            elif isinstance(val, str):
                types.append("VARCHAR")
            elif isinstance(val, list):
                types.append("ARRAY")
            elif isinstance(val, dict):
                types.append("MAP")
            else:
                types.append(None)
        return types

    def _execute_adhoc_query(self, sql: str) -> None:
        """
        Execute an ad-hoc SQL query against a running pipeline.

        Uses Feldera's ad-hoc query endpoint backed by Datafusion.
        Lazily resolves a :class:`Pipeline` object on first use.

        :param sql: The SELECT query to execute.
        """
        try:
            pipeline = self._get_pipeline()
            rows = list(pipeline.query(sql))

            self._results = [dict(r) for r in rows]

            if self._results:
                self._columns = list(self._results[0].keys())
            else:
                self._columns = []

            self._rowcount = len(self._results)
            self._column_types = self._infer_column_types()
            logger.debug("Ad-hoc query returned %d rows", self._rowcount)

        except Exception as exc:
            logger.error("Ad-hoc query failed: %s", exc)
            self._results = []
            self._columns = []
            self._column_types = None
            self._rowcount = 0
            raise

    def fetchall(self) -> List[Tuple]:
        """
        Fetch all remaining rows from the result set.

        :return: A list of row tuples.
        """
        if self._results is None:
            return []
        rows = [tuple(r.values()) for r in self._results]
        self._results = []
        return rows

    def fetchone(self) -> Optional[Tuple]:
        """
        Fetch the next row from the result set.

        :return: A single row tuple, or None if no more rows.
        """
        if not self._results:
            return None
        row = self._results.pop(0)
        return tuple(row.values())

    def fetchmany(self, size: int = 1) -> List[Tuple]:
        """
        Fetch multiple rows from the result set.

        :param size: The number of rows to fetch.
        :return: A list of row tuples.
        """
        if not self._results:
            return []
        batch = self._results[:size]
        self._results = self._results[size:]
        return [tuple(r.values()) for r in batch]

    def close(self) -> None:
        """Close the cursor."""
        self._closed = True
        self._results = None
        self._columns = None
        self._column_types = None
