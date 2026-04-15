import logging
from decimal import Decimal
from typing import Any, Dict, List, NamedTuple, Optional, Sequence, Tuple

from dbt.adapters.feldera.sql_parser import SqlIntent
from dbt.adapters.feldera.sqlglot_parser import parser as _parser
from feldera.pipeline import Pipeline
from feldera.rest.feldera_client import FelderaClient

logger = logging.getLogger(__name__)

#: Maximum number of rows to scan when inferring column types.
#
_TYPE_SCAN_LIMIT = 50


class ColumnDescription(NamedTuple):
    """
    DB-API 2.0 compatibtle column description.
    """

    name: str
    type_code: Optional[str] = None
    display_size: None = None
    internal_size: None = None
    precision: None = None
    scale: None = None
    null_ok: None = None


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
        """Return the number of rows returned by the last ad-hoc query.

        For DDL and ingress operations, returns 0. The continuous pipeline
        model does not have an equivalent "rows affected" count.
        """
        return self._rowcount

    @property
    def description(self) -> Optional[List[ColumnDescription]]:
        """
        Column descriptions for the last query result.

        Returns a list of :class:`ColumnDescription` when there are columns,
        or ``None`` when there are no columns (DDL, INSERT, empty).

        Read by ``dbt test`` (via ``_get_result_table`` — uses ``name``),
        ``dbt run`` with model contracts, and ``dbt snapshot``.
        """
        if not self._columns:
            return None
        types = self._column_types or [None] * len(self._columns)
        return [
            ColumnDescription(
                name=col,
                type_code=types[i] if i < len(types) else None,
            )
            for i, col in enumerate(self._columns)
        ]

    def execute(self, sql: str, bindings: Optional[Sequence] = None) -> None:
        """
        Execute a SQL statement by routing to the appropriate Feldera endpoint.

        For queries that return results, the query is routed to the ad-hoc query
        endpoint backed by Datafusion.

        For queries that perform data manipulation, the query is currently logged
        but not executed directly against the pipeline query endpoint, as the adapter
        currently expects the dbt adapter run to handle these operations.

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
        """Infer SQL type names from Python values in the result set.

        Scans up to :data:`_TYPE_SCAN_LIMIT` rows so that a leading
        ``None`` doesn't hide the real type.  Handles ``Decimal`` values
        returned by the Feldera SDK (``json.loads(..., parse_float=Decimal)``).
        """
        if not self._results or not self._columns:
            return [None] * len(self._columns) if self._columns else []

        types: List[Optional[str]] = [None] * len(self._columns)
        resolved = 0
        target = len(self._columns)

        for row in self._results[:_TYPE_SCAN_LIMIT]:
            if resolved >= target:
                break
            for idx, col in enumerate(self._columns):
                if types[idx] is not None:
                    continue
                val = row.get(col)
                inferred = self._python_type_to_sql(val)
                if inferred is not None:
                    types[idx] = inferred
                    resolved += 1
        return types

    @staticmethod
    def _python_type_to_sql(val: object) -> Optional[str]:
        """Map a Python value to a Feldera SQL type name, or ``None``."""
        if val is None:
            return None
        if isinstance(val, bool):
            return "BOOLEAN"
        if isinstance(val, int):
            return "INTEGER"
        if isinstance(val, Decimal):
            return "DECIMAL"
        if isinstance(val, float):
            return "DOUBLE"
        if isinstance(val, str):
            return "VARCHAR"
        if isinstance(val, list):
            return "ARRAY"
        if isinstance(val, dict):
            return "MAP"
        return None

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
        Fetch all remaining rows from the result set produced by an ad-hoc query.

        The result set is a point-in-time snapshot from Feldera's DataFusion
        ad-hoc query engine.

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
