from __future__ import annotations

import logging
from contextlib import contextmanager
from typing import Any, ContextManager, Optional

import agate
from dbt_common.exceptions import DbtRuntimeError

from dbt.adapters.base import BaseConnectionManager
from dbt.adapters.contracts.connection import (
    AdapterResponse,
    Connection,
    ConnectionState,
)
from dbt.adapters.feldera.cursor import FelderaCursor
from feldera.rest.feldera_client import FelderaClient

logger = logging.getLogger(__name__)


class FelderaConnectionManager(BaseConnectionManager):
    """
    Manages connections to Feldera's REST API.
    """

    TYPE = "feldera"

    @classmethod
    def open(cls, connection: Connection) -> Connection:
        """
        Open a connection to Feldera by creating an HTTP client session.

        :param connection: The dbt connection object containing credentials.
        :return: The opened connection.
        """
        if connection.state == ConnectionState.OPEN:
            logger.debug("Connection already open, skipping open.")
            return connection

        credentials = connection.credentials
        try:
            client = FelderaClient(
                url=credentials.host,
                api_key=credentials.api_key if credentials.api_key else None,
                timeout=credentials.timeout,
            )
            connection.handle = FelderaConnectionHandle(client, credentials)
            connection.state = ConnectionState.OPEN
            logger.debug("Connected to Feldera at %s", credentials.host)
        except Exception as exc:
            logger.error("Failed to connect to Feldera: %s", exc)
            connection.handle = None
            connection.state = ConnectionState.FAIL
            raise DbtRuntimeError(f"Failed to connect to Feldera at {credentials.host}: {exc}") from exc

        return connection

    def cancel(self, connection: Connection) -> None:
        """Cancel is a no-op for REST-based connections."""
        pass

    @classmethod
    def get_response(cls, cursor: FelderaCursor) -> AdapterResponse:
        """
        Build an AdapterResponse from the cursor state.

        :param cursor: The FelderaCursor after execution.
        :return: An AdapterResponse with row count info.
        """
        return AdapterResponse(
            _message="OK",
            rows_affected=cursor.rowcount,
        )

    def begin(self) -> None:
        """
        No-op: Feldera has transaction support, but the adapter does not currently
        implement it as most dbt backend warehouse implementations do not support this.

        See Also:
            https://docs.feldera.com/api/begin-transaction
        """
        pass

    def commit(self) -> None:
        """
        No-op: Feldera has transaction support, but the adapter does not currently
        implement it as most dbt backend warehouse implementations do not support this.

        See Also:
            https://docs.feldera.com/api/commit-transaction
        """
        pass

    def cancel_open(self) -> Optional[list[str]]:
        """Cancel open queries. REST requests auto-timeout."""
        return []

    @contextmanager
    def exception_handler(self, sql: str) -> ContextManager:
        """
        Context manager for handling exceptions during SQL execution.

        :param sql: The SQL being executed (for error context).
        """
        try:
            yield
        except DbtRuntimeError:
            raise
        except Exception as exc:
            logger.error("Error executing SQL: %s\n%s", sql[:200], exc)
            raise DbtRuntimeError(f"Feldera error: {exc}") from exc

    def add_query(
        self,
        sql: str,
        auto_begin: bool = True,
        bindings: Optional[Any] = None,
        abridge_sql_log: bool = False,
    ) -> tuple[Connection, Any]:
        """
        Execute a SQL statement and return the connection and cursor.

        Required by dbt's seed materialization (``load_csv_rows``).

        :param sql: The SQL to execute.
        :param auto_begin: Ignored (Feldera is non-transactional).
        :param bindings: Optional bindings (not used).
        :param abridge_sql_log: Whether to truncate SQL in logs.
        :return: Tuple of (connection, cursor).
        """
        connection = self.get_thread_connection()
        cursor = connection.handle.cursor()

        with self.exception_handler(sql):
            if abridge_sql_log:
                logger.debug("Executing (abridged): %s...", sql[:512])
            else:
                logger.debug("Executing: %s", sql[:200])
            cursor.execute(sql, bindings)

        return connection, cursor

    def execute(
        self,
        sql: str,
        auto_begin: bool = False,
        fetch: bool = False,
        limit: Optional[int] = None,
    ) -> tuple[AdapterResponse, agate.Table]:
        """
        Execute SQL against Feldera.

        Routes the SQL through FelderaCursor which classifies the intent
        (DDL, ad-hoc query, etc.) and dispatches accordingly.

        :param sql: The SQL to execute.
        :param auto_begin: Ignored (Feldera is non-transactional).
        :param fetch: Whether to fetch results.
        :param limit: Max rows to return.
        :return: Tuple of (AdapterResponse, agate.Table).
        """
        connection = self.get_thread_connection()
        cursor = connection.handle.cursor()

        with self.exception_handler(sql):
            cursor.execute(sql)

        response = self.get_response(cursor)

        if fetch:
            table = self._get_result_table(cursor, limit)
        else:
            table = agate.Table(rows=[], column_names=[], column_types=[])

        return response, table

    @staticmethod
    def _get_result_table(cursor: "FelderaCursor", limit: Optional[int] = None) -> agate.Table:
        """
        Build an agate.Table from cursor results.

        :param cursor: Executed FelderaCursor.
        :param limit: Max rows.
        :return: agate.Table with results.
        """
        rows = cursor.fetchall()
        if limit is not None:
            rows = rows[:limit]

        if cursor.description:
            column_names = [col[0] for col in cursor.description]
        else:
            column_names = []

        return agate.Table(rows=rows, column_names=column_names)


class FelderaConnectionHandle:
    """
    A connection handle wrapping the Feldera REST client.

    Provides a cursor factory compatible with DB-API 2.0 patterns.
    """

    def __init__(self, client: FelderaClient, credentials: Any) -> None:
        self._client = client
        self._credentials = credentials

    @property
    def client(self) -> FelderaClient:
        """Return the underlying Feldera REST client."""
        return self._client

    @property
    def credentials(self) -> Any:
        """Return the connection credentials."""
        return self._credentials

    def cursor(self) -> FelderaCursor:
        """
        Create a new cursor for executing queries.

        :return: A FelderaCursor instance.
        """
        pipeline_name = self._credentials.pipeline_name or self._credentials.schema
        return FelderaCursor(self._client, pipeline_name)

    def close(self) -> None:
        """Close is a no-op for REST-based connections."""
        pass
