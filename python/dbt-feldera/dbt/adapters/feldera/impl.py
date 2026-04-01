import logging
import math
from typing import FrozenSet, List, Optional, Set, Tuple

import agate
from dbt.adapters.base import BaseAdapter, available
from dbt.adapters.base.relation import InformationSchema
from dbt.adapters.contracts.relation import RelationType

from dbt.adapters.feldera.column import FelderaColumn
from dbt.adapters.feldera.connections import FelderaConnectionHandle, FelderaConnectionManager
from dbt.adapters.feldera.credentials import FelderaCredentials
from dbt.adapters.feldera.pipeline_manager import PipelineStateManager
from dbt.adapters.feldera.relation import FelderaRelation

logger = logging.getLogger(__name__)

# Singleton for pipeline state shared across threads
_pipeline_state = PipelineStateManager()


class FelderaAdapter(BaseAdapter):
    """
    The dbt adapter for Feldera.

    Routes dbt operations to Feldera's REST API, leveraging DBSP for
    automatic incremental view maintenance.

    Concept mapping between dbt and Feldera:

    * **database** → Feldera API host / instance (set via ``host`` in profile).
    * **schema**   → Feldera **pipeline** (a named, compiled SQL program).
    * **relation** → a **table** or **view** inside a pipeline's SQL program.

    dbt passes a :class:`FelderaRelation` object to most adapter methods.
    ``relation.schema`` yields the pipeline name, while
    ``relation.identifier`` yields the table/view name within that pipeline.
    """

    ConnectionManager = FelderaConnectionManager
    Relation = FelderaRelation
    Column = FelderaColumn

    @classmethod
    def date_function(cls) -> str:
        """Return the SQL date function for Feldera.

        See Also:
            https://docs.feldera.com/sql/datetime/#now
        """
        return "NOW()"

    @classmethod
    def is_cancelable(cls) -> bool:
        """Feldera REST queries cannot be individually cancelled."""
        return False

    def debug_query(self) -> None:
        """Verify connectivity by hitting the Feldera healthz endpoint."""
        connection = self.connections.get_thread_connection()
        client = connection.handle.client
        config = client.get_config()
        logger.debug("Feldera debug_query OK: %s", config)

    @classmethod
    def convert_text_type(cls, agate_table: agate.Table, col_idx: int) -> str:
        return "VARCHAR"

    @classmethod
    def convert_number_type(cls, agate_table: agate.Table, col_idx: int) -> str:
        # Check if any values have decimal parts
        for row in agate_table.rows:
            val = row[col_idx]
            if val is not None and val != int(val):
                return "DOUBLE"
        return "BIGINT"

    @classmethod
    def convert_boolean_type(cls, agate_table: agate.Table, col_idx: int) -> str:
        return "BOOLEAN"

    @classmethod
    def convert_datetime_type(cls, agate_table: agate.Table, col_idx: int) -> str:
        return "TIMESTAMP"

    @classmethod
    def convert_date_type(cls, agate_table: agate.Table, col_idx: int) -> str:
        return "DATE"

    @classmethod
    def convert_time_type(cls, agate_table: agate.Table, col_idx: int) -> str:
        return "TIME"

    def _get_client(self):
        """Get the FelderaClient from the current connection handle."""
        conn = self.connections.get_thread_connection()
        handle: FelderaConnectionHandle = conn.handle
        return handle.client

    def _get_credentials(self) -> FelderaCredentials:
        """Get the credentials from the current connection."""
        conn = self.connections.get_thread_connection()
        return conn.credentials

    def _get_pipeline_name(self, schema: Optional[str] = None) -> str:
        """
        Determine the pipeline name from schema or credentials.

        :param schema: Optional schema name override.
        :return: The pipeline name to use.
        """
        if schema:
            return schema
        creds = self._get_credentials()
        return creds.pipeline_name or creds.schema

    # ── Schema operations ──────────────────────────────────────────────

    @available
    def list_schemas(self, database: str) -> List[str]:
        """
        List available schemas (pipeline names) in Feldera.

        :param database: The database name (ignored; all pipelines are listed).
        :return: A list of pipeline names.
        """
        try:
            client = self._get_client()
            pipelines = client.pipelines()
            return [p.name for p in pipelines]
        except Exception as exc:
            logger.warning("Failed to list schemas: %s", exc)
            return []

    @available
    def check_schema_exists(self, database: str, schema: str) -> bool:
        """
        Check if a schema (pipeline) exists.

        :param database: The database name (ignored).
        :param schema: The pipeline name to check.
        :return: True if the pipeline exists.
        """
        return schema in self.list_schemas(database)

    def create_schema(self, relation: FelderaRelation) -> None:
        """
        Create a schema (pipeline) if it does not exist.

        This is a soft create: if the pipeline already exists, it is a no-op.

        :param relation: A dbt relation used only to extract the pipeline name
            via ``relation.schema``.
        """
        pipeline_name = self._get_pipeline_name(relation.schema)
        if self.check_schema_exists(relation.database, pipeline_name):
            logger.debug("Pipeline '%s' already exists, skipping create", pipeline_name)
            return
        logger.debug("Schema create is deferred until pipeline deployment for '%s'", pipeline_name)

    def drop_schema(self, relation: FelderaRelation) -> None:
        """
        Drop a schema (pipeline).

        Stops and deletes the pipeline from Feldera.

        :param relation: A dbt relation used only to extract the pipeline name
            via ``relation.schema``.  The relation's identifier (table/view)
            is not relevant here — the entire pipeline is dropped.
        """
        pipeline_name = self._get_pipeline_name(relation.schema)
        client = self._get_client()
        _pipeline_state.stop(client, pipeline_name)
        try:
            from feldera.enums import PipelineFieldSelector

            inner = client.get_pipeline(pipeline_name, PipelineFieldSelector.STATUS)
            if inner is not None:
                client.delete_pipeline(pipeline_name)
                logger.info("Deleted pipeline '%s'", pipeline_name)
        except Exception as exc:
            logger.debug("Pipeline '%s' not found for deletion: %s", pipeline_name, exc)

    # ── Relation operations ────────────────────────────────────────────

    def drop_relation(self, relation: FelderaRelation) -> None:
        """
        Drop a relation (table or view) from the pipeline.

        Removes the DDL from the pipeline state manager. The pipeline will
        be recompiled on the next deploy.

        :param relation: The relation to drop.
        """
        pipeline_name = self._get_pipeline_name(relation.schema)
        name = relation.identifier
        _pipeline_state.remove_table(pipeline_name, name)
        _pipeline_state.remove_view(pipeline_name, name)
        logger.debug("Dropped relation '%s' from pipeline '%s'", name, pipeline_name)

    def truncate_relation(self, relation: FelderaRelation) -> None:
        """
        Truncate a relation by stopping the pipeline and clearing its storage.

        :param relation: The relation to truncate.
        """
        pipeline_name = self._get_pipeline_name(relation.schema)
        client = self._get_client()
        _pipeline_state.stop(client, pipeline_name)
        _pipeline_state.clear_storage(client, pipeline_name)
        logger.debug("Truncated pipeline '%s' (storage cleared)", pipeline_name)

    def rename_relation(
        self,
        from_relation: FelderaRelation,
        to_relation: FelderaRelation,
    ) -> None:
        """
        Rename a relation within the pipeline.
        """
        raise NotImplementedError("`rename_relation` is currently not supported by the Feldera adapter.")

    @available
    def get_columns_in_relation(self, relation: FelderaRelation) -> List[FelderaColumn]:
        """
        Get column metadata for a relation from the Feldera pipeline schema.

        :param relation: The relation to introspect.
        :return: A list of FelderaColumn objects.
        """
        pipeline_name = self._get_pipeline_name(relation.schema)
        client = self._get_client()

        try:
            from feldera.enums import PipelineFieldSelector

            inner = client.get_pipeline(pipeline_name, PipelineFieldSelector.ALL)
            if inner is None:
                return []

            target_name = relation.identifier.lower() if relation.identifier else ""

            for table in inner.tables:
                if table.name.lower() == target_name:
                    return [FelderaColumn.from_feldera_field(f) for f in table.fields]

            for view in inner.views:
                if view.name.lower() == target_name:
                    return [FelderaColumn.from_feldera_field(f) for f in view.fields]

        except Exception as exc:
            logger.warning("Failed to get columns for '%s': %s", relation.identifier, exc)

        return []

    def expand_column_types(
        self,
        goal: FelderaRelation,
        current: FelderaRelation,
    ) -> None:
        """
        No-op: This is currently not supported in this dbt adapter.

        See Also:
            https://github.com/dbt-labs/dbt-adapters/blob/8518ddf7dcc66e14826e837f3fcbe31e3483d30b/dbt-bigquery/src/dbt/adapters/bigquery/impl.py#L354
        """
        pass

    @available
    def list_relations_without_caching(
        self,
        schema_relation: FelderaRelation,
    ) -> List[FelderaRelation]:
        """
        List all tables and views in a pipeline without using the cache.

        :param schema_relation: A relation identifying the schema (pipeline).
        :return: A list of FelderaRelation objects.
        """
        pipeline_name = self._get_pipeline_name(schema_relation.schema)
        client = self._get_client()

        relations = []
        try:
            from feldera.enums import PipelineFieldSelector

            inner = client.get_pipeline(pipeline_name, PipelineFieldSelector.ALL)
            if inner is None:
                return []

            for table in inner.tables:
                relations.append(
                    self.Relation.create(
                        database=schema_relation.database,
                        schema=pipeline_name,
                        identifier=table.name,
                        type=RelationType.Table,
                    )
                )

            for view in inner.views:
                relations.append(
                    self.Relation.create(
                        database=schema_relation.database,
                        schema=pipeline_name,
                        identifier=view.name,
                        type=RelationType.View,
                    )
                )

        except Exception as exc:
            logger.warning("Failed to list relations for pipeline '%s': %s", pipeline_name, exc)

        return relations

    # ── Catalog ────────────────────────────────────────────────────────

    def _get_one_catalog(
        self,
        information_schema: InformationSchema,
        schemas: Set[str],
        used_schemas: FrozenSet[Tuple[str, str]],
    ) -> "agate.Table":
        """
        Build a catalog agate.Table for the given schemas by introspecting
        Feldera pipelines directly (no SQL information_schema).
        """
        from dbt_common.clients.agate_helper import table_from_data

        catalog_columns = [
            "table_database",
            "table_schema",
            "table_name",
            "table_type",
            "table_comment",
            "table_owner",
            "column_name",
            "column_index",
            "column_type",
            "column_comment",
        ]
        rows = []
        for schema_name in schemas:
            schema_relation = self.Relation.create(
                database=str(information_schema.database),
                schema=schema_name,
            )
            relations = self.list_relations_without_caching(schema_relation)
            for relation in relations:
                columns = self.get_columns_in_relation(relation)
                for idx, col in enumerate(columns, start=1):
                    rows.append(
                        {
                            "table_database": relation.database,
                            "table_schema": relation.schema,
                            "table_name": relation.identifier,
                            "table_type": str(relation.type),
                            "table_comment": None,
                            "table_owner": None,
                            "column_name": col.name,
                            "column_index": idx,
                            "column_type": col.dtype,
                            "column_comment": None,
                        }
                    )

        table = table_from_data(rows, catalog_columns)
        return self._catalog_filter_table(table, used_schemas)

    # ── Quoting ────────────────────────────────────────────────────────

    def quote(self, identifier: str) -> str:
        """
        Quote an identifier using double quotes (Calcite SQL standard).

        :param identifier: The identifier to quote.
        :return: The quoted identifier.
        """
        return f'"{identifier}"'

    # ── Available methods for Jinja macros ─────────────────────────────

    @available
    def register_view(self, pipeline_name: str, view_name: str, view_sql: str) -> str:
        """
        Register a CREATE VIEW statement with the pipeline state manager.

        Called from Jinja materializations to collect model SQL.

        :param pipeline_name: The pipeline (schema) name.
        :param view_name: The view name.
        :param view_sql: The full CREATE VIEW SQL statement.
        :return: An empty string (macros expect a string return).
        """
        _pipeline_state.register_view(pipeline_name, view_name, view_sql)
        return ""

    @available
    def register_table(self, pipeline_name: str, table_name: str, table_sql: str) -> str:
        """
        Register a CREATE TABLE statement with the pipeline state manager.

        :param pipeline_name: The pipeline (schema) name.
        :param table_name: The table name.
        :param table_sql: The full CREATE TABLE SQL statement.
        :return: An empty string.
        """
        _pipeline_state.register_table(pipeline_name, table_name, table_sql)
        return ""

    @available
    def deploy_pipeline(self, pipeline_name: str) -> str:
        """
        Deploy the assembled pipeline to Feldera.

        Assembles all registered tables and views into a SQL program,
        creates/updates the pipeline, waits for compilation, and starts it.

        :param pipeline_name: The pipeline (schema) name.
        :return: An empty string.
        """
        client = self._get_client()
        creds = self._get_credentials()
        _pipeline_state.deploy(
            client,
            pipeline_name,
            compilation_profile=creds.compilation_profile,
            workers=creds.workers,
            timeout=creds.timeout,
        )
        return ""

    @available
    def wait_for_pipeline(self, pipeline_name: str) -> str:
        """
        Wait for a pipeline to reach the RUNNING state.

        :param pipeline_name: The pipeline (schema) name.
        :return: An empty string.
        """
        client = self._get_client()
        p = _pipeline_state.get_pipeline(client, pipeline_name)
        if p is not None:
            creds = self._get_credentials()
            p.wait_for_status(
                expected_status=__import__("feldera.enums", fromlist=["PipelineStatus"]).PipelineStatus.RUNNING,
                timeout=creds.timeout,
            )
        return ""

    @available
    def stop_pipeline(self, pipeline_name: str) -> str:
        """
        Stop a running pipeline.

        :param pipeline_name: The pipeline (schema) name.
        :return: An empty string.
        """
        client = self._get_client()
        _pipeline_state.stop(client, pipeline_name)
        return ""

    @available
    def push_seed_data(self, pipeline_name: str, table_name: str, data: list) -> str:
        """
        Push seed data into a pipeline table via HTTP ingress.

        :param pipeline_name: The pipeline (schema) name.
        :param table_name: The target table name.
        :param data: A list of dicts representing rows to ingest.
        :return: An empty string.
        """
        client = self._get_client()
        try:
            client.push_to_pipeline(
                pipeline_name=pipeline_name,
                table_name=table_name,
                format="json",
                data=data,
                array=True,
                update_format="raw",
            )
            logger.info("Pushed %d rows to '%s.%s'", len(data), pipeline_name, table_name)
        except Exception as exc:
            logger.error("Failed to push seed data to '%s.%s': %s", pipeline_name, table_name, exc)
            raise

        return ""

    @available
    def stash_seed(
        self,
        pipeline_name: str,
        table_name: str,
        agate_table: agate.Table,
        column_types: dict | None = None,
    ) -> str:
        """
        Stash seed data for deferred push after pipeline deployment.

        Because ``create_or_replace`` clears pipeline storage, all seeds
        are collected first. After all tables are registered and the pipeline
        is deployed once, the stashed data is pushed via HTTP ingress.

        :param pipeline_name: The pipeline (schema) name.
        :param table_name: The seed table name.
        :param agate_table: The agate Table with seed data.
        :param column_types: Optional column type overrides from seed config.
        :return: An empty string.
        """
        _pipeline_state.stash_seed(pipeline_name, table_name, agate_table, column_types or {})
        return ""

    @available
    def finalize_seeds(self) -> str:
        """
        Deploy pipelines with pending seeds and push all stashed data,
        then update any pipelines with pending views.

        Called from the on-run-end hook in ``dbt_project.yml``. Handles
        both ``dbt seed`` (pending seeds) and ``dbt run`` (pending views)
        in a single entry point.

        For seeds: creates the pipeline with ``create_or_replace`` (fresh
        deploy), then pushes all stashed seed data via HTTP ingress.

        For views: updates an existing pipeline with new view DDLs via
        ``modify`` (preserving existing table data).

        No-ops if there are neither pending seeds nor pending views.

        :return: An empty string.
        """
        client = self._get_client()
        creds = self._get_credentials()

        seed_pipelines = _pipeline_state.get_pending_seed_pipelines()
        for pipeline_name in seed_pipelines:
            logger.info("Finalizing seeds for pipeline '%s'", pipeline_name)

            _pipeline_state.deploy(
                client,
                pipeline_name,
                compilation_profile=creds.compilation_profile,
                workers=creds.workers,
                timeout=creds.timeout,
            )

            for table_name, agate_table, column_types in _pipeline_state.pop_pending_seeds(pipeline_name):
                rows = self._convert_agate_rows(agate_table, column_types)
                if rows:
                    self.push_seed_data(pipeline_name, table_name, rows)
                    logger.info(
                        "Loaded %d seed rows into '%s.%s'",
                        len(rows),
                        pipeline_name,
                        table_name,
                    )

        view_pipelines = _pipeline_state.get_pipelines_with_views()
        for pipeline_name in view_pipelines:
            if pipeline_name in seed_pipelines:
                # Already deployed with seeds — views were included in assemble_program
                continue

            logger.info("Updating pipeline '%s' with views", pipeline_name)
            _pipeline_state.update_with_views(
                client,
                pipeline_name,
                compilation_profile=creds.compilation_profile,
                workers=creds.workers,
                timeout=creds.timeout,
            )

        return ""

    @staticmethod
    def _convert_agate_rows(agate_table: agate.Table, column_types: dict | None = None) -> list:
        """
        Convert agate table rows to JSON-serializable dicts.

        Uses column_types overrides from the seed config to cast values
        to the correct Python types for JSON ingestion. Without overrides,
        falls back to agate's inferred types.

        :param agate_table: The agate Table.
        :param column_types: Optional dict mapping column name to SQL type string.
        :return: A list of dicts with properly-typed values.
        """
        from decimal import Decimal

        column_types = column_types or {}
        column_names = agate_table.column_names

        def _sanitize_float(v):
            """Convert NaN/Infinity to None for JSON compatibility."""
            f = float(v)
            if math.isnan(f) or math.isinf(f):
                return None
            return f

        # Build a per-column cast function based on SQL type overrides
        _INT_TYPES = {"INT", "INTEGER", "SMALLINT", "TINYINT", "BIGINT"}
        _FLOAT_TYPES = {"DOUBLE", "FLOAT", "REAL", "DECIMAL", "NUMERIC"}
        _BOOL_TYPES = {"BOOLEAN", "BOOL"}

        def _make_caster(sql_type: str):
            from dbt.adapters.feldera.sqlglot_parser import parser

            upper = parser.sql_type_base_name(sql_type)
            if upper in _INT_TYPES:
                return lambda v: int(v) if v is not None else None
            if upper in _FLOAT_TYPES:
                return lambda v: _sanitize_float(v) if v is not None else None
            if upper in _BOOL_TYPES:
                return lambda v: bool(v) if v is not None else None
            # VARCHAR, TIMESTAMP, DATE — keep as string
            return None

        casters = {}
        for col_name in column_names:
            sql_type = column_types.get(col_name)
            if sql_type:
                casters[col_name] = _make_caster(sql_type)

        rows = []
        for row in agate_table.rows:
            row_dict = {}
            for i, col_name in enumerate(column_names):
                value = row[i]
                if value is None:
                    row_dict[col_name] = None
                    continue

                caster = casters.get(col_name)
                if caster is not None:
                    try:
                        row_dict[col_name] = caster(value)
                        continue
                    except (ValueError, TypeError):
                        pass

                # Fallback: auto-convert based on Python type
                if isinstance(value, Decimal):
                    if value.is_nan() or value.is_infinite():
                        row_dict[col_name] = None
                    elif value == int(value):
                        row_dict[col_name] = int(value)
                    else:
                        row_dict[col_name] = float(value)
                elif isinstance(value, bool):
                    row_dict[col_name] = value
                elif hasattr(value, "isoformat"):
                    row_dict[col_name] = value.isoformat()
                else:
                    row_dict[col_name] = str(value)
            rows.append(row_dict)

        return rows

    @available
    def load_seed_from_agate(self, pipeline_name: str, table_name: str, agate_table: agate.Table) -> str:
        """
        Push seed data from an agate table into a running pipeline via HTTP ingress.

        Handles type conversion from agate's Python types (Decimal, datetime, etc.)
        to JSON-serializable values that Feldera can ingest.

        :param pipeline_name: The pipeline name.
        :param table_name: The target table name.
        :param agate_table: The agate Table with seed data.
        :return: An empty string.
        """
        rows = self._convert_agate_rows(agate_table)

        if rows:
            self.push_seed_data(pipeline_name, table_name, rows)
            logger.info("Loaded %d seed rows into '%s.%s'", len(rows), pipeline_name, table_name)

        return ""

    @available
    def get_pipeline_state_manager(self) -> PipelineStateManager:
        """
        Return the global pipeline state manager.

        Used by materializations that need direct access to pipeline state.

        :return: The PipelineStateManager singleton.
        """
        return _pipeline_state

    @available
    def get_seed_column_types(self, model: dict) -> str:
        """
        Build a column-definition string for a seed's CREATE TABLE.

        Reads the agate table from the seed's ``agate_table`` attribute and
        maps Python types to Feldera SQL types.

        :param model: The dbt model (seed) node.
        :return: A comma-separated column definition string.
        """
        agate_table = model.get("agate_table")
        if agate_table is None:
            logger.warning("Seed model has no agate_table; using generic VARCHAR columns")
            return ""

        col_defs = []
        for col_name, col_type in zip(agate_table.column_names, agate_table.column_types):
            sql_type = self._agate_type_to_feldera(col_type)
            col_defs.append(f'"{col_name}" {sql_type}')

        return ", ".join(col_defs)

    @staticmethod
    def _agate_type_to_feldera(agate_type: object) -> str:
        """
        Map an agate column type to a Feldera SQL type.

        :param agate_type: The agate type object.
        :return: The Feldera SQL type string.
        """
        type_name = type(agate_type).__name__
        mapping = {
            "Text": "VARCHAR",
            "Number": "DECIMAL",
            "Boolean": "BOOLEAN",
            "Date": "DATE",
            "DateTime": "TIMESTAMP",
            "TimeDelta": "INTERVAL",
        }
        return mapping.get(type_name, "VARCHAR")
