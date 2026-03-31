import logging
import threading
from typing import Dict, List, Optional, Tuple

import agate

from feldera.enums import CompilationProfile, PipelineStatus
from feldera.pipeline import Pipeline
from feldera.pipeline_builder import PipelineBuilder
from feldera.rest.errors import FelderaAPIError
from feldera.rest.feldera_client import FelderaClient
from feldera.runtime_config import RuntimeConfig

logger = logging.getLogger(__name__)


class PipelineStateManager:
    """
    Manages the mapping between dbt models and Feldera pipeline SQL.

    Thread-safe: dbt runs models in parallel, so all state mutations
    are protected by a mutex.

    This class collects individual table/view DDL statements from dbt models
    and assembles them into a complete pipeline SQL program for deployment.
    """

    def __init__(self) -> None:
        self._lock = threading.RLock()
        self._tables: Dict[str, Dict[str, str]] = {}
        self._views: Dict[str, Dict[str, str]] = {}
        self._deployed_sql: Dict[str, str] = {}
        self._pipelines: Dict[str, Pipeline] = {}
        self._pending_seeds: Dict[str, List[Tuple[str, agate.Table, dict]]] = {}

    def register_table(self, pipeline: str, name: str, ddl: str) -> None:
        """
        Register a CREATE TABLE statement for a pipeline.

        :param pipeline: The pipeline (schema) name.
        :param name: The table name.
        :param ddl: The full CREATE TABLE DDL statement.
        """
        with self._lock:
            self._tables.setdefault(pipeline, {})[name] = ddl
            logger.debug("Registered table '%s' in pipeline '%s'", name, pipeline)

    def register_view(self, pipeline: str, name: str, ddl: str) -> None:
        """
        Register a CREATE VIEW statement for a pipeline.

        :param pipeline: The pipeline (schema) name.
        :param name: The view name.
        :param ddl: The full CREATE VIEW DDL statement.
        """
        with self._lock:
            self._views.setdefault(pipeline, {})[name] = ddl
            logger.debug("Registered view '%s' in pipeline '%s'", name, pipeline)

    def remove_table(self, pipeline: str, name: str) -> None:
        """
        Remove a table from the pipeline.

        :param pipeline: The pipeline (schema) name.
        :param name: The table name to remove.
        """
        with self._lock:
            if pipeline in self._tables:
                self._tables[pipeline].pop(name, None)

    def remove_view(self, pipeline: str, name: str) -> None:
        """
        Remove a view from the pipeline.

        :param pipeline: The pipeline (schema) name.
        :param name: The view name to remove.
        """
        with self._lock:
            if pipeline in self._views:
                self._views[pipeline].pop(name, None)

    def get_tables(self, pipeline: str) -> Dict[str, str]:
        """
        Return a copy of all registered tables for a pipeline.

        :param pipeline: The pipeline (schema) name.
        :return: A dict mapping table name to DDL.
        """
        with self._lock:
            return dict(self._tables.get(pipeline, {}))

    def get_views(self, pipeline: str) -> Dict[str, str]:
        """
        Return a copy of all registered views for a pipeline.

        :param pipeline: The pipeline (schema) name.
        :return: A dict mapping view name to DDL.
        """
        with self._lock:
            return dict(self._views.get(pipeline, {}))

    def assemble_program(self, pipeline: str) -> str:
        """
        Assemble all registered tables and views into a pipeline SQL program.

        Tables are emitted first (inputs), then views (transforms).

        :param pipeline: The pipeline (schema) name.
        :return: The assembled SQL program string.
        """
        with self._lock:
            parts = []
            for _name, ddl in self._tables.get(pipeline, {}).items():
                parts.append(ddl.rstrip().rstrip(";") + ";")
            for _name, ddl in self._views.get(pipeline, {}).items():
                parts.append(ddl.rstrip().rstrip(";") + ";")
            return "\n\n".join(parts)

    def has_sql_changed(self, pipeline: str) -> bool:
        """
        Check if the assembled SQL differs from the last deployed version.

        :param pipeline: The pipeline (schema) name.
        :return: True if the SQL has changed or was never deployed.
        """
        with self._lock:
            current = self.assemble_program(pipeline)
            return current != self._deployed_sql.get(pipeline, "")

    def stash_seed(
        self,
        pipeline: str,
        table_name: str,
        agate_table: agate.Table,
        column_types: dict | None = None,
    ) -> None:
        """
        Stash seed data for deferred ingestion after pipeline deployment.

        Seeds cannot be pushed until the pipeline is compiled and running.
        Since ``create_or_replace`` clears pipeline storage, we collect
        all seed data first, deploy once, then push everything.

        :param pipeline: The pipeline (schema) name.
        :param table_name: The seed table name.
        :param agate_table: The agate Table containing seed data.
        :param column_types: Optional column type overrides from seed config.
        """
        with self._lock:
            self._pending_seeds.setdefault(pipeline, []).append((table_name, agate_table, column_types or {}))
            logger.debug("Stashed seed '%s' for pipeline '%s'", table_name, pipeline)

    def has_pending_seeds(self, pipeline: str) -> bool:
        """
        Check if there are stashed seeds awaiting deployment.

        :param pipeline: The pipeline (schema) name.
        :return: True if there are pending seeds.
        """
        with self._lock:
            return bool(self._pending_seeds.get(pipeline))

    def get_pending_seed_pipelines(self) -> List[str]:
        """
        Return all pipeline names that have pending seeds.

        :return: A list of pipeline names with stashed seed data.
        """
        with self._lock:
            return [p for p, seeds in self._pending_seeds.items() if seeds]

    def pop_pending_seeds(self, pipeline: str) -> List[Tuple[str, agate.Table, dict]]:
        """
        Pop and return all pending seeds for a pipeline.

        :param pipeline: The pipeline (schema) name.
        :return: A list of (table_name, agate_table, column_types) tuples.
        """
        with self._lock:
            return self._pending_seeds.pop(pipeline, [])

    def deploy(
        self,
        client: FelderaClient,
        pipeline: str,
        compilation_profile: str = "dev",
        workers: int = 4,
        timeout: int = 300,
    ) -> Pipeline:
        """
        Deploy the assembled pipeline to Feldera.

        Creates or updates the pipeline with the current SQL, waits for
        compilation, and starts the pipeline.

        :param client: The Feldera REST client.
        :param pipeline: The pipeline (schema) name.
        :param compilation_profile: The compilation profile to use.
        :param workers: Number of worker threads for the pipeline.
        :param timeout: Compilation timeout in seconds.
        :return: The deployed Pipeline object.
        """
        with self._lock:
            sql = self.assemble_program(pipeline)
            if not sql.strip():
                raise RuntimeError(f"Pipeline '{pipeline}' has no tables or views registered")

            logger.info("Deploying pipeline '%s' (%d chars of SQL)", pipeline, len(sql))

            profile = CompilationProfile.DEV
            if compilation_profile == "optimized":
                profile = CompilationProfile.OPTIMIZED
            elif compilation_profile == "unoptimized":
                profile = CompilationProfile.UNOPTIMIZED

            runtime_config = RuntimeConfig.default()
            runtime_config.workers = workers

            builder = PipelineBuilder(
                client,
                name=pipeline,
                sql=sql,
                compilation_profile=profile,
                runtime_config=runtime_config,
            )

            p = builder.create_or_replace(wait=True)
            logger.info("Pipeline '%s' compiled successfully, starting...", pipeline)

            p.start()
            p.wait_for_status(PipelineStatus.RUNNING, timeout=timeout)
            logger.info("Pipeline '%s' is running", pipeline)

            self._deployed_sql[pipeline] = sql
            self._pipelines[pipeline] = p
            return p

    def update_with_views(
        self,
        client: FelderaClient,
        pipeline: str,
        compilation_profile: str = "dev",
        workers: int = 4,
        timeout: int = 300,
    ) -> Pipeline:
        """
        Update an existing pipeline with new views, preserving table data.

        Unlike :meth:`deploy`, this does NOT call ``clear_storage``. It fetches
        the existing pipeline's SQL, extracts table DDLs (preserving seed data),
        replaces any existing view DDLs with the newly registered ones, and
        recompiles.

        :param client: The Feldera REST client.
        :param pipeline: The pipeline (schema) name.
        :param compilation_profile: The compilation profile to use.
        :param workers: Number of worker threads for the pipeline.
        :param timeout: Compilation timeout in seconds.
        :return: The updated Pipeline object.
        """
        with self._lock:
            local_views = self._views.get(pipeline, {})
            if not local_views:
                p = self.get_pipeline(client, pipeline)
                if p is not None:
                    return p
                raise RuntimeError(f"Pipeline '{pipeline}' has no views and does not exist")

            # Fetch existing pipeline SQL (contains table DDLs from dbt seed)
            try:
                p = Pipeline.get(pipeline, client)
                existing_sql = p.program_code()
            except FelderaAPIError:
                # Pipeline doesn't exist yet — fall back to full deploy
                logger.info("Pipeline '%s' not found, falling back to full deploy", pipeline)
                return self.deploy(client, pipeline, compilation_profile, workers, timeout)

            # Extract only CREATE TABLE statements from existing SQL
            # (discard existing CREATE VIEW statements to avoid duplicates)
            table_parts = self._extract_table_ddls(existing_sql)

            # Include locally registered tables not already in the
            # existing pipeline
            local_tables = self._tables.get(pipeline, {})
            if local_tables:
                existing_names = self._extract_table_names(table_parts)
                for name, ddl in local_tables.items():
                    if name.lower() not in existing_names:
                        table_parts.append(ddl.rstrip().rstrip(";") + ";")
                        logger.debug("Added locally registered table '%s' to pipeline '%s'", name, pipeline)

            # Assemble new views
            view_parts = []
            for _name, ddl in local_views.items():
                view_parts.append(ddl.rstrip().rstrip(";") + ";")

            # Combine existing tables with new views
            full_sql = "\n\n".join(table_parts + view_parts)

            logger.info(
                "Updating pipeline '%s' with %d views (%d chars total SQL)",
                pipeline,
                len(local_views),
                len(full_sql),
            )

            # Stop the pipeline (preserve data — no clear_storage)
            try:
                p.stop(force=True)
            except Exception:
                pass  # May already be stopped

            # Update SQL and wait for recompilation
            p.modify(sql=full_sql)
            inner = client._wait_for_compilation(pipeline, timeout_s=timeout)

            # Recreate pipeline wrapper from compiled result
            p = Pipeline._from_inner(inner, client)

            logger.info("Pipeline '%s' recompiled with views, starting...", pipeline)

            p.start()
            p.wait_for_status(PipelineStatus.RUNNING, timeout=timeout)
            logger.info("Pipeline '%s' is running with views", pipeline)

            self._deployed_sql[pipeline] = full_sql
            self._pipelines[pipeline] = p
            return p

    @staticmethod
    def _extract_table_ddls(sql: str) -> List[str]:
        """
        Extract CREATE TABLE statements from a pipeline SQL program.

        Returns only ``CREATE TABLE`` DDL strings.

        :param sql: The full pipeline SQL program.
        :return: A list of CREATE TABLE DDL strings (with trailing semicolons).
        """
        from dbt.adapters.feldera.sqlglot_parser import parser

        return parser.extract_table_ddls(sql)

    @staticmethod
    def _extract_table_names(table_ddls: List[str]) -> set:
        """
        Extract table names from a list of CREATE TABLE DDL statements.

        :param table_ddls: List of DDL strings (e.g., ``CREATE TABLE "foo" (...);``).
        :return: A set of lowercase table names.
        """
        from dbt.adapters.feldera.sqlglot_parser import parser

        return parser.extract_table_names(table_ddls)

    def has_pending_views(self) -> bool:
        """
        Check if there are locally registered views not yet deployed.

        :return: True if any pipeline has registered views.
        """
        with self._lock:
            return any(bool(views) for views in self._views.values())

    def get_pipelines_with_views(self) -> List[str]:
        """
        Return pipeline names that have locally registered views.

        :return: A list of pipeline names.
        """
        with self._lock:
            return [p for p, views in self._views.items() if views]

    def stop(self, client: FelderaClient, pipeline: str) -> None:
        """
        Stop a running pipeline.

        :param client: The Feldera REST client.
        :param pipeline: The pipeline (schema) name.
        """
        with self._lock:
            if pipeline in self._pipelines:
                try:
                    self._pipelines[pipeline].stop(force=True)
                    logger.info("Pipeline '%s' stopped", pipeline)
                except Exception as exc:
                    logger.warning("Failed to stop pipeline '%s': %s", pipeline, exc)
            else:
                try:
                    p = Pipeline.get(pipeline, client)
                    p.stop(force=True)
                    logger.info("Pipeline '%s' stopped", pipeline)
                except FelderaAPIError:
                    logger.debug("Pipeline '%s' not found, nothing to stop", pipeline)

    def clear_storage(self, client: FelderaClient, pipeline: str) -> None:
        """
        Clear the storage of a pipeline, forcing full recomputation on next start.

        :param client: The Feldera REST client.
        :param pipeline: The pipeline (schema) name.
        """
        with self._lock:
            try:
                p = Pipeline.get(pipeline, client)
                p.clear_storage()
                logger.info("Storage cleared for pipeline '%s'", pipeline)
            except FelderaAPIError:
                logger.debug("Pipeline '%s' not found, nothing to clear", pipeline)

    def get_pipeline(self, client: FelderaClient, pipeline: str) -> Optional[Pipeline]:
        """
        Retrieve a pipeline by name.

        :param client: The Feldera REST client.
        :param pipeline: The pipeline (schema) name.
        :return: The Pipeline object, or None if not found.
        """
        with self._lock:
            if pipeline in self._pipelines:
                return self._pipelines[pipeline]
            try:
                p = Pipeline.get(pipeline, client)
                self._pipelines[pipeline] = p
                return p
            except FelderaAPIError:
                return None
