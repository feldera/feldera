import logging
import threading
import time
from typing import Dict, List, Optional, Tuple

import agate

from feldera.enums import CompilationProfile, PipelineFieldSelector, PipelineStatus, ProgramStatus
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
        """``{pipeline_name: {table_name: CREATE TABLE DDL}}``"""
        self._views: Dict[str, Dict[str, str]] = {}
        """``{pipeline_name: {view_name: CREATE VIEW DDL}}``"""
        self._deployed_sql: Dict[str, str] = {}
        """``{pipeline_name: last_deployed_sql_program}``"""
        self._pipelines: Dict[str, Pipeline] = {}
        """``{pipeline_name: Pipeline SDK object}``"""
        self._pending_seeds: Dict[str, List[Tuple[str, agate.Table, dict]]] = {}
        """``{pipeline_name: [(table_name, agate_table, {col: sql_type})]}``"""

    def register_table(self, pipeline: str, name: str, ddl: str) -> None:
        """
        Register a CREATE TABLE statement for a pipeline.

        Raises :class:`ValueError` if a table with the same name is already
        registered for this pipeline.  Feldera's SQL compiler also rejects
        duplicate table names (``Duplicate declaration``), so catching
        duplicates here gives a clearer error earlier.

        :param pipeline: The pipeline (schema) name.
        :param name: The table name.
        :param ddl: The full CREATE TABLE DDL statement.
        :raises ValueError: If a table with *name* is already registered.
        """
        with self._lock:
            existing = self._tables.get(pipeline, {})
            if name in existing:
                raise ValueError(f"Table '{name}' is already registered in the pipeline: '{pipeline}'. ")
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

    def remove_table_if_exists(self, pipeline: str, name: str) -> None:
        """
        Remove a table from the pipeline state if it exists.

        No-op when the table is not registered.

        :param pipeline: The pipeline (schema) name.
        :param name: The table name to remove.
        """
        with self._lock:
            if pipeline in self._tables:
                self._tables[pipeline].pop(name, None)

    def remove_view_if_exists(self, pipeline: str, name: str) -> None:
        """
        Remove a view from the pipeline state if it exists.

        No-op when the view is not registered.

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
        :return: A dict mapping table name (``str``) to ``CREATE TABLE``
            DDL string (``str``). Empty dict if the pipeline has no tables.
        """
        with self._lock:
            return dict(self._tables.get(pipeline, {}))

    def get_views(self, pipeline: str) -> Dict[str, str]:
        """
        Return a copy of all registered views for a pipeline.

        :param pipeline: The pipeline (schema) name.
        :return: A dict mapping view name (``str``) to ``CREATE VIEW``
            DDL string (``str``). Empty dict if the pipeline has no views.
        """
        with self._lock:
            return dict(self._views.get(pipeline, {}))

    def assemble_program(self, pipeline: str) -> str:
        """
        Assemble all registered tables and views into a pipeline SQL program.

        Tables are emitted first (inputs), then views (transforms).
        Views are emitted in registration (insertion) order — dbt resolves
        the dependency graph before calling materializations, so views
        arrive in the correct dependency order.

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
        :return: A list of ``(table_name, agate_table, column_types)`` tuples
            where *column_types* maps column name → SQL type string override.
            Returns an empty list if no seeds are pending.
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

        Creates or replaces the pipeline with the current SQL program, waits
        for compilation to succeed, and starts the pipeline.  After
        ``create_or_replace`` the pipeline is in a stopped state with storage
        cleared, so ``start()`` brings it up from a clean slate.

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
        full_refresh: bool = False,
    ) -> Pipeline:
        """
        Update and redeploy an existing pipeline with new views.

        Preserves table data and connector offsets unless *full_refresh* is
        set.  Unlike :meth:`deploy`, this does **not** call
        ``create_or_replace``. The full lifecycle is:

        1. Stops the pipeline if running, via ``force=True``.
        2. Fetches the existing SQL and extracts table DDLs.
        3. Replaces existing view DDLs with newly registered ones.
        4. Optionally clears storage (on ``--full-refresh``).
        5. Recompiles via ``modify(sql=...)``.
        6. Starts the pipeline.

        The pipeline can be in any state. Running pipelines are
        force-stopped before modification.
        
        If the pipeline does not exist, it is deployed first.

        :param client: The Feldera REST client.
        :param pipeline: The pipeline (schema) name.
        :param compilation_profile: The compilation profile to use.
        :param workers: Number of worker threads for the pipeline.
        :param timeout: Compilation timeout in seconds.
        :param full_refresh: If True, clear all pipeline storage before
            restarting so state is recomputed from scratch.
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

            # Stop the pipeline before modifying its SQL
            try:
                p.stop(force=True)
            except Exception:
                pass  # May already be stopped

            # On full-refresh, clear all stored state so the pipeline
            # recomputes everything from scratch when restarted.
            if full_refresh:
                try:
                    p.clear_storage()
                    logger.info("Cleared storage for pipeline '%s' (full-refresh)", pipeline)
                except Exception as exc:
                    logger.warning("Failed to clear storage for pipeline '%s': %s", pipeline, exc)

            # Update SQL and wait for recompilation
            p.modify(sql=full_sql)
            self._wait_for_compilation(p, timeout)

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

        Quoted identifiers preserve their original case; unquoted
        identifiers are lowercased (SQL standard).

        :param table_ddls: List of DDL strings (e.g., ``CREATE TABLE "foo" (...);``).
        :return: A set of table names (case-sensitive for quoted, lowercase for unquoted).
        """
        from dbt.adapters.feldera.sqlglot_parser import parser

        return parser.extract_table_names(table_ddls)

    @staticmethod
    def _wait_for_compilation(
        pipeline: Pipeline,
        timeout: int,
        poll_interval: float = 1.0,
    ) -> None:
        """
        Wait for a pipeline to finish compiling using the Pipeline SDK.

        Polls ``pipeline.program_status()`` until compilation succeeds or
        fails, raising on error or timeout.

        :param pipeline: The Pipeline object to monitor.
        :param timeout: Maximum seconds to wait for compilation.
        :param poll_interval: Seconds between status polls.
        :raises TimeoutError: If compilation does not finish within *timeout*.
        :raises RuntimeError: If compilation fails (SQL error, Rust error, etc.).
        """
        compiling_states = (
            ProgramStatus.Pending,
            ProgramStatus.CompilingSql,
            ProgramStatus.SqlCompiled,
            ProgramStatus.CompilingRust,
        )

        start_time = time.monotonic()
        while True:
            elapsed = time.monotonic() - start_time
            if elapsed > timeout:
                raise TimeoutError(f"Timed out waiting for pipeline '{pipeline.name}' to compile after {timeout}s")

            status = pipeline.program_status()

            if status == ProgramStatus.Success:
                pipeline.refresh(PipelineFieldSelector.ALL)
                return

            if status not in compiling_states:
                error = pipeline.program_error()
                error_message = f"Pipeline '{pipeline.name}' failed to compile: {status}\n"

                sql_errors = error.get("sql_compilation", {}).get("messages")
                if sql_errors:
                    for sql_error in sql_errors:
                        error_message += (
                            f"{sql_error.get('error_type', '')}\n"
                            f"{sql_error.get('message', '')}\n"
                            f"Code snippet:\n{sql_error.get('snippet', '')}\n"
                        )
                    raise RuntimeError(error_message)

                rust_error = error.get("rust_compilation")
                if rust_error is not None:
                    error_message += f"Rust Error: {rust_error}\n"

                system_error = error.get("system_error")
                if system_error is not None:
                    error_message += f"System Error: {system_error}"

                raise RuntimeError(error_message)

            logger.debug(
                "Pipeline '%s' still compiling (status=%s), waiting %.1fs",
                pipeline.name,
                status,
                poll_interval,
            )
            time.sleep(poll_interval)

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
