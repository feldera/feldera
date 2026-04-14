"""
DuckDB client that executes queries inside a Docker sidecar container.

The DuckDB sidecar shares a Docker volume with the Feldera pipeline-manager,
so it can read Delta Lake tables written by Feldera.
"""

import json
import logging
import subprocess
import time
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)

DELTA_ROOT = "/data/delta"
COMPOSE_FILE = str(Path(__file__).parent / "docker-compose.yml")
PROJECT_NAME = "feldera-dbt-test"


class DockerDuckDB:
    """
    Execute DuckDB SQL inside a Docker Compose sidecar container.

    Each call to :meth:`sql` runs ``docker compose exec duckdb duckdb -json``
    and returns the parsed JSON result rows.

    :param compose_file: Path to the docker-compose.yml file.
    :param project_name: Docker Compose project name.
    :param service: Name of the DuckDB service in docker-compose.yml.
    """

    def __init__(
        self,
        compose_file: str = COMPOSE_FILE,
        project_name: str = PROJECT_NAME,
        service: str = "duckdb",
    ) -> None:
        self._compose_file = compose_file
        self._project_name = project_name
        self._service = service

    def sql(self, query: str, timeout: int = 30) -> list[dict[str, Any]]:
        """
        Execute a SQL query via the DuckDB sidecar and return result rows.

        The ``delta`` extension is pre-installed at container startup and
        auto-loaded by DuckDB when ``delta_scan()`` is used.

        :param query: The SQL query to execute.
        :param timeout: Command timeout in seconds.
        :return: A list of row dicts (column name → value).
        :raises RuntimeError: If the command fails.
        """
        cmd = [
            "docker",
            "compose",
            "-f",
            self._compose_file,
            "-p",
            self._project_name,
            "exec",
            "-T",
            self._service,
            "/duckdb",
            "-json",
            "-c",
            query,
        ]

        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=timeout,
        )

        if result.returncode != 0:
            raise RuntimeError(
                f"DuckDB query failed (exit {result.returncode}):\nSQL: {query}\nstderr: {result.stderr[:1000]}"
            )

        output = result.stdout.strip()
        if not output or output == "[]":
            return []

        try:
            return json.loads(output)
        except json.JSONDecodeError as exc:
            raise RuntimeError(f"Failed to parse DuckDB JSON output: {exc}\nraw output: {output[:500]}") from exc

    def delta_path(self, table_name: str) -> str:
        """Return the in-container path for a Delta table."""
        return f"{DELTA_ROOT}/{table_name}"

    def get_net_count(self, table_name: str) -> int:
        """
        Return the net row count (inserts minus deletes) for a Delta table.

        :param table_name: The Delta table subdirectory name.
        :return: Net row count.
        """
        path = self.delta_path(table_name)
        rows = self.sql(
            f"""
            SELECT
                COUNT(*) FILTER (WHERE __feldera_op = 'i') AS inserts,
                COUNT(*) FILTER (WHERE __feldera_op = 'd') AS deletes
            FROM delta_scan('{path}')
            """
        )
        if not rows:
            return 0
        return int(rows[0]["inserts"]) - int(rows[0]["deletes"])

    def get_delta_version(self, table_name: str) -> int:
        """
        Return the current version of a Delta table by counting log entries.

        :param table_name: The Delta table subdirectory name.
        :return: The current Delta table version (0-based), or -1 if empty.
        """
        path = self.delta_path(table_name)
        rows = self.sql(f"SELECT count(*) AS n FROM glob('{path}/_delta_log/*.json')")
        if not rows or int(rows[0]["n"]) == 0:
            return -1
        return int(rows[0]["n"]) - 1

    def list_parquet_files(self, table_name: str) -> set[str]:
        """
        Return the set of Parquet file names in a Delta table directory.

        :param table_name: The Delta table subdirectory name.
        :return: Set of file name strings.
        """
        path = self.delta_path(table_name)
        rows = self.sql(f"SELECT replace(file, '{path}/', '') AS name FROM glob('{path}/*.parquet') AS t(file)")
        return {r["name"] for r in rows}

    def has_parquet_files(self, table_name: str) -> bool:
        """Check whether a Delta table has at least one Parquet data file."""
        try:
            path = self.delta_path(table_name)
            rows = self.sql(f"SELECT 1 AS ok FROM delta_scan('{path}') LIMIT 1")
            return len(rows) > 0
        except Exception:
            return False

    def wait_for_tables(
        self,
        table_names: list[str],
        timeout: int = 120,
        poll_interval: int = 5,
    ) -> None:
        """
        Poll until all Delta tables have at least one Parquet file.

        :param table_names: List of table names (subdirectory names).
        :param timeout: Maximum wait time in seconds.
        :param poll_interval: Time between polls in seconds.
        :raises TimeoutError: If not all tables are populated in time.
        """
        start = time.time()
        while time.time() - start < timeout:
            missing = [t for t in table_names if not self.has_parquet_files(t)]
            if not missing:
                logger.info(
                    "All %d Delta tables have data after %.1fs",
                    len(table_names),
                    time.time() - start,
                )
                return
            time.sleep(poll_interval)

        missing = [t for t in table_names if not self.has_parquet_files(t)]
        raise TimeoutError(f"Delta tables not ready after {timeout}s. Missing: {missing}")

    def wait_for_net_count(
        self,
        table_name: str,
        min_count: int,
        timeout: int = 120,
        poll_interval: int = 5,
    ) -> int:
        """
        Poll a Delta table until its net count reaches *min_count*.

        :param table_name: The Delta table subdirectory name.
        :param min_count: Minimum expected net row count.
        :param timeout: Maximum wait time in seconds.
        :param poll_interval: Seconds between polls.
        :return: The actual net count once threshold is met.
        :raises TimeoutError: If not reached in time.
        """
        start = time.time()
        last_count = 0
        while time.time() - start < timeout:
            try:
                last_count = self.get_net_count(table_name)
                if last_count >= min_count:
                    logger.info(
                        "Delta '%s' net count reached %d (target: %d) after %.1fs",
                        table_name,
                        last_count,
                        min_count,
                        time.time() - start,
                    )
                    return last_count
            except Exception as exc:
                logger.debug("Delta read failed (retrying): %s", exc)
            time.sleep(poll_interval)

        raise TimeoutError(
            f"Delta '{table_name}' net count did not reach {min_count} within {timeout}s (last: {last_count})"
        )
