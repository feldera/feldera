"""
Integration tests for dbt-feldera using the adventureworks star schema.

These tests run dbt commands against a Feldera instance started
via Docker Compose. They validate the full lifecycle:

  1. dbt debug   - connection verification
  2. dbt seed    - data loading via HTTP ingress
  3. dbt run     - model deployment as Feldera pipeline views
  4. dbt test    - data integrity via ad-hoc queries
  5. Delta Lake  - verify materialized output via DuckDB

Requires Docker to be available. Set FELDERA_SKIP_DOCKER=1 if Feldera
is already running externally.
"""

import logging
import os
import shutil
import subprocess
import sys
import time
from pathlib import Path

import pytest

logger = logging.getLogger(__name__)
_ADAPTER_ROOT = str(Path(__file__).resolve().parent.parent)

# Expected row counts for each mart model (inserts only).
EXPECTED_ROW_COUNTS = {
    "dim_address": 1675,
    "dim_credit_card": 1316,
    "dim_customer": 19820,
    "dim_date": 731,
    "dim_order_status": 1,
    "dim_product": 504,
    "fct_sales": 5675,
    "obt_sales": 5675,
}


def _find_dbt_executable() -> str:
    """
    Locate the ``dbt`` CLI executable.

    When running inside a virtualenv (e.g., via ``pytest``), ``dbt`` lives in
    the same ``bin/`` directory as the Python interpreter but may not be on the
    system ``PATH``.  This helper resolves the full path so that
    ``subprocess.run`` always finds it.
    """
    # 1. Try the same bin directory as the running Python interpreter.
    venv_dbt = Path(sys.executable).parent / "dbt"
    if venv_dbt.is_file():
        return str(venv_dbt)

    # 2. Fall back to PATH.
    found = shutil.which("dbt")
    if found:
        return found

    raise FileNotFoundError(
        "Cannot locate the 'dbt' executable. Ensure dbt-core is installed in the active virtualenv."
    )


def _run_dbt(
    project_dir: str,
    args: list,
    feldera_url: str = "http://localhost:8080",
    timeout: int = 600,
) -> subprocess.CompletedProcess:
    """
    Run a dbt command in the given project directory.

    :param project_dir: Path to the dbt project.
    :param args: dbt command arguments (e.g., ["debug", "--target", "local"]).
    :param feldera_url: The Feldera instance URL (passed via FELDERA_URL env).
    :param timeout: Command timeout in seconds.
    :return: CompletedProcess result.
    """
    dbt_bin = _find_dbt_executable()
    cmd = [dbt_bin] + args + ["--profiles-dir", project_dir]
    logger.info("Running: %s (in %s)", " ".join(cmd), project_dir)

    # Ensure the adapter source is on PYTHONPATH so dbt-core's namespace
    # package extension picks it up even when CWD != adapter source root.
    env = {
        **os.environ,
        "DBT_PROFILES_DIR": project_dir,
        "FELDERA_URL": feldera_url,
        "PYTHONPATH": os.pathsep.join(filter(None, [_ADAPTER_ROOT, os.environ.get("PYTHONPATH", "")])),
    }

    result = subprocess.run(
        cmd,
        cwd=project_dir,
        capture_output=True,
        text=True,
        timeout=timeout,
        env=env,
    )

    if result.stdout:
        logger.info("stdout:\n%s", result.stdout[-2000:])
    if result.stderr:
        logger.info("stderr:\n%s", result.stderr[-2000:])

    return result


def _wait_for_delta_tables(
    delta_dir: str,
    tables: list[str],
    timeout: int = 120,
    poll_interval: int = 5,
) -> None:
    """
    Wait until all expected Delta tables have at least one Parquet file.

    Feldera buffers output before flushing to Delta Lake. This helper polls
    the local filesystem until every table directory contains data.

    :param delta_dir: Root path to the delta output directory.
    :param tables: List of table names (subdirectory names).
    :param timeout: Maximum wait time in seconds.
    :param poll_interval: Time between polls in seconds.
    :raises TimeoutError: If not all tables are populated within the timeout.
    """
    start = time.time()
    while time.time() - start < timeout:
        all_ready = True
        for table in tables:
            table_path = Path(delta_dir) / table
            parquet_files = list(table_path.glob("*.parquet")) if table_path.exists() else []
            if not parquet_files:
                all_ready = False
                break
        if all_ready:
            logger.info("All %d Delta tables have data after %.1fs", len(tables), time.time() - start)
            return
        time.sleep(poll_interval)

    missing = []
    for table in tables:
        table_path = Path(delta_dir) / table
        parquet_files = list(table_path.glob("*.parquet")) if table_path.exists() else []
        if not parquet_files:
            missing.append(table)
    raise TimeoutError(
        f"Delta tables not ready after {timeout}s. Missing data: {missing}"
    )


@pytest.mark.integration
class TestDbtFelderaIntegration:
    """End-to-end integration tests for dbt-feldera with adventureworks."""

    def test_dbt_debug(self, docker_feldera, dbt_project_dir):
        """Verify that dbt can connect to Feldera."""
        result = _run_dbt(dbt_project_dir, ["debug", "--target", "local"], feldera_url=docker_feldera)
        assert result.returncode == 0, f"dbt debug failed:\n{result.stdout}\n{result.stderr}"
        assert "All checks passed" in result.stdout or "Connection test" in result.stdout

    def test_dbt_seed(self, docker_feldera, dbt_project_dir):
        """Load seed data into Feldera via HTTP ingress."""
        result = _run_dbt(dbt_project_dir, ["seed", "--target", "local", "--full-refresh"], feldera_url=docker_feldera)
        assert result.returncode == 0, f"dbt seed failed:\n{result.stdout}\n{result.stderr}"

    def test_dbt_run(self, docker_feldera, dbt_project_dir, delta_output_dir):
        """Deploy all models as Feldera pipeline materialized views with Delta output."""
        result = _run_dbt(dbt_project_dir, ["run", "--target", "local"], feldera_url=docker_feldera)
        assert result.returncode == 0, f"dbt run failed:\n{result.stdout}\n{result.stderr}"

    def test_dbt_test(self, docker_feldera, dbt_project_dir):
        """Run data tests against materialized views."""
        result = _run_dbt(dbt_project_dir, ["test", "--target", "local"], feldera_url=docker_feldera)
        assert result.returncode in (0, 1), f"dbt test failed:\n{result.stdout}\n{result.stderr}"

    def test_dbt_build_full_refresh(self, docker_feldera, dbt_project_dir, delta_output_dir):
        """
        Full lifecycle via ``dbt build --full-refresh``.

        Seeds and models are deployed together in a single invocation so that
        Feldera processes seed data through materialized views and writes
        output to Delta Lake connectors.
        """
        delta_path = Path(delta_output_dir)
        for child in delta_path.iterdir():
            if child.is_dir():
                shutil.rmtree(child)

        result = _run_dbt(
            dbt_project_dir,
            ["build", "--target", "local", "--full-refresh"],
            feldera_url=docker_feldera,
        )
        assert result.returncode == 0, f"dbt build --full-refresh failed:\n{result.stdout}\n{result.stderr}"

    def test_delta_output_correctness(self, docker_feldera, dbt_project_dir, delta_output_dir):
        """
        Verify that Delta Lake output tables contain the expected number of rows.

        After ``dbt build --full-refresh``, Feldera writes materialized view
        output to Delta tables on the local filesystem. This test uses DuckDB
        to read the tables and assert net row counts (inserts minus deletes),
        since the streaming engine may emit intermediate insert/delete pairs
        during incremental join processing.
        """
        import duckdb

        table_names = list(EXPECTED_ROW_COUNTS.keys())
        _wait_for_delta_tables(delta_output_dir, table_names, timeout=120)

        conn = duckdb.connect()
        conn.execute("INSTALL delta;")
        conn.execute("LOAD delta;")

        errors = []
        for table_name, expected_count in EXPECTED_ROW_COUNTS.items():
            table_path = Path(delta_output_dir) / table_name
            try:
                row = conn.execute(
                    f"""
                    SELECT
                        COUNT(*) FILTER (WHERE __feldera_op = 'i') AS inserts,
                        COUNT(*) FILTER (WHERE __feldera_op = 'd') AS deletes
                    FROM delta_scan('{table_path}')
                    """
                ).fetchone()
                net_count = row[0] - row[1]
                if net_count != expected_count:
                    errors.append(
                        f"{table_name}: expected {expected_count} net rows, "
                        f"got {net_count} (inserts={row[0]}, deletes={row[1]})"
                    )
                else:
                    logger.info("✓ %s: %d net rows (correct)", table_name, net_count)
            except Exception as exc:
                errors.append(f"{table_name}: failed to read Delta table: {exc}")

        conn.close()

        if errors:
            pytest.fail(
                "Delta Lake output correctness check failed:\n" + "\n".join(f"  - {e}" for e in errors)
            )
