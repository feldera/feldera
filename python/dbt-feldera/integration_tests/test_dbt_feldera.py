"""
Integration tests for dbt-feldera using the adventureworks star schema.

These tests run dbt commands against a Feldera instance started
via Docker Compose. They validate the full lifecycle:

  1. dbt debug   - connection verification
  2. dbt seed    - data loading via HTTP ingress
  3. dbt run     - model deployment as Feldera pipeline views
  4. dbt test    - data integrity via ad-hoc queries
  5. Delta Lake  - verify materialized output via DuckDB
  6. Kafka       - incremental view maintenance via Kafka to Delta

Requires Docker to be available. Set FELDERA_SKIP_DOCKER=1 if Feldera
is already running externally.
"""

import json
import logging
import os
import random
import shutil
import subprocess
import sys
import time
import urllib.error
import urllib.request
from pathlib import Path

import pytest
from docker_manager import PROJECT_NAME, DockerManager, Service

logger = logging.getLogger(__name__)
_ADAPTER_ROOT = str(Path(__file__).resolve().parent.parent)

# Expected row counts for each mart model (seed-only, net inserts minus deletes).
EXPECTED_ROW_COUNTS = {
    "dim_address": 1675,
    "dim_credit_card": 1316,
    "dim_customer": 19820,
    "dim_date": 731,
    "dim_order_status": 1,
    "dim_product": 504,
    "fct_recent_sales": 1566,
    "fct_sales": 5675,
    "obt_sales": 5675,
}

# Test timeouts
_DBT_COMMAND_TIMEOUT_SECONDS = 600
_PIPELINE_IDLE_TIMEOUT_SECONDS = 120.0
_PIPELINE_SHUTDOWN_TIMEOUT_SECONDS = 60
_DUCKDB_TIMEOUT_SECONDS = 120
_KAFKA_TOPIC_VERIFY_TIMEOUT_SECONDS = 120
_KAFKA_TOPIC_VERIFY_POLL_SECONDS = 5

# Valid FK references into seed dimension data for generating Kafka records.
_VALID_PRODUCT_IDS = [776, 777, 778, 771, 772, 773, 774, 775]
_VALID_CUSTOMER_IDS = [29825, 29672, 29734, 29994, 30089, 30052]
_VALID_CREDIT_CARD_IDS = [16281, 5618, 1, 2, 3, 4]
_VALID_ADDRESS_IDS = [985, 921, 613, 446, 940, 606]


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
    timeout: int = _DBT_COMMAND_TIMEOUT_SECONDS,
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
        "DBT_THREADS": os.environ.get("DBT_THREADS", "4"),
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


# ---------------------------------------------------------------------------
# Kafka helpers
# ---------------------------------------------------------------------------


def _produce_to_kafka(records: list[dict], topic: str, kafka_proxy_url: str) -> None:
    """
    Produce JSON records to a Kafka topic via Kafka proxy HTTP API.

    :param records: List of dicts to produce as JSON values.
    :param topic: The Kafka topic name.
    :param kafka_proxy_url: Base URL of the Kafka proxy (e.g., http://localhost:18082).
    :raises RuntimeError: If the produce request fails.
    """
    url = f"{kafka_proxy_url}/topics/{topic}"
    payload = json.dumps({"records": [{"value": r} for r in records]}).encode()
    req = urllib.request.Request(
        url,
        data=payload,
        headers={"Content-Type": "application/vnd.kafka.json.v2+json"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            body = json.loads(resp.read())
            errors = [o for o in body.get("offsets", []) if o.get("error")]
            if errors:
                raise RuntimeError(f"Kafka produce errors: {errors}")
            logger.info("Produced %d records to topic '%s'", len(records), topic)
    except urllib.error.HTTPError as exc:
        raise RuntimeError(f"Kafka proxy POST to {url} failed ({exc.code}): {exc.read().decode()}") from exc


def _ensure_kafka_topic(topic: str, kafka_proxy_url: str) -> None:
    """
    Ensure a Kafka topic exists before a Feldera pipeline tries to consume it.

    Uses ``rpk`` inside the Redpanda container to create the topic, with
    a fallback to the Kafka proxy producer API. Verifies the topic exists
    via Kafka proxy before returning.

    :param topic: The Kafka topic name.
    :param kafka_proxy_url: Base URL of the Kafka proxy (used to verify topic exists).
    :raises RuntimeError: If the topic cannot be created or verified.
    """
    compose_file = str(Path(__file__).parent / "docker-compose.yml")
    manager = DockerManager(compose_file=compose_file)

    # Try creating via rpk inside the Redpanda container (most reliable).
    try:
        result = subprocess.run(
            [
                "docker",
                "compose",
                "-f",
                compose_file,
                "-p",
                PROJECT_NAME,
                "exec",
                "-T",
                Service.REDPANDA,
                "rpk",
                "topic",
                "create",
                topic,
                "--brokers",
                "localhost:29092",
            ],
            capture_output=True,
            text=True,
            timeout=15,
        )
        if result.returncode == 0:
            logger.info("Created Kafka topic '%s' via rpk", topic)
        elif "exists" in result.stderr.lower() or "exists" in result.stdout.lower():
            logger.info("Kafka topic '%s' already exists", topic)
        else:
            logger.warning("rpk topic create returned %d: %s %s", result.returncode, result.stdout, result.stderr)
    except Exception as exc:
        logger.warning("rpk topic create failed: %s", exc)

    # Verify the topic exists via Kafka proxy
    max_attempts = _KAFKA_TOPIC_VERIFY_TIMEOUT_SECONDS // _KAFKA_TOPIC_VERIFY_POLL_SECONDS
    for attempt in range(max_attempts):
        try:
            with urllib.request.urlopen(f"{kafka_proxy_url}/topics", timeout=10) as resp:
                topics = json.loads(resp.read())
                if topic in topics:
                    logger.info("Verified Kafka topic '%s' exists", topic)
                    return
        except Exception as exc:
            logger.debug("Topic verification attempt %d failed: %s", attempt + 1, exc)
        time.sleep(_KAFKA_TOPIC_VERIFY_POLL_SECONDS)

    # Fetch container logs for diagnostics before raising.
    try:
        container_logs = manager.logs(service=Service.REDPANDA)
    except Exception as exc:
        container_logs = f"(failed to retrieve container logs: {exc})"

    logger.error(
        "Failed to verify Kafka topic '%s' after %d attempts.\n--- Redpanda container logs (last 100 lines) ---\n%s",
        topic,
        max_attempts,
        container_logs,
    )
    raise RuntimeError(
        f"Kafka topic '{topic}' could not be created or verified. See Redpanda container logs above for details."
    )


def _generate_sales_records(count: int, start_order_id: int = 99001) -> list[dict]:
    """
    Generate random sales event records with valid FK references to seed data.

    Each record represents one order line-item that can flow through
    ``fct_sales`` → ``obt_sales``.

    :param count: Number of records to generate.
    :param start_order_id: Starting salesorderid (should be above seed range).
    :return: List of record dicts matching the ``kafka_sales`` table schema.
    """
    records = []
    for i in range(count):
        records.append(
            {
                "salesorderid": start_order_id + i,
                "salesorderdetailid": 200_000 + i,
                "productid": random.choice(_VALID_PRODUCT_IDS),
                "customerid": random.choice(_VALID_CUSTOMER_IDS),
                "creditcardid": random.choice(_VALID_CREDIT_CARD_IDS),
                "shiptoaddressid": random.choice(_VALID_ADDRESS_IDS),
                "order_status": 5,
                "orderdate": "2024-01-15",
                "orderqty": random.randint(1, 10),
                "unitprice": round(random.uniform(10.0, 2000.0), 2),
            }
        )
    return records


def _stop_feldera_pipeline(feldera_url: str, pipeline_name: str) -> None:
    """
    Stop a Feldera pipeline.

    Uses a graceful stop (``force=False``) so a checkpoint is created
    before the pipeline shuts down.

    :param feldera_url: Base URL of the Feldera instance.
    :param pipeline_name: Name of the pipeline to stop.
    :raises TimeoutError: If the pipeline does not stop within 60 seconds.
    """
    from feldera.enums import PipelineStatus
    from feldera.pipeline import Pipeline as FelderaPipeline
    from feldera.rest.feldera_client import FelderaClient

    client = FelderaClient(url=feldera_url)
    try:
        p = FelderaPipeline.get(pipeline_name, client)
        p.stop(force=False)
        p.wait_for_status(PipelineStatus.SHUTDOWN, timeout=_PIPELINE_SHUTDOWN_TIMEOUT_SECONDS)
        logger.info("Pipeline '%s' is stopped", pipeline_name)
    except Exception as exc:
        logger.warning("Failed to stop pipeline '%s': %s", pipeline_name, exc)


def _wait_for_pipeline_idle(
    feldera_url: str,
    pipeline_name: str,
    timeout_s: float = _PIPELINE_IDLE_TIMEOUT_SECONDS,
    poll_interval_s: float = 0.5,
) -> None:
    """
    Poll the pipeline until all currently buffered input has been processed.

    Takes a snapshot of ``total_input_records`` (which already includes all
    data pushed by the preceding ``dbt`` command, since ``input_json`` is a
    synchronous HTTP POST) and waits until ``total_processed_records``
    catches up.  Returns as soon as the condition is met.

    This intentionally avoids the two built-in SDK wait helpers:

    * ``Pipeline.wait_for_idle()`` requires both counters to be
      **unchanged** *and* **equal**.  Kafka connectors emit ~1 heartbeat
      record per second, so both counters increment in lockstep and the
      "unchanged" condition is never satisfied — causing a guaranteed
      timeout.

    * ``Pipeline.wait_for_completion()`` checks the ``pipeline_complete``
      flag, which requires every input connector to signal end-of-input.
      Streaming connectors like Kafka never do, so it blocks forever.

    The snapshot approach sidesteps both issues: we capture the current
    ``total_input_records`` (our processing target) and poll until
    ``total_processed_records >= target``.  Kafka heartbeats that arrive
    after the snapshot are irrelevant — we only need the seed data that
    was already accepted to be fully processed.

    :param feldera_url: Base URL of the Feldera instance.
    :param pipeline_name: Name of the pipeline to poll.
    :param timeout_s: Maximum time to wait before raising.
    :param poll_interval_s: Seconds between polls.
    :raises RuntimeError: If the pipeline does not catch up in time.
    """
    from feldera.pipeline import Pipeline as FelderaPipeline
    from feldera.rest.feldera_client import FelderaClient

    client = FelderaClient(url=feldera_url)
    p = FelderaPipeline.get(pipeline_name, client)

    # Snapshot: all seed data has been accepted by the time dbt returns,
    # so total_input_records is our processing target.
    target = p.stats().global_metrics.total_input_records
    logger.info(
        "Waiting for pipeline '%s' to process %d input records",
        pipeline_name,
        target,
    )

    start = time.time()
    while True:
        metrics = p.stats().global_metrics
        processed = metrics.total_processed_records
        if processed >= target:
            logger.info(
                "Pipeline '%s' caught up: processed=%d >= target=%d (%.1fs)",
                pipeline_name,
                processed,
                target,
                time.time() - start,
            )
            return

        if time.time() - start > timeout_s:
            raise RuntimeError(
                f"Pipeline '{pipeline_name}' did not finish processing "
                f"within {timeout_s}s (processed={processed}, target={target})"
            )

        time.sleep(poll_interval_s)


# ---------------------------------------------------------------------------
# Integration tests
# ---------------------------------------------------------------------------


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

    def test_dbt_run(self, docker_feldera, dbt_project_dir, docker_duckdb, kafka_proxy_url):
        """Deploy all models as Feldera pipeline materialized views with Delta output."""
        _ensure_kafka_topic("sales_events", kafka_proxy_url)
        result = _run_dbt(dbt_project_dir, ["run", "--target", "local"], feldera_url=docker_feldera)
        assert result.returncode == 0, f"dbt run failed:\n{result.stdout}\n{result.stderr}"

    def test_dbt_test(self, docker_feldera, dbt_project_dir):
        """Run data tests against materialized views."""
        _wait_for_pipeline_idle(docker_feldera, "adventureworks")
        result = _run_dbt(dbt_project_dir, ["test", "--target", "local"], feldera_url=docker_feldera)
        assert result.returncode in (0, 1), f"dbt test failed:\n{result.stdout}\n{result.stderr}"

    def test_dbt_build_full_refresh(self, docker_feldera, dbt_project_dir, docker_duckdb, kafka_proxy_url):
        """
        Full lifecycle via ``dbt build --full-refresh``.

        Seeds and models are deployed together in a single invocation so that
        Feldera processes seed data through materialized views and writes
        output to Delta Lake connectors.
        """
        _ensure_kafka_topic("sales_events", kafka_proxy_url)

        result = _run_dbt(
            dbt_project_dir,
            ["build", "--target", "local", "--full-refresh"],
            feldera_url=docker_feldera,
        )
        assert result.returncode == 0, f"dbt build --full-refresh failed:\n{result.stdout}\n{result.stderr}"

        _wait_for_pipeline_idle(docker_feldera, "adventureworks")

    def test_delta_output_correctness(self, docker_feldera, dbt_project_dir, docker_duckdb):
        """
        Verify that Delta Lake output tables contain the expected number of rows.

        After ``dbt build --full-refresh``, Feldera writes materialized view
        output to Delta tables in the shared Docker volume. This test uses
        the DuckDB sidecar to read the tables and assert net row counts
        (inserts minus deletes).
        """
        _wait_for_pipeline_idle(docker_feldera, "adventureworks")

        table_names = list(EXPECTED_ROW_COUNTS.keys())
        docker_duckdb.wait_for_tables(table_names, timeout=_DUCKDB_TIMEOUT_SECONDS)

        errors = []
        for table_name, expected_count in EXPECTED_ROW_COUNTS.items():
            try:
                net_count = docker_duckdb.get_net_count(table_name)
                if net_count != expected_count:
                    errors.append(f"{table_name}: expected {expected_count} net rows, got {net_count}")
                else:
                    logger.info("✓ %s: %d net rows (correct)", table_name, net_count)
            except Exception as exc:
                errors.append(f"{table_name}: failed to read Delta table: {exc}")

        if errors:
            pytest.fail("Delta Lake output correctness check failed:\n" + "\n".join(f"  - {e}" for e in errors))

    def test_kafka_ivm_auto_consume(self, docker_feldera, dbt_project_dir, docker_duckdb, kafka_proxy_url):
        """
        IVM test: Kafka data is auto-consumed by the running pipeline.

        After ``dbt build --full-refresh``, the pipeline is running with
        ``start_from: latest`` on the Kafka connector. When we produce
        records, the pipeline consumes them immediately and incrementally
        updates the Delta output.

        Verifies:
        - New rows appear in obt_sales Delta (net count increases)
        - Delta version increases (new commit with new Parquet files)
        - No existing Parquet files are removed (proves incremental write)
        """
        docker_duckdb.wait_for_tables(["obt_sales", "fct_sales"], timeout=_DUCKDB_TIMEOUT_SECONDS)

        pre_obt_net = docker_duckdb.get_net_count("obt_sales")
        pre_fct_net = docker_duckdb.get_net_count("fct_sales")
        pre_obt_files = docker_duckdb.list_parquet_files("obt_sales")

        logger.info(
            "Pre-Kafka state: obt_sales=%d net rows (%d files), fct_sales=%d net rows",
            pre_obt_net,
            len(pre_obt_files),
            pre_fct_net,
        )

        kafka_batch_1 = _generate_sales_records(count=10, start_order_id=99001)
        _produce_to_kafka(kafka_batch_1, "sales_events", kafka_proxy_url)

        expected_fct_net = pre_fct_net + len(kafka_batch_1)
        expected_obt_net = pre_obt_net + len(kafka_batch_1)

        actual_fct_net = docker_duckdb.wait_for_net_count(
            "fct_sales", expected_fct_net, timeout=_DUCKDB_TIMEOUT_SECONDS
        )
        actual_obt_net = docker_duckdb.wait_for_net_count(
            "obt_sales", expected_obt_net, timeout=_DUCKDB_TIMEOUT_SECONDS
        )

        assert actual_fct_net == expected_fct_net, (
            f"fct_sales: expected {expected_fct_net} net rows, got {actual_fct_net}"
        )
        assert actual_obt_net == expected_obt_net, (
            f"obt_sales: expected {expected_obt_net} net rows, got {actual_obt_net}"
        )

        post_obt_files = docker_duckdb.list_parquet_files("obt_sales")

        assert pre_obt_files.issubset(post_obt_files), (
            f"Existing Parquet files were removed (not incremental). Missing: {pre_obt_files - post_obt_files}"
        )
        new_files = post_obt_files - pre_obt_files
        assert new_files, "No new Parquet files were added (expected incremental write)"

        logger.info(
            "IVM auto-consume: obt_sales %d new Parquet files, net rows %d→%d (+%d from Kafka)",
            len(new_files),
            pre_obt_net,
            actual_obt_net,
            len(kafka_batch_1),
        )

    def test_kafka_ivm_restart_resume(self, docker_feldera, dbt_project_dir, docker_duckdb, kafka_proxy_url):
        """
        IVM test: Pipeline restart preserves Kafka offsets.

        Stops the pipeline, produces new records to Kafka, then runs
        ``dbt run`` (non-full-refresh) which uses ``update_with_views``
        (modify + restart). The pipeline resumes from the committed Kafka
        offset and processes only the new records.

        Verifies:
        - Pipeline successfully restarts via ``dbt run``
        - Only new records appear in Delta output (offset preserved)
        - Delta writes are incremental (new files only)
        """
        pre_obt_net = docker_duckdb.get_net_count("obt_sales")
        pre_fct_net = docker_duckdb.get_net_count("fct_sales")
        pre_obt_files = docker_duckdb.list_parquet_files("obt_sales")

        logger.info(
            "Pre-restart state: obt_sales=%d net rows, fct_sales=%d net rows",
            pre_obt_net,
            pre_fct_net,
        )

        _stop_feldera_pipeline(docker_feldera, "adventureworks")

        kafka_batch_2 = _generate_sales_records(count=5, start_order_id=99201)
        _produce_to_kafka(kafka_batch_2, "sales_events", kafka_proxy_url)
        logger.info("Produced %d records while pipeline is stopped", len(kafka_batch_2))

        result = _run_dbt(
            dbt_project_dir,
            ["run", "--target", "local"],
            feldera_url=docker_feldera,
        )
        assert result.returncode == 0, f"dbt run (restart) failed:\n{result.stdout}\n{result.stderr}"

        expected_fct_net = pre_fct_net + len(kafka_batch_2)
        expected_obt_net = pre_obt_net + len(kafka_batch_2)

        actual_fct_net = docker_duckdb.wait_for_net_count(
            "fct_sales", expected_fct_net, timeout=_DUCKDB_TIMEOUT_SECONDS
        )
        actual_obt_net = docker_duckdb.wait_for_net_count(
            "obt_sales", expected_obt_net, timeout=_DUCKDB_TIMEOUT_SECONDS
        )

        assert actual_fct_net == expected_fct_net, (
            f"fct_sales: expected {expected_fct_net} net rows, got {actual_fct_net}"
        )
        assert actual_obt_net == expected_obt_net, (
            f"obt_sales: expected {expected_obt_net} net rows, got {actual_obt_net}"
        )

        post_obt_files = docker_duckdb.list_parquet_files("obt_sales")
        assert pre_obt_files.issubset(post_obt_files), (
            f"Existing Parquet files were removed (not incremental). Missing: {pre_obt_files - post_obt_files}"
        )

        logger.info(
            "IVM restart-resume: net rows obt=%d→%d, fct=%d→%d (+%d from Kafka after restart)",
            pre_obt_net,
            actual_obt_net,
            pre_fct_net,
            actual_fct_net,
            len(kafka_batch_2),
        )


# ---------------------------------------------------------------------------
# Fabric integration tests (pure SQL, no Kafka/Delta)
# ---------------------------------------------------------------------------


@pytest.mark.fabric
class TestDbtFelderaFabric:
    """Integration tests for dbt-feldera running against Fabric-hosted Feldera.

    These tests use the ``fabric`` dbt target, which disables Kafka input
    connectors and Delta output connectors.  The tests validate the core
    SQL pipeline lifecycle:

      1. dbt debug   — connection verification
      2. dbt seed    — data loading via HTTP ingress
      3. dbt build   — model deployment + data tests (excluding kafka models)
      4. dbt test    — data integrity via ad-hoc queries

    Requires a Feldera instance reachable at ``FELDERA_URL`` (default:
    ``http://localhost:3000`` via Privy proxy).
    """

    def test_fabric_dbt_debug(self, fabric_feldera, dbt_project_dir):
        """Verify that dbt can connect to Fabric Feldera."""
        result = _run_dbt(dbt_project_dir, ["debug", "--target", "fabric"], feldera_url=fabric_feldera)
        assert result.returncode == 0, f"dbt debug failed:\n{result.stdout}\n{result.stderr}"
        assert "All checks passed" in result.stdout or "Connection test" in result.stdout

    def test_fabric_dbt_seed(self, fabric_feldera, dbt_project_dir):
        """Load seed data into Fabric Feldera via HTTP ingress."""
        result = _run_dbt(
            dbt_project_dir,
            ["seed", "--target", "fabric", "--full-refresh"],
            feldera_url=fabric_feldera,
        )
        assert result.returncode == 0, f"dbt seed failed:\n{result.stdout}\n{result.stderr}"

    def test_fabric_dbt_build(self, fabric_feldera, dbt_project_dir):
        """Deploy all models (excluding kafka) and run data tests."""
        result = _run_dbt(
            dbt_project_dir,
            ["build", "--target", "fabric", "--full-refresh"],
            feldera_url=fabric_feldera,
        )
        assert result.returncode == 0, f"dbt build failed:\n{result.stdout}\n{result.stderr}"

        _wait_for_pipeline_idle(fabric_feldera, "adventureworks")

    def test_fabric_dbt_test(self, fabric_feldera, dbt_project_dir):
        """Run data tests against materialized views on Fabric."""
        _wait_for_pipeline_idle(fabric_feldera, "adventureworks")
        result = _run_dbt(dbt_project_dir, ["test", "--target", "fabric"], feldera_url=fabric_feldera)
        assert result.returncode in (0, 1), f"dbt test failed:\n{result.stdout}\n{result.stderr}"
