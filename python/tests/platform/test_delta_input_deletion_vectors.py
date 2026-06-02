"""Snapshot read of a Delta table with deletion vectors.

PySpark seeds the fixture (delta-rs cannot write DVs). The builder lives in
``fixtures/deletion_vectors.py`` and runs via ``uv run --with delta-spark`` so
the Spark/JVM wheels are only fetched on cache miss.
"""

from __future__ import annotations

import json
from pathlib import Path

from feldera import PipelineBuilder
from feldera.runtime_config import RuntimeConfig
from feldera.testutils import FELDERA_TEST_NUM_HOSTS, FELDERA_TEST_NUM_WORKERS

from tests import TEST_CLIENT
from tests.utils import DeltaTestLocation, ensure_delta_spark_fixture


TABLE = "dv_data"
CONNECTOR = "dv_in"
TOTAL_ROWS = 200
EXPECTED_ROWS_AFTER_DV = 100
# Bump to invalidate cached MinIO copies when the fixture definition changes.
FIXTURE_VERSION = "dv_snapshot_v1"

# Spark builder that writes the DV-enabled table. It runs in a subprocess
# (see ensure_delta_spark_fixture) rather than being imported here.
_FIXTURE_BUILDER = Path(__file__).parent / "fixtures" / "deletion_vectors.py"


def _log_has_dv_entries(loc: DeltaTestLocation) -> bool:
    """Return True when any Delta log entry carries a deletion vector.

    The Spark builder validates the active row count itself before exiting,
    so on the Python side we only need to confirm that the fixture exists
    and is DV-shaped — neither delta-rs nor the deltalake wheel reads DVs.
    """
    try:
        log_paths = loc.log_json_paths()
    except FileNotFoundError:
        return False
    for log_path in log_paths:
        for line in loc._read_text(log_path).splitlines():
            if not line.strip():
                continue
            try:
                action = json.loads(line)
            except json.JSONDecodeError:
                continue
            if (action.get("add") or {}).get("deletionVector"):
                return True
    return False


def _build_sql(loc: DeltaTestLocation) -> str:
    connectors = json.dumps(
        [
            {
                "name": CONNECTOR,
                "transport": {
                    "name": "delta_table_input",
                    "config": dict(loc.connector_config),
                },
            }
        ]
    ).replace("'", "''")
    return (
        f"CREATE TABLE {TABLE} ("
        "id INT NOT NULL,"
        "name VARCHAR,"
        "value DOUBLE"
        f") WITH ('materialized' = 'true', 'connectors' = '{connectors}');"
    )


def test_delta_input_snapshot_with_deletion_vectors(pipeline_name):
    """Snapshot read of a DV-enabled table must skip soft-deleted rows."""
    loc = DeltaTestLocation.create(
        pipeline_name,
        mode="snapshot",
        stable_subpath=FIXTURE_VERSION,
    )
    try:
        ensure_delta_spark_fixture(
            loc,
            _FIXTURE_BUILDER,
            [TOTAL_ROWS, EXPECTED_ROWS_AFTER_DV],
            is_present=_log_has_dv_entries,
        )

        pipeline = PipelineBuilder(
            TEST_CLIENT,
            pipeline_name,
            sql=_build_sql(loc),
            runtime_config=RuntimeConfig(
                workers=FELDERA_TEST_NUM_WORKERS,
                hosts=FELDERA_TEST_NUM_HOSTS,
                logging="debug",
            ),
        ).create_or_replace()
        pipeline.start()
        pipeline.wait_for_completion(force_stop=False, timeout_s=600)

        rows = list(pipeline.query(f"SELECT COUNT(*) AS c FROM {TABLE}"))
        assert int(rows[0]["c"]) == EXPECTED_ROWS_AFTER_DV, (
            "snapshot ingest of a DV-enabled table must drop the soft-deleted "
            f"rows ({TOTAL_ROWS - EXPECTED_ROWS_AFTER_DV} rows have id % 2 = 0)"
        )

        pipeline.stop(force=True)
    finally:
        loc.cleanup()
