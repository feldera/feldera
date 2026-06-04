"""Snapshot read of a column-mapped Delta table after schema evolution.

A table with ``delta.columnMapping.mode = 'name'`` stores opaque physical
Parquet column names (``col-<uuid>``) that differ from the logical names; with
``mode = 'id'`` columns are matched by Parquet field ID instead. Either way, a
correct snapshot read must resolve every value through the column-mapping
metadata in the Delta log. PySpark seeds the fixture (it is currently the only
writer that supports column-mapping rename/drop); the builder lives in
``fixtures/column_mapping.py`` and runs via ``uv run --with delta-spark`` so
the Spark/JVM wheels are only fetched on cache miss.
"""

from __future__ import annotations

import json
from pathlib import Path

from feldera import PipelineBuilder
from feldera.runtime_config import RuntimeConfig
from feldera.testutils import FELDERA_TEST_NUM_HOSTS, FELDERA_TEST_NUM_WORKERS

from tests import TEST_CLIENT
from tests.platform.fixtures.column_mapping import EXPECTED_ROWS
from tests.utils import DeltaTestLocation, ensure_delta_spark_fixture

TABLE = "t"
CONNECTOR = "delta_in"
# Bump to invalidate cached MinIO copies when the fixture definition changes.
FIXTURE_VERSION = "v1"

# Spark builder that writes the column-mapped, schema-evolved table. It runs in
# a subprocess (see ensure_delta_spark_fixture) rather than being imported here.
_FIXTURE_BUILDER = Path(__file__).parent / "fixtures" / "column_mapping.py"


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
    # The SQL schema declares the final, post-evolution logical columns.
    return (
        f"CREATE TABLE {TABLE} ("
        "id BIGINT NOT NULL,"
        "full_name VARCHAR,"
        "country VARCHAR"
        f") WITH ('materialized' = 'true', 'connectors' = '{connectors}');"
    )


def _snapshot_rows(pipeline) -> list[dict]:
    rows = pipeline.query(f"SELECT id, full_name, country FROM {TABLE}")
    return sorted(
        (
            {"id": r["id"], "full_name": r["full_name"], "country": r["country"]}
            for r in rows
        ),
        key=lambda r: r["id"],
    )


def _run_column_mapping_snapshot_test(pipeline_name: str, mapping_mode: str) -> None:
    """A snapshot read of a column-mapped, schema-evolved table returns the
    final logical schema and correctly resolves physical column names.

    Driven by one wrapper test per ``delta.columnMapping.mode`` (rather than
    ``pytest.mark.parametrize``) so each case gets a distinct pipeline name —
    the ``pipeline_name`` fixture derives the name from the test function, and
    parametrized cases sharing one name could collide under ``pytest -n``.
    """
    loc = DeltaTestLocation.create(
        pipeline_name,
        mode="snapshot",
        stable_subpath=f"column_mapping_{mapping_mode}_{FIXTURE_VERSION}",
    )
    try:
        ensure_delta_spark_fixture(loc, _FIXTURE_BUILDER, builder_args=(mapping_mode,))

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

        rows = _snapshot_rows(pipeline)
        assert rows == EXPECTED_ROWS, (
            "snapshot read must resolve column-mapped, schema-evolved data: "
            "dropped 'amount' gone, 'country' NULL for pre-add rows; "
            f"got {rows}"
        )

        pipeline.stop(force=True)
    finally:
        loc.cleanup()


def test_delta_input_column_mapping_name_snapshot(pipeline_name):
    """Snapshot read with ``delta.columnMapping.mode = 'name'``."""
    _run_column_mapping_snapshot_test(pipeline_name, "name")


def test_delta_input_column_mapping_id_snapshot(pipeline_name):
    """Snapshot read with ``delta.columnMapping.mode = 'id'``."""
    _run_column_mapping_snapshot_test(pipeline_name, "id")
