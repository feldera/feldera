"""Snapshot read of a Delta table with deletion vectors.

PySpark seeds the fixture (delta-rs cannot write DVs). The builder lives in
``_dv_fixture_builder.py`` and runs via ``uv run --with delta-spark`` so the
Spark/JVM wheels are only fetched on cache miss.
"""

from __future__ import annotations

import json
import shutil
import subprocess
import tempfile
from pathlib import Path

from feldera import PipelineBuilder
from feldera.runtime_config import RuntimeConfig
from feldera.testutils import FELDERA_TEST_NUM_HOSTS, FELDERA_TEST_NUM_WORKERS

from tests import TEST_CLIENT
from tests.utils import DeltaTestLocation


TABLE = "dv_data"
CONNECTOR = "dv_in"
TOTAL_ROWS = 200
EXPECTED_ROWS_AFTER_DV = 100
# Bump to invalidate cached MinIO copies when the fixture definition changes.
FIXTURE_VERSION = "dv_snapshot_v1"

# Spark builder that writes the DV-enabled table. It runs in a subprocess
# (see _ensure_dv_snapshot_fixture) rather than being imported here.
_FIXTURE_BUILDER = Path(__file__).parent / "_dv_fixture_builder.py"


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


def _ensure_dv_snapshot_fixture(loc: DeltaTestLocation) -> None:
    """Build the DV fixture if absent; reuse the cached copy otherwise.

    The fixture lives at a shared, commit-independent path
    (``stable_subpath``), so the Spark build runs at most once per
    ``FIXTURE_VERSION`` and later runs just read the cached table.
    """
    if _log_has_dv_entries(loc):
        return

    if shutil.which("uv") is None:
        raise RuntimeError(
            "`uv` is required on PATH to rebuild the DV fixture "
            "(builder runs via `uv run --with delta-spark`)."
        )

    # Stage in a temp dir so a half-finished build cannot leak into the upload.
    # Writing DV-enabled Delta tables needs the Delta Lake Spark JARs;
    # `delta-spark` is the clean way to pull them plus a matching pyspark
    # (see _dv_fixture_builder.py for why not bare pyspark). `uv run --with`
    # installs the stack only on this rare rebuild path.
    staging = Path(tempfile.mkdtemp(prefix="feldera_dv_stage_"))
    try:
        subprocess.run(
            [
                "uv",
                "run",
                "--with",
                "delta-spark>=4.2,<5",
                "python",
                str(_FIXTURE_BUILDER),
                str(staging),
                str(TOTAL_ROWS),
                str(EXPECTED_ROWS_AFTER_DV),
            ],
            check=True,
        )
        # Upload data files first, then _delta_log in version order, so a
        # reader observing mid-upload never sees a log referencing a missing
        # parquet.
        if loc.local_dir is not None:
            if loc.local_dir.exists():
                shutil.rmtree(loc.local_dir)
            shutil.copytree(staging, loc.local_dir)
        else:
            fs = loc._s3_filesystem()
            files = [f for f in sorted(staging.rglob("*")) if f.is_file()]
            for f in sorted(files, key=lambda p: ("_delta_log" in p.parts, p.name)):
                rel = f.relative_to(staging).as_posix()
                with fs.open_output_stream(f"{loc.root_path}/{rel}") as out:
                    out.write(f.read_bytes())
    finally:
        shutil.rmtree(staging, ignore_errors=True)

    if not _log_has_dv_entries(loc):
        raise RuntimeError(
            f"DV fixture at {loc.uri} has no deletion-vector log entries "
            "after upload — partial upload, or DV-stripping middleware?"
        )


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
        _ensure_dv_snapshot_fixture(loc)

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
