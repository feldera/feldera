"""
Iceberg input connector: end-to-end platform tests.

These exercise the connector against a live pipeline manager, which the
in-crate Rust tests (that drive a `Controller` in-process) cannot cover:

* ``test_iceberg_snapshot_all_types`` — the connector ingests a snapshot
  spanning every Feldera SQL type Iceberg can represent, and the SQL
  materialized view holds the expected rows.
* ``test_iceberg_snapshot_resume_no_reingest`` — with at-least-once fault
  tolerance, suspending after a completed snapshot and resuming re-reads
  nothing: the data survives and the connector reports zero re-ingested
  records.
* ``test_iceberg_ordered_snapshot_ingests_all_rows`` — a ``timestamp_column``
  with ``LATENESS`` ingests the snapshot as several timestamp-ordered
  transactions, and every row lands exactly once.
* ``test_iceberg_ordered_snapshot_resume_skips_ingested`` — suspending an
  ordered read once it has committed a range, then resuming, re-reads fewer
  than all rows: the seek point skips already-ingested ranges.

The table backend toggles automatically via :class:`IcebergTestLocation`:
local runs use a filesystem warehouse under ``/tmp``; CI runs use the
in-cluster MinIO bucket over S3 so the pipeline and the test runner share
storage.
"""

from __future__ import annotations

import json
import re
from datetime import date, datetime, time, timezone
from decimal import Decimal
from http import HTTPStatus

import pytest

from feldera import PipelineBuilder
from feldera.enums import FaultToleranceModel
from feldera.runtime_config import RuntimeConfig
from feldera.testutils import FELDERA_TEST_NUM_HOSTS, FELDERA_TEST_NUM_WORKERS
from tests import TEST_CLIENT, enterprise_only
from tests.platform.helper import api_url, get
from tests.utils import IcebergTestLocation, wait_for_condition

TABLE = "t"
CONNECTOR = "iceberg_in"
ENDPOINT = f"{TABLE}.{CONNECTOR}"


# ─── schema shared by every test ────────────────────────────────────────
#
# One column per Feldera SQL type that Iceberg can represent. The Iceberg,
# Arrow, and SQL descriptions are kept side by side so a type is added in
# exactly one place.

_SQL_COLUMNS = [
    "id BIGINT NOT NULL",
    "b BOOLEAN NOT NULL",
    "i INT NOT NULL",
    "l BIGINT NOT NULL",
    "r REAL NOT NULL",
    "d DOUBLE NOT NULL",
    "dec DECIMAL(10, 3) NOT NULL",
    "dt DATE NOT NULL",
    "tm TIME NOT NULL",
    "ts TIMESTAMP NOT NULL",
    "s VARCHAR NOT NULL",
    "fixed BINARY(5) NOT NULL",
    "varbin VARBINARY NOT NULL",
    "tstz TIMESTAMP WITH TIME ZONE NOT NULL",
]


def _iceberg_schema():
    from pyiceberg.schema import Schema
    from pyiceberg.types import (
        BinaryType,
        BooleanType,
        DateType,
        DecimalType,
        DoubleType,
        FixedType,
        FloatType,
        IntegerType,
        LongType,
        NestedField,
        StringType,
        TimestampType,
        TimestamptzType,
        TimeType,
    )

    return Schema(
        NestedField(1, "id", LongType(), required=True),
        NestedField(2, "b", BooleanType(), required=True),
        NestedField(3, "i", IntegerType(), required=True),
        NestedField(4, "l", LongType(), required=True),
        NestedField(5, "r", FloatType(), required=True),
        NestedField(6, "d", DoubleType(), required=True),
        NestedField(7, "dec", DecimalType(10, 3), required=True),
        NestedField(8, "dt", DateType(), required=True),
        NestedField(9, "tm", TimeType(), required=True),
        NestedField(10, "ts", TimestampType(), required=True),
        NestedField(11, "s", StringType(), required=True),
        NestedField(12, "fixed", FixedType(5), required=True),
        NestedField(13, "varbin", BinaryType(), required=True),
        NestedField(14, "tstz", TimestamptzType(), required=True),
    )


def _arrow_schema():
    import pyarrow as pa

    return pa.schema(
        [
            pa.field("id", pa.int64(), nullable=False),
            pa.field("b", pa.bool_(), nullable=False),
            pa.field("i", pa.int32(), nullable=False),
            pa.field("l", pa.int64(), nullable=False),
            pa.field("r", pa.float32(), nullable=False),
            pa.field("d", pa.float64(), nullable=False),
            pa.field("dec", pa.decimal128(10, 3), nullable=False),
            pa.field("dt", pa.date32(), nullable=False),
            pa.field("tm", pa.time64("us"), nullable=False),
            pa.field("ts", pa.timestamp("us"), nullable=False),
            pa.field("s", pa.string(), nullable=False),
            pa.field("fixed", pa.binary(5), nullable=False),
            pa.field("varbin", pa.binary(), nullable=False),
            pa.field("tstz", pa.timestamp("us", tz="UTC"), nullable=False),
        ]
    )


# Base timestamp for the generated rows: 2024-01-01T00:00:00Z. Rows step
# `ts` forward so a `LATENESS` window splits them into ordered ranges.
_BASE_TS = datetime(2024, 1, 1, tzinfo=timezone.utc)


def _row(row_id: int, *, ts_hours: int) -> dict:
    """One deterministic row keyed by ``row_id``.

    ``ts_hours`` sets both the naive ``ts`` and the tz-aware ``tstz`` so the
    ordered-ingest test can spread rows across day-wide windows.
    """
    event = _BASE_TS + _hours(ts_hours)
    return {
        "id": row_id,
        "b": row_id % 2 == 0,
        "i": row_id,
        "l": row_id * 1_000_000,
        "r": float(row_id) + 0.5,
        "d": float(row_id) * 1.25,
        "dec": Decimal(f"{row_id}.125"),
        "dt": date(2024, 1, 1),
        "tm": time(12, 0, 0),
        "ts": event.replace(tzinfo=None),
        "s": f"row_{row_id}",
        "fixed": bytes([row_id % 256]) * 5,
        "varbin": bytes([row_id % 256, (row_id + 1) % 256]),
        "tstz": event,
    }


def _hours(n: int):
    from datetime import timedelta

    return timedelta(hours=n)


def _arrow_table(rows: list[dict]):
    import pyarrow as pa

    return pa.Table.from_pylist(rows, schema=_arrow_schema())


def _build_sql(loc: IcebergTestLocation, *, lateness: bool = False, **extra) -> str:
    columns = list(_SQL_COLUMNS)
    if lateness:
        # Replace the plain `ts` column with a lateness-annotated one.
        columns = [
            c
            if not c.startswith("ts ")
            else "ts TIMESTAMP NOT NULL LATENESS INTERVAL 1 DAY"
            for c in columns
        ]
    connector = {
        "name": CONNECTOR,
        "transport": {
            "name": "iceberg_input",
            "config": loc.connector_config(**extra),
        },
    }
    connectors = json.dumps([connector]).replace("'", "''")
    cols = ",\n  ".join(columns)
    return (
        f"CREATE TABLE {TABLE} (\n  {cols}\n) "
        f"WITH ('materialized' = 'true', 'connectors' = '{connectors}');"
    )


def _build_pipeline(name: str, sql: str, *, fault_tolerant: bool = False):
    kwargs = dict(
        workers=FELDERA_TEST_NUM_WORKERS,
        hosts=FELDERA_TEST_NUM_HOSTS,
        logging="debug",
    )
    if fault_tolerant:
        kwargs["storage"] = True
        kwargs["fault_tolerance_model"] = FaultToleranceModel.AtLeastOnce
    return PipelineBuilder(
        TEST_CLIENT,
        name,
        sql=sql,
        runtime_config=RuntimeConfig(**kwargs),
    ).create_or_replace()


# ─── metric helpers (Prometheus scrape, filtered by endpoint) ────────────


def _metric(pipeline_name: str, metric_name: str) -> float:
    response = get(api_url(f"/pipelines/{pipeline_name}/metrics?format=prometheus"))
    assert response.status_code == HTTPStatus.OK, response.text
    pattern = rf'^{re.escape(metric_name)}\{{[^}}]*endpoint="{re.escape(ENDPOINT)}"[^}}]*\}}\s+(\S+)'
    for line in response.text.splitlines():
        match = re.match(pattern, line)
        if match:
            return float(match.group(1))
    return -1.0


def _metric_int(pipeline_name: str, metric_name: str) -> int:
    """Integer value of a counter/gauge (cf. ``_delta_counter``). -1 if absent."""
    value = _metric(pipeline_name, metric_name)
    return -1 if value < 0 else int(value)


def _phase_from_metrics(pipeline_name: str) -> int:
    """Connector phase gauge: 2 == the snapshot read completed."""
    return _metric_int(pipeline_name, "input_connector_iceberg_phase")


def _row_count(pipeline) -> int:
    rows = list(pipeline.query(f"SELECT COUNT(*) AS c FROM {TABLE}"))
    return int(rows[0]["c"])


def _wait_for_completed(pipeline, pipeline_name: str, timeout_s: float = 120.0) -> None:
    wait_for_condition(
        "iceberg snapshot completed (phase == 2)",
        lambda: _phase_from_metrics(pipeline_name) == 2,
        timeout_s=timeout_s,
        poll_interval_s=0.2,
    )


# ─── tests ───────────────────────────────────────────────────────────────


@enterprise_only
def test_iceberg_snapshot_all_types(pipeline_name):
    """A snapshot spanning every Iceberg-representable SQL type ingests
    completely, and the materialized table holds the expected rows."""
    loc = IcebergTestLocation.create(pipeline_name)
    try:
        rows = [_row(i, ts_hours=i) for i in range(20)]
        loc.create_table(_iceberg_schema())
        loc.append(_arrow_table(rows))

        pipeline = _build_pipeline(pipeline_name, _build_sql(loc, mode="snapshot"))
        pipeline.start()
        _wait_for_completed(pipeline, pipeline_name)

        assert _row_count(pipeline) == len(rows)

        # Spot-check the scalar columns that round-trip cleanly through the
        # ad-hoc query JSON. Reaching `phase == 2` with the right row count
        # already proves every declared column parsed without error.
        got = list(
            pipeline.query(f"SELECT id, b, i, l, s, dec FROM {TABLE} ORDER BY id")
        )
        assert [r["id"] for r in got] == [r["id"] for r in rows]
        assert [r["b"] for r in got] == [r["b"] for r in rows]
        assert [r["s"] for r in got] == [r["s"] for r in rows]
        assert [Decimal(str(r["dec"])) for r in got] == [r["dec"] for r in rows]

        pipeline.stop(force=True)
    finally:
        loc.remove_if_local()


@enterprise_only
def test_iceberg_snapshot_resume_no_reingest(pipeline_name):
    """At-least-once FT: suspending after a completed snapshot and resuming
    re-reads nothing. The rows survive and the connector reports zero
    re-ingested records — a restart never re-reads a finished table."""
    loc = IcebergTestLocation.create(pipeline_name)
    try:
        rows = [_row(i, ts_hours=i) for i in range(50)]
        loc.create_table(_iceberg_schema())
        loc.append(_arrow_table(rows))

        sql = _build_sql(loc, mode="snapshot")
        pipeline = _build_pipeline(pipeline_name, sql, fault_tolerant=True)
        pipeline.start()
        _wait_for_completed(pipeline, pipeline_name)
        assert _row_count(pipeline) == len(rows)
        assert _metric_int(
            pipeline_name, "input_connector_iceberg_snapshot_records_total"
        ) == len(rows)

        # Checkpoint, suspend, and resume the SAME pipeline incarnation.
        pipeline.checkpoint(wait=True)
        pipeline.stop(force=False)
        pipeline.start()

        # Data intact and the connector jumps straight back to completed
        # without re-reading a single record.
        assert _row_count(pipeline) == len(rows)
        _wait_for_completed(pipeline, pipeline_name)
        assert (
            _metric_int(pipeline_name, "input_connector_iceberg_snapshot_records_total")
            == 0
        ), "a resumed, already-completed snapshot must re-read no records"

        pipeline.stop(force=True)
    finally:
        loc.remove_if_local()


@enterprise_only
def test_iceberg_ordered_snapshot_ingests_all_rows(pipeline_name):
    """A ``timestamp_column`` with ``LATENESS`` ingests the snapshot as
    several timestamp-ordered transactions, and every row lands once."""
    loc = IcebergTestLocation.create(pipeline_name)
    try:
        # 60 rows, one per hour → 3 distinct days → several 1-day windows.
        rows = [_row(i, ts_hours=i) for i in range(60)]
        loc.create_table(_iceberg_schema())
        loc.append(_arrow_table(rows))

        sql = _build_sql(
            loc,
            lateness=True,
            mode="snapshot",
            timestamp_column="ts",
            transaction_mode="snapshot",
        )
        pipeline = _build_pipeline(pipeline_name, sql)
        pipeline.start()
        _wait_for_completed(pipeline, pipeline_name)

        assert _row_count(pipeline) == len(rows)
        # Ordered ingest breaks the snapshot into per-window transactions.
        assert (
            _metric_int(
                pipeline_name,
                "input_connector_iceberg_snapshot_transaction_starts",
            )
            > 1
        ), "a lateness-ordered snapshot must span more than one transaction"

        pipeline.stop(force=True)
    finally:
        loc.remove_if_local()


_RECORDS_METRIC = "input_connector_iceberg_snapshot_records_total"


@enterprise_only
def test_iceberg_ordered_snapshot_resume_skips_ingested(pipeline_name):
    """Resuming an ordered read skips ranges it already ingested.

    Suspend the read once it has committed at least one range, resume, and
    check the second incarnation re-reads fewer than all rows. The exact
    suspend point is not controlled, so the assertion is deterministic across
    every point it can land on:

    * suspended mid-read: the checkpointed timestamp skips the committed
      ranges, so the resumed read ingests only the remainder (< N).
    * already completed when suspended: the end-of-input state resumes into
      completion and re-reads nothing (0 < N).

    Either way the resumed read must be < N; a broken seek that re-read the
    whole snapshot would ingest N and fail here. The final table always holds
    every row.
    """
    loc = IcebergTestLocation.create(pipeline_name)
    try:
        # 240 hourly rows over 10 days -> ~10 one-day ranges, so a suspend has
        # several committed range boundaries to land after.
        rows = [_row(i, ts_hours=i) for i in range(240)]
        n = len(rows)
        loc.create_table(_iceberg_schema())
        loc.append(_arrow_table(rows))

        sql = _build_sql(
            loc,
            lateness=True,
            mode="snapshot",
            timestamp_column="ts",
            transaction_mode="snapshot",
        )
        pipeline = _build_pipeline(pipeline_name, sql, fault_tolerant=True)
        pipeline.start()

        # Wait until at least one range has been ingested, then suspend. This
        # gate is always reached (the snapshot has rows) and removes the race
        # on ingest speed: whether the value is partial or already N, the
        # resume assertion below holds.
        wait_for_condition(
            "iceberg ordered read ingested at least one record",
            lambda: _metric_int(pipeline_name, _RECORDS_METRIC) > 0,
            timeout_s=120.0,
            poll_interval_s=0.05,
        )

        pipeline.checkpoint(wait=True)
        pipeline.stop(force=False)
        pipeline.start()

        _wait_for_completed(pipeline, pipeline_name)

        reingested = _metric_int(pipeline_name, _RECORDS_METRIC)
        assert reingested < n, (
            f"resumed read re-ingested {reingested} of {n} rows; the seek point "
            "must skip already-ingested ranges"
        )
        assert _row_count(pipeline) == n, "every row must survive the resume"

        pipeline.stop(force=True)
    finally:
        loc.remove_if_local()


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
