"""Delta input tests for tables with deletion vectors.

Covers the three ingest modes that interact with deletion vectors (DVs):

* ``snapshot``: delta-rs applies DVs inside its table provider.
* ``snapshot_and_follow``: the follow phase replays ``add``/``remove`` log
  actions per file and must apply each action's DV itself.
* ``cdc``: a DV rewrite commit re-references one immutable data file (identified
  by its Parquet path) in both a ``remove`` and a new ``add``, only changing that
  file's deletion vector. The connector recognizes this same-path pair as a
  metadata-only rewrite and skips it; the test pins down that invariant.

PySpark seeds the fixtures (delta-rs cannot write DVs). The builder lives in
``fixtures/deletion_vectors.py`` and runs via ``uv run --with delta-spark`` so
the Spark/JVM wheels are only fetched on cache miss.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import NamedTuple

from feldera import PipelineBuilder
from feldera.runtime_config import RuntimeConfig
from feldera.testutils import (
    FELDERA_TEST_NUM_HOSTS,
    FELDERA_TEST_NUM_WORKERS,
    number_of_input_records,
)

from tests import TEST_CLIENT
from tests.utils import DeltaTestLocation, ensure_delta_spark_fixture


TABLE = "dv_data"
CONNECTOR = "dv_in"
TOTAL_ROWS = 200
EXPECTED_ROWS_AFTER_DV = 100
# Rows the even-id soft-delete masks (== the odd-id survivors, both halves equal).
DV_DELETED_ROWS = TOTAL_ROWS - EXPECTED_ROWS_AFTER_DV
# Records a follow-mode DV rewrite must feed into the circuit. A same-path
# `add`/`remove` pair changes only the rows whose deletion-vector membership
# flipped, so the connector must ingest just those, not re-read the whole file.
# The follow phase reads one DV commit; snapshot reads the pinned base version.
#
#   plain fixture (v0 base, v1 DV-delete): snapshot TOTAL_ROWS + follow deletes
#                                          exactly the DV_DELETED_ROWS.
#   cdc fixture   (v1 base, v2 restore)  : snapshot EXPECTED_ROWS_AFTER_DV +
#                                          follow re-inserts the DV_DELETED_ROWS.
#
# Before the same-path pairing fix, follow re-read whole files (~2N-K records),
# so these totals were ~500 and ~400 -- the amplification these bounds catch.
FOLLOW_DV_DELETE_INPUT_RECORDS = TOTAL_ROWS + DV_DELETED_ROWS
FOLLOW_RESTORE_INPUT_RECORDS = EXPECTED_ROWS_AFTER_DV + DV_DELETED_ROWS
# Bump to invalidate cached MinIO copies when the fixture definition changes.
FIXTURE_VERSION = "dv_snapshot_v1"
# CDC-shaped fixture (see fixtures/deletion_vectors.py --cdc): v0 inserts
# TOTAL_ROWS events, v1 DV-deletes the even ids, v2 restores them (shrinking
# the DVs away), v3 appends one event.
CDC_FIXTURE_VERSION = "dv_cdc_v1"
CDC_EXPECTED_ACTIVE = TOTAL_ROWS + 1
# Overwrite-shaped fixture (see fixtures/deletion_vectors.py --overwrite): v0
# inserts TOTAL_ROWS events, v1 DV-deletes the even ids, v2 re-inserts exactly
# those even events via INSERT OVERWRITE. After v2 only the EXPECTED_ROWS_AFTER_DV
# even events remain.
OVERWRITE_FIXTURE_VERSION = "dv_cdc_overwrite_v1"

# Spark builder that writes the DV-enabled table. It runs in a subprocess
# (see ensure_delta_spark_fixture) rather than being imported here.
_FIXTURE_BUILDER = Path(__file__).parent / "fixtures" / "deletion_vectors.py"


def _log_has_dv_entries(loc: DeltaTestLocation) -> bool:
    """Return True when any Delta log entry carries a deletion vector.

    The Spark builder validates the active row count itself before exiting,
    so on the Python side we only need to confirm that the fixture exists
    and is DV-shaped; neither delta-rs nor the deltalake wheel reads DVs.
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


def _build_sql(
    loc: DeltaTestLocation,
    *,
    columns: str,
    extra_config: dict | None = None,
) -> str:
    config = dict(loc.connector_config)
    config.update(extra_config or {})
    connectors = json.dumps(
        [
            {
                "name": CONNECTOR,
                "transport": {
                    "name": "delta_table_input",
                    "config": config,
                },
            }
        ]
    ).replace("'", "''")
    return (
        f"CREATE TABLE {TABLE} ({columns})"
        f" WITH ('materialized' = 'true', 'connectors' = '{connectors}');"
    )


class DvIngest(NamedTuple):
    """Outcome of ingesting a DV fixture.

    ``total`` and ``even_id_rows`` are TABLE row counts; ``input_records`` is
    ``total_input_records`` (records fed into the circuit). ``input_records``
    exposes read amplification the row counts cannot: the circuit nets a whole-
    file re-read back to the same result, so only the ingested-record count
    distinguishes a minimal DV-delta read from re-reading the whole file.
    """

    total: int
    even_id_rows: int
    input_records: int


def _run_to_completion(pipeline_name: str, sql: str) -> DvIngest:
    """Run the pipeline until end-of-input.

    ``even_id_rows`` exists because every fixture soft-deletes exactly the even
    ids: a correct ingest leaves zero of them, and counting them catches an
    *inverted* deletion vector (ingesting the deleted half), which COUNT(*)
    alone cannot, since both halves have the same size.
    """
    pipeline = PipelineBuilder(
        TEST_CLIENT,
        pipeline_name,
        sql=sql,
        runtime_config=RuntimeConfig(
            workers=FELDERA_TEST_NUM_WORKERS,
            hosts=FELDERA_TEST_NUM_HOSTS,
            logging="debug",
        ),
    ).create_or_replace()
    pipeline.start()
    pipeline.wait_for_completion(force_stop=False, timeout_s=600)

    rows = list(
        pipeline.query(
            f"SELECT COUNT(*) AS total,"
            f" COALESCE(SUM(CASE WHEN id % 2 = 0 THEN 1 ELSE 0 END), 0) AS even_id_rows"
            f" FROM {TABLE}"
        )
    )
    # Read before stopping: the metric is only live while the pipeline runs.
    input_records = number_of_input_records(pipeline)
    pipeline.stop(force=True)
    return DvIngest(int(rows[0]["total"]), int(rows[0]["even_id_rows"]), input_records)


def _ingest_dv_fixture(
    pipeline_name: str,
    *,
    mode: str,
    fixture_version: str = FIXTURE_VERSION,
    expected_active: int = EXPECTED_ROWS_AFTER_DV,
    cdc: bool = False,
    overwrite: bool = False,
    columns: str = "id INT NOT NULL, name VARCHAR, value DOUBLE",
    extra_config: dict | None = None,
) -> DvIngest:
    """Ensure the fixture exists, ingest it in `mode`, return the outcome.

    Owns the location's lifecycle (create/cleanup); the tests reduce to a
    call plus their assertions. Returns ``_run_to_completion``'s [`DvIngest`].
    """
    builder_flags = (["--cdc"] if cdc else []) + (["--overwrite"] if overwrite else [])
    loc = DeltaTestLocation.create(
        pipeline_name,
        mode=mode,
        stable_subpath=fixture_version,
    )
    try:
        ensure_delta_spark_fixture(
            loc,
            _FIXTURE_BUILDER,
            [TOTAL_ROWS, expected_active, *builder_flags],
            is_present=_log_has_dv_entries,
        )
        return _run_to_completion(
            pipeline_name,
            _build_sql(loc, columns=columns, extra_config=extra_config),
        )
    finally:
        loc.cleanup()


def test_delta_input_snapshot_with_deletion_vectors(pipeline_name):
    """Snapshot read of a DV-enabled table must skip soft-deleted rows."""
    result = _ingest_dv_fixture(pipeline_name, mode="snapshot")
    assert result.total == EXPECTED_ROWS_AFTER_DV, (
        "snapshot ingest of a DV-enabled table must drop the soft-deleted "
        f"rows ({DV_DELETED_ROWS} rows have id % 2 = 0)"
    )
    assert result.even_id_rows == 0, (
        "the surviving rows must be the odd ids, not the deleted even ids"
    )


def test_delta_input_follow_with_deletion_vectors(pipeline_name):
    """The follow path must apply the deletion vectors carried by log actions.

    Replays the fixture's DV commit (v1) from version 0. The commit is a
    same-path `remove`/`add` pair (the file is immutable; only its deletion
    vector changed), so the connector reads just the newly-masked rows and
    retracts them, leaving the DV survivors. It must not re-read the whole file.
    """
    result = _ingest_dv_fixture(
        pipeline_name,
        mode="snapshot_and_follow",
        extra_config={"version": 0, "end_version": 1},
    )
    assert result.total == EXPECTED_ROWS_AFTER_DV, (
        "follow ingest of a DV commit must retract the soft-deleted rows; "
        f"{TOTAL_ROWS} rows means the add action's deletion vector was ignored"
    )
    assert result.even_id_rows == 0, (
        "the surviving rows must be the odd ids, not the deleted even ids"
    )
    # A same-path add/remove DV rewrite must ingest only the flipped rows.
    # Re-reading the whole file (the pre-fix behavior) would net the same rows
    # but roughly double this count, so the record total is what guards it.
    assert result.input_records == FOLLOW_DV_DELETE_INPUT_RECORDS, (
        f"follow ingested {result.input_records} records for a "
        f"{DV_DELETED_ROWS}-row DV delete; expected "
        f"{FOLLOW_DV_DELETE_INPUT_RECORDS} (snapshot {TOTAL_ROWS} + "
        f"{DV_DELETED_ROWS} deleted). A far larger count means the connector "
        "re-read the whole rewritten file instead of just the DV delta"
    )


def test_delta_input_follow_restore_with_deletion_vectors(pipeline_name):
    """Follow mode must apply a deletion vector carried by a `remove` action.

    Reuses the CDC fixture and replays its restore commit (v2) in follow mode,
    starting from the post-delete snapshot (v1). The restore is a same-path
    `remove` (carrying the delete's deletion vector) and `add` (DV cleared): the
    connector pairs them and re-inserts exactly the rows the DV un-masked, the
    soft-deleted even ids. It must not re-read the whole restored file.

    This exercises the restore direction of the pair, where the flipped rows come
    from the `remove` side's non-empty deletion vector (the v0->v1 follow test's
    `remove` has no DV, as the file was unmasked at v0).
    """
    result = _ingest_dv_fixture(
        pipeline_name,
        mode="snapshot_and_follow",
        fixture_version=CDC_FIXTURE_VERSION,
        expected_active=CDC_EXPECTED_ACTIVE,
        cdc=True,
        columns="id BIGINT NOT NULL",
        extra_config={"version": 1, "end_version": 2},
    )
    assert result.total == TOTAL_ROWS, (
        "the restore's `add` must re-insert the whole file after its `remove` "
        "(masked by the delete's deletion vector) retracts only the survivors; "
        f"got {result.total} rows, expected {TOTAL_ROWS}"
    )
    assert result.even_id_rows == TOTAL_ROWS // 2, (
        "the restored even ids must be present; their absence means the "
        "`remove` action's deletion vector was applied to the wrong rows"
    )
    # The restore flips the same rows back: its `remove` (masked to the odd
    # survivors) and `add` (DV cleared) differ only in the even ids, so follow
    # must re-insert exactly those, not re-read the whole restored file.
    assert result.input_records == FOLLOW_RESTORE_INPUT_RECORDS, (
        f"follow ingested {result.input_records} records for a "
        f"{DV_DELETED_ROWS}-row restore; expected "
        f"{FOLLOW_RESTORE_INPUT_RECORDS} (snapshot {EXPECTED_ROWS_AFTER_DV} + "
        f"{DV_DELETED_ROWS} restored). A far larger count means the connector "
        "re-read the whole file on each side of the same-path rewrite"
    )


def test_delta_input_cdc_overwrite_masks_removed_dv_rows(pipeline_name):
    """CDC must mask a `remove` action's deletion vector before subtracting it.

    The fixture's v2 overwrites the table by re-inserting exactly the even
    events the v1 deletion vector soft-deleted. In CDC mode that overwrite's
    `remove` is *unmatched* (a new file path) and carries the delete's deletion
    vector: the only commit shape that drives the connector's `removes_masked`
    path. Replaying only that commit (version 2): masking the `remove` to its
    surviving (odd) rows means the re-inserted even events are not cancelled by
    the `EXCEPT ALL` and are emitted. Without masking, the `remove` would read
    the whole file (including its DV-dead even rows) and wrongly cancel the
    new even events, leaving zero.
    """
    result = _ingest_dv_fixture(
        pipeline_name,
        mode="cdc",
        fixture_version=OVERWRITE_FIXTURE_VERSION,
        expected_active=EXPECTED_ROWS_AFTER_DV,
        overwrite=True,
        columns="id BIGINT NOT NULL",
        extra_config={
            "version": 1,
            "end_version": 2,
            "cdc_delete_filter": "__feldera_op = 'd'",
            "cdc_order_by": "__feldera_ts asc, lsn asc",
        },
    )
    assert result.total == EXPECTED_ROWS_AFTER_DV, (
        "the re-inserted even events must survive the `EXCEPT ALL`; a count of "
        "0 means the `remove`'s deletion vector was ignored and its DV-dead "
        "rows over-subtracted the new events"
    )
    assert result.even_id_rows == EXPECTED_ROWS_AFTER_DV, (
        "every re-inserted event has an even id"
    )


def test_delta_input_cdc_with_deletion_vectors(pipeline_name):
    """DV rewrite commits on a CDC source must not re-emit or drop events.

    In CDC mode a commit that only rewrites deletion vectors (`remove` +
    `add` of the same path) is metadata-only, and the connector skips it.
    The fixture's v1 (DV delete) and v2 (restore, shrinking the DVs back)
    are both such commits; replaying from version 0 must yield only the
    single v3 insert.
    """
    result = _ingest_dv_fixture(
        pipeline_name,
        mode="cdc",
        fixture_version=CDC_FIXTURE_VERSION,
        expected_active=CDC_EXPECTED_ACTIVE,
        cdc=True,
        columns="id BIGINT NOT NULL",
        extra_config={
            "version": 0,
            "end_version": 3,
            "cdc_delete_filter": "__feldera_op = 'd'",
            "cdc_order_by": "__feldera_ts asc, lsn asc",
        },
    )
    assert result.total == 1, (
        "CDC replay of DV rewrite commits must net to zero events: "
        "only the single v3 insert event may arrive; a higher count "
        "means soft-deleted or restored events were re-ingested"
    )
