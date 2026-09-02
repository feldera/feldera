"""Delta input tests for the `change_feed` option, against Spark-written change data.

The Rust tests build their fixtures with delta-rs, which records a change feed
but cannot write a deletion vector beside one: its `DELETE` is copy-on-write.
Delta Spark can, and what it does then is the reason these tests exist.

With deletion vectors enabled, Spark records change data for an `UPDATE` and
*none* for a `DELETE`: the same-path `add`/`remove` pair and its deletion
vectors already say exactly which rows left, so there is nothing new to write.
Spark's own change feed reader derives those deletes from the pair. A connector
that reads the pair without applying the deletion vectors nets it to zero and
silently keeps deleted rows -- which is what delta-rs's `CdfLoadBuilder` does,
and why the reader lives in the connector instead.

Three fixtures, all from ``fixtures/change_data_feed.py``:

* copy-on-write, unpartitioned -- change data for every modifying commit, so a
  one-row `UPDATE` costs a whole-file rewrite under `change_feed = off` and two
  rows under `auto`;
* deletion vectors, partitioned by ``grp`` -- the Databricks-shaped table, where
  the `DELETE` reaches the connector through the fallback;
* column mapping, partitioned -- every column on disk is ``col-<uuid>`` and the
  partition directory is an opaque prefix, so the log is the only place a
  partition value exists and the logical name cannot find it.
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


TABLE = "change_feed_data"
CONNECTOR = "change_feed_in"
TOTAL_ROWS = 200

# The fixture's history: v0 creates the table, v1 inserts TOTAL_ROWS rows, v2
# updates id 1, v3 deletes the even ids, v4 appends id TOTAL_ROWS + 1, v5 merges.
# Every variant shares it, so these version numbers hold whatever the flags.
CREATE_VERSION = 0
UPDATE_VERSION = 2
DELETE_VERSION = 3
APPEND_VERSION = 4
MERGE_VERSION = 5
LAST_VERSION = MERGE_VERSION
# `--add-column` appends two more, so the five above keep their numbers.
ADD_COLUMN_VERSION = 6
EXTRA_UPDATE_VERSION = 7
UPDATED_ID = 1
UPDATED_NAME = "updated"
ROWS_AFTER_DELETE = TOTAL_ROWS // 2
# v4 appends one row; v5's merge removes one and adds one, so the count holds.
ROWS_AFTER_APPEND = ROWS_AFTER_DELETE + 1
EXPECTED_ACTIVE = ROWS_AFTER_APPEND

# The v5 merge: one row updated, one deleted, one inserted.
MERGED_NAME = "merged"
MERGE_DELETED_ID = 5
MERGE_INSERTED_ID = TOTAL_ROWS + 3
# What the merge recorded: an update's two images, a delete, and an insert.
MERGE_CHANGE_ROWS = 4

# The `--add-column` variant's v7 sets `extra` on one surviving row.
EXTRA_UPDATED_ID = 7
EXTRA_VALUE = "set"

# Records each setting feeds into the circuit replaying v0..v2: an empty snapshot,
# the TOTAL_ROWS-row insert, then the one-row UPDATE.
#
# Copy-on-write rewrites the single file the row lives in, so `off` retracts all
# of it and re-inserts all of it; the rows cancel in the circuit and the result
# is right, but the reading is not. `auto` reads the two rows Spark recorded:
# one update_preimage, one update_postimage.
OFF_UPDATE_INPUT_RECORDS = TOTAL_ROWS + 2 * TOTAL_ROWS
AUTO_UPDATE_INPUT_RECORDS = TOTAL_ROWS + 2

# Bump to invalidate cached fixture copies when the builder changes.
FIXTURE_VERSION = "change_feed_plain_v2"
DV_FIXTURE_VERSION = "change_feed_dv_partitioned_v2"
MAPPED_FIXTURE_VERSION = "change_feed_column_mapped_v2"
ADD_COLUMN_FIXTURE_VERSION = "change_feed_add_column_v1"

_FIXTURE_BUILDER = Path(__file__).parent / "fixtures" / "change_data_feed.py"


def _log_has_cdc_entries(loc: DeltaTestLocation) -> bool:
    """Return True when any Delta log entry references a change data file.

    A fixture without one would send every commit down the add/remove fallback
    and prove nothing about the change feed, so this is the shape check rather
    than mere existence.
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
            if action.get("cdc"):
                return True
    return False


def _build_sql(loc: DeltaTestLocation, *, columns: str, extra_config: dict) -> str:
    config = dict(loc.connector_config)
    config.update(extra_config)
    connectors = json.dumps(
        [
            {
                "name": CONNECTOR,
                "transport": {"name": "delta_table_input", "config": config},
            }
        ]
    ).replace("'", "''")
    return (
        f"CREATE TABLE {TABLE} ({columns})"
        f" WITH ('materialized' = 'true', 'connectors' = '{connectors}');"
    )


class ChangeFeedIngest(NamedTuple):
    """Outcome of ingesting a change feed fixture.

    ``input_records`` is what separates the two modes: they agree on the table
    contents by construction, and only the number of records fed into the
    circuit shows which one read the changed rows and which read the files
    those rows lived in.
    """

    total: int
    even_id_rows: int
    updated_rows: int
    merged_rows: int
    input_records: int
    # Rows whose `extra` is set, or None when the table has no such column.
    extra_rows: int | None
    # Rows whose `grp` equals `id % 3`, or None when the table is unpartitioned.
    partition_matches: int | None


def _run_to_completion(
    pipeline_name: str, sql: str, partitioned: bool, add_column: bool
) -> ChangeFeedIngest:
    """Run the pipeline until end-of-input and summarize the table.

    ``even_id_rows`` is the assertion that a `DELETE` actually arrived: the
    fixture deletes exactly the even ids, and a delete that netted to zero
    leaves them all in place while COUNT(*) alone still looks plausible.
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

    # `grp` is only selected for a partitioned fixture; the unpartitioned SQL
    # table does not declare the column.
    partition_match = (
        " COALESCE(SUM(CASE WHEN grp = CAST(id % 3 AS VARCHAR) THEN 1 ELSE 0 END), 0)"
        "   AS partition_matches,"
        if partitioned
        else ""
    )
    extra_count = (
        f" COALESCE(SUM(CASE WHEN extra = '{EXTRA_VALUE}' THEN 1 ELSE 0 END), 0)"
        "   AS extra_rows,"
        if add_column
        else ""
    )
    rows = list(
        pipeline.query(
            "SELECT COUNT(*) AS total,"
            " COALESCE(SUM(CASE WHEN id % 2 = 0 THEN 1 ELSE 0 END), 0) AS even_id_rows,"
            f"{partition_match}"
            f"{extra_count}"
            f" COALESCE(SUM(CASE WHEN name = '{UPDATED_NAME}' THEN 1 ELSE 0 END), 0)"
            "   AS updated_rows,"
            f" COALESCE(SUM(CASE WHEN name = '{MERGED_NAME}' THEN 1 ELSE 0 END), 0)"
            "   AS merged_rows"
            f" FROM {TABLE}"
        )
    )
    # Read before stopping: the metric is only live while the pipeline runs.
    input_records = number_of_input_records(pipeline)
    pipeline.stop(force=True)
    return ChangeFeedIngest(
        total=int(rows[0]["total"]),
        even_id_rows=int(rows[0]["even_id_rows"]),
        updated_rows=int(rows[0]["updated_rows"]),
        merged_rows=int(rows[0]["merged_rows"]),
        input_records=input_records,
        extra_rows=int(rows[0]["extra_rows"]) if add_column else None,
        partition_matches=int(rows[0]["partition_matches"]) if partitioned else None,
    )


def _ingest(
    pipeline_name: str,
    *,
    mode: str,
    version: int,
    end_version: int,
    fixture_version: str = FIXTURE_VERSION,
    deletion_vectors: bool = False,
    partitioned: bool = False,
    column_mapping: bool = False,
    add_column: bool = False,
    change_feed: str = "auto",
    columns: str = "id BIGINT NOT NULL, name VARCHAR, value DOUBLE",
) -> ChangeFeedIngest:
    """Ensure the fixture exists, replay `version`..`end_version`, summarize."""
    flags = (
        (["--deletion-vectors"] if deletion_vectors else [])
        + (["--partitioned"] if partitioned else [])
        + (["--column-mapping"] if column_mapping else [])
        + (["--add-column"] if add_column else [])
    )
    loc = DeltaTestLocation.create(
        pipeline_name,
        mode=mode,
        stable_subpath=fixture_version,
    )
    try:
        ensure_delta_spark_fixture(
            loc,
            _FIXTURE_BUILDER,
            [TOTAL_ROWS, EXPECTED_ACTIVE, *flags],
            is_present=_log_has_cdc_entries,
        )
        return _run_to_completion(
            pipeline_name,
            _build_sql(
                loc,
                columns=columns,
                extra_config={
                    "version": version,
                    "end_version": end_version,
                    "change_feed": change_feed,
                },
            ),
            partitioned,
            add_column,
        )
    finally:
        loc.cleanup()


def test_delta_input_change_feed_added_column(pipeline_name):
    """Read a range that spans an `ALTER TABLE ADD COLUMN`.

    Adding a column is the only schema change a change-feed table can undergo:
    Delta rejects `DROP COLUMN` and `RENAME COLUMN` without column mapping, and
    rejects them again with column mapping once a change feed is enabled. So the
    reader never has to interpret change data written under a column that has
    since been renamed away -- only under one that did not exist yet, which is
    this.

    The whole history replays, so the change data of v2, v3 and v5 was written
    before `extra` existed while the SQL table declares it. Those rows must
    arrive with `extra` NULL rather than failing the read, and v7's update, the
    one commit that recorded the column, must be the only row that has it.
    """
    result = _ingest(
        pipeline_name,
        mode="snapshot_and_follow",
        version=CREATE_VERSION,
        end_version=EXTRA_UPDATE_VERSION,
        fixture_version=ADD_COLUMN_FIXTURE_VERSION,
        add_column=True,
        columns="id BIGINT NOT NULL, name VARCHAR, value DOUBLE, extra VARCHAR",
    )

    assert result.total == EXPECTED_ACTIVE, (
        "the added column changes no row count; a short count means commits "
        f"before v{ADD_COLUMN_VERSION} failed to read against the wider schema. "
        f"Got {result.total}"
    )
    assert result.extra_rows == 1, (
        f"only id {EXTRA_UPDATED_ID}, which v{EXTRA_UPDATE_VERSION} updated, may "
        f"carry '{EXTRA_VALUE}'; got {result.extra_rows} rows with it"
    )
    assert result.updated_rows == 1 and result.merged_rows == 1, (
        "the commits before the column was added must still apply in full"
    )
    assert result.even_id_rows == 0, (
        "the delete's invariant must survive the schema change"
    )


def test_delta_input_change_feed_merge(pipeline_name):
    """A `MERGE` is the only commit that records all four change types at once.

    It is also the shape a Databricks source is usually maintained by, and the
    one the change feed is worth the most on: four recorded rows against the
    hundred-row file a copy-on-write merge rewrites.

    Replaying v5 alone isolates it -- the snapshot at v4 is the table before the
    merge, so every row difference is the merge's doing.
    """
    auto = _ingest(
        f"{pipeline_name}_auto",
        mode="snapshot_and_follow",
        version=APPEND_VERSION,
        end_version=MERGE_VERSION,
        change_feed="auto",
    )
    off = _ingest(
        f"{pipeline_name}_off",
        mode="snapshot_and_follow",
        version=APPEND_VERSION,
        end_version=MERGE_VERSION,
        change_feed="off",
    )

    assert auto == off._replace(input_records=auto.input_records), (
        f"the two settings disagree on the table: auto={auto}, off={off}"
    )
    assert auto.total == EXPECTED_ACTIVE, (
        "the merge deletes one row and inserts one, so the count holds; got "
        f"{auto.total}"
    )
    assert auto.merged_rows == 1, (
        "the merge's UPDATE clause must land exactly once: an update reaches the "
        "connector as a retraction of the pre-image and an insertion of the "
        "post-image, and 0 means the two cancelled"
    )
    assert auto.even_id_rows == 0, (
        "the merge inserts an odd id, so no even id may appear"
    )

    assert auto.input_records == ROWS_AFTER_APPEND + MERGE_CHANGE_ROWS, (
        f"expected the {ROWS_AFTER_APPEND}-row snapshot plus the "
        f"{MERGE_CHANGE_ROWS} rows the merge recorded; got {auto.input_records}"
    )
    assert off.input_records > auto.input_records, (
        "a copy-on-write merge rewrites the file its matched rows live in, so "
        f"reading file actions must cost more than {auto.input_records} records; "
        f"got {off.input_records}"
    )


def test_delta_input_change_feed_matches_follow(pipeline_name):
    """Reading a table's whole history through the change feed or through its
    files must land on the same contents.

    Covers every commit shape the reader distinguishes: an `UPDATE`, a `DELETE`,
    and a `MERGE` that record change data, and an append that records none and
    so falls back to the added file.
    """
    auto = _ingest(
        f"{pipeline_name}_auto",
        mode="snapshot_and_follow",
        version=CREATE_VERSION,
        end_version=LAST_VERSION,
        change_feed="auto",
    )
    off = _ingest(
        f"{pipeline_name}_off",
        mode="snapshot_and_follow",
        version=CREATE_VERSION,
        end_version=LAST_VERSION,
        change_feed="off",
    )

    assert auto == off._replace(input_records=auto.input_records), (
        f"the two settings disagree on the table: auto={auto}, off={off}"
    )
    assert auto.total == EXPECTED_ACTIVE, (
        f"expected {EXPECTED_ACTIVE} rows after the fixture's five commits "
        f"({ROWS_AFTER_DELETE} survivors of the delete plus the appended row); "
        f"got {auto.total}"
    )
    assert auto.even_id_rows == 0, (
        "the deleted even ids must be gone; their presence means the DELETE "
        "commit's change data was not applied"
    )
    assert auto.updated_rows == 1, (
        "the UPDATE's post-image must replace its pre-image exactly once"
    )


def test_delta_input_change_feed_reads_only_changed_rows(pipeline_name):
    """The point of the mode: a one-row `UPDATE` costs one row in, one row out.

    Copy-on-write rewrites the whole file, so `follow` reads it twice, and the
    retractions cancel the insertions back to the same table. Only the ingested
    record count tells the two apart, which is why it is the assertion.
    """
    auto = _ingest(
        f"{pipeline_name}_auto",
        mode="snapshot_and_follow",
        version=CREATE_VERSION,
        end_version=UPDATE_VERSION,
        change_feed="auto",
    )
    off = _ingest(
        f"{pipeline_name}_off",
        mode="snapshot_and_follow",
        version=CREATE_VERSION,
        end_version=UPDATE_VERSION,
        change_feed="off",
    )

    assert auto.total == off.total == TOTAL_ROWS, (
        "the UPDATE changes no row count, so both modes must hold the "
        f"insert's {TOTAL_ROWS} rows; got auto={auto.total}, off={off.total}"
    )
    assert auto.updated_rows == off.updated_rows == 1

    assert off.input_records == OFF_UPDATE_INPUT_RECORDS, (
        f"change_feed=off ingested {off.input_records} records for a one-row update; "
        f"expected {OFF_UPDATE_INPUT_RECORDS} (the {TOTAL_ROWS}-row insert plus "
        f"the same file retracted and re-inserted)"
    )
    assert auto.input_records == AUTO_UPDATE_INPUT_RECORDS, (
        f"change_feed=auto ingested {auto.input_records} records for a one-row update; "
        f"expected {AUTO_UPDATE_INPUT_RECORDS} (the {TOTAL_ROWS}-row insert plus "
        "the pre-image and post-image). A count near "
        f"{OFF_UPDATE_INPUT_RECORDS} means the change data files were "
        "ignored and the rewritten file was read instead"
    )


def test_delta_input_change_feed_deletion_vector_delete(pipeline_name):
    """A deletion-vector `DELETE` records no change data, and must still arrive.

    Spark writes change data for the `UPDATE` at v2 but none for the `DELETE` at
    v3: the same-path `add`/`remove` pair and its deletion vectors already say
    which rows left. The connector reads that commit through the fallback and
    retracts exactly the rows the vector newly masks.

    The table is partitioned, so `grp` reaches the connector only from the log
    action -- through the change data path at v2 and the fallback at v3.
    """
    result = _ingest(
        pipeline_name,
        mode="snapshot_and_follow",
        version=CREATE_VERSION,
        end_version=DELETE_VERSION,
        fixture_version=DV_FIXTURE_VERSION,
        deletion_vectors=True,
        partitioned=True,
        columns="id BIGINT NOT NULL, name VARCHAR, value DOUBLE, grp VARCHAR",
    )

    assert result.total == ROWS_AFTER_DELETE, (
        f"expected the {ROWS_AFTER_DELETE} odd ids to survive the deletion-"
        f"vector delete; got {result.total}. {TOTAL_ROWS} means the delete "
        "netted to zero, the failure mode of reading the add/remove pair "
        "without applying its deletion vectors"
    )
    assert result.even_id_rows == 0, (
        "the survivors must be the odd ids, not the deleted even ids"
    )
    assert result.updated_rows == 1, (
        "the v2 update, which does record change data, must still apply"
    )


def _assert_partition_values(result: ChangeFeedIngest, what: str) -> None:
    """Every ingested row must carry the partition value its file was under.

    `grp` is `id % 3`, and Delta keeps it in the log rather than in the data
    file, so a row whose partition value went missing fails this equality
    instead of merely arriving NULL.
    """
    assert result.total == ROWS_AFTER_DELETE, (
        f"{what}: expected {ROWS_AFTER_DELETE} rows, got {result.total}"
    )
    assert result.partition_matches == result.total, (
        f"{what}: {result.total - (result.partition_matches or 0)} of "
        f"{result.total} rows carry the wrong partition value; a change data "
        "file does not name its partition, so the value has to come from the "
        "log action"
    )


def test_delta_input_change_feed_partition_column(pipeline_name):
    """Partition values through the change feed and through the fallback."""
    _assert_partition_values(
        _ingest(
            pipeline_name,
            mode="snapshot_and_follow",
            version=CREATE_VERSION,
            end_version=DELETE_VERSION,
            fixture_version=DV_FIXTURE_VERSION,
            deletion_vectors=True,
            partitioned=True,
            columns="id BIGINT NOT NULL, name VARCHAR, value DOUBLE, grp VARCHAR",
        ),
        "deletion-vector fixture",
    )


def test_delta_input_change_feed_column_mapping(pipeline_name):
    """Read a change feed from a column-mapped table.

    Under `delta.columnMapping.mode = 'name'` every column lives on disk as
    `col-<uuid>`, and change data files are no exception: the connector declares
    them by physical name and renames them back afterwards. `_change_type` is
    not mapped, so it has to survive a projection that rewrites everything
    around it.

    The table is also partitioned, which under column mapping is the harder
    case: Delta drops the `grp=<value>` directory for an opaque prefix and keys
    `partitionValues` by physical name, leaving the log as the only source of
    the value and the logical name useless for looking it up.

    Deletion vectors are off here, so unlike the fixture above every commit
    including the `DELETE` carries change data, and the whole history is read
    through the change feed rather than the fallback.
    """
    result = _ingest(
        pipeline_name,
        mode="snapshot_and_follow",
        version=CREATE_VERSION,
        end_version=DELETE_VERSION,
        fixture_version=MAPPED_FIXTURE_VERSION,
        partitioned=True,
        column_mapping=True,
        columns="id BIGINT NOT NULL, name VARCHAR, value DOUBLE, grp VARCHAR",
    )

    assert result.even_id_rows == 0, (
        "the deleted even ids must be gone; a change data file read under the "
        "wrong names would deliver nulls rather than rows to retract"
    )
    assert result.updated_rows == 1, (
        "the update's post-image must replace its pre-image exactly once"
    )
    _assert_partition_values(result, "column-mapped fixture")
