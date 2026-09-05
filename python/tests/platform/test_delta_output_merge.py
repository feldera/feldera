"""Delta Lake output connector: merge mode.

`update_mode = merge` keeps the target table equal to the current contents of the
view, rather than appending a change log for a downstream job to fold in. The
connector supersedes a row by appending its new version and marking the old one
deleted in a deletion vector, so no data file is ever rewritten.

These tests cover what only a running pipeline can show: the config reaches the
connector, the startup checks fire, and the table left behind holds the right
number of live rows and no rewritten files. Row-level correctness of the write
path is covered by the Rust tests in `crates/adapters`;
:func:`test_delta_spark_agrees_through_maintenance` covers agreement with the
reference implementation.

Row counts come from the Delta log rather than the data, because the pinned
`deltalake` wheel refuses a table advertising the `deletionVectors` reader
feature -- which is the reader-compatibility cost merge mode asks a table owner
to accept.
"""

import json
import pathlib
import tempfile

import pytest
from feldera import PipelineBuilder
from feldera.runtime_config import RuntimeConfig
from feldera.testutils import (
    FELDERA_TEST_NUM_HOSTS,
    FELDERA_TEST_NUM_WORKERS,
)
from tests import TEST_CLIENT, enterprise_only
from tests.utils import DeltaTestLocation, run_delta_spark

# ─── helpers ───────────────────────────────────────────────────────────


def _sql(loc: DeltaTestLocation, extra: dict | None = None) -> str:
    """A view keyed on `id`, with a merge-mode connector on it.

    Merge mode needs a unique key, which the index and the connector's `index`
    property supply.
    """
    config = dict(loc.connector_config)
    config["update_mode"] = "merge"
    if extra:
        config.update(extra)
    connectors = json.dumps(
        [
            {
                "index": "v_idx",
                "transport": {"name": "delta_table_output", "config": config},
            }
        ]
    )
    return (
        "CREATE TABLE t (id INT NOT NULL, tag VARCHAR) WITH ('materialized' = 'true');\n"
        "CREATE MATERIALIZED VIEW v WITH ('connectors' = '" + connectors + "') AS "
        "SELECT id, tag FROM t;\n"
        "CREATE INDEX v_idx ON v (id);"
    )


def _build_pipeline(name: str, sql: str):
    return PipelineBuilder(
        TEST_CLIENT,
        name,
        sql=sql,
        runtime_config=RuntimeConfig(
            workers=FELDERA_TEST_NUM_WORKERS,
            hosts=FELDERA_TEST_NUM_HOSTS,
            logging="debug",
        ),
    ).create_or_replace()


def _active_adds(loc: DeltaTestLocation) -> dict[str, dict]:
    """The current `add` action per data file path."""
    active: dict[str, dict] = {}
    for log_path in loc.log_json_paths():
        for line in loc._read_text(log_path).splitlines():
            action = json.loads(line)
            if (add := action.get("add")) is not None:
                active[add["path"]] = add
            elif (remove := action.get("remove")) is not None:
                active.pop(remove["path"], None)
    return active


# ─── tests ─────────────────────────────────────────────────────────────


@enterprise_only
def test_merge_tracks_the_view_row_count(pipeline_name):
    """The table holds one live row per key in the view, through every operation.

    The update is the interesting case: a connector that appended the new row
    without superseding the old one would show 13 rows here instead of 10.
    """
    loc = DeltaTestLocation.create(pipeline_name, mode="append")
    try:
        pipeline = _build_pipeline(pipeline_name, _sql(loc))
        pipeline.start()

        pipeline.input_json(
            "t", [{"id": i, "tag": f"v1_{i}"} for i in range(10)], wait=True
        )
        assert loc.live_row_count() == 10
        after_insert = set(_active_adds(loc))

        # An update reaches the connector as a delete and an insert of one key, which
        # it must recognize as superseding a row rather than as two changes.
        updates = []
        for i in range(3):
            updates.append({"delete": {"id": i, "tag": f"v1_{i}"}})
            updates.append({"insert": {"id": i, "tag": f"v2_{i}"}})
        pipeline.input_json("t", updates, update_format="insert_delete", wait=True)

        # The mechanism before the count, so a count that is right for another reason
        # cannot pass for a working tombstone.
        adds = _active_adds(loc)
        tombstoned = sum(
            add["deletionVector"]["cardinality"]
            for add in adds.values()
            if add.get("deletionVector")
        )
        assert tombstoned == 3, (
            f"expected 3 superseded rows recorded in deletion vectors, found {tombstoned}"
        )
        assert loc.live_row_count() == 10

        # The file holding the superseded rows is still there carrying a vector, not
        # rewritten under a new path.
        assert after_insert <= set(adds), (
            "an update rewrote a data file instead of tombstoning rows in it"
        )

        pipeline.input_json(
            "t",
            [{"delete": {"id": i, "tag": f"v1_{i}"}} for i in range(5, 8)],
            update_format="insert_delete",
            wait=True,
        )
        assert loc.live_row_count() == 7

        # Deleting a key the view no longer holds is a no-op, not a double subtraction.
        pipeline.input_json(
            "t",
            [{"delete": {"id": 5, "tag": f"v1_{5}"}}],
            update_format="insert_delete",
            wait=True,
        )
        assert loc.live_row_count() == 7

        pipeline.stop(force=True)
    finally:
        loc.cleanup()


@enterprise_only
def test_merge_requires_deletion_vectors(pipeline_name):
    """A table without deletion vectors must fail the pipeline at startup.

    The connector sets the property on a table it creates, so the fixture builds one
    without it: an externally administered table before anyone ran the `ALTER TABLE`.
    The error must name that statement, since the connector will not run it.
    """
    import pyarrow as pa
    from deltalake import write_deltalake

    loc = DeltaTestLocation.create(pipeline_name, mode="append")
    try:
        write_deltalake(
            loc.uri,
            pa.table({"id": pa.array([1], pa.int32()), "tag": pa.array(["x"])}),
            storage_options=loc.writer_storage_options(),
        )

        pipeline = _build_pipeline(pipeline_name, _sql(loc))
        with pytest.raises(Exception) as caught:
            pipeline.start()
        message = str(caught.value)
        assert "enableDeletionVectors" in message, message
        assert "ALTER TABLE" in message, message
    finally:
        loc.cleanup()


@enterprise_only
def test_merge_into_a_partitioned_table(pipeline_name):
    """A partitioned target table, with the partition column inside the key.

    Delta keeps a partition column's value in the log, not in the data file, so the
    connector must reconstruct it to find the row to supersede. This needs a table the
    connector did not create, so the fixture builds it first.
    """
    import pyarrow as pa
    from deltalake import write_deltalake

    loc = DeltaTestLocation.create(pipeline_name, mode="append")
    try:
        # An empty partitioned table with deletion vectors on, as an administrator
        # would hand it over.
        write_deltalake(
            loc.uri,
            pa.table(
                {
                    "id": pa.array([], pa.int32()),
                    "tag": pa.array([], pa.string()),
                }
            ),
            partition_by=["tag"],
            configuration={"delta.enableDeletionVectors": "true"},
            storage_options=loc.writer_storage_options(),
        )

        config = dict(loc.connector_config)
        config["update_mode"] = "merge"
        connectors = json.dumps(
            [
                {
                    "index": "v_idx",
                    "transport": {"name": "delta_table_output", "config": config},
                }
            ]
        )
        sql = (
            "CREATE TABLE t (id INT NOT NULL, tag VARCHAR NOT NULL) "
            "WITH ('materialized' = 'true');\n"
            "CREATE MATERIALIZED VIEW v WITH ('connectors' = '" + connectors + "') AS "
            "SELECT id, tag FROM t;\n"
            # The partition column is part of the key.
            "CREATE INDEX v_idx ON v (id, tag);"
        )

        pipeline = _build_pipeline(pipeline_name, sql)
        pipeline.start()

        # The same id in two partitions: superseding one must leave the other alone.
        pipeline.input_json(
            "t",
            [{"id": i, "tag": tag} for tag in ("a", "b") for i in range(5)],
            wait=True,
        )
        assert loc.live_row_count() == 10

        pipeline.input_json(
            "t",
            [{"delete": {"id": 3, "tag": "b"}}],
            update_format="insert_delete",
            wait=True,
        )
        assert loc.live_row_count() == 9

        adds = _active_adds(loc)
        tombstoned = sum(
            add["deletionVector"]["cardinality"]
            for add in adds.values()
            if add.get("deletionVector")
        )
        assert tombstoned == 1, (
            f"expected exactly one superseded row, found {tombstoned}: deleting (3, 'b') "
            "must not touch (3, 'a')"
        )

        pipeline.stop(force=True)
    finally:
        loc.cleanup()


@enterprise_only
def test_merge_requires_a_unique_key(pipeline_name):
    """Without the `index` property there is no row to supersede."""
    loc = DeltaTestLocation.create(pipeline_name, mode="append")
    try:
        config = dict(loc.connector_config)
        config["update_mode"] = "merge"
        connectors = json.dumps(
            [{"transport": {"name": "delta_table_output", "config": config}}]
        )
        sql = (
            "CREATE TABLE t (id INT NOT NULL) WITH ('materialized' = 'true');\n"
            "CREATE MATERIALIZED VIEW v WITH ('connectors' = '"
            + connectors
            + "') AS SELECT id FROM t;"
        )

        pipeline = _build_pipeline(pipeline_name, sql)
        with pytest.raises(Exception) as caught:
            pipeline.start()
        assert "unique key" in str(caught.value), str(caught.value)
    finally:
        loc.cleanup()


@enterprise_only
def test_delta_spark_agrees_through_maintenance(pipeline_name):
    """Delta Spark must read what merge mode left live, and keep reading it.

    The only check against the reference implementation; delta-rs cannot be
    evidence, because the connector writes through it. Reading proves the
    ``remove``/``add`` pair installs a vector Spark honors, ``OPTIMIZE`` that a
    rewrite materializes it, ``VACUUM`` that its file counts as referenced.
    """
    loc = DeltaTestLocation.create(pipeline_name, mode="append")
    with tempfile.TemporaryDirectory(prefix="merge_spark_") as staging:
        try:
            pipeline = _build_pipeline(pipeline_name, _sql(loc))
            pipeline.start()

            pipeline.input_json(
                "t", [{"id": i, "tag": f"v1_{i}"} for i in range(6)], wait=True
            )
            # A strict subset superseded, so a vector survives: superseding
            # every row in a file drops the file instead.
            changes = []
            for i in range(3):
                changes.append({"delete": {"id": i, "tag": f"v1_{i}"}})
                changes.append({"insert": {"id": i, "tag": f"v2_{i}"}})
            changes.append({"delete": {"id": 5, "tag": "v1_5"}})
            pipeline.input_json("t", changes, update_format="insert_delete", wait=True)
            pipeline.stop(force=True)

            expected = sorted(
                [{"id": i, "tag": f"v2_{i}"} for i in range(3)]
                + [{"id": i, "tag": f"v1_{i}"} for i in (3, 4)],
                key=lambda row: row["id"],
            )
            assert loc.live_row_count() == len(expected)
            # Otherwise the test would pass on a table with nothing to honor.
            assert any(
                add.get("deletionVector") for add in _active_adds(loc).values()
            ), "the fixture left no deletion vector for Spark to honor"

            table = loc.fetch_tree(pathlib.Path(staging) / "table")
            script = pathlib.Path(__file__).parent / "fixtures" / "merge_verify.py"
            stdout = run_delta_spark(script, [table])
            # Spark logs to stdout too, so take only the JSON lines.
            phases = {}
            for line in stdout.splitlines():
                if line.startswith("{"):
                    report = json.loads(line)
                    phases[report["phase"]] = sorted(
                        report["rows"], key=lambda row: row["id"]
                    )

            for phase in ("initial", "after_optimize", "after_vacuum"):
                assert phases[phase] == expected, (
                    f"Delta Spark disagrees with the connector {phase}: "
                    f"{phases[phase]} != {expected}"
                )
        finally:
            loc.cleanup()
