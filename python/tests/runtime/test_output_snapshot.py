import json
import time

from feldera import PipelineBuilder
from feldera.enums import BootstrapPolicy
from feldera.pipeline import Pipeline
from feldera.runtime_config import RuntimeConfig, Storage
from feldera.testutils import FELDERA_TEST_NUM_WORKERS
from tests import TEST_CLIENT
from tests.utils import DeltaTestLocation, wait_for_condition


def sorted_rows(rows: list[dict]) -> list[dict]:
    """Sort rows into a stable order for assertions."""
    return sorted(rows, key=lambda row: json.dumps(row, sort_keys=True, default=str))


def normalize_egress_rows(rows: list[dict]) -> list[dict]:
    """Strip JSON egress update envelopes down to plain row objects."""
    normalized = []
    for row in rows:
        if "insert" in row and isinstance(row["insert"], dict):
            normalized.append(row["insert"])
        elif "delete" in row and isinstance(row["delete"], dict):
            normalized.append(row["delete"])
        else:
            normalized.append(row)
    return normalized


def collect_output_chunks(
    stream, expected_rows: int, timeout_s: float = 60.0
) -> tuple[list[dict], list[dict]]:
    """Read a fixed number of rows from the streaming egress API.

    Fails if the rows do not arrive within ``timeout_s`` seconds.  An idle
    stream sends an empty chunk every 3 seconds, so the deadline is checked
    even when no rows arrive.
    """
    deadline = time.monotonic() + timeout_s
    chunks: list[dict] = []
    rows: list[dict] = []

    while len(rows) < expected_rows:
        assert time.monotonic() < deadline, (
            f"received {len(rows)} of {expected_rows} rows in {timeout_s} s"
        )
        chunk = next(stream)
        chunks.append(chunk)
        rows.extend(chunk.get("json_data") or [])

    return chunks, rows


def test_egress_send_snapshot(pipeline_name):
    """An `/egress` stream with `send_snapshot` starts with the view's
    contents, then carries the view's changes.

    A Delta sink on the view's first index receives every change while the
    stream is open and after it closes.  The index shares the view's output
    handle, and `v1` sorts before `v1_idx`: the order in which the stream
    used to take all of the view's output from the sink (#7281).
    """
    location = DeltaTestLocation.create(pipeline_name)
    sink = _delta_connector(location, send_snapshot=False, index="v1_idx")
    sql = f"""
    CREATE TABLE t1(id INT NOT NULL) WITH ('materialized' = 'true');
    CREATE MATERIALIZED VIEW v1 WITH (
        'connectors' = '{json.dumps([sink])}'
    ) AS SELECT * FROM t1;
    CREATE INDEX v1_idx ON v1(id);
    """.strip()

    def push(ids: list[int]):
        """Insert rows with `ids` into `t1` and wait until they are processed."""
        TEST_CLIENT.push_to_pipeline(
            pipeline_name,
            "t1",
            "json",
            [{"id": i} for i in ids],
            array=True,
            update_format="raw",
            wait=True,
            wait_timeout_s=30.0,
        )

    def wait_for_sink(ids: list[int]):
        """Wait until the Delta sink holds exactly the rows with `ids`."""
        expected = sorted_rows([{"id": i} for i in ids])
        wait_for_condition(
            f"the Delta sink on v1_idx holds ids {ids}",
            lambda: sorted_rows(location.read_rows()) == expected,
            timeout_s=60.0,
            poll_interval_s=1.0,
        )

    def egress_endpoints() -> list[str]:
        """Names of the pipeline's `/egress` endpoints."""
        return [
            output.endpoint_name
            for output in pipeline.stats().outputs
            if ".api-" in (output.endpoint_name or "")
        ]

    pipeline = PipelineBuilder(
        TEST_CLIENT,
        name=pipeline_name,
        sql=sql,
        runtime_config=RuntimeConfig(workers=FELDERA_TEST_NUM_WORKERS),
    ).create_or_replace()
    pipeline.start()

    push([1, 2])
    wait_for_sink([1, 2])

    # Open the stream directly: `listen_to_pipeline` keeps the response in a
    # generator that the test cannot close.
    response = TEST_CLIENT.http.post(
        path=f"/pipelines/{pipeline_name}/egress/v1",
        params={
            "format": "json",
            "array": "false",
            "backpressure": "true",
            "send_snapshot": "true",
        },
        stream=True,
    )
    stream = (json.loads(line) for line in response.iter_lines() if line)

    snapshot_chunks, snapshot_rows = collect_output_chunks(stream, 2)
    assert sorted_rows(normalize_egress_rows(snapshot_rows)) == [
        {"id": 1},
        {"id": 2},
    ]
    assert snapshot_chunks
    assert all(chunk.get("snapshot") is True for chunk in snapshot_chunks)

    push([3])

    delta_chunks, delta_rows = collect_output_chunks(stream, 1)
    assert normalize_egress_rows(delta_rows) == [{"id": 3}]
    assert delta_chunks
    assert all(chunk.get("snapshot") is False for chunk in delta_chunks)
    wait_for_sink([1, 2, 3])

    # Close the stream, and wait for the pipeline to remove its endpoint.
    response.close()
    wait_for_condition(
        "the pipeline removes the /egress endpoint",
        lambda: not egress_endpoints(),
        timeout_s=30.0,
        poll_interval_s=0.5,
    )

    push([4])
    wait_for_sink([1, 2, 3, 4])


# Number of deterministic rows pushed into ``t1`` and expected to land
# identically in every Delta sink. Large enough that the snapshot path
# isn't trivially indistinguishable from a tiny delta.
_NUM_ROWS = 1000


def _make_rows(n: int) -> list[dict]:
    """Generate ``n`` deterministic rows. ``id`` is the integer primary key
    used by the indexed views; ``n``/``s`` are derived from ``id`` so each
    row is uniquely recognizable on read-back.
    """
    return [{"id": i, "n": i * 100, "s": f"row_{i:04d}"} for i in range(1, n + 1)]


_ROWS = _make_rows(_NUM_ROWS)


def _delta_connector(
    location: DeltaTestLocation,
    *,
    send_snapshot: bool,
    name: str = "delta_out",
    index: str | None = None,
) -> dict:
    """Build a `delta_table_output` connector config."""
    connector: dict = {
        "name": name,
        "send_snapshot": send_snapshot,
        "transport": {
            "name": "delta_table_output",
            "config": location.connector_config,
        },
    }
    if index is not None:
        connector["index"] = index
    return connector


_VIEWS = (
    # (label, view name, index name passed to connector, CREATE INDEX statements)
    ("v_plain", "v_plain", None, ""),
    (
        "v_one_index",
        "v_one_index",
        "v_one_index_idx",
        "CREATE INDEX v_one_index_idx ON v_one_index(id);",
    ),
    (
        "v_two_indexes",
        "v_two_indexes",
        "v_two_indexes_idx_id",
        "CREATE INDEX v_two_indexes_idx_id ON v_two_indexes(id);\n"
        "CREATE INDEX v_two_indexes_idx_n ON v_two_indexes(n);",
    ),
    (
        "v_two_indexes_2",
        "v_two_indexes_2",
        "v_two_indexes_2_idx_n",
        "CREATE INDEX v_two_indexes_2_idx_id ON v_two_indexes_2(id);\n"
        "CREATE INDEX v_two_indexes_2_idx_n ON v_two_indexes_2(n);",
    ),
)


def _build_sql(locations: list[DeltaTestLocation], send_snapshot: bool) -> str:
    """Render the pipeline SQL with all four views, choosing the value of
    ``send_snapshot`` for each connector. Index-attached connectors use
    the same index name across rounds (only the boolean flips), so the
    pipeline diff between two SQL snapshots classifies each connector as
    modified rather than added/removed.
    """
    parts = [
        "CREATE TABLE t1(id INT NOT NULL, n BIGINT NOT NULL, s VARCHAR NOT NULL)\n"
        "    WITH ('materialized' = 'true');",
    ]
    for (_, view, index, indexes_sql), loc in zip(_VIEWS, locations):
        connector = _delta_connector(loc, send_snapshot=send_snapshot, index=index)
        parts.append(
            f"CREATE MATERIALIZED VIEW {view} WITH (\n"
            f"    'connectors' = '{json.dumps([connector])}'\n"
            f") AS SELECT * FROM t1;"
        )
        if indexes_sql:
            parts.append(indexes_sql)
    return "\n\n".join(parts)


def _build_sql_round3(
    locations: list[DeltaTestLocation],
    delta1_location: DeltaTestLocation,
    delta2_location: DeltaTestLocation,
    delta3_location: DeltaTestLocation,
) -> str:
    """Render SQL for round 3.

    The original connectors remain unchanged with ``send_snapshot=true``.
    The new view forces bootstrapping.  ``delta2`` and ``delta3`` are added
    to an existing view with snapshot enabled and disabled, respectively.
    """
    parts = [
        "CREATE TABLE t1(id INT NOT NULL, n BIGINT NOT NULL, s VARCHAR NOT NULL)\n"
        "    WITH ('materialized' = 'true');",
    ]
    for (label, view, index, indexes_sql), loc in zip(_VIEWS, locations):
        connectors = [
            _delta_connector(loc, send_snapshot=True, index=index),
        ]
        if label == "v_plain":
            connectors.extend(
                [
                    _delta_connector(
                        delta2_location,
                        send_snapshot=True,
                        name="delta2",
                    ),
                    _delta_connector(
                        delta3_location,
                        send_snapshot=False,
                        name="delta3",
                    ),
                ]
            )
        parts.append(
            f"CREATE MATERIALIZED VIEW {view} WITH (\n"
            f"    'connectors' = '{json.dumps(connectors)}'\n"
            f") AS SELECT * FROM t1;"
        )
        if indexes_sql:
            parts.append(indexes_sql)

    delta1 = _delta_connector(delta1_location, send_snapshot=False, name="delta1")
    parts.append(
        "CREATE MATERIALIZED VIEW v_added WITH (\n"
        f"    'connectors' = '{json.dumps([delta1])}'\n"
        ") AS SELECT * FROM t1;"
    )
    return "\n\n".join(parts)


def test_delta_output_send_snapshot_after_flag_flip(pipeline_name):
    """Verify snapshot delivery to delta sinks across a connector
    modification (`send_snapshot: false` → `send_snapshot: true`).

    Four view shapes are exercised, each with its own delta sink:

    * ``v_plain``         – materialized view, no index.
    * ``v_one_index``     – materialized view with one ``CREATE INDEX``;
                            the connector reads the view through that
                            index.
    * ``v_two_indexes``   – materialized view with two ``CREATE INDEX``;
                            the connector reads through the first.
    * ``v_two_indexes_2`` – materialized view with two ``CREATE INDEX``;
                            the connector reads through the second.

    Round 1 (`send_snapshot=false`):
        Push the rows. They reach every delta sink as ordinary
        incremental updates. Take a checkpoint, stop the pipeline.

    Round 2 (`send_snapshot=true`):
        Resume from the checkpoint with `send_snapshot` flipped to
        ``true`` on every connector. The view rows are recovered from
        the checkpoint and each connector replays them as its initial
        snapshot, repopulating its delta sink from scratch.

    Round 2 pushes no new data, so each delta sink reaching ``expected``
    proves the initial snapshot delivered the full materialized-view
    contents — for every combination of indexes and which index the
    connector reads through.

    Round 3 adds a new view with a new Delta connector (``delta1``) and
    adds two Delta connectors to an existing view: ``delta2`` with
    ``send_snapshot=true`` and ``delta3`` with ``send_snapshot=false``.
    Adding the view forces actual bootstrapping.  ``delta1`` and
    ``delta2`` should be populated during startup, while ``delta3``
    should remain empty until future deltas arrive.
    """
    locations = [
        DeltaTestLocation.create(f"{pipeline_name}_{label}") for label, *_ in _VIEWS
    ]
    delta1_location = DeltaTestLocation.create(f"{pipeline_name}_delta1")
    delta2_location = DeltaTestLocation.create(f"{pipeline_name}_delta2")
    delta3_location = DeltaTestLocation.create(f"{pipeline_name}_delta3")
    expected = sorted_rows(_ROWS)

    # Round 1: send_snapshot=false. Push three rows; they land via the
    # delta path.
    pipeline = PipelineBuilder(
        TEST_CLIENT,
        name=pipeline_name,
        sql=_build_sql(locations, send_snapshot=False),
        runtime_config=RuntimeConfig(
            workers=FELDERA_TEST_NUM_WORKERS,
            storage=Storage(),
        ),
    ).create_or_replace()
    pipeline.start()

    pipeline.input_json("t1", _ROWS, wait=True, wait_timeout_s=30.0)

    for (label, *_), loc in zip(_VIEWS, locations):
        wait_for_condition(
            f"round 1: {label} delta sink receives rows via the delta path",
            lambda loc=loc: sorted_rows(loc.read_rows()) == expected,
            timeout_s=60.0,
            poll_interval_s=1.0,
        )

    pipeline.checkpoint(wait=True)
    pipeline.stop(force=True)

    # Round 2: flip to send_snapshot=true and resume from the checkpoint.
    TEST_CLIENT.patch_pipeline(
        name=pipeline_name,
        sql=_build_sql(locations, send_snapshot=True),
        runtime_config=RuntimeConfig(
            workers=FELDERA_TEST_NUM_WORKERS,
            storage=Storage(config={"start_from_checkpoint": "latest"}),
        ).to_dict(),
    )
    pipeline = Pipeline.get(pipeline_name, TEST_CLIENT)
    pipeline.start(bootstrap_policy=BootstrapPolicy.ALLOW)

    for (label, *_), loc in zip(_VIEWS, locations):
        wait_for_condition(
            f"round 2: {label} delta sink rebuilt from the snapshot",
            lambda loc=loc: sorted_rows(loc.read_rows()) == expected,
            timeout_s=60.0,
            poll_interval_s=1.0,
        )

    for (_, *_), loc in zip(_VIEWS, locations):
        assert sorted_rows(loc.read_rows()) == expected

    pipeline.checkpoint(wait=True)
    pipeline.stop(force=True)

    # Round 3: add a new view and new output connectors to an existing view.
    # The new view forces bootstrapping.  The connector on the new view and
    # the existing-view connector with send_snapshot=true are populated during
    # startup; the existing-view connector with send_snapshot=false is not.
    TEST_CLIENT.patch_pipeline(
        name=pipeline_name,
        sql=_build_sql_round3(
            locations,
            delta1_location,
            delta2_location,
            delta3_location,
        ),
        runtime_config=RuntimeConfig(
            workers=FELDERA_TEST_NUM_WORKERS,
            storage=Storage(config={"start_from_checkpoint": "latest"}),
        ).to_dict(),
    )
    pipeline = Pipeline.get(pipeline_name, TEST_CLIENT)
    pipeline.start(bootstrap_policy=BootstrapPolicy.ALLOW)

    wait_for_condition(
        "round 3: delta1 on new bootstrapped view receives rows",
        lambda: sorted_rows(delta1_location.read_rows()) == expected,
        timeout_s=60.0,
        poll_interval_s=1.0,
    )
    wait_for_condition(
        "round 3: delta2 on existing view receives snapshot rows",
        lambda: sorted_rows(delta2_location.read_rows()) == expected,
        timeout_s=60.0,
        poll_interval_s=1.0,
    )
    assert delta3_location.read_rows() == []
