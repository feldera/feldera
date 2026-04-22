import json
import pathlib
import tempfile

import pyarrow as pa
import pyarrow.parquet as pq

from tests import TEST_CLIENT
from tests.platform.helper import (
    create_pipeline,
    gen_pipeline_name,
    start_pipeline,
    wait_for_condition,
)


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


def collect_output_chunks(stream, expected_rows: int) -> tuple[list[dict], list[dict]]:
    """Read a fixed number of rows from the streaming egress API."""
    chunks: list[dict] = []
    rows: list[dict] = []

    while len(rows) < expected_rows:
        chunk = next(stream)
        chunks.append(chunk)
        rows.extend(chunk.get("json_data") or [])

    return chunks, rows


def _local_delta_dir(pipeline_name: str) -> pathlib.Path:
    """Allocate a fresh local directory to back a Delta table for one test run."""
    return pathlib.Path(tempfile.mkdtemp(prefix=f"{pipeline_name}_delta_", dir="/tmp"))


def _delta_log_paths(local_dir: pathlib.Path) -> list[pathlib.Path]:
    return sorted((local_dir / "_delta_log").glob("*.json"))


def _read_delta_rows(local_dir: pathlib.Path) -> list[dict]:
    """Read the active rows of a Delta table by replaying its transaction log.

    Replays add/remove actions to derive the current set of parquet files and
    reads them with pyarrow. Drops Feldera-internal `__feldera_*` columns. The
    log-only approach avoids the `deltalake` package, whose wheel aborts on
    aarch64 hosts, while still reflecting truncations and rewrites correctly.
    """
    active: dict[str, None] = {}
    for log_path in _delta_log_paths(local_dir):
        for line in log_path.read_text(encoding="utf-8").splitlines():
            action = json.loads(line)
            if (add := action.get("add")) is not None:
                active[add["path"]] = None
            if (remove := action.get("remove")) is not None:
                active.pop(remove["path"], None)

    if not active:
        return []

    tables = [pq.read_table(local_dir / rel) for rel in sorted(active)]
    return [
        {key: value for key, value in row.items() if not key.startswith("__feldera_")}
        for row in pa.concat_tables(tables).to_pylist()
    ]


@gen_pipeline_name
def test_egress_send_snapshot(pipeline_name):
    sql = """
    CREATE TABLE t1(id INT) WITH ('materialized' = 'true');
    CREATE MATERIALIZED VIEW v1 AS SELECT * FROM t1;
    """.strip()

    create_pipeline(pipeline_name, sql)
    start_pipeline(pipeline_name)

    TEST_CLIENT.push_to_pipeline(
        pipeline_name,
        "t1",
        "json",
        [{"id": 1}, {"id": 2}],
        array=True,
        update_format="raw",
        wait=True,
        wait_timeout_s=30.0,
    )

    stream_factory = TEST_CLIENT.listen_to_pipeline(
        pipeline_name,
        "v1",
        format="json",
        send_snapshot=True,
    )
    stream = stream_factory()

    snapshot_chunks, snapshot_rows = collect_output_chunks(stream, 2)
    assert sorted_rows(normalize_egress_rows(snapshot_rows)) == [
        {"id": 1},
        {"id": 2},
    ]
    assert snapshot_chunks
    assert all(chunk.get("snapshot") is True for chunk in snapshot_chunks)

    TEST_CLIENT.push_to_pipeline(
        pipeline_name,
        "t1",
        "json",
        [{"id": 3}],
        array=True,
        update_format="raw",
        wait=True,
        wait_timeout_s=30.0,
    )

    delta_chunks, delta_rows = collect_output_chunks(stream, 1)
    assert normalize_egress_rows(delta_rows) == [{"id": 3}]
    assert delta_chunks
    assert all(chunk.get("snapshot") is False for chunk in delta_chunks)


@gen_pipeline_name
def test_delta_output_send_snapshot(pipeline_name):
    """Drive a `delta_table_output` connector with `send_snapshot: true` and
    confirm the sink reflects the materialized view contents on first start
    and follows incremental updates afterwards.

    Uses a local filesystem-backed Delta table so the test runs without any
    external storage. Reads the table by replaying its transaction log
    (avoiding the `deltalake` Python package, which is unavailable on some
    hosts).
    """
    local_dir = _local_delta_dir(pipeline_name)
    connector = {
        "name": "delta_out",
        "send_snapshot": True,
        "transport": {
            "name": "delta_table_output",
            "config": {
                "uri": f"file://{local_dir}",
                "mode": "truncate",
            },
        },
    }

    sql = f"""
    CREATE TABLE t1(id INT) WITH ('materialized' = 'true');
    CREATE MATERIALIZED VIEW v1 WITH (
      'connectors' = '{json.dumps([connector])}'
    ) AS SELECT * FROM t1;
    """.strip()

    create_pipeline(pipeline_name, sql)
    start_pipeline(pipeline_name)

    TEST_CLIENT.push_to_pipeline(
        pipeline_name,
        "t1",
        "json",
        [{"id": 10}, {"id": 11}],
        array=True,
        update_format="raw",
        wait=True,
        wait_timeout_s=30.0,
    )

    wait_for_condition(
        "delta sink receives initial rows",
        lambda: sorted_rows(_read_delta_rows(local_dir)) == [{"id": 10}, {"id": 11}],
        timeout_s=30.0,
        poll_interval_s=1.0,
    )

    TEST_CLIENT.push_to_pipeline(
        pipeline_name,
        "t1",
        "json",
        [{"id": 12}],
        array=True,
        update_format="raw",
        wait=True,
        wait_timeout_s=30.0,
    )

    wait_for_condition(
        "delta sink follows incremental updates",
        lambda: sorted_rows(_read_delta_rows(local_dir))
        == [{"id": 10}, {"id": 11}, {"id": 12}],
        timeout_s=30.0,
        poll_interval_s=1.0,
    )
