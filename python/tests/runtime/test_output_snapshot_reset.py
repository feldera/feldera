import json
import pathlib
import tempfile
import time
import uuid
from collections import Counter
from dataclasses import dataclass
from urllib.parse import urlparse

import pyarrow as pa
import pyarrow.fs as pafs
import pyarrow.parquet as pq

from feldera.enums import BootstrapPolicy
from feldera.pipeline_builder import PipelineBuilder
from feldera.runtime_config import RuntimeConfig
from feldera.testutils import FELDERA_TEST_NUM_HOSTS, FELDERA_TEST_NUM_WORKERS
from tests import (
    MINIO_BUCKET,
    MINIO_ENDPOINT,
    MINIO_REGION,
    TEST_CLIENT,
    enterprise_only,
    required_env,
    runs_in_ci,
)
from tests.platform.helper import (
    adhoc_query_json,
    create_pipeline,
    gen_pipeline_name,
    pipeline_stats,
    start_pipeline,
    wait_for_condition,
    wait_for_program_success,
)


@dataclass
class DeltaTestLocation:
    """Describe where the Delta sink writes test data and how to read it back."""

    uri: str
    connector_config: dict[str, object]
    root_path: str
    local_dir: pathlib.Path | None = None
    filesystem: pafs.FileSystem | None = None

    @classmethod
    def create(cls, pipeline_name: str) -> "DeltaTestLocation":
        """Use the local filesystem for local runs and MinIO-backed S3 in CI."""
        if runs_in_ci():
            access_key = required_env("CI_K8S_MINIO_ACCESS_KEY_ID")
            secret_key = required_env("CI_K8S_MINIO_SECRET_ACCESS_KEY")
            prefix = f"{pipeline_name}/{uuid.uuid4().hex}"
            root_path = f"{MINIO_BUCKET}/{prefix}"
            minio_endpoint = MINIO_ENDPOINT.rstrip("/")
            parsed_endpoint = urlparse(minio_endpoint)
            if (
                parsed_endpoint.scheme not in {"http", "https"}
                or not parsed_endpoint.netloc
            ):
                raise ValueError(
                    "CI_MINIO_ENDPOINT must be a full URL, e.g. "
                    "'http://minio.minio.svc.cluster.local:9000'"
                )

            return cls(
                uri=f"s3://{root_path}",
                connector_config={
                    "uri": f"s3://{root_path}",
                    "mode": "truncate",
                    "aws_access_key_id": access_key,
                    "aws_secret_access_key": secret_key,
                    "aws_region": MINIO_REGION,
                    "aws_endpoint": minio_endpoint,
                    "aws_allow_http": str(parsed_endpoint.scheme == "http").lower(),
                },
                root_path=root_path,
                filesystem=pafs.S3FileSystem(
                    access_key=access_key,
                    secret_key=secret_key,
                    region=MINIO_REGION,
                    scheme=parsed_endpoint.scheme,
                    endpoint_override=parsed_endpoint.netloc,
                ),
            )

        local_dir = pathlib.Path(
            tempfile.mkdtemp(prefix=f"{pipeline_name}_delta_", dir="/tmp")
        )
        return cls(
            uri=f"file://{local_dir}",
            connector_config={
                "uri": f"file://{local_dir}",
                "mode": "truncate",
            },
            root_path=str(local_dir),
            local_dir=local_dir,
        )

    def log_json_paths(self) -> list[str]:
        """List Delta transaction log JSON files in version order."""
        if self.local_dir is not None:
            return [
                str(path)
                for path in sorted((self.local_dir / "_delta_log").glob("*.json"))
            ]

        assert self.filesystem is not None
        infos = self.filesystem.get_file_info(
            pafs.FileSelector(f"{self.root_path}/_delta_log", recursive=False)
        )
        return sorted(
            info.path
            for info in infos
            if info.type == pafs.FileType.File and info.path.endswith(".json")
        )

    def read_text(self, path: str) -> str:
        """Read a text file from the configured Delta storage backend."""
        if self.local_dir is not None:
            return pathlib.Path(path).read_text(encoding="utf-8")

        assert self.filesystem is not None
        with self.filesystem.open_input_file(path) as handle:
            return handle.readall().decode("utf-8")

    def read_table(self, relative_path: str) -> pa.Table:
        """Read a Delta data file as a PyArrow table."""
        if self.local_dir is not None:
            return pq.read_table(self.local_dir / relative_path)

        assert self.filesystem is not None
        return pq.read_table(
            f"{self.root_path}/{relative_path}", filesystem=self.filesystem
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


def delta_log_version(location: DeltaTestLocation) -> int:
    """Return the latest Delta transaction log version written by the sink."""
    versions = [
        int(pathlib.PurePosixPath(path).stem)
        for path in location.log_json_paths()
        if pathlib.PurePosixPath(path).stem.isdigit()
    ]
    if not versions:
        raise AssertionError(f"no Delta log versions found under {location.uri}")
    return max(versions)


def read_delta_rows(location: DeltaTestLocation) -> list[dict]:
    """Read the current Delta table contents and drop Feldera metadata columns."""
    active_paths: dict[str, None] = {}

    for log_path in location.log_json_paths():
        for line in location.read_text(log_path).splitlines():
            action = json.loads(line)
            add = action.get("add")
            if add is not None:
                active_paths[add["path"]] = None
            remove = action.get("remove")
            if remove is not None:
                active_paths.pop(remove["path"], None)

    if not active_paths:
        return []

    tables = [location.read_table(rel_path) for rel_path in sorted(active_paths)]
    return [
        {key: value for key, value in row.items() if not key.startswith("__feldera_")}
        for row in pa.concat_tables(tables).to_pylist()
    ]


def read_delta_ids(location: DeltaTestLocation) -> list[int]:
    """Read Delta table ids in stable order."""
    return sorted(int(row["id"]) for row in read_delta_rows(location))


def view_ids(pipeline_name: str, view_name: str) -> list[int]:
    """Read ids from a materialized view in stable order."""
    rows = adhoc_query_json(pipeline_name, f"SELECT id FROM {view_name} ORDER BY id")
    return [int(row["id"]) for row in rows]


def assert_incrementing_ids(
    actual_ids: list[int], expected_count: int, description: str
) -> None:
    """Assert ids are exactly 0..N without dumping thousands of ids on failure."""
    expected_ids = list(range(expected_count))
    counts = Counter(actual_ids)
    duplicate_ids = [id for id, count in sorted(counts.items()) if count > 1][:5]
    missing_ids = [id for id in expected_ids if id not in counts][:5]
    if actual_ids != expected_ids:
        raise AssertionError(
            f"{description}: expected ids 0..{expected_count - 1}; "
            f"got {len(actual_ids)} ids, unique={len(counts)}, "
            f"missing_first={missing_ids}, duplicate_first={duplicate_ids}, "
            f"first={actual_ids[:5]}, last={actual_ids[-5:]}"
        )


def output_connector_metrics(
    pipeline_name: str, view_name: str, connector_name: str
) -> dict:
    """Return raw output connector metrics."""
    return TEST_CLIENT.output_connector_stats(pipeline_name, view_name, connector_name)[
        "metrics"
    ]


def build_bigint_datagen_delta_sql(
    table_location: DeltaTestLocation,
    *,
    include_datagen: bool,
    datagen_rows: int,
    output_threads: int,
    use_index: bool,
    send_snapshot: bool,
) -> str:
    """Build the datagen-to-Delta SQL used by the checkpoint/reset test."""
    output_config = dict(table_location.connector_config)
    output_config["threads"] = output_threads
    output_connector = {
        "name": "delta_out",
        "send_snapshot": send_snapshot,
        "transport": {
            "name": "delta_table_output",
            "config": output_config,
        },
        "enable_output_buffer": True,
        "max_output_buffer_time_millis": 1000,
    }
    if use_index:
        output_connector["index"] = "bigint_sequence_keys_out_idx"

    if include_datagen:
        input_connector = {
            "name": "datagen",
            "paused": True,
            "transport": {
                "name": "datagen",
                "config": {
                    "workers": 1,
                    "plan": [
                        {
                            "limit": datagen_rows,
                            "rate": 100,
                            "worker_chunk_size": 100,
                            "fields": {
                                "id": {
                                    "strategy": "increment",
                                    "range": [0, datagen_rows],
                                }
                            },
                        }
                    ],
                },
            },
        }
        table_with_clause = f"""
        WITH (
          'materialized' = 'true',
          'connectors' = '{json.dumps([input_connector])}'
        )
        """
    else:
        table_with_clause = "WITH ('materialized' = 'true')"

    index_sql = (
        "CREATE INDEX bigint_sequence_keys_out_idx ON bigint_sequence_keys_out(id);"
        if use_index
        else ""
    )

    return f"""
    CREATE TABLE bigint_sequence_keys (
      id BIGINT NOT NULL
    ) {table_with_clause};

    CREATE MATERIALIZED VIEW bigint_sequence_keys_out
    WITH (
      'connectors' = '{json.dumps([output_connector])}'
    )
    AS SELECT * FROM bigint_sequence_keys;

    {index_sql}
    """.strip()


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
def test_delta_output_reset(pipeline_name):
    table_location = DeltaTestLocation.create(pipeline_name)
    connector = {
        "name": "delta_out",
        "send_snapshot": True,
        "transport": {
            "name": "delta_table_output",
            "config": table_location.connector_config,
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
        lambda: (
            sorted_rows(read_delta_rows(table_location)) == [{"id": 10}, {"id": 11}]
        ),
        timeout_s=30.0,
        poll_interval_s=1.0,
    )
    version_before_reset = delta_log_version(table_location)

    # Multiple resets in quick succession must not compromise sink state once
    # the final reset proceeds and its snapshot is replayed.
    burst_reset_tokens = [
        TEST_CLIENT.reset_output_connector(pipeline_name, "v1", "delta_out")
        for _ in range(10)
    ]
    assert all(burst_reset_tokens), "each burst reset must return a token"
    assert len(set(burst_reset_tokens)) == len(burst_reset_tokens)

    reset_token = TEST_CLIENT.reset_output_connector(pipeline_name, "v1", "delta_out")
    assert reset_token, "reset endpoint must return a token"
    TEST_CLIENT.wait_for_reset(pipeline_name, reset_token, timeout_s=30.0)
    assert TEST_CLIENT.reset_completed(pipeline_name, reset_token)

    wait_for_condition(
        "delta sink is truncated and snapshot replayed after reset",
        lambda: (
            delta_log_version(table_location) > version_before_reset
            and sorted_rows(read_delta_rows(table_location)) == [{"id": 10}, {"id": 11}]
        ),
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
        "delta sink follows incremental updates after reset",
        lambda: (
            sorted_rows(read_delta_rows(table_location))
            == [{"id": 10}, {"id": 11}, {"id": 12}]
        ),
        timeout_s=30.0,
        poll_interval_s=1.0,
    )


def run_delta_reset_checkpoint_snapshot_scenario(
    pipeline_name: str,
    *,
    output_threads: int,
    use_index: bool,
    reset_count: int = 1,
) -> None:
    """1. Start paused datagen inside one manual transaction.
    2. Reset Delta output one or more times after the transaction has produced rows.
    3. Commit and assert Delta exactly matches the materialized view.
    4. Checkpoint, restart without input, replay a snapshot, and assert Delta matches.
    """
    datagen_rows = 2000
    table_location = DeltaTestLocation.create(pipeline_name)
    view_name = "bigint_sequence_keys_out"
    connector_name = "delta_out"
    expected_ids = list(range(datagen_rows))

    sql_with_input = build_bigint_datagen_delta_sql(
        table_location,
        include_datagen=True,
        datagen_rows=datagen_rows,
        output_threads=output_threads,
        use_index=use_index,
        send_snapshot=False,
    )
    pipeline = PipelineBuilder(
        TEST_CLIENT,
        pipeline_name,
        sql=sql_with_input,
        runtime_config=RuntimeConfig(
            workers=FELDERA_TEST_NUM_WORKERS,
            hosts=FELDERA_TEST_NUM_HOSTS,
            fault_tolerance_model=None,
        ),
    ).create_or_replace()

    pipeline.start(timeout_s=300)
    transaction_id = pipeline.start_transaction()
    pipeline.resume_connector("bigint_sequence_keys", "datagen")
    wait_for_condition(
        "datagen produces rows inside the manual transaction",
        lambda: (
            (stats := pipeline_stats(pipeline_name).get("global_metrics", {})).get(
                "transaction_status"
            )
            == "TransactionInProgress"
            and stats.get("transaction_records", 0) > 0
        ),
        timeout_s=15.0,
        poll_interval_s=0.5,
    )

    for reset_index in range(reset_count):
        reset_token = pipeline.reset_output_connector(view_name, connector_name)
        assert reset_token, "reset endpoint must return a token"
        pipeline.wait_for_reset(reset_token, timeout_s=60.0)
        assert pipeline.reset_completed(reset_token)
        if reset_index + 1 < reset_count:
            time.sleep(1.0)

    def datagen_produced_all_rows() -> bool:
        stats = pipeline_stats(pipeline_name)
        return any(
            endpoint.get("metrics", {}).get("total_records", 0) >= datagen_rows
            for endpoint in stats.get("inputs", [])
        )

    wait_for_condition(
        "datagen produces the requested rows",
        datagen_produced_all_rows,
        timeout_s=60.0,
        poll_interval_s=1.0,
    )
    pipeline.commit_transaction(
        transaction_id=transaction_id,
        wait=True,
        timeout_s=60.0,
    )

    def datagen_finished_and_output_drained() -> bool:
        stats = pipeline_stats(pipeline_name)
        input_done = all(
            endpoint.get("metrics", {}).get("end_of_input") is True
            for endpoint in stats.get("inputs", [])
        )
        no_transaction = (
            stats.get("global_metrics", {}).get("transaction_status") == "NoTransaction"
        )
        metrics = output_connector_metrics(pipeline_name, view_name, connector_name)
        output_drained = (
            metrics.get("queued_records", 0) == 0
            and metrics.get("buffered_records", 0) == 0
            and metrics.get("transmitted_records", 0) >= datagen_rows
        )
        return input_done and no_transaction and output_drained

    wait_for_condition(
        "datagen reaches end-of-input and Delta output drains",
        datagen_finished_and_output_drained,
        timeout_s=60.0,
        poll_interval_s=1.0,
    )
    wait_for_condition(
        "delta sink receives datagen rows after reset",
        lambda: len(read_delta_ids(table_location)) >= datagen_rows,
        timeout_s=30.0,
        poll_interval_s=1.0,
    )

    assert_incrementing_ids(
        view_ids(pipeline_name, view_name),
        datagen_rows,
        "materialized view before restart",
    )
    assert_incrementing_ids(
        read_delta_ids(table_location),
        datagen_rows,
        "Delta table before restart",
    )

    pipeline.stop(force=False, timeout_s=90.0)

    sql_without_input = build_bigint_datagen_delta_sql(
        table_location,
        include_datagen=False,
        datagen_rows=datagen_rows,
        output_threads=output_threads,
        use_index=use_index,
        send_snapshot=True,
    )
    expected_program_version = pipeline.program_version() + 1
    pipeline.modify(sql=sql_without_input)
    wait_for_program_success(pipeline_name, expected_program_version)

    pipeline.start(
        bootstrap_policy=BootstrapPolicy.ALLOW,
        timeout_s=150,
    )

    def output_checkpoint_metrics_restored() -> bool:
        metrics = output_connector_metrics(pipeline_name, view_name, connector_name)
        return (
            metrics.get("transmitted_records", 0) >= datagen_rows
            and metrics.get("queued_records", 0) == 0
            and metrics.get("buffered_records", 0) == 0
        )

    # The checkpointed snapshot_sent bit is not exposed via output connector
    # stats. The observable contract is that flipping send_snapshot to true
    # causes the restarted connector to replay the checkpointed view state.
    wait_for_condition(
        "output connector checkpointed records are restored and flushed",
        output_checkpoint_metrics_restored,
        timeout_s=30.0,
        poll_interval_s=1.0,
    )
    wait_for_condition(
        "materialized view is restored from checkpoint",
        lambda: view_ids(pipeline_name, view_name) == expected_ids,
        timeout_s=30.0,
        poll_interval_s=1.0,
    )

    assert_incrementing_ids(
        view_ids(pipeline_name, view_name),
        datagen_rows,
        "materialized view after restart",
    )
    assert_incrementing_ids(
        read_delta_ids(table_location),
        datagen_rows,
        "Delta table after restart",
    )


@enterprise_only
@gen_pipeline_name
def test_delta_rcs_output_threads4_indexed(pipeline_name):
    run_delta_reset_checkpoint_snapshot_scenario(
        pipeline_name,
        output_threads=4,
        use_index=True,
        reset_count=5,
    )


@enterprise_only
@gen_pipeline_name
def test_delta_rcs_output_threads1(pipeline_name):
    run_delta_reset_checkpoint_snapshot_scenario(
        pipeline_name,
        output_threads=1,
        use_index=False,
    )
