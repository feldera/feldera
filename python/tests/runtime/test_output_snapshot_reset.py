import json
import pathlib
import tempfile
import uuid
from dataclasses import dataclass

import pyarrow as pa
import pyarrow.fs as pafs
import pyarrow.parquet as pq

from tests import (
    MINIO_BUCKET,
    MINIO_ENDPOINT,
    MINIO_REGION,
    TEST_CLIENT,
    required_env,
    runs_in_ci,
)
from tests.platform.helper import (
    create_pipeline,
    gen_pipeline_name,
    start_pipeline,
    wait_for_condition,
)


@dataclass
class DeltaTestLocation:
    """Describe where the Delta sink writes test data and how to read it back."""

    uri: str
    connector_config: dict[str, str]
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

            return cls(
                uri=f"s3://{root_path}",
                connector_config={
                    "uri": f"s3://{root_path}",
                    "mode": "truncate",
                    "aws_access_key_id": access_key,
                    "aws_secret_access_key": secret_key,
                    "aws_region": MINIO_REGION,
                    "aws_endpoint": f"http://{MINIO_ENDPOINT}",
                    "aws_allow_http": "true",
                },
                root_path=root_path,
                filesystem=pafs.S3FileSystem(
                    access_key=access_key,
                    secret_key=secret_key,
                    region=MINIO_REGION,
                    scheme="http",
                    endpoint_override=MINIO_ENDPOINT,
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


@gen_pipeline_name
def test_snapshot_and_follow_egress_mode(pipeline_name):
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
        mode="snapshot_and_follow",
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
def test_reset_output_connector_replays_snapshot(pipeline_name):
    table_location = DeltaTestLocation.create(pipeline_name)
    connector = {
        "name": "delta_out",
        "mode": "snapshot_and_follow",
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

    TEST_CLIENT.reset_output_connector(pipeline_name, "v1", "delta_out")

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
