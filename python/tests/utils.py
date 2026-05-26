import logging
import os
import pathlib
import shutil
import tempfile
import time
import uuid
from dataclasses import dataclass
from urllib.parse import urlparse


MINIO_BUCKET = os.environ.get("CI_MINIO_BUCKET", "ci-tests")
MINIO_ENDPOINT = os.environ.get(
    "CI_MINIO_ENDPOINT", "http://minio.minio.svc.cluster.local:9000"
)
MINIO_REGION = os.environ.get("CI_MINIO_REGION", "us-east-1")
KAFKA_BOOTSTRAP = os.environ.get(
    "KAFKA_BOOTSTRAP_SERVERS", "ci-kafka-bootstrap.kafka:9092"
)


def env_truthy(name: str) -> bool:
    """Return True when an environment variable is set to a truthy value."""
    value = os.environ.get(name)
    return value is not None and value.lower() not in {"", "0", "false", "no"}


def required_env(name: str) -> str:
    """Return a required environment variable or raise a descriptive error."""
    value = os.environ.get(name)
    if value:
        return value
    raise RuntimeError(f"required environment variable '{name}' is not set")


def runs_in_ci() -> bool:
    """Return True when the test suite is running under CI."""
    return env_truthy("CI")


@dataclass
class DeltaTestLocation:
    """Describe where the Delta sink writes test data and how to read it back."""

    uri: str
    connector_config: dict[str, object]
    root_path: str
    local_dir: pathlib.Path | None = None

    @classmethod
    def create(
        cls,
        pipeline_name: str,
        *,
        mode: str = "truncate",
        stable_subpath: str | None = None,
    ) -> "DeltaTestLocation":
        """Use the local filesystem for local runs and MinIO-backed S3 in CI.

        :param mode: Value of the connector's ``mode`` field. Output
            connectors use ``"truncate"`` (the default); input connectors
            should pass ``"snapshot"``.
        :param stable_subpath: When set, locates the table at a fixed
            path so cached data persists across test runs. When unset,
            a fresh random subpath is used (the original behavior). Use
            a stable path only when the table contents are deterministic
            and idempotent across runs.
        """

        if runs_in_ci():
            access_key = required_env("CI_K8S_MINIO_ACCESS_KEY_ID")
            secret_key = required_env("CI_K8S_MINIO_SECRET_ACCESS_KEY")
            tail = stable_subpath if stable_subpath is not None else uuid.uuid4().hex
            prefix = f"{pipeline_name}/{tail}"
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
                    "mode": mode,
                    "aws_access_key_id": access_key,
                    "aws_secret_access_key": secret_key,
                    "aws_region": MINIO_REGION,
                    "aws_endpoint": minio_endpoint,
                    "aws_allow_http": str(parsed_endpoint.scheme == "http").lower(),
                },
                root_path=root_path,
            )

        if stable_subpath is not None:
            local_dir = pathlib.Path("/tmp") / f"{pipeline_name}_{stable_subpath}"
            local_dir.mkdir(parents=True, exist_ok=True)
        else:
            local_dir = pathlib.Path(
                tempfile.mkdtemp(prefix=f"{pipeline_name}_delta_", dir="/tmp")
            )
        return cls(
            uri=f"file://{local_dir}",
            connector_config={
                "uri": f"file://{local_dir}",
                "mode": mode,
            },
            root_path=str(local_dir),
            local_dir=local_dir,
        )

    def delta_storage_options(self) -> dict[str, str]:
        """Return `deltalake` storage_options derived from the connector config."""
        return {
            k: str(v)
            for k, v in self.connector_config.items()
            if k not in ("uri", "mode")
        }

    def _s3_filesystem(self):
        """Build a pyarrow ``S3FileSystem`` from the connector config.

        Pyarrow imports are deferred to keep module-level test collection
        cheap on hosts that don't read Delta tables.
        """
        import pyarrow.fs as pafs

        cfg = self.connector_config
        endpoint = str(cfg["aws_endpoint"]).rstrip("/")
        parsed_endpoint = urlparse(endpoint)
        return pafs.S3FileSystem(
            access_key=str(cfg["aws_access_key_id"]),
            secret_key=str(cfg["aws_secret_access_key"]),
            region=str(cfg["aws_region"]),
            scheme=parsed_endpoint.scheme,
            endpoint_override=parsed_endpoint.netloc,
        )

    def log_json_paths(self) -> list[str]:
        """List Delta transaction log JSON files in version order."""
        if self.local_dir is not None:
            return [
                str(path)
                for path in sorted((self.local_dir / "_delta_log").glob("*.json"))
            ]

        import pyarrow.fs as pafs

        fs = self._s3_filesystem()
        infos = fs.get_file_info(
            pafs.FileSelector(f"{self.root_path}/_delta_log", recursive=False)
        )
        return sorted(
            info.path
            for info in infos
            if info.type == pafs.FileType.File and info.path.endswith(".json")
        )

    def _read_text(self, path: str) -> str:
        """Read a text file from the configured backend."""
        if self.local_dir is not None:
            return pathlib.Path(path).read_text(encoding="utf-8")

        with self._s3_filesystem().open_input_file(path) as handle:
            return handle.readall().decode("utf-8")

    def _read_parquet(self, relative_path: str):
        """Read a Delta data file (relative to ``root_path``) as a pyarrow.Table."""
        import pyarrow.parquet as pq

        if self.local_dir is not None:
            return pq.read_table(self.local_dir / relative_path)

        return pq.read_table(
            f"{self.root_path}/{relative_path}", filesystem=self._s3_filesystem()
        )

    def read_rows(self) -> list[dict]:
        """Read the active rows of the Delta table by replaying its log.

        Walks ``_delta_log/*.json``, follows ``add``/``remove`` actions to
        derive the current set of parquet files, reads them with pyarrow
        and drops Feldera-internal ``__feldera_*`` columns. Works against
        both local-filesystem and S3-backed (MinIO) tables; avoids the
        ``deltalake`` Python package, whose wheel aborts on aarch64 hosts.
        """
        import json as _json

        import pyarrow as pa

        active: dict[str, None] = {}
        for log_path in self.log_json_paths():
            for line in self._read_text(log_path).splitlines():
                action = _json.loads(line)
                if (add := action.get("add")) is not None:
                    active[add["path"]] = None
                if (remove := action.get("remove")) is not None:
                    active.pop(remove["path"], None)

        if not active:
            return []

        tables = [self._read_parquet(rel) for rel in sorted(active)]
        return [
            {
                key: value
                for key, value in row.items()
                if not key.startswith("__feldera_")
            }
            for row in pa.concat_tables(tables).to_pylist()
        ]

    def row_count(self, missing_ok: bool = False) -> int:
        """Return the row count of the current Delta snapshot.

        Uses the per-file `numRecords` stats recorded in the delta log, so
        we never need to scan parquet (and never need pyarrow). The
        `deltalake` import is deferred to keep module-level test collection
        cheap on hosts that don't read delta tables.

        :param missing_ok: When True, return ``-1`` if the table does not
            exist instead of raising. Useful when probing a cache.
        """
        from deltalake import DeltaTable
        from deltalake.exceptions import TableNotFoundError

        try:
            dt = DeltaTable(self.uri, storage_options=self.delta_storage_options())
        except TableNotFoundError:
            if missing_ok:
                return -1
            raise
        return dt.count()

    def cleanup(self) -> None:
        """Remove the local temp directory, if any.

        No-op on the CI/MinIO path: the bucket is ephemeral and the uuid
        prefix prevents collisions, so leaked keys are acceptable.
        """
        if self.local_dir is not None:
            shutil.rmtree(self.local_dir, ignore_errors=True)
            self.local_dir = None


def wait_for_condition(
    description: str,
    predicate_func,
    timeout_s: float | None,
    poll_interval_s: float,
) -> None:
    """Poll ``predicate_func`` until it returns truthy or the timeout elapses.

    :param description: Human-readable description used in timeout/errors.
    :param predicate_func: Callable returning ``True`` when condition is met.
    :param timeout_s: Maximum wait time in seconds. ``None`` means wait forever.
    :param poll_interval_s: Poll interval in seconds.

    :raises TimeoutError: If the condition is not met within ``timeout_s``.
    """
    if timeout_s is not None and poll_interval_s > timeout_s:
        raise ValueError(
            f"poll interval ({poll_interval_s}s) cannot be larger than"
            f" timeout ({timeout_s}s)"
        )

    timestamp_start_s = time.monotonic()
    timestamp_deadline_s = (
        timestamp_start_s + timeout_s if timeout_s is not None else float("inf")
    )
    attempt = 0
    while True:
        if time.monotonic() > timestamp_deadline_s:
            raise TimeoutError(
                f"timeout ({timeout_s:.1f}s) waiting for condition '{description}'"
            )
        attempt += 1
        if predicate_func():
            logging.debug(
                f"condition '{description}' met after"
                f" {time.monotonic() - timestamp_start_s:.1f}s"
            )
            return
        time.sleep(poll_interval_s)
