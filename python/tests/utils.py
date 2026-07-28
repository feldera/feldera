import logging
import os
import pathlib
import shutil
import subprocess
import tempfile
import time
import uuid
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from typing import TYPE_CHECKING
from urllib.parse import urlparse

if TYPE_CHECKING:
    from feldera.output_handler import OutputHandler


MINIO_BUCKET = os.environ.get("CI_MINIO_BUCKET", "ci-tests")
MINIO_ENDPOINT = os.environ.get(
    "CI_MINIO_ENDPOINT", "http://minio.minio.svc.cluster.local:9000"
)
MINIO_REGION = os.environ.get("CI_MINIO_REGION", "us-east-1")
# rclone S3 provider name for checkpoint sync; GCS S3-interop rejects
# requests signed with MinIO provider quirks (403 on HeadObject).
MINIO_PROVIDER = os.environ.get("CI_MINIO_PROVIDER", "Minio")
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
    # True when ``stable_subpath`` was used at construction time. ``cleanup()``
    # honors this by leaving the directory in place so the next run reuses the
    # cached fixture instead of paying to rebuild it.
    stable: bool = False

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
        :param stable_subpath: When set, locates the table at a fixed,
            shared path (``_fixtures/<stable_subpath>``) that is *not*
            namespaced by the pipeline name or commit SHA, so a fixture
            built once is reused by every later run and every commit.
            When unset, a fresh random subpath under ``pipeline_name`` is
            used (the original behavior). Use a stable path only for
            fixtures whose contents are deterministic across runs.
        """

        if runs_in_ci():
            access_key = required_env("CI_K8S_MINIO_ACCESS_KEY_ID")
            secret_key = required_env("CI_K8S_MINIO_SECRET_ACCESS_KEY")
            if stable_subpath is not None:
                prefix = f"_fixtures/{stable_subpath}"
            else:
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
                    "mode": mode,
                    "aws_access_key_id": access_key,
                    "aws_secret_access_key": secret_key,
                    "aws_region": MINIO_REGION,
                    "aws_endpoint": minio_endpoint,
                    "aws_allow_http": str(parsed_endpoint.scheme == "http").lower(),
                },
                root_path=root_path,
                stable=stable_subpath is not None,
            )

        if stable_subpath is not None:
            local_dir = pathlib.Path("/tmp/feldera_fixtures") / stable_subpath
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
            stable=stable_subpath is not None,
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

    def delta_log_exists(self) -> bool:
        """Return True when a Delta log is present at this location.

        Used to decide whether a cached fixture (see ``stable_subpath``) can
        be reused instead of rebuilt.
        """
        try:
            return len(self.log_json_paths()) > 0
        except FileNotFoundError:
            return False

    def _place_tree(self, staging: pathlib.Path) -> None:
        """Copy a locally-built Delta table tree to where this location stores
        its data: the local directory, or the S3/MinIO bucket, depending on
        how this location was created.

        Some fixtures can only be produced on the local filesystem (e.g. a
        PySpark-written table). For a local target, any existing content at
        ``self.local_dir`` is deleted before the copy. S3/MinIO targets get
        the data files first and ``_delta_log`` last, so a reader observing
        the upload mid-flight never sees a log referencing a missing parquet.
        """
        if self.local_dir is not None:
            if self.local_dir.exists():
                shutil.rmtree(self.local_dir)
            shutil.copytree(staging, self.local_dir)
            return

        fs = self._s3_filesystem()
        files = [path for path in sorted(staging.rglob("*")) if path.is_file()]
        for path in sorted(files, key=lambda p: ("_delta_log" in p.parts, p.name)):
            rel = path.relative_to(staging).as_posix()
            with fs.open_output_stream(f"{self.root_path}/{rel}") as out:
                out.write(path.read_bytes())

    def cleanup(self) -> None:
        """Remove the local temp directory, if any.

        No-op when ``stable_subpath`` was used at construction time:
        the whole point of a stable path is to cache contents across
        runs, so deleting it would defeat the cache. Also a no-op on
        the CI/MinIO path: there is no local directory to remove, so any
        objects this run wrote to the bucket are simply left in place.
        The shared MinIO bucket is long-lived and these tests never delete
        from it, so those objects accumulate; non-stable runs each use a
        unique uuid prefix so they never collide, and the leftover volume
        for this internal CI bucket is accepted rather than swept here.
        """
        if self.local_dir is not None and not self.stable:
            shutil.rmtree(self.local_dir, ignore_errors=True)
            self.local_dir = None


@dataclass
class IcebergTestLocation:
    """Where an Iceberg test table lives and how to read/write it.

    A test writes rows with ``pyiceberg`` (the ``iceberg-rust`` connector
    cannot write yet) and then points the connector at the table via
    ``metadata_location`` — the catalog-free access path. Both sides reach
    the same storage:

    * Local runs put the warehouse under ``/tmp`` and use bare filesystem
      paths (no ``file://`` scheme), matching the Rust FS harness and the
      fork's local ``FileIO`` resolver.
    * CI runs put the warehouse in the in-cluster MinIO bucket over S3, so
      the pipeline pod and the test runner reach the table through the same
      object store, exactly like :class:`DeltaTestLocation`.

    The ``pyiceberg`` SQL catalog metadata (a SQLite file) always lives on
    local disk; it is used only by the writer (the test runner), never by
    the connector.
    """

    warehouse: str
    table_location: str
    catalog_db: str
    namespace: str
    table_name: str
    fileio_config: dict[str, str]
    # Local directory removed on teardown. In local runs it is the whole
    # warehouse (data + SQLite catalog); in CI it is only the catalog dir (the
    # warehouse lives in S3/MinIO). `None` leaves nothing to remove.
    cleanup_dir: pathlib.Path | None = None

    NAMESPACE = "iceberg_test"
    TABLE = "test_table"

    @classmethod
    def create(cls, pipeline_name: str) -> "IcebergTestLocation":
        """Local filesystem for local runs, MinIO-backed S3 in CI."""
        if runs_in_ci():
            access_key = required_env("CI_K8S_MINIO_ACCESS_KEY_ID")
            secret_key = required_env("CI_K8S_MINIO_SECRET_ACCESS_KEY")
            prefix = f"{pipeline_name}/{uuid.uuid4().hex}"
            root = f"{MINIO_BUCKET}/{prefix}"
            endpoint = MINIO_ENDPOINT.rstrip("/")
            parsed = urlparse(endpoint)
            if parsed.scheme not in {"http", "https"} or not parsed.netloc:
                raise ValueError(
                    "CI_MINIO_ENDPOINT must be a full URL, e.g. "
                    "'http://minio.minio.svc.cluster.local:9000'"
                )
            fileio_config = {
                "s3.endpoint": endpoint,
                "s3.access-key-id": access_key,
                "s3.secret-access-key": secret_key,
                "s3.region": MINIO_REGION,
                "s3.path-style-access": "true",
            }
            # The SQLite catalog is only touched by the writer, so it stays
            # on local disk even when the warehouse is remote.
            catalog_dir = pathlib.Path(
                tempfile.mkdtemp(prefix=f"{pipeline_name}_iceberg_cat_", dir="/tmp")
            )
            return cls(
                warehouse=f"s3://{root}",
                table_location=f"s3://{root}/{cls.TABLE}",
                catalog_db=str(catalog_dir / "catalog.db"),
                namespace=cls.NAMESPACE,
                table_name=cls.TABLE,
                fileio_config=fileio_config,
                cleanup_dir=catalog_dir,
            )

        local_dir = pathlib.Path(
            tempfile.mkdtemp(prefix=f"{pipeline_name}_iceberg_", dir="/tmp")
        )
        # Bare (scheme-less) table path: the Rust FS harness reads tables
        # written this way, and the connector's local resolver expects it.
        return cls(
            warehouse=f"file://{local_dir}",
            table_location=f"{local_dir}/{cls.TABLE}",
            catalog_db=str(local_dir / "catalog.db"),
            namespace=cls.NAMESPACE,
            table_name=cls.TABLE,
            fileio_config={},
            cleanup_dir=local_dir,
        )

    @property
    def qualified_name(self) -> str:
        return f"{self.namespace}.{self.table_name}"

    def _catalog(self):
        """Build the ``pyiceberg`` SQL catalog. Deferred import keeps module
        collection cheap on hosts that never touch Iceberg."""
        from pyiceberg.catalog.sql import SqlCatalog

        return SqlCatalog(
            "test",
            **{
                "uri": f"sqlite:///{self.catalog_db}",
                "warehouse": self.warehouse,
                **self.fileio_config,
            },
        )

    def create_table(self, schema, partition_spec=None):
        """Create (replacing any prior) the test table and return it."""
        catalog = self._catalog()
        try:
            catalog.create_namespace(self.namespace)
        except Exception:
            pass  # Already exists.
        try:
            catalog.drop_table(self.qualified_name)
        except Exception:
            pass  # Nothing to drop.

        from pyiceberg.partitioning import UNPARTITIONED_PARTITION_SPEC

        return catalog.create_table(
            self.qualified_name,
            schema,
            location=self.table_location,
            partition_spec=partition_spec or UNPARTITIONED_PARTITION_SPEC,
        )

    def append(self, arrow_table) -> None:
        """Append one Arrow batch as a new Iceberg snapshot."""
        table = self._catalog().load_table(self.qualified_name)
        table.append(arrow_table)

    def metadata_location(self) -> str:
        """Return the current table metadata file location.

        This is the value handed to the connector's ``metadata_location``
        option, so a reload picks up the latest snapshot after appends.
        """
        return self._catalog().load_table(self.qualified_name).metadata_location

    def connector_config(self, **extra) -> dict[str, object]:
        """Transport config pointing the connector at this table.

        Merges ``metadata_location`` and any storage (``s3.*``) options with
        caller-supplied ``extra`` fields (e.g. ``mode``, ``timestamp_column``).
        """
        config: dict[str, object] = {"metadata_location": self.metadata_location()}
        config.update(self.fileio_config)
        config.update(extra)
        return config

    def row_count(self) -> int:
        table = self._catalog().load_table(self.qualified_name)
        return table.scan().to_arrow().num_rows

    def remove_if_local(self) -> None:
        """Remove the local temp directory, if any.

        A no-op on the S3 data itself (as with :class:`DeltaTestLocation`):
        objects this run wrote to the shared MinIO bucket use a unique
        prefix and are left in place. In CI only the local SQLite catalog dir
        is removed; in local runs the whole warehouse dir is removed.
        """
        if self.cleanup_dir is not None:
            shutil.rmtree(self.cleanup_dir, ignore_errors=True)
            self.cleanup_dir = None


def ensure_delta_spark_fixture(
    loc: DeltaTestLocation,
    builder_script: str | os.PathLike[str],
    builder_args: Sequence[object] = (),
    *,
    delta_spark_spec: str = "delta-spark>=4.2,<5",
    is_present: Callable[[DeltaTestLocation], bool] | None = None,
    max_attempts: int = 3,
) -> None:
    """Ensure a PySpark-authored Delta fixture exists at ``loc`` (cached).

    Some Delta features (deletion vectors, column-mapping schema evolution)
    can only be written by Delta Spark, not by delta-rs or the ``deltalake``
    wheel. This builds such a fixture once and reuses it:

    * If the fixture is already present (``is_present``, defaulting to
      :meth:`DeltaTestLocation.delta_log_exists`), do nothing — so a
      ``stable_subpath`` cache is reused across runs.
    * Otherwise run ``builder_script`` in an isolated environment
      (``uv run --no-project --with <delta_spark_spec> python <builder_script>
      <staging> *builder_args``), staging into a temp dir so a half-finished
      build can never leak into the upload, then place the tree onto ``loc``'s
      backend. ``--no-project`` keeps the builder hermetic: it depends only on
      ``delta_spark_spec``, never on building the enclosing project.

    The heavy PySpark + JVM stack is pulled only on this rare rebuild path.

    :param builder_script: Path to a standalone script that writes a Delta
        table to the directory given as its first argument.
    :param builder_args: Extra positional arguments passed after the staging
        directory. Each is stringified verbatim with ``str()`` — pass
        primitives; ``None`` would become the literal string ``"None"``.
    :param is_present: Predicate deciding whether the fixture already exists;
        also re-checked after upload to catch partial uploads.
    :param max_attempts: How many times to run the builder before giving up.
        Spark resolves the delta-spark dependency tree from Maven Central on
        this path; that download is prone to transient failures (0-byte
        artifacts, half-written Ivy cache files, gateway timeouts) that clear on
        a fresh attempt, so retry rather than fail the test on infra flakiness.
    """
    present = (
        is_present if is_present is not None else DeltaTestLocation.delta_log_exists
    )
    if present(loc):
        return

    if shutil.which("uv") is None:
        raise RuntimeError(
            "`uv` is required on PATH to build the PySpark Delta fixture "
            f"(builder runs via `uv run --with {delta_spark_spec}`)."
        )

    for attempt in range(1, max_attempts + 1):
        # Fresh staging each attempt: a builder that died mid-write must not
        # leak a partial tree into the next attempt or the upload.
        staging = pathlib.Path(tempfile.mkdtemp(prefix="feldera_delta_fixture_"))
        try:
            subprocess.run(
                [
                    "uv",
                    "run",
                    "--no-project",
                    "--with",
                    delta_spark_spec,
                    "python",
                    str(builder_script),
                    str(staging),
                    *(str(arg) for arg in builder_args),
                ],
                check=True,
            )
            loc._place_tree(staging)
            break
        except subprocess.CalledProcessError:
            if attempt == max_attempts:
                raise
            logging.warning(
                "PySpark Delta fixture build failed (attempt %d/%d); retrying. "
                "Usually a transient Maven/Ivy download failure.",
                attempt,
                max_attempts,
            )
            time.sleep(5 * attempt)
        finally:
            shutil.rmtree(staging, ignore_errors=True)

    if not present(loc):
        raise RuntimeError(
            f"Delta fixture at {loc.uri} is still absent after the builder "
            "ran and the tree was uploaded."
        )


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


def wait_for_records(
    handler: "OutputHandler",
    count: int,
    timeout_s: float | None = 60.0,
    poll_interval_s: float = 0.1,
) -> None:
    """Poll a listener until it has buffered at least ``count`` records.

    A listener receives its records over an HTTP stream that a background
    thread reads, so nothing the pipeline reports implies those records have
    reached this process. A completion token promises only that the chunk was
    handed to the output connector, and pipeline-side idleness says nothing at
    all about the client. Reading the handler does not wait either, so call
    this first.

    :param handler: The :class:`feldera.output_handler.OutputHandler` returned
        by ``Pipeline.listen``.
    :param count: Number of records to wait for. Must be positive; waiting for
        zero records would return without observing anything.
    :param timeout_s: Maximum wait time in seconds. ``None`` means wait forever.
    :param poll_interval_s: Poll interval in seconds.

    :raises TimeoutError: If fewer than ``count`` records arrive in time.
    """
    if count < 1:
        raise ValueError(f"record count must be positive, got {count}")

    wait_for_condition(
        f"{count} record(s) on the '{handler.view_name}' listener",
        lambda: len(handler.to_pandas(clear_buffer=False)) >= count,
        timeout_s,
        poll_interval_s,
    )
