import contextlib
import fcntl
import json
import logging
import os
import pathlib
import re
import shutil
import subprocess
import tempfile
import time
import uuid
from collections.abc import Callable, Iterator, Sequence
from dataclasses import dataclass
from typing import TYPE_CHECKING
from urllib.parse import urlparse

from feldera.testutils import suite_tag

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
LOCAL_FIXTURE_ROOT = pathlib.Path("/tmp/feldera_fixtures")


def fixture_suite_dir() -> str:
    """Bucket subdirectory holding one test suite's cached fixtures.

    CI runs `python` and `python-multihost` concurrently against one MinIO
    bucket. A fixture build is not byte-reproducible (fresh `col-<uuid>`
    physical names, fresh Parquet file names), so two suites building at one
    path overwrite each other's identically-named `_delta_log/*.json` and the
    surviving log describes columns its data files do not have. In CI that
    gives `suite-m` for `python-multihost` and `suite` for `python`.
    """
    tag = suite_tag()
    return f"suite-{tag}" if tag else "suite"


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

    # Written last by ``_place_tree``: a builder killed mid-upload leaves a
    # tree that already satisfies ``delta_log_exists``.
    READY_MARKER = "_fixture_ready"

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
        :param stable_subpath: When set, locates the table at a fixed path
            (``_fixtures/<suite>/<stable_subpath>``) that is *not* namespaced
            by the pipeline name or commit SHA, so every test needing that
            fixture shares one build of it. When unset, a fresh random subpath
            under ``pipeline_name`` is used (the original behavior). Use a
            stable path only for fixtures whose contents are deterministic
            across runs.

            The cache spans runs locally, but not in CI: MinIO and its volume
            are created fresh per integration-test run, so each suite rebuilds
            every fixture once per run. See :func:`fixture_suite_dir` for why
            suites must not share the path.
        """

        if runs_in_ci():
            access_key = required_env("CI_K8S_MINIO_ACCESS_KEY_ID")
            secret_key = required_env("CI_K8S_MINIO_SECRET_ACCESS_KEY")
            if stable_subpath is not None:
                prefix = f"_fixtures/{fixture_suite_dir()}/{stable_subpath}"
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
            # No suite split: one suite per developer machine.
            local_dir = LOCAL_FIXTURE_ROOT / stable_subpath
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

    def writer_storage_options(self) -> dict[str, str] | None:
        """`storage_options` for `deltalake.write_deltalake`, or None for local.

        S3 writes need the unsafe-rename opt-in: deltalake's default lock
        provider is DynamoDB, which the test environment does not run.
        """
        opts = self.delta_storage_options()
        if not opts:
            return None
        if self.uri.startswith("s3://"):
            opts.setdefault("aws_s3_allow_unsafe_rename", "true")
        return opts

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
        import pyarrow as pa

        active: dict[str, None] = {}
        for log_path in self.log_json_paths():
            for line in self._read_text(log_path).splitlines():
                action = json.loads(line)
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

    def live_row_count(self) -> int:
        """Rows a reader sees, counting deletion vectors.

        Replays ``_delta_log/*.json`` to find the current ``add`` per path, then
        sums each file's ``numRecords`` less the cardinality of its deletion
        vector. That is the arithmetic the Delta protocol defines for a live row
        count, and it is what :meth:`row_count` gets wrong on a table written by
        the merge-mode output connector, which supersedes rows with vectors
        rather than by rewriting files.

        Reads the log rather than the data because no Python Delta reader
        available here applies deletion vectors: the pinned ``deltalake`` wheel
        refuses a table that advertises the ``deletionVectors`` reader feature
        outright. Row-level cross-engine checks live in the Rust tests, which
        drive Delta Spark.
        """
        # Keyed by path: within one commit the connector emits the `remove` and
        # then the `add` for a file whose vector changed, so last write wins and
        # leaves the current `add`.
        active: dict[str, dict] = {}
        for log_path in self.log_json_paths():
            for line in self._read_text(log_path).splitlines():
                action = json.loads(line)
                if (add := action.get("add")) is not None:
                    active[add["path"]] = add
                elif (remove := action.get("remove")) is not None:
                    active.pop(remove["path"], None)

        total = 0
        for add in active.values():
            stats = add.get("stats")
            if stats is None:
                raise AssertionError(
                    f"data file {add['path']!r} has no statistics, so its rows "
                    "cannot be counted from the log"
                )
            rows = json.loads(stats)["numRecords"]
            vector = add.get("deletionVector")
            total += rows - (vector["cardinality"] if vector else 0)
        return total

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

    def fixture_is_complete(self) -> bool:
        """Return True when a fully placed fixture tree is present here.

        A half-placed tree lacks the ``READY_MARKER`` and so reads as absent.
        """
        if self.local_dir is not None:
            return (self.local_dir / self.READY_MARKER).is_file()

        import pyarrow.fs as pafs

        info = self._s3_filesystem().get_file_info(
            f"{self.root_path}/{self.READY_MARKER}"
        )
        return info.type == pafs.FileType.File

    def _place_tree(self, staging: pathlib.Path) -> None:
        """Copy a locally-built Delta table tree to where this location stores
        its data: the local directory, or the S3/MinIO bucket, depending on
        how this location was created.

        Some fixtures can only be produced on the local filesystem (e.g. a
        PySpark-written table). For a local target, any existing content at
        ``self.local_dir`` is deleted before the copy. S3/MinIO targets get
        the data files first and ``_delta_log`` last, so a reader observing
        the upload mid-flight never sees a log referencing a missing parquet.
        The ``READY_MARKER`` is removed before the first write and written
        last of all, so a reader that finds it knows the whole tree landed.
        """
        if self.local_dir is not None:
            if self.local_dir.exists():
                shutil.rmtree(self.local_dir)
            # `dirs_exist_ok`: an unlocked `create()` elsewhere may have
            # recreated the directory between the rmtree and here.
            shutil.copytree(staging, self.local_dir, dirs_exist_ok=True)
            (self.local_dir / self.READY_MARKER).write_text("", encoding="utf-8")
            return

        fs = self._s3_filesystem()
        # Marker off before the first byte lands, on after the last, so a
        # reader that finds it never sees a half-replaced tree.
        with contextlib.suppress(FileNotFoundError):
            fs.delete_file(f"{self.root_path}/{self.READY_MARKER}")
        files = [path for path in sorted(staging.rglob("*")) if path.is_file()]
        for path in sorted(files, key=lambda p: ("_delta_log" in p.parts, p.name)):
            rel = path.relative_to(staging).as_posix()
            with fs.open_output_stream(f"{self.root_path}/{rel}") as out:
                out.write(path.read_bytes())
        with fs.open_output_stream(f"{self.root_path}/{self.READY_MARKER}") as out:
            out.write(b"")

    def fetch_tree(self, dest: pathlib.Path) -> pathlib.Path:
        """Copy this location's Delta table to a local directory and return it.

        The inverse of :meth:`_place_tree`, for a tool that speaks only the
        local filesystem. Spark reading S3 would need an S3A stack on top of the
        Delta JARs, and the bytes are the same either way.
        """
        dest.mkdir(parents=True, exist_ok=True)
        if self.local_dir is not None:
            shutil.copytree(self.local_dir, dest, dirs_exist_ok=True)
            return dest

        import pyarrow.fs as pafs

        fs = self._s3_filesystem()
        infos = fs.get_file_info(pafs.FileSelector(self.root_path, recursive=True))
        for info in infos:
            if info.type != pafs.FileType.File:
                continue
            target = dest / pathlib.PurePosixPath(info.path).relative_to(self.root_path)
            target.parent.mkdir(parents=True, exist_ok=True)
            with fs.open_input_file(info.path) as handle:
                target.write_bytes(handle.readall())
        return dest

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


@contextlib.contextmanager
def _fixture_build_lock(key: str) -> Iterator[None]:
    """Serialize the check-then-build of the fixture at ``key`` across processes.

    :func:`fixture_suite_dir` keeps the other suite off this path, so every
    contender is an xdist worker on this host and a local ``flock`` suffices
    even for a remote fixture. The kernel drops it if a builder dies.
    """
    lock_dir = LOCAL_FIXTURE_ROOT / ".locks"
    lock_dir.mkdir(parents=True, exist_ok=True)
    lock_path = lock_dir / f"{re.sub(r'[^A-Za-z0-9._-]', '_', key)}.lock"
    with open(lock_path, "w") as handle:
        fcntl.flock(handle, fcntl.LOCK_EX)
        try:
            yield
        finally:
            fcntl.flock(handle, fcntl.LOCK_UN)


def run_delta_spark(
    script: str | os.PathLike[str],
    args: Sequence[object] = (),
    *,
    delta_spark_spec: str = "delta-spark>=4.2,<5",
    max_attempts: int = 3,
    capture_output: bool = True,
) -> str:
    """Run a standalone Delta Spark script and return its stdout.

    Runs under ``uv run --no-project --with <delta_spark_spec>``, so the PySpark
    and JVM stack is pulled only when a test needs it. Retries because Spark
    resolves delta-spark from Maven Central here, which fails transiently.

    :param args: Positional arguments after the script path, each stringified.
    :param capture_output: ``False`` lets output through to the test log.
    """
    if shutil.which("uv") is None:
        raise RuntimeError(
            "`uv` is required on PATH to run a Delta Spark script "
            f"(runs via `uv run --with {delta_spark_spec}`)."
        )

    command = [
        "uv",
        "run",
        "--no-project",
        "--with",
        delta_spark_spec,
        "python",
        str(script),
        *(str(arg) for arg in args),
    ]
    for attempt in range(1, max_attempts + 1):
        try:
            completed = subprocess.run(
                command, check=True, capture_output=capture_output, text=True
            )
            return completed.stdout if capture_output else ""
        except subprocess.CalledProcessError as e:
            if attempt == max_attempts:
                raise
            logging.warning(
                "Delta Spark script %s failed (attempt %d/%d); retrying. "
                "Usually a transient Maven/Ivy download failure.\n%s",
                script,
                attempt,
                max_attempts,
                e.stderr,
            )
            time.sleep(5 * attempt)
    raise AssertionError("unreachable")


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

    * If the fixture is already present (the completion marker is there and
      ``is_present``, defaulting to :meth:`DeltaTestLocation.delta_log_exists`,
      agrees), do nothing, so every test needing it shares one build.
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
    :param is_present: Predicate deciding whether the fixture already holds the
        content this caller wants; also re-checked after upload. It is ANDed
        with :meth:`DeltaTestLocation.fixture_is_complete`, which is what rules
        out a partially placed tree.
    :param max_attempts: How many times to run the builder before giving up.
        Spark resolves the delta-spark dependency tree from Maven Central on
        this path; that download is prone to transient failures (0-byte
        artifacts, half-written Ivy cache files, gateway timeouts) that clear on
        a fresh attempt, so retry rather than fail the test on infra flakiness.
    """
    matches = (
        is_present if is_present is not None else DeltaTestLocation.delta_log_exists
    )

    def present(location: DeltaTestLocation) -> bool:
        return location.fixture_is_complete() and matches(location)

    if present(loc):
        return

    if shutil.which("uv") is None:
        raise RuntimeError(
            "`uv` is required on PATH to build the PySpark Delta fixture "
            f"(builder runs via `uv run --with {delta_spark_spec}`)."
        )

    with _fixture_build_lock(loc.root_path):
        _build_delta_spark_fixture(
            loc,
            builder_script,
            builder_args,
            delta_spark_spec=delta_spark_spec,
            present=present,
            max_attempts=max_attempts,
        )


def _build_delta_spark_fixture(
    loc: DeltaTestLocation,
    builder_script: str | os.PathLike[str],
    builder_args: Sequence[object],
    *,
    delta_spark_spec: str,
    present: Callable[[DeltaTestLocation], bool],
    max_attempts: int,
) -> None:
    """Build and place the fixture at ``loc``. Call under the build lock."""
    # Whoever held the lock before us may have built it already.
    if present(loc):
        return

    for attempt in range(1, max_attempts + 1):
        # Fresh staging each attempt: a builder that died mid-write must not
        # leak a partial tree into the next attempt or the upload.
        staging = pathlib.Path(tempfile.mkdtemp(prefix="feldera_delta_fixture_"))
        try:
            run_delta_spark(
                builder_script,
                [staging, *builder_args],
                delta_spark_spec=delta_spark_spec,
                max_attempts=1,
                capture_output=False,
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
