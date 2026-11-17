"""Harness for the end-to-end tests of checkpoint sync.

The tests in ``test_checkpoint_sync_e2e.py`` drive real pipelines through
pushes, garbage collection, restores and failures. This module gives them
three oracles, so that a test states a scenario and the harness checks it:

* :class:`Ledger` models the input of the test program. After every round of
  input and after every restore, the views of the pipeline must equal the
  views computed from the ledger.
* :class:`SyncBucket` reads the bucket through the object store's own API and
  audits it: every checkpoint in the bucket must be restorable, and no file
  may be left that no checkpoint refers to.
* :class:`SyncPipeline` runs the steps of a scenario, keeps the list of the
  checkpoints it pushed, and audits the bucket after every push.

The bucket layout that the audit knows is the layout the ``sync-checkpoint``
crate writes::

    owner.json                  the pipeline that may push to the bucket
    <uuid>.zip                  the metadata of a checkpoint (solo pipeline)
    checkpoints.feldera         the catalog: the newest checkpoint first
    host<N>/<uuid>.zip          the same two, for each host of a multihost
    host<N>/checkpoints.feldera pipeline
    dependencies/<uuid>.json    the batch files a checkpoint needs
    <name>.feldera              batch files, shared by checkpoints and hosts

The tests run in parallel, in the processes of pytest-xdist and next to other
test runs on the same platform and object store:

* Every bucket path carries the prefix of the run and the name of its test
  (:class:`Scenario`), so no two tests write to the same path.
* The bucket paths that many tests restore from are built once for the whole
  run, and tests only read them (:func:`golden_bucket`).
* A pipeline belongs to one process. When a scenario is done with it, the
  next scenario of the process takes it over (:class:`SyncPipeline`).

No step depends on how fast the platform or the object store is: every wait
ends on the condition that the scenario needs, and the limits of the waits
(``STATE_TIMEOUT_S`` and the others) only bound a failure. To check that, run
the tests with ``FELDERA_TEST_SYNC_FLAGS="--bwlimit 30k"``, which makes every
transfer slow.

Nor does a step depend on the platform that runs the pipelines. Platforms
differ in what a stop does with input that no checkpoint covers, so the
harness refuses to start a pipeline again after it was stopped with such
input (:meth:`SyncPipeline._expect_known_state`).
"""

from __future__ import annotations

import datetime
import fcntl
import hashlib
import hmac
import io
import itertools
import json
import logging
import os
import platform
import re
import sys
import time
import zipfile
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Iterable, Optional
from urllib.parse import quote, urlparse
from uuid import UUID

import pyarrow.fs as pafs
import requests

from feldera import Pipeline, PipelineBuilder
from feldera.enums import FaultToleranceModel, PipelineStatus
from feldera.runtime_config import RuntimeConfig, Storage
from feldera.testutils import (
    FELDERA_TEST_NUM_HOSTS,
    FELDERA_TEST_NUM_WORKERS,
    PIPELINE_NAME_MAX_LEN,
    reclaim_pipeline,
    unique_pipeline_name,
)
from tests import TEST_CLIENT
from tests.utils import ObjectStore, wait_for_condition

LOGGER = logging.getLogger(__name__)

# Set to a directory to measure which lines of the sync code the tests
# execute. Each pipeline then writes an LLVM coverage profile under
# `<directory>/<test>/`, provided that the platform compiles pipelines with
# `-C instrument-coverage`. The `%c` makes the pipeline write its counters
# continuously, because a forced stop gives it no chance to write them.
COVERAGE_DIR_ENV = "FELDERA_TEST_COVERAGE_DIR"

MULTIHOST = FELDERA_TEST_NUM_HOSTS > 1

# One program for every pipeline of the harness, so that a scenario can take
# over the pipeline of an earlier one, and any pipeline can restore a
# checkpoint of another.
#
# * `dim` is loaded once. With every batch spilled to storage (see
#   `SPILL_EVERYTHING`), later checkpoints share its batch files.
# * `fact` gains, changes and loses rows in every round, so every checkpoint
#   has batch files that the next one no longer needs.
# * `by_grp` keeps a join and an aggregate, whose state no table shows. A
#   restore that loses part of it gives wrong results only for later input.
# * `gen` has a connector with a position. A restore that loses the position
#   generates the rows again, and `gen` has no key to hide the duplicates.
SQL = """
CREATE TABLE dim (id INT NOT NULL PRIMARY KEY, grp INT NOT NULL)
WITH ('materialized' = 'true');

CREATE TABLE fact (
    id BIGINT NOT NULL PRIMARY KEY,
    dim_id INT NOT NULL,
    amount BIGINT NOT NULL,
    payload VARCHAR NOT NULL
) WITH ('materialized' = 'true');

CREATE TABLE gen (id BIGINT NOT NULL, v BIGINT NOT NULL)
WITH (
    'materialized' = 'true',
    'connectors' = '[{
        "name": "gen",
        "transport": {
            "name": "datagen",
            "config": {"plan": [{"limit": 5000}]}
        }
    }]'
);

CREATE MATERIALIZED VIEW by_grp AS
SELECT d.grp, COUNT(*) AS n, SUM(f.amount) AS total
FROM fact f JOIN dim d ON f.dim_id = d.id
GROUP BY d.grp;

CREATE MATERIALIZED VIEW gen_total AS SELECT COUNT(*) AS n, SUM(v) AS s FROM gen;
"""

# Storage settings that write every batch to a file as soon as it exists.
#
# With the default thresholds a small batch stays in memory and every
# checkpoint writes it to a new file, so two checkpoints of a small test share
# no batch file at all. Shared files are what the incremental upload and the
# garbage collection have to get right.
SPILL_EVERYTHING = {"min_spill_size": "0", "min_step_spill_size": "0"}

# rclone flags that make the sync fail in one chosen way. They go through
# `SyncConfig.flags`, so they need nothing but the pipeline's configuration.
#
# Every `rclone delete` removes one file and then fails.
FLAGS_ONE_DELETE = ["--max-delete", "1"]
# Every transfer and every `rclone cat` fails; listings and lookups succeed.
FLAGS_NO_TRANSFER = ["--max-transfer", "1B"]

# What a pipeline logs after every pull that it completes.
_PULLED = "checkpoint pulled successfully"
# A line number past the end of any log.
_PAST_THE_END = 2**63 - 1
# The log lines that say most about a failed test: warnings, errors, panics,
# and what the sync itself logged.
_NOTABLE = re.compile(r"\b(WARN|ERROR)\b|panic|rclone|sync", re.IGNORECASE)
# How many log lines of each kind the diagnostics of a failed test show, and
# how many characters of each line.
_LOG_EXCERPT_LINES = 40
_LOG_EXCERPT_WIDTH = 1000

# How long the harness waits for the effect of a step. A wait ends as soon as
# the effect shows, so a long limit costs time only in a test that fails. The
# limits allow for a loaded CI cluster, where a step that takes a second on a
# developer's machine can take a minute.
#
# The views of the pipeline equal the ledger.
STATE_TIMEOUT_S = 120.0
# The pipeline makes a checkpoint, or pushes one, by itself.
SYNC_TIMEOUT_S = 180.0
# A standby pipeline completes a pull, or becomes active.
STANDBY_TIMEOUT_S = 300.0
# The object store accepts a request of the harness.
STORE_TIMEOUT_S = 60.0

# A `checkpoint_interval` so long that the pipeline makes no checkpoint by
# itself during a test.
NO_AUTOMATIC_CHECKPOINTS = "3600s"

# Set to rclone flags, separated by spaces, to add them to every pipeline of
# the harness. `--bwlimit 30k` makes every transfer slow, which shows whether
# a test depends on the speed of the object store.
EXTRA_FLAGS_ENV = "FELDERA_TEST_SYNC_FLAGS"

# The longest prefix that `unique_pipeline_name` prepends in any suite: `run_`,
# five characters of the commit SHA, the one-character tag of the
# `python-multihost` suite, and the separator. The harness fits its names
# behind this prefix in every suite, so that a name cannot be short enough in
# one suite and too long in another.
_LONGEST_TAG_LEN = len("run_abcde") + len("m") + len("_")
_NAME_BUDGET = PIPELINE_NAME_MAX_LEN - _LONGEST_TAG_LEN
# The longest role of a pipeline or bucket path within a test, such as
# "intruder".
_MAX_ROLE_LEN = 9

# Pipelines that the scenarios of this process are done with: stopped, with
# empty storage. Every pipeline of the harness runs the same program, so the
# next scenario takes one of these instead of creating a pipeline, whose
# compilation takes longer than most scenarios.
_IDLE_PIPELINES: list[Pipeline] = []
_PIPELINES_CREATED = itertools.count()
# Tells the pipelines of the processes of one test run apart: pytest-xdist
# names each of its worker processes.
_PROCESS = os.environ.get("PYTEST_XDIST_WORKER", "main")

N_DIM = 500
N_GROUPS = 20
ROWS_PER_ROUND = 2000

OWNER_FILE = "owner.json"
CATALOG_FILE = "checkpoints.feldera"
DEPENDENCIES_DIR = "dependencies"
DEPENDENCIES_IN_ZIP = "dependencies.json"

_UUID = r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}"
_ZIP = re.compile(rf"^(?:(host\d+)/)?({_UUID})\.zip$")
_CATALOG = re.compile(rf"^(?:(host\d+)/)?{re.escape(CATALOG_FILE)}$")
_DEPENDENCIES = re.compile(rf"^{DEPENDENCIES_DIR}/({_UUID})\.json$")
_BATCH = re.compile(r"^[^/]+\.feldera$")


class Ledger:
    """The rows that the test program must hold, and its views.

    Every row is a function of the round that writes it, so two ledgers that
    ran the same number of rounds are equal, and a pipeline that was restored
    to round `r` can be fed round `r + 1` again.
    """

    def __init__(self) -> None:
        self.dim = {i: i % N_GROUPS for i in range(N_DIM)}
        self.fact: dict[int, dict] = {}
        self.round = 0

    def copy(self) -> "Ledger":
        """Returns an independent copy, for example the state at a checkpoint."""
        other = Ledger()
        other.fact = {k: dict(v) for k, v in self.fact.items()}
        other.round = self.round
        return other

    def dim_rows(self) -> list[dict]:
        return [{"id": i, "grp": g} for i, g in self.dim.items()]

    def next_round(self) -> list[dict]:
        """Advances the ledger by one round.

        A round inserts `ROWS_PER_ROUND` rows, changes the amount of 200
        earlier rows, and deletes 100 earlier rows.

        :return: the changes of the round, in the insert/delete JSON format.
        """
        self.round += 1
        r = self.round
        changes = []
        existing = sorted(self.fact)
        for i in range(ROWS_PER_ROUND):
            row_id = r * 100_000 + i
            row = {
                "id": row_id,
                "dim_id": (row_id * 7) % N_DIM,
                "amount": row_id % 1000,
                "payload": hashlib.sha256(str(row_id).encode()).hexdigest(),
            }
            self.fact[row_id] = row
            changes.append({"insert": row})
        for row_id in existing[r::10][:200]:
            row = dict(self.fact[row_id])
            row["amount"] += 1_000_000 * r
            self.fact[row_id] = row
            changes.append({"insert": row})
        for row_id in existing[(r + 5) :: 20][:100]:
            changes.append({"delete": self.fact.pop(row_id)})
        return changes

    def by_grp(self) -> dict[int, tuple[int, int]]:
        """Returns the `by_grp` view: group -> (row count, sum of amounts)."""
        out: dict[int, list[int]] = {}
        for row in self.fact.values():
            acc = out.setdefault(self.dim[row["dim_id"]], [0, 0])
            acc[0] += 1
            acc[1] += row["amount"]
        return {g: (n, total) for g, (n, total) in out.items()}


@dataclass(frozen=True)
class ObjectInfo:
    """Size and modification time of an object in the bucket."""

    size: int
    mtime: Any


@dataclass
class CheckpointFiles:
    """What the bucket holds of one checkpoint."""

    uuid: str
    # "" for a solo pipeline, "host<N>" for a host of a multihost pipeline.
    host: str
    # The batch files the checkpoint needs, read from the zip itself; None if
    # the zip cannot be read.
    batches: Optional[frozenset]
    # The contents of `dependencies/<uuid>.json`; None if there is no such file.
    dependencies_file: Optional[frozenset]
    # The needed batch files that are not in the bucket.
    missing: frozenset = frozenset()

    @property
    def restorable(self) -> bool:
        return self.batches is not None and not self.missing


@dataclass
class Audit:
    """A classification of every object in a bucket path.

    :ivar objects: every object, by its path relative to the bucket path.
    :ivar owner: the parsed `owner.json`, None if it is absent or not JSON.
    :ivar checkpoints: every checkpoint that has a zip, by UUID.
    :ivar catalogs: the entries of each catalog, by host ("" for solo).
    :ivar batch_files: the batch files in the bucket.
    :ivar orphan_batches: batch files that no checkpoint in the bucket needs.
    :ivar orphan_dependencies: UUIDs with a dependencies file and no zip.
    :ivar unknown: objects that are no part of the layout.
    """

    objects: dict
    owner: Optional[dict]
    checkpoints: dict
    catalogs: dict
    batch_files: set
    orphan_batches: set
    orphan_dependencies: set
    unknown: set

    def uuids(self) -> set:
        return set(self.checkpoints)

    def restorable(self) -> set:
        return {u for u, c in self.checkpoints.items() if c.restorable}

    def latest(self) -> set:
        """Returns the checkpoints that the catalogs name, which are the
        ones that a start from `latest` restores: one for a solo pipeline,
        one for each host of a multihost pipeline."""
        return {
            str(entries[0].get("uuid")) for entries in self.catalogs.values() if entries
        }

    def incomplete(self) -> dict:
        """Returns the checkpoints with missing batch files: UUID -> files."""
        return {u: c.missing for u, c in self.checkpoints.items() if c.missing}

    def unreadable(self) -> set:
        return {u for u, c in self.checkpoints.items() if c.batches is None}

    def batches_of(self, uuids: Iterable[str]) -> set:
        out: set = set()
        for uuid in uuids:
            out |= self.checkpoints[uuid].batches or frozenset()
        return out

    def problems(
        self,
        allow_incomplete: Iterable[str] = (),
        allow_unreadable: Iterable[str] = (),
        allow_orphan_dependencies: Iterable[str] = (),
        allow_orphan_batches: Iterable[str] = (),
    ) -> list[str]:
        """Lists every violation of the invariants of the bucket.

        A test that damages the bucket on purpose names what it expects to
        find through the `allow_*` arguments; everything else is a problem.

        :param allow_incomplete: checkpoints that may miss batch files.
        :param allow_unreadable: checkpoints whose zip may be unreadable.
        :param allow_orphan_dependencies: UUIDs whose dependencies file may
            be left without a zip.
        :param allow_orphan_batches: batch files that may belong to no
            readable checkpoint.
        :return: one message for each violation; empty if the bucket is
            consistent.
        """
        out = []
        for uuid, files in sorted(self.incomplete().items()):
            if uuid not in set(allow_incomplete):
                out.append(
                    f"checkpoint {uuid} cannot be restored: {len(files)} batch"
                    f" files are missing, for example {sorted(files)[:3]}"
                )
        for uuid in sorted(self.unreadable() - set(allow_unreadable)):
            out.append(f"the zip of checkpoint {uuid} cannot be read")
        if orphans := self.orphan_batches - set(allow_orphan_batches):
            out.append(
                f"{len(orphans)} batch files belong to no checkpoint"
                f" in the bucket, for example {sorted(orphans)[:3]}"
            )
        for uuid in sorted(self.orphan_dependencies - set(allow_orphan_dependencies)):
            out.append(f"dependencies/{uuid}.json has no checkpoint zip")
        for uuid, files in sorted(self.checkpoints.items()):
            if (
                files.batches is not None
                and files.dependencies_file is not None
                and files.dependencies_file != files.batches
            ):
                out.append(f"dependencies/{uuid}.json differs from the list in the zip")
        if self.unknown:
            out.append(f"unexpected objects: {sorted(self.unknown)[:5]}")
        return out

    def summary(self) -> str:
        return (
            f"{len(self.objects)} objects, {len(self.checkpoints)} checkpoints"
            f" ({len(self.restorable())} restorable), {len(self.batch_files)}"
            f" batch files ({len(self.orphan_batches)} orphans), owner"
            f" {(self.owner or {}).get('pipeline_name')}"
        )


def _arch() -> str:
    return platform.machine().lower()


class SyncBucket:
    """One bucket path that pipelines sync checkpoints to, as the object
    store itself shows it.

    The harness reads and changes the bucket itself, not through a pipeline,
    so what it sees does not depend on the code under test. It reads through
    `pyarrow`; see :func:`_s3_request` for how it writes.
    """

    def __init__(self, name: str) -> None:
        """
        :param name: a name that is unique to the test, normally the name of
            the pipeline that pushes to the bucket path.
        """
        self.name = name
        self.store = ObjectStore.from_env()
        self.fs = self.store.pyarrow_fs()
        # `<bucket>/<key prefix>`, which is how pyarrow addresses the store.
        self.path = f"{self.store.bucket}/{_arch()}/{name}"
        self._zip_cache: dict = {}

    @property
    def sync_bucket(self) -> str:
        """The `bucket` (or `read_bucket`) value of a sync configuration."""
        return self.store.sync_bucket(self.path)

    def objects(self) -> dict:
        """Returns every object under the path: relative path -> ObjectInfo."""
        selector = pafs.FileSelector(self.path, recursive=True, allow_not_found=True)
        prefix = len(self.path) + 1
        return {
            info.path[prefix:]: ObjectInfo(info.size, info.mtime)
            for info in self.fs.get_file_info(selector)
            if info.is_file
        }

    def read(self, rel: str) -> bytes:
        with self.fs.open_input_file(f"{self.path}/{rel}") as f:
            return f.read()

    def write(self, rel: str, data: bytes) -> None:
        path = f"{self.path}/{rel}"
        if self.store.scheme == "s3":
            _s3_request(self.store, "PUT", path, data)
        else:
            with self.fs.open_output_stream(path) as f:
                f.write(data)

    def delete(self, *rels: str) -> None:
        """Removes objects."""
        for rel in rels:
            path = f"{self.path}/{rel}"
            if self.store.scheme == "s3":
                _s3_request(self.store, "DELETE", path)
            else:
                self.fs.delete_file(path)

    def wipe(self) -> None:
        """Removes every object under the path."""
        self.delete(*self.objects())

    def clone_to(self, other: "SyncBucket") -> None:
        """Copies every object of this path to `other`, which must be empty."""
        assert not other.objects(), f"{other.path} is not empty"
        for rel in self.objects():
            other.write(rel, self.read(rel))

    def _zip_batches(self, rel: str, info: ObjectInfo) -> Optional[frozenset]:
        """Returns the batch files named inside a checkpoint zip, or None if
        the zip cannot be read."""
        key = (rel, info.size, info.mtime)
        if key not in self._zip_cache:
            try:
                archive = zipfile.ZipFile(io.BytesIO(self.read(rel)))
                self._zip_cache[key] = _batches(archive.read(DEPENDENCIES_IN_ZIP))
            except (zipfile.BadZipFile, KeyError, ValueError):
                self._zip_cache[key] = None
        return self._zip_cache[key]

    def audit(self) -> Audit:
        """Reads the bucket path and classifies every object in it."""
        objects = self.objects()
        owner = None
        zips: dict = {}
        catalogs: dict = {}
        dependencies: dict = {}
        batch_files: set = set()
        unknown: set = set()
        for rel in objects:
            if rel == OWNER_FILE:
                try:
                    owner = json.loads(self.read(rel))
                except ValueError:
                    owner = None
            elif match := _ZIP.match(rel):
                zips[match.group(2)] = (match.group(1) or "", rel)
            elif match := _CATALOG.match(rel):
                try:
                    catalogs[match.group(1) or ""] = json.loads(self.read(rel))
                except ValueError:
                    unknown.add(rel)
            elif match := _DEPENDENCIES.match(rel):
                try:
                    dependencies[match.group(1)] = _batches(self.read(rel))
                except ValueError:
                    dependencies[match.group(1)] = frozenset({"<unreadable>"})
            elif _BATCH.match(rel):
                batch_files.add(rel)
            else:
                unknown.add(rel)

        checkpoints = {}
        needed: set = set()
        for uuid, (host, rel) in zips.items():
            batches = self._zip_batches(rel, objects[rel])
            # A zip that cannot be read still names its files through the
            # dependencies file, if it has one.
            known = batches if batches is not None else dependencies.get(uuid)
            needed |= known or frozenset()
            checkpoints[uuid] = CheckpointFiles(
                uuid=uuid,
                host=host,
                batches=batches,
                dependencies_file=dependencies.get(uuid),
                missing=frozenset((batches or frozenset()) - batch_files),
            )
        return Audit(
            objects=objects,
            owner=owner,
            checkpoints=checkpoints,
            catalogs=catalogs,
            batch_files=batch_files,
            orphan_batches=batch_files - needed,
            orphan_dependencies=set(dependencies) - set(zips),
            unknown=unknown,
        )


def _s3_request(store: ObjectStore, method: str, path: str, body: bytes = b"") -> None:
    """Writes or deletes one object of an S3-compatible store with a signed
    request (AWS Signature Version 4, path-style addressing).

    The harness changes the bucket with plain requests because the `pyarrow`
    S3 client does more than it is asked: after a delete it writes an empty
    object named after the parent directory, and it uploads even a small
    object in parts, which not every store accepts.

    Both requests give the same result however often they are sent, so a
    request that the store did not answer, or answered with a server error,
    is sent again until `STORE_TIMEOUT_S` is over.

    :param store: the store, with its endpoint, region and static keys.
    :param method: `PUT` or `DELETE`.
    :param path: `<bucket>/<key>` of the object.
    :param body: the contents to write, for `PUT`.
    """
    deadline = time.monotonic() + STORE_TIMEOUT_S
    while True:
        try:
            response = _s3_send(store, method, path, body)
            outcome = f"{response.status_code} {response.text[:300]}"
            if response.status_code in (200, 204):
                return
            transient = response.status_code in (429, 500, 502, 503, 504)
        except (requests.ConnectionError, requests.Timeout) as error:
            outcome = f"{type(error).__name__}: {error}"
            transient = True
        assert transient and time.monotonic() < deadline, f"{method} {path}: {outcome}"
        LOGGER.warning("%s %s: %s; sending the request again", method, path, outcome)
        time.sleep(1.0)


def _s3_send(
    store: ObjectStore, method: str, path: str, body: bytes
) -> requests.Response:
    """Signs and sends one request for :func:`_s3_request`.

    :return: the response of the store, whatever its status.
    """
    endpoint = urlparse(store.endpoint)
    uri = "/" + quote(path, safe="/-_.~")
    now = datetime.datetime.now(datetime.timezone.utc)
    amz_date = now.strftime("%Y%m%dT%H%M%SZ")
    date = now.strftime("%Y%m%d")
    payload_hash = hashlib.sha256(body).hexdigest()
    headers = {
        "host": endpoint.netloc,
        "x-amz-content-sha256": payload_hash,
        "x-amz-date": amz_date,
    }
    signed_headers = ";".join(sorted(headers))
    canonical_request = "\n".join(
        [
            method,
            uri,
            "",
            "".join(f"{name}:{headers[name]}\n" for name in sorted(headers)),
            signed_headers,
            payload_hash,
        ]
    )
    scope = f"{date}/{store.region}/s3/aws4_request"
    string_to_sign = "\n".join(
        [
            "AWS4-HMAC-SHA256",
            amz_date,
            scope,
            hashlib.sha256(canonical_request.encode()).hexdigest(),
        ]
    )

    def sign(key: bytes, message: str) -> bytes:
        return hmac.new(key, message.encode(), hashlib.sha256).digest()

    key = f"AWS4{store.secret_key}".encode()
    for part in (date, store.region, "s3", "aws4_request"):
        key = sign(key, part)
    signature = hmac.new(key, string_to_sign.encode(), hashlib.sha256).hexdigest()
    headers["authorization"] = (
        f"AWS4-HMAC-SHA256 Credential={store.access_key}/{scope},"
        f" SignedHeaders={signed_headers}, Signature={signature}"
    )
    return requests.request(
        method, f"{store.endpoint}{uri}", headers=headers, data=body, timeout=30
    )


def _batches(dependencies_json: bytes) -> frozenset:
    """Parses a dependencies file: the current object form or the legacy list."""
    parsed = json.loads(dependencies_json)
    return frozenset(parsed["batches"] if isinstance(parsed, dict) else parsed)


def uuid_time(uuid: str) -> float:
    """Returns the creation time of a version 7 UUID, in Unix seconds."""
    return (UUID(uuid).int >> 80) / 1000.0


@dataclass
class Checkpoint:
    """A checkpoint that a :class:`SyncPipeline` made."""

    # The UUID that names the checkpoint in the bucket. A multihost pipeline
    # has one per host; this is the one the sync API reports.
    uuid: str
    # The step of the checkpoint, which is the same on every host.
    steps: int
    # The state the pipeline must have after a restore of this checkpoint.
    ledger: Ledger
    # The UUIDs of the checkpoint on all hosts; `{uuid}` for a solo pipeline.
    uuids: frozenset = frozenset()
    # The contents of the `gen_total` view, which no restore may change.
    gen_total: Optional[dict] = None
    # When the checkpoint was made, on the clock of the test.
    created: float = 0.0
    pushed: bool = False
    # The batch files of the checkpoint, known once it was pushed.
    batches: frozenset = frozenset()


@dataclass
class SyncOptions:
    """The settings of one run of a :class:`SyncPipeline`."""

    start_from: Optional[str] = None
    strict: bool = False
    retention_min_count: int = 2
    min_retention: str = "0s"
    push_interval: Optional[str] = None
    pull_interval: str = "2s"
    checkpoint_interval: str = NO_AUTOMATIC_CHECKPOINTS
    read_bucket: Optional[SyncBucket] = None
    take_bucket_ownership: bool = False
    flags: list = field(default_factory=list)
    auth_err: bool = False
    endpoint: Optional[str] = None
    # A bucket value to use as it is, for example one that does not exist.
    bucket_override: Optional[str] = None
    spill_everything: bool = True


class SyncPipeline:
    """A pipeline that runs the harness program and syncs to one bucket path.

    The methods are the steps of a scenario. Each one waits for its effect
    and checks it, so a test reads as a list of steps.
    """

    def __init__(
        self,
        role: str,
        bucket: SyncBucket,
        ledger: Optional[Ledger] = None,
        coverage_label: Optional[str] = None,
    ) -> None:
        """Takes an idle pipeline of this process, or creates one if there
        is none.

        :param role: what the pipeline is in its scenario, for example
            `<test>_standby`; it names the pipeline in the messages of the
            harness.
        :param bucket: the bucket path to sync to.
        :param ledger: the expected state of the pipeline; an empty ledger
            if omitted.
        :param coverage_label: the directory, under the coverage directory,
            for the coverage profiles of this pipeline; the role if omitted.
        """
        self.role = role
        self.coverage_label = coverage_label or role
        self.bucket = bucket
        self.ledger = ledger or Ledger()
        self.options = SyncOptions()
        self.checkpoints: list[Checkpoint] = []
        self.gen_total: Optional[dict] = None
        self.timeline: list[str] = []
        # Whether another scenario may use the pipeline after this one.
        self.reusable = True
        # Whether the pipeline holds input that no checkpoint covers, and
        # whether it did when the scenario stopped it; see `_expect_known_state`.
        self._unsaved_input = False
        self._stopped_with_unsaved_input = False
        # What identifies the numbering of the lines of the pipeline's log;
        # see `_log_end`. The platform tells the real value.
        self._log_epoch = str(UUID(int=0))
        if _IDLE_PIPELINES:
            self.pipeline: Pipeline = _IDLE_PIPELINES.pop()
        else:
            self.pipeline = PipelineBuilder(
                TEST_CLIENT,
                unique_pipeline_name(f"sync_{_PROCESS}_{next(_PIPELINES_CREATED)}"),
                sql=SQL,
                runtime_config=self._runtime_config(),
            ).create_or_replace()
        # The name of the pipeline on the platform.
        self.name = self.pipeline.name
        self._log(f"is the pipeline {self.name}")
        # Where the log of the pipeline ends now: what follows is the log of
        # this scenario, and what precedes it that of earlier ones.
        self._log_start = self._log_end()

    # ------------------------------------------------------------------
    # Configuration
    # ------------------------------------------------------------------

    def _sync_config(self) -> dict:
        o = self.options
        sync = self.bucket.store.checkpoint_sync_connection(
            self.bucket.path, auth_err=o.auth_err, endpoint=o.endpoint
        )
        if o.bucket_override is not None:
            sync["bucket"] = o.bucket_override
        sync.update(
            {
                "start_from_checkpoint": o.start_from,
                "fail_if_no_checkpoint": o.strict,
                "standby_pull_interval": o.pull_interval,
                "checkpoint_push_interval": o.push_interval,
                "retention_min_count": o.retention_min_count,
                "min_retention": o.min_retention,
            }
        )
        flags = list(o.flags) + os.environ.get(EXTRA_FLAGS_ENV, "").split()
        if flags:
            sync["flags"] = list(sync.get("flags", [])) + flags
        if o.read_bucket is not None:
            sync["read_bucket"] = o.read_bucket.sync_bucket
        if o.take_bucket_ownership:
            sync["take_bucket_ownership"] = True
        return sync

    def _runtime_config(self) -> RuntimeConfig:
        o = self.options
        storage: dict = {
            "backend": {"name": "file", "config": {"sync": self._sync_config()}}
        }
        if o.spill_everything:
            storage.update(SPILL_EVERYTHING)
        env = {}
        if coverage_dir := os.environ.get(COVERAGE_DIR_ENV):
            env["LLVM_PROFILE_FILE"] = (
                f"{coverage_dir}/{self.coverage_label}/p-%p%c.profraw"
            )
        return RuntimeConfig(
            workers=FELDERA_TEST_NUM_WORKERS,
            hosts=FELDERA_TEST_NUM_HOSTS,
            fault_tolerance_model=FaultToleranceModel.AtLeastOnce,
            storage=Storage(config=storage),
            checkpoint_interval=o.checkpoint_interval,
            env=env,
        )

    def _log(self, message: str) -> None:
        self.timeline.append(f"{time.strftime('%H:%M:%S')} {self.role}: {message}")
        LOGGER.info("%s: %s", self.role, message)

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    def start(self, **options) -> None:
        """Starts the pipeline with the given sync settings.

        :param options: fields of :class:`SyncOptions`; the others keep their
            defaults, not the values of the previous run.
        """
        self._expect_known_state()
        self.options = SyncOptions(**options)
        self.pipeline.set_runtime_config(self._runtime_config())
        self._log(f"start {options}")
        self.pipeline.start()

    def start_standby(self, **options) -> None:
        """Starts the pipeline in standby and waits for its first pull."""
        self._expect_known_state()
        self.options = SyncOptions(**options)
        self.pipeline.set_runtime_config(self._runtime_config())
        self._log(f"start standby {options}")
        # The log also holds the pulls of the earlier runs of the pipeline.
        before = self._log_end()
        self.pipeline.start_standby()
        self.wait_for_standby_pull(before)
        self._expect_status(PipelineStatus.STANDBY)

    def _expect_known_state(self) -> None:
        """Requires that what the pipeline starts from does not depend on
        the platform.

        The harness stops a pipeline by force. On Kubernetes a
        fault-tolerant pipeline with one host still writes a checkpoint as it
        stops, so it restarts with all its input; on a local runner, and with
        several hosts, it restarts from the last checkpoint before the stop.
        The two agree only if no input arrived after that checkpoint.
        """
        assert not self._stopped_with_unsaved_input, (
            f"{self.role}: the scenario stopped the pipeline with input that no"
            " checkpoint covered, and starts it again. Whether the pipeline"
            " still has that input depends on the platform. Make a checkpoint"
            " before the stop, or clear the storage after it."
        )

    def activate(self, timeout_s: float = STANDBY_TIMEOUT_S) -> None:
        self._log("activate")
        self.pipeline.activate(timeout_s=timeout_s)
        self._expect_status(PipelineStatus.RUNNING)

    def _expect_status(self, expected: PipelineStatus) -> None:
        """Requires the pipeline to have the status `expected`.

        The platform reports a pipeline as unavailable for a moment when one
        probe of it times out, as happens on a loaded machine, so the status
        gets a minute to show.
        """
        status = None

        def as_expected() -> bool:
            nonlocal status
            status = self.pipeline.status()
            return status == expected

        try:
            wait_for_condition(f"status {expected}", as_expected, 60.0, 1.0)
        except TimeoutError:
            raise AssertionError(
                f"{self.role}: the status is {status}, expected {expected}"
            ) from None

    def _log_end(self) -> str:
        """Returns the cursor of the end of the pipeline's log, as it is now.

        The platform keeps the last megabyte of a pipeline's log, over all
        the runs of the pipeline, and numbers the lines. A cursor names a
        line by its number, so it stays valid when the platform discards
        old lines; counting the lines of the log does not.
        """
        epoch = self._log_epoch
        while True:
            # The platform reads a position past the end of the log as the
            # end, and reports the position that it used.
            with self.pipeline.resume_logs(f"{epoch}:{_PAST_THE_END}") as stream:
                position = stream.position
            if position.epoch == epoch:
                return position.cursor()
            # The first call, or the platform has started to number the
            # lines anew.
            epoch = self._log_epoch = position.epoch

    def _log_after(self, cursor: Optional[str]) -> tuple[list[str], Optional[str]]:
        """Returns the lines that the pipeline logged after a position.

        :param cursor: the position, from :meth:`_log_end` or from an earlier
            call; None for all that the platform keeps of the log.
        :return: the lines, and the cursor to pass to the next call.
        """
        end = int(self._log_end().rpartition(":")[2])
        with self.pipeline.resume_logs(cursor) as stream:
            if stream.position.epoch != self._log_epoch:
                # The platform started to number the lines anew between the
                # two requests, so `end` says nothing about this stream. The
                # next call reads the new log from its beginning.
                return [], None
            # The stream waits for new lines when it reaches the end of the
            # log, so read the lines that were there before it opened, and
            # no more.
            count = max(end - stream.position.seq, 0)
            lines = list(itertools.islice(stream, count))
            return lines, stream.cursor()

    def wait_for_standby_pull(
        self, after: Optional[str] = None, timeout_s: float = STANDBY_TIMEOUT_S
    ) -> None:
        """Blocks until the standby pipeline completes a pull.

        A standby pipeline has no controller, so its log is the only place
        that shows a pull. The harness polls the log for new lines instead
        of following it: a stream that it follows blocks for as long as the
        pipeline is silent, and ends when the connection drops.

        :param after: a position in the log, from :meth:`_log_end`: only a
            pull that the pipeline logged after it counts. None for any pull
            that the log holds, also one of an earlier run.
        """
        cursor = after

        def pulled() -> bool:
            nonlocal cursor
            lines, cursor = self._log_after(cursor)
            return any(_PULLED in line for line in lines)

        try:
            wait_for_condition(
                "the standby pipeline completes a pull", pulled, timeout_s, 1.0
            )
        except TimeoutError:
            tail = "\n  ".join(list(self.pipeline.logs(follow=False))[-15:])
            raise AssertionError(
                f"{self.role}: no checkpoint pull in {timeout_s}s; the log ends"
                f" with:\n  {tail}"
            ) from None

    def expect_start_error(self, pattern: str, standby: bool = False, **options) -> str:
        """Starts the pipeline and requires the start to fail.

        The failure must leave the bucket as it was: a start reads the
        bucket, and only `take_bucket_ownership` may write to it.

        :param pattern: a regular expression that the error must match.
        :param standby: start in standby and activate; the activation must
            fail.
        :return: the text of the error.
        """
        before = self.bucket.objects()
        self.options = SyncOptions(**options)
        self.pipeline.set_runtime_config(self._runtime_config())
        self._log(f"start, expecting an error like /{pattern}/ {options}")
        try:
            if standby:
                self.pipeline.start_standby()
                self.pipeline.activate(timeout_s=STANDBY_TIMEOUT_S)
            else:
                self.pipeline.start()
        except RuntimeError as error:
            text = str(error)
        else:
            rows = self.query_count()
            raise AssertionError(
                f"{self.role} started (with {rows} fact rows) although the start"
                f" had to fail with /{pattern}/"
            )
        assert re.search(pattern, text), (
            f"{self.role} failed to start, but not with /{pattern}/:\n{text}"
        )
        if not options.get("take_bucket_ownership"):
            assert self.bucket.objects() == before, (
                f"the failed start of {self.role} changed the bucket"
            )
        self.pipeline.stop(force=True)
        return text

    def stop(self) -> None:
        self._log("stop")
        self.pipeline.stop(force=True)
        self._stopped_with_unsaved_input = self._unsaved_input

    def wipe(self) -> None:
        """Stops the pipeline and clears its local storage."""
        self.stop()
        self.pipeline.clear_storage()
        self._unsaved_input = self._stopped_with_unsaved_input = False

    def release(self, reuse: bool) -> list[str]:
        """Ends the part of the pipeline in its scenario: stops it and
        clears its storage.

        :param reuse: let a later scenario of this process use the pipeline.
            Pass False after a failed test, whose pipelines may be in a
            state that the harness does not know.
        :return: one message for each step that failed.
        """
        failures = reclaim_pipeline(self.name)
        if reuse and self.reusable and not failures:
            _IDLE_PIPELINES.append(self.pipeline)
        return failures

    def rename(self, new_name: str) -> None:
        """Renames the pipeline, which must be stopped. Its bucket path and
        its identity as the owner of that path stay the same."""
        new_name = unique_pipeline_name(new_name)
        # An earlier run of the test may have left a pipeline with this name.
        # There is usually none, and then every step of this fails.
        reclaim_pipeline(new_name, delete=True)
        TEST_CLIENT.http.patch(f"/pipelines/{self.name}", {"name": new_name})
        self._log(f"renamed to {new_name}")
        self.name = new_name
        self.pipeline = Pipeline.get(new_name, TEST_CLIENT)
        # The new name is that of one test.
        self.reusable = False

    # ------------------------------------------------------------------
    # Input and state
    # ------------------------------------------------------------------

    def load(self) -> None:
        """Loads `dim` and waits for the generated input, then checks the
        state. Call once, on a pipeline that starts from nothing."""
        self.pipeline.input_json("dim", self.ledger.dim_rows())
        self._unsaved_input = True
        self.pipeline.wait_for_completion()
        self.gen_total = self._gen_total()
        if not MULTIHOST:
            assert self.gen_total == {"n": 5000, "s": 12_497_500}, self.gen_total
        self.verify()

    def round(self, count: int = 1) -> None:
        """Feeds `count` rounds of changes and checks the state after each."""
        for _ in range(count):
            changes = self.ledger.next_round()
            self.pipeline.input_json("fact", changes, update_format="insert_delete")
            self._unsaved_input = True
            self.verify()
        self._log(f"round {self.ledger.round}: {len(self.ledger.fact)} fact rows")

    def query_count(self) -> int:
        return list(self.pipeline.query("SELECT COUNT(*) AS n FROM fact"))[0]["n"]

    def _gen_total(self) -> dict:
        row = list(self.pipeline.query("SELECT * FROM gen_total"))[0]
        return {"n": row["n"], "s": int(row["s"])}

    def _by_grp(self) -> dict:
        return {
            row["grp"]: (row["n"], int(row["total"]))
            for row in self.pipeline.query("SELECT * FROM by_grp")
        }

    def verify(
        self, ledger: Optional[Ledger] = None, timeout_s: float = STATE_TIMEOUT_S
    ) -> None:
        """Requires the state of the pipeline to equal the ledger.

        The views are polled, because a step's output can become visible to
        ad-hoc queries after the input is reported as processed: a moment
        later in a solo pipeline, and once every host has committed the step
        in a multihost one.

        :param ledger: the expected state; the pipeline's own ledger if
            omitted.
        """
        ledger = ledger or self.ledger
        expected = (ledger.by_grp(), len(ledger.fact), self.gen_total)
        got = None

        def matches() -> bool:
            nonlocal got
            got = (
                self._by_grp(),
                self.query_count(),
                self._gen_total() if self.gen_total is not None else None,
            )
            return got == expected

        try:
            wait_for_condition(
                "pipeline state equals the ledger", matches, timeout_s, 0.5
            )
        except TimeoutError:
            raise AssertionError(
                f"{self.role}: state differs from the ledger at round {ledger.round}:\n"
                f"  fact rows: {got[1]}, expected {expected[1]}\n"
                f"  gen_total: {got[2]}, expected {expected[2]}\n"
                f"  by_grp differs in groups"
                f" {sorted(g for g in set(got[0]) | set(expected[0]) if got[0].get(g) != expected[0].get(g))[:10]}"
            ) from None

    def restored(self, checkpoint: Checkpoint, feed: bool = True) -> None:
        """Declares that the pipeline was just started from `checkpoint`,
        and checks it: the state must be the checkpoint's state, and the
        next round of input must combine with it correctly.

        :param checkpoint: the checkpoint the pipeline started from.
        :param feed: feed one more round of input after the check.
        """
        self.ledger = checkpoint.ledger.copy()
        self.gen_total = checkpoint.gen_total
        self._unsaved_input = False
        self._log(f"restored {checkpoint.uuid}; expecting round {self.ledger.round}")
        self.verify()
        if feed:
            self.round()

    # ------------------------------------------------------------------
    # Checkpoints and pushes
    # ------------------------------------------------------------------

    def checkpoint(self) -> Checkpoint:
        """Makes a checkpoint and returns it."""
        # The remote GC compares creation times in whole seconds; keep
        # consecutive checkpoints at least a second apart so that what it
        # must collect does not depend on timing.
        if self.checkpoints:
            wait = self.checkpoints[-1].created + 1.1 - time.time()
            if wait > 0:
                time.sleep(wait)
        self.pipeline.checkpoint(wait=True)
        self._unsaved_input = False
        listed = self.pipeline.checkpoints()
        newest = max(listed, key=lambda c: (c.steps, str(c.uuid)))
        made = Checkpoint(
            uuid=str(newest.uuid),
            steps=newest.steps,
            uuids=frozenset(str(c.uuid) for c in listed if c.steps == newest.steps),
            ledger=self.ledger.copy(),
            gen_total=self.gen_total,
            created=time.time(),
        )
        self.checkpoints.append(made)
        self._log(f"checkpoint {made.uuid} at round {self.ledger.round}")
        return made

    def wait_for_automatic_checkpoint(
        self, timeout_s: float = SYNC_TIMEOUT_S
    ) -> Checkpoint:
        """Waits for a checkpoint that the pipeline made by itself and that
        covers all the input so far, and returns it.

        Use with a short `checkpoint_interval`, and feed no input until the
        scenario is done with the checkpoint: every later automatic
        checkpoint then holds the same state.
        """
        processed = self.pipeline.stats().global_metrics.total_processed_records
        found = None

        def covers_input() -> bool:
            nonlocal found
            by_step: dict = {}
            for c in self.pipeline.checkpoints():
                by_step.setdefault(c.steps, []).append(c)
            for steps, group in sorted(by_step.items()):
                # A multihost pipeline splits the records between its hosts.
                if sum(c.processed_records for c in group) >= processed:
                    found = (str(group[0].uuid), steps)
                    return True
            return False

        wait_for_condition(
            "an automatic checkpoint covers the input", covers_input, timeout_s, 0.5
        )
        self._unsaved_input = False
        made = Checkpoint(
            uuid=found[0],
            steps=found[1],
            uuids=frozenset(
                str(c.uuid) for c in self.pipeline.checkpoints() if c.steps == found[1]
            ),
            ledger=self.ledger.copy(),
            gen_total=self.gen_total,
            created=time.time(),
        )
        self.checkpoints.append(made)
        self._log(f"automatic checkpoint {made.uuid} at round {self.ledger.round}")
        return made

    def wait_for_automatic_push(
        self, made: Checkpoint, timeout_s: float = SYNC_TIMEOUT_S
    ) -> None:
        """Waits until the periodic push has pushed `made` or a later
        checkpoint."""
        status: dict = {}
        # The UUID of a checkpoint grows with the time of the checkpoint (UUID
        # version 7), so a UUID at or after `made` names `made` or a later
        # checkpoint. The list of local checkpoints cannot tell: a pipeline
        # that checkpoints by itself drops the pushed checkpoint from it as
        # soon as a newer one exists.
        first = min(UUID(u) for u in made.uuids | {made.uuid})

        def pushed() -> bool:
            nonlocal status
            status = self.sync_status()
            steps = {str(c.uuid): c.steps for c in self.pipeline.checkpoints()}
            done = [u for u in (status.get("periodic"), status.get("success")) if u]
            return any(UUID(u) >= first or steps.get(u, -1) >= made.steps for u in done)

        try:
            wait_for_condition(
                "the periodic push covers the checkpoint", pushed, timeout_s, 0.5
            )
        except TimeoutError:
            raise AssertionError(
                f"{self.role}: no periodic push of {made.uuid} (step {made.steps})"
                f" in {timeout_s}s; sync status: {status}"
            ) from None
        made.pushed = True
        self._log(f"periodic push covers {made.uuid}")

    def push(self, audit: bool = True) -> Checkpoint:
        """Pushes the newest checkpoint and audits the bucket.

        :param audit: check the invariants of the bucket after the push; a
            test that damaged the bucket on purpose audits it itself.
        :return: the checkpoint that was pushed.
        """
        before = self.bucket.audit()
        started = time.time()
        uuid = self.pipeline.sync_checkpoint(wait=True)
        made = self._checkpoint_for(uuid)
        made.pushed = True
        self._log(f"pushed {uuid}")
        after = self.bucket.audit()
        # A multihost pipeline pushes one zip per host for the checkpoint.
        made.batches = frozenset(
            after.batches_of((made.uuids | {made.uuid}) & after.uuids())
        )
        if audit:
            self.audit_after_push(made, after, started, time.time())
            # A push uploads the batch files that the newest checkpoint in
            # the bucket lacks, and no others. A pipeline that carries on from
            # that checkpoint therefore uploads only new files. A pipeline
            # that went back to an older checkpoint uploads again the files
            # that only older checkpoints in the bucket have. Every push
            # replaces the catalog and the ownership file.
            newest = before.batches_of(before.latest() & before.uuids())
            rewritten = sorted(
                rel
                for rel, info in before.objects.items()
                if rel in after.objects
                and after.objects[rel] != info
                and not _CATALOG.match(rel)
                and rel != OWNER_FILE
                and rel not in made.batches - newest
            )
            assert not rewritten, (
                f"{self.role}: the push of {uuid} rewrote {len(rewritten)} objects"
                f" that were already in the bucket, for example {rewritten[:3]}"
            )
        return made

    def expect_push_error(self, pattern: str) -> str:
        """Pushes the newest checkpoint and requires the push to fail
        without changing the bucket.

        :param pattern: a regular expression that the error must match.
        :return: the text of the error.
        """
        before = self.bucket.objects()
        self._log(f"push, expecting an error like /{pattern}/")
        try:
            self.pipeline.sync_checkpoint(wait=True)
        except RuntimeError as error:
            text = str(error)
        else:
            raise AssertionError(f"{self.role}: the push had to fail with /{pattern}/")
        assert re.search(pattern, text), (
            f"{self.role}: the push failed, but not with /{pattern}/:\n{text}"
        )
        assert self.bucket.objects() == before, (
            f"the failed push of {self.role} changed the bucket"
        )
        assert self.sync_status().get("running") == [], self.sync_status()
        return text

    def _checkpoint_for(self, uuid: str) -> Checkpoint:
        """Returns the record of the checkpoint that a push reports.

        :param uuid: the UUID that the push reports.
        :return: the record that the scenario has of the checkpoint. A
            pipeline that checkpoints by itself can push a checkpoint that
            the scenario has not seen; that checkpoint gets a new record.
        """
        uuid = str(uuid)
        for made in self.checkpoints:
            if uuid == made.uuid or uuid in made.uuids:
                return made
        if self.options.checkpoint_interval != NO_AUTOMATIC_CHECKPOINTS:
            # The pipeline made this checkpoint after the last one that the
            # scenario saw. A scenario feeds no input while it uses automatic
            # checkpoints (see `wait_for_automatic_checkpoint`), so the new
            # checkpoint holds the state of that last one.
            last = self.checkpoints[-1]
            made = Checkpoint(
                uuid=uuid,
                steps=last.steps,
                uuids=frozenset({uuid}),
                ledger=last.ledger.copy(),
                gen_total=last.gen_total,
                created=time.time(),
            )
            self.checkpoints.append(made)
            self._log(f"automatic checkpoint {uuid}, made after {last.uuid}")
            return made
        if MULTIHOST:
            # The sync API may report another host's UUID for the same step.
            return self.checkpoints[-1]
        raise AssertionError(
            f"{self.role}: pushed {uuid}, which is not a checkpoint of this"
            f" scenario: {[c.uuid for c in self.checkpoints]}"
        )

    def sync_status(self) -> dict:
        return self.pipeline.client.sync_checkpoint_status(self.name)

    def pushed(self) -> list[Checkpoint]:
        return [c for c in self.checkpoints if c.pushed]

    def expected_retention(self, gc_from: float, gc_to: float) -> tuple[set, set]:
        """Computes which pushed checkpoints a remote GC must keep and which
        it must remove.

        The GC removes the oldest checkpoints beyond `retention_min_count`,
        but none younger than `min_retention`; it compares creation times in
        whole seconds. The GC ran at some time in [`gc_from`, `gc_to`] on the
        pipeline's clock, so a checkpoint whose age was near `min_retention`
        then may be either kept or removed.

        :return: `(must_keep, must_remove)`, as sets of UUIDs. A multihost
            pipeline has no remote GC and must keep everything.
        """
        pushed = [c.uuid for c in self.pushed()]
        if MULTIHOST:
            return set(pushed), set()
        min_age = _seconds(self.options.min_retention)
        slack = 2.0
        surely_old = [u for u in pushed if uuid_time(u) < gc_from - min_age - slack]
        maybe_old = [u for u in pushed if uuid_time(u) < gc_to - min_age + slack]
        excess = len(pushed) - self.options.retention_min_count
        must_remove = set(surely_old[: max(excess, 0)])
        may_remove = set(maybe_old[: max(excess, 0)])
        return set(pushed) - may_remove, must_remove

    def audit_after_push(
        self, made: Checkpoint, audit: Audit, gc_from: float, gc_to: float
    ) -> None:
        """Checks the invariants of the bucket after a successful push.

        :param made: the checkpoint that was pushed.
        :param audit: the bucket as it is after the push.
        :param gc_from: the earliest time the remote GC of the push can have
            run, on the clock of the test.
        :param gc_to: the latest such time.
        """
        problems = audit.problems()
        must_keep, must_remove = self.expected_retention(gc_from, gc_to)
        on_host0 = {u for u, c in audit.checkpoints.items() if c.host == "host0"}
        if not MULTIHOST:
            if missing := must_keep - audit.uuids():
                problems.append(f"the GC removed checkpoints it had to keep: {missing}")
            if left := must_remove & audit.uuids():
                problems.append(f"the GC kept checkpoints it had to remove: {left}")
            if strangers := audit.uuids() - {c.uuid for c in self.checkpoints}:
                problems.append(f"checkpoints of another pipeline: {strangers}")
            catalog = audit.catalogs.get("", [])
            if [entry.get("uuid") for entry in catalog] != [made.uuid]:
                problems.append(
                    f"the catalog names {[e.get('uuid') for e in catalog]},"
                    f" not the pushed checkpoint {made.uuid}"
                )
            remote = {str(c["uuid"]) for c in self.pipeline.remote_checkpoints()}
            if remote != audit.uuids():
                problems.append(
                    f"remote_checkpoints() reports {sorted(remote)}, the bucket"
                    f" holds {sorted(audit.uuids())}"
                )
        else:
            hosts = {c.host for c in audit.checkpoints.values()}
            expected_hosts = {f"host{i}" for i in range(FELDERA_TEST_NUM_HOSTS)}
            if hosts != expected_hosts:
                problems.append(f"checkpoints of hosts {hosts}, not {expected_hosts}")
            if len(on_host0) < len(self.pushed()):
                problems.append(
                    f"host0 has {len(on_host0)} checkpoints after {len(self.pushed())}"
                    f" pushes, although a multihost pipeline has no remote GC"
                )
        owner = (audit.owner or {}).get("pipeline_name")
        if owner != f"pipeline-{self.pipeline.id()}":
            problems.append(f"owner.json names {owner}, not this pipeline")
        assert not problems, (
            f"{self.role}: the bucket is inconsistent after the push of"
            f" {made.uuid}:\n  " + "\n  ".join(problems) + f"\n  {audit.summary()}"
        )

    def diagnostics(self) -> str:
        """Returns what a failed test needs: the steps so far, the sync
        status, the state of the bucket and an excerpt of the pipeline's
        log."""
        lines = list(self.timeline)
        try:
            lines.append(f"sync status: {self.sync_status()}")
        except Exception as error:  # the pipeline may not run
            lines.append(f"sync status unavailable: {error}")
        try:
            audit = self.bucket.audit()
            lines.append(f"bucket {self.bucket.path}: {audit.summary()}")
            lines.extend(f"  {p}" for p in audit.problems())
        except Exception as error:
            lines.append(f"bucket audit failed: {error}")
        try:
            lines.extend(self._log_excerpt())
        except Exception as error:
            lines.append(f"log of {self.name} unavailable: {error}")
        return "\n".join(lines)

    def _log_excerpt(self) -> list[str]:
        """Returns the part of the pipeline's log that a failed test needs.

        The platform keeps the log of a pipeline after the pipeline stops,
        so the excerpt also shows why a pipeline failed to start.

        :return: the last notable lines (see `_NOTABLE`) that the pipeline
            logged in this scenario, then its last lines of any kind.
        """
        logged, _ = self._log_after(self._log_start)
        # A line can hold a whole configuration; its beginning says enough.
        logged = [line[:_LOG_EXCERPT_WIDTH] for line in logged]
        notable = [line for line in logged if _NOTABLE.search(line)]
        out = [
            f"log of {self.name}: {len(logged)} lines in this scenario,"
            f" {len(notable)} of them notable; the last notable ones:"
        ]
        out.extend(f"  {line}" for line in notable[-_LOG_EXCERPT_LINES:])
        out.append(f"log of {self.name}: the last lines:")
        out.extend(f"  {line}" for line in logged[-_LOG_EXCERPT_LINES:])
        return out


_UNITS = {"ms": 0.001, "s": 1.0, "m": 60.0, "h": 3600.0, "d": 86400.0}


def _seconds(duration: str) -> float:
    """Parses a duration such as `"20s"` or `"1d"` into seconds."""
    match = re.fullmatch(r"(\d+)(ms|s|m|h|d)", duration)
    assert match, f"unsupported duration {duration!r}"
    return int(match.group(1)) * _UNITS[match.group(2)]


class Scenario:
    """The pipelines and bucket paths of one test, and their cleanup."""

    def __init__(self, test_name: str) -> None:
        # A pipeline name is `<run prefix>_<base>_<role>` and has a length
        # limit. A long test name, such as that of a parametrized test, is
        # cut and gets a hash, so that two tests never share a pipeline.
        base = re.sub(r"[^a-z0-9_]+", "_", test_name.lower()).strip("_")
        base = base.removeprefix("test_")
        longest = _NAME_BUDGET - len("_") - _MAX_ROLE_LEN
        if len(base) > longest:
            digest = hashlib.sha1(test_name.encode()).hexdigest()[:6]
            base = f"{base[: longest - len(digest) - 1]}_{digest}"
        self.base = base
        self.pipelines: list[SyncPipeline] = []
        self.buckets: list[SyncBucket] = []

    def pipeline(
        self,
        role: str = "main",
        bucket: Optional[SyncBucket] = None,
        ledger: Optional[Ledger] = None,
    ) -> SyncPipeline:
        """Gives the test a pipeline with empty storage.

        :param role: tells the pipelines of one test apart, for example
            "primary" and "standby".
        :param bucket: a bucket path to sync to. The scenario neither empties
            it nor removes it; without it the pipeline gets a new, empty
            path, which the scenario removes at the end.
        """
        if bucket is None:
            bucket = self.bucket(role)
        made = SyncPipeline(
            self.name(role), bucket, ledger=ledger, coverage_label=self.base
        )
        self.pipelines.append(made)
        return made

    def name(self, role: str) -> str:
        """Returns the name of the test's pipeline or bucket path `role`,
        without the prefix of the run."""
        assert len(role) <= _MAX_ROLE_LEN, (
            f"the role '{role}' is longer than {_MAX_ROLE_LEN} characters, which"
            " is what the names of the harness leave room for"
        )
        return f"{self.base}_{role}"

    def bucket(self, role: str) -> SyncBucket:
        """Creates a bucket path of the test that no pipeline pushes to yet."""
        made = SyncBucket(unique_pipeline_name(self.name(role)))
        # A run that was interrupted, or an earlier run under the same name,
        # may have left objects behind.
        made.wipe()
        self.buckets.append(made)
        return made

    def diagnostics(self) -> str:
        return "\n\n".join(p.diagnostics() for p in self.pipelines)

    def cleanup(self, reuse: bool = True) -> list[str]:
        """Stops and clears every pipeline and empties every bucket path.

        :param reuse: let later scenarios of this process use the pipelines;
            see :meth:`SyncPipeline.release`.
        :return: one message for each step that failed.
        """
        failures = []
        for pipeline in self.pipelines:
            failures.extend(pipeline.release(reuse))
        self.pipelines = []
        for bucket in self.buckets:
            try:
                bucket.wipe()
            except Exception as error:
                failures.append(f"{bucket.path}: wipe: {error}")
        return failures


# ---------------------------------------------------------------------------
# Buckets that are built once for the whole test run
# ---------------------------------------------------------------------------

_GOLDEN_PREFIX = "sync-golden-"


@dataclass
class Golden:
    """A bucket path with pushed checkpoints, which tests restore from and
    never change."""

    bucket: SyncBucket
    checkpoints: list[Checkpoint]

    @property
    def latest(self) -> Checkpoint:
        return self.checkpoints[-1]

    def _description(self) -> dict:
        """Returns what another process needs to use the bucket, as JSON."""
        return {
            "bucket": self.bucket.name,
            "checkpoints": [
                {
                    "uuid": c.uuid,
                    "steps": c.steps,
                    "uuids": sorted(c.uuids),
                    # A ledger is a function of its number of rounds.
                    "rounds": c.ledger.round,
                    "gen_total": c.gen_total,
                    "created": c.created,
                    "batches": sorted(c.batches),
                }
                for c in self.checkpoints
            ],
        }

    @staticmethod
    def _described(description: dict) -> "Golden":
        checkpoints = []
        for c in description["checkpoints"]:
            ledger = Ledger()
            for _ in range(c["rounds"]):
                ledger.next_round()
            checkpoints.append(
                Checkpoint(
                    uuid=c["uuid"],
                    steps=c["steps"],
                    uuids=frozenset(c["uuids"]),
                    ledger=ledger,
                    gen_total=c["gen_total"],
                    created=c["created"],
                    pushed=True,
                    batches=frozenset(c["batches"]),
                )
            )
        return Golden(SyncBucket(description["bucket"]), checkpoints)


def _build_golden(name: str, rounds_per_checkpoint: int) -> Golden:
    """Pushes three checkpoints to a new bucket path, with a pipeline that
    is then free for other scenarios."""
    scenario = Scenario(name)
    try:
        seed = scenario.pipeline("seed")
        seed.start(retention_min_count=3)
        seed.load()
        for _ in range(3):
            seed.round(rounds_per_checkpoint)
            seed.checkpoint()
            seed.push()
    except BaseException:
        print(scenario.diagnostics(), file=sys.stderr)
        for failure in scenario.cleanup(reuse=False):
            print(f"cleanup: {failure}", file=sys.stderr)
        raise
    made = Golden(seed.bucket, seed.pushed())
    # The bucket path outlives the scenario, so only the pipeline is released.
    for failure in seed.release(reuse=True):
        print(f"cleanup: {failure}", file=sys.stderr)
    return made


def golden_bucket(shared_dir: Path, name: str, rounds_per_checkpoint: int) -> Golden:
    """Returns a bucket path with three pushed checkpoints, which the first
    caller in the test run builds.

    pytest-xdist runs the tests in several processes, and each process that
    needs the bucket calls this function. A lock file lets one of them build
    the bucket; the others wait, and then read the description that the
    builder left next to the lock. Tests only read the bucket, so any number
    of them can use it at the same time. :func:`remove_golden_buckets`
    removes it when the run ends.

    :param shared_dir: a directory that all the processes of the test run,
        and no other run, share.
    :param name: tells the golden buckets of a run apart.
    :param rounds_per_checkpoint: rounds of input before each checkpoint; two
        buckets built with different values hold different data.
    :return: the bucket and its checkpoints.
    """
    saved = shared_dir / f"{_GOLDEN_PREFIX}{name}.json"
    with open(shared_dir / f"{_GOLDEN_PREFIX}{name}.lock", "w") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX)
        if not saved.exists():
            try:
                made = _build_golden(name, rounds_per_checkpoint)
            except Exception as error:
                # The other processes then fail at once, instead of building
                # the bucket, and failing, one after another.
                failure = {"error": f"{type(error).__name__}: {error}"}
                saved.write_text(json.dumps(failure))
                raise
            saved.write_text(json.dumps(made._description()))
            return made
        description = json.loads(saved.read_text())
    if "error" in description:
        raise RuntimeError(
            f"another process failed to build the golden bucket '{name}':"
            f" {description['error']}"
        )
    return Golden._described(description)


def remove_golden_buckets(shared_dir: Path) -> list[str]:
    """Empties the golden buckets of the test run. Call after the last test.

    :param shared_dir: the directory that :func:`golden_bucket` got.
    :return: one message for each step that failed.
    """
    failures = []
    for saved in sorted(shared_dir.glob(f"{_GOLDEN_PREFIX}*.json")):
        description = json.loads(saved.read_text())
        if "error" in description:
            continue
        try:
            SyncBucket(description["bucket"]).wipe()
        except Exception as error:
            failures.append(f"{description['bucket']}: wipe: {error}")
    return failures
