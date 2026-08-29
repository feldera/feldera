"""Concurrent builders of one cached Delta fixture must not collide.

``ensure_delta_spark_fixture`` caches at a path shared by every test needing
that fixture, so xdist workers reach it at once. The build is not idempotent
(data file and physical column names are fresh per build), so two builders
placing a tree at once leave a log describing files the tree does not have.

The builder subprocess is stubbed: this covers the locking, not PySpark.
"""

from __future__ import annotations

import json
import multiprocessing
import os
import pathlib
import shutil
import subprocess
import sys
import time
import types
import uuid

import pytest

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[2]))

from tests.utils import LOCAL_FIXTURE_ROOT, DeltaTestLocation  # noqa: E402

# Wide enough that an unlocked second builder lands inside the first's window.
_BUILD_SECONDS = 1.0
_BUILDERS = 2


def _write_fake_delta_tree(staging: pathlib.Path, seed: int) -> None:
    """Write a minimal Delta tree whose data file name is unique to ``seed``.

    The log names a data file only this build produced, which is what makes a
    concurrent build destructive.
    """
    log_dir = staging / "_delta_log"
    log_dir.mkdir(parents=True, exist_ok=True)
    data_file = f"part-00000-{seed}.parquet"
    (staging / data_file).write_text(str(seed), encoding="utf-8")
    (log_dir / "00000000000000000000.json").write_text(
        json.dumps({"add": {"path": data_file}}), encoding="utf-8"
    )


def _build_fixture(subpath: str, build_log: str, barrier) -> None:
    """Child process: build the fixture once, recording that it did.

    Spawned, so the ``flock`` is crossed by real processes as xdist does it.
    """
    # Drive the local backend; the S3 branch wants MinIO credentials.
    os.environ.pop("CI", None)

    from tests import utils

    def fake_builder_run(cmd, check=False, **kwargs):
        staging = pathlib.Path(cmd[-1])
        time.sleep(_BUILD_SECONDS)
        _write_fake_delta_tree(staging, seed=os.getpid())
        with open(build_log, "a", encoding="utf-8") as handle:
            handle.write(f"{os.getpid()}\n")
        return subprocess.CompletedProcess(cmd, 0)

    utils.subprocess = types.SimpleNamespace(
        run=fake_builder_run,
        CalledProcessError=subprocess.CalledProcessError,
    )

    loc = utils.DeltaTestLocation.create("fixture_lock_test", stable_subpath=subpath)
    barrier.wait()
    utils.ensure_delta_spark_fixture(loc, "stub_builder.py")


@pytest.fixture(name="fixture_subpath")
def fixture_subpath_fixture():
    """A cache path of this test's own, removed afterwards."""
    subpath = f"build_lock_test_{uuid.uuid4().hex}"
    yield subpath
    shutil.rmtree(LOCAL_FIXTURE_ROOT / subpath, ignore_errors=True)


def test_concurrent_builders_build_once(tmp_path, fixture_subpath):
    """Racing builders produce exactly one build and one coherent tree."""
    if shutil.which("uv") is None:
        pytest.skip("ensure_delta_spark_fixture requires `uv` on PATH")

    build_log = tmp_path / "builds.txt"
    ctx = multiprocessing.get_context("spawn")
    barrier = ctx.Barrier(_BUILDERS)
    workers = [
        ctx.Process(
            target=_build_fixture,
            args=(fixture_subpath, str(build_log), barrier),
        )
        for _ in range(_BUILDERS)
    ]
    for worker in workers:
        worker.start()
    for worker in workers:
        worker.join(timeout=120)
        # A wedged builder is the failure under test: reap it, or
        # multiprocessing's atexit join hangs pytest instead of failing it.
        if worker.is_alive():
            worker.terminate()
            worker.join(timeout=10)

    builds = build_log.read_text(encoding="utf-8").split() if build_log.exists() else []
    assert len(builds) == 1, (
        f"exactly one process may build the fixture, but {len(builds)} did "
        f"({builds}); the losers must wait on the lock and then see the "
        "finished fixture instead of rebuilding over it"
    )

    assert [worker.exitcode for worker in workers] == [0] * _BUILDERS, (
        "a non-zero exit means the builders tripped over each other's place-tree"
    )

    fixture_dir = LOCAL_FIXTURE_ROOT / fixture_subpath
    assert (fixture_dir / DeltaTestLocation.READY_MARKER).is_file(), (
        "the completion marker must be present once the tree is placed"
    )

    log_file = fixture_dir / "_delta_log" / "00000000000000000000.json"
    added = json.loads(log_file.read_text(encoding="utf-8"))["add"]["path"]
    assert (fixture_dir / added).is_file(), (
        f"the log names data file '{added}' which is not in the tree: the "
        "fixture is a mix of two builds"
    )


def test_half_placed_fixture_is_rebuilt(monkeypatch, fixture_subpath):
    """A tree left behind by a builder that died mid-place must not be reused.

    It already has a ``_delta_log``, so the log-existence check alone would
    hand the wreckage to every later test.
    """
    if shutil.which("uv") is None:
        pytest.skip("ensure_delta_spark_fixture requires `uv` on PATH")

    monkeypatch.delenv("CI", raising=False)

    from tests import utils

    # Log placed, data file and marker never written.
    fixture_dir = LOCAL_FIXTURE_ROOT / fixture_subpath
    (fixture_dir / "_delta_log").mkdir(parents=True, exist_ok=True)
    (fixture_dir / "_delta_log" / "00000000000000000000.json").write_text(
        json.dumps({"add": {"path": "part-00000-vanished.parquet"}}), encoding="utf-8"
    )
    assert utils.DeltaTestLocation.create(
        "fixture_lock_test", stable_subpath=fixture_subpath
    ).delta_log_exists(), "precondition: the half-placed tree looks present"

    builds = []

    def fake_builder_run(cmd, check=False, **kwargs):
        _write_fake_delta_tree(pathlib.Path(cmd[-1]), seed=len(builds))
        builds.append(cmd)
        return subprocess.CompletedProcess(cmd, 0)

    monkeypatch.setattr(
        utils,
        "subprocess",
        types.SimpleNamespace(
            run=fake_builder_run,
            CalledProcessError=subprocess.CalledProcessError,
        ),
    )

    loc = utils.DeltaTestLocation.create(
        "fixture_lock_test", stable_subpath=fixture_subpath
    )
    utils.ensure_delta_spark_fixture(loc, "stub_builder.py")

    assert len(builds) == 1, (
        "a fixture without the completion marker must be rebuilt, not reused"
    )
    assert (fixture_dir / DeltaTestLocation.READY_MARKER).is_file()
    assert not (fixture_dir / "part-00000-vanished.parquet").exists(), (
        "the rebuild must replace the half-placed tree, not merge with it"
    )


@pytest.mark.parametrize(
    ("tag", "expected"), [("multihost", "suite-multihost"), ("", "suite")]
)
def test_stable_path_is_suite_namespaced(monkeypatch, tag, expected):
    """Concurrent CI suites must not share a fixture path in the MinIO bucket."""
    from tests import utils

    monkeypatch.setenv("CI", "1")
    monkeypatch.setenv("FELDERA_TEST_TAG_SUFFIX", tag)
    monkeypatch.setenv("CI_K8S_MINIO_ACCESS_KEY_ID", "key")
    monkeypatch.setenv("CI_K8S_MINIO_SECRET_ACCESS_KEY", "secret")

    loc = utils.DeltaTestLocation.create("p", stable_subpath="column_mapping_v3")

    assert loc.root_path.endswith(f"_fixtures/{expected}/column_mapping_v3")
