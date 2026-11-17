"""End-to-end tests of checkpoint sync: push, garbage collection, restore,
standby and failures.

Every test is a scenario on real pipelines. The harness in
``checkpoint_sync_harness.py`` checks each step: the state of the pipeline
against a model of its input, and the contents of the bucket against the
invariants of the sync protocol. A test therefore asserts little itself; it
states the steps and what is special about the scenario.

Faults are injected in two ways, neither of which needs anything but the
pipeline's configuration and access to the object store:

* through `SyncConfig.flags`, with rclone flags that make one kind of
  operation fail (see ``FLAGS_ONE_DELETE`` and ``FLAGS_NO_TRANSFER``);
* by editing the bucket between two steps.
"""

from __future__ import annotations

import sys
import time
from uuid import uuid4

import pytest

from feldera.testutils import enterprise_only, single_host_only
from tests.utils import ObjectStore

from .checkpoint_sync_harness import (
    DEPENDENCIES_DIR,
    FLAGS_NO_TRANSFER,
    FLAGS_ONE_DELETE,
    CATALOG_FILE,
    MULTIHOST,
    NO_AUTOMATIC_CHECKPOINTS,
    OWNER_FILE,
    Checkpoint,
    Scenario,
    golden_bucket,
)


@pytest.fixture
def scenario(request):
    """The pipelines and bucket paths of the test. After a test that did not
    pass it prints what the harness knows; after every test it cleans up."""
    made = Scenario(request.node.name)
    yield made
    report = getattr(request.node, "report_call", None)
    passed = report is not None and report.passed
    if not passed:
        print(made.diagnostics(), file=sys.stderr)
    # A test that did not pass leaves its pipelines in an unknown state, so
    # no later test gets them.
    for failure in made.cleanup(reuse=passed):
        print(f"cleanup: {failure}", file=sys.stderr)


@enterprise_only
def test_push_gc_restore(scenario):
    """Pushes checkpoints until the remote GC removes one, then restores the
    newest checkpoint and an older one into empty local storage."""
    p = scenario.pipeline()
    # An empty bucket path holds no checkpoint: the pipeline starts from
    # nothing instead of failing.
    p.start(start_from="latest", retention_min_count=2)
    p.load()
    made = []
    for _ in range(3):
        p.round()
        p.checkpoint()
        made.append(p.push())
    c1, c2, c3 = made

    # The scenario must exercise what it claims to: checkpoints that share
    # batch files, and a collected checkpoint with files of its own.
    assert c2.batches & c3.batches, "consecutive checkpoints share no batch file"
    audit = p.bucket.audit()
    if not MULTIHOST:
        own = c1.batches - c2.batches - c3.batches
        assert len(own) >= 2, "the collected checkpoint had no batch files of its own"
        assert not own & audit.batch_files, (
            "the GC left files of the collected checkpoint"
        )
        assert c1.batches & c2.batches <= audit.batch_files, (
            "the GC removed files that a retained checkpoint shares"
        )

    # Restore the newest checkpoint; the restored pipeline keeps pushing.
    p.wipe()
    p.start(start_from="latest", strict=True, retention_min_count=2)
    p.restored(c3)
    p.checkpoint()
    c4 = p.push()

    if MULTIHOST:
        # Each host has its own checkpoint UUIDs, so a multihost pipeline
        # cannot name one checkpoint to start from.
        return

    # Restore by UUID: first the newest checkpoint, then the older one that
    # the bucket still holds.
    p.wipe()
    p.start(start_from=c4.uuid, strict=True, retention_min_count=2)
    p.restored(c4)
    p.wipe()
    p.start(start_from=c3.uuid, strict=True, retention_min_count=2)
    p.restored(c3)


@enterprise_only
@single_host_only
def test_gc_failure_and_recovery(scenario):
    """A remote GC that fails partway must leave no file that a later GC
    cannot find, and the next GC must finish the work.

    The GC finds checkpoints by their zips, so it must remove the batch
    files of a checkpoint before the zip. ``--max-delete 1`` lets every
    `rclone delete` remove one file and then fail, which stops the GC after
    its first deletion.
    """
    p = scenario.pipeline()
    p.start(retention_min_count=1)
    p.load()
    p.round()
    p.checkpoint()
    c1 = p.push()

    # The next push would collect `c1`; its GC stops after one deletion.
    p.stop()
    p.start(retention_min_count=1, flags=FLAGS_ONE_DELETE)
    p.round()
    p.checkpoint()
    before = p.bucket.audit()
    c2 = p.push(audit=False)
    after = p.bucket.audit()

    doomed = c1.batches - c2.batches
    assert len(doomed) >= 2, "the GC had fewer than two files to remove"
    removed = set(before.objects) - set(after.objects)
    assert len(removed) == 1 and removed <= doomed, (
        f"the failed GC removed {sorted(removed)}; expected one batch file of {c1.uuid}"
    )
    # The checkpoint is damaged but still listed, so a later GC finds it:
    # nothing is orphaned.
    assert c1.uuid in after.uuids(), (
        "the failed GC removed the zip before the batch files"
    )
    assert after.problems(allow_incomplete=[c1.uuid]) == []
    assert c2.uuid in after.restorable()

    # A pull of the half-collected checkpoint fails. It must not start the
    # pipeline from nothing, as it would for a checkpoint that is not there.
    reader = scenario.pipeline("reader", bucket=p.bucket)
    reader.expect_start_error(
        rf"checkpoint '{c1.uuid}' is incomplete in the bucket", start_from=c1.uuid
    )

    # Without the flag, the next push finishes the collection of `c1` and
    # collects `c2`. The audit of the push requires exactly that.
    p.stop()
    p.start(retention_min_count=1)
    p.round()
    p.checkpoint()
    c3 = p.push()
    assert p.bucket.audit().uuids() == {c3.uuid}

    p.wipe()
    p.start(start_from="latest", strict=True, retention_min_count=1)
    p.restored(c3)


@enterprise_only
@single_host_only
def test_gc_keeps_checkpoint_with_unreadable_dependencies(scenario):
    """The remote GC keeps the zip of a checkpoint whose batch files it
    cannot name, and collects the other checkpoints.

    Without the zip, nothing could find the batch files of that checkpoint
    again.
    """
    p = scenario.pipeline()
    p.start(retention_min_count=3)
    p.load()
    made = []
    for _ in range(3):
        p.round()
        p.checkpoint()
        made.append(p.push())
    c1, c2, c3 = made

    # Damage `c1`: its list of batch files is in the zip and in the
    # dependencies file, and now in neither.
    p.bucket.write(f"{c1.uuid}.zip", b"not a zip")
    p.bucket.delete(f"{DEPENDENCIES_DIR}/{c1.uuid}.json")

    # Local storage has a copy of the list, so start from the bucket instead.
    p.wipe()
    p.start(start_from="latest", strict=True, retention_min_count=1)
    p.restored(c3)
    p.checkpoint()
    c4 = p.push(audit=False)

    audit = p.bucket.audit()
    assert audit.uuids() == {c1.uuid, c4.uuid}, (
        f"expected the damaged checkpoint and the newest one, found {sorted(audit.uuids())}"
    )
    assert audit.unreadable() == {c1.uuid}
    # The files that only `c1` needs stay; the audit cannot attribute them,
    # because it cannot read `c1` either.
    assert (
        audit.problems(allow_unreadable=[c1.uuid], allow_orphan_batches=c1.batches)
        == []
    )
    assert c4.uuid in audit.restorable()

    # The damaged checkpoint does not stop later pushes.
    p.round()
    p.checkpoint()
    c5 = p.push(audit=False)
    assert p.bucket.audit().uuids() == {c1.uuid, c5.uuid}


@enterprise_only
@single_host_only
def test_gc_reads_dependencies_from_the_zip(scenario):
    """A checkpoint without a `dependencies/<uuid>.json` file, as older
    versions wrote them, is collected like any other: the remote GC reads
    the list of its batch files from the zip."""
    p = scenario.pipeline()
    p.start(retention_min_count=3)
    p.load()
    made = []
    for _ in range(3):
        p.round()
        p.checkpoint()
        made.append(p.push())
    for old in made[:2]:
        p.bucket.delete(f"{DEPENDENCIES_DIR}/{old.uuid}.json")

    # Local storage has copies of the lists, so start from the bucket.
    p.wipe()
    p.start(start_from="latest", strict=True, retention_min_count=1)
    p.restored(made[2])
    p.checkpoint()
    # The audit of the push requires the three old checkpoints to be gone
    # and no file of theirs to be left.
    newest = p.push()
    assert p.bucket.audit().uuids() == {newest.uuid}


@enterprise_only
@single_host_only
def test_gc_after_rollback(scenario):
    """A pipeline that goes back to an older checkpoint and carries on must
    not lose the checkpoints it pushes afterwards.

    After the rollback the bucket holds two branches of history. The newest
    checkpoint shares files with the checkpoint it came from, and not with
    the checkpoints of the abandoned branch in between.
    """
    p = scenario.pipeline()
    p.start(retention_min_count=3)
    p.load()
    made = []
    for _ in range(3):
        p.round()
        p.checkpoint()
        made.append(p.push())
    c1 = made[0]

    # Go back to the first checkpoint. Before new input arrives, the next
    # checkpoint consists of the files of `c1`, most of which the abandoned
    # branch no longer has. Its push collects `c1`; the audit of the push
    # requires the new checkpoint to be whole.
    p.wipe()
    p.start(start_from=c1.uuid, strict=True, retention_min_count=3)
    p.restored(c1, feed=False)
    p.checkpoint()
    c4 = p.push()
    assert c1.uuid not in p.bucket.audit().uuids(), "the push collected nothing"

    p.wipe()
    p.start(start_from="latest", strict=True, retention_min_count=3)
    p.restored(c4)


# ---------------------------------------------------------------------------
# Restores from a bucket that is built once
# ---------------------------------------------------------------------------


@pytest.fixture(scope="session")
def golden(run_shared_dir):
    """A bucket path with three checkpoints that tests restore from but never
    change. The whole test run shares one, whichever process needs it first
    builds it; see `golden_bucket`."""
    return golden_bucket(run_shared_dir, "golden", rounds_per_checkpoint=1)


@pytest.fixture(scope="session")
def other_golden(run_shared_dir):
    """A second bucket path with three checkpoints, whose data differs from
    `golden`, for tests that must tell which bucket a restore read."""
    return golden_bucket(run_shared_dir, "golden2", rounds_per_checkpoint=2)


def _uuid_cases(*cases):
    """Marks the cases that start from a UUID as single-host only: each host
    of a multihost pipeline has its own checkpoint UUIDs."""
    return [
        pytest.param(
            *case,
            marks=pytest.mark.skipif(
                MULTIHOST and case[0] == "uuid", reason="UUID restores are single-host"
            ),
        )
        for case in cases
    ]


def _selector(start_from: str, checkpoint: Checkpoint) -> str:
    return checkpoint.uuid if start_from == "uuid" else "latest"


@enterprise_only
@pytest.mark.parametrize(
    "start_from,standby,source",
    _uuid_cases(
        ("latest", True, "bucket"),
        ("uuid", True, "bucket"),
        ("latest", False, "read_bucket"),
        ("uuid", False, "read_bucket"),
        ("latest", True, "read_bucket"),
        ("uuid", True, "read_bucket"),
        ("latest", False, "bucket_over_read_bucket"),
        ("uuid", False, "bucket_over_read_bucket"),
    ),
)
def test_restore_from_remote(
    scenario, golden, other_golden, start_from, standby, source
):
    """Restores into a pipeline with empty local storage, from each remote
    source, by `latest` and by UUID, directly and through standby.

    * `bucket`: the checkpoint is in `bucket`.
    * `read_bucket`: `bucket` is empty, so the pipeline falls back to
      `read_bucket`.
    * `bucket_over_read_bucket`: both hold checkpoints, with different data;
      `bucket` wins.
    """
    # By UUID the test asks for the middle checkpoint, so that "the newest"
    # is not the answer by accident.
    wanted = golden.checkpoints[1] if start_from == "uuid" else golden.latest
    options = {"start_from": _selector(start_from, wanted), "strict": True}
    if source == "bucket":
        reader = scenario.pipeline("reader", bucket=golden.bucket)
    elif source == "read_bucket":
        reader = scenario.pipeline("reader", bucket=scenario.bucket("empty"))
        options["read_bucket"] = golden.bucket
    else:
        reader = scenario.pipeline("reader", bucket=golden.bucket)
        options["read_bucket"] = other_golden.bucket

    before = golden.bucket.objects()
    if standby:
        reader.start_standby(**options)
        reader.activate()
    else:
        reader.start(**options)
    reader.restored(wanted)
    reader.stop()
    assert golden.bucket.objects() == before, "a restore changed the bucket it read"


# What each kind of damage does to a restore. Every one must fail the start,
# also with `fail_if_no_checkpoint` off: the checkpoint is there, and starting
# from nothing instead would silently drop the pipeline's state.
RESTORE_FAILURES = {
    "bad_credentials": None,  # the pattern depends on the object store
    "missing_bucket": r"does not exist|directory not found|NoSuchBucket",
    "failed_download": r"max transfer limit reached",
    "incomplete_checkpoint": r"is incomplete in the bucket",
    "corrupt_zip": r"failed to unzip checkpoint",
    # The catalog names the latest checkpoint; a start from a UUID does not
    # read it.
    "corrupt_catalog": r"failed to parse catalog file",
    "empty_catalog": r"no checkpoints found",
}


@enterprise_only
@pytest.mark.parametrize(
    "start_from,damage",
    _uuid_cases(
        *[
            (s, d)
            for d in RESTORE_FAILURES
            for s in ("latest", "uuid")
            if not (s == "uuid" and d.endswith("_catalog"))
        ]
    ),
)
def test_restore_failure_does_not_start_fresh(scenario, golden, start_from, damage):
    """A restore that fails for any reason but "there is no such checkpoint"
    fails the start, with `fail_if_no_checkpoint` off."""
    wanted = golden.latest
    options: dict = {"start_from": _selector(start_from, wanted)}
    pattern = RESTORE_FAILURES[damage]
    bucket = golden.bucket
    if damage == "bad_credentials":
        options["auth_err"] = True
        pattern = ObjectStore.from_env().auth_error_pattern
    elif damage == "missing_bucket":
        store = ObjectStore.from_env()
        options["bucket_override"] = store.sync_bucket(
            f"no-such-bucket-{uuid4().hex[:12]}/checkpoints"
        )
    elif damage == "failed_download":
        options["flags"] = FLAGS_NO_TRANSFER
    else:
        # Damage a copy, not the bucket that other tests read.
        bucket = scenario.bucket("damaged")
        golden.bucket.clone_to(bucket)
        host = "host0/" if MULTIHOST else ""
        if damage == "incomplete_checkpoint":
            bucket.delete(sorted(wanted.batches)[0])
        elif damage == "corrupt_zip":
            zips = [
                r for r in bucket.objects() if r.startswith(host) and r.endswith(".zip")
            ]
            for rel in zips:
                bucket.write(rel, b"not a zip")
        elif damage == "corrupt_catalog":
            bucket.write(f"{host}{CATALOG_FILE}", b"not a catalog")
        else:
            bucket.write(f"{host}{CATALOG_FILE}", b"[]")

    reader = scenario.pipeline("reader", bucket=bucket)
    reader.expect_start_error(pattern, **options)


@enterprise_only
@pytest.mark.parametrize(
    "start_from,strict",
    _uuid_cases(("latest", False), ("latest", True), ("uuid", False), ("uuid", True)),
)
def test_restore_without_a_checkpoint(scenario, golden, start_from, strict):
    """With no such checkpoint in the bucket, `fail_if_no_checkpoint` decides:
    the start fails, or the pipeline starts from nothing."""
    options: dict = {}
    if start_from == "uuid":
        # An existing bucket that lacks this checkpoint.
        reader = scenario.pipeline("reader", bucket=golden.bucket)
        selector = str(uuid4())
    else:
        # A bucket path with nothing in it, and a `read_bucket` to fall back
        # to that has nothing either.
        reader = scenario.pipeline("reader")
        selector = "latest"
        options["read_bucket"] = scenario.bucket("read")

    if strict:
        reader.expect_start_error(
            r"not found", start_from=selector, strict=True, **options
        )
        return
    reader.start(start_from=selector, **options)
    assert reader.query_count() == 0
    # A pipeline that started from nothing works: it takes input.
    reader.load()
    reader.round()


# ---------------------------------------------------------------------------
# Local checkpoints
# ---------------------------------------------------------------------------


@enterprise_only
def test_local_checkpoint_wins(scenario, other_golden):
    """A checkpoint in local storage is used before any remote one: before
    `read_bucket` when `bucket` is empty, and before an older checkpoint in
    `bucket`. With a wrong order the pipeline would come up with another
    pipeline's data, or with older data than it has."""
    p = scenario.pipeline()
    p.start()
    p.load()
    p.round()
    local_1 = p.checkpoint()

    # `bucket` is empty and `read_bucket` holds other data: local wins.
    p.stop()
    p.start(start_from="latest", read_bucket=other_golden.bucket)
    p.restored(local_1)
    if not MULTIHOST:
        p.stop()
        p.start(start_from=local_1.uuid, read_bucket=other_golden.bucket)
        p.restored(local_1)

    # Push a checkpoint, then make a newer one that stays local: the newer,
    # local one wins over the one in `bucket`.
    p.checkpoint()
    p.push()
    p.round()
    local_2 = p.checkpoint()
    p.stop()
    p.start(start_from="latest")
    p.restored(local_2)
    if not MULTIHOST:
        p.stop()
        p.start(start_from=local_2.uuid)
        p.restored(local_2)


# ---------------------------------------------------------------------------
# Standby
# ---------------------------------------------------------------------------


@enterprise_only
@pytest.mark.parametrize("start_from", _uuid_cases(("latest",), ("uuid",)))
def test_standby_failover(scenario, start_from):
    """A standby pipeline follows the pushes of a primary and replaces it.

    With `latest`, the standby comes up with the last checkpoint the primary
    pushed, although the primary's GC removed older ones meanwhile. With a
    UUID it comes up with that checkpoint, whatever was pushed later. Either
    way it takes the bucket over, and the old primary can no longer push.
    """
    primary = scenario.pipeline("primary")
    # By UUID, the checkpoint the standby asks for must survive the GC.
    retention = 2 if start_from == "latest" else 10
    primary.start(retention_min_count=retention)
    primary.load()
    primary.round()
    primary.checkpoint()
    first = primary.push()

    standby = scenario.pipeline("standby", bucket=primary.bucket)
    standby.start_standby(
        start_from=_selector(start_from, first),
        pull_interval="1s",
        take_bucket_ownership=True,
        retention_min_count=retention,
    )
    for _ in range(3):
        primary.round()
        primary.checkpoint()
        last = primary.push()
    primary.stop()

    standby.activate()
    standby.restored(last if start_from == "latest" else first)

    # The standby is the primary now: it owns the bucket and pushes to it.
    standby.checkpoint()
    standby.push(audit=False)
    audit = standby.bucket.audit()
    assert audit.problems() == []
    assert audit.owner["pipeline_name"] == f"pipeline-{standby.pipeline.id()}"

    # A restart does not go back to standby. With `latest` it resumes from
    # the pipeline's own newest checkpoint; a UUID names the checkpoint to
    # start from on every start, so the pipeline returns to it.
    resumed = standby.checkpoints[-1]
    standby.stop()
    standby.start(
        start_from=_selector(start_from, first), retention_min_count=retention
    )
    standby.restored(resumed if start_from == "latest" else first)

    # The old primary lost the bucket while it was down.
    primary.start(retention_min_count=retention)
    primary.round()
    primary.checkpoint()
    primary.expect_push_error(
        r"already owned by[\s\S]*Refusing to write checkpoint data"
    )


@enterprise_only
def test_standby_moves_from_read_bucket_to_bucket(scenario, golden):
    """A standby whose `bucket` is still empty starts from `read_bucket`,
    and follows `bucket` once the primary pushes to it."""
    primary = scenario.pipeline("primary")
    primary.start()
    primary.load()

    standby = scenario.pipeline("standby", bucket=primary.bucket)
    # The first pull can only come from `read_bucket`.
    standby.start_standby(
        start_from="latest", pull_interval="1s", read_bucket=golden.bucket
    )

    for _ in range(2):
        primary.round()
        primary.checkpoint()
        last = primary.push()
    primary.stop()

    standby.activate()
    standby.restored(last)


# ---------------------------------------------------------------------------
# Bucket ownership
# ---------------------------------------------------------------------------

REFUSED = r"already owned by[\s\S]*Refusing to write checkpoint data"
TAKEN_OVER = (
    r"was taken over by[\s\S]*while this pipeline[\s\S]*was running"
    r"[\s\S]*Refusing to write checkpoint data"
)


@enterprise_only
def test_bucket_ownership(scenario):
    """The first pipeline to push owns the bucket path. Another pipeline's
    push is refused before it writes anything, and so is every push while
    `owner.json` is unreadable."""
    owner = scenario.pipeline("owner")
    owner.start()
    owner.load()
    owner.round()
    owner.checkpoint()
    owner.push()

    intruder = scenario.pipeline("intruder", bucket=owner.bucket)
    intruder.start()
    intruder.load()
    intruder.round()
    intruder.checkpoint()
    intruder.expect_push_error(REFUSED)
    intruder.stop()

    # The refused push did not disturb the owner.
    owner.round()
    owner.checkpoint()
    owner.push()

    # An ownership file that cannot be read is no license to take the bucket.
    saved = owner.bucket.read(OWNER_FILE)
    owner.round()
    owner.checkpoint()
    for garbled in (b"", b"{not json"):
        owner.bucket.write(OWNER_FILE, garbled)
        owner.expect_push_error(r"owner\.json[\s\S]*malformed")
    owner.bucket.write(OWNER_FILE, saved)
    owner.push()

    # Without an ownership file the bucket is free; the next push claims it.
    owner.bucket.delete(OWNER_FILE)
    owner.round()
    owner.checkpoint()
    owner.push()

    # A rename changes the name users see, not the identity that owns the
    # bucket: the renamed pipeline restores from the bucket and pushes to it.
    newest = owner.checkpoints[-1]
    owner.wipe()
    owner.rename(scenario.name("renamed"))
    owner.start(start_from="latest", strict=True)
    owner.restored(newest)
    owner.checkpoint()
    owner.push()


@enterprise_only
def test_bucket_takeover_at_startup_only(scenario):
    """`take_bucket_ownership` takes the bucket of another pipeline, but only
    as the pipeline starts. A running pipeline that loses the bucket fails
    its next push, even if it set the flag itself."""
    first = scenario.pipeline("first")
    second = scenario.pipeline("second", bucket=first.bucket)
    # The two pipelines push unrelated checkpoints to one bucket in turn.
    # Keep them all: what the remote GC does with such a bucket is the
    # subject of `test_gc_after_rollback`.
    keep_all = {"retention_min_count": 100}

    def owner() -> str:
        return first.bucket.audit().owner["pipeline_name"]

    def push(pipeline) -> None:
        pipeline.round()
        pipeline.checkpoint()
        # Two pipelines push to this bucket in turn, so the audit of a push,
        # which expects one pipeline's checkpoints, does not apply.
        pipeline.push(audit=False)
        assert first.bucket.audit().problems() == []

    # The bucket has no owner yet; `first` takes it as it starts, not only
    # with its first push.
    first.start(take_bucket_ownership=True, **keep_all)
    assert owner() == f"pipeline-{first.pipeline.id()}"
    first.load()
    push(first)

    # `second` takes the bucket as it starts, before it pushes anything.
    second.start(take_bucket_ownership=True, **keep_all)
    assert owner() == f"pipeline-{second.pipeline.id()}"
    second.load()
    push(second)

    # `first` lost the bucket while it was running. It set the flag when it
    # started, so the error says what happened and what to do.
    first.round()
    first.checkpoint()
    first.expect_push_error(TAKEN_OVER)

    # Restarted with the flag, `first` takes the bucket back...
    first.stop()
    first.start(take_bucket_ownership=True, **keep_all)
    assert owner() == f"pipeline-{first.pipeline.id()}"
    push(first)

    # ...and `second`, which set the flag when it started, does not take it
    # back by pushing.
    second.round()
    second.checkpoint()
    second.expect_push_error(TAKEN_OVER)

    # An ownership file that cannot be read stops a push, but not a
    # takeover: that is what the flag is for.
    first.bucket.write(OWNER_FILE, b"{not json")
    second.stop()
    second.start(take_bucket_ownership=True, **keep_all)
    assert owner() == f"pipeline-{second.pipeline.id()}"


# ---------------------------------------------------------------------------
# Automatic checkpoints and pushes, retention by age
# ---------------------------------------------------------------------------


@enterprise_only
@pytest.mark.parametrize(
    "checkpoint_interval,push_interval",
    [
        ("2s", None),
        # The push timer is longer than the time to the first checkpoint,
        # so the pipeline is idle when the timer expires and must wake up
        # for the push by itself.
        (NO_AUTOMATIC_CHECKPOINTS, "20s"),
        ("2s", "2s"),
    ],
    ids=["automatic_checkpoint", "automatic_push", "both_automatic"],
)
def test_automatic_checkpoint_and_push(scenario, checkpoint_interval, push_interval):
    """Checkpoints and pushes that the pipeline makes by itself restore like
    the ones a user asks for, in every combination of the two."""
    p = scenario.pipeline()
    p.start(checkpoint_interval=checkpoint_interval, push_interval=push_interval)
    p.load()
    p.round()
    if checkpoint_interval == NO_AUTOMATIC_CHECKPOINTS:
        made = p.checkpoint()
    else:
        made = p.wait_for_automatic_checkpoint()
    if push_interval is None:
        p.push(audit=False)
    else:
        p.wait_for_automatic_push(made)

    # An audit needs a bucket at rest, and with both timers on the pipeline
    # never stops pushing. So stop it first. The stop can cut a remote GC
    # short, which leaves a checkpoint without some of its files, but never
    # the checkpoint that the catalog names. It cannot leave a file that
    # nothing refers to: no input arrived after `made`, so a later checkpoint
    # needs no batch file that the bucket lacks.
    p.stop()
    audit = p.bucket.audit()
    assert audit.latest() and audit.latest() <= audit.restorable(), audit.summary()
    assert (
        audit.problems(
            allow_incomplete=audit.uuids() - audit.latest(),
            allow_orphan_dependencies=audit.orphan_dependencies,
        )
        == []
    )

    # No input arrived after `made`, so whichever checkpoint is the newest in
    # the bucket by now, it holds the state of `made`.
    p.wipe()
    p.start(start_from="latest", strict=True)
    p.restored(made)


@enterprise_only
@single_host_only
def test_retention_by_age(scenario):
    """`min_retention` protects a young checkpoint from the remote GC, also
    beyond `retention_min_count`, and no longer once the checkpoint is old
    enough.

    The test does not race a checkpoint against the limit, which a slow
    machine would lose: first the limit is far longer than the test, then it
    is shorter than the age that the checkpoints have by then.
    """
    p = scenario.pipeline()
    # By count alone, each push removes the checkpoint before it. The audit
    # of each push requires every checkpoint that is surely too young to stay.
    p.start(retention_min_count=1, min_retention="3600s")
    p.load()
    young = []
    for _ in range(3):
        p.round()
        p.checkpoint()
        young.append(p.push())
    assert p.bucket.audit().uuids() == {c.uuid for c in young}

    # The limit is a setting, so a restart changes it. Once the three
    # checkpoints are surely older than the new limit, the next push must
    # remove them; the audit of the push requires that, too.
    min_retention_s = 5
    p.stop()
    p.start(retention_min_count=1, min_retention=f"{min_retention_s}s")
    p.restored(young[-1])
    time.sleep(max(0.0, young[-1].created + min_retention_s + 4 - time.time()))
    p.checkpoint()
    newest = p.push()
    assert p.bucket.audit().uuids() == {newest.uuid}


# ---------------------------------------------------------------------------
# Failed pushes, user flags
# ---------------------------------------------------------------------------


@enterprise_only
@pytest.mark.parametrize("damage", ["bad_credentials", "missing_bucket"])
def test_push_failure(scenario, damage):
    """A push that cannot reach its bucket fails, reports why, and leaves
    the pipeline able to push once the configuration is right."""
    p = scenario.pipeline()
    if damage == "bad_credentials":
        options: dict = {"auth_err": True}
        pattern = ObjectStore.from_env().auth_error_pattern
    else:
        missing = f"no-such-bucket-{uuid4().hex[:12]}"
        options = {
            "bucket_override": ObjectStore.from_env().sync_bucket(
                f"{missing}/checkpoints"
            )
        }
        pattern = rf"{missing}[\s\S]*does not exist"
    p.start(**options)
    p.load()
    p.round()
    made = p.checkpoint()
    p.expect_push_error(pattern)
    status = p.sync_status()
    assert (status.get("failure") or {}).get("uuid") == made.uuid, status
    assert status.get("success") is None and status.get("periodic") is None, status

    p.stop()
    p.start()
    p.restored(made)
    p.checkpoint()
    p.push()


@enterprise_only
@pytest.mark.parametrize("case", ["no_head_object", "credentials_in_flags"])
def test_user_flags(scenario, case):
    """The rclone flags of `SyncConfig.flags` reach every rclone command.

    * `no_head_object`: a flag users set to save requests. It must not
      break the lookups that tell a missing object from a present one.
    * `credentials_in_flags`: the configured secret key is wrong and the
      right one is a flag, so each rclone command works only if it got
      the flags.
    """
    store = ObjectStore.from_env()
    if store.scheme != "s3":
        pytest.skip("the flags of this test are S3 flags")
    if case == "no_head_object":
        options: dict = {"flags": ["--s3-no-head-object"]}
    else:
        options = {
            "auth_err": True,
            "flags": ["--s3-secret-access-key", store.secret_key],
        }

    p = scenario.pipeline()
    p.start(retention_min_count=1, **options)
    p.load()
    for _ in range(2):
        p.round()
        p.checkpoint()
        newest = p.push()

    # The ownership check reads `owner.json` with the flags, too.
    intruder = scenario.pipeline("intruder", bucket=p.bucket)
    intruder.start(**options)
    intruder.load()
    intruder.round()
    intruder.checkpoint()
    intruder.expect_push_error(REFUSED)
    intruder.stop()

    p.wipe()
    p.start(start_from="latest", strict=True, retention_min_count=1, **options)
    p.restored(newest)
    if not MULTIHOST:
        p.wipe()
        p.start(start_from=newest.uuid, strict=True, retention_min_count=1, **options)
        p.restored(newest)
