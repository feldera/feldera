import json
import logging
import platform
import random
import sys
import time
import warnings
from typing import Optional
from uuid import UUID, uuid4

from feldera import Pipeline
from feldera.enums import FaultToleranceModel, PipelineStatus
from feldera.runtime_config import RuntimeConfig, Storage
from feldera.testutils import (
    FELDERA_TEST_NUM_HOSTS,
    FELDERA_TEST_NUM_WORKERS,
    single_host_only,
)
from tests import enterprise_only
from tests.shared_test_pipeline import SharedTestPipeline
from tests.utils import ObjectStore

from .helper import wait_for_condition


LOGGER = logging.getLogger(__name__)

# An object store endpoint that cannot be reached: port 1 on the pipeline's
# own loopback, where nothing listens.  A sync pointed here stays in progress
# for as long as the test needs, because the sync retries a connection failure
# rather than giving up on it.  A sync against a working endpoint, by contrast,
# can finish before the first status request arrives, leaving no window to
# observe.
#
# The window comes from that retrying, not from how the network treats the
# address, so this holds wherever the tests run.  An unroutable address (say
# TEST-NET-1) would rely on the network dropping the packets rather than
# rejecting them; loopback never leaves the pod and is refused immediately
# everywhere, which the retry loop then rides out just the same.
UNREACHABLE_ENDPOINT = "http://127.0.0.1:1"

# Extra time, beyond the time to pull the extra checkpoints
# (pull_interval * extra_ckpts), that a standby gets to reach RUNNING after
# activate().  Activation waits for three pulls in a row: the standby
# pull in flight on the slowest host, a final pull before activation, and a
# non-standby pull.  On a slow node a single pull can take 30-40s, so allow
# several pull round-trips.
ACTIVATE_PULL_BUDGET_S = 180

_CHECKPOINT_SYNC_BUCKET_ARCH = platform.machine().lower()


def checkpoint_sync_bucket(pipeline_name: str) -> str:
    """``<bucket>/<prefix>`` a pipeline syncs its checkpoints to."""
    return f"{ObjectStore.from_env().bucket}/{_CHECKPOINT_SYNC_BUCKET_ARCH}/{pipeline_name}"


def checkpoint_sync_owner(bucket_name: str) -> Optional[dict]:
    """Read the checkpoint-sync owner file if Python can reach the object store."""
    try:
        fs = ObjectStore.from_env().pyarrow_fs()
        owner_path = f"{checkpoint_sync_bucket(bucket_name)}/owner.json"
        info = fs.get_file_info(owner_path)
    except Exception as e:
        print(
            f"object store not accessible from the Python SDK; skipping owner.json check: {e}",
            file=sys.stderr,
        )
        return None

    if not info.is_file:
        raise AssertionError(f"checkpoint ownership file not found: {owner_path}")

    with fs.open_input_file(owner_path) as f:
        owner = json.loads(f.read().decode("utf-8"))

    LOGGER.debug(
        "read checkpoint ownership file from S3: bucket=%s path=%s pipeline_name=%s",
        checkpoint_sync_bucket(bucket_name),
        owner_path,
        owner.get("pipeline_name"),
    )
    return owner


def storage_cfg(
    pipeline_name: str,
    endpoint: Optional[str] = None,
    start_from_checkpoint: Optional[str] = None,
    strict: bool = False,
    auth_err: bool = False,
    standby: bool = False,
    pull_interval: int = 2,
    push_interval: Optional[int] = None,
    retention_min_count: int = 1,
    retention_min_age: int = 0,
    read_bucket: Optional[str] = None,
    take_bucket_ownership: bool = False,
) -> dict:
    if standby:
        warnings.warn(
            "The 'standby' storage config field is deprecated. "
            "Use pipeline.start_standby() instead.",
            DeprecationWarning,
            stacklevel=2,
        )

    # The store (and any credentials it needs) is resolved here, not at
    # import time, so collection does not blow up where the env is unset.
    store = ObjectStore.from_env()
    sync: dict = store.checkpoint_sync_connection(
        checkpoint_sync_bucket(pipeline_name), auth_err=auth_err, endpoint=endpoint
    )
    sync.update(
        {
            "start_from_checkpoint": start_from_checkpoint,
            "fail_if_no_checkpoint": strict,
            "standby_pull_interval": f"{pull_interval}s",
            "checkpoint_push_interval": None
            if push_interval is None
            else f"{push_interval}s",
            "retention_min_count": retention_min_count,
            "min_retention": f"{retention_min_age}d",
        }
    )
    if read_bucket is not None:
        sync["read_bucket"] = store.sync_bucket(read_bucket)
    if take_bucket_ownership:
        sync["take_bucket_ownership"] = True
    return {
        "backend": {
            "name": "file",
            "config": {
                "sync": sync,
            },
        }
    }


class TestCheckpointSync(SharedTestPipeline):
    def _wait_for_standby_checkpoint_pull(self, pipeline, timeout_s: float = 120):
        """Block until the standby pipeline reports its first checkpoint pull.

        A pipeline in standby has no controller yet, so neither /metrics nor
        /checkpoints can report pull progress; the log line is the only signal.
        The pipeline logs it again on every pull, so an evicted line recurs.
        """
        end = time.monotonic() + timeout_s
        for log in pipeline.logs():
            if "checkpoint pulled successfully" in log:
                return
            if time.monotonic() > end:
                raise TimeoutError(
                    f"{pipeline.name} timed out waiting to pull checkpoint"
                )
        raise RuntimeError(
            f"{pipeline.name} log stream ended before it pulled a checkpoint"
        )

    def _configure_and_start(
        self,
        ft_interval: int = 60,
        push_interval: Optional[int] = None,
        retention_min_age: int = 0,
    ):
        """Configure the pipeline with AtLeastOnce FT and start it."""
        storage_config = storage_cfg(
            self.pipeline.name,
            push_interval=push_interval,
            retention_min_count=1,
            retention_min_age=retention_min_age,
        )
        self.pipeline.set_runtime_config(
            RuntimeConfig(
                workers=FELDERA_TEST_NUM_WORKERS,
                hosts=FELDERA_TEST_NUM_HOSTS,
                fault_tolerance_model=FaultToleranceModel.AtLeastOnce,
                storage=Storage(config=storage_config),
                checkpoint_interval=f"{ft_interval}s",
            )
        )
        self.pipeline.start()

    def _insert_data_and_wait(self):
        """Insert random rows and block until the pipeline processes them all.

        Returns (processed, got_before) where got_before is the view snapshot.
        """
        before = self.pipeline.stats().global_metrics.total_processed_records

        random.seed(time.time())
        total = random.randint(10, 20)
        data = [{"c0": i, "c1": str(i)} for i in range(1, total)]
        self.pipeline.input_json("t0", data)
        self.pipeline.execute("INSERT INTO t0 VALUES (21, 'exists')")

        start = time.monotonic()
        timeout = 5
        while True:
            processed = self.pipeline.stats().global_metrics.total_processed_records
            if processed == before + total:
                break
            if time.monotonic() - start > timeout:
                raise TimeoutError(
                    f"timed out while waiting for pipeline to process {total} records"
                )
            time.sleep(0.1)

        # The processed counter updates before the step's output becomes
        # visible to ad-hoc queries; retry briefly instead of comparing a
        # single racy snapshot.
        start = time.monotonic()
        while True:
            got_before = list(self.pipeline.query("SELECT * FROM v0"))
            if len(got_before) == processed:
                break
            if time.monotonic() - start > 10:
                raise RuntimeError(
                    f"adhoc query returned {len(got_before)} but {processed} records "
                    f"were processed: {got_before}"
                )
            time.sleep(0.2)
        print(f"{self.pipeline.name}: records: {total}, {got_before}", file=sys.stderr)

        return processed, got_before

    def _latest_checkpoint(self, processed=None, timeout_s=10.0):
        """Return (chk_uuid, chk_steps) for the most recently created checkpoint.

        If *processed* is given, polls until the latest checkpoint covers at
        least that many records (summed across pods for multihost pipelines),
        up to *timeout_s* seconds.  Use after checkpoint(wait=True).
        """
        chk_uuid = None
        chk_steps = None

        def checkpoint_visible() -> bool:
            nonlocal chk_uuid, chk_steps
            chks = self.pipeline.checkpoints()
            if not chks:
                return False
            latest = max(chks, key=lambda c: c.steps)
            if processed is not None:
                step_total = sum(
                    c.processed_records for c in chks if c.steps == latest.steps
                )
                if step_total < processed:
                    return False
            chk_uuid = latest.uuid
            chk_steps = latest.steps
            return True

        wait_for_condition(
            "latest checkpoint visible with sufficient processed records",
            checkpoint_visible,
            timeout_s=timeout_s,
            poll_interval_s=0.5,
        )
        return chk_uuid, chk_steps

    def _checkpoint_steps(self, synced_uuid) -> Optional[int]:
        """Return the step count for *synced_uuid*, or None if not found."""
        return next(
            (
                c.steps
                for c in self.pipeline.checkpoints()
                if str(c.uuid) == str(synced_uuid)
            ),
            None,
        )

    def _sync_and_verify(self, chk_uuid=None, chk_steps=None):
        """Trigger a manual sync and assert it covers *chk_uuid*. Returns the synced UUID."""
        uuid = self.pipeline.sync_checkpoint(wait=True)
        print("Synced Checkpoint UUID:", uuid, file=sys.stderr)
        if chk_uuid is not None:
            s_steps = self._checkpoint_steps(uuid)
            if chk_steps is not None and s_steps is not None:
                assert s_steps >= chk_steps
            else:
                assert UUID(str(uuid)) >= UUID(str(chk_uuid))
        return uuid

    def _restart_from_checkpoint(
        self,
        start_from,
        ft_interval: int = 60,
        auth_err: bool = False,
        strict: bool = False,
        standby: bool = False,
        clear_storage: bool = True,
    ):
        """Stop the running pipeline and restart it from *start_from*."""
        self.pipeline.stop(force=True)
        if clear_storage:
            self.pipeline.clear_storage()

        storage_config = storage_cfg(
            pipeline_name=self.pipeline.name,
            start_from_checkpoint=start_from,
            auth_err=auth_err,
            strict=strict,
        )
        self.pipeline.set_runtime_config(
            RuntimeConfig(
                workers=FELDERA_TEST_NUM_WORKERS,
                hosts=FELDERA_TEST_NUM_HOSTS,
                fault_tolerance_model=FaultToleranceModel.AtLeastOnce,
                storage=Storage(config=storage_config),
                checkpoint_interval=f"{ft_interval}s",
            )
        )

        if not standby:
            self.pipeline.start()
        else:
            self.pipeline.start_standby()
            self._wait_for_standby_checkpoint_pull(self.pipeline, timeout_s=120)
            assert self.pipeline.status() == PipelineStatus.STANDBY
            self.pipeline.activate()

    def _sync_status(self) -> dict:
        """Return the raw `/checkpoint/sync_status` response.

        The SDK's `sync_checkpoint_status` collapses the response into a
        `CheckpointStatus`, which hides `running`.
        """
        return self.pipeline.client.sync_checkpoint_status(self.pipeline.name)

    def _drain_sync(self, uuid, timeout_s: float = 120.0) -> tuple[dict, bool]:
        """Poll `/checkpoint/sync_status` until the sync of *uuid* finishes.

        Asserts on every sample that *uuid* is visible, that is, that it is
        either in `running` or recorded in `success`/`failure`.  The pipeline
        inserts *uuid* into `running` before answering the sync request, and
        moves it from `running` to `success`/`failure` under a single lock, so
        a caller that polls after a sync request can never see the UUID
        disappear.  With more than one host, the coordinator provides this
        guarantee: it answers the sync request and `/checkpoint/sync_status`
        from its own status, keyed by host 0's UUID, instead of merging the
        hosts' statuses.

        `running` alone decides when the sync is over.  `success` and `failure`
        are sticky records of the last sync to reach each outcome, so re-syncing
        a UUID that already failed starts out matching `running` and `failure`
        at once; reading the sticky record as completion would report the new
        sync as finished before it began.

        Returns the final status, and whether the poll caught the sync in
        `running`: a sync that ends within one status round-trip is already over
        by the first sample.
        """
        saw_running = False
        end = time.monotonic() + timeout_s
        while True:
            status = self._sync_status()
            if uuid in (status.get("running") or []):
                saw_running = True
            else:
                terminal = (
                    status.get("success"),
                    (status.get("failure") or {}).get("uuid"),
                )
                assert uuid in terminal, (
                    f"sync '{uuid}' is neither running nor finished: {status}"
                )
                return status, saw_running

            if time.monotonic() > end:
                raise TimeoutError(
                    f"timed out after {timeout_s}s waiting for sync '{uuid}': {status}"
                )
            time.sleep(0.05)

    # =========================================================================
    # Tests
    # =========================================================================

    @enterprise_only
    def test_autherr_fail(self):
        self._configure_and_start()
        self._insert_data_and_wait()
        self.pipeline.checkpoint(wait=True)
        time.sleep(1)
        self._sync_and_verify()

        with self.assertRaisesRegex(
            RuntimeError, ObjectStore.from_env().auth_error_pattern
        ):
            self._restart_from_checkpoint("latest", auth_err=True, strict=True)

    @enterprise_only
    def test_take_bucket_ownership_at_startup_only(self):
        # `take_bucket_ownership` lets a pipeline take over a bucket that
        # another pipeline owns, but only as it starts. Pushes never take
        # ownership, so a pipeline that loses the bucket while it is running
        # fails its next push, even if it set the flag itself.
        ft = FaultToleranceModel.AtLeastOnce

        first = self.new_pipeline_with_suffix("first")
        second = self.new_pipeline_with_suffix("second")
        bucket_name = first.name

        def configure(pipeline: Pipeline, take_bucket_ownership: bool):
            pipeline.set_runtime_config(
                RuntimeConfig(
                    workers=FELDERA_TEST_NUM_WORKERS,
                    hosts=FELDERA_TEST_NUM_HOSTS,
                    fault_tolerance_model=ft,
                    storage=Storage(
                        config=storage_cfg(
                            bucket_name, take_bucket_ownership=take_bucket_ownership
                        )
                    ),
                )
            )

        def assert_owner(pipeline: Pipeline):
            owner = checkpoint_sync_owner(bucket_name)
            if owner is not None:
                self.assertEqual(owner["pipeline_name"], f"pipeline-{pipeline.id()}")

        def push(pipeline: Pipeline, start: int):
            pipeline.input_json(
                "t0",
                [
                    {"c0": i, "c1": f"{pipeline.name}_{i}"}
                    for i in range(start, start + 5)
                ],
            )
            pipeline.wait_for_completion()
            pipeline.checkpoint(wait=True)
            pipeline.sync_checkpoint(wait=True)

        # `first` claims the bucket with its first push.
        configure(first, take_bucket_ownership=False)
        first.start()
        push(first, 1)
        assert_owner(first)

        # `second` takes the bucket over as it starts, before pushing anything.
        configure(second, take_bucket_ownership=True)
        second.start()
        assert_owner(second)
        push(second, 6)
        assert_owner(second)

        # `first` lost the bucket while running, so its pushes now fail.
        with self.assertRaisesRegex(
            RuntimeError,
            "S3 checkpoint bucket is already owned by [\\s\\S]*"
            "Refusing to write checkpoint data",
        ):
            push(first, 11)
        assert_owner(second)

        # Restarted with the flag, `first` takes the bucket back as it starts...
        first.stop(force=True)
        configure(first, take_bucket_ownership=True)
        first.start()
        assert_owner(first)
        push(first, 16)

        # ...and `second`, which set the flag but is now running, fails its
        # next push instead of taking the bucket back.
        with self.assertRaisesRegex(
            RuntimeError,
            "S3 checkpoint bucket was taken over by [\\s\\S]*"
            "while this pipeline [\\s\\S]*was running[\\s\\S]*"
            "Refusing to write checkpoint data",
        ):
            push(second, 21)
        assert_owner(first)

        second.stop(force=True)
        first.stop(force=True)
        second.clear_storage()
        first.clear_storage()

    @enterprise_only
    @single_host_only
    def test_nonexistent_checkpoint(self):
        self._configure_and_start()
        self._insert_data_and_wait()
        self.pipeline.checkpoint(wait=True)
        time.sleep(1)
        self._sync_and_verify()

        self._restart_from_checkpoint(uuid4(), strict=False)

        got_after = list(self.pipeline.query("SELECT * FROM v0"))
        self.assertEqual(got_after, [])  # pipeline started fresh: no data
        self.pipeline.stop(force=True)
        self.pipeline.clear_storage()

    @enterprise_only
    def test_automated_sync_auth_error(self):
        """
        CREATE TABLE t0 (c0 INT, c1 VARCHAR);
        CREATE MATERIALIZED VIEW v0 AS SELECT * FROM t0;
        """
        # Configure pipeline with automatic periodic sync and bad credentials.
        # The periodic sync should fail, and `periodic` in the sync status
        # should remain None (not report a UUID for a failed sync).
        storage_config = storage_cfg(
            self.pipeline.name,
            push_interval=5,
            auth_err=True,
        )
        self.pipeline.set_runtime_config(
            RuntimeConfig(
                workers=FELDERA_TEST_NUM_WORKERS,
                hosts=FELDERA_TEST_NUM_HOSTS,
                fault_tolerance_model=FaultToleranceModel.AtLeastOnce,
                storage=Storage(config=storage_config),
                checkpoint_interval="5s",
            )
        )
        self.pipeline.start()

        # Insert data so the pipeline has something to checkpoint.
        data = [{"c0": i, "c1": str(i)} for i in range(1, 10)]
        self.pipeline.input_json("t0", data)
        self.pipeline.wait_for_completion()

        # Wait for a checkpoint to be created.
        def checkpoint_exists() -> bool:
            return len(self.pipeline.checkpoints()) > 0

        wait_for_condition(
            "checkpoint is created",
            checkpoint_exists,
            timeout_s=30.0,
            poll_interval_s=0.5,
        )

        # Wait long enough for the periodic sync to have attempted (and failed).
        time.sleep(15)

        # The periodic sync should have failed due to bad credentials.
        # `periodic` must be None — a failed sync should not report a UUID.
        status = self.pipeline.client.sync_checkpoint_status(self.pipeline.name)
        print(f"sync_status after failed periodic sync: {status}", file=sys.stderr)
        self.assertIsNone(
            status.get("periodic"),
            f"periodic should be None after failed sync, got: {status}",
        )

        self.pipeline.stop(force=True)
        self.pipeline.clear_storage()

    @enterprise_only
    def test_sync_status_in_flight(self):
        """
        CREATE TABLE t0 (c0 INT, c1 VARCHAR);
        CREATE MATERIALIZED VIEW v0 AS SELECT * FROM t0;
        """
        # The sync cannot finish against an endpoint nothing answers on, so the
        # window in which it is running is as wide as the test needs.  Without
        # that, a sync can start and finish between the 202 and the first
        # status request, and an implementation that never populated `running`
        # would be indistinguishable from one that does.
        self.pipeline.set_runtime_config(
            RuntimeConfig(
                workers=FELDERA_TEST_NUM_WORKERS,
                hosts=FELDERA_TEST_NUM_HOSTS,
                fault_tolerance_model=FaultToleranceModel.AtLeastOnce,
                storage=Storage(
                    config=storage_cfg(
                        self.pipeline.name, endpoint=UNREACHABLE_ENDPOINT
                    )
                ),
                checkpoint_interval="60s",
            )
        )
        self.pipeline.start()
        processed, _ = self._insert_data_and_wait()
        self.pipeline.checkpoint(wait=True)
        chk_uuid, _ = self._latest_checkpoint(processed)

        uuid = self.pipeline.sync_checkpoint()
        self.assertEqual(str(uuid), str(chk_uuid))

        def sync_is_running() -> bool:
            return uuid in (self._sync_status().get("running") or [])

        wait_for_condition(
            f"checkpoint sync '{uuid}' is reported as running",
            sync_is_running,
            timeout_s=30.0,
            poll_interval_s=0.2,
        )

        # Requesting the same checkpoint again while it is syncing coalesces
        # with the sync in progress: same UUID back, and still one entry.
        self.assertEqual(self.pipeline.sync_checkpoint(), uuid)
        status = self._sync_status()
        self.assertEqual(
            status.get("running"),
            [uuid],
            f"a duplicate request must not add a second entry: {status}",
        )
        self.assertIsNone(
            status.get("success"), f"nothing can have succeeded yet: {status}"
        )

        # Stopping while the sync is still in flight exercises the guard's
        # release-on-drop path; the pipeline must stop rather than wait it out.
        self.pipeline.stop(force=True)
        self.pipeline.clear_storage()

    @enterprise_only
    def test_sync_status_running_after_failure(self):
        """
        CREATE TABLE t0 (c0 INT, c1 VARCHAR);
        CREATE MATERIALIZED VIEW v0 AS SELECT * FROM t0;
        """
        # Bad credentials fail every sync, which checks that a sync releases
        # its slot in `running` however it ends.  A leaked slot would wedge the
        # checkpoint: later requests to sync it are dropped as duplicates, and
        # the status keeps reporting a sync that is no longer running.
        self.pipeline.set_runtime_config(
            RuntimeConfig(
                workers=FELDERA_TEST_NUM_WORKERS,
                hosts=FELDERA_TEST_NUM_HOSTS,
                fault_tolerance_model=FaultToleranceModel.AtLeastOnce,
                storage=Storage(config=storage_cfg(self.pipeline.name, auth_err=True)),
                checkpoint_interval="60s",
            )
        )
        self.pipeline.start()
        processed, _ = self._insert_data_and_wait()
        self.pipeline.checkpoint(wait=True)
        chk_uuid, _ = self._latest_checkpoint(processed)

        uuid = self.pipeline.sync_checkpoint()
        self.assertEqual(str(uuid), str(chk_uuid))

        status, _ = self._drain_sync(uuid)
        self.assertEqual(
            (status.get("failure") or {}).get("uuid"),
            uuid,
            f"sync should have failed on bad credentials: {status}",
        )

        # Re-syncing the same checkpoint is accepted rather than rejected as
        # already-failed, and fails the same way.  A local MinIO 403 usually
        # lands before the first status request, so this does not try to catch
        # the second sync in `running`; `test_sync_status_in_flight` covers that
        # against an endpoint that cannot answer.
        self.assertEqual(self.pipeline.sync_checkpoint(), uuid)
        status, _ = self._drain_sync(uuid)
        self.assertEqual(
            (status.get("failure") or {}).get("uuid"),
            uuid,
            f"re-sync should have failed too: {status}",
        )

        self.pipeline.stop(force=True)
        self.pipeline.clear_storage()

    @enterprise_only
    def test_standby_takes_over_from_read_bucket(self):
        # In standby mode, when bucket is initially empty the pipeline falls back
        # to read_bucket. Once the main pipeline pushes a newer checkpoint to
        # bucket, standby picks it up on the next poll.
        ft = FaultToleranceModel.AtLeastOnce
        pull_interval = 2

        # Step 1: create the source pipeline (becomes read_bucket for standby).
        source = self.new_pipeline_with_suffix("source")
        source.set_runtime_config(
            RuntimeConfig(
                workers=FELDERA_TEST_NUM_WORKERS,
                hosts=FELDERA_TEST_NUM_HOSTS,
                fault_tolerance_model=ft,
                storage=Storage(config=storage_cfg(source.name)),
            )
        )
        source.start()
        data_initial = [{"c0": i, "c1": f"initial_{i}"} for i in range(1, 11)]
        source.input_json("t0", data_initial)
        source.wait_for_completion()
        source.checkpoint(wait=True)
        source.sync_checkpoint(wait=True)
        source.stop(force=True)

        source_bucket = checkpoint_sync_bucket(source.name)

        # Step 2: start the main pipeline; it will push newer checkpoints to
        # its own bucket during the test.
        storage_config = storage_cfg(self.pipeline.name)
        self.pipeline.set_runtime_config(
            RuntimeConfig(
                workers=FELDERA_TEST_NUM_WORKERS,
                hosts=FELDERA_TEST_NUM_HOSTS,
                fault_tolerance_model=ft,
                storage=Storage(config=storage_config),
            )
        )
        self.pipeline.start()

        # Step 3: start the standby pipeline with empty bucket, read_bucket=source.
        standby = self.new_pipeline_with_suffix("standby")
        standby.set_runtime_config(
            RuntimeConfig(
                workers=FELDERA_TEST_NUM_WORKERS,
                hosts=FELDERA_TEST_NUM_HOSTS,
                fault_tolerance_model=ft,
                storage=Storage(
                    config=storage_cfg(
                        self.pipeline.name,
                        start_from_checkpoint="latest",
                        pull_interval=pull_interval,
                        read_bucket=source_bucket,
                    )
                ),
            )
        )
        standby.start_standby()

        # Wait for standby to pull at least one checkpoint (from read_bucket).
        self._wait_for_standby_checkpoint_pull(standby, timeout_s=120)

        # Step 4: main pipeline inserts data and pushes multiple checkpoints
        # to its own bucket.
        extra_ckpts = random.randint(3, 6)
        for i in range(extra_ckpts):
            new_data = [{"c0": 100 + i, "c1": f"main_{100 + i}"}]
            self.pipeline.input_json("t0", new_data)
            self.pipeline.wait_for_completion()
            self.pipeline.checkpoint(wait=True)
            self.pipeline.sync_checkpoint(wait=True)
            time.sleep(0.2)

        expected = list(self.pipeline.query("SELECT * FROM v0"))
        print(
            f"{self.pipeline.name}: expected records after main pushes: {len(expected)}",
            file=sys.stderr,
        )
        self.pipeline.stop(force=True)

        # Step 5: activate standby — it should have tracked the latest bucket
        # checkpoint by now.
        try:
            assert standby.status() == PipelineStatus.STANDBY
            # activate() blocks until the manager reports RUNNING or
            # AwaitingApproval, so check the result. Do not wait on the
            # "pipeline activated" log line instead: it is logged once, and the
            # manager's bounded log buffer evicts it under trace noise.
            status = standby.activate(
                timeout_s=(pull_interval * extra_ckpts) + ACTIVATE_PULL_BUDGET_S
            )
            assert status == PipelineStatus.RUNNING, (
                f"standby activation stopped at {status}, not RUNNING"
            )

            got = list(standby.query("SELECT * FROM v0"))
            print(
                f"{standby.name}: got after activation: {len(got)}",
                file=sys.stderr,
            )
        finally:
            standby.stop(force=True)

        self.assertCountEqual(expected, got)

        standby.clear_storage()
        self.pipeline.clear_storage()
        source.clear_storage()

    # -------------------------------------------------------------------------
    # Additional local-priority and read_bucket tests
    # -------------------------------------------------------------------------

    @enterprise_only
    def test_remote_checkpoints(self):
        """
        CREATE TABLE t0 (c0 INT, c1 VARCHAR);
        CREATE MATERIALIZED VIEW v0 AS SELECT * FROM t0;
        """
        # retention_min_count=2 keeps both checkpoints on remote.
        storage_config = storage_cfg(
            self.pipeline.name,
            retention_min_count=2,
        )
        self.pipeline.set_runtime_config(
            RuntimeConfig(
                workers=FELDERA_TEST_NUM_WORKERS,
                hosts=FELDERA_TEST_NUM_HOSTS,
                fault_tolerance_model=FaultToleranceModel.AtLeastOnce,
                storage=Storage(config=storage_config),
                checkpoint_interval="60s",
            )
        )
        self.pipeline.start()

        # Snapshot any UUIDs already present in remote before this test pushed
        # anything, so we can filter them out of assertions.
        pre_existing = {str(c["uuid"]) for c in self.pipeline.remote_checkpoints()}

        self._insert_data_and_wait()
        self.pipeline.checkpoint(wait=True)
        time.sleep(1)
        uuid1 = self._sync_and_verify()

        self._insert_data_and_wait()
        self.pipeline.checkpoint(wait=True)
        time.sleep(1)
        uuid2 = self._sync_and_verify()

        remote = self.pipeline.remote_checkpoints()
        print(f"remote checkpoints: {remote}", file=sys.stderr)

        pushed = {str(c["uuid"]) for c in remote} - pre_existing
        self.assertIn(str(uuid1), pushed)
        self.assertIn(str(uuid2), pushed)

        self.pipeline.stop(force=True)
        self.pipeline.clear_storage()
