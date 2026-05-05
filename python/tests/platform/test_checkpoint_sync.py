import random
import sys
import time
from typing import Optional
from uuid import UUID, uuid4

from feldera.enums import FaultToleranceModel, PipelineStatus
from feldera.runtime_config import RuntimeConfig, Storage
from feldera.testutils import (
    FELDERA_TEST_NUM_HOSTS,
    FELDERA_TEST_NUM_WORKERS,
    single_host_only,
)
from tests import enterprise_only
from tests.shared_test_pipeline import SharedTestPipeline
from tests.utils import (
    MINIO_BUCKET,
    MINIO_ENDPOINT,
    MINIO_REGION,
    required_env,
)

from .helper import wait_for_condition


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
) -> dict:
    # MinIO credentials are read here (not at import time) so collection
    # does not blow up in environments where they are unset.
    access_key = required_env("CI_K8S_MINIO_ACCESS_KEY_ID")
    secret_key = required_env("CI_K8S_MINIO_SECRET_ACCESS_KEY")

    sync: dict = {
        "bucket": f"{MINIO_BUCKET}/{pipeline_name}",
        "access_key": access_key,
        "secret_key": secret_key if not auth_err else secret_key + "extra",
        "provider": "Minio",
        "endpoint": endpoint or MINIO_ENDPOINT,
        "region": MINIO_REGION,
        "start_from_checkpoint": start_from_checkpoint,
        "fail_if_no_checkpoint": strict,
        "standby": standby,
        "pull_interval": pull_interval,
        "push_interval": push_interval,
        "retention_min_count": retention_min_count,
        "retention_min_age": retention_min_age,
    }
    if read_bucket is not None:
        sync["read_bucket"] = read_bucket
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
        end = time.monotonic() + timeout_s
        for log in pipeline.logs():
            if "checkpoint pulled successfully" in log:
                return
            if time.monotonic() > end:
                raise TimeoutError(
                    f"{pipeline.name} timed out waiting to pull checkpoint"
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
                checkpoint_interval_secs=ft_interval,
            )
        )
        self.pipeline.start()

    def _insert_data_and_wait(self):
        """Insert random rows and block until the pipeline processes them all.

        Returns (processed, got_before) where got_before is the view snapshot.
        """
        random.seed(time.time())
        total = random.randint(10, 20)
        data = [{"c0": i, "c1": str(i)} for i in range(1, total)]
        self.pipeline.input_json("t0", data)
        self.pipeline.execute("INSERT INTO t0 VALUES (21, 'exists')")

        start = time.monotonic()
        timeout = 5
        while True:
            processed = self.pipeline.stats().global_metrics.total_processed_records
            if processed == total:
                break
            if time.monotonic() - start > timeout:
                raise TimeoutError(
                    f"timed out while waiting for pipeline to process {total} records"
                )
            time.sleep(0.1)

        got_before = list(self.pipeline.query("SELECT * FROM v0"))
        print(f"{self.pipeline.name}: records: {total}, {got_before}", file=sys.stderr)

        if len(got_before) != processed:
            raise RuntimeError(
                f"adhoc query returned {len(got_before)} but {processed} records were "
                f"processed: {got_before}"
            )

        return processed, got_before

    def _wait_for_automated_checkpoint(self, processed):
        """Poll until an automated checkpoint covers all *processed* records.

        In multihost pipelines UUIDs are generated independently per pod and
        cannot be compared across pods, so we track steps instead.

        Returns (chk_uuid, chk_steps).
        """
        chk_uuid = None
        chk_steps = None

        def checkpoint_created() -> bool:
            nonlocal chk_uuid, chk_steps
            chks = self.pipeline.checkpoints()
            # Group by steps so that multihost totals are summed correctly.
            by_steps: dict = {}
            for c in chks:
                by_steps.setdefault(c.steps, []).append(c)
            for steps_val, step_chks in by_steps.items():
                step_total = sum(c.processed_records for c in step_chks)
                print(
                    f"Total: {step_total}, chks: {[chk.to_dict() for chk in step_chks]}",
                    file=sys.stderr,
                )
                # processed_records is partitioned across pods (not replicated),
                # so summing per-pod values gives the global total.
                if step_total == processed:
                    chk_uuid = step_chks[0].uuid
                    chk_steps = steps_val
                    return True
            return False

        wait_for_condition(
            "automated checkpoint is created for current processed records",
            checkpoint_created,
            timeout_s=30.0,
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

    def _wait_for_automated_sync(self, chk_uuid=None, chk_steps=None):
        """Poll until the periodic sync has uploaded a checkpoint at least as recent as *chk_uuid*."""

        def checkpoint_sync_completed() -> bool:
            try:
                synced = self.pipeline.last_successful_checkpoint_sync()
                print("Automatically synced checkpoint UUID:", synced, file=sys.stderr)
                if synced is None:
                    return False
                if chk_uuid is None:
                    return True
                s_steps = self._checkpoint_steps(synced)
                if chk_steps is not None and s_steps is not None:
                    return s_steps >= chk_steps
                return UUID(str(synced)) >= UUID(str(chk_uuid))
            except RuntimeError:
                return False

        wait_for_condition(
            "automated checkpoint sync completes",
            checkpoint_sync_completed,
            timeout_s=30.0,
            poll_interval_s=0.5,
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
            standby=standby,
        )
        self.pipeline.set_runtime_config(
            RuntimeConfig(
                workers=FELDERA_TEST_NUM_WORKERS,
                hosts=FELDERA_TEST_NUM_HOSTS,
                fault_tolerance_model=FaultToleranceModel.AtLeastOnce,
                storage=Storage(config=storage_config),
                checkpoint_interval_secs=ft_interval,
            )
        )

        if not standby:
            self.pipeline.start()
        else:
            self.pipeline.start_standby()
            self._wait_for_standby_checkpoint_pull(self.pipeline, timeout_s=120)
            assert self.pipeline.status() == PipelineStatus.STANDBY
            self.pipeline.activate()

    # =========================================================================
    # Tests
    # =========================================================================

    @enterprise_only
    def test_checkpoint_sync(self):
        """
        CREATE TABLE t0 (c0 INT, c1 VARCHAR);
        CREATE MATERIALIZED VIEW v0 AS SELECT * FROM t0;
        """
        self._configure_and_start()
        _, got_before = self._insert_data_and_wait()
        self.pipeline.checkpoint(wait=True)
        time.sleep(1)
        self._sync_and_verify()

        self._restart_from_checkpoint("latest")

        got_after = list(self.pipeline.query("SELECT * FROM v0"))
        print(
            f"{self.pipeline.name}: after: {len(got_after)}, {got_after}",
            file=sys.stderr,
        )
        self.assertCountEqual(got_before, got_after)
        self.pipeline.stop(force=True)
        self.pipeline.clear_storage()

    @enterprise_only
    @single_host_only
    def test_from_uuid(self):
        # retention_min_age prevents the checkpoint from being garbage-collected
        # before the pipeline restarts from it.
        self._configure_and_start(retention_min_age=5)
        _, got_before = self._insert_data_and_wait()
        self.pipeline.checkpoint(wait=True)
        time.sleep(1)
        uuid = self._sync_and_verify()

        self._restart_from_checkpoint(uuid)

        got_after = list(self.pipeline.query("SELECT * FROM v0"))
        print(
            f"{self.pipeline.name}: after: {len(got_after)}, {got_after}",
            file=sys.stderr,
        )
        self.assertCountEqual(got_before, got_after)
        self.pipeline.stop(force=True)
        self.pipeline.clear_storage()

    @enterprise_only
    def test_without_clearing_storage(self):
        self._configure_and_start()
        _, got_before = self._insert_data_and_wait()
        self.pipeline.checkpoint(wait=True)
        time.sleep(1)
        self._sync_and_verify()

        self._restart_from_checkpoint("latest", clear_storage=False)

        got_after = list(self.pipeline.query("SELECT * FROM v0"))
        print(
            f"{self.pipeline.name}: after: {len(got_after)}, {got_after}",
            file=sys.stderr,
        )
        self.assertCountEqual(got_before, got_after)
        self.pipeline.stop(force=True)

    @enterprise_only
    def test_automated_checkpoint(self):
        self._configure_and_start(ft_interval=5)
        processed, got_before = self._insert_data_and_wait()
        chk_uuid, chk_steps = self._wait_for_automated_checkpoint(processed)
        time.sleep(1)
        self._sync_and_verify(chk_uuid, chk_steps)

        self._restart_from_checkpoint("latest")

        got_after = list(self.pipeline.query("SELECT * FROM v0"))
        print(
            f"{self.pipeline.name}: after: {len(got_after)}, {got_after}",
            file=sys.stderr,
        )
        self.assertCountEqual(got_before, got_after)
        self.pipeline.stop(force=True)
        self.pipeline.clear_storage()

    @enterprise_only
    def test_automated_checkpoint_sync(self):
        self._configure_and_start(ft_interval=5, push_interval=10)
        processed, got_before = self._insert_data_and_wait()
        chk_uuid, chk_steps = self._wait_for_automated_checkpoint(processed)
        time.sleep(1)
        self._wait_for_automated_sync(chk_uuid, chk_steps)

        self._restart_from_checkpoint("latest")

        got_after = list(self.pipeline.query("SELECT * FROM v0"))
        print(
            f"{self.pipeline.name}: after: {len(got_after)}, {got_after}",
            file=sys.stderr,
        )
        self.assertCountEqual(got_before, got_after)
        self.pipeline.stop(force=True)
        self.pipeline.clear_storage()

    @enterprise_only
    def test_automated_checkpoint_sync1(self):
        # Manual checkpoint, automated sync.
        self._configure_and_start(ft_interval=5, push_interval=10)
        _, got_before = self._insert_data_and_wait()
        self.pipeline.checkpoint(wait=True)
        time.sleep(1)
        self._wait_for_automated_sync()

        self._restart_from_checkpoint("latest")

        got_after = list(self.pipeline.query("SELECT * FROM v0"))
        print(
            f"{self.pipeline.name}: after: {len(got_after)}, {got_after}",
            file=sys.stderr,
        )
        self.assertCountEqual(got_before, got_after)
        self.pipeline.stop(force=True)
        self.pipeline.clear_storage()

    @enterprise_only
    def test_autherr_fail(self):
        self._configure_and_start()
        self._insert_data_and_wait()
        self.pipeline.checkpoint(wait=True)
        time.sleep(1)
        self._sync_and_verify()

        with self.assertRaisesRegex(RuntimeError, "SignatureDoesNotMatch|Forbidden"):
            self._restart_from_checkpoint("latest", auth_err=True, strict=True)

    @enterprise_only
    def test_autherr(self):
        self._configure_and_start()
        self._insert_data_and_wait()
        self.pipeline.checkpoint(wait=True)
        time.sleep(1)
        self._sync_and_verify()

        with self.assertRaisesRegex(RuntimeError, "SignatureDoesNotMatch|Forbidden"):
            self._restart_from_checkpoint("latest", auth_err=True, strict=False)

    @enterprise_only
    @single_host_only
    def test_nonexistent_checkpoint_fail(self):
        self._configure_and_start()
        self._insert_data_and_wait()
        self.pipeline.checkpoint(wait=True)
        time.sleep(1)
        self._sync_and_verify()

        with self.assertRaisesRegex(RuntimeError, "were not found in source"):
            self._restart_from_checkpoint(uuid4(), strict=True)

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
    def test_standby_activation(self):
        self._configure_and_start()
        _, got_before = self._insert_data_and_wait()
        self.pipeline.checkpoint(wait=True)
        time.sleep(1)
        self._sync_and_verify()

        self._restart_from_checkpoint("latest", standby=True)

        got_after = list(self.pipeline.query("SELECT * FROM v0"))
        print(
            f"{self.pipeline.name}: after: {len(got_after)}, {got_after}",
            file=sys.stderr,
        )
        self.assertCountEqual(got_before, got_after)
        self.pipeline.stop(force=True)
        self.pipeline.clear_storage()

    @enterprise_only
    @single_host_only
    def test_standby_activation_from_uuid(self):
        self._configure_and_start(retention_min_age=5)
        _, got_before = self._insert_data_and_wait()
        self.pipeline.checkpoint(wait=True)
        time.sleep(1)
        uuid = self._sync_and_verify()

        self._restart_from_checkpoint(uuid, standby=True)

        got_after = list(self.pipeline.query("SELECT * FROM v0"))
        print(
            f"{self.pipeline.name}: after: {len(got_after)}, {got_after}",
            file=sys.stderr,
        )
        self.assertCountEqual(got_before, got_after)
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
                checkpoint_interval_secs=5,
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
    def test_standby_fallback(self, from_uuid: bool = False):
        # Step 1: Start main pipeline
        storage_config = storage_cfg(self.pipeline.name, retention_min_age=1)
        ft = FaultToleranceModel.AtLeastOnce
        self.pipeline.set_runtime_config(
            RuntimeConfig(
                workers=FELDERA_TEST_NUM_WORKERS,
                hosts=FELDERA_TEST_NUM_HOSTS,
                fault_tolerance_model=ft,
                storage=Storage(config=storage_config),
            )
        )
        self.pipeline.start()

        # Insert initial data
        random.seed(time.time())
        total_initial = random.randint(10, 20)
        data_initial = [{"c0": i, "c1": str(i)} for i in range(1, total_initial)]
        self.pipeline.input_json("t0", data_initial)
        self.pipeline.execute("INSERT INTO t0 VALUES (21, 'exists')")
        self.pipeline.wait_for_completion()

        got_before = list(self.pipeline.query("select * from v0"))

        # Step 2: Create checkpoint and sync
        self.pipeline.checkpoint(wait=True)
        uuid = self.pipeline.sync_checkpoint(wait=True)

        # Step 3: Start standby pipeline
        standby = self.new_pipeline_with_suffix("standby")
        pull_interval = 1
        standby.set_runtime_config(
            RuntimeConfig(
                workers=FELDERA_TEST_NUM_WORKERS,
                hosts=FELDERA_TEST_NUM_HOSTS,
                fault_tolerance_model=ft,
                storage=Storage(
                    config=storage_cfg(
                        self.pipeline.name,
                        start_from_checkpoint=uuid if from_uuid else "latest",
                        standby=True,
                        pull_interval=pull_interval,
                    )
                ),
            )
        )
        standby.start_standby()

        self._wait_for_standby_checkpoint_pull(standby, timeout_s=120)

        # Step 4: Add more data and make 3-10 checkpoints
        extra_ckpts = random.randint(3, 10)
        total_additional = 0

        for i in range(extra_ckpts):
            new_val = 100 + i
            new_data = [{"c0": new_val, "c1": f"extra_{new_val}"}]
            self.pipeline.input_json("t0", new_data)
            self.pipeline.wait_for_completion()
            total_additional += 1
            self.pipeline.checkpoint(wait=True)
            self.pipeline.sync_checkpoint(wait=True)
            time.sleep(0.2)

        got_expected = (
            got_before if from_uuid else list(self.pipeline.query("SELECT * FROM v0"))
        )
        print(
            f"{self.pipeline.name}: final records before shutdown: {got_expected}",
            file=sys.stderr,
        )

        # Step 5: Stop main and activate standby
        self.pipeline.stop(force=True)

        assert standby.status() == PipelineStatus.STANDBY
        standby.activate(timeout_s=(pull_interval * extra_ckpts) + 60)

        got_after = list(standby.query("SELECT * FROM v0"))

        # Cleanup
        standby.stop(force=True)

        # Step 6: Validate standby has all expected records
        print(
            f"{standby.name}: final records after activation: {got_after}",
            file=sys.stderr,
        )
        self.assertCountEqual(got_expected, got_after)

        standby.start()
        got_final = list(standby.query("SELECT * FROM v0"))
        standby.stop(force=True)

        self.assertCountEqual(got_after, got_final)

        self.pipeline.clear_storage()

    @enterprise_only
    @single_host_only
    def test_standby_fallback_from_uuid(self):
        self.test_standby_fallback(from_uuid=True)

    # -------------------------------------------------------------------------
    # Local checkpoint priority
    # -------------------------------------------------------------------------

    @enterprise_only
    def test_local_checkpoint_priority(self):
        # After syncing checkpoint A to S3, taking a local-only checkpoint B
        # (without syncing), and restarting without clearing storage,
        # the pipeline must resume from checkpoint B (local), not re-download
        # checkpoint A from S3.
        ft = FaultToleranceModel.AtLeastOnce

        storage_config = storage_cfg(self.pipeline.name, retention_min_count=2)
        self.pipeline.set_runtime_config(
            RuntimeConfig(
                workers=FELDERA_TEST_NUM_WORKERS,
                hosts=FELDERA_TEST_NUM_HOSTS,
                fault_tolerance_model=ft,
                storage=Storage(config=storage_config),
            )
        )
        self.pipeline.start()

        # Insert data_a and sync checkpoint A to S3.
        data_a = [{"c0": i, "c1": str(i)} for i in range(1, 11)]
        self.pipeline.input_json("t0", data_a)
        self.pipeline.wait_for_completion()
        self.pipeline.checkpoint(wait=True)
        self.pipeline.sync_checkpoint(wait=True)

        # Insert data_b and take a local-only checkpoint B (no sync).
        data_b = [{"c0": i, "c1": str(i)} for i in range(11, 21)]
        self.pipeline.input_json("t0", data_b)
        self.pipeline.wait_for_completion()
        self.pipeline.checkpoint(wait=True)
        # Intentionally NOT calling sync_checkpoint — remote still has A only.

        expected = list(self.pipeline.query("SELECT * FROM v0"))
        print(
            f"{self.pipeline.name}: expected records (A+B): {len(expected)}",
            file=sys.stderr,
        )
        # Stop without clearing storage — local checkpoint B is preserved.
        self.pipeline.stop(force=True)

        # Restart with start_from_checkpoint=latest and local storage intact.
        # Expected: use local checkpoint B (has A+B data), skip remote pull.
        storage_config = storage_cfg(
            self.pipeline.name,
            start_from_checkpoint="latest",
            retention_min_count=2,
        )
        self.pipeline.set_runtime_config(
            RuntimeConfig(
                workers=FELDERA_TEST_NUM_WORKERS,
                hosts=FELDERA_TEST_NUM_HOSTS,
                fault_tolerance_model=ft,
                storage=Storage(config=storage_config),
            )
        )
        self.pipeline.start()
        got = list(self.pipeline.query("SELECT * FROM v0"))
        print(
            f"{self.pipeline.name}: got after restart: {len(got)}",
            file=sys.stderr,
        )
        self.assertCountEqual(expected, got)

        self.pipeline.stop(force=True)
        self.pipeline.clear_storage()

    # -------------------------------------------------------------------------
    # read_bucket tests
    # -------------------------------------------------------------------------

    @enterprise_only
    def test_read_bucket(
        self,
        standby: bool = False,
        from_uuid: bool = False,
    ):
        # When bucket is empty but read_bucket holds a checkpoint, the pipeline
        # seeds from read_bucket.
        ft = FaultToleranceModel.AtLeastOnce

        # Step 1: push a checkpoint to the source pipeline's bucket (becomes read_bucket).
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

        random.seed(time.time())
        total = random.randint(10, 20)
        data = [{"c0": i, "c1": str(i)} for i in range(1, total)]
        source.input_json("t0", data)
        source.wait_for_completion()
        expected = list(source.query("SELECT * FROM v0"))

        source.checkpoint(wait=True)
        uuid = source.sync_checkpoint(wait=True)
        source.stop(force=True)

        source_bucket = f"{MINIO_BUCKET}/{source.name}"

        # Step 2: start the main pipeline with an empty bucket and read_bucket
        # pointing at the source.
        storage_config = storage_cfg(
            self.pipeline.name,
            start_from_checkpoint=uuid if from_uuid else "latest",
            read_bucket=source_bucket,
            strict=True,
            standby=standby,
            pull_interval=2,
        )
        self.pipeline.set_runtime_config(
            RuntimeConfig(
                workers=FELDERA_TEST_NUM_WORKERS,
                hosts=FELDERA_TEST_NUM_HOSTS,
                fault_tolerance_model=ft,
                storage=Storage(config=storage_config),
            )
        )

        if not standby:
            self.pipeline.start()
        else:
            self.pipeline.start_standby()

            try:
                self._wait_for_standby_checkpoint_pull(self.pipeline, timeout_s=120)

                assert self.pipeline.status() == PipelineStatus.STANDBY
                self.pipeline.activate()
            except Exception:
                self.pipeline.stop(force=True)
                raise

        try:
            got = list(self.pipeline.query("SELECT * FROM v0"))
            print(
                f"{self.pipeline.name}: expected={len(expected)}, got={len(got)}",
                file=sys.stderr,
            )
        finally:
            self.pipeline.stop(force=True)

        self.assertCountEqual(expected, got)
        self.pipeline.clear_storage()
        source.clear_storage()

    @enterprise_only
    @single_host_only
    def test_read_bucket_from_uuid(self):
        # read_bucket fallback works when start_from_checkpoint is a UUID.
        self.test_read_bucket(from_uuid=True)

    @enterprise_only
    def test_read_bucket_standby(self):
        # Standby pipeline seeds from read_bucket when bucket is empty.
        self.test_read_bucket(standby=True)

    @enterprise_only
    @single_host_only
    def test_read_bucket_standby_from_uuid(self):
        # Standby pipeline seeds from read_bucket using a specific UUID.
        self.test_read_bucket(standby=True, from_uuid=True)

    @enterprise_only
    def test_bucket_preferred_over_read_bucket(self):
        # When both bucket and read_bucket hold checkpoints, the pipeline uses
        # bucket (its own checkpoint), not read_bucket.
        ft = FaultToleranceModel.AtLeastOnce

        # Step 1: push a checkpoint to the source pipeline (becomes read_bucket).
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
        data_source = [{"c0": i, "c1": f"source_{i}"} for i in range(1, 11)]
        source.input_json("t0", data_source)
        source.wait_for_completion()
        source.checkpoint(wait=True)
        source.sync_checkpoint(wait=True)
        source.stop(force=True)

        source_bucket = f"{MINIO_BUCKET}/{source.name}"

        # Step 2: push a checkpoint to the main pipeline's own bucket.
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
        data_main = [{"c0": i, "c1": f"main_{i}"} for i in range(11, 21)]
        self.pipeline.input_json("t0", data_main)
        self.pipeline.wait_for_completion()
        expected = list(self.pipeline.query("SELECT * FROM v0"))
        self.pipeline.checkpoint(wait=True)
        self.pipeline.sync_checkpoint(wait=True)
        self.pipeline.stop(force=True)
        self.pipeline.clear_storage()

        # Step 3: restart with both bucket (has main data) and read_bucket (has
        # source data). The pipeline should restore from bucket, not read_bucket.
        storage_config = storage_cfg(
            self.pipeline.name,
            start_from_checkpoint="latest",
            read_bucket=source_bucket,
        )
        self.pipeline.set_runtime_config(
            RuntimeConfig(
                workers=FELDERA_TEST_NUM_WORKERS,
                hosts=FELDERA_TEST_NUM_HOSTS,
                fault_tolerance_model=ft,
                storage=Storage(config=storage_config),
            )
        )
        self.pipeline.start()
        got = list(self.pipeline.query("SELECT * FROM v0"))
        print(
            f"{self.pipeline.name}: expected={len(expected)}, got={len(got)}",
            file=sys.stderr,
        )
        # Must see main data only, not source data.
        self.assertCountEqual(expected, got)

        self.pipeline.stop(force=True)
        self.pipeline.clear_storage()
        source.clear_storage()

    @enterprise_only
    def test_standby_bucket_takes_over_from_read_bucket(self):
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

        source_bucket = f"{MINIO_BUCKET}/{source.name}"

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
                        standby=True,
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
            standby.activate(timeout_s=(pull_interval * extra_ckpts) + 60)

            start = time.monotonic()
            end = start + 120
            for log in standby.logs():
                if "activated" in log:
                    break
                if time.monotonic() > end:
                    raise TimeoutError("Timed out waiting for standby to activate")

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
    @single_host_only
    def test_local_checkpoint_priority_from_uuid(self):
        # The local-first code path for UUID differs from `latest`; verify it
        # independently.  When start_from_checkpoint is a UUID that exists only
        # in local storage (never synced to remote), the pipeline must restore
        # from that local checkpoint without attempting a remote pull.
        ft = FaultToleranceModel.AtLeastOnce

        storage_config = storage_cfg(self.pipeline.name, retention_min_count=2)
        self.pipeline.set_runtime_config(
            RuntimeConfig(
                workers=FELDERA_TEST_NUM_WORKERS,
                hosts=FELDERA_TEST_NUM_HOSTS,
                fault_tolerance_model=ft,
                storage=Storage(config=storage_config),
            )
        )
        self.pipeline.start()

        # Insert data_a and sync checkpoint A to remote.
        data_a = [{"c0": i, "c1": str(i)} for i in range(1, 11)]
        self.pipeline.input_json("t0", data_a)
        self.pipeline.wait_for_completion()
        self.pipeline.checkpoint(wait=True)
        self.pipeline.sync_checkpoint(wait=True)

        # Insert data_b and take a local-only checkpoint B (no remote sync).
        data_b = [{"c0": i, "c1": str(i)} for i in range(11, 21)]
        self.pipeline.input_json("t0", data_b)
        self.pipeline.wait_for_completion()
        self.pipeline.checkpoint(wait=True)
        # Intentionally NOT calling sync_checkpoint — remote only has checkpoint A.

        # UUID of checkpoint B (the newest local checkpoint, not in remote).
        chks = self.pipeline.checkpoints()
        local_uuid = max(chks, key=lambda c: c.uuid).uuid

        expected = list(self.pipeline.query("SELECT * FROM v0"))
        print(
            f"{self.pipeline.name}: expected (A+B): {len(expected)}",
            file=sys.stderr,
        )
        # Stop without clearing storage so checkpoint B remains on disk.
        self.pipeline.stop(force=True)

        # Restart specifying the UUID of the local-only checkpoint B.
        # If local-first is broken the pipeline would try remote, fail (strict=False),
        # and start fresh — the assertCountEqual below would then catch the regression.
        storage_config = storage_cfg(
            self.pipeline.name,
            start_from_checkpoint=local_uuid,
            retention_min_count=2,
        )
        self.pipeline.set_runtime_config(
            RuntimeConfig(
                workers=FELDERA_TEST_NUM_WORKERS,
                hosts=FELDERA_TEST_NUM_HOSTS,
                fault_tolerance_model=ft,
                storage=Storage(config=storage_config),
            )
        )
        self.pipeline.start()
        got = list(self.pipeline.query("SELECT * FROM v0"))
        print(
            f"{self.pipeline.name}: got after restart: {len(got)}",
            file=sys.stderr,
        )
        self.pipeline.stop(force=True)

        self.assertCountEqual(expected, got)
        self.pipeline.clear_storage()

    @enterprise_only
    def test_local_priority_over_read_bucket(self):
        # Local checkpoint wins over read_bucket even when the primary S3 bucket
        # is empty.  Priority order: local > bucket > read_bucket.
        ft = FaultToleranceModel.AtLeastOnce

        # Step 1: source pipeline creates a checkpoint (will become read_bucket).
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
        data_source = [{"c0": i, "c1": f"source_{i}"} for i in range(1, 11)]
        source.input_json("t0", data_source)
        source.wait_for_completion()
        source.checkpoint(wait=True)
        source.sync_checkpoint(wait=True)
        source.stop(force=True)

        source_bucket = f"{MINIO_BUCKET}/{source.name}"

        # Step 2: main pipeline takes a LOCAL-ONLY checkpoint (never synced).
        # Its own S3 bucket stays empty, ensuring the only remote source of data
        # is read_bucket.
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
        data_main = [{"c0": i, "c1": f"main_{i}"} for i in range(11, 21)]
        self.pipeline.input_json("t0", data_main)
        self.pipeline.wait_for_completion()
        expected = list(self.pipeline.query("SELECT * FROM v0"))
        self.pipeline.checkpoint(wait=True)
        # Intentionally NOT syncing — main bucket stays empty.
        self.pipeline.stop(force=True)

        print(
            f"{self.pipeline.name}: expected (main data only): {len(expected)}",
            file=sys.stderr,
        )

        # Step 3: restart with start_from=latest, empty bucket, read_bucket=source.
        # Local storage has main's checkpoint.  Local must win over read_bucket.
        # If local-first is broken, read_bucket would be used and we'd see
        # source data instead of main data.
        storage_config = storage_cfg(
            self.pipeline.name,
            start_from_checkpoint="latest",
            read_bucket=source_bucket,
        )
        self.pipeline.set_runtime_config(
            RuntimeConfig(
                workers=FELDERA_TEST_NUM_WORKERS,
                hosts=FELDERA_TEST_NUM_HOSTS,
                fault_tolerance_model=ft,
                storage=Storage(config=storage_config),
            )
        )
        self.pipeline.start()
        got = list(self.pipeline.query("SELECT * FROM v0"))
        print(
            f"{self.pipeline.name}: got after restart: {len(got)}",
            file=sys.stderr,
        )
        self.pipeline.stop(force=True)

        self.assertCountEqual(expected, got)
        self.pipeline.clear_storage()
        source.clear_storage()

    @enterprise_only
    @single_host_only
    def test_local_priority_over_read_bucket_from_uuid(self):
        # UUID variant of test_local_priority_over_read_bucket.
        # When a specific UUID exists only in local storage (bucket empty,
        # read_bucket has a different checkpoint), local must win.
        # Exercises read_local_checkpoint_by_uuid rather than
        # read_local_latest_checkpoint.
        ft = FaultToleranceModel.AtLeastOnce

        # Step 1: source pipeline creates a checkpoint (will become read_bucket).
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
        data_source = [{"c0": i, "c1": f"source_{i}"} for i in range(1, 11)]
        source.input_json("t0", data_source)
        source.wait_for_completion()
        source.checkpoint(wait=True)
        source.sync_checkpoint(wait=True)
        source.stop(force=True)

        source_bucket = f"{MINIO_BUCKET}/{source.name}"

        # Step 2: main pipeline takes a LOCAL-ONLY checkpoint (never synced).
        # Main bucket stays empty; source_bucket (read_bucket) has a checkpoint
        # with a different UUID.
        storage_config = storage_cfg(self.pipeline.name, retention_min_count=2)
        self.pipeline.set_runtime_config(
            RuntimeConfig(
                workers=FELDERA_TEST_NUM_WORKERS,
                hosts=FELDERA_TEST_NUM_HOSTS,
                fault_tolerance_model=ft,
                storage=Storage(config=storage_config),
            )
        )
        self.pipeline.start()
        data_main = [{"c0": i, "c1": f"main_{i}"} for i in range(11, 21)]
        self.pipeline.input_json("t0", data_main)
        self.pipeline.wait_for_completion()
        self.pipeline.checkpoint(wait=True)
        # Intentionally NOT syncing — main bucket stays empty.

        # UUID of the local-only checkpoint (not in bucket, not in read_bucket).
        chks = self.pipeline.checkpoints()
        local_uuid = max(chks, key=lambda c: c.uuid).uuid

        expected = list(self.pipeline.query("SELECT * FROM v0"))
        print(
            f"{self.pipeline.name}: expected (main data only): {len(expected)}",
            file=sys.stderr,
        )
        self.pipeline.stop(force=True)

        # Step 3: restart with the local UUID and read_bucket configured.
        # If local-first breaks: bucket has no such UUID, read_bucket has no such
        # UUID, strict=False → starts fresh.  assertCountEqual catches the regression.
        storage_config = storage_cfg(
            self.pipeline.name,
            start_from_checkpoint=local_uuid,
            read_bucket=source_bucket,
            retention_min_count=2,
        )
        self.pipeline.set_runtime_config(
            RuntimeConfig(
                workers=FELDERA_TEST_NUM_WORKERS,
                hosts=FELDERA_TEST_NUM_HOSTS,
                fault_tolerance_model=ft,
                storage=Storage(config=storage_config),
            )
        )
        self.pipeline.start()
        got = list(self.pipeline.query("SELECT * FROM v0"))
        print(
            f"{self.pipeline.name}: got after restart: {len(got)}",
            file=sys.stderr,
        )
        self.pipeline.stop(force=True)

        self.assertCountEqual(expected, got)
        self.pipeline.clear_storage()
        source.clear_storage()

    @enterprise_only
    def test_read_bucket_strict_fail(self):
        # When fail_if_no_checkpoint=True and both the primary bucket and
        # read_bucket are empty, the pipeline must fail to start.
        ft = FaultToleranceModel.AtLeastOnce

        # A bucket path with no checkpoints.
        empty_read_bucket = f"{MINIO_BUCKET}/{self.pipeline.name}_read_bucket_empty"

        storage_config = storage_cfg(
            self.pipeline.name,
            start_from_checkpoint="latest",
            read_bucket=empty_read_bucket,
            strict=True,
        )
        self.pipeline.set_runtime_config(
            RuntimeConfig(
                workers=FELDERA_TEST_NUM_WORKERS,
                hosts=FELDERA_TEST_NUM_HOSTS,
                fault_tolerance_model=ft,
                storage=Storage(config=storage_config),
            )
        )
        with self.assertRaisesRegex(RuntimeError, r"not found"):
            self.pipeline.start()

    @enterprise_only
    @single_host_only
    def test_bucket_preferred_over_read_bucket_from_uuid(self):
        # When start_from_checkpoint is a specific UUID, the primary bucket is
        # still preferred over read_bucket.
        ft = FaultToleranceModel.AtLeastOnce

        # Step 1: source pipeline creates a checkpoint (will become read_bucket).
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
        data_source = [{"c0": i, "c1": f"source_{i}"} for i in range(1, 11)]
        source.input_json("t0", data_source)
        source.wait_for_completion()
        source.checkpoint(wait=True)
        source.sync_checkpoint(wait=True)
        source.stop(force=True)

        source_bucket = f"{MINIO_BUCKET}/{source.name}"

        # Step 2: main pipeline creates and syncs its own checkpoint.
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
        data_main = [{"c0": i, "c1": f"main_{i}"} for i in range(11, 21)]
        self.pipeline.input_json("t0", data_main)
        self.pipeline.wait_for_completion()
        expected = list(self.pipeline.query("SELECT * FROM v0"))
        self.pipeline.checkpoint(wait=True)
        uuid = self.pipeline.sync_checkpoint(wait=True)
        self.pipeline.stop(force=True)
        self.pipeline.clear_storage()

        print(
            f"{self.pipeline.name}: expected (main data only): {len(expected)}",
            file=sys.stderr,
        )

        # Step 3: restart specifying uuid from main's bucket, with read_bucket=source.
        # The pipeline must restore from main's bucket (uuid), not source (read_bucket).
        # If read_bucket were incorrectly preferred, we'd get source data instead.
        storage_config = storage_cfg(
            self.pipeline.name,
            start_from_checkpoint=uuid,
            read_bucket=source_bucket,
        )
        self.pipeline.set_runtime_config(
            RuntimeConfig(
                workers=FELDERA_TEST_NUM_WORKERS,
                hosts=FELDERA_TEST_NUM_HOSTS,
                fault_tolerance_model=ft,
                storage=Storage(config=storage_config),
            )
        )
        self.pipeline.start()
        got = list(self.pipeline.query("SELECT * FROM v0"))
        print(
            f"{self.pipeline.name}: got after restart: {len(got)}",
            file=sys.stderr,
        )
        self.pipeline.stop(force=True)

        self.assertCountEqual(expected, got)
        self.pipeline.clear_storage()
        source.clear_storage()
