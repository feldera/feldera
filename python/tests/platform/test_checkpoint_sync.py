import os
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

from .helper import wait_for_condition

DEFAULT_ENDPOINT = os.environ.get(
    "DEFAULT_MINIO_ENDPOINT", "http://minio.extra.svc.cluster.local:9000"
)
DEFAULT_BUCKET = os.environ.get("DEFAULT_MINIO_BUCKET", "default")
ACCESS_KEY = os.environ.get("MINIO_ACCESS_KEY", "minio")
SECRET_KEY = os.environ.get("MINIO_SECRET_KEY", "miniopasswd")


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
    sync: dict = {
        "bucket": f"{DEFAULT_BUCKET}/{pipeline_name}",
        "access_key": ACCESS_KEY,
        "secret_key": SECRET_KEY if not auth_err else SECRET_KEY + "extra",
        "provider": "Minio",
        "endpoint": endpoint or DEFAULT_ENDPOINT,
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
    @enterprise_only
    @single_host_only
    def test_checkpoint_sync(
        self,
        from_uuid: bool = False,
        random_uuid: bool = False,
        clear_storage: bool = True,
        auth_err: bool = False,
        strict: bool = False,
        expect_empty: bool = False,
        standby: bool = False,
        ft_interval: int = 60,
        automated_checkpoint: bool = False,
        automated_sync_interval: Optional[int] = None,
    ):
        """
        CREATE TABLE t0 (c0 INT, c1 VARCHAR);
        CREATE MATERIALIZED VIEW v0 AS SELECT * FROM t0;
        """

        storage_config = storage_cfg(
            self.pipeline.name,
            push_interval=automated_sync_interval,
            retention_min_count=1,
            retention_min_age=5 if from_uuid else 0,
        )
        ft = FaultToleranceModel.AtLeastOnce

        self.pipeline.set_runtime_config(
            RuntimeConfig(
                workers=FELDERA_TEST_NUM_WORKERS,
                hosts=FELDERA_TEST_NUM_HOSTS,
                fault_tolerance_model=ft,
                storage=Storage(config=storage_config),
                checkpoint_interval_secs=ft_interval,
            )
        )
        self.pipeline.start()

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
                f"adhoc query returned {len(got_before)} but {processed} records were processed: {got_before}"
            )

        chk_uuid = None

        if not automated_checkpoint:
            self.pipeline.checkpoint(wait=True)
        else:
            # Wait for at least one automated checkpoint to be created with current data.
            chk_uuid_holder = {"value": None}

            def checkpoint_created() -> bool:
                chks = self.pipeline.checkpoints()
                chk = next((x for x in chks if x.processed_records == processed), None)
                if chk is None:
                    return False
                chk_uuid_holder["value"] = chk.uuid
                return True

            wait_for_condition(
                "automated checkpoint is created for current processed records",
                checkpoint_created,
                timeout_s=30.0,
                poll_interval_s=0.5,
            )
            chk_uuid = chk_uuid_holder["value"]

        print("Checkpoint UUID:", chk_uuid, file=sys.stderr)
        time.sleep(1)

        if automated_sync_interval is not None:

            def checkpoint_sync_completed() -> bool:
                try:
                    synced = self.pipeline.last_successful_checkpoint_sync()
                    print(
                        "Automatically synced checkpoint UUID:", synced, file=sys.stderr
                    )
                    if synced is not None and chk_uuid is not None:
                        if synced >= UUID(chk_uuid):
                            return True
                        return False
                    if synced is not None:
                        return True
                    return False
                except RuntimeError:
                    return False

            wait_for_condition(
                "automated checkpoint sync completes",
                checkpoint_sync_completed,
                timeout_s=30.0,
                poll_interval_s=0.5,
            )
        else:
            uuid = self.pipeline.sync_checkpoint(wait=True)
            print("Synced Checkpoint UUID:", uuid, file=sys.stderr)
            if chk_uuid is not None:
                assert UUID(uuid) >= UUID(chk_uuid)

        self.pipeline.stop(force=True)

        if clear_storage:
            self.pipeline.clear_storage()

        if random_uuid:
            uuid = uuid4()

        # Restart pipeline from checkpoint
        storage_config = storage_cfg(
            pipeline_name=self.pipeline.name,
            start_from_checkpoint=uuid if from_uuid else "latest",
            auth_err=auth_err,
            strict=strict,
            standby=standby,
        )
        self.pipeline.set_runtime_config(
            RuntimeConfig(
                workers=FELDERA_TEST_NUM_WORKERS,
                hosts=FELDERA_TEST_NUM_HOSTS,
                fault_tolerance_model=ft,
                storage=Storage(config=storage_config),
                checkpoint_interval_secs=ft_interval,
            )
        )

        if not standby:
            self.pipeline.start()
        else:
            self.pipeline.start_standby()

            # wait for the pipeline to initialize
            start = time.monotonic()
            # wait for a maximum of 120 seconds for the pipeline to provison
            end = start + 120

            # wait for the pipeline to finish provisoning
            for log in self.pipeline.logs():
                if "checkpoint pulled successfully" in log:
                    break

                if time.monotonic() > end:
                    raise TimeoutError(
                        f"{self.pipeline.name} timed out waiting to pull checkpoint"
                    )

            if standby:
                assert self.pipeline.status() == PipelineStatus.STANDBY
                self.pipeline.activate()

        got_after = list(self.pipeline.query("SELECT * FROM v0"))

        print(
            f"{self.pipeline.name}: after: {len(got_after)}, {got_after}",
            file=sys.stderr,
        )

        if expect_empty:
            got_before = []

        self.assertCountEqual(got_before, got_after)

        self.pipeline.stop(force=True)

        if clear_storage:
            self.pipeline.clear_storage()

    @enterprise_only
    @single_host_only
    def test_from_uuid(self):
        self.test_checkpoint_sync(from_uuid=True)

    @enterprise_only
    @single_host_only
    def test_without_clearing_storage(self):
        self.test_checkpoint_sync(clear_storage=False)

    @enterprise_only
    @single_host_only
    def test_automated_checkpoint(self):
        self.test_checkpoint_sync(ft_interval=5, automated_checkpoint=True)

    @enterprise_only
    @single_host_only
    def test_automated_checkpoint_sync(self):
        self.test_checkpoint_sync(
            ft_interval=5, automated_checkpoint=True, automated_sync_interval=10
        )

    @enterprise_only
    @single_host_only
    def test_automated_checkpoint_sync1(self):
        self.test_checkpoint_sync(ft_interval=5, automated_sync_interval=10)

    @enterprise_only
    @single_host_only
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
    @single_host_only
    def test_autherr_fail(self):
        with self.assertRaisesRegex(RuntimeError, "SignatureDoesNotMatch|Forbidden"):
            self.test_checkpoint_sync(auth_err=True, strict=True)

    @enterprise_only
    @single_host_only
    def test_autherr(self):
        with self.assertRaisesRegex(RuntimeError, "SignatureDoesNotMatch|Forbidden"):
            self.test_checkpoint_sync(auth_err=True, strict=False)

    @enterprise_only
    @single_host_only
    def test_nonexistent_checkpoint_fail(self):
        with self.assertRaisesRegex(RuntimeError, "were not found in source"):
            self.test_checkpoint_sync(random_uuid=True, from_uuid=True, strict=True)

    @enterprise_only
    @single_host_only
    def test_nonexistent_checkpoint(self):
        self.test_checkpoint_sync(
            random_uuid=True, from_uuid=True, strict=False, expect_empty=True
        )

    @enterprise_only
    @single_host_only
    def test_standby_activation(self):
        self.test_checkpoint_sync(standby=True)

    @enterprise_only
    @single_host_only
    def test_standby_activation_from_uuid(self):
        self.test_checkpoint_sync(standby=True, from_uuid=True)

    @enterprise_only
    @single_host_only
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

        # Wait until standby pulls the first checkpoint
        start = time.monotonic()
        end = start + 120
        for log in standby.logs():
            if "checkpoint pulled successfully" in log:
                break
            if time.monotonic() > end:
                raise TimeoutError(
                    "Timed out waiting for standby pipeline to pull checkpoint"
                )

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

        for log in standby.logs():
            if "activated" in log:
                break
            if time.monotonic() > end:
                raise TimeoutError("Timed out waiting for standby pipeline to activate")

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
    @single_host_only
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
    @single_host_only
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

        source_bucket = f"{DEFAULT_BUCKET}/{source.name}"

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
                start = time.monotonic()
                end = start + 120
                for log in self.pipeline.logs():
                    if "checkpoint pulled successfully" in log:
                        break
                    if time.monotonic() > end:
                        raise TimeoutError(
                            "Timed out waiting for standby pipeline to pull from read_bucket"
                        )

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
    @single_host_only
    def test_read_bucket_standby(self):
        # Standby pipeline seeds from read_bucket when bucket is empty.
        self.test_read_bucket(standby=True)

    @enterprise_only
    @single_host_only
    def test_read_bucket_standby_from_uuid(self):
        # Standby pipeline seeds from read_bucket using a specific UUID.
        self.test_read_bucket(standby=True, from_uuid=True)

    @enterprise_only
    @single_host_only
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

        source_bucket = f"{DEFAULT_BUCKET}/{source.name}"

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
    @single_host_only
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

        source_bucket = f"{DEFAULT_BUCKET}/{source.name}"

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
        start = time.monotonic()
        end = start + 120
        for log in standby.logs():
            if "checkpoint pulled successfully" in log:
                break
            if time.monotonic() > end:
                raise TimeoutError(
                    "Timed out waiting for standby to pull from read_bucket"
                )

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
    @single_host_only
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

        source_bucket = f"{DEFAULT_BUCKET}/{source.name}"

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

        source_bucket = f"{DEFAULT_BUCKET}/{source.name}"

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
    @single_host_only
    def test_read_bucket_strict_fail(self):
        # When fail_if_no_checkpoint=True and both the primary bucket and
        # read_bucket are empty, the pipeline must fail to start.
        ft = FaultToleranceModel.AtLeastOnce

        # A bucket path with no checkpoints.
        empty_read_bucket = f"{DEFAULT_BUCKET}/{self.pipeline.name}_read_bucket_empty"

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

        source_bucket = f"{DEFAULT_BUCKET}/{source.name}"

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
