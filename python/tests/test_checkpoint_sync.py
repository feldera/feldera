from tests.shared_test_pipeline import SharedTestPipeline
from tests import enterprise_only
from feldera.runtime_config import RuntimeConfig, Storage
from feldera.enums import PipelineStatus, FaultToleranceModel
from typing import Optional
import os
import sys
import time
from uuid import uuid4
import random


DEFAULT_ENDPOINT = os.environ.get(
    "DEFAULT_MINIO_ENDPOINT", "http://minio.extra.svc.cluster.local:9000"
)
DEFAULT_BUCKET = "default"
ACCESS_KEY = "minio"
SECRET_KEY = "miniopasswd"


def storage_cfg(
    pipeline_name: str,
    endpoint: Optional[str] = None,
    start_from_checkpoint: Optional[str] = None,
    strict: bool = False,
    auth_err: bool = False,
    standby: bool = False,
    pull_interval: int = 2,
) -> dict:
    return {
        "backend": {
            "name": "file",
            "config": {
                "sync": {
                    "bucket": f"{DEFAULT_BUCKET}/{pipeline_name}",
                    "access_key": ACCESS_KEY,
                    "secret_key": SECRET_KEY if not auth_err else SECRET_KEY + "extra",
                    "provider": "Minio",
                    "endpoint": endpoint or DEFAULT_ENDPOINT,
                    "start_from_checkpoint": start_from_checkpoint,
                    "fail_if_no_checkpoint": strict,
                    "standby": standby,
                    "pull_interval": pull_interval,
                }
            },
        }
    }


class TestCheckpointSync(SharedTestPipeline):
    @enterprise_only
    def test_checkpoint_sync(
        self,
        from_uuid: bool = False,
        random_uuid: bool = False,
        clear_storage: bool = True,
        auth_err: bool = False,
        strict: bool = False,
        expect_empty: bool = False,
        standby: bool = False,
    ):
        """
        CREATE TABLE t0 (c0 INT, c1 VARCHAR);
        CREATE MATERIALIZED VIEW v0 AS SELECT * FROM t0;
        """

        storage_config = storage_cfg(self.pipeline.name)
        ft = FaultToleranceModel.AtLeastOnce

        self.pipeline.set_runtime_config(
            RuntimeConfig(
                fault_tolerance_model=ft, storage=Storage(config=storage_config)
            )
        )
        self.pipeline.start()

        random.seed(time.time())
        total = random.randint(10, 20)
        data = [{"c0": i, "c1": str(i)} for i in range(1, total)]
        self.pipeline.input_json("t0", data)
        self.pipeline.execute("INSERT INTO t0 VALUES (21, 'exists')")

        start = time.time()
        timeout = 5

        while True:
            processed = self.pipeline.stats().global_metrics.total_processed_records
            if processed == total:
                break

            if time.time() - start > timeout:
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

        self.pipeline.checkpoint(wait=True)
        uuid = self.pipeline.sync_checkpoint(wait=True)

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
                fault_tolerance_model=ft, storage=Storage(config=storage_config)
            )
        )

        if not standby:
            self.pipeline.start()
        else:
            self.pipeline.start(wait=False)

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
                        f"{self.pipeline.name} timedout waiting to pull checkpoint"
                    )

            if standby:
                # wait for 8 seconds, this should be more than enough time
                time.sleep(8)
                assert self.pipeline.status() == PipelineStatus.INITIALIZING

                self.pipeline.activate(timeout_s=10)

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
    def test_from_uuid(self):
        self.test_checkpoint_sync(from_uuid=True)

    @enterprise_only
    def test_without_clearing_storage(self):
        self.test_checkpoint_sync(clear_storage=False)

    @enterprise_only
    def test_autherr_fail(self):
        with self.assertRaisesRegex(RuntimeError, "SignatureDoesNotMatch"):
            self.test_checkpoint_sync(auth_err=True, strict=True)

    @enterprise_only
    def test_autherr(self):
        self.test_checkpoint_sync(auth_err=True, strict=False, expect_empty=True)

    @enterprise_only
    def test_nonexistent_checkpoint_fail(self):
        with self.assertRaisesRegex(RuntimeError, "were not found in source"):
            self.test_checkpoint_sync(random_uuid=True, from_uuid=True, strict=True)

    @enterprise_only
    def test_nonexistent_checkpoint(self):
        self.test_checkpoint_sync(random_uuid=True, from_uuid=True, expect_empty=True)

    @enterprise_only
    def test_standby_activation(self):
        self.test_checkpoint_sync(standby=True)

    @enterprise_only
    def test_standby_activation_from_uuid(self):
        self.test_checkpoint_sync(standby=True, from_uuid=True)

    @enterprise_only
    def test_standby_fallback(self, from_uuid: bool = False):
        # Step 1: Start main pipeline
        storage_config = storage_cfg(self.pipeline.name)
        ft = FaultToleranceModel.AtLeastOnce
        self.pipeline.set_runtime_config(
            RuntimeConfig(
                fault_tolerance_model=ft, storage=Storage(config=storage_config)
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
        standby.start(wait=False)

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

        assert standby.status() == PipelineStatus.INITIALIZING
        standby.activate(timeout_s=(pull_interval * extra_ckpts) + 60)

        for log in standby.logs():
            if "activated" in log:
                break
            if time.monotonic() > end:
                raise TimeoutError("Timed out waiting for standby pipeline to activate")

        # Step 6: Validate standby has all expected records
        got_after = list(standby.query("SELECT * FROM v0"))
        print(
            f"{standby.name}: final records after activation: {got_after}",
            file=sys.stderr,
        )
        self.assertCountEqual(got_expected, got_after)

        # Cleanup
        standby.stop(force=True)

        standby.start()
        got_final = list(standby.query("SELECT * FROM v0"))
        standby.stop(force=True)

        self.assertCountEqual(got_after, got_final)

        self.pipeline.clear_storage()

    @enterprise_only
    def test_standby_fallback_from_uuid(self):
        self.test_standby_fallback(from_uuid=True)
