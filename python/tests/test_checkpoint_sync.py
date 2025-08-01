from tests.shared_test_pipeline import SharedTestPipeline
from tests import enterprise_only
from feldera.runtime_config import RuntimeConfig, Storage
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
    ):
        """
        CREATE TABLE t0 (c0 INT, c1 VARCHAR);
        CREATE MATERIALIZED VIEW v0 AS SELECT * FROM t0;
        """

        storage_config = storage_cfg(self.pipeline.name)

        self.pipeline.set_runtime_config(
            RuntimeConfig(storage=Storage(config=storage_config))
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
        )
        self.pipeline.set_runtime_config(
            RuntimeConfig(storage=Storage(config=storage_config))
        )
        self.pipeline.start()
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
