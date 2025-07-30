from tests.shared_test_pipeline import SharedTestPipeline
from tests import enterprise_only
from feldera.runtime_config import RuntimeConfig, Storage
from typing import Optional
import os
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
                    "bucket": DEFAULT_BUCKET,
                    "access_key": ACCESS_KEY,
                    "secret_key": SECRET_KEY if not auth_err else SECRET_KEY + "extra",
                    "provider": "Minio",
                    "endpoint": endpoint or DEFAULT_ENDPOINT,
                    "start_from_checkpoint": start_from_checkpoint,
                    "strict_start_from": strict,
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
        CREATE MATERIALIZED VIEW v0 AS SELECT c0 FROM t0;
        """
        storage_config = storage_cfg()

        self.set_runtime_config(RuntimeConfig(storage=Storage(config=storage_config)))
        self.pipeline.start()

        random.seed(time.time())
        data = [{"c0": i, "c1": str(i)} for i in range(1, random.randint(10, 20))]
        self.pipeline.input_json("t0", data)
        self.pipeline.execute("INSERT INTO t0 VALUES (4, 'exists')")
        got_before = list(self.pipeline.query("SELECT * FROM v0"))

        self.pipeline.checkpoint(wait=True)
        uuid = self.pipeline.sync_checkpoint(wait=True)

        self.pipeline.stop(force=True)

        if clear_storage:
            self.pipeline.clear_storage()

        if random_uuid:
            uuid = uuid4()

        # Restart pipeline from checkpoint
        storage_config = storage_cfg(
            start_from_checkpoint=uuid if from_uuid else "latest",
            auth_err=auth_err,
            strict=strict,
        )
        self.set_runtime_config(RuntimeConfig(storage=Storage(config=storage_config)))
        self.pipeline.start()
        got_after = list(self.pipeline.query("SELECT * FROM v0"))

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
