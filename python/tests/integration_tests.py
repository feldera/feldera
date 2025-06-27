import unittest
from typing import Optional
from feldera import PipelineBuilder, Pipeline
from feldera.runtime_config import RuntimeConfig, Storage
from tests import TEST_CLIENT

DEFAULT_ENDPOINT = "http://minio.extra.svc.cluster.local:9000"
DEFAULT_BUCKET = "default"
ACCESS_KEY = "minio"
SECRET_KEY = "miniopasswd"


def storage_cfg(
    endpoint: Optional[str] = None,
    start_from_checkpoint: bool = False,
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
                }
            },
        }
    }


class TestIntegrationTests(unittest.TestCase):
    def test_checkpoint_sync(self, auth_err: bool = False, name: Optional[str] = None):
        c = TEST_CLIENT
        storage_config = storage_cfg()

        pipeline_name = name or "test_checkpoint_sync0"
        sql = """
            CREATE TABLE t0 (c0 int, c1 varchar);
            CREATE MATERIALIZED VIEW v0 AS SELECT c0 FROM t0;
        """

        # Create and start pipeline
        p = PipelineBuilder(
            c,
            pipeline_name,
            sql,
            runtime_config=RuntimeConfig(storage=Storage(config=storage_config)),
        ).create_or_replace()
        p.start()

        # Insert data and trigger checkpoint
        data = [{"c0": i, "c1": str(i)} for i in range(1, 4)]
        p.input_json("t0", data)
        p.execute("INSERT INTO t0 VALUES (4, 'exists')")
        got_before = list(p.query("SELECT * FROM v0"))
        print("got before:", got_before)

        p.checkpoint(wait=True)
        p.sync_checkpoint(wait=True)

        p.stop()
        p.unbind_storage()

        # Restart from checkpoint
        storage_config = storage_cfg(start_from_checkpoint=True, auth_err=auth_err)

        p = PipelineBuilder(
            c,
            pipeline_name,
            sql,
            runtime_config=RuntimeConfig(storage=Storage(config=storage_config)),
        ).create_or_replace()
        p.start()
        got_after = list(p.query("SELECT * FROM v0"))

        self.assertCountEqual(got_before, got_after)

        p.stop()
        p.unbind_storage()

    def test_checkpoint_sync_err(self):
        name = "test_checkpoint_sync0"
        with self.assertRaisesRegex(RuntimeError, "SignatureDoesNotMatch"):
            self.test_checkpoint_sync(name=name, auth_err=True)

        p = Pipeline.get(name, TEST_CLIENT)
        p.stop()
        p.delete(True)
