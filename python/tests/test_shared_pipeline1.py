from typing import Optional
from feldera.runtime_config import RuntimeConfig, Storage
from tests import enterprise_only
from tests.shared_test_pipeline import SharedTestPipeline


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


class TestCheckpointSync(SharedTestPipeline):
    @enterprise_only
    def test_checkpoint_sync(self, auth_err: bool = False):
        """
        CREATE TABLE t0 (c0 INT, c1 VARCHAR);
        CREATE MATERIALIZED VIEW v0 AS SELECT c0 FROM t0;
        """
        storage_config = storage_cfg()

        self.set_runtime_config(RuntimeConfig(storage=Storage(config=storage_config)))
        self.pipeline.start()

        data = [{"c0": i, "c1": str(i)} for i in range(1, 4)]
        self.pipeline.input_json("t0", data)
        self.pipeline.execute("INSERT INTO t0 VALUES (4, 'exists')")
        got_before = list(self.pipeline.query("SELECT * FROM v0"))

        self.pipeline.checkpoint(wait=True)
        self.pipeline.sync_checkpoint(wait=True)

        self.pipeline.stop(force=True)
        self.pipeline.clear_storage()

        # Restart pipeline from checkpoint
        storage_config = storage_cfg(start_from_checkpoint=True, auth_err=auth_err)
        self.set_runtime_config(RuntimeConfig(storage=Storage(config=storage_config)))
        self.pipeline.start()
        got_after = list(self.pipeline.query("SELECT * FROM v0"))

        self.assertCountEqual(got_before, got_after)

        self.pipeline.stop(force=True)
        self.pipeline.clear_storage()

    @enterprise_only
    def test_checkpoint_sync_err(self):
        with self.assertRaisesRegex(RuntimeError, "SignatureDoesNotMatch"):
            self.test_checkpoint_sync(auth_err=True)
