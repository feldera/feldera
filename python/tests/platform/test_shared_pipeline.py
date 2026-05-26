import gzip
import io
import json
import os
import tempfile
import time
import unittest
import zipfile

from feldera import Pipeline
from feldera.enums import PipelineFieldSelector, PipelineStatus
from feldera.rest.errors import FelderaAPIError
from feldera.runtime_config import RuntimeConfig
from tests import TEST_CLIENT, enterprise_only
from tests.shared_test_pipeline import SharedTestPipeline, sql
from tests.helper import http_request, API_PREFIX, wait_for_condition
from feldera.testutils import (
    FELDERA_TEST_NUM_WORKERS,
    FELDERA_TEST_NUM_HOSTS,
)


BASE_SQL = """
CREATE TABLE tbl(id INT) WITH ('materialized' = 'true', 'connectors' = '[{
    "name": "d1",
    "paused": true,
    "transport": {
        "name": "datagen",
        "config": {
            "plan": [{
                "limit": 10,
                "rate": 1
            }]
        }
    }
}]');
CREATE MATERIALIZED VIEW v0 AS SELECT * FROM tbl;
CREATE MATERIALIZED VIEW "V0" AS SELECT * FROM tbl WHERE id % 2 <> 0;
CREATE MATERIALIZED VIEW "DATE" AS SELECT * FROM tbl WHERE id % 2 = 0;
"""


class TestPipeline(SharedTestPipeline):
    @sql(BASE_SQL)
    def test_list_pipelines(self):
        pipelines = TEST_CLIENT.pipelines()
        assert len(pipelines) > 0
        assert self.pipeline.name in [p.name for p in pipelines]

    def test_get_pipeline(self):
        p = TEST_CLIENT.get_pipeline(self.pipeline.name, PipelineFieldSelector.ALL)
        assert self.pipeline.name == p.name

    def test_get_pipeline_config(self):
        config = TEST_CLIENT.get_runtime_config(self.pipeline.name)
        assert config is not None

    def test_get_pipeline_stats(self):
        self.pipeline.start()
        stats = TEST_CLIENT.get_pipeline_stats(self.pipeline.name)
        assert stats is not None
        assert stats.get("global_metrics") is not None
        assert stats.get("inputs") is not None
        assert stats.get("outputs") is not None

    def test_start_compaction(self):
        self.pipeline.start()
        self.pipeline.input_json("tbl", [{"id": 1}, {"id": 2}])

        before = list(self.pipeline.query("SELECT COUNT(*) AS num_rows FROM v0"))
        self.pipeline.start_compaction()
        self.pipeline.start_compaction()
        after = list(self.pipeline.query("SELECT COUNT(*) AS num_rows FROM v0"))

        assert after == before

    @sql("CREATE VIEW id_plus_one AS SELECT id + 1 FROM tbl;")
    def test_failed_pipeline_stop(self):
        self.pipeline.start()
        data = [{"id": 2147483647}]
        self.pipeline.input_json("tbl", data, wait=False)
        wait_for_condition(
            "pipeline stops with deployment error after worker panic",
            lambda: (
                self.pipeline.status() == PipelineStatus.STOPPED
                and len(self.pipeline.deployment_error()) > 0
            ),
            timeout_s=20.0,
            poll_interval_s=1.0,
        )
        self.pipeline.stop(force=True)

    def test_pipeline_resource_config(self):
        from feldera.runtime_config import Resources, RuntimeConfig

        config = {
            "cpu_cores_max": 3,
            "cpu_cores_min": 2,
            "memory_mb_max": 500,
            "memory_mb_min": 300,
            "storage_mb_max": None,
            "storage_class": None,
            "namespace": None,
            "service_account_name": None,
        }

        resources = Resources(config)
        self.pipeline.set_runtime_config(
            RuntimeConfig(
                workers=FELDERA_TEST_NUM_WORKERS,
                hosts=FELDERA_TEST_NUM_HOSTS,
                resources=resources,
            )
        )
        self.pipeline.start()
        got = TEST_CLIENT.get_pipeline(
            self.pipeline.name, PipelineFieldSelector.ALL
        ).runtime_config["resources"]
        assert got == config

    def test_support_bundle(self):
        self.pipeline.start()

        # All collections enabled.
        support_bundle_bytes = self.pipeline.support_bundle()
        assert isinstance(support_bundle_bytes, bytes)
        assert len(support_bundle_bytes) > 0

        try:
            with zipfile.ZipFile(io.BytesIO(support_bundle_bytes), "r") as zip_file:
                file_list = zip_file.namelist()
                # Files now live inside per-collection timestamp directories.
                assert any(item.endswith("/circuit_profile.json") for item in file_list)
                assert "metadata.txt" in file_list
                assert "metadata.json" in file_list
        except zipfile.BadZipFile:
            self.fail("Support bundle is not a valid ZIP file")

        with tempfile.NamedTemporaryFile(suffix=".zip", delete=False) as temp_file:
            temp_path = temp_file.name

        try:
            self.pipeline.support_bundle(output_path=temp_path)

            assert os.path.exists(temp_path)
            with zipfile.ZipFile(temp_path, "r") as zip_file:
                file_list = zip_file.namelist()
                assert len(file_list) > 0
                assert "metadata.json" in file_list
        finally:
            if os.path.exists(temp_path):
                os.unlink(temp_path)

    def test_support_bundle_with_selectors(self):
        self.pipeline.start()

        # All collections disabled.
        support_bundle_bytes = self.pipeline.support_bundle(
            circuit_profile=False,
            heap_profile=False,
            metrics=False,
            logs=False,
            stats=False,
            pipeline_config=False,
            system_config=False,
            dataflow_graph=False,
            pipeline_events=False,
        )

        assert isinstance(support_bundle_bytes, bytes)
        assert len(support_bundle_bytes) > 0

        with zipfile.ZipFile(io.BytesIO(support_bundle_bytes), "r") as zip_file:
            file_list = zip_file.namelist()
            # Only the two metadata files survive when every collection is disabled.
            assert sorted(file_list) == ["metadata.json", "metadata.txt"]

        # Partial collections enabled.
        support_bundle_bytes = self.pipeline.support_bundle(
            circuit_profile=True,
            heap_profile=False,
            metrics=True,
            logs=False,
            stats=True,
            pipeline_config=False,
            system_config=True,
            pipeline_events=False,
        )

        assert isinstance(support_bundle_bytes, bytes)
        assert len(support_bundle_bytes) > 0

        with zipfile.ZipFile(io.BytesIO(support_bundle_bytes), "r") as zip_file:
            file_list = zip_file.namelist()
            assert len(file_list) >= 3
            assert "metadata.txt" in file_list
            assert "metadata.json" in file_list

    def test_support_bundle_limit(self):
        # limit=1 with collect=True yields exactly one collection directory.
        self.pipeline.start()

        support_bundle_bytes = self.pipeline.support_bundle(limit=1)
        assert isinstance(support_bundle_bytes, bytes)

        with zipfile.ZipFile(io.BytesIO(support_bundle_bytes), "r") as zip_file:
            file_list = zip_file.namelist()
            collection_dirs = {
                item.split("/", 1)[0] for item in file_list if "/" in item
            }
            assert len(collection_dirs) == 1

    def test_listen_non_existent_view_paused(self):
        self.pipeline.start_paused()
        with self.assertRaises(ValueError):
            self.pipeline.listen("FrodoBagginsInMordor")

    def test_listen_non_existent_view_running(self):
        self.pipeline.start()
        with self.assertRaises(ValueError):
            self.pipeline.listen("FrodoBagginsInMordor")

    def test_pipelines(self):
        assert self.pipeline.name in [p.name for p in Pipeline.all(TEST_CLIENT)]

    @enterprise_only
    def test_samply_profile(self):
        self.pipeline.set_runtime_config(
            RuntimeConfig(dev_tweaks={"profiling": "samply"})
        )
        self.pipeline.start()

        duration = 5
        self.pipeline.start_samply_profile(duration)
        time.sleep(duration)

        timeout = time.monotonic() + 5

        samply_profile_bytes = None

        while time.monotonic() < timeout:
            try:
                samply_profile_bytes = self.pipeline.get_samply_profile()
                if samply_profile_bytes:
                    break
                time.sleep(0.1)
            except FelderaAPIError as e:
                if e.status_code == 500:
                    raise

        assert isinstance(samply_profile_bytes, bytes)
        assert len(samply_profile_bytes) > 0

        try:
            with gzip.GzipFile(fileobj=io.BytesIO(samply_profile_bytes)) as gz:
                chunk = gz.read(10)
                assert len(chunk) > 0
        except OSError:
            self.fail("Samply profile is not a valid GZIP file")

    # Verify /circuit_profile returns a valid ZIP via the streaming proxy
    # (Transfer-Encoding: chunked, Content-Encoding: identity since ZIP
    # is already compressed).
    def test_circuit_profile_streaming(self):
        self.pipeline.start()

        resp = http_request(
            "GET",
            f"{API_PREFIX}/pipelines/{self.pipeline.name}/circuit_profile",
            timeout=120,
        )
        assert resp.status_code == 200, (
            f"Expected 200, got {resp.status_code}: {resp.text[:200]}"
        )

        try:
            with zipfile.ZipFile(io.BytesIO(resp.content), "r") as zf:
                assert len(zf.namelist()) > 0, "ZIP is empty"
        except zipfile.BadZipFile:
            self.fail("circuit_profile did not return a valid ZIP")

        ce = resp.headers.get("content-encoding", "identity")
        assert ce == "identity", (
            f"Expected Content-Encoding: identity for ZIP, got {ce}"
        )

        # The streaming proxy sets Transfer-Encoding: chunked;
        # the buffered path would set Content-Length instead.
        assert resp.headers.get("transfer-encoding") == "chunked", (
            "Expected Transfer-Encoding: chunked (streaming proxy)"
        )

    # Verify /circuit_json_profile streams valid JSON via the streaming proxy
    # and that the gzip round-trip is transparent (compressed and uncompressed
    # responses yield the same data).
    def test_circuit_json_profile_streaming(self):
        self.pipeline.start()

        # `requests` auto-decompresses gzip, so resp.content is plain JSON.
        resp_gzip = http_request(
            "GET",
            f"{API_PREFIX}/pipelines/{self.pipeline.name}/circuit_json_profile",
            headers={"Accept-Encoding": "gzip"},
            timeout=120,
        )
        assert resp_gzip.status_code == 200, (
            f"Expected 200, got {resp_gzip.status_code}: {resp_gzip.text[:200]}"
        )
        try:
            data_gzip = resp_gzip.json()
        except json.JSONDecodeError:
            self.fail("circuit_json_profile (gzip) did not return valid JSON")
        assert isinstance(data_gzip, (dict, list))

        assert resp_gzip.headers.get("transfer-encoding") == "chunked", (
            "Expected Transfer-Encoding: chunked (streaming proxy)"
        )

        resp_plain = http_request(
            "GET",
            f"{API_PREFIX}/pipelines/{self.pipeline.name}/circuit_json_profile",
            headers={"Accept-Encoding": "identity"},
            timeout=120,
        )
        assert resp_plain.status_code == 200
        try:
            data_plain = resp_plain.json()
        except json.JSONDecodeError:
            self.fail("circuit_json_profile (identity) did not return valid JSON")

        assert data_gzip == data_plain, (
            "Compressed and uncompressed responses returned different data"
        )


if __name__ == "__main__":
    unittest.main()
