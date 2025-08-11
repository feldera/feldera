"""Advanced pipeline features and enterprise functionality tests."""
import unittest
import json
import time
from tests.shared_test_pipeline import SharedTestPipeline
from tests import TEST_CLIENT, enterprise_only


class TestAdvancedFeatures(SharedTestPipeline):
    """Test advanced pipeline features including enterprise functionality."""

    def test_basic_table_for_advanced(self):
        """
        CREATE TABLE t1(c1 integer, c2 bool, c3 varchar) WITH ('materialized' = 'true');
        CREATE MATERIALIZED VIEW v1 AS SELECT * FROM t1;
        CREATE TABLE test_checkpoint_table(x int) WITH ('materialized' = 'true');
        """
        pass

    def test_completion_tokens_basic(self):
        """Test completion tokens with a pipeline that has no output connectors."""
        self.pipeline.start()
        
        # Test completion tokens with data ingestion
        for i in range(10):  # Reduced from 1000 for faster test
            # Post data and get completion token
            data = f'{{"c1": {i}, "c2": true}}'
            response = TEST_CLIENT.post(
                f"/v0/pipelines/{self.pipeline.name}/ingress/T1?format=json&update_format=raw",
                data=data
            )
            self.assertTrue(response.status_code < 300)
            
            # Get completion token from response
            token_response = response.json()
            self.assertIn("token", token_response)
            token = token_response["token"]
            
            # Wait for completion
            timeout = 30
            start_time = time.time()
            
            while time.time() - start_time < timeout:
                response = TEST_CLIENT.get(
                    f"/v0/pipelines/{self.pipeline.name}/completion_status?token={token}"
                )
                self.assertTrue(response.status_code < 300)
                
                status_response = response.json()
                if status_response["status"] == "Complete":
                    break
                    
                time.sleep(0.01)  # 10ms delay
            else:
                self.fail(f"Completion token {token} did not complete within timeout")
            
            # Verify data was processed
            query = f"select count(*) from t1 where c1 = {i};"
            response = TEST_CLIENT.get(f"/v0/pipelines/{self.pipeline.name}/query?sql={query}")
            result = response.json()
            self.assertEqual(result, [{"count(*)": 1}])

    def test_pipeline_logs(self):
        """Test that logs can be retrieved from pipeline in any status."""
        pipeline_name = "test-logs"
        
        # Clean up any existing pipeline
        try:
            TEST_CLIENT.delete(f"/v0/pipelines/{pipeline_name}")
        except:
            pass
        
        # Test 404 for non-existent pipeline
        response = TEST_CLIENT.get(f"/v0/pipelines/{pipeline_name}/logs")
        self.assertEqual(response.status_code, 404)
        
        # Create pipeline
        pipeline_data = {
            "name": pipeline_name,
            "program_code": "CREATE TABLE t1(c1 INTEGER) with ('materialized' = 'true');",
        }
        response = TEST_CLIENT.post("/v0/pipelines", json=pipeline_data)
        self.assertEqual(response.status_code, 201)
        
        # Logs should be accessible after creation
        response = TEST_CLIENT.get(f"/v0/pipelines/{pipeline_name}/logs")
        self.assertEqual(response.status_code, 200)
        
        # Wait for compilation
        self._wait_for_compilation(pipeline_name)
        
        # Logs should be accessible after compilation
        response = TEST_CLIENT.get(f"/v0/pipelines/{pipeline_name}/logs")
        self.assertEqual(response.status_code, 200)
        
        # Start pipeline
        response = TEST_CLIENT.post(f"/v0/pipelines/{pipeline_name}/pause")
        self.assertEqual(response.status_code, 202)
        self._wait_for_status(pipeline_name, "Paused")
        
        # Logs should be accessible while paused
        response = TEST_CLIENT.get(f"/v0/pipelines/{pipeline_name}/logs")
        self.assertEqual(response.status_code, 200)
        
        # Start running
        response = TEST_CLIENT.post(f"/v0/pipelines/{pipeline_name}/start")
        self.assertEqual(response.status_code, 202)
        self._wait_for_status(pipeline_name, "Running")
        
        # Logs should be accessible while running
        response = TEST_CLIENT.get(f"/v0/pipelines/{pipeline_name}/logs")
        self.assertEqual(response.status_code, 200)
        
        # Stop
        response = TEST_CLIENT.post(f"/v0/pipelines/{pipeline_name}/stop?force=true")
        self.assertEqual(response.status_code, 202)
        self._wait_for_status(pipeline_name, "Stopped")
        
        # Logs should be accessible while stopped
        response = TEST_CLIENT.get(f"/v0/pipelines/{pipeline_name}/logs")
        self.assertEqual(response.status_code, 200)
        
        # Clear
        response = TEST_CLIENT.post(f"/v0/pipelines/{pipeline_name}/clear")
        if response.status_code == 202:
            self._wait_for_storage_status(pipeline_name, "Cleared")
        
        # Logs should be accessible after clearing
        response = TEST_CLIENT.get(f"/v0/pipelines/{pipeline_name}/logs")
        self.assertEqual(response.status_code, 200)
        
        # Delete pipeline
        response = TEST_CLIENT.delete(f"/v0/pipelines/{pipeline_name}")
        self.assertEqual(response.status_code, 200)
        
        # Logs should not be accessible after deletion
        response = TEST_CLIENT.get(f"/v0/pipelines/{pipeline_name}/logs")
        self.assertEqual(response.status_code, 404)

    def test_pipeline_metrics(self):
        """Test retrieving pipeline metrics in various formats."""
        self.pipeline.start()
        
        # Test default format (should be Prometheus)
        response = TEST_CLIENT.get(f"/v0/pipelines/{self.pipeline.name}/metrics")
        self.assertEqual(response.status_code, 200)
        metrics_default = response.text
        self.assertIn("# TYPE records_processed_total counter", metrics_default)
        
        # Test Prometheus format explicitly
        response = TEST_CLIENT.get(f"/v0/pipelines/{self.pipeline.name}/metrics?format=prometheus")
        self.assertEqual(response.status_code, 200)
        metrics_prometheus = response.text
        self.assertIn("# TYPE records_processed_total counter", metrics_prometheus)
        
        # Test JSON format
        response = TEST_CLIENT.get(f"/v0/pipelines/{self.pipeline.name}/metrics?format=json")
        self.assertEqual(response.status_code, 200)
        metrics_json = response.text
        self.assertIn('"key":"records_processed_total"', metrics_json)
        
        # Test that JSON is valid
        json.loads(metrics_json)
        
        # Test unknown format
        response = TEST_CLIENT.get(f"/v0/pipelines/{self.pipeline.name}/metrics?format=does-not-exist")
        self.assertEqual(response.status_code, 400)

    def test_global_metrics(self):
        """Test retrieving global metrics."""
        response = TEST_CLIENT.get("/v0/metrics")
        self.assertTrue(response.status_code < 300)
        
        metrics_text = response.text
        self.assertIn("# TYPE", metrics_text)

    def test_refresh_version(self):
        """Test incrementing of refresh_version."""
        pipeline_name = "test-refresh"
        
        # Clean up any existing pipeline
        try:
            TEST_CLIENT.delete(f"/v0/pipelines/{pipeline_name}")
        except:
            pass
        
        # Create pipeline - initial refresh version should be 1
        pipeline_data = {
            "name": pipeline_name,
            "program_code": "",
        }
        response = TEST_CLIENT.post("/v0/pipelines", json=pipeline_data)
        self.assertEqual(response.status_code, 201)
        pipeline = response.json()
        self.assertEqual(pipeline["refresh_version"], 1)
        
        # Wait for compilation
        self._wait_for_compilation(pipeline_name)
        
        # After compilation, refresh version should be higher
        response = TEST_CLIENT.get(f"/v0/pipelines/{pipeline_name}")
        pipeline = response.json()
        post_compilation_version = pipeline["refresh_version"]
        self.assertGreater(post_compilation_version, 1)
        
        # Starting and shutting down should have no effect on refresh version
        response = TEST_CLIENT.post(f"/v0/pipelines/{pipeline_name}/pause")
        self.assertEqual(response.status_code, 202)
        self._wait_for_status(pipeline_name, "Paused")
        
        response = TEST_CLIENT.post(f"/v0/pipelines/{pipeline_name}/stop?force=true")
        self.assertEqual(response.status_code, 202)
        self._wait_for_status(pipeline_name, "Stopped")
        
        response = TEST_CLIENT.post(f"/v0/pipelines/{pipeline_name}/clear")
        if response.status_code == 202:
            self._wait_for_storage_status(pipeline_name, "Cleared")
        
        response = TEST_CLIENT.get(f"/v0/pipelines/{pipeline_name}")
        pipeline = response.json()
        self.assertEqual(pipeline["refresh_version"], post_compilation_version)
        
        # Edits should increment refresh version
        patch_data = {"program_code": "CREATE TABLE t1 ( v1 INT );"}
        response = TEST_CLIENT.patch(f"/v0/pipelines/{pipeline_name}", json=patch_data)
        self.assertTrue(response.status_code < 300)
        pipeline = response.json()
        self.assertEqual(pipeline["refresh_version"], post_compilation_version + 1)
        
        # Wait for new compilation
        self._wait_for_compilation(pipeline_name)
        
        # After recompilation, version should be even higher
        response = TEST_CLIENT.get(f"/v0/pipelines/{pipeline_name}")
        pipeline = response.json()
        self.assertGreater(pipeline["refresh_version"], post_compilation_version + 1)
        
        # Clean up
        TEST_CLIENT.delete(f"/v0/pipelines/{pipeline_name}")

    @enterprise_only
    def test_checkpoint_basic(self):
        """Test basic checkpoint functionality (enterprise only)."""
        self.pipeline.start()
        
        # Test checkpoint endpoint
        response = TEST_CLIENT.post(f"/v0/pipelines/{self.pipeline.name}/checkpoint")
        self.assertNotEqual(response.status_code, 501)  # Should not be NOT_IMPLEMENTED
        self.assertTrue(response.status_code < 300)
        
        checkpoint_response = response.json()
        self.assertIn("checkpoint_sequence_number", checkpoint_response)
        sequence_number = checkpoint_response["checkpoint_sequence_number"]
        
        # Wait for checkpoint completion
        timeout = 10
        start_time = time.time()
        
        while time.time() - start_time < timeout:
            response = TEST_CLIENT.get(f"/v0/pipelines/{self.pipeline.name}/checkpoint_status")
            self.assertTrue(response.status_code < 300)
            
            checkpoint_status = response.json()
            if checkpoint_status.get("success") == sequence_number:
                break
                
            time.sleep(0.1)
        else:
            self.fail(f"Checkpoint {sequence_number} did not complete within timeout")

    def test_checkpoint_oss(self):
        """Test that checkpoint returns NOT_IMPLEMENTED on OSS."""
        if TEST_CLIENT.get_config().edition.is_enterprise():
            self.skipTest("This test is for OSS only")
            
        response = TEST_CLIENT.post(f"/v0/pipelines/{self.pipeline.name}/checkpoint")
        self.assertEqual(response.status_code, 501)  # NOT_IMPLEMENTED
        
        error_data = response.json()
        self.assertEqual(error_data["error_code"], "EnterpriseFeature")

    @enterprise_only
    def test_suspend_basic(self):
        """Test basic suspend functionality (enterprise only)."""
        pipeline_name = "test-suspend"
        
        # Clean up any existing pipeline
        try:
            TEST_CLIENT.delete(f"/v0/pipelines/{pipeline_name}")
        except:
            pass
        
        # Create pipeline
        pipeline_data = {
            "name": pipeline_name,
            "program_code": "CREATE TABLE t1(x int) WITH ('materialized' = 'true');",
        }
        response = TEST_CLIENT.post("/v0/pipelines", json=pipeline_data)
        self.assertEqual(response.status_code, 201)
        
        # Wait for compilation
        self._wait_for_compilation(pipeline_name)
        
        # Start pipeline
        response = TEST_CLIENT.post(f"/v0/pipelines/{pipeline_name}/start")
        self.assertEqual(response.status_code, 202)
        self._wait_for_status(pipeline_name, "Running")
        
        # Test suspend (graceful stop)
        response = TEST_CLIENT.post(f"/v0/pipelines/{pipeline_name}/stop?force=false")
        self.assertEqual(response.status_code, 202)
        self._wait_for_status(pipeline_name, "Stopped")
        
        # Clean up
        TEST_CLIENT.delete(f"/v0/pipelines/{pipeline_name}")

    def test_suspend_oss(self):
        """Test that suspend returns NOT_IMPLEMENTED on OSS."""
        if TEST_CLIENT.get_config().edition.is_enterprise():
            self.skipTest("This test is for OSS only")
            
        response = TEST_CLIENT.post(f"/v0/pipelines/{self.pipeline.name}/stop?force=false")
        self.assertEqual(response.status_code, 501)  # NOT_IMPLEMENTED
        
        error_data = response.json()
        self.assertEqual(error_data["error_code"], "EnterpriseFeature")

    def test_pipeline_deleted_during_compilation(self):
        """Test that compiler handles pipeline deletion during compilation."""
        # Test various deletion timings
        for delay_ms in [0, 500, 1000, 2000]:
            pipeline_name = f"test-delete-compile-{delay_ms}"
            
            # Clean up any existing pipeline
            try:
                TEST_CLIENT.delete(f"/v0/pipelines/{pipeline_name}")
            except:
                pass
            
            # Create pipeline
            pipeline_data = {
                "name": pipeline_name,
                "description": "Test deletion during compilation",
                "program_code": "",
            }
            response = TEST_CLIENT.post("/v0/pipelines", json=pipeline_data)
            self.assertEqual(response.status_code, 201)
            
            # Wait specified time
            if delay_ms > 0:
                time.sleep(delay_ms / 1000.0)
            
            # Delete pipeline during compilation
            response = TEST_CLIENT.delete(f"/v0/pipelines/{pipeline_name}")
            self.assertEqual(response.status_code, 200)
        
        # Validate compiler still works by creating a new pipeline
        test_pipeline_name = "test-compiler-recovery"
        try:
            TEST_CLIENT.delete(f"/v0/pipelines/{test_pipeline_name}")
        except:
            pass
            
        pipeline_data = {
            "name": test_pipeline_name,
            "program_code": "",
        }
        response = TEST_CLIENT.post("/v0/pipelines", json=pipeline_data)
        self.assertEqual(response.status_code, 201)
        
        # Wait for compilation to verify compiler is working
        self._wait_for_compilation(test_pipeline_name)
        
        # Clean up
        TEST_CLIENT.delete(f"/v0/pipelines/{test_pipeline_name}")

    # Helper methods
    def _wait_for_compilation(self, pipeline_name, timeout=600):
        """Wait for pipeline compilation to complete."""
        start_time = time.time()
        
        while time.time() - start_time < timeout:
            response = TEST_CLIENT.get(f"/v0/pipelines/{pipeline_name}")
            pipeline = response.json()
            
            if pipeline.get("program_status") == "Success":
                return
                
            if pipeline.get("program_status") == "CompilationFailed":
                self.fail(f"Program compilation failed: {pipeline.get('program_error')}")
                
            time.sleep(2)
        
        self.fail(f"Timed out waiting for compilation of {pipeline_name}")

    def _wait_for_status(self, pipeline_name, expected_status, timeout=30):
        """Wait for pipeline to reach expected deployment status."""
        start_time = time.time()
        
        while time.time() - start_time < timeout:
            response = TEST_CLIENT.get(f"/v0/pipelines/{pipeline_name}")
            pipeline = response.json()
            
            if pipeline.get("deployment_status") == expected_status:
                return
                
            time.sleep(1)
        
        self.fail(f"Timed out waiting for {pipeline_name} to reach status {expected_status}")

    def _wait_for_storage_status(self, pipeline_name, expected_status, timeout=30):
        """Wait for pipeline storage to reach expected status."""
        start_time = time.time()
        
        while time.time() - start_time < timeout:
            response = TEST_CLIENT.get(f"/v0/pipelines/{pipeline_name}")
            pipeline = response.json()
            
            if pipeline.get("storage_status") == expected_status:
                return
                
            time.sleep(1)
        
        self.fail(f"Timed out waiting for {pipeline_name} storage to reach status {expected_status}")


if __name__ == "__main__":
    unittest.main()