"""Pipeline orchestration and lifecycle tests converted from Rust integration tests."""
import unittest
import json
import time
from tests.shared_test_pipeline import SharedTestPipeline
from tests import TEST_CLIENT, enterprise_only


class TestPipelineOrchestration(SharedTestPipeline):
    """Test pipeline lifecycle operations: start, stop, pause, clear."""

    def test_basic_table(self):
        """
        CREATE TABLE t1(c1 INTEGER) WITH ('materialized' = 'true');
        CREATE MATERIALIZED VIEW v1 AS SELECT * FROM t1;
        """
        pass

    def test_pipeline_create_compile_delete(self):
        """Test creating a pipeline, waiting for compilation, and deleting it."""
        pipeline_name = "test-compile"
        
        # Clean up any existing pipeline
        try:
            TEST_CLIENT.delete(f"/v0/pipelines/{pipeline_name}")
        except:
            pass
        
        # Create pipeline
        pipeline_data = {
            "name": pipeline_name,
            "description": "Test compilation",
            "runtime_config": {},
            "program_code": "CREATE TABLE t1(c1 INTEGER);",
            "program_config": {}
        }
        response = TEST_CLIENT.post("/v0/pipelines", json=pipeline_data)
        self.assertEqual(response.status_code, 201)
        
        # Get pipeline to verify creation
        response = TEST_CLIENT.get(f"/v0/pipelines/{pipeline_name}")
        self.assertEqual(response.status_code, 200)
        
        # Wait for compilation (up to 10 minutes)
        timeout = 600
        start_time = time.time()
        
        while time.time() - start_time < timeout:
            response = TEST_CLIENT.get(f"/v0/pipelines/{pipeline_name}")
            pipeline = response.json()
            
            if pipeline.get("program_status") == "Success":
                break
                
            if pipeline.get("program_status") == "CompilationFailed":
                self.fail(f"Program compilation failed: {pipeline.get('program_error')}")
                
            time.sleep(2)
        else:
            self.fail("Timed out waiting for program compilation")
        
        # Delete pipeline
        response = TEST_CLIENT.delete(f"/v0/pipelines/{pipeline_name}")
        self.assertEqual(response.status_code, 200)
        
        # Verify deletion
        response = TEST_CLIENT.get(f"/v0/pipelines/{pipeline_name}")
        self.assertEqual(response.status_code, 404)

    def test_deploy_pipeline(self):
        """Test deploying, running, and managing a pipeline."""
        pipeline_name = "test-deploy"
        
        # Clean up any existing pipeline
        try:
            TEST_CLIENT.delete(f"/v0/pipelines/{pipeline_name}")
        except:
            pass
        
        # Create and prepare pipeline
        pipeline_data = {
            "name": pipeline_name,
            "program_code": "CREATE TABLE t1(c1 INTEGER) with ('materialized' = 'true'); CREATE MATERIALIZED VIEW v1 AS SELECT * FROM t1;",
        }
        response = TEST_CLIENT.post("/v0/pipelines", json=pipeline_data)
        self.assertEqual(response.status_code, 201)
        
        # Wait for compilation
        self._wait_for_compilation(pipeline_name)
        
        # Start pipeline
        response = TEST_CLIENT.post(f"/v0/pipelines/{pipeline_name}/start")
        self.assertEqual(response.status_code, 202)
        
        # Wait for running status
        self._wait_for_status(pipeline_name, "Running")
        
        # Push some data
        data = "1\n2\n3\n"
        response = TEST_CLIENT.post(f"/v0/pipelines/{pipeline_name}/ingress/t1", data=data)
        self.assertTrue(response.status_code < 300)
        
        # Push more data with Windows-style newlines
        data = "4\r\n5\r\n6"
        response = TEST_CLIENT.post(f"/v0/pipelines/{pipeline_name}/ingress/t1", data=data)
        self.assertTrue(response.status_code < 300)
        
        # Pause pipeline
        response = TEST_CLIENT.post(f"/v0/pipelines/{pipeline_name}/pause")
        self.assertEqual(response.status_code, 202)
        
        # Wait for paused status
        self._wait_for_status(pipeline_name, "Paused")
        
        # Query data in paused state
        query = "select * from t1 order by c1;"
        response = TEST_CLIENT.get(f"/v0/pipelines/{pipeline_name}/query?sql={query}")
        self.assertEqual(response.status_code, 200)
        result = response.json()
        expected = [{"c1": 1}, {"c1": 2}, {"c1": 3}, {"c1": 4}, {"c1": 5}, {"c1": 6}]
        self.assertEqual(result, expected)
        
        # Start pipeline again
        response = TEST_CLIENT.post(f"/v0/pipelines/{pipeline_name}/start")
        self.assertEqual(response.status_code, 202)
        
        # Wait for running status
        self._wait_for_status(pipeline_name, "Running")
        
        # Query data in running state
        response = TEST_CLIENT.get(f"/v0/pipelines/{pipeline_name}/query?sql={query}")
        self.assertEqual(response.status_code, 200)
        result = response.json()
        self.assertEqual(result, expected)
        
        # Stop and clear
        self._stop_force_and_clear(pipeline_name)
        
        # Clean up
        TEST_CLIENT.delete(f"/v0/pipelines/{pipeline_name}")

    def test_pipeline_panic_handling(self):
        """Test that pipeline panics are correctly reported."""
        pipeline_name = "test-panic"
        
        # Clean up any existing pipeline
        try:
            TEST_CLIENT.delete(f"/v0/pipelines/{pipeline_name}")
        except:
            pass
        
        # Create pipeline with SQL that will panic
        pipeline_data = {
            "name": pipeline_name,
            "program_code": "CREATE TABLE t1(c1 INTEGER); CREATE VIEW v1 AS SELECT ELEMENT(ARRAY [2, 3]) FROM t1;",
        }
        response = TEST_CLIENT.post("/v0/pipelines", json=pipeline_data)
        self.assertEqual(response.status_code, 201)
        
        # Wait for compilation
        self._wait_for_compilation(pipeline_name)
        
        # Start pipeline
        response = TEST_CLIENT.post(f"/v0/pipelines/{pipeline_name}/start")
        self.assertEqual(response.status_code, 202)
        
        # Wait for running status
        self._wait_for_status(pipeline_name, "Running")
        
        # Push data that should cause panic
        data = "1\n2\n3\n"
        response = TEST_CLIENT.post(f"/v0/pipelines/{pipeline_name}/ingress/t1", data=data)
        
        # Wait for error status
        timeout = 60
        start_time = time.time()
        
        while time.time() - start_time < timeout:
            response = TEST_CLIENT.get(f"/v0/pipelines/{pipeline_name}")
            pipeline = response.json()
            
            if pipeline.get("deployment_status") == "Failed":
                error = pipeline.get("deployment_error", {})
                self.assertEqual(error.get("error_code"), "RuntimeError.WorkerPanic")
                break
                
            time.sleep(1)
        else:
            self.fail("Timed out waiting for pipeline panic to be detected")
        
        # Clean up
        self._stop_force_and_clear(pipeline_name)
        TEST_CLIENT.delete(f"/v0/pipelines/{pipeline_name}")

    def test_pipeline_restart(self):
        """Test starting, stopping, starting and stopping again."""
        pipeline_name = "test-restart"
        
        # Clean up any existing pipeline
        try:
            TEST_CLIENT.delete(f"/v0/pipelines/{pipeline_name}")
        except:
            pass
        
        # Create pipeline
        pipeline_data = {
            "name": pipeline_name,
            "program_code": "CREATE TABLE t1(c1 INTEGER); CREATE VIEW v1 AS SELECT * FROM t1;",
        }
        response = TEST_CLIENT.post("/v0/pipelines", json=pipeline_data)
        self.assertEqual(response.status_code, 201)
        
        # Wait for compilation
        self._wait_for_compilation(pipeline_name)
        
        # Start -> Stop -> Start -> Stop
        response = TEST_CLIENT.post(f"/v0/pipelines/{pipeline_name}/start")
        self.assertEqual(response.status_code, 202)
        self._wait_for_status(pipeline_name, "Running")
        
        response = TEST_CLIENT.post(f"/v0/pipelines/{pipeline_name}/stop?force=true")
        self.assertEqual(response.status_code, 202)
        self._wait_for_status(pipeline_name, "Stopped")
        
        response = TEST_CLIENT.post(f"/v0/pipelines/{pipeline_name}/start")
        self.assertEqual(response.status_code, 202)
        self._wait_for_status(pipeline_name, "Running")
        
        response = TEST_CLIENT.post(f"/v0/pipelines/{pipeline_name}/stop?force=true")
        self.assertEqual(response.status_code, 202)
        self._wait_for_status(pipeline_name, "Stopped")
        
        # Clean up
        self._stop_force_and_clear(pipeline_name)
        TEST_CLIENT.delete(f"/v0/pipelines/{pipeline_name}")

    def test_pipeline_stop_force_after_start(self):
        """Test stopping pipeline at various stages after starting."""
        pipeline_name = "test-stop-force"
        
        # Clean up any existing pipeline
        try:
            TEST_CLIENT.delete(f"/v0/pipelines/{pipeline_name}")
        except:
            pass
        
        # Create pipeline
        pipeline_data = {
            "name": pipeline_name,
            "program_code": "CREATE TABLE t1(c1 INTEGER);",
        }
        response = TEST_CLIENT.post("/v0/pipelines", json=pipeline_data)
        self.assertEqual(response.status_code, 201)
        
        # Wait for compilation
        self._wait_for_compilation(pipeline_name)
        
        # Test various durations before stopping
        for duration_ms in [0, 50, 100, 250, 500]:
            # Start pipeline
            response = TEST_CLIENT.post(f"/v0/pipelines/{pipeline_name}/start")
            self.assertEqual(response.status_code, 202)
            
            # Wait specified duration
            if duration_ms > 0:
                time.sleep(duration_ms / 1000.0)
            
            # Stop forcefully
            response = TEST_CLIENT.post(f"/v0/pipelines/{pipeline_name}/stop?force=true")
            self.assertEqual(response.status_code, 202)
            
            # Wait for stopped status
            self._wait_for_status(pipeline_name, "Stopped")
            
            # Clear for next iteration
            response = TEST_CLIENT.post(f"/v0/pipelines/{pipeline_name}/clear")
            if response.status_code == 202:
                self._wait_for_storage_status(pipeline_name, "Cleared")
        
        # Clean up
        TEST_CLIENT.delete(f"/v0/pipelines/{pipeline_name}")

    @enterprise_only
    def test_pipeline_stop_without_force(self):
        """Test stopping pipeline without force (graceful shutdown)."""
        pipeline_name = "test-stop-graceful"
        
        # Clean up any existing pipeline
        try:
            TEST_CLIENT.delete(f"/v0/pipelines/{pipeline_name}")
        except:
            pass
        
        # Create pipeline
        pipeline_data = {
            "name": pipeline_name,
            "program_code": "CREATE TABLE t1(c1 INTEGER);",
        }
        response = TEST_CLIENT.post("/v0/pipelines", json=pipeline_data)
        self.assertEqual(response.status_code, 201)
        
        # Wait for compilation
        self._wait_for_compilation(pipeline_name)
        
        # Start pipeline
        response = TEST_CLIENT.post(f"/v0/pipelines/{pipeline_name}/start")
        self.assertEqual(response.status_code, 202)
        self._wait_for_status(pipeline_name, "Running")
        
        # Stop without force
        response = TEST_CLIENT.post(f"/v0/pipelines/{pipeline_name}/stop?force=false")
        self.assertEqual(response.status_code, 202)
        self._wait_for_status(pipeline_name, "Stopped")
        
        # Clean up
        TEST_CLIENT.delete(f"/v0/pipelines/{pipeline_name}")

    def test_pipeline_clear(self):
        """Test clearing pipeline storage."""
        pipeline_name = "test-clear"
        
        # Clean up any existing pipeline
        try:
            TEST_CLIENT.delete(f"/v0/pipelines/{pipeline_name}")
        except:
            pass
        
        # Create pipeline
        pipeline_data = {
            "name": pipeline_name,
            "program_code": "CREATE TABLE t1(c1 INTEGER);",
        }
        response = TEST_CLIENT.post("/v0/pipelines", json=pipeline_data)
        self.assertEqual(response.status_code, 201)
        
        # Initially should be Cleared
        response = TEST_CLIENT.get(f"/v0/pipelines/{pipeline_name}")
        pipeline = response.json()
        self.assertEqual(pipeline["storage_status"], "Cleared")
        
        # Wait for compilation
        self._wait_for_compilation(pipeline_name)
        
        # Start pipeline - storage becomes InUse
        response = TEST_CLIENT.post(f"/v0/pipelines/{pipeline_name}/start")
        self.assertEqual(response.status_code, 202)
        self._wait_for_status(pipeline_name, "Running")
        
        response = TEST_CLIENT.get(f"/v0/pipelines/{pipeline_name}")
        pipeline = response.json()
        self.assertEqual(pipeline["storage_status"], "InUse")
        
        # Cannot clear while running
        response = TEST_CLIENT.post(f"/v0/pipelines/{pipeline_name}/clear")
        self.assertEqual(response.status_code, 400)
        
        # Stop pipeline - storage remains InUse
        response = TEST_CLIENT.post(f"/v0/pipelines/{pipeline_name}/stop?force=true")
        self.assertEqual(response.status_code, 202)
        self._wait_for_status(pipeline_name, "Stopped")
        
        response = TEST_CLIENT.get(f"/v0/pipelines/{pipeline_name}")
        pipeline = response.json()
        self.assertEqual(pipeline["storage_status"], "InUse")
        
        # Now can clear
        response = TEST_CLIENT.post(f"/v0/pipelines/{pipeline_name}/clear")
        self.assertEqual(response.status_code, 202)
        self._wait_for_storage_status(pipeline_name, "Cleared")
        
        # Clean up
        TEST_CLIENT.delete(f"/v0/pipelines/{pipeline_name}")

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

    def _stop_force_and_clear(self, pipeline_name):
        """Stop pipeline forcefully and clear storage."""
        response = TEST_CLIENT.post(f"/v0/pipelines/{pipeline_name}/stop?force=true")
        if response.status_code == 202:
            self._wait_for_status(pipeline_name, "Stopped")
        
        response = TEST_CLIENT.post(f"/v0/pipelines/{pipeline_name}/clear")
        if response.status_code == 202:
            self._wait_for_storage_status(pipeline_name, "Cleared")


if __name__ == "__main__":
    unittest.main()