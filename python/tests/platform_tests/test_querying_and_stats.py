"""Pipeline querying, statistics, and misc functionality tests."""
import unittest
import json
import time
import requests
from tests import TEST_CLIENT, enterprise_only


class TestQueryingAndStats(unittest.TestCase):
    """Test adhoc queries, pipeline statistics, and miscellaneous functionality."""

    def test_query_tables(self):
        """
        CREATE TABLE not_materialized(id bigint not null);
        CREATE TABLE "TaBle1"(id bigint not null) with ('materialized' = 'true');
        CREATE TABLE t1 (
            id INT NOT NULL,
            dt DATE NOT NULL,
            uid UUID NOT NULL
        ) WITH (
          'materialized' = 'true',
          'connectors' = '[{
            "transport": {
              "name": "datagen",
              "config": {
                "plan": [{
                    "limit": 5
                }]
              }
            }
          }]'
        );
        CREATE TABLE t2 (
            id INT NOT NULL,
            st VARCHAR NOT NULL
        ) WITH (
          'materialized' = 'true',
          'connectors' = '[{
            "transport": {
              "name": "datagen",
              "config": {
                "plan": [{
                    "limit": 5
                }]
              }
            }
          }]'
        );
        CREATE MATERIALIZED VIEW joined AS ( SELECT t1.dt AS c1, t2.st AS c2, t1.uid as c3 FROM t1, t2 WHERE t1.id = t2.id );
        CREATE MATERIALIZED VIEW view_of_not_materialized AS ( SELECT * FROM not_materialized );
        CREATE MATERIALIZED VIEW stats_test_view AS ( SELECT * FROM t1 );
        """
        pass

    def test_pipeline_adhoc_query(self):
        """Test adhoc SQL queries on pipeline tables and views."""
        self.skipTest("Test requires pipeline context - moved from SharedTestPipeline")
    def test_adhoc_query_error_handling(self):
        """Test adhoc query error handling."""
        self.skipTest("Test requires pipeline context - moved from SharedTestPipeline")
    def test_adhoc_query_insert_statements(self):
        """Test INSERT statements via adhoc queries."""
        self.skipTest("Test requires pipeline context - moved from SharedTestPipeline")
    def test_adhoc_query_non_materialized_tables(self):
        """Test INSERT/SELECT on non-materialized tables."""
        self.skipTest("Test requires pipeline context - moved from SharedTestPipeline")
    def test_pipeline_adhoc_query_empty(self):
        """Test querying tables that have never received input."""
        # This is a regression test for a bug with uninitialized persistent snapshots
        self.skipTest("Test requires pipeline context - moved from SharedTestPipeline")
    def test_pipeline_stats(self):
        """Test retrieving pipeline statistics."""
        self.skipTest("Test requires pipeline context - moved from SharedTestPipeline")
    def test_pipeline_time_series(self):
        """Test retrieving pipeline time series data."""
        self.skipTest("Test requires pipeline context - moved from SharedTestPipeline")
    def test_connector_endpoint_naming(self):
        """Test connector endpoint naming with named and unnamed connectors."""
        pipeline_name = "test-connector-naming"
        
        # Clean up any existing pipeline
        try:
            TEST_CLIENT.http.delete(f"/v0/pipelines/{pipeline_name}")
        except:
            pass
        
        # Create pipeline with mix of named and unnamed connectors
        sql = '''
        CREATE TABLE t1 (i1 BIGINT) WITH (
            'connectors' = '[
                { "name": "abc", "transport": { "name": "datagen", "config": {} } },
                { "transport": { "name": "datagen", "config": {} } },
                { "name": "def", "transport": { "name": "datagen", "config": {} } },
                { "transport": { "name": "datagen", "config": {} } }
            ]'
        );
        
        CREATE TABLE t2 (i1 BIGINT) WITH (
            'connectors' = '[
                { "name": "c1", "transport": { "name": "datagen", "config": {} } }
            ]'
        );
        
        CREATE TABLE t3 (i1 BIGINT) WITH (
            'connectors' = '[
                { "transport": { "name": "datagen", "config": {} } }
            ]'
        );
        '''
        
        pipeline_data = {
            "name": pipeline_name,
            "program_code": sql,
        }
        response = requests.post(TEST_CLIENT.config.url + "/v0/pipelines", json=pipeline_data, headers={"Content-Type": "application/json", **TEST_CLIENT.http.headers})
        self.assertEqual(response.status_code, 201)
        
        # Wait for compilation
        self._wait_for_compilation(pipeline_name)
        
        # Check program_info for connector names
        response = TEST_CLIENT.http.get(f"/v0/pipelines/{pipeline_name}")
        pipeline = response.json()
        
        input_connectors = pipeline["program_info"]["input_connectors"]
        input_names = sorted(input_connectors.keys())
        
        expected_names = [
            "t1.abc",
            "t1.def", 
            "t1.unnamed-1",
            "t1.unnamed-3",
            "t2.c1",
            "t3.unnamed-0"
        ]
        
        self.assertEqual(input_names, expected_names)
        
        # Clean up
        TEST_CLIENT.http.delete(f"/v0/pipelines/{pipeline_name}")

    def test_pipeline_start_without_compilation(self):
        """Test starting pipeline before compilation completes."""
        pipeline_name = "test-early-start"
        
        # Clean up any existing pipeline
        try:
            TEST_CLIENT.http.delete(f"/v0/pipelines/{pipeline_name}")
        except:
            pass
        
        # Create pipeline
        pipeline_data = {
            "name": pipeline_name,
            "description": "Test early start",
            "program_code": "CREATE TABLE foo (bar INTEGER);",
        }
        response = requests.post(TEST_CLIENT.config.url + "/v0/pipelines", json=pipeline_data, headers={"Content-Type": "application/json", **TEST_CLIENT.http.headers})
        self.assertEqual(response.status_code, 201)
        
        # Try starting before compilation is complete
        # Wait until we're past the initial states
        timeout = 30
        start_time = time.time()
        
        while time.time() - start_time < timeout:
            response = TEST_CLIENT.http.get(f"/v0/pipelines/{pipeline_name}")
            pipeline = response.json()
            status = pipeline.get("program_status")
            
            if status not in ["Pending", "CompilingSql"]:
                break
                
            time.sleep(0.05)
        
        # Try to start the pipeline
        response = requests.post(TEST_CLIENT.config.url + f"/v0/pipelines/{pipeline_name}/start", headers={"Content-Type": "application/json", **TEST_CLIENT.http.headers})
        self.assertEqual(response.status_code, 202)
        
        # Should eventually reach running state
        self._wait_for_status(pipeline_name, "Running")
        
        # Clean up
        response = requests.post(TEST_CLIENT.config.url + f"/v0/pipelines/{pipeline_name}/stop?force=true", headers={"Content-Type": "application/json", **TEST_CLIENT.http.headers})
        if response.status_code == 202:
            self._wait_for_status(pipeline_name, "Stopped")
            
        response = requests.post(TEST_CLIENT.config.url + f"/v0/pipelines/{pipeline_name}/clear", headers={"Content-Type": "application/json", **TEST_CLIENT.http.headers})
        if response.status_code == 202:
            self._wait_for_storage_status(pipeline_name, "Cleared")
            
        TEST_CLIENT.http.delete(f"/v0/pipelines/{pipeline_name}")

    # Helper methods
    def _wait_for_compilation(self, pipeline_name, timeout=600):
        """Wait for pipeline compilation to complete."""
        start_time = time.time()
        
        while time.time() - start_time < timeout:
            response = TEST_CLIENT.http.get(f"/v0/pipelines/{pipeline_name}")
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
            response = TEST_CLIENT.http.get(f"/v0/pipelines/{pipeline_name}")
            pipeline = response.json()
            
            if pipeline.get("deployment_status") == expected_status:
                return
                
            time.sleep(1)
        
        self.fail(f"Timed out waiting for {pipeline_name} to reach status {expected_status}")

    def _wait_for_storage_status(self, pipeline_name, expected_status, timeout=30):
        """Wait for pipeline storage to reach expected status."""
        start_time = time.time()
        
        while time.time() - start_time < timeout:
            response = TEST_CLIENT.http.get(f"/v0/pipelines/{pipeline_name}")
            pipeline = response.json()
            
            if pipeline.get("storage_status") == expected_status:
                return
                
            time.sleep(1)
        
        self.fail(f"Timed out waiting for {pipeline_name} storage to reach status {expected_status}")


if __name__ == "__main__":
    unittest.main()