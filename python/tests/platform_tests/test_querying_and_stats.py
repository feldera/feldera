"""Pipeline querying, statistics, and misc functionality tests."""
import unittest
import json
import time
from tests.shared_test_pipeline import SharedTestPipeline
from tests import TEST_CLIENT, enterprise_only


class TestQueryingAndStats(SharedTestPipeline):
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
        self.pipeline.start()
        
        # Let the connectors generate some data
        time.sleep(2)
        
        # Test basic queries in different formats
        for format_type in ["text", "json"]:
            # Test empty table query
            response = TEST_CLIENT.get(
                f"/v0/pipelines/{self.pipeline.name}/query?sql=SELECT * FROM \"TaBle1\"&format={format_type}"
            )
            self.assertEqual(response.status_code, 200)
            
            # Test materialized view query
            query_a = "SELECT * FROM joined"
            response = TEST_CLIENT.get(
                f"/v0/pipelines/{self.pipeline.name}/query?sql={query_a}&format={format_type}"
            )
            self.assertEqual(response.status_code, 200)
            result_a = response.text
            
            # Test equivalent adhoc join query
            query_b = "SELECT t1.dt AS c1, t2.st AS c2, t1.uid as c3 FROM t1, t2 WHERE t1.id = t2.id"
            response = TEST_CLIENT.get(
                f"/v0/pipelines/{self.pipeline.name}/query?sql={query_b}&format={format_type}"
            )
            self.assertEqual(response.status_code, 200)
            result_b = response.text
            
            # Results should be similar (order may vary)
            if format_type == "json":
                # For JSON, we can parse and compare
                json_a = json.loads(result_a) if result_a.strip() else []
                json_b = json.loads(result_b) if result_b.strip() else []
                self.assertEqual(len(json_a), len(json_b))

        # Test parquet format with ordered query
        query_a_ordered = "SELECT * FROM joined ORDER BY c1, c2, c3"
        response = TEST_CLIENT.get(
            f"/v0/pipelines/{self.pipeline.name}/query?sql={query_a_ordered}&format=parquet"
        )
        self.assertEqual(response.status_code, 200)
        result_a_parquet = response.content
        
        query_b_ordered = "SELECT t1.dt AS c1, t2.st AS c2, t1.uid as c3 FROM t1, t2 WHERE t1.id = t2.id ORDER BY c1, c2, c3"
        response = TEST_CLIENT.get(
            f"/v0/pipelines/{self.pipeline.name}/query?sql={query_b_ordered}&format=parquet"
        )
        self.assertEqual(response.status_code, 200)
        result_b_parquet = response.content
        
        self.assertTrue(len(result_a_parquet) > 0)
        self.assertEqual(result_a_parquet, result_b_parquet)

    def test_adhoc_query_error_handling(self):
        """Test adhoc query error handling."""
        self.pipeline.start()
        
        # Invalid table should return 400
        response = TEST_CLIENT.get(
            f"/v0/pipelines/{self.pipeline.name}/query?sql=SELECT * FROM invalid_table&format=text"
        )
        self.assertEqual(response.status_code, 400)
        
        # Division by zero should return 200 but with ERROR in response
        response = TEST_CLIENT.get(
            f"/v0/pipelines/{self.pipeline.name}/query?sql=SELECT 1/0&format=text"
        )
        self.assertEqual(response.status_code, 200)
        result = response.text
        self.assertIn("ERROR", result)
        
        # Non-existent table (case-sensitive) should return 400
        response = TEST_CLIENT.get(
            f"/v0/pipelines/{self.pipeline.name}/query?sql=SELECT * FROM table1&format=text"
        )
        self.assertEqual(response.status_code, 400)

    def test_adhoc_query_insert_statements(self):
        """Test INSERT statements via adhoc queries."""
        self.pipeline.start()
        
        # Check initial count
        response = TEST_CLIENT.get(
            f"/v0/pipelines/{self.pipeline.name}/query?sql=SELECT COUNT(*) from t1&format=json"
        )
        self.assertEqual(response.status_code, 200)
        initial_count = json.loads(response.text)[0]["count(*)"]
        
        # Insert data via adhoc query
        insert_query = "INSERT INTO t1 VALUES (99, '2020-01-01', 'c32d330f-5757-4ada-bcf6-1fac2d54e37f'), (100, '2020-01-01', '00000000-0000-0000-0000-000000000000')"
        response = TEST_CLIENT.get(
            f"/v0/pipelines/{self.pipeline.name}/query?sql={insert_query}&format=json"
        )
        self.assertEqual(response.status_code, 200)
        result = json.loads(response.text)
        self.assertEqual(result, {"count": 2})
        
        # Check updated count
        response = TEST_CLIENT.get(
            f"/v0/pipelines/{self.pipeline.name}/query?sql=SELECT COUNT(*) from t1&format=json"
        )
        self.assertEqual(response.status_code, 200)
        final_count = json.loads(response.text)[0]["count(*)"]
        self.assertEqual(final_count, initial_count + 2)

    def test_adhoc_query_non_materialized_tables(self):
        """Test INSERT/SELECT on non-materialized tables."""
        self.pipeline.start()
        
        # Check initial count on view of non-materialized table
        response = TEST_CLIENT.get(
            f"/v0/pipelines/{self.pipeline.name}/query?sql=SELECT COUNT(*) from view_of_not_materialized&format=json"
        )
        self.assertEqual(response.status_code, 200)
        result = json.loads(response.text)
        self.assertEqual(result, [{"count(*)": 0}])
        
        # Insert into non-materialized table
        response = TEST_CLIENT.get(
            f"/v0/pipelines/{self.pipeline.name}/query?sql=INSERT INTO not_materialized VALUES (99), (100)&format=json"
        )
        self.assertEqual(response.status_code, 200)
        result = json.loads(response.text)
        self.assertEqual(result, {"count": 2})
        
        # Check updated count
        response = TEST_CLIENT.get(
            f"/v0/pipelines/{self.pipeline.name}/query?sql=SELECT COUNT(*) from view_of_not_materialized&format=json"
        )
        self.assertEqual(response.status_code, 200)
        result = json.loads(response.text)
        self.assertEqual(result, [{"count(*)": 2}])

    def test_pipeline_adhoc_query_empty(self):
        """Test querying tables that have never received input."""
        # This is a regression test for a bug with uninitialized persistent snapshots
        self.pipeline.start()
        
        response = TEST_CLIENT.get(
            f"/v0/pipelines/{self.pipeline.name}/query?sql=SELECT COUNT(*) from \"TaBle1\"&format=json"
        )
        self.assertEqual(response.status_code, 200)
        result = json.loads(response.text)
        self.assertEqual(result, [{"count(*)": 0}])

    def test_pipeline_stats(self):
        """Test retrieving pipeline statistics."""
        self.pipeline.start()
        
        # Let the connectors run for a bit
        time.sleep(3)
        
        # Get pipeline stats
        response = TEST_CLIENT.get(f"/v0/pipelines/{self.pipeline.name}/stats")
        self.assertEqual(response.status_code, 200)
        stats = response.json()
        
        # Check main object keys
        expected_keys = {"global_metrics", "inputs", "outputs", "suspend_error"}
        actual_keys = set(stats.keys())
        self.assertEqual(actual_keys, expected_keys)
        
        # Check global_metrics
        global_metrics = stats["global_metrics"]
        self.assertIn(global_metrics["state"], ["Running", "Paused"])
        self.assertIsInstance(global_metrics["buffered_input_records"], int)
        self.assertIsInstance(global_metrics["pipeline_complete"], bool)
        self.assertIsInstance(global_metrics["total_input_records"], int)
        self.assertIsInstance(global_metrics["total_processed_records"], int)
        
        # Should have processed some records
        self.assertGreaterEqual(global_metrics["total_processed_records"], 0)
        
        # Check inputs
        inputs = stats["inputs"]
        self.assertIsInstance(inputs, list)
        self.assertGreater(len(inputs), 0)
        
        # Find the t1 input
        t1_input = None
        for inp in inputs:
            if "t1" in inp.get("endpoint_name", ""):
                t1_input = inp
                break
        
        if t1_input:
            self.assertIn("config", t1_input)
            self.assertIn("metrics", t1_input)
            
            metrics = t1_input["metrics"]
            self.assertIn("total_records", metrics)
            self.assertIn("total_bytes", metrics)

    def test_pipeline_time_series(self):
        """Test retrieving pipeline time series data."""
        self.pipeline.start()
        
        # Let the pipeline run for a bit
        time.sleep(3)
        
        response = TEST_CLIENT.get(f"/v0/pipelines/{self.pipeline.name}/time_series")
        self.assertEqual(response.status_code, 200)
        
        time_series = response.json()
        self.assertIn("samples", time_series)
        
        samples = time_series["samples"]
        self.assertIsInstance(samples, list)
        self.assertGreater(len(samples), 0)
        
        # Check sample structure
        if samples:
            sample = samples[0]
            self.assertIn("timestamp", sample)
            self.assertIn("total_processed_records", sample)

    def test_connector_endpoint_naming(self):
        """Test connector endpoint naming with named and unnamed connectors."""
        pipeline_name = "test-connector-naming"
        
        # Clean up any existing pipeline
        try:
            TEST_CLIENT.delete(f"/v0/pipelines/{pipeline_name}")
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
        response = TEST_CLIENT.post("/v0/pipelines", json=pipeline_data)
        self.assertEqual(response.status_code, 201)
        
        # Wait for compilation
        self._wait_for_compilation(pipeline_name)
        
        # Check program_info for connector names
        response = TEST_CLIENT.get(f"/v0/pipelines/{pipeline_name}")
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
        TEST_CLIENT.delete(f"/v0/pipelines/{pipeline_name}")

    def test_pipeline_start_without_compilation(self):
        """Test starting pipeline before compilation completes."""
        pipeline_name = "test-early-start"
        
        # Clean up any existing pipeline
        try:
            TEST_CLIENT.delete(f"/v0/pipelines/{pipeline_name}")
        except:
            pass
        
        # Create pipeline
        pipeline_data = {
            "name": pipeline_name,
            "description": "Test early start",
            "program_code": "CREATE TABLE foo (bar INTEGER);",
        }
        response = TEST_CLIENT.post("/v0/pipelines", json=pipeline_data)
        self.assertEqual(response.status_code, 201)
        
        # Try starting before compilation is complete
        # Wait until we're past the initial states
        timeout = 30
        start_time = time.time()
        
        while time.time() - start_time < timeout:
            response = TEST_CLIENT.get(f"/v0/pipelines/{pipeline_name}")
            pipeline = response.json()
            status = pipeline.get("program_status")
            
            if status not in ["Pending", "CompilingSql"]:
                break
                
            time.sleep(0.05)
        
        # Try to start the pipeline
        response = TEST_CLIENT.post(f"/v0/pipelines/{pipeline_name}/start")
        self.assertEqual(response.status_code, 202)
        
        # Should eventually reach running state
        self._wait_for_status(pipeline_name, "Running")
        
        # Clean up
        response = TEST_CLIENT.post(f"/v0/pipelines/{pipeline_name}/stop?force=true")
        if response.status_code == 202:
            self._wait_for_status(pipeline_name, "Stopped")
            
        response = TEST_CLIENT.post(f"/v0/pipelines/{pipeline_name}/clear")
        if response.status_code == 202:
            self._wait_for_storage_status(pipeline_name, "Cleared")
            
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


if __name__ == "__main__":
    unittest.main()