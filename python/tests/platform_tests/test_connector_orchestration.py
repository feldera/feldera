"""Connector orchestration and pipeline configuration tests."""
import unittest
import json
import time
from tests.shared_test_pipeline import SharedTestPipeline
from tests import TEST_CLIENT, enterprise_only


class TestConnectorOrchestrationAndConfig(SharedTestPipeline):
    """Test connector orchestration, pipeline configuration, and field selectors."""

    def test_orchestration_table(self):
        """
        CREATE TABLE numbers (
            num DOUBLE
        ) WITH (
            'connectors' = '[{
                "name": "c1",
                "transport": {
                    "name": "datagen",
                    "config": {"plan": [{ "rate": 100, "fields": { "num": { "range": [0, 1000], "strategy": "uniform" } } }]}
                }
            }]'
        );
        CREATE TABLE numbers_multi (
            num DOUBLE
        ) WITH (
            'connectors' = '[
                {
                    "name": "c1",
                    "transport": {
                        "name": "datagen",
                        "config": {"plan": [{ "rate": 100, "fields": { "num": { "range": [0, 1000], "strategy": "uniform" } } }]}
                    }
                },
                {
                    "name": "c2",
                    "transport": {
                        "name": "datagen",
                        "config": {"plan": [{ "rate": 100, "fields": { "num": { "range": [1000, 2000], "strategy": "uniform" } } }]}
                    }
                }
            ]'
        );
        CREATE TABLE case_test_numbers (
            num DOUBLE
        ) WITH (
            'connectors' = '[{
                "name": "c1",
                "transport": {
                    "name": "datagen",
                    "config": {"plan": [{ "rate": 100, "fields": { "num": { "range": [0, 1000], "strategy": "uniform" } } }]}
                }
            }]'
        );
        CREATE TABLE "case_sensitive_Numbers" (
            num DOUBLE
        ) WITH (
            'connectors' = '[{
                "name": "c1",
                "transport": {
                    "name": "datagen",
                    "config": {"plan": [{ "rate": 100, "fields": { "num": { "range": [0, 1000], "strategy": "uniform" } } }]}
                }
            }]'
        );
        CREATE TABLE special_chars_table (
            num DOUBLE
        ) WITH (
            'connectors' = '[{
                "name": "aA0_-",
                "transport": {
                    "name": "datagen",
                    "config": {"plan": [{ "rate": 100, "fields": { "num": { "range": [0, 1000], "strategy": "uniform" } } }]}
                }
            }]'
        );
        CREATE TABLE "numbers +C0_-,.!%()&/" (
            num DOUBLE
        ) WITH (
            'connectors' = '[{
                "name": "aA0_-",
                "transport": {
                    "name": "datagen",
                    "config": {"plan": [{ "rate": 100, "fields": { "num": { "range": [0, 1000], "strategy": "uniform" } } }]}
                }
            }]'
        );
        """
        pass

    def test_pipeline_get_field_selector(self):
        """Test pipeline field selectors."""
        pipeline_name = "test-selectors"
        
        # Clean up any existing pipeline
        try:
            TEST_CLIENT.delete(f"/v0/pipelines/{pipeline_name}")
        except:
            pass
        
        # Create pipeline
        pipeline_data = {
            "name": pipeline_name,
            "program_code": "CREATE TABLE t1(c1 INT);",
        }
        response = TEST_CLIENT.post("/v0/pipelines", json=pipeline_data)
        self.assertEqual(response.status_code, 201)
        
        # Define expected fields for different selectors
        all_fields = [
            "id", "name", "description", "created_at", "version", "platform_version",
            "runtime_config", "program_code", "udf_rust", "udf_toml", "program_config",
            "program_version", "program_status", "program_status_since", "program_error",
            "program_info", "deployment_status", "deployment_status_since",
            "deployment_desired_status", "deployment_error", "refresh_version", "storage_status"
        ]
        
        status_fields = [
            "id", "name", "description", "created_at", "version", "platform_version",
            "program_version", "program_status", "program_status_since",
            "deployment_status", "deployment_status_since", "deployment_desired_status",
            "deployment_error", "refresh_version", "storage_status"
        ]
        
        # Test different selectors
        test_cases = [
            ("", all_fields),
            ("all", all_fields),
            ("status", status_fields),
        ]
        
        for base_endpoint in ["/v0/pipelines", f"/v0/pipelines/{pipeline_name}"]:
            for selector_value, expected_fields in test_cases:
                # Build endpoint URL
                if selector_value:
                    endpoint = f"{base_endpoint}?selector={selector_value}"
                else:
                    endpoint = base_endpoint
                
                response = TEST_CLIENT.get(endpoint)
                self.assertEqual(response.status_code, 200)
                
                # Parse response
                data = response.json()
                if isinstance(data, list):
                    # List endpoint
                    self.assertGreater(len(data), 0)
                    pipeline_obj = None
                    for item in data:
                        if item["name"] == pipeline_name:
                            pipeline_obj = item
                            break
                    self.assertIsNotNone(pipeline_obj, f"Pipeline {pipeline_name} not found in list")
                else:
                    # Single pipeline endpoint
                    pipeline_obj = data
                
                # Check that the pipeline object has exactly the expected fields
                actual_fields = sorted(pipeline_obj.keys())
                expected_fields_sorted = sorted(expected_fields)
                
                self.assertEqual(
                    actual_fields,
                    expected_fields_sorted,
                    f"Field mismatch for selector '{selector_value}' on endpoint '{endpoint}'"
                )
        
        # Clean up
        TEST_CLIENT.delete(f"/v0/pipelines/{pipeline_name}")

    def test_pipeline_runtime_config_validation(self):
        """Test pipeline runtime configuration validation."""
        pipeline_name = "test-runtime-config"
        
        # Clean up any existing pipeline
        try:
            TEST_CLIENT.delete(f"/v0/pipelines/{pipeline_name}")
        except:
            pass
        
        # Test valid runtime configs
        valid_configs = [
            (None, {"workers": 1}),  # Default config
            ({}, {"workers": 1}),    # Empty config
            ({"workers": 12}, {"workers": 12}),
            ({
                "workers": 100,
                "resources": {
                    "cpu_cores_min": 5.0,
                    "storage_mb_max": 2000,
                    "storage_class": "normal"
                }
            }, {"workers": 100}),
        ]
        
        for runtime_config, expected_workers in valid_configs:
            body = {
                "name": pipeline_name,
                "program_code": "CREATE TABLE t1(c1 INT);",
            }
            if runtime_config is not None:
                body["runtime_config"] = runtime_config
            
            response = TEST_CLIENT.put(f"/v0/pipelines/{pipeline_name}", json=body)
            self.assertTrue(response.status_code in [200, 201])
            
            pipeline = response.json()
            self.assertEqual(pipeline["runtime_config"]["workers"], expected_workers["workers"])
        
        # Test invalid runtime configs
        invalid_configs = [
            {"workers": "not-a-number"},
            {"resources": {"storage_mb_max": "not-a-number"}},
        ]
        
        for runtime_config in invalid_configs:
            body = {
                "name": pipeline_name,
                "program_code": "CREATE TABLE t1(c1 INT);",
                "runtime_config": runtime_config
            }
            
            response = TEST_CLIENT.put(f"/v0/pipelines/{pipeline_name}", json=body)
            self.assertEqual(response.status_code, 400)
            
            error_data = response.json()
            self.assertEqual(error_data["error_code"], "InvalidRuntimeConfig")
        
        # Test patching runtime config
        original_body = {
            "name": f"{pipeline_name}-patch",
            "program_code": "CREATE TABLE t1(c1 INT);",
            "runtime_config": {
                "workers": 100,
                "storage": True,
                "resources": {
                    "cpu_cores_min": 2,
                    "storage_mb_max": 500,
                    "storage_class": "fast"
                }
            }
        }
        
        response = TEST_CLIENT.post("/v0/pipelines", json=original_body)
        self.assertEqual(response.status_code, 201)
        
        # Patch with new runtime config
        patch_body = {
            "runtime_config": {
                "workers": 1,
                "resources": {
                    "storage_mb_max": 123,
                }
            }
        }
        
        response = TEST_CLIENT.patch(f"/v0/pipelines/{pipeline_name}-patch", json=patch_body)
        self.assertEqual(response.status_code, 200)
        
        pipeline = response.json()
        self.assertEqual(pipeline["runtime_config"]["workers"], 1)
        self.assertEqual(pipeline["runtime_config"]["resources"]["storage_mb_max"], 123)
        
        # Clean up
        TEST_CLIENT.delete(f"/v0/pipelines/{pipeline_name}")
        TEST_CLIENT.delete(f"/v0/pipelines/{pipeline_name}-patch")

    def test_pipeline_program_config_validation(self):
        """Test pipeline program configuration validation."""
        pipeline_name = "test-program-config"
        
        # Clean up any existing pipeline
        try:
            TEST_CLIENT.delete(f"/v0/pipelines/{pipeline_name}")
        except:
            pass
        
        # Test valid program configs
        valid_configs = [
            (None, None),  # Default
            ({}, None),    # Empty
            ({"profile": "dev"}, "dev"),
            ({"cache": True}, None),
            ({"profile": "dev", "cache": False}, "dev"),
        ]
        
        for program_config, expected_profile in valid_configs:
            body = {
                "name": pipeline_name,
                "program_code": "CREATE TABLE t1(c1 INT);",
            }
            if program_config is not None:
                body["program_config"] = program_config
            
            response = TEST_CLIENT.put(f"/v0/pipelines/{pipeline_name}", json=body)
            self.assertTrue(response.status_code in [200, 201])
            
            pipeline = response.json()
            if expected_profile:
                self.assertEqual(pipeline["program_config"]["profile"], expected_profile)
            
        # Test invalid program configs
        invalid_configs = [
            {"profile": "does-not-exist"},
            {"cache": 123},
            {"profile": 123},
            {"profile": "unknown", "cache": "a"},
        ]
        
        for program_config in invalid_configs:
            body = {
                "name": pipeline_name,
                "program_code": "CREATE TABLE t1(c1 INT);",
                "program_config": program_config
            }
            
            response = TEST_CLIENT.put(f"/v0/pipelines/{pipeline_name}", json=body)
            self.assertEqual(response.status_code, 400)
            
            error_data = response.json()
            self.assertEqual(error_data["error_code"], "InvalidProgramConfig")
        
        # Clean up
        TEST_CLIENT.delete(f"/v0/pipelines/{pipeline_name}")

    def test_connector_orchestration_basic(self):
        """Test basic connector orchestration."""
        self.pipeline.start()
        
        # Check initial state - pipeline paused, connector running
        time.sleep(0.5)
        stats = TEST_CLIENT.get_pipeline_stats(self.pipeline.name)
        
        # Find connector state
        connector_paused = None
        for input_connector in stats["inputs"]:
            if input_connector["endpoint_name"] == "numbers.c1":
                connector_paused = input_connector["paused"]
                break
        
        self.assertIsNotNone(connector_paused)
        # Initially connector should be running (not paused)
        # and pipeline should be paused
        pipeline_paused = stats["global_metrics"]["state"] == "Paused"
        self.assertTrue(pipeline_paused)
        self.assertFalse(connector_paused)
        
        # Pause the connector
        response = TEST_CLIENT.post(
            f"/v0/pipelines/{self.pipeline.name}/tables/numbers/connectors/c1/pause"
        )
        self.assertEqual(response.status_code, 200)
        
        time.sleep(0.5)
        
        # Check state - both pipeline and connector should be paused
        stats = TEST_CLIENT.get_pipeline_stats(self.pipeline.name)
        pipeline_paused = stats["global_metrics"]["state"] == "Paused"
        connector_paused = None
        for input_connector in stats["inputs"]:
            if input_connector["endpoint_name"] == "numbers.c1":
                connector_paused = input_connector["paused"]
                break
        
        self.assertTrue(pipeline_paused)
        self.assertTrue(connector_paused)
        
        # Start the pipeline
        response = TEST_CLIENT.post(f"/v0/pipelines/{self.pipeline.name}/start")
        self.assertEqual(response.status_code, 202)
        
        # Wait for running status
        timeout = 30
        start_time = time.time()
        while time.time() - start_time < timeout:
            response = TEST_CLIENT.get(f"/v0/pipelines/{self.pipeline.name}")
            if response.json().get("deployment_status") == "Running":
                break
            time.sleep(1)
        
        time.sleep(0.5)
        
        # Check state - pipeline running, connector still paused
        stats = TEST_CLIENT.get_pipeline_stats(self.pipeline.name)
        pipeline_paused = stats["global_metrics"]["state"] == "Paused"
        connector_paused = None
        for input_connector in stats["inputs"]:
            if input_connector["endpoint_name"] == "numbers.c1":
                connector_paused = input_connector["paused"]
                break
        
        self.assertFalse(pipeline_paused)
        self.assertTrue(connector_paused)
        
        # Start the connector
        response = TEST_CLIENT.post(
            f"/v0/pipelines/{self.pipeline.name}/tables/numbers/connectors/c1/start"
        )
        self.assertEqual(response.status_code, 200)
        
        time.sleep(0.5)
        
        # Check state - both pipeline and connector should be running
        stats = TEST_CLIENT.get_pipeline_stats(self.pipeline.name)
        pipeline_paused = stats["global_metrics"]["state"] == "Paused"
        connector_paused = None
        total_processed = stats["global_metrics"]["total_processed_records"]
        
        for input_connector in stats["inputs"]:
            if input_connector["endpoint_name"] == "numbers.c1":
                connector_paused = input_connector["paused"]
                break
        
        self.assertFalse(pipeline_paused)
        self.assertFalse(connector_paused)
        self.assertGreater(total_processed, 0)

    def test_connector_orchestration_errors(self):
        """Test connector orchestration error cases."""
        self.pipeline.start()
        
        # Valid endpoints should return OK
        valid_endpoints = [
            "/v0/pipelines/{}/tables/case_test_numbers/connectors/c1/start",
            "/v0/pipelines/{}/tables/case_test_numbers/connectors/c1/pause",
            "/v0/pipelines/{}/tables/Case_Test_Numbers/connectors/c1/pause",  # Case insensitive
            "/v0/pipelines/{}/tables/CASE_TEST_NUMBERS/connectors/c1/pause",  # Case insensitive
        ]
        
        for endpoint_template in valid_endpoints:
            endpoint = endpoint_template.format(self.pipeline.name)
            response = TEST_CLIENT.post(endpoint)
            self.assertEqual(response.status_code, 200, f"Failed for endpoint: {endpoint}")
        
        # Invalid endpoints should return appropriate errors
        invalid_endpoints = [
            ("/v0/pipelines/{}/tables/case_test_numbers/connectors/c1/action2", 400),  # Invalid action
            ("/v0/pipelines/{}/tables/case_test_numbers/connectors/c1/START", 400),    # Case sensitive action
            ("/v0/pipelines/nonexistent/start", 404),                                  # Pipeline not found
            ("/v0/pipelines/nonexistent/tables/case_test_numbers/connectors/c1/start", 404),  # Pipeline not found
            ("/v0/pipelines/{}/tables/case_test_numbers/connectors/c2/start", 404),    # Connector not found
            ("/v0/pipelines/{}/tables/case_test_numbers/connectors/C1/start", 404),    # Connector case sensitive
            ("/v0/pipelines/{}/tables/nonexistent/connectors/c1/start", 404),         # Table not found
        ]
        
        for endpoint_template, expected_status in invalid_endpoints:
            endpoint = endpoint_template.format(self.pipeline.name)
            response = TEST_CLIENT.post(endpoint)
            self.assertEqual(response.status_code, expected_status, f"Failed for endpoint: {endpoint}")

    def test_connector_case_sensitivity(self):
        """Test case sensitivity in connector orchestration."""
        self.pipeline.start()
        
        # Test case-insensitive table names
        case_insensitive_endpoints = [
            "/v0/pipelines/{}/tables/case_test_numbers/connectors/c1/pause",
            "/v0/pipelines/{}/tables/Case_Test_Numbers/connectors/c1/pause",
            "/v0/pipelines/{}/tables/CASE_TEST_NUMBERS/connectors/c1/pause",
        ]
        
        for endpoint_template in case_insensitive_endpoints:
            endpoint = endpoint_template.format(self.pipeline.name)
            response = TEST_CLIENT.post(endpoint)
            self.assertEqual(response.status_code, 200, f"Case insensitive failed: {endpoint}")
        
        # Test case-sensitive table names (quoted)
        # Note: The case sensitive table name needs URL encoding for special characters
        import urllib.parse
        quoted_table = '"case_sensitive_Numbers"'
        encoded_table = urllib.parse.quote(quoted_table, safe='')
        
        case_sensitive_endpoint = f"/v0/pipelines/{self.pipeline.name}/tables/{encoded_table}/connectors/c1/pause"
        response = TEST_CLIENT.post(case_sensitive_endpoint)
        self.assertEqual(response.status_code, 200)

    def test_connector_special_characters(self):
        """Test connector names with special characters."""
        self.pipeline.start()
        
        # Test connector with special characters in name
        endpoint = f"/v0/pipelines/{self.pipeline.name}/tables/special_chars_table/connectors/aA0_-/pause"
        response = TEST_CLIENT.post(endpoint)
        self.assertEqual(response.status_code, 200)
        
        # Test table name with special characters (URL encoded)
        import urllib.parse
        quoted_table = '"numbers +C0_-,.!%()&/"'
        encoded_table = urllib.parse.quote(quoted_table, safe='')
        
        endpoint = f"/v0/pipelines/{self.pipeline.name}/tables/{encoded_table}/connectors/aA0_-/pause"
        response = TEST_CLIENT.post(endpoint)
        self.assertEqual(response.status_code, 200)


if __name__ == "__main__":
    unittest.main()