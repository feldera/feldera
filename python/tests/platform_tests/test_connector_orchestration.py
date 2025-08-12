"""Connector orchestration and pipeline configuration tests."""
import unittest
import json
import time
import requests
from tests import TEST_CLIENT, enterprise_only


class TestConnectorOrchestrationAndConfig(unittest.TestCase):
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
            TEST_CLIENT.http.delete(f"/v0/pipelines/{pipeline_name}")
        except:
            pass
        
        # Create pipeline
        pipeline_data = {
            "name": pipeline_name,
            "program_code": "CREATE TABLE t1(c1 INT);",
        }
        response = requests.post(TEST_CLIENT.config.url + "/v0/pipelines", json=pipeline_data, headers={"Content-Type": "application/json", **TEST_CLIENT.http.headers})
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
                
                response = requests.get(TEST_CLIENT.config.url + endpoint, headers={"Content-Type": "application/json", **TEST_CLIENT.http.headers})
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
        TEST_CLIENT.http.delete(f"/v0/pipelines/{pipeline_name}")

    def test_pipeline_runtime_config_validation(self):
        """Test pipeline runtime configuration validation."""
        pipeline_name = "test-runtime-config"
        
        # Clean up any existing pipeline
        try:
            TEST_CLIENT.http.delete(f"/v0/pipelines/{pipeline_name}")
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
            
            response = requests.put(TEST_CLIENT.config.url + f"/v0/pipelines/{pipeline_name}", json=body, headers={"Content-Type": "application/json", **TEST_CLIENT.http.headers})
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
            
            response = requests.put(TEST_CLIENT.config.url + f"/v0/pipelines/{pipeline_name}", json=body, headers={"Content-Type": "application/json", **TEST_CLIENT.http.headers})
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
        
        response = requests.post(TEST_CLIENT.config.url + "/v0/pipelines", json=original_body, headers={"Content-Type": "application/json", **TEST_CLIENT.http.headers})
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
        TEST_CLIENT.http.delete(f"/v0/pipelines/{pipeline_name}")
        TEST_CLIENT.http.delete(f"/v0/pipelines/{pipeline_name}-patch")

    def test_pipeline_program_config_validation(self):
        """Test pipeline program configuration validation."""
        pipeline_name = "test-program-config"
        
        # Clean up any existing pipeline
        try:
            TEST_CLIENT.http.delete(f"/v0/pipelines/{pipeline_name}")
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
            
            response = requests.put(TEST_CLIENT.config.url + f"/v0/pipelines/{pipeline_name}", json=body, headers={"Content-Type": "application/json", **TEST_CLIENT.http.headers})
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
            
            response = requests.put(TEST_CLIENT.config.url + f"/v0/pipelines/{pipeline_name}", json=body, headers={"Content-Type": "application/json", **TEST_CLIENT.http.headers})
            self.assertEqual(response.status_code, 400)
            
            error_data = response.json()
            self.assertEqual(error_data["error_code"], "InvalidProgramConfig")
        
        # Clean up
        TEST_CLIENT.http.delete(f"/v0/pipelines/{pipeline_name}")

    def test_connector_orchestration_basic(self):
        """Test basic connector orchestration."""
        self.skipTest("Test requires pipeline context - moved from SharedTestPipeline")
    def test_connector_orchestration_errors(self):
        """Test connector orchestration error cases."""
        self.skipTest("Test requires pipeline context - moved from SharedTestPipeline")
    def test_connector_case_sensitivity(self):
        """Test case sensitivity in connector orchestration."""
        self.skipTest("Test requires pipeline context - moved from SharedTestPipeline")
    def test_connector_special_characters(self):
        """Test connector names with special characters."""
        self.skipTest("Test requires pipeline context - moved from SharedTestPipeline")
if __name__ == "__main__":
    unittest.main()