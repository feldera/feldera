"""Pipeline CRUD and basic operations tests converted from Rust integration tests."""
import unittest
import json
from tests import TEST_CLIENT, enterprise_only


class TestPipelineCrud(unittest.TestCase):
    """Test pipeline creation, retrieval, updating, and deletion."""

    def test_empty_table(self):
        """
        CREATE TABLE test_table(id INT) WITH ('materialized' = 'true');
        """
        pass

    def test_pipeline_post_validation(self):
        """Test pipeline creation validation."""
        import requests
        import json
        
        # Helper function to make requests with error handling
        def make_request(data):
            return requests.post(
                f"{TEST_CLIENT.config.url}/v0/pipelines",
                headers={"Content-Type": "application/json", **TEST_CLIENT.http.headers},
                data=json.dumps(data) if data else "",
                timeout=5,
                verify=TEST_CLIENT.config.requests_verify
            )
        
        # Empty body should return BAD_REQUEST
        response = make_request({})
        self.assertEqual(response.status_code, 400)

        # Missing name should return BAD_REQUEST
        response = make_request({"program_code": ""})
        self.assertEqual(response.status_code, 400)

        # Missing program code should return BAD_REQUEST
        response = make_request({"name": "test-validation"})
        self.assertEqual(response.status_code, 400)

    def test_pipeline_create_minimum(self):
        """Test pipeline creation with minimum fields."""
        import requests
        import json
        
        pipeline_name = "test-minimum"
        
        # Clean up any existing pipeline
        try:
            TEST_CLIENT.delete_pipeline(pipeline_name)
        except:
            pass
        
        # Create pipeline with minimum body
        pipeline_data = {
            "name": pipeline_name,
            "program_code": "",
        }
        response = requests.post(
            f"{TEST_CLIENT.config.url}/v0/pipelines",
            headers={"Content-Type": "application/json", **TEST_CLIENT.http.headers},
            data=json.dumps(pipeline_data),
            timeout=5,
            verify=TEST_CLIENT.config.requests_verify
        )
        self.assertEqual(response.status_code, 201)
        
        pipeline = response.json()
        self.assertEqual(pipeline["name"], "test-minimum")
        self.assertEqual(pipeline["description"], "")
        self.assertEqual(pipeline["program_code"], "")
        self.assertEqual(pipeline["udf_rust"], "")
        self.assertEqual(pipeline["udf_toml"], "")
        
        # Clean up
        TEST_CLIENT.delete_pipeline(pipeline_name)

    def test_pipeline_create_full(self):
        """Test pipeline creation with all fields."""
        import requests
        import json
        
        pipeline_name = "test-full"
        
        # Clean up any existing pipeline
        try:
            requests.delete(TEST_CLIENT.config.url + f"/v0/pipelines/{pipeline_name}", headers={"Content-Type": "application/json", **TEST_CLIENT.http.headers})
        except:
            pass
        
        # Create pipeline with all fields
        pipeline_data = {
            "name": pipeline_name,
            "description": "Test pipeline description",
            "runtime_config": {
                "workers": 123
            },
            "program_code": "CREATE TABLE t1(c1 INTEGER);",
            "udf_rust": "// Rust code",
            "udf_toml": "[dependencies]",
            "program_config": {
                "profile": "dev"
            }
        }
        response = requests.post(
            f"{TEST_CLIENT.config.url}/v0/pipelines",
            headers={"Content-Type": "application/json", **TEST_CLIENT.http.headers},
            data=json.dumps(pipeline_data),
            timeout=5,
            verify=TEST_CLIENT.config.requests_verify
        )
        self.assertEqual(response.status_code, 201)
        
        pipeline = response.json()
        self.assertEqual(pipeline["name"], pipeline_name)
        self.assertEqual(pipeline["description"], "Test pipeline description")
        self.assertEqual(pipeline["runtime_config"]["workers"], 123)
        self.assertEqual(pipeline["program_code"], "CREATE TABLE t1(c1 INTEGER);")
        self.assertEqual(pipeline["udf_rust"], "// Rust code")
        self.assertEqual(pipeline["udf_toml"], "[dependencies]")
        self.assertEqual(pipeline["program_config"]["profile"], "dev")
        
        # Clean up
        requests.delete(TEST_CLIENT.config.url + f"/v0/pipelines/{pipeline_name}", headers={"Content-Type": "application/json", **TEST_CLIENT.http.headers})

    def test_pipeline_get_and_list(self):
        """Test pipeline retrieval and listing."""
        import requests
        import json
        
        # List should initially not include our test pipelines
        pipelines = TEST_CLIENT.http.get("/pipelines")
        initial_names = {p["name"] for p in pipelines}

        pipeline1_name = "test-get-1"
        pipeline2_name = "test-get-2"
        
        # Clean up any existing pipelines
        for name in [pipeline1_name, pipeline2_name]:
            try:
                requests.delete(TEST_CLIENT.config.url + f"/v0/pipelines/{name}", headers={"Content-Type": "application/json", **TEST_CLIENT.http.headers})
            except:
                pass

        # Test 404 for non-existent pipeline
        try:
            TEST_CLIENT.http.get(f"/pipelines/{pipeline1_name}")
            self.fail("Expected exception for non-existent pipeline")
        except Exception:
            # This is expected - pipeline doesn't exist
            pass

        # Create first pipeline
        sql1 = "CREATE TABLE t1(c1 INT);"
        pipeline_data1 = {
            "name": pipeline1_name,
            "program_code": sql1,
        }
        response = requests.post(
            f"{TEST_CLIENT.config.url}/v0/pipelines",
            headers={"Content-Type": "application/json", **TEST_CLIENT.http.headers},
            data=json.dumps(pipeline_data1),
            timeout=5,
            verify=TEST_CLIENT.config.requests_verify
        )
        self.assertEqual(response.status_code, 201)

        # List should have one more pipeline
        pipelines = TEST_CLIENT.http.get("/pipelines")
        current_names = {p["name"] for p in pipelines}
        self.assertEqual(len(current_names - initial_names), 1)
        self.assertIn(pipeline1_name, current_names)

        # Get first pipeline
        pipeline1 = TEST_CLIENT.http.get(f"/pipelines/{pipeline1_name}")
        self.assertEqual(pipeline1["name"], pipeline1_name)
        self.assertEqual(pipeline1["program_code"], sql1)

        # Create second pipeline
        sql2 = "CREATE TABLE t2(c2 INT);"
        pipeline_data2 = {
            "name": pipeline2_name,
            "program_code": sql2,
        }
        response = requests.post(
            f"{TEST_CLIENT.config.url}/v0/pipelines",
            headers={"Content-Type": "application/json", **TEST_CLIENT.http.headers},
            data=json.dumps(pipeline_data2),
            timeout=5,
            verify=TEST_CLIENT.config.requests_verify
        )
        self.assertEqual(response.status_code, 201)

        # List should have two more pipelines
        pipelines = TEST_CLIENT.http.get("/pipelines")
        current_names = {p["name"] for p in pipelines}
        self.assertEqual(len(current_names - initial_names), 2)
        self.assertIn(pipeline1_name, current_names)
        self.assertIn(pipeline2_name, current_names)

        # Get second pipeline
        pipeline2 = TEST_CLIENT.http.get(f"/pipelines/{pipeline2_name}")
        self.assertEqual(pipeline2["name"], pipeline2_name)
        self.assertEqual(pipeline2["program_code"], sql2)

        # Clean up
        for name in [pipeline1_name, pipeline2_name]:
            requests.delete(TEST_CLIENT.config.url + f"/v0/pipelines/{pipeline_name}", headers={"Content-Type": "application/json", **TEST_CLIENT.http.headers})

    def test_pipeline_name_conflict(self):
        """Test that duplicate pipeline names are rejected."""
        import requests
        import json
        
        pipeline_name = "test-conflict"
        
        # Clean up any existing pipeline
        try:
            requests.delete(TEST_CLIENT.config.url + f"/v0/pipelines/{pipeline_name}", headers={"Content-Type": "application/json", **TEST_CLIENT.http.headers})
        except:
            pass
        
        pipeline_data = {
            "name": pipeline_name,
            "description": "First description",
            "program_code": "CREATE TABLE t1(c1 INTEGER);",
        }
        
        # First creation should succeed
        response = requests.post(
            f"{TEST_CLIENT.config.url}/v0/pipelines",
            headers={"Content-Type": "application/json", **TEST_CLIENT.http.headers},
            data=json.dumps(pipeline_data),
            timeout=5,
            verify=TEST_CLIENT.config.requests_verify
        )
        self.assertEqual(response.status_code, 201)
        
        # Second creation with same name should fail
        pipeline_data["description"] = "Different description"
        pipeline_data["program_code"] = "CREATE TABLE t2(c2 VARCHAR);"
        
        response = requests.post(
            f"{TEST_CLIENT.config.url}/v0/pipelines",
            headers={"Content-Type": "application/json", **TEST_CLIENT.http.headers},
            data=json.dumps(pipeline_data),
            timeout=5,
            verify=TEST_CLIENT.config.requests_verify
        )
        self.assertEqual(response.status_code, 409)  # Conflict
        
        # Clean up
        requests.delete(TEST_CLIENT.config.url + f"/v0/pipelines/{pipeline_name}", headers={"Content-Type": "application/json", **TEST_CLIENT.http.headers})

    def test_pipeline_naming_validation(self):
        """Test pipeline naming validation."""
        import requests
        import json
        
        # Valid names should work
        for name in ["test", "test_1", "test-1", "Test1", "a"*100]:  # Up to 100 chars
            try:
                requests.delete(TEST_CLIENT.config.url + f"/v0/pipelines/{name}", headers={"Content-Type": "application/json", **TEST_CLIENT.http.headers})
            except:
                pass
                
            pipeline_data = {
                "name": name,
                "program_code": "",
            }
            response = requests.post(
                f"{TEST_CLIENT.config.url}/v0/pipelines",
                headers={"Content-Type": "application/json", **TEST_CLIENT.http.headers},
                data=json.dumps(pipeline_data),
                timeout=5,
                verify=TEST_CLIENT.config.requests_verify
            )
            self.assertEqual(response.status_code, 201)
            requests.delete(TEST_CLIENT.config.url + f"/v0/pipelines/{name}", headers={"Content-Type": "application/json", **TEST_CLIENT.http.headers})

        # Invalid names should be rejected
        for name in ["", "a"*101, "%abc"]:  # Empty, too long, invalid chars
            pipeline_data = {
                "name": name,
                "program_code": "",
            }
            response = requests.post(
                f"{TEST_CLIENT.config.url}/v0/pipelines",
                headers={"Content-Type": "application/json", **TEST_CLIENT.http.headers},
                data=json.dumps(pipeline_data),
                timeout=5,
                verify=TEST_CLIENT.config.requests_verify
            )
            self.assertEqual(response.status_code, 400)


if __name__ == "__main__":
    unittest.main()