"""Health and cluster management tests converted from Rust integration tests."""
import unittest
import time
import json
from tests.shared_test_pipeline import SharedTestPipeline
from tests import TEST_CLIENT, enterprise_only


class TestHealthAndCluster(SharedTestPipeline):
    """Test health endpoints and cluster functionality."""

    def test_cluster_health_check(self):
        """Test the cluster health endpoint."""
        # Keep trying for up to 5 minutes to get healthy status
        timeout = 300  # 5 minutes
        start_time = time.time()
        
        while time.time() - start_time < timeout:
            health_response = TEST_CLIENT.get("/v0/config/health")
            self.assertIn(health_response.status_code, [200, 503])
            
            health_data = health_response.json()
            
            # Safely extract and check "healthy" for both runner and compiler
            runner_healthy = health_data.get("runner", {}).get("healthy", False)
            compiler_healthy = health_data.get("compiler", {}).get("healthy", False)
            
            if runner_healthy and compiler_healthy:
                self.assertEqual(health_response.status_code, 200)
                # Both services are healthy, test passes
                return
            else:
                # If either service is unhealthy, status code must be 503
                self.assertEqual(health_response.status_code, 503)
                time.sleep(2)  # Wait 2 seconds before retrying
        
        self.fail(f"Timed out waiting for both runner and compiler to become healthy. "
                 f"Last health status: {health_data}")

    def test_health_check_simple(self):
        """Test the simple health check endpoint."""
        max_attempts = 30
        attempt = 0
        
        while attempt < max_attempts:
            response = TEST_CLIENT.get("/healthz")
            
            if response.status_code == 200:
                data = response.json()
                expected = {"status": "healthy"}
                self.assertEqual(data, expected)
                return
            elif response.status_code == 500:
                data = response.json()
                expected = {"status": "unhealthy: unable to reach database (see logs for further details)"}
                self.assertEqual(data, expected)
                
            attempt += 1
            time.sleep(1)
        
        self.fail("Health check never returned healthy status within 30 attempts")


if __name__ == "__main__":
    unittest.main()