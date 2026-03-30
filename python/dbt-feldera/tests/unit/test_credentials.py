import unittest

from dbt.adapters.feldera.credentials import FelderaCredentials


class TestFelderaCredentials(unittest.TestCase):
    """Unit tests for FelderaCredentials."""

    def test_default_values(self):
        """Test that defaults are populated correctly."""
        creds = FelderaCredentials(schema="test_pipeline", database="default")
        self.assertEqual(creds.host, "http://localhost:8080")
        self.assertIsNone(creds.api_key)
        self.assertIsNone(creds.pipeline_name)
        self.assertEqual(creds.compilation_profile, "dev")
        self.assertEqual(creds.workers, 4)
        self.assertEqual(creds.timeout, 300)

    def test_type_property(self):
        """Test that the type property returns 'feldera'."""
        creds = FelderaCredentials(schema="test", database="default")
        self.assertEqual(creds.type, "feldera")

    def test_unique_field(self):
        """Test that unique_field returns the host."""
        creds = FelderaCredentials(host="http://my-host:9090", schema="test", database="default")
        self.assertEqual(creds.unique_field, "http://my-host:9090")

    def test_connection_keys(self):
        """Test that _connection_keys returns the expected tuple."""
        creds = FelderaCredentials(schema="test", database="default")
        keys = creds._connection_keys()
        self.assertIn("host", keys)
        self.assertIn("pipeline_name", keys)
        self.assertIn("compilation_profile", keys)
        self.assertIn("workers", keys)

    def test_custom_values(self):
        """Test credentials with custom values."""
        creds = FelderaCredentials(
            host="http://feldera.example.com:8080",
            api_key="apikey:test123",
            pipeline_name="my_pipeline",
            compilation_profile="optimized",
            workers=16,
            timeout=600,
            schema="my_schema",
            database="default",
        )
        self.assertEqual(creds.host, "http://feldera.example.com:8080")
        self.assertEqual(creds.api_key, "apikey:test123")
        self.assertEqual(creds.pipeline_name, "my_pipeline")
        self.assertEqual(creds.compilation_profile, "optimized")
        self.assertEqual(creds.workers, 16)
        self.assertEqual(creds.timeout, 600)


if __name__ == "__main__":
    unittest.main()
