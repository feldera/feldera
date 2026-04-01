import unittest

from dbt.adapters.feldera.impl import FelderaAdapter


class TestFelderaAdapterStaticMethods(unittest.TestCase):
    """Unit tests for FelderaAdapter static/class methods."""

    def test_date_function(self):
        self.assertEqual(FelderaAdapter.date_function(), "NOW()")

    def test_is_cancelable(self):
        self.assertFalse(FelderaAdapter.is_cancelable())

    def test_convert_text_type(self):
        self.assertEqual(FelderaAdapter.convert_text_type(None, 0), "VARCHAR")

    def test_convert_boolean_type(self):
        self.assertEqual(FelderaAdapter.convert_boolean_type(None, 0), "BOOLEAN")

    def test_convert_datetime_type(self):
        self.assertEqual(FelderaAdapter.convert_datetime_type(None, 0), "TIMESTAMP")

    def test_convert_date_type(self):
        self.assertEqual(FelderaAdapter.convert_date_type(None, 0), "DATE")

    def test_convert_time_type(self):
        self.assertEqual(FelderaAdapter.convert_time_type(None, 0), "TIME")


class TestFelderaAdapterPluginRegistration(unittest.TestCase):
    """Test that the adapter plugin registers correctly."""

    def test_plugin_import(self):
        from dbt.adapters.feldera import Plugin

        self.assertIsNotNone(Plugin)
        self.assertEqual(Plugin.adapter, FelderaAdapter)

    def test_plugin_credentials(self):
        from dbt.adapters.feldera import Plugin
        from dbt.adapters.feldera.credentials import FelderaCredentials

        self.assertEqual(Plugin.credentials, FelderaCredentials)

    def test_plugin_include_path_exists(self):
        import os

        from dbt.adapters.feldera import Plugin

        self.assertTrue(os.path.isdir(Plugin.include_path))

    def test_dbt_project_yml_exists(self):
        import os

        from dbt.adapters.feldera import Plugin

        dbt_project = os.path.join(Plugin.include_path, "dbt_project.yml")
        self.assertTrue(os.path.isfile(dbt_project))


if __name__ == "__main__":
    unittest.main()
