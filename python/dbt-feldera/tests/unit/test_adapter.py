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


class TestConvertAgateRows(unittest.TestCase):
    """Unit tests for FelderaAdapter._convert_agate_rows NaN/Infinity handling."""

    def _make_table(self, rows, column_names, column_types=None):
        import agate

        if column_types is None:
            column_types = [agate.data_types.Number()] * len(column_names)
        return agate.Table(rows=rows, column_names=column_names, column_types=column_types)

    def test_nan_decimal_becomes_none(self):
        from decimal import Decimal

        table = self._make_table([(Decimal("NaN"),)], ["value"])
        result = FelderaAdapter._convert_agate_rows(table)
        self.assertIsNone(result[0]["value"])

    def test_infinity_decimal_becomes_none(self):
        from decimal import Decimal

        table = self._make_table([(Decimal("Infinity"),)], ["value"])
        result = FelderaAdapter._convert_agate_rows(table)
        self.assertIsNone(result[0]["value"])

    def test_negative_infinity_decimal_becomes_none(self):
        from decimal import Decimal

        table = self._make_table([(Decimal("-Infinity"),)], ["value"])
        result = FelderaAdapter._convert_agate_rows(table)
        self.assertIsNone(result[0]["value"])

    def test_normal_decimal_integer_becomes_int(self):
        from decimal import Decimal

        table = self._make_table([(Decimal("42"),)], ["value"])
        result = FelderaAdapter._convert_agate_rows(table)
        self.assertEqual(result[0]["value"], 42)
        self.assertIsInstance(result[0]["value"], int)

    def test_normal_decimal_float_becomes_float(self):
        from decimal import Decimal

        table = self._make_table([(Decimal("3.14"),)], ["value"])
        result = FelderaAdapter._convert_agate_rows(table)
        self.assertAlmostEqual(result[0]["value"], 3.14)
        self.assertIsInstance(result[0]["value"], float)

    def test_nan_with_float_caster_becomes_none(self):
        from decimal import Decimal

        table = self._make_table([(Decimal("NaN"),)], ["value"])
        result = FelderaAdapter._convert_agate_rows(table, column_types={"value": "DOUBLE"})
        self.assertIsNone(result[0]["value"])

    def test_infinity_with_float_caster_becomes_none(self):
        from decimal import Decimal

        table = self._make_table([(Decimal("Infinity"),)], ["value"])
        result = FelderaAdapter._convert_agate_rows(table, column_types={"value": "FLOAT"})
        self.assertIsNone(result[0]["value"])


if __name__ == "__main__":
    unittest.main()
