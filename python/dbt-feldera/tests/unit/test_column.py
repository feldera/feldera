import unittest

from dbt.adapters.feldera.column import FelderaColumn


class TestFelderaColumn(unittest.TestCase):
    """Unit tests for FelderaColumn."""

    def test_translate_type_varchar(self):
        self.assertEqual(FelderaColumn.translate_type("VARCHAR"), "text")

    def test_translate_type_bigint(self):
        self.assertEqual(FelderaColumn.translate_type("BIGINT"), "integer")

    def test_translate_type_integer(self):
        self.assertEqual(FelderaColumn.translate_type("INTEGER"), "integer")

    def test_translate_type_int(self):
        self.assertEqual(FelderaColumn.translate_type("INT"), "integer")

    def test_translate_type_boolean(self):
        self.assertEqual(FelderaColumn.translate_type("BOOLEAN"), "boolean")

    def test_translate_type_double(self):
        self.assertEqual(FelderaColumn.translate_type("DOUBLE"), "number")

    def test_translate_type_decimal_with_precision(self):
        self.assertEqual(FelderaColumn.translate_type("DECIMAL(10,2)"), "number")

    def test_translate_type_timestamp(self):
        self.assertEqual(FelderaColumn.translate_type("TIMESTAMP"), "datetime")

    def test_translate_type_date(self):
        self.assertEqual(FelderaColumn.translate_type("DATE"), "date")

    def test_translate_type_time(self):
        self.assertEqual(FelderaColumn.translate_type("TIME"), "time")

    def test_translate_type_array(self):
        self.assertEqual(FelderaColumn.translate_type("ARRAY"), "text")

    def test_translate_type_map(self):
        self.assertEqual(FelderaColumn.translate_type("MAP"), "text")

    def test_translate_type_unknown_defaults_to_text(self):
        self.assertEqual(FelderaColumn.translate_type("UNKNOWN_TYPE"), "text")

    def test_translate_type_case_insensitive(self):
        self.assertEqual(FelderaColumn.translate_type("varchar"), "text")
        self.assertEqual(FelderaColumn.translate_type("Bigint"), "integer")

    def test_from_feldera_field(self):
        field = {"name": "user_id", "columntype": {"type": "BIGINT", "nullable": False}}
        col = FelderaColumn.from_feldera_field(field)
        self.assertEqual(col.column, "user_id")
        self.assertEqual(col.dtype, "BIGINT")

    def test_from_feldera_field_with_string_type(self):
        field = {"name": "name", "columntype": "VARCHAR"}
        col = FelderaColumn.from_feldera_field(field)
        self.assertEqual(col.column, "name")
        self.assertEqual(col.dtype, "VARCHAR")

    def test_from_feldera_field_missing_name(self):
        field = {"columntype": {"type": "INT"}}
        col = FelderaColumn.from_feldera_field(field)
        self.assertEqual(col.column, "")

    def test_is_string(self):
        col = FelderaColumn(column="name", dtype="VARCHAR")
        self.assertTrue(col.is_string())

    def test_is_integer(self):
        col = FelderaColumn(column="id", dtype="BIGINT")
        self.assertTrue(col.is_integer())

    def test_is_number(self):
        col = FelderaColumn(column="amount", dtype="DECIMAL(10,2)")
        self.assertTrue(col.is_number())

    def test_is_float(self):
        col = FelderaColumn(column="ratio", dtype="DOUBLE")
        self.assertTrue(col.is_float())

    def test_string_size(self):
        col = FelderaColumn(column="name", dtype="VARCHAR")
        self.assertEqual(col.string_size(), 65535)


if __name__ == "__main__":
    unittest.main()
