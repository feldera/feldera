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
        self.assertEqual(FelderaColumn.translate_type("DOUBLE"), "float")

    def test_translate_type_decimal_with_precision(self):
        self.assertEqual(FelderaColumn.translate_type("DECIMAL(10,2)"), "numeric")

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

    def test_is_string_feldera_types(self):
        """Feldera types not in the base class's hardcoded string list."""
        for dtype in ("STRING", "CHAR", "BINARY", "VARBINARY"):
            with self.subTest(dtype=dtype):
                col = FelderaColumn(column="x", dtype=dtype)
                self.assertTrue(col.is_string(), f"{dtype} should be classified as string")

    def test_is_string_negative(self):
        for dtype in ("INTEGER", "DOUBLE", "BOOLEAN", "TIMESTAMP"):
            with self.subTest(dtype=dtype):
                col = FelderaColumn(column="x", dtype=dtype)
                self.assertFalse(col.is_string(), f"{dtype} should not be classified as string")

    def test_is_integer(self):
        col = FelderaColumn(column="id", dtype="BIGINT")
        self.assertTrue(col.is_integer())

    def test_is_integer_feldera_types(self):
        """Feldera types not in the base class's hardcoded integer list."""
        for dtype in ("TINYINT", "INT"):
            with self.subTest(dtype=dtype):
                col = FelderaColumn(column="x", dtype=dtype)
                self.assertTrue(col.is_integer(), f"{dtype} should be classified as integer")

    def test_is_integer_all_variants(self):
        for dtype in ("TINYINT", "SMALLINT", "INTEGER", "INT", "BIGINT"):
            with self.subTest(dtype=dtype):
                col = FelderaColumn(column="x", dtype=dtype)
                self.assertTrue(col.is_integer(), f"{dtype} should be classified as integer")

    def test_is_integer_negative(self):
        for dtype in ("DOUBLE", "VARCHAR", "BOOLEAN"):
            with self.subTest(dtype=dtype):
                col = FelderaColumn(column="x", dtype=dtype)
                self.assertFalse(col.is_integer(), f"{dtype} should not be classified as integer")

    def test_is_number(self):
        col = FelderaColumn(column="amount", dtype="DECIMAL(10,2)")
        self.assertTrue(col.is_number())

    def test_is_number_includes_integers(self):
        """is_number must return True for all integer types too."""
        for dtype in ("TINYINT", "SMALLINT", "INTEGER", "INT", "BIGINT"):
            with self.subTest(dtype=dtype):
                col = FelderaColumn(column="x", dtype=dtype)
                self.assertTrue(col.is_number(), f"{dtype} should be classified as number")

    def test_is_number_includes_floats(self):
        for dtype in ("REAL", "DOUBLE", "DECIMAL", "NUMERIC"):
            with self.subTest(dtype=dtype):
                col = FelderaColumn(column="x", dtype=dtype)
                self.assertTrue(col.is_number(), f"{dtype} should be classified as number")

    def test_is_number_parametrised(self):
        """Parameterised types like DECIMAL(10,2) must work — the base class fails on these."""
        for dtype in ("DECIMAL(10,2)", "NUMERIC(18,4)"):
            with self.subTest(dtype=dtype):
                col = FelderaColumn(column="x", dtype=dtype)
                self.assertTrue(col.is_number(), f"{dtype} should be classified as number")

    def test_is_number_negative(self):
        for dtype in ("VARCHAR", "BOOLEAN", "TIMESTAMP", "DATE"):
            with self.subTest(dtype=dtype):
                col = FelderaColumn(column="x", dtype=dtype)
                self.assertFalse(col.is_number(), f"{dtype} should not be classified as number")

    def test_is_float(self):
        col = FelderaColumn(column="ratio", dtype="DOUBLE")
        self.assertTrue(col.is_float())

    def test_is_float_all_variants(self):
        """is_float covers IEEE floating-point types: REAL (32-bit) and DOUBLE (64-bit)."""
        for dtype in ("REAL", "DOUBLE"):
            with self.subTest(dtype=dtype):
                col = FelderaColumn(column="x", dtype=dtype)
                self.assertTrue(col.is_float(), f"{dtype} should be classified as float")

    def test_is_float_negative(self):
        for dtype in ("INTEGER", "BIGINT", "VARCHAR", "BOOLEAN", "DECIMAL", "NUMERIC"):
            with self.subTest(dtype=dtype):
                col = FelderaColumn(column="x", dtype=dtype)
                self.assertFalse(col.is_float(), f"{dtype} should not be classified as float")

    def test_is_numeric(self):
        """is_numeric covers fixed-precision decimal types."""
        for dtype in ("DECIMAL", "NUMERIC"):
            with self.subTest(dtype=dtype):
                col = FelderaColumn(column="x", dtype=dtype)
                self.assertTrue(col.is_numeric(), f"{dtype} should be classified as numeric")

    def test_is_numeric_parametrised(self):
        for dtype in ("DECIMAL(10,2)", "NUMERIC(18,4)"):
            with self.subTest(dtype=dtype):
                col = FelderaColumn(column="x", dtype=dtype)
                self.assertTrue(col.is_numeric(), f"{dtype} should be classified as numeric")

    def test_is_numeric_negative(self):
        for dtype in ("INTEGER", "DOUBLE", "VARCHAR", "BOOLEAN"):
            with self.subTest(dtype=dtype):
                col = FelderaColumn(column="x", dtype=dtype)
                self.assertFalse(col.is_numeric(), f"{dtype} should not be classified as numeric")

    def test_is_methods_case_insensitive(self):
        """dtype may arrive in any case from Feldera."""
        col = FelderaColumn(column="x", dtype="varchar")
        self.assertTrue(col.is_string())
        col = FelderaColumn(column="x", dtype="bigint")
        self.assertTrue(col.is_integer())
        col = FelderaColumn(column="x", dtype="double")
        self.assertTrue(col.is_float())
        col = FelderaColumn(column="x", dtype="decimal")
        self.assertTrue(col.is_numeric())


if __name__ == "__main__":
    unittest.main()
