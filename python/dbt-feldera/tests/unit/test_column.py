import unittest

from dbt.adapters.feldera.column import FelderaColumn

# Which canonical labels make each predicate return True.
_PRED_TRUE_LABELS = {
    "is_string": {"text"},
    "is_integer": {"integer"},
    "is_float": {"float"},
    "is_numeric": {"numeric"},
    "is_number": {"integer", "float", "numeric"},
}

# Extra translate_type inputs beyond plain TYPE_LABELS keys.
_EXTRA_TRANSLATE_CASES = [
    # Parameterised types (sqlglot must strip precision/scale).
    ("DECIMAL(10,2)", "numeric"),
    ("NUMERIC(18,4)", "numeric"),
    # Case variations (Feldera may return any casing).
    ("varchar", "text"),
    ("Bigint", "integer"),
    ("double", "float"),
    ("decimal", "numeric"),
    # Unknown type falls back to "text".
    ("UNKNOWN_TYPE", "text"),
]

# (field_dict, expected_column_name, expected_dtype)
_FROM_FIELD_CASES = [
    ({"name": "user_id", "columntype": {"type": "BIGINT", "nullable": False}}, "user_id", "BIGINT"),
    ({"name": "name", "columntype": "VARCHAR"}, "name", "VARCHAR"),
    ({"columntype": {"type": "INT"}}, "", "INT"),
]


class TestFelderaColumn(unittest.TestCase):
    """Unit tests for FelderaColumn."""

    # ── translate_type ──────────────────────────────────────────────

    def test_translate_type_all_known(self):
        """Every TYPE_LABELS key round-trips through sqlglot parsing."""
        for sql_type, expected in FelderaColumn.TYPE_LABELS.items():
            with self.subTest(sql_type=sql_type):
                self.assertEqual(FelderaColumn.translate_type(sql_type), expected)

    def test_translate_type_extras(self):
        """Parameterised types, mixed case, and unknowns."""
        for sql_type, expected in _EXTRA_TRANSLATE_CASES:
            with self.subTest(sql_type=sql_type):
                self.assertEqual(FelderaColumn.translate_type(sql_type), expected)

    # ── Classification predicates ───────────────────────────────────

    def test_predicates_all_known_types(self):
        """For every TYPE_LABELS entry, each is_* predicate is correct."""
        for sql_type, label in FelderaColumn.TYPE_LABELS.items():
            col = FelderaColumn(column="x", dtype=sql_type)
            for pred, true_labels in _PRED_TRUE_LABELS.items():
                with self.subTest(dtype=sql_type, pred=pred):
                    self.assertEqual(getattr(col, pred)(), label in true_labels)

    def test_predicates_extra_types(self):
        """Parameterised types, case variations, and unknowns classify correctly."""
        for sql_type, label in _EXTRA_TRANSLATE_CASES:
            col = FelderaColumn(column="x", dtype=sql_type)
            for pred, true_labels in _PRED_TRUE_LABELS.items():
                with self.subTest(dtype=sql_type, pred=pred):
                    self.assertEqual(getattr(col, pred)(), label in true_labels)

    # ── from_feldera_field ──────────────────────────────────────────

    def test_from_feldera_field(self):
        for field, exp_name, exp_dtype in _FROM_FIELD_CASES:
            with self.subTest(field=field):
                col = FelderaColumn.from_feldera_field(field)
                self.assertEqual(col.column, exp_name)
                self.assertEqual(col.dtype, exp_dtype)


if __name__ == "__main__":
    unittest.main()
