import unittest

from dbt.adapters.feldera.cursor import FelderaCursor
from dbt.adapters.feldera.sql_parser import SqlIntent
from dbt.adapters.feldera.sqlglot_parser import parser


class TestSqlIntentClassification(unittest.TestCase):
    """Unit tests for SQL intent classification."""

    def test_select_is_adhoc_query(self):
        self.assertEqual(parser.classify("SELECT * FROM users"), SqlIntent.ADHOC_QUERY)

    def test_select_with_whitespace(self):
        self.assertEqual(parser.classify("  SELECT 1"), SqlIntent.ADHOC_QUERY)

    def test_with_cte_is_adhoc_query(self):
        self.assertEqual(
            parser.classify("WITH cte AS (SELECT 1) SELECT * FROM cte"),
            SqlIntent.ADHOC_QUERY,
        )

    def test_create_table_is_ddl(self):
        self.assertEqual(
            parser.classify("CREATE TABLE foo (id INT)"),
            SqlIntent.PIPELINE_DDL,
        )

    def test_create_view_is_ddl(self):
        self.assertEqual(
            parser.classify("CREATE VIEW bar AS SELECT 1"),
            SqlIntent.PIPELINE_DDL,
        )

    def test_drop_is_ddl(self):
        self.assertEqual(parser.classify("DROP TABLE foo"), SqlIntent.PIPELINE_DDL)

    def test_alter_is_ddl(self):
        self.assertEqual(
            parser.classify("ALTER TABLE foo ADD COLUMN bar INT"),
            SqlIntent.PIPELINE_DDL,
        )

    def test_insert_is_ingress(self):
        self.assertEqual(
            parser.classify("INSERT INTO foo VALUES (1)"),
            SqlIntent.DATA_INGRESS,
        )

    def test_empty_string_is_noop(self):
        self.assertEqual(parser.classify(""), SqlIntent.NO_OP)

    def test_whitespace_only_is_noop(self):
        self.assertEqual(parser.classify("   "), SqlIntent.NO_OP)

    def test_case_insensitive(self):
        self.assertEqual(parser.classify("select * from foo"), SqlIntent.ADHOC_QUERY)
        self.assertEqual(parser.classify("create table foo (id int)"), SqlIntent.PIPELINE_DDL)

    def test_unknown_defaults_to_adhoc(self):
        self.assertEqual(parser.classify("EXPLAIN SELECT 1"), SqlIntent.ADHOC_QUERY)


class TestFelderaCursor(unittest.TestCase):
    """Unit tests for FelderaCursor."""

    def _make_cursor(self):
        """Create a cursor with a mock client."""

        class MockClient:
            pass

        return FelderaCursor(MockClient(), "test_pipeline")

    def test_initial_state(self):
        cursor = self._make_cursor()
        self.assertEqual(cursor.rowcount, -1)
        self.assertIsNone(cursor.description)

    def test_execute_noop(self):
        cursor = self._make_cursor()
        cursor.execute("")
        self.assertEqual(cursor.rowcount, 0)
        self.assertEqual(cursor.fetchall(), [])

    def test_execute_ddl_is_captured(self):
        cursor = self._make_cursor()
        cursor.execute("CREATE TABLE foo (id INT)")
        self.assertEqual(cursor.rowcount, 0)
        self.assertEqual(cursor.fetchall(), [])

    def test_fetchall_empty(self):
        cursor = self._make_cursor()
        cursor.execute("")
        self.assertEqual(cursor.fetchall(), [])

    def test_fetchone_empty(self):
        cursor = self._make_cursor()
        cursor.execute("")
        self.assertIsNone(cursor.fetchone())

    def test_fetchmany_empty(self):
        cursor = self._make_cursor()
        cursor.execute("")
        self.assertEqual(cursor.fetchmany(5), [])

    def test_close(self):
        cursor = self._make_cursor()
        cursor.close()
        with self.assertRaises(RuntimeError):
            cursor.execute("SELECT 1")

    def test_results_after_manual_set(self):
        """Test fetch methods when results are manually set (simulating a response)."""
        cursor = self._make_cursor()
        cursor._results = [{"id": 1, "name": "alice"}, {"id": 2, "name": "bob"}]
        cursor._columns = ["id", "name"]
        cursor._rowcount = 2

        desc = cursor.description
        self.assertEqual(len(desc), 2)
        self.assertEqual(desc[0][0], "id")
        self.assertEqual(desc[1][0], "name")

    def test_fetchone_returns_tuple(self):
        cursor = self._make_cursor()
        cursor._results = [{"id": 1, "name": "alice"}, {"id": 2, "name": "bob"}]
        row = cursor.fetchone()
        self.assertEqual(row, (1, "alice"))
        row2 = cursor.fetchone()
        self.assertEqual(row2, (2, "bob"))
        self.assertIsNone(cursor.fetchone())

    def test_fetchall_returns_all(self):
        cursor = self._make_cursor()
        cursor._results = [{"id": 1}, {"id": 2}, {"id": 3}]
        rows = cursor.fetchall()
        self.assertEqual(len(rows), 3)
        self.assertEqual(rows[0], (1,))
        self.assertEqual(rows[2], (3,))
        # After fetchall, subsequent fetch returns empty
        self.assertEqual(cursor.fetchall(), [])

    def test_fetchmany_returns_batch(self):
        cursor = self._make_cursor()
        cursor._results = [{"id": i} for i in range(5)]
        batch = cursor.fetchmany(3)
        self.assertEqual(len(batch), 3)
        remaining = cursor.fetchmany(10)
        self.assertEqual(len(remaining), 2)


class TestFelderaCursorDescription(unittest.TestCase):
    """Unit tests for FelderaCursor.description type inference."""

    def _make_cursor(self):
        class MockClient:
            pass

        return FelderaCursor(MockClient(), "test_pipeline")

    def test_description_none_when_no_columns(self):
        cursor = self._make_cursor()
        self.assertIsNone(cursor.description)

    def test_description_infers_integer_type(self):
        cursor = self._make_cursor()
        cursor._results = [{"id": 42}]
        cursor._columns = ["id"]
        cursor._column_types = cursor._infer_column_types()
        desc = cursor.description
        self.assertEqual(desc[0][0], "id")
        self.assertEqual(desc[0][1], "INTEGER")

    def test_description_infers_string_type(self):
        cursor = self._make_cursor()
        cursor._results = [{"name": "alice"}]
        cursor._columns = ["name"]
        cursor._column_types = cursor._infer_column_types()
        desc = cursor.description
        self.assertEqual(desc[0][1], "VARCHAR")

    def test_description_infers_boolean_type(self):
        cursor = self._make_cursor()
        cursor._results = [{"active": True}]
        cursor._columns = ["active"]
        cursor._column_types = cursor._infer_column_types()
        desc = cursor.description
        self.assertEqual(desc[0][1], "BOOLEAN")

    def test_description_infers_float_type(self):
        cursor = self._make_cursor()
        cursor._results = [{"score": 3.14}]
        cursor._columns = ["score"]
        cursor._column_types = cursor._infer_column_types()
        desc = cursor.description
        self.assertEqual(desc[0][1], "DOUBLE")

    def test_description_null_value_gives_none_type(self):
        cursor = self._make_cursor()
        cursor._results = [{"val": None}]
        cursor._columns = ["val"]
        cursor._column_types = cursor._infer_column_types()
        desc = cursor.description
        self.assertEqual(desc[0][0], "val")
        self.assertIsNone(desc[0][1])

    def test_description_mixed_types(self):
        cursor = self._make_cursor()
        cursor._results = [{"id": 1, "name": "alice", "active": True, "score": 9.5}]
        cursor._columns = ["id", "name", "active", "score"]
        cursor._column_types = cursor._infer_column_types()
        desc = cursor.description
        self.assertEqual(desc[0][1], "INTEGER")
        self.assertEqual(desc[1][1], "VARCHAR")
        self.assertEqual(desc[2][1], "BOOLEAN")
        self.assertEqual(desc[3][1], "DOUBLE")

    def test_description_format_is_7_tuple(self):
        cursor = self._make_cursor()
        cursor._results = [{"id": 1, "name": "alice"}]
        cursor._columns = ["id", "name"]
        cursor._column_types = cursor._infer_column_types()
        desc = cursor.description
        for entry in desc:
            self.assertEqual(len(entry), 7)

    def test_description_infers_array_type(self):
        cursor = self._make_cursor()
        cursor._results = [{"tags": ["a", "b"]}]
        cursor._columns = ["tags"]
        cursor._column_types = cursor._infer_column_types()
        desc = cursor.description
        self.assertEqual(desc[0][1], "ARRAY")

    def test_description_infers_map_type(self):
        cursor = self._make_cursor()
        cursor._results = [{"meta": {"k": "v"}}]
        cursor._columns = ["meta"]
        cursor._column_types = cursor._infer_column_types()
        desc = cursor.description
        self.assertEqual(desc[0][1], "MAP")

    def test_description_empty_results_gives_none_types(self):
        cursor = self._make_cursor()
        cursor._results = []
        cursor._columns = ["id", "name"]
        cursor._column_types = cursor._infer_column_types()
        desc = cursor.description
        self.assertEqual(len(desc), 2)
        self.assertIsNone(desc[0][1])
        self.assertIsNone(desc[1][1])

    def test_description_without_column_types_falls_back_to_none(self):
        """Existing code that sets _columns but not _column_types still works."""
        cursor = self._make_cursor()
        cursor._results = [{"id": 1}]
        cursor._columns = ["id"]
        # Do NOT call _infer_column_types
        desc = cursor.description
        self.assertEqual(desc[0][0], "id")
        self.assertIsNone(desc[0][1])


if __name__ == "__main__":
    unittest.main()
