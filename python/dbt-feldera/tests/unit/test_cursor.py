import unittest
from decimal import Decimal

from dbt.adapters.feldera.cursor import ColumnDescription, FelderaCursor
from dbt.adapters.feldera.sql_parser import SqlIntent
from dbt.adapters.feldera.sqlglot_parser import parser

# ── Data tables ─────────────────────────────────────────────────────

# (sql, expected_intent)
_SQL_INTENT_CASES = [
    ("SELECT * FROM users", SqlIntent.ADHOC_QUERY),
    ("  SELECT 1", SqlIntent.ADHOC_QUERY),
    ("WITH cte AS (SELECT 1) SELECT * FROM cte", SqlIntent.ADHOC_QUERY),
    ("CREATE TABLE foo (id INT)", SqlIntent.PIPELINE_DDL),
    ("CREATE VIEW bar AS SELECT 1", SqlIntent.PIPELINE_DDL),
    ("DROP TABLE foo", SqlIntent.PIPELINE_DDL),
    ("ALTER TABLE foo ADD COLUMN bar INT", SqlIntent.ADHOC_QUERY),
    ("INSERT INTO foo VALUES (1)", SqlIntent.DATA_INGRESS),
    ("", SqlIntent.NO_OP),
    ("   ", SqlIntent.NO_OP),
    ("select * from foo", SqlIntent.ADHOC_QUERY),
    ("create table foo (id int)", SqlIntent.PIPELINE_DDL),
    ("EXPLAIN SELECT 1", SqlIntent.ADHOC_QUERY),
]

# (sql, expected_rowcount)  — DDL / NO_OP execute produces no rows.
_EXECUTE_NORESULT_CASES = [
    ("", 0),
    ("CREATE TABLE foo (id INT)", 0),
]

# (method_name, args, expected_return)  — fetch on empty result set.
_EMPTY_FETCH_CASES = [
    ("fetchall", [], []),
    ("fetchone", [], None),
    ("fetchmany", [5], []),
]

# (rows, columns, expected_types, label)
# Covers single-value mapping, empty results, null scan, and type lattice.
_TYPE_INFERENCE_CASES = [
    # -- Single-value type mapping --
    ([{"v": 42}], ["v"], ["INTEGER"], "integer"),
    ([{"v": "alice"}], ["v"], ["VARCHAR"], "string"),
    ([{"v": True}], ["v"], ["BOOLEAN"], "boolean"),
    # Python float → DOUBLE.  The Feldera SDK uses parse_float=Decimal so
    # JSON numbers arrive as Decimal; this covers raw-float edge cases.
    ([{"v": 3.14}], ["v"], ["DOUBLE"], "float"),
    # Feldera SDK: json.loads(..., parse_float=Decimal).
    ([{"v": Decimal("3.14")}], ["v"], ["DECIMAL"], "decimal"),
    ([{"v": None}], ["v"], [None], "null"),
    ([{"v": ["a", "b"]}], ["v"], ["ARRAY"], "array"),
    # Both ROW and MAP deserialize as dict; MAP is the safe default.
    ([{"v": {"k": "v"}}], ["v"], ["MAP"], "map"),
    # -- Empty results --
    ([], ["id", "name"], [None, None], "empty-results"),
    # -- Multiple columns in one row --
    (
        [{"id": 1, "name": "alice", "active": True, "score": 9.5}],
        ["id", "name", "active", "score"],
        ["INTEGER", "VARCHAR", "BOOLEAN", "DOUBLE"],
        "mixed-types",
    ),
    # -- Null scan: resolve type from later rows --
    (
        [{"v": None}, {"v": None}, {"v": 42}],
        ["v"],
        ["INTEGER"],
        "null-then-int",
    ),
    ([{"v": None}, {"v": None}], ["v"], [None], "all-null"),
    (
        [{"a": None, "b": "hello"}, {"a": Decimal("1.5"), "b": None}],
        ["a", "b"],
        ["DECIMAL", "VARCHAR"],
        "null-scan-mixed-cols",
    ),
    # -- Type lattice / widening --
    ([{"v": 32}, {"v": Decimal("32.5")}], ["v"], ["DECIMAL"], "int+decimal"),
    ([{"v": Decimal("1.5")}, {"v": 42}], ["v"], ["DECIMAL"], "decimal+int"),
    ([{"v": 10}, {"v": 3.14}], ["v"], ["DOUBLE"], "int+double"),
    ([{"v": Decimal("1.5")}, {"v": 2.7}], ["v"], ["DOUBLE"], "decimal+double"),
    ([{"v": True}, {"v": 42}], ["v"], [None], "bool+int-conflict"),
    ([{"v": [1, 2]}, {"v": {"k": "v"}}], ["v"], [None], "array+map-conflict"),
    ([{"v": 1}, {"v": 2}, {"v": 3}], ["v"], ["INTEGER"], "same-type-stable"),
    ([{"v": 42}, {"v": "text"}], ["v"], [None], "int+varchar-conflict"),
]


# ── Helpers ─────────────────────────────────────────────────────────


def _make_cursor():
    """Create a FelderaCursor with a lightweight mock client."""

    class _MockClient:
        pass

    return FelderaCursor(_MockClient(), "test_pipeline")


# ── Test classes ────────────────────────────────────────────────────


class TestSqlIntentClassification(unittest.TestCase):
    """Data-driven SQL intent classification tests."""

    def test_intent_cases(self):
        for sql, expected in _SQL_INTENT_CASES:
            with self.subTest(sql=sql):
                self.assertEqual(parser.classify(sql), expected)


class TestFelderaCursor(unittest.TestCase):
    """Unit tests for FelderaCursor execute / fetch behaviour."""

    def test_initial_state(self):
        cursor = _make_cursor()
        self.assertEqual(cursor.rowcount, -1)
        self.assertIsNone(cursor.description)

    def test_execute_noresult(self):
        """DDL and NO_OP execute produce zero rows."""
        for sql, expected_rowcount in _EXECUTE_NORESULT_CASES:
            with self.subTest(sql=sql):
                cursor = _make_cursor()
                cursor.execute(sql)
                self.assertEqual(cursor.rowcount, expected_rowcount)
                self.assertEqual(cursor.fetchall(), [])

    def test_empty_fetch(self):
        """All fetch methods return empty on a no-result execute."""
        for method, args, expected in _EMPTY_FETCH_CASES:
            with self.subTest(method=method):
                cursor = _make_cursor()
                cursor.execute("")
                self.assertEqual(getattr(cursor, method)(*args), expected)

    def test_close_raises_on_execute(self):
        cursor = _make_cursor()
        cursor.close()
        with self.assertRaises(RuntimeError):
            cursor.execute("SELECT 1")

    def test_results_after_manual_set(self):
        cursor = _make_cursor()
        cursor._results = [{"id": 1, "name": "alice"}, {"id": 2, "name": "bob"}]
        cursor._columns = ["id", "name"]
        cursor._rowcount = 2
        desc = cursor.description
        self.assertEqual(len(desc), 2)
        self.assertEqual(desc[0][0], "id")
        self.assertEqual(desc[1][0], "name")

    def test_fetchone_returns_tuple(self):
        cursor = _make_cursor()
        cursor._results = [{"id": 1, "name": "alice"}, {"id": 2, "name": "bob"}]
        self.assertEqual(cursor.fetchone(), (1, "alice"))
        self.assertEqual(cursor.fetchone(), (2, "bob"))
        self.assertIsNone(cursor.fetchone())

    def test_fetchall_returns_all(self):
        cursor = _make_cursor()
        cursor._results = [{"id": 1}, {"id": 2}, {"id": 3}]
        rows = cursor.fetchall()
        self.assertEqual(len(rows), 3)
        self.assertEqual(rows[0], (1,))
        self.assertEqual(rows[2], (3,))
        self.assertEqual(cursor.fetchall(), [])

    def test_fetchmany_returns_batch(self):
        cursor = _make_cursor()
        cursor._results = [{"id": i} for i in range(5)]
        self.assertEqual(len(cursor.fetchmany(3)), 3)
        self.assertEqual(len(cursor.fetchmany(10)), 2)


class TestFelderaCursorDescription(unittest.TestCase):
    """Unit tests for FelderaCursor.description type inference."""

    def test_type_inference(self):
        """Data-driven: single-value, null-scan, lattice, and mixed-type cases."""
        for rows, columns, expected_types, label in _TYPE_INFERENCE_CASES:
            with self.subTest(label=label):
                cursor = _make_cursor()
                cursor._results = rows
                cursor._columns = columns
                cursor._column_types = cursor._infer_column_types()
                self.assertEqual(cursor._column_types, expected_types)

    def test_description_none_when_no_columns(self):
        cursor = _make_cursor()
        self.assertIsNone(cursor.description)

    def test_description_format_is_7_tuple(self):
        cursor = _make_cursor()
        cursor._results = [{"id": 1, "name": "alice"}]
        cursor._columns = ["id", "name"]
        cursor._column_types = cursor._infer_column_types()
        for entry in cursor.description:
            self.assertEqual(len(entry), 7)

    def test_description_without_column_types_falls_back_to_none(self):
        """_columns set but _column_types not called — type_code is None."""
        cursor = _make_cursor()
        cursor._results = [{"id": 1}]
        cursor._columns = ["id"]
        desc = cursor.description
        self.assertEqual(desc[0][0], "id")
        self.assertIsNone(desc[0][1])

    def test_description_returns_named_tuples(self):
        """Each entry is a ColumnDescription with named fields."""
        cursor = _make_cursor()
        cursor._results = [{"id": 1}]
        cursor._columns = ["id"]
        cursor._column_types = cursor._infer_column_types()
        entry = cursor.description[0]
        self.assertIsInstance(entry, ColumnDescription)
        self.assertEqual(entry.name, "id")
        self.assertEqual(entry.type_code, "INTEGER")
        for field in ("display_size", "internal_size", "precision", "scale", "null_ok"):
            self.assertIsNone(getattr(entry, field))

    def test_description_named_tuple_unpackable(self):
        """NamedTuple is positionally unpackable per PEP 249."""
        cursor = _make_cursor()
        cursor._results = [{"id": 1}]
        cursor._columns = ["id"]
        cursor._column_types = cursor._infer_column_types()
        name, type_code, *rest = cursor.description[0]
        self.assertEqual(name, "id")
        self.assertEqual(type_code, "INTEGER")
        self.assertEqual(len(rest), 5)
        self.assertTrue(all(r is None for r in rest))


if __name__ == "__main__":
    unittest.main()
