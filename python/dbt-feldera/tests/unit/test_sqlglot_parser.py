import unittest

from dbt.adapters.feldera.sql_parser import SqlIntent
from dbt.adapters.feldera.sqlglot_parser import SqlglotParser

_parser = SqlglotParser()

# ---------------------------------------------------------------------------
# SqlParser.classify
# ---------------------------------------------------------------------------


class TestClassifySqlIntent(unittest.TestCase):
    """Tests for sqlglot-based SQL intent classification."""

    # -- adhoc_query --------------------------------------------------------

    def test_select(self):
        self.assertEqual(_parser.classify("SELECT * FROM users"), SqlIntent.ADHOC_QUERY)

    def test_select_leading_whitespace(self):
        self.assertEqual(_parser.classify("  SELECT 1"), SqlIntent.ADHOC_QUERY)

    def test_select_case_insensitive(self):
        self.assertEqual(_parser.classify("select * from foo"), SqlIntent.ADHOC_QUERY)

    def test_with_cte(self):
        self.assertEqual(
            _parser.classify("WITH cte AS (SELECT 1) SELECT * FROM cte"),
            SqlIntent.ADHOC_QUERY,
        )

    def test_union(self):
        self.assertEqual(
            _parser.classify("SELECT 1 UNION ALL SELECT 2"),
            SqlIntent.ADHOC_QUERY,
        )

    def test_explain_falls_back_to_adhoc(self):
        self.assertEqual(_parser.classify("EXPLAIN SELECT 1"), SqlIntent.ADHOC_QUERY)

    def test_unknown_keyword_falls_back_to_adhoc(self):
        self.assertEqual(_parser.classify("SHOW TABLES"), SqlIntent.ADHOC_QUERY)

    # -- pipeline_ddl -------------------------------------------------------

    def test_create_table(self):
        self.assertEqual(
            _parser.classify("CREATE TABLE foo (id INT)"),
            SqlIntent.PIPELINE_DDL,
        )

    def test_create_view(self):
        self.assertEqual(
            _parser.classify("CREATE VIEW bar AS SELECT 1"),
            SqlIntent.PIPELINE_DDL,
        )

    def test_create_materialized_view(self):
        self.assertEqual(
            _parser.classify("CREATE MATERIALIZED VIEW mv AS SELECT 1"),
            SqlIntent.PIPELINE_DDL,
        )

    def test_drop_table(self):
        self.assertEqual(_parser.classify("DROP TABLE foo"), SqlIntent.PIPELINE_DDL)

    def test_alter_table(self):
        self.assertEqual(
            _parser.classify("ALTER TABLE foo ADD COLUMN bar INT"),
            SqlIntent.PIPELINE_DDL,
        )

    def test_create_case_insensitive(self):
        self.assertEqual(
            _parser.classify("create table foo (id int)"),
            SqlIntent.PIPELINE_DDL,
        )

    # -- data_ingress -------------------------------------------------------

    def test_insert(self):
        self.assertEqual(
            _parser.classify("INSERT INTO foo VALUES (1)"),
            SqlIntent.DATA_INGRESS,
        )

    # -- no_op --------------------------------------------------------------

    def test_empty(self):
        self.assertEqual(_parser.classify(""), SqlIntent.NO_OP)

    def test_whitespace_only(self):
        self.assertEqual(_parser.classify("   "), SqlIntent.NO_OP)

    def test_comment_only(self):
        self.assertEqual(_parser.classify("-- just a comment"), SqlIntent.NO_OP)

    # -- comments before real SQL -------------------------------------------

    def test_line_comment_then_select(self):
        self.assertEqual(
            _parser.classify("-- comment\nSELECT 1"),
            SqlIntent.ADHOC_QUERY,
        )

    def test_block_comment_then_select(self):
        self.assertEqual(
            _parser.classify("/* block */ SELECT 1"),
            SqlIntent.ADHOC_QUERY,
        )


# ---------------------------------------------------------------------------
# extract_table_ddls
# ---------------------------------------------------------------------------


class TestExtractTableDdls(unittest.TestCase):
    """Tests for extracting CREATE TABLE DDLs from a pipeline program."""

    def test_mixed_program(self):
        program = (
            "CREATE TABLE t1 (id INT);\n\n"
            "CREATE TABLE t2 (name VARCHAR);\n\n"
            "CREATE VIEW v1 AS SELECT * FROM t1;\n\n"
            "CREATE VIEW v2 AS SELECT * FROM t2;"
        )
        ddls = _parser.extract_table_ddls(program)
        self.assertEqual(len(ddls), 2)
        for ddl in ddls:
            self.assertTrue(ddl.upper().startswith("CREATE TABLE"))
            self.assertTrue(ddl.endswith(";"))

    def test_only_views(self):
        program = "CREATE VIEW v1 AS SELECT 1; CREATE VIEW v2 AS SELECT 2;"
        ddls = _parser.extract_table_ddls(program)
        self.assertEqual(ddls, [])

    def test_empty_sql(self):
        self.assertEqual(_parser.extract_table_ddls(""), [])
        self.assertEqual(_parser.extract_table_ddls("   "), [])

    def test_single_table(self):
        ddls = _parser.extract_table_ddls("CREATE TABLE foo (id INT);")
        self.assertEqual(len(ddls), 1)

    def test_quoted_identifiers(self):
        ddls = _parser.extract_table_ddls('CREATE TABLE "MyTable" ("col" INT);')
        self.assertEqual(len(ddls), 1)
        self.assertIn("MyTable", ddls[0])

    def test_semicolons_in_strings(self):
        program = "CREATE TABLE t1 (id INT);\nCREATE TABLE t2 (d VARCHAR DEFAULT 'a;b');"
        ddls = _parser.extract_table_ddls(program)
        self.assertEqual(len(ddls), 2)

    def test_materialized_view_excluded(self):
        program = "CREATE TABLE t1 (id INT);\nCREATE MATERIALIZED VIEW mv AS SELECT 1;"
        ddls = _parser.extract_table_ddls(program)
        self.assertEqual(len(ddls), 1)

    def test_feldera_with_connectors_preserved(self):
        """Feldera WITH ('connectors' = '...') syntax must be preserved verbatim."""
        original = "CREATE TABLE kafka_sales (id INT) WITH ('connectors' = '[{\"name\": \"kafka\"}]');"
        program = original + "\nCREATE VIEW v1 AS SELECT * FROM kafka_sales;"
        ddls = _parser.extract_table_ddls(program)
        self.assertEqual(len(ddls), 1)
        # The quoted key must survive round-tripping.
        self.assertIn("'connectors'", ddls[0])


# ---------------------------------------------------------------------------
# extract_table_names
# ---------------------------------------------------------------------------


class TestExtractTableNames(unittest.TestCase):
    """Tests for extracting table names from DDL strings."""

    def test_basic(self):
        ddls = [
            "CREATE TABLE foo (id INT);",
            "CREATE TABLE bar (name VARCHAR);",
        ]
        names = _parser.extract_table_names(ddls)
        self.assertEqual(names, {"foo", "bar"})

    def test_case_normalized(self):
        ddls = ['CREATE TABLE "MyTable" (id INT);']
        names = _parser.extract_table_names(ddls)
        self.assertIn("mytable", names)

    def test_empty_list(self):
        self.assertEqual(_parser.extract_table_names([]), set())

    def test_invalid_ddl_skipped(self):
        names = _parser.extract_table_names(["not valid sql at all", "CREATE TABLE good (id INT);"])
        self.assertIn("good", names)

    def test_quoted_names(self):
        ddls = ['CREATE TABLE "quoted_name" (id INT);']
        names = _parser.extract_table_names(ddls)
        self.assertIn("quoted_name", names)


# ---------------------------------------------------------------------------
# rename_in_ddl
# ---------------------------------------------------------------------------


class TestRenameInDdl(unittest.TestCase):
    """Tests for AST-based DDL rename."""

    def test_rename_table(self):
        ddl = "CREATE TABLE old_name (id INT, name VARCHAR)"
        result = _parser.rename_in_ddl(ddl, "old_name", "new_name")
        self.assertIn("new_name", result)
        self.assertNotIn("old_name", result)
        self.assertIn("CREATE TABLE", result.upper())

    def test_rename_view(self):
        ddl = "CREATE VIEW old_v AS SELECT id FROM t1"
        result = _parser.rename_in_ddl(ddl, "old_v", "new_v")
        self.assertIn("new_v", result)
        self.assertNotIn("old_v", result)

    def test_rename_preserves_columns(self):
        ddl = "CREATE TABLE tbl (id INT, name VARCHAR, age INT)"
        result = _parser.rename_in_ddl(ddl, "tbl", "renamed")
        self.assertIn("id", result)
        self.assertIn("name", result)
        self.assertIn("age", result)

    def test_rename_quoted_identifier(self):
        ddl = 'CREATE TABLE "OldName" (id INT)'
        result = _parser.rename_in_ddl(ddl, "OldName", "NewName")
        self.assertIn("NewName", result)

    def test_no_match_falls_back(self):
        ddl = "CREATE TABLE completely_different (id INT)"
        result = _parser.rename_in_ddl(ddl, "nonexistent", "new_name")
        # Falls back to str.replace which won't find a match
        self.assertIn("completely_different", result)

    def test_rename_with_view_body(self):
        ddl = "CREATE VIEW stats AS SELECT COUNT(*) AS cnt FROM events"
        result = _parser.rename_in_ddl(ddl, "stats", "statistics")
        self.assertIn("statistics", result)
        # The body table reference should remain unchanged
        self.assertIn("events", result)

    def test_rename_preserves_feldera_with_clause(self):
        """Feldera WITH ('connectors' = '...') must survive rename."""
        ddl = "CREATE TABLE old_t (id INT) WITH ('connectors' = '[{\"name\": \"kafka\"}]')"
        result = _parser.rename_in_ddl(ddl, "old_t", "new_t")
        self.assertIn("new_t", result)
        self.assertNotIn("old_t", result)
        self.assertIn("'connectors'", result)


# ---------------------------------------------------------------------------
# sql_type_base_name
# ---------------------------------------------------------------------------


class TestSqlTypeBaseName(unittest.TestCase):
    """Tests for extracting the base type name from a SQL type string."""

    def test_simple_types(self):
        self.assertEqual(_parser.sql_type_base_name("INT"), "INT")
        self.assertEqual(_parser.sql_type_base_name("VARCHAR"), "VARCHAR")
        self.assertEqual(_parser.sql_type_base_name("BOOLEAN"), "BOOLEAN")
        self.assertEqual(_parser.sql_type_base_name("BIGINT"), "BIGINT")
        self.assertEqual(_parser.sql_type_base_name("DOUBLE"), "DOUBLE")
        self.assertEqual(_parser.sql_type_base_name("FLOAT"), "FLOAT")
        # sqlglot normalizes REAL → FLOAT
        self.assertEqual(_parser.sql_type_base_name("REAL"), "FLOAT")
        self.assertEqual(_parser.sql_type_base_name("DATE"), "DATE")
        self.assertEqual(_parser.sql_type_base_name("TIMESTAMP"), "TIMESTAMP")

    def test_parametric_types(self):
        self.assertEqual(_parser.sql_type_base_name("DECIMAL(10,2)"), "DECIMAL")
        self.assertEqual(_parser.sql_type_base_name("VARCHAR(255)"), "VARCHAR")
        # sqlglot normalizes NUMERIC → DECIMAL
        self.assertEqual(_parser.sql_type_base_name("NUMERIC(5,3)"), "DECIMAL")

    def test_case_insensitive_input(self):
        self.assertEqual(_parser.sql_type_base_name("varchar"), "VARCHAR")
        self.assertEqual(_parser.sql_type_base_name("decimal(10,2)"), "DECIMAL")
        self.assertEqual(_parser.sql_type_base_name("Bigint"), "BIGINT")

    def test_with_whitespace(self):
        self.assertEqual(_parser.sql_type_base_name("  INT  "), "INT")

    def test_integer_alias(self):
        # sqlglot normalizes INTEGER → INT
        result = _parser.sql_type_base_name("INTEGER")
        self.assertIn(result, ("INT", "INTEGER"))

    def test_array_type(self):
        result = _parser.sql_type_base_name("ARRAY")
        self.assertEqual(result, "ARRAY")

    def test_unknown_type_fallback(self):
        # Types sqlglot doesn't know should still return something reasonable
        result = _parser.sql_type_base_name("WEIRD_CUSTOM_TYPE")
        self.assertEqual(result, "WEIRD_CUSTOM_TYPE")


if __name__ == "__main__":
    unittest.main()
