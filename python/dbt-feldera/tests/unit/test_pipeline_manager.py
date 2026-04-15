import threading
import unittest

from dbt.adapters.feldera.pipeline_manager import PipelineStateManager


class TestPipelineStateManager(unittest.TestCase):
    """Unit tests for PipelineStateManager."""

    def setUp(self):
        self.manager = PipelineStateManager()

    def test_register_table(self):
        self.manager.register_table("test_pipe", "events", "CREATE TABLE events (id INT);")
        tables = self.manager.get_tables("test_pipe")
        self.assertIn("events", tables)
        self.assertEqual(tables["events"], "CREATE TABLE events (id INT);")

    def test_register_view(self):
        self.manager.register_view("test_pipe", "stats", "CREATE VIEW stats AS SELECT 1;")
        views = self.manager.get_views("test_pipe")
        self.assertIn("stats", views)

    def test_remove_table_if_exists(self):
        self.manager.register_table("pipe", "t1", "CREATE TABLE t1 (id INT);")
        self.manager.remove_table_if_exists("pipe", "t1")
        tables = self.manager.get_tables("pipe")
        self.assertNotIn("t1", tables)

    def test_remove_view_if_exists(self):
        self.manager.register_view("pipe", "v1", "CREATE VIEW v1 AS SELECT 1;")
        self.manager.remove_view_if_exists("pipe", "v1")
        views = self.manager.get_views("pipe")
        self.assertNotIn("v1", views)

    def test_remove_nonexistent_does_not_throw(self):
        """Removing a non-existent item should not raise or affect other state."""
        self.manager.register_table("pipe", "keep_me", "CREATE TABLE keep_me (id INT);")
        self.manager.remove_table_if_exists("pipe", "nonexistent")
        self.manager.remove_view_if_exists("pipe", "nonexistent")
        tables = self.manager.get_tables("pipe")
        self.assertEqual(len(tables), 1)
        self.assertIn("keep_me", tables)

    def test_get_tables_empty_pipeline(self):
        tables = self.manager.get_tables("nonexistent_pipeline")
        self.assertEqual(tables, {})

    def test_get_views_empty_pipeline(self):
        views = self.manager.get_views("nonexistent_pipeline")
        self.assertEqual(views, {})

    def test_assemble_program_tables_first(self):
        """Tables should appear before views in the assembled program."""
        self.manager.register_table("pipe", "t1", "CREATE TABLE t1 (id INT)")
        self.manager.register_view("pipe", "v1", "CREATE VIEW v1 AS SELECT * FROM t1")
        program = self.manager.assemble_program("pipe")
        t_pos = program.index("CREATE TABLE")
        v_pos = program.index("CREATE VIEW")
        self.assertLess(t_pos, v_pos)

    def test_assemble_program_empty(self):
        program = self.manager.assemble_program("empty_pipeline")
        self.assertEqual(program, "")

    def test_assemble_program_semicolons(self):
        """Each statement should end with a semicolon."""
        self.manager.register_table("pipe", "t1", "CREATE TABLE t1 (id INT)")
        self.manager.register_view("pipe", "v1", "CREATE VIEW v1 AS SELECT 1")
        program = self.manager.assemble_program("pipe")
        statements = [s.strip() for s in program.split("\n\n") if s.strip()]
        for stmt in statements:
            self.assertTrue(stmt.endswith(";"), f"Statement does not end with ';': {stmt}")

    def test_assemble_program_no_double_semicolons(self):
        """Statements that already have semicolons should not get doubles."""
        self.manager.register_table("pipe", "t1", "CREATE TABLE t1 (id INT);")
        program = self.manager.assemble_program("pipe")
        self.assertNotIn(";;", program)

    def test_has_sql_changed_first_time(self):
        """SQL should always be 'changed' the first time."""
        self.manager.register_table("pipe", "t1", "CREATE TABLE t1 (id INT)")
        self.assertTrue(self.manager.has_sql_changed("pipe"))

    def test_has_sql_changed_after_no_change(self):
        """After marking as deployed, unchanged SQL should report no change."""
        self.manager.register_table("pipe", "t1", "CREATE TABLE t1 (id INT)")
        self.manager._deployed_sql["pipe"] = self.manager.assemble_program("pipe")
        self.assertFalse(self.manager.has_sql_changed("pipe"))

    def test_has_sql_changed_after_modification(self):
        """After modifying SQL, it should report changed."""
        self.manager.register_table("pipe", "t1", "CREATE TABLE t1 (id INT)")
        self.manager._deployed_sql["pipe"] = self.manager.assemble_program("pipe")
        self.manager.register_view("pipe", "v1", "CREATE VIEW v1 AS SELECT 1")
        self.assertTrue(self.manager.has_sql_changed("pipe"))

    def test_multiple_pipelines_isolated(self):
        """Tables/views in different pipelines should not interfere."""
        self.manager.register_table("pipe_a", "t1", "CREATE TABLE t1 (a INT)")
        self.manager.register_table("pipe_b", "t1", "CREATE TABLE t1 (b INT)")
        tables_a = self.manager.get_tables("pipe_a")
        tables_b = self.manager.get_tables("pipe_b")
        self.assertIn("(a INT)", tables_a["t1"])
        self.assertIn("(b INT)", tables_b["t1"])

    def test_duplicate_table_raises(self):
        """Re-registering a table in the same pipeline raises ValueError."""
        self.manager.register_table("pipe", "t1", "CREATE TABLE t1 (id INT)")
        with self.assertRaises(ValueError):
            self.manager.register_table("pipe", "t1", "CREATE TABLE t1 (id BIGINT)")

    def test_thread_safety(self):
        """Concurrent registrations should not corrupt state."""
        errors = []

        def register_many(prefix, count):
            try:
                for i in range(count):
                    self.manager.register_table("pipe", f"{prefix}_{i}", f"CREATE TABLE {prefix}_{i} (id INT)")
                    self.manager.register_view("pipe", f"v_{prefix}_{i}", f"CREATE VIEW v_{prefix}_{i} AS SELECT 1")
            except Exception as e:
                errors.append(e)

        threads = [threading.Thread(target=register_many, args=(f"thread_{t}", 50)) for t in range(4)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        self.assertEqual(len(errors), 0, f"Thread safety errors: {errors}")
        tables = self.manager.get_tables("pipe")
        views = self.manager.get_views("pipe")

        # 4 threads * 50 tables
        self.assertEqual(len(tables), 200)
        self.assertEqual(len(views), 200)


class TestPipelineStateManagerRename(unittest.TestCase):
    """Unit tests for rename operations on PipelineStateManager."""

    def setUp(self):
        self.manager = PipelineStateManager()

    def test_rename_table_updates_ddl(self):
        """Simulating a table rename: old name removed, new name present."""
        self.manager.register_table("pipe", "old_tbl", "CREATE TABLE old_tbl (id INT);")
        # Simulate the rename performed by the adapter
        old_ddl = self.manager.get_tables("pipe")["old_tbl"]
        new_ddl = old_ddl.replace("old_tbl", "new_tbl")
        self.manager.remove_table_if_exists("pipe", "old_tbl")
        self.manager.register_table("pipe", "new_tbl", new_ddl)

        tables = self.manager.get_tables("pipe")
        self.assertNotIn("old_tbl", tables)
        self.assertIn("new_tbl", tables)
        self.assertEqual(tables["new_tbl"], "CREATE TABLE new_tbl (id INT);")

    def test_rename_view_updates_ddl(self):
        """Simulating a view rename: old name removed, new name present."""
        self.manager.register_view("pipe", "old_view", "CREATE VIEW old_view AS SELECT 1;")
        old_ddl = self.manager.get_views("pipe")["old_view"]
        new_ddl = old_ddl.replace("old_view", "new_view")
        self.manager.remove_view_if_exists("pipe", "old_view")
        self.manager.register_view("pipe", "new_view", new_ddl)

        views = self.manager.get_views("pipe")
        self.assertNotIn("old_view", views)
        self.assertIn("new_view", views)
        self.assertEqual(views["new_view"], "CREATE VIEW new_view AS SELECT 1;")

    def test_view_replacement_preserves_order(self):
        """Re-registering a view preserves its position (Python dict semantics)."""
        self.manager.register_view("pipe", "v1", "CREATE VIEW v1 AS SELECT 1")
        self.manager.register_view("pipe", "v2", "CREATE VIEW v2 AS SELECT 2")
        # Replace v1 with new DDL
        self.manager.register_view("pipe", "v1", "CREATE VIEW v1 AS SELECT 'new'")
        program = self.manager.assemble_program("pipe")
        v1_pos = program.index("v1")
        v2_pos = program.index("v2")
        self.assertLess(v1_pos, v2_pos, "v1 should still come before v2 after replacement")

    def test_view_order_preserved_in_assembly(self):
        """Views emitted in registration (insertion) order."""
        self.manager.register_view("pipe", "alpha", "CREATE VIEW alpha AS SELECT 1")
        self.manager.register_view("pipe", "beta", "CREATE VIEW beta AS SELECT 2")
        self.manager.register_view("pipe", "gamma", "CREATE VIEW gamma AS SELECT 3")
        program = self.manager.assemble_program("pipe")
        alpha_pos = program.index("alpha")
        beta_pos = program.index("beta")
        gamma_pos = program.index("gamma")
        self.assertLess(alpha_pos, beta_pos)
        self.assertLess(beta_pos, gamma_pos)


if __name__ == "__main__":
    unittest.main()
