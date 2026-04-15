import unittest

from dbt.adapters.feldera.relation import FelderaRelation, FelderaRelationType


class TestFelderaRelationType(unittest.TestCase):
    """Unit tests for FelderaRelationType enum."""

    def test_table_type(self):
        self.assertEqual(FelderaRelationType.Table, "table")

    def test_view_type(self):
        self.assertEqual(FelderaRelationType.View, "view")

    def test_materialized_view_type(self):
        self.assertEqual(FelderaRelationType.MaterializedView, "materialized_view")


class TestFelderaRelation(unittest.TestCase):
    """Unit tests for FelderaRelation."""

    def test_quote_character(self):
        """Verify the default quote character is double-quote (Calcite standard)."""
        rel = FelderaRelation.create(
            database="default",
            schema="analytics",
            identifier="user_stats",
        )
        self.assertEqual(rel.quote_character, '"')

    def test_create_basic(self):
        """Test creating a basic relation."""
        rel = FelderaRelation.create(
            database="default",
            schema="my_pipeline",
            identifier="events",
        )
        self.assertEqual(rel.database, "default")
        self.assertEqual(rel.schema, "my_pipeline")
        self.assertEqual(rel.identifier, "events")


if __name__ == "__main__":
    unittest.main()
