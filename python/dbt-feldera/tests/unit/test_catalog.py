import unittest
from unittest.mock import MagicMock

from dbt.adapters.feldera.column import FelderaColumn
from dbt.adapters.feldera.impl import FelderaAdapter
from dbt.adapters.feldera.relation import FelderaRelation

CATALOG_COLUMNS = [
    "table_database",
    "table_schema",
    "table_name",
    "table_type",
    "table_comment",
    "table_owner",
    "column_name",
    "column_index",
    "column_type",
    "column_comment",
]


class TestGetOneCatalog(unittest.TestCase):
    """Tests for FelderaAdapter._get_one_catalog."""

    def _build_adapter_mock(self, relations, columns_by_relation):
        """Return a mock that looks enough like a FelderaAdapter."""
        adapter = MagicMock(spec=FelderaAdapter)
        adapter.Relation = FelderaRelation
        adapter.list_relations_without_caching = MagicMock(return_value=relations)
        adapter.get_columns_in_relation = MagicMock(side_effect=lambda r: columns_by_relation.get(r.identifier, []))
        adapter._catalog_filter_table = MagicMock(side_effect=lambda table, _: table)
        # Bind the real method so we can call it
        adapter._get_one_catalog = FelderaAdapter._get_one_catalog.__get__(adapter)
        return adapter

    def _make_info_schema(self, database="default"):
        info = MagicMock()
        info.database = database
        return info

    def test_empty_schemas(self):
        """No schemas → empty catalog."""
        adapter = self._build_adapter_mock([], {})
        info = self._make_info_schema()
        table = adapter._get_one_catalog(info, set(), frozenset())
        self.assertEqual(len(table), 0)
        self.assertEqual(list(table.column_names), CATALOG_COLUMNS)

    def test_schema_with_no_relations(self):
        """A schema exists but has no relations → empty catalog."""
        adapter = self._build_adapter_mock([], {})
        info = self._make_info_schema()
        table = adapter._get_one_catalog(info, {"my_pipeline"}, frozenset())
        self.assertEqual(len(table), 0)

    def test_single_relation_with_columns(self):
        """One relation with two columns to two catalog rows.

        Columns come from FelderaColumn objects. In production:
        Pipeline.get() -> p.tables()/p.views() -> field dicts -> FelderaColumn.
        Here we mock the column list to test catalog assembly in isolation.
        """
        rel = FelderaRelation.create(database="default", schema="my_pipeline", identifier="users")
        cols = [
            FelderaColumn(column="id", dtype="INTEGER"),
            FelderaColumn(column="name", dtype="VARCHAR"),
        ]
        adapter = self._build_adapter_mock([rel], {"users": cols})
        info = self._make_info_schema()

        table = adapter._get_one_catalog(
            info,
            {"my_pipeline"},
            frozenset(),
        )

        self.assertEqual(len(table), 2)
        self.assertEqual(list(table.column_names), CATALOG_COLUMNS)

        row0 = dict(zip(table.column_names, table.rows[0].values()))
        self.assertEqual(row0["table_database"], "default")
        self.assertEqual(row0["table_schema"], "my_pipeline")
        self.assertEqual(row0["table_name"], "users")
        self.assertEqual(row0["column_name"], "id")
        self.assertEqual(row0["column_index"], 1)
        self.assertEqual(row0["column_type"], "INTEGER")

        row1 = dict(zip(table.column_names, table.rows[1].values()))
        self.assertEqual(row1["column_name"], "name")
        self.assertEqual(row1["column_index"], 2)
        self.assertEqual(row1["column_type"], "VARCHAR")

    def test_multiple_relations(self):
        """Two relations → catalog contains rows for both."""
        rel_users = FelderaRelation.create(database="default", schema="pipe", identifier="users")
        rel_orders = FelderaRelation.create(database="default", schema="pipe", identifier="orders")
        cols_users = [FelderaColumn(column="id", dtype="INTEGER")]
        cols_orders = [
            FelderaColumn(column="order_id", dtype="BIGINT"),
            FelderaColumn(column="total", dtype="DECIMAL"),
        ]
        adapter = self._build_adapter_mock(
            [rel_users, rel_orders],
            {"users": cols_users, "orders": cols_orders},
        )
        info = self._make_info_schema()

        table = adapter._get_one_catalog(info, {"pipe"}, frozenset())

        self.assertEqual(len(table), 3)
        names = [row["table_name"] for row in table]
        self.assertIn("users", names)
        self.assertIn("orders", names)

    def test_relation_with_no_columns(self):
        """Relation with no columns → no rows for that relation.

        Feldera's SQL compiler does not support tables without columns.
        This test verifies the catalog builder handles the edge case
        gracefully rather than crashing.
        """
        rel = FelderaRelation.create(database="default", schema="pipe", identifier="empty_table")
        adapter = self._build_adapter_mock([rel], {"empty_table": []})
        info = self._make_info_schema()

        table = adapter._get_one_catalog(info, {"pipe"}, frozenset())
        self.assertEqual(len(table), 0)


if __name__ == "__main__":
    unittest.main()
