import sys
import sqlite3
from types import ModuleType
import re

from tests.aggregate_tests.aggtst_base import Table, View, TstAccumulator, DEBUG
from tests.aggregate_tests.atest_register_tests import register_tests_in_module


class SQLiteRunner:
    """Run Tables and Views on SQLite using a sqlite3 cursor"""

    def __init__(self, con: sqlite3.Connection):
        self.con = con
        self.cur = con.cursor()

    def run_table(self, table: Table):
        self.cur.execute(table.get_sql())
        for row in table.get_data():
            values = list(row["insert"].values())
            placeholders = ", ".join(["?"] * len(values))
            sql = f"INSERT INTO {table.name} VALUES ({placeholders})"
            self.cur.execute(sql, values)
        self.con.commit()

    def run_view(self, view: View):
        # Rewrite view query for SQLite
        sqlite_sql = re.sub(
            r"CREATE\s+(MATERIALIZED|LOCAL)\s+VIEW",
            "CREATE VIEW",
            view.get_sql(),
            flags=re.IGNORECASE,
        )
        self.cur.execute(sqlite_sql)
        self.con.commit()

    def run_all(self, tables: list[Table], views: list[View]):
        for table in tables:
            if DEBUG:
                print(f"\nCreating and inserting into table in SQLite: `{table.name}`")
            self.run_table(table)

        for view in views:
            if DEBUG:
                print(f"\nExecuting view in SQLite: `{view.name}`")
                print(f"Expected Data for `{view.name}` before SQLite run: {view.data}")
                print(f"\nCreating view in SQLite: `{view.name}`")

            self.run_view(view)

            self.cur.execute(f"SELECT * FROM {view.name}")
            columns = [desc[0] for desc in self.cur.description]
            rows = self.cur.fetchall()

            # Normalize the result from SQLite as lists of dictionaries
            normalized_rows = normalize_sqlite_rows(columns, rows)
            if DEBUG:
                print(f"Results from view `{view.name}` in SQLite: \n{normalized_rows}")

            # Replace the view's expected data with the normalized query results from SQLite
            view.data = normalized_rows


def discover_and_run_sqlite_tests(
    class_name: str, dir_name: str, sqlite_db_path=":memory:"
):
    """Find all tests loaded by the current module and register them"""
    ta = TstAccumulator()
    loaded = []
    for key, module in sys.modules.items():
        if isinstance(module, ModuleType):
            if not module.__name__.startswith(f"tests.{dir_name}"):
                continue
            loaded.append(module)
    for module in loaded:
        register_tests_in_module(module, ta, class_name)

    # Connect to SQLite to run tables/views
    con = sqlite3.connect(sqlite_db_path)
    runner = SQLiteRunner(con)
    runner.run_all(ta.tables, ta.views)
    con.close()

    return ta


def normalize_sqlite_rows(columns, rows):
    """Convert result from SQLite to lists of dicts with column names"""
    return [dict(zip(columns, row)) for row in rows]
