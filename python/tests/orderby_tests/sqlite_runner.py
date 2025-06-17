import sqlite3
import re

from tests.aggregate_tests.aggtst_base import Table, View, DEBUG
from tests.aggregate_tests.atest_run import discover_tests
from tests.orderby_tests.automate_orderby_views import AutomateOrderByTests
from decimal import Decimal


sqlite_db_path = ":memory:"  # Create an in-memory database


class SQLiteRunner:
    """Run Tables and Views on SQLite using a sqlite3 cursor"""

    def __init__(self, con: sqlite3.Connection):
        self.con = con
        self.cur = con.cursor()

    def fill_table(self, table: Table):
        """Inserts the data from `table` into the SQLite table with the same name"""
        self.cur.execute(
            table.get_sql()
        )  # Get table definition from `get_sql` and create a table in SQLite with the same name
        for row in table.get_data():
            values = list(row["insert"].values())
            placeholders = ", ".join(["?"] * len(values))
            sql = f"INSERT INTO {table.name} VALUES ({placeholders})"
            self.cur.execute(sql, values)
        self.con.commit()

    def create_view(self, view: View):
        """Rewrite the view query for SQLite"""
        sqlite_sql = re.sub(
            r"CREATE\s+(MATERIALIZED|LOCAL)\s+VIEW",
            "CREATE VIEW",
            view.get_sql(),
            flags=re.IGNORECASE,
        )
        self.cur.execute(sqlite_sql)
        self.con.commit()

    def evaluate_view(self, view: View):
        """Create view in SQLite and return normalized results"""
        self.create_view(view)

        self.cur.execute(f"SELECT * FROM {view.name}")
        columns = [desc[0] for desc in self.cur.description]
        rows = self.cur.fetchall()

        # Normalize the result from SQLite as lists of dictionaries
        return normalize_sqlite_rows(columns, rows)

    def evaluate_views(self, tables: list[Table], views: list[View]):
        """Execute all tables and views in SQLite and update expected data with the view results"""
        for table in tables:
            if DEBUG:
                print(f"\nCreating and inserting into table in SQLite: `{table.name}`")
            self.fill_table(table)

        for view in views:
            if DEBUG:
                print(f"\nExecuting view in SQLite: `{view.name}`")
                print(f"Expected Data for `{view.name}` before SQLite run: {view.data}")
                print(f"\nCreating view in SQLite: `{view.name}`")

            normalized_rows = self.evaluate_view(view)
            if DEBUG:
                print(f"Results from view `{view.name}` in SQLite: \n{normalized_rows}")

            # Replace the view's expected data with the normalized query results from SQLite
            view.data = normalized_rows


def discover_sqlite_tests(class_name: str, dir_name: str, extra_register=bool):
    """Find all tests loaded by the current module and register them"""
    ta = discover_tests(class_name, dir_name)

    # list of tables starting with `orderby_tbl_sqlite`
    sqlite_tables = [t for t in ta.tables if t.name.startswith("orderby_tbl_sqlite")]

    if extra_register:
        # Dynamically add parameterized ORDER BY views for all existing tables
        for table in sqlite_tables:  # iterate only over the list of table objects starting with `orderby_tbl_sqlite` registered in the Accumulator
            AutomateOrderByTests(table.name).register(ta)

    sqlite_table_names = {t.name for t in sqlite_tables}
    # list of views that reference the tables starting with `orderby_tbl_sqlite`
    sqlite_views = [
        v for v in ta.views if any(tbl in v.sql for tbl in sqlite_table_names)
    ]

    # Connect to SQLite to run tables/views
    con = sqlite3.connect(sqlite_db_path)
    runner = SQLiteRunner(con)
    runner.evaluate_views(sqlite_tables, sqlite_views)
    con.close()  # Close the connection to SQLite and delete the database

    return ta


def normalize_sqlite_rows(columns, rows):
    """Convert result from SQLite to lists of dicts with column names"""
    result = []
    for row in rows:
        row_dict = {}
        for col, value in zip(columns, row):
            # If output contains Floating Point values for any column, convert them to Decimal values
            if isinstance(value, float):
                row_dict[col] = Decimal(str(value))
            else:
                row_dict[col] = value
        result.append(row_dict)
    return result
