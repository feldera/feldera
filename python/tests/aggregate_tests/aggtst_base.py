"""Run multiple Python tests in a single pipeline"""

import unittest

from feldera import PipelineBuilder, Pipeline
from tests import TEST_CLIENT
from feldera.enums import CompilationProfile
from feldera.runtime_config import RuntimeConfig
from typing import TypeAlias, Dict
import re

JSON: TypeAlias = dict[str, "JSON"] | list["JSON"] | str | int | float | bool | None

# pylint: disable=too-many-function-args,missing-function-docstring,no-self-argument,no-self-use,invalid-name,line-too-long,too-few-public-methods,missing-class-docstring,super-init-not-called

DEBUG = False


def beautify(sql: str) -> str:
    body = sql.split("\n")
    body = [x.strip() for x in body]
    return "\n".join(body)


class SqlObject:
    """Base class for tables and views
    Each sql object has a name, a definition in SQL, and some data"""

    def __init__(self, name: str, sql: str, data: JSON):
        """Create a SQL object"""
        self.name = name
        self.sql = beautify(sql)
        self.data = data

    def get_sql(self) -> str:
        """Get the SQL defining the object"""
        return self.sql

    def get_data(self) -> JSON:
        """Get the data that is the object's contents"""
        return self.data

    @staticmethod
    def extract_name(sql: str, table: bool) -> str:
        """Extract a table or view name from a SQL string;
        assumes a nicely written SQL program"""

        if table:
            pattern = r"CREATE\s+TABLE\s+(\w+)"
        else:
            pattern = r"CREATE\s+(?:MATERIALIZED|LOCAL)\s+VIEW\s+(\w+)"

        match = re.search(pattern, sql, re.IGNORECASE)
        # If a match is found, return the table name
        if match:
            return match.group(1)
        raise Exception("Could not parse sql '" + sql + "'")


class Table(SqlObject):
    """A SQL table with contents"""

    @staticmethod
    def add_insert(data: JSON) -> JSON:
        return [{"insert": x} for x in data]

    def __init__(self, sql: str, data: JSON):
        super().__init__(SqlObject.extract_name(sql, True), sql, Table.add_insert(data))
        if DEBUG:
            print(self.as_sql_insert())

    @staticmethod
    def row_to_values(row: Dict[str, any]) -> str:
        result = ""
        for key, value in row.items():
            if result != "":
                result += ", "
            if value is None:
                result += "NULL"
            else:
                result += str(value)
        return result

    def as_sql_insert(self) -> str:
        """Returns an insert statement for the table.  Assumes that the dictionary
        contains the columns in the right order"""
        stat = "INSERT INTO " + self.name + " VALUES"
        for row in self.data:
            if stat != "":
                stat += ", "
            stat += "(" + Table.row_to_values(row["insert"]) + ")"
        return stat + ";"


class View(SqlObject):
    """A SQL view with contents"""

    @staticmethod
    def sqlObject_is_local(sql: str) -> bool:
        """Checks if the SQL statement defines a local view"""
        pattern = r"\s+LOCAL\s+VIEW"
        return bool(re.search(pattern, sql, re.IGNORECASE))

    def __init__(self, sql: str, data: JSON):
        super().__init__(SqlObject.extract_name(sql, False), sql, data)
        # 'local' flag set based on SQL check
        self.local = View.sqlObject_is_local(sql)

    def assert_result(self, data, expected, msg):
        """Compare the data received and expected data and raise an error if they don't match"""
        # Sorting data received and expected data by string representation to ensure consistent sequence order between them
        sorted_data = sorted(data, key=lambda x: str(x))
        sorted_expected = sorted(expected, key=lambda x: str(x))

        if sorted_data != sorted_expected:
            raise AssertionError(
                f"{msg}\nExpected:\n{sorted_expected}\nReceived:\n{sorted_data}"
            )

    def validate(self, pipeline: Pipeline):
        """Check that the data received matches the expected data"""
        if not self.local:  # If it's not a local view, perform validation
            if DEBUG:
                print(f"Querying view `{self.name}`...")
            data = list(pipeline.query(f"SELECT * FROM {self.name};"))
            if DEBUG:
                print(f"Received data from view `{self.name}`: \n{data}")

            expected = self.get_data()

            tc = unittest.TestCase()
            tc.maxDiff = None  # display the difference between expected and actual results during an assertion error, even if the difference is large
            self.assert_result(
                data, expected, f"\nASSERTION ERROR: failed view: {self.name}"
            )


class TstAccumulator:
    """Base class which accumulates multiple DBSP tests to run and executes them"""

    def __init__(self):
        self.tables = []
        self.views = []

    def add_table(self, table: Table):
        """Add a new table to the program"""
        if DEBUG:
            print(f"Adding table {table.name}")
        self.tables.append(table)

    def add_view(self, view: View):
        """Add a new view to the program"""
        if DEBUG:
            print(f"Adding view {view.name}")
        self.views.append(view)

    @staticmethod
    def generate_sql(tables: list[Table], views: list[View]) -> str:
        """Generate SQL for the program"""
        result = ""
        for table in tables:
            if DEBUG:
                print(table.name, table.sql)
            result += table.get_sql() + ";\n\n"
        result += "\n"
        for view in views:
            result += view.get_sql() + ";\n\n"
        if DEBUG:
            print("Generated sql\n" + result)
        return result

    def run_pipeline(self, sql: str, views: list[View]):
        """Run pipeline with the given SQL, load tables, validate views, and shutdown"""
        pipeline = PipelineBuilder(
            TEST_CLIENT,
            "test",
            sql=sql,
            compilation_profile=CompilationProfile.DEV,
            runtime_config=RuntimeConfig(provisioning_timeout_secs=60),
        ).create_or_replace()

        pipeline.start()

        for table in self.tables:
            pipeline.input_json(
                table.name, table.get_data(), update_format="insert_delete"
            )

        pipeline.wait_for_completion(force_stop=False, timeout_s=3600)

        for view in views:
            view.validate(pipeline)

        pipeline.stop(force=True)

    def assert_expected_error(self, view: View, actual_exception: Exception):
        """Validate the error produced by the failing pipeline with the expected error type"""
        expected_substring = (
            str(getattr(view, "expected_error", "") or "").strip().lower()
        )
        actual_message = str(actual_exception).strip().lower()

        if DEBUG:
            print(
                f"[DEBUG] View `{view.name}` expected error substring: '{expected_substring}'"
            )
            print(
                f"[DEBUG] View `{view.name}` received error message:\n{actual_message}"
            )

        if expected_substring not in actual_message:
            raise AssertionError(
                f"\n[FAIL] failed view: {view.name} did not produce expected error substring.\n"
                f"Expected to find: '{expected_substring}'\n"
                f"Received error message:\n{actual_message}"
            )  # Validate based on: does the error received contain the expected substring?

        if DEBUG:
            print(f"[PASS] View `{view.name}` failed as expected.")

    def run_expected_failures(self):
        """Run each view that is expected to fail in a separate pipeline"""
        # List of views that contain the attribute: expected error type
        failing_views = [v for v in self.views if v.expected_error]
        for view in failing_views:
            sql = TstAccumulator.generate_sql(
                self.tables, [view]
            )  # Contains SQL for the failing view and its related tables only
            try:
                self.run_pipeline(sql, views=[view])
                raise AssertionError(
                    f"View: `{view.name}` was expected to fail, but it passed."
                )
            except Exception as e:
                self.assert_expected_error(view, e)

    def run_expected_successes(self):
        """Run all views that are expected to pass in a single pipeline"""
        # List of views that don't contain the attribute: expected error type
        passing_views = [v for v in self.views if not v.expected_error]
        if not passing_views:
            return
        sql = TstAccumulator.generate_sql(
            self.tables, passing_views
        )  # Contains SQL for all passing views and their related tables
        self.run_pipeline(sql, views=passing_views)

    def run_tests(self):
        """Run all tests registered"""
        self.run_expected_failures()
        self.run_expected_successes()


class TstTable:
    """Base class for defining tables"""

    def __init__(self):
        self.sql = ""
        self.data = []

    def register(self, ta: TstAccumulator):
        ta.add_table(Table(self.sql, self.data))


class TstView:
    """Base class for defining views"""

    expected_error = None

    def __init__(self):
        self.sql = ""

    def register(self, ta: TstAccumulator):
        view_data = getattr(self, "data", [])
        v = View(self.sql, view_data)
        v.expected_error = getattr(self, "expected_error", None)
        ta.add_view(v)
