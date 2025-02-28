"""Run multiple Python tests in a single pipeline"""

import unittest

from feldera import PipelineBuilder, Pipeline
from tests import TEST_CLIENT
from feldera.enums import CompilationProfile

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
        self.local = View.sqlObject_is_local(sql)  # 'local' flag set based on SQL check

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
            data = list(pipeline.query(f"SELECT * FROM {self.name};"))
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

    def generate_sql(self) -> str:
        """Generate SQL for the program"""
        result = ""
        for table in self.tables:
            if DEBUG:
                print(table.name, table.sql)
            result += table.get_sql() + ";\n\n"
        result += "\n"
        for view in self.views:
            result += view.get_sql() + ";\n\n"
        if DEBUG:
            print("Generated sql\n" + result)
        return result

    def run_tests(self):
        """Run all tests registered"""
        sql = self.generate_sql()
        pipeline = PipelineBuilder(
            TEST_CLIENT, "test", sql=sql, compilation_profile=CompilationProfile.DEV
        ).create_or_replace()

        pipeline.start()

        for table in self.tables:
            data = table.get_data()
            pipeline.input_json(table.name, data, update_format="insert_delete")

        pipeline.wait_for_completion(shutdown=False, timeout_s=3600)
        for view in self.views:
            view.validate(pipeline)

        pipeline.shutdown()


class TstTable:
    """Base class for defining tables"""

    def __init__(self):
        self.sql = ""
        self.data = []

    def register(self, ta: TstAccumulator):
        ta.add_table(Table(self.sql, self.data))


class TstView:
    """Base class for defining views"""

    def __init__(self):
        self.sql = ""
        self.data = []

    def register(self, ta: TstAccumulator):
        ta.add_view(View(self.sql, self.data))
