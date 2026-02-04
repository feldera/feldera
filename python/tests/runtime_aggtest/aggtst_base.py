"""Run multiple Python tests in a single pipeline"""

import hashlib
import re
import unittest
from typing import Dict, TypeAlias

from feldera import Pipeline, PipelineBuilder
from feldera.enums import CompilationProfile
from feldera.rest.errors import FelderaAPIError
from feldera.runtime_config import RuntimeConfig
from feldera.testutils import FELDERA_TEST_NUM_WORKERS, FELDERA_TEST_NUM_HOSTS
from tests import TEST_CLIENT, unique_pipeline_name

JSON: TypeAlias = dict[str, "JSON"] | list["JSON"] | str | int | float | bool | None

# pylint: disable=too-many-function-args,missing-function-docstring,no-self-argument,no-self-use,invalid-name,line-too-long,too-few-public-methods,missing-class-docstring,super-init-not-called

DEBUG = False
APPEND_ONLY = False


def sql_hash(sql: str, length: int = 8) -> str:
    """Return a short stable hash string for a SQL statement."""
    return hashlib.sha256(sql.encode("utf-8")).hexdigest()[:length]


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
        if APPEND_ONLY:
            sql = beautify(sql) + " WITH ('append_only'='true')"
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
            # print(f"{msg}\nExpected:\n{sorted_expected}\nReceived:\n{sorted_data}")
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


class CompileTimeError(Exception):
    """Raised when the pipeline fails during compilation."""


class DeploymentErrorException(Exception):
    """Adds deployment error information to an exception.

    This wrapper is used to address the following race that occurs in negative tests:

    - Pipeline panics during a negative test (as expected)
    - The client keeps polling the pipeline, e.g., with /completion_status.
    - Before the client request hits the pipeline, the runner detects the
      panic and starts stopping the pipeline.
    - At this point, instead of an error indicating that the pipeline failed
      with a panic, the client will get back a different error - that the
      manager cannot interact with the pipeline in the stopping state.
      The problem with this is that the test expects the exception to
      contain a specific substring, and will fail if the string is not
      found.

    To address this, this wrapper enriches the exception thrown
    by the pipeline with the text in the deployment_error field of the
    pipeline.
    """

    def __init__(self, deployment_error: str, original_exception: Exception):
        self.deployment_error = deployment_error
        self.original_exception = original_exception
        super().__init__(deployment_error)

    def __str__(self) -> str:
        return (
            f"{str(self.original_exception)}\n"
            f"Pipeline deployment error: {self.deployment_error}"
        )


class RuntimeExecutionError(Exception):
    """Raised when the pipeline execution fails during runtime."""


class TstAccumulator:
    """Base class which accumulates multiple DBSP tests to run and executes them"""

    def __init__(self):
        self.tables = []
        self.views = []

    @staticmethod
    def get_object_type_name(obj: SqlObject) -> str:
        """Get the type name of a SQL object (Table or View)"""
        return "View" if isinstance(obj, View) else "Table"

    @staticmethod
    def has_expected_error(obj: SqlObject) -> bool:
        """Check if a table or a view has a non-empty expected_error attribute"""
        expected_error = getattr(obj, "expected_error", None)
        return expected_error is not None and str(expected_error).strip() != ""

    @staticmethod
    def filter_by_expected_error(
        objects: list[SqlObject], should_fail: bool
    ) -> list[SqlObject]:
        """Filter tables and views based on whether they are expected to fail or not"""
        if should_fail:
            return [obj for obj in objects if TstAccumulator.has_expected_error(obj)]
        else:
            return [
                obj for obj in objects if not TstAccumulator.has_expected_error(obj)
            ]

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

    def run_pipeline(
        self,
        pipeline_name_prefix: str,
        sql: str,
        views: list[View],
        tables: list[Table] = None,
    ):
        """Run pipeline with the given SQL, load tables, validate views, and shutdown"""
        if tables is None:
            tables = self.tables

        pipeline = None
        sql_id = sql_hash(sql)
        pipeline_name = unique_pipeline_name(f"{pipeline_name_prefix}_{sql_id}")
        try:
            # Pipelines that fail to compile will remain None
            # as `PipelineBuilder` will raise an exception and not
            # return a `Pipeline` object.
            pipeline = PipelineBuilder(
                TEST_CLIENT,
                pipeline_name,
                sql=sql,
                compilation_profile=CompilationProfile.UNOPTIMIZED,
                runtime_config=RuntimeConfig(
                    workers=FELDERA_TEST_NUM_WORKERS,
                    hosts=FELDERA_TEST_NUM_HOSTS,
                    provisioning_timeout_secs=180,
                    logging="debug",
                ),
            ).create_or_replace()

            pipeline.start()

            for table in tables:
                if table.get_data() != []:
                    pipeline.input_json(
                        table.name, table.get_data(), update_format="insert_delete"
                    )

                pipeline.wait_for_completion(force_stop=False, timeout_s=320)

            for view in views:
                view.validate(pipeline)

        except Exception as e:
            # Determine if this is a compile time or runtime error based on the pipeline state
            if pipeline is None:
                # Failure during pipeline creation is a compile time error
                raise CompileTimeError(f"COMPILE-TIME ERROR:\n{e}") from e
            else:
                # Augment exception with deployment error if available so that
                # assert_expected_error can pattern-match both against the expected error substring.
                deployment_error = pipeline.deployment_error()
                if deployment_error is not None and deployment_error:
                    # Convert deployment_error dict to string for exception
                    raise DeploymentErrorException(str(deployment_error), e)

                else:
                    # Pipeline was created, so the failure here is a runtime error
                    raise RuntimeExecutionError(f"RUNTIME ERROR:\n{e}") from e
        finally:
            # Try to get the pipelines that were created by
            # `PipelineBuilder` but failed to compile.
            if pipeline is None:
                try:
                    pipeline = Pipeline.get(pipeline_name, TEST_CLIENT)
                except FelderaAPIError as e:
                    if "UnknownPipelineName" in str(e):
                        pass
                    else:
                        raise

            if pipeline is not None:
                pipeline.stop(force=True)
                pipeline.delete(True)

    def assert_expected_error(self, obj: SqlObject, actual_exception: Exception):
        """Validate the error produced by the failing pipeline with the expected error type"""
        expected_substring = (
            str(getattr(obj, "expected_error", "") or "").strip().lower()
        )
        actual_message = str(actual_exception).strip().lower()

        obj_type = self.get_object_type_name(obj)

        if DEBUG:
            print(
                f"[DEBUG] {obj_type} `{obj.name}` expected error substring: '{expected_substring}'"
            )
            print(
                f"[DEBUG] {obj_type} `{obj.name}` received error message:\n{actual_message}"
            )

        if expected_substring not in actual_message:
            raise AssertionError(
                f"\n[FAIL] failed {obj_type.lower()}: {obj.name} did not produce expected error substring.\n"
                f"Expected to find: '{expected_substring}'\n"
                f"Received error message:\n{actual_message}"
            )

        if DEBUG:
            print(f"[PASS] {obj_type} `{obj.name}` failed as expected.")

    def run_failing_object_test(
        self,
        obj: SqlObject,
        pipeline_name_prefix: str,
        sql: str,
        views: list[View],
        tables: list[Table],
    ):
        """Run a test for a single object(view, table) expected to fail and verify it produces the expected error"""
        obj_type = self.get_object_type_name(obj)
        if DEBUG:
            print(f"Testing failing {obj_type.lower()}: {obj.name}...")

        try:
            self.run_pipeline(pipeline_name_prefix, sql, views=views, tables=tables)
            raise AssertionError(
                f"{obj_type}: `{obj.name}` was expected to fail, but it passed."
            )
        except AssertionError:
            raise
        except Exception as e:
            self.assert_expected_error(obj, e)

    def run_table_tests(self, pipeline_name_prefix: str):
        """Test passing tables together in a single pipeline, failing tables separately in individual pipelines"""
        # Separate tables by whether they have a non-empty expected_error attribute
        failing_tables = self.filter_by_expected_error(self.tables, should_fail=True)
        passing_tables = self.filter_by_expected_error(self.tables, should_fail=False)

        # Test all passing tables together
        if passing_tables:
            if DEBUG:
                print(f"Testing {len(passing_tables)} passing tables together...")
            sql = TstAccumulator.generate_sql(
                passing_tables, []
            )  # Contains SQL for all passing tables
            self.run_pipeline(
                pipeline_name_prefix, sql, views=[], tables=passing_tables
            )

        # Test each failing table individually
        for table in failing_tables:
            sql = table.get_sql()  # Contains SQL for the failing tables
            self.run_failing_object_test(
                table, pipeline_name_prefix, sql, views=[], tables=[table]
            )

    def run_expected_failures(self, pipeline_name_prefix: str):
        """Loop through each view that is expected to fail in a separate pipeline"""
        # Only use passing tables when testing views
        passing_tables = self.filter_by_expected_error(self.tables, should_fail=False)
        failing_views = self.filter_by_expected_error(self.views, should_fail=True)

        for view in failing_views:
            sql = TstAccumulator.generate_sql(
                passing_tables, [view]
            )  # Contains SQL for the failing views and tables
            self.run_failing_object_test(
                view, pipeline_name_prefix, sql, views=[view], tables=passing_tables
            )

    def run_expected_successes(self, pipeline_name_prefix: str):
        """Run all views that are expected to pass in a single pipeline"""
        # Use only passing tables when testing views
        passing_tables = self.filter_by_expected_error(self.tables, should_fail=False)
        passing_views = self.filter_by_expected_error(self.views, should_fail=False)

        if not passing_views:
            return
        sql = TstAccumulator.generate_sql(
            passing_tables, passing_views
        )  # Contains SQL for all passing views and their related tables
        self.run_pipeline(
            pipeline_name_prefix, sql, views=passing_views, tables=passing_tables
        )

    def run_tests(self, pipeline_name_prefix: str):
        """Run all tests registered"""
        # Test tables (passing tables together, failing tables individually)
        self.run_table_tests(pipeline_name_prefix)
        # Test views (failing views individually, passing views together)
        self.run_expected_failures(pipeline_name_prefix)
        self.run_expected_successes(pipeline_name_prefix)


class TstTable:
    """Base class for defining tables"""

    expected_error = None

    def __init__(self):
        self.sql = ""
        self.data = []

    def register(self, ta: TstAccumulator):
        table = Table(self.sql, self.data)
        table.expected_error = getattr(self, "expected_error", None)
        ta.add_table(table)


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
