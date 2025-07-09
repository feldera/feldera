from tests.aggregate_tests.aggtst_base import TstView


# Test different types of negative tests that are expected to fail
# Case 1: expected to fail -> fails
class ignore_check_negtest1(TstView):
    """Program expects Error: "Unimplemented" but receives Error: "Not Yet Implemented"
    what happens? ->
        SQL Compilation doesn't pass,
        Program Error"""

    def __init__(self):
        # checked manually
        self.sql = """CREATE MATERIALIZED VIEW check_negtest1 AS SELECT
                      CONCAT(bin, bin) AS bin
                      FROM illegal_tbl"""
        self.expected_error = "Unimplemented"


# Case 2: expected to fail -> fails
class ignore_check_negtest2(TstView):
    """Despite the SQL being correct, test case is supplied 'expected_error_type' attribute(it is considered a negative test), and data attribute isn't supplied
    what happens? ->
        SQL Compilation passes,
        Rust Compilation passes,
        but throws AssertionError, since expected_data != received_data"""

    def __init__(self):
        # checked manually
        self.sql = """CREATE MATERIALIZED VIEW check_negtest2 AS SELECT
                      CONCAT(str, str) AS str
                      FROM illegal_tbl"""
        self.expected_error = "Not yet implemented"


# Case 3: program was expected to fail-> but passes
class ignore_check_negtest3(TstView):
    """Despite the SQL being correct, test case is supplied 'expected_error_type' attribute(it is considered a negative test), but data attribute is supplied
    what happens?
        SQL Passes,
        Rust Compilation Passes,
        Program Execution Passes,
        but throws AssertionError, and Pipeline shuts down"""

    def __init__(self):
        # checked manually
        self.data = [{"str": "hello hello "}]
        self.sql = """CREATE MATERIALIZED VIEW check_negtest3 AS SELECT
                      CONCAT(str, str) AS str
                      FROM illegal_tbl
                      WHERE id = 0"""
        self.expected_error = "Not yet implemented"
