from tests.aggregate_tests.aggtst_base import TstView


# Test different types of negative tests that are expected to fail but end up passing


# Case 1:
# Program expects Error: "Unimplemented" but receives Error: "Not Yet Implemented"
class illarg_check_negtest1(TstView):
    def __init__(self):
        # checked manually
        self.sql = """CREATE MATERIALIZED VIEW check_negtest1 AS SELECT
                      CONCAT(bin, bin) AS bin
                      FROM illegal_tbl"""
        self.expected_error = "Unimplemented"


# Case 2:
# Despite the SQL being correct, test case is supplied 'expected_error_type' attribute(it is considered a negative test), and data attribute isn't supplied
class illarg_check_negtest2(TstView):
    def __init__(self):
        # checked manually
        self.sql = """CREATE MATERIALIZED VIEW check_negtest2 AS SELECT
                      CONCAT(str, str) AS str
                      FROM illegal_tbl"""
        self.expected_error = "Not yet implemented"


# Case 3:
# Despite the SQL being correct, test case is supplied 'expected_error_type' attribute(it is considered a negative test), but data attribute is supplied
class illarg_check_negtest3(TstView):
    def __init__(self):
        # checked manually
        self.data = [{"str": "hello hello "}]
        self.sql = """CREATE MATERIALIZED VIEW check_negtest3 AS SELECT
                      CONCAT(str, str) AS str
                      FROM illegal_tbl"""
        self.expected_error = "Not yet implemented"
