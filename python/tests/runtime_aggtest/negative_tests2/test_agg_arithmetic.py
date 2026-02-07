from tests.runtime_aggtest.aggtst_base import TstView


# Overflow in Aggregates Operations that perform arithmetic


# Sum
class neg_sum_tiny_int(TstView):
    def __init__(self):
        # checked manually
        self.sql = """CREATE MATERIALIZED VIEW sum_tiny_int AS SELECT
                      SUM(tiny_int) AS tiny_int
                      FROM numeric_tbl1"""
        self.expected_error = "Error converting 128 to TINYINT: out of range integral type conversion attempted"


class neg_sum_small_int(TstView):
    def __init__(self):
        # checked manually
        self.sql = """CREATE MATERIALIZED VIEW sum_small_int AS SELECT
                      SUM(small_int) AS small_int
                      FROM numeric_tbl1"""
        self.expected_error = "Error converting 32768 to SMALLINT: out of range integral type conversion attempted"


class neg_sum_big_int(TstView):
    def __init__(self):
        # checked manually
        self.sql = """CREATE MATERIALIZED VIEW sum_big_int AS SELECT
                      SUM(big_int) AS big_int
                      FROM numeric_tbl1"""
        self.expected_error = "Error converting 9223372036854775808 to BIGINT: out of range integral type conversion attempted"


class neg_sum_intt(TstView):
    def __init__(self):
        # checked manually
        self.sql = """CREATE MATERIALIZED VIEW sum_intt AS SELECT
                      SUM(intt) AS intt
                      FROM numeric_tbl1"""
        self.expected_error = "Error converting 2147483648 to INTEGER: out of range integral type conversion attempted"


class neg_sum_tiny_un_int(TstView):
    def __init__(self):
        # checked manually
        self.sql = """CREATE MATERIALIZED VIEW sum_tiny_un_int AS SELECT
                      SUM(tiny_int) AS tiny_int
                      FROM numeric_un_tbl1"""
        self.expected_error = "Error converting 256 to TINYINT UNSIGNED: out of range integral type conversion attempted"


class neg_sum_small_un_int(TstView):
    def __init__(self):
        # checked manually
        self.sql = """CREATE MATERIALIZED VIEW sum_small_un_int AS SELECT
                      SUM(small_int) AS small_int
                      FROM numeric_un_tbl1"""
        self.expected_error = "Error converting 65536 to SMALLINT UNSIGNED: out of range integral type conversion attempted"


class neg_sum_big_un_int(TstView):
    def __init__(self):
        # checked manually
        self.sql = """CREATE MATERIALIZED VIEW sum_big_un_int AS SELECT
                      SUM(big_int) AS big_int
                      FROM numeric_un_tbl1"""
        self.expected_error = "Error converting 18446744073709551616 to BIGINT UNSIGNED: out of range integral type conversion attempted"


class neg_sum_un_intt(TstView):
    def __init__(self):
        # checked manually
        self.sql = """CREATE MATERIALIZED VIEW sum_un_intt AS SELECT
                      SUM(intt) AS intt
                      FROM numeric_un_tbl1"""
        self.expected_error = "Error converting 4294967296 to INTEGER UNSIGNED: out of range integral type conversion attempted"
