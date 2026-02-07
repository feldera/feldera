from tests.runtime_aggtest.aggtst_base import TstView


# AVG
class illarg_window_avg_legal(TstView):
    def __init__(self):
        # checked manually
        self.data = [{"intt": -6}, {"intt": -6}, {"intt": -6}]
        self.sql = """CREATE MATERIALIZED VIEW illarg_window_avg_legal AS SELECT
                      AVG(intt) OVER () AS intt
                      FROM illegal_tbl"""


# Negative Test
class illarg_window_avg_illegal(TstView):
    def __init__(self):
        # checked manually
        self.sql = """CREATE MATERIALIZED VIEW illarg_window_avg_illegal AS SELECT
            AVG(booll) OVER () AS booll_lead
        FROM illegal_tbl"""
        self.expected_error = " Cannot apply 'AVG' to arguments of type"


# COUNT
class illarg_window_count_legal(TstView):
    def __init__(self):
        # checked manually
        self.data = [
            {"id": 0, "count": 3},
            {"id": 1, "count": 3},
            {"id": 2, "count": 3},
        ]
        self.sql = """CREATE MATERIALIZED VIEW illarg_window_count_legal AS SELECT
                      id, COUNT(*) OVER () AS count
                      FROM illegal_tbl"""


class illarg_window_count_legal1(TstView):
    def __init__(self):
        # checked manually
        self.data = [
            {"id": 0, "count": 2},
            {"id": 1, "count": 2},
            {"id": 2, "count": 2},
        ]
        self.sql = """CREATE MATERIALIZED VIEW illarg_window_count_legal1 AS SELECT
                      id, COUNT(intt) OVER () AS count
                      FROM illegal_tbl"""


# Negative Test
class illarg_window_count_illegal(TstView):
    def __init__(self):
        # checked manually
        self.data = [{"intt": -6}]
        self.sql = """CREATE MATERIALIZED VIEW illarg_window_count_illegal AS SELECT
                      COUNT(*) OVER (ORDER BY arr) AS count
                      FROM illegal_tbl"""
        self.expected_error = (
            "OVER currently cannot sort on columns with type 'VARCHAR ARRAY'"
        )


# maybe a diff error message?
class illarg_window_count_illegal1(TstView):
    def __init__(self):
        # checked manually
        self.data = [{"intt": -6}]
        self.sql = """CREATE MATERIALIZED VIEW illarg_window_count_illegal1 AS SELECT
                      COUNT(NULL) OVER () AS count
                      FROM illegal_tbl"""
        self.expected_error = "Argument of aggregate has NULL type"


# DENSE_RANK
class illarg_window_dense_rank_legal(TstView):
    def __init__(self):
        # checked manually
        self.data = [{"id": 0, "dr": 1}, {"id": 1, "dr": 2}, {"id": 2, "dr": 3}]
        self.sql = """CREATE MATERIALIZED VIEW illarg_window_dense_rank_legal AS
                        SELECT * FROM (
                            SELECT id,
                                   DENSE_RANK() OVER (ORDER BY id) AS dr
                            FROM illegal_tbl
                        ) sub
                        WHERE dr <= 10; """


# Negative Test
class illarg_window_dense_rank_illegal(TstView):
    def __init__(self):
        # checked manually
        self.sql = """CREATE MATERIALIZED VIEW illarg_window_dense_rank_illegal AS SELECT
                      id, DENSE_RANK() OVER (ORDER BY 1) AS count
                      FROM illegal_tbl"""
        self.expected_error = "DENSE_RANK only supported in a TopK pattern"


# LAG
class illarg_window_lag_legal(TstView):
    def __init__(self):
        # checked manually
        self.data = [{"intt": -12}, {"intt": -1}, {"intt": None}]
        self.sql = """CREATE MATERIALIZED VIEW illarg_window_lag_legal AS SELECT
            LEAD(intt) OVER () AS intt
        FROM illegal_tbl"""


# Negative Test
class illarg_window_lag_illegal(TstView):
    def __init__(self):
        # checked manually
        self.sql = """CREATE MATERIALIZED VIEW illarg_window_lag_illegal AS SELECT
            LAG(arr[2], intt) OVER () AS arr
        FROM illegal_tbl"""
        self.expected_error = (
            "Currently LAG/LEAD amount must be a compile-time constant"
        )


# LEAD
class illarg_window_lead_legal(TstView):
    def __init__(self):
        # checked manually
        self.data = [{"intt": -12}, {"intt": -1}, {"intt": None}]
        self.sql = """CREATE MATERIALIZED VIEW illarg_window_lead_legal AS SELECT
                      LEAD(intt) OVER () AS intt
                      FROM illegal_tbl"""


# Negative Test
class illarg_window_lead_illegal(TstView):
    def __init__(self):
        # checked manually
        self.sql = """CREATE MATERIALIZED VIEW illarg_window_lead_illegal AS SELECT
                      LEAD(udt[2], intt) OVER () AS udt
                      FROM illegal_tbl"""
        self.expected_error = (
            "Currently LAG/LEAD amount must be a compile-time constant"
        )


# RANK
class illarg_window_rank_legal(TstView):
    def __init__(self):
        # checked manually
        self.data = [{"id": 0, "dr": 1}, {"id": 1, "dr": 2}, {"id": 2, "dr": 3}]
        self.sql = """CREATE MATERIALIZED VIEW illarg_window_rank_legal AS
                        SELECT * FROM (
                            SELECT id,
                                   RANK() OVER (ORDER BY id) AS dr
                            FROM illegal_tbl
                        ) sub
                        WHERE dr <= 10; """


# Negative Test
class illarg_window_rank_illegal(TstView):
    def __init__(self):
        # checked manually
        self.sql = """CREATE MATERIALIZED VIEW illarg_window_rank_illegal AS SELECT
                      id, RANK() OVER (ORDER BY NULL) AS count
                      FROM illegal_tbl"""
        self.expected_error = "RANK only supported in a TopK pattern"


# MAX
class illarg_window_max_legal(TstView):
    def __init__(self):
        # checked manually
        self.data = [{"intt_max": -1}, {"intt_max": -1}, {"intt_max": -1}]
        self.sql = """CREATE MATERIALIZED VIEW illarg_window_max_legal AS SELECT
                      MAX(intt) OVER () AS intt_max
                      FROM illegal_tbl"""


# Negative Test
class illarg_window_max_illegal(TstView):
    def __init__(self):
        # checked manually
        self.sql = """CREATE MATERIALIZED VIEW illarg_window_max_illegal AS SELECT
                      MAX(udt, intt) OVER () AS udt_max
                      FROM illegal_tbl"""
        self.expected_error = "Invalid number of arguments to function 'MAX'"


# MIN
class illarg_window_min_legal(TstView):
    def __init__(self):
        # checked manually
        self.data = [{"intt_max": -12}, {"intt_max": -12}, {"intt_max": -12}]
        self.sql = """CREATE MATERIALIZED VIEW illarg_window_min_legal AS SELECT
                      MIN(intt) OVER () AS intt_max
                      FROM illegal_tbl"""


# Negative Test
class illarg_window_min_illegal(TstView):
    def __init__(self):
        # checked manually
        self.sql = """CREATE MATERIALIZED VIEW illarg_window_min_illegal AS SELECT
                      MIN(udt, intt) OVER () AS udt_max
                      FROM illegal_tbl"""
        self.expected_error = "Invalid number of arguments to function 'MIN'"


# ROW_NUMBER
class illarg_window_row_num_legal(TstView):
    def __init__(self):
        # checked manually
        self.data = [{"id": 0, "dr": 1}, {"id": 1, "dr": 2}, {"id": 2, "dr": 3}]
        self.sql = """CREATE MATERIALIZED VIEW illarg_window_row_num_legal AS
                        SELECT * FROM (
                            SELECT id,
                                   ROW_NUMBER() OVER (ORDER BY id) AS dr
                            FROM illegal_tbl
                        ) sub
                        WHERE dr <= 10; """


# Negative Test
class illarg_window_row_num_illegal(TstView):
    def __init__(self):
        # checked manually
        self.sql = """CREATE MATERIALIZED VIEW illarg_window_row_num_illegal AS SELECT
                      id, ROW_NUMBER() OVER (ORDER BY NULL) AS count
                      FROM illegal_tbl"""
        self.expected_error = "ROW_NUMBER only supported in a TopK pattern"


# SUM
class illarg_window_sum_legal(TstView):
    def __init__(self):
        # checked manually
        self.data = [{"roww_sum": -13}, {"roww_sum": -13}, {"roww_sum": -13}]
        self.sql = """CREATE MATERIALIZED VIEW illarg_window_sum_legal AS SELECT
                      SUM(intt) OVER () AS roww_sum
                      FROM illegal_tbl"""


# Negative Test
class illarg_window_sum_illegal(TstView):
    def __init__(self):
        # checked manually
        self.sql = """CREATE MATERIALIZED VIEW illarg_window_sum_illegal AS SELECT
                      SUM(roww) OVER () AS roww_sum
                      FROM illegal_tbl"""
        self.expected_error = "Cannot apply 'SUM' to arguments of type"
