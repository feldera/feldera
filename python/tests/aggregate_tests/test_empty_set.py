from tests.aggregate_tests.aggtst_base import TstView


# ROW
class aggtst_count_col_emp_test_row(TstView):
    def __init__(self):
        self.data = [{"count_col": 0}]
        self.sql = """CREATE MATERIALIZED VIEW count_col_emp_row AS SELECT
                      COUNT(DISTINCT ROW(c1, c2, c3)) AS count_col
                      FROM row_tbl
                      WHERE FALSE"""


class aggtst_max_emp_test_row(TstView):
    def __init__(self):
        self.data = [{"max": None}]
        self.sql = """CREATE MATERIALIZED VIEW max_emp_row AS SELECT
                      MAX(ROW(c1, c2, c3)) FILTER(WHERE c1 IS NULL) AS max
                      FROM row_tbl"""


class aggtst_min_emp_test_row(TstView):
    def __init__(self):
        self.data = [{"min": None}]
        self.sql = """CREATE MATERIALIZED VIEW min_emp_row AS SELECT
                      MIN(ROW(c1, c2, c3)) AS min
                      FROM row_tbl
                      WHERE FALSE"""


class aggtst_argmin_emp_test_row(TstView):
    def __init__(self):
        self.data = [{"argmin": None}]
        self.sql = """CREATE MATERIALIZED VIEW argmin_emp_test_row AS SELECT
                      ARG_MIN(ROW(c1, c2, c3), c2) AS argmin
                      FROM row_tbl
                      WHERE FALSE"""


class aggtst_argmax_emp_test_row(TstView):
    def __init__(self):
        self.data = [{"argmax": None}]
        self.sql = """CREATE MATERIALIZED VIEW argmax_emp_test_row AS SELECT
                      ARG_MAX(ROW(c1, c2, c3), c2) AS argmax
                      FROM row_tbl
                      WHERE FALSE"""


class aggtst_arrgg_emp_test_row(TstView):
    def __init__(self):
        self.data = [{"arragg": []}]
        self.sql = """CREATE MATERIALIZED VIEW arrgg_emp_test_row AS SELECT
                      ARRAY_AGG(ROW(c2, c3)) AS arragg
                      FROM row_tbl
                      WHERE FALSE"""


class aggtst_some_emp_test_row(TstView):
    def __init__(self):
        self.data = [{"sme": None}]
        self.sql = """CREATE MATERIALIZED VIEW some_emp_row AS SELECT
                      SOME(c1 > SAFE_CAST(c2 AS INT)) AS sme
                      FROM row_tbl"""


class aggtst_every_emp_test_row(TstView):
    def __init__(self):
        self.data = [{"evry": None}]
        self.sql = """CREATE MATERIALIZED VIEW every_emp_row AS SELECT
                      EVERY(c1 > SAFE_CAST(c2 AS INT)) AS evry
                      FROM row_tbl"""


# Int
class aggtst_avg_emp_test(TstView):
    def __init__(self):
        self.data = [{"c1_avg": None, "c2_avg": None}]
        self.sql = """CREATE MATERIALIZED VIEW avg_emp AS SELECT
                      AVG(c1) FILTER(WHERE c1 < 1) AS c1_avg,
                      AVG(c2) FILTER(WHERE c2 < 2) AS c2_avg
                      FROM int_tbl"""


class aggtst_sum_emp_test(TstView):
    def __init__(self):
        self.data = [{"sum": None}]
        self.sql = """CREATE MATERIALIZED VIEW sum_emp AS SELECT
                      SUM(c1) FILTER(WHERE c1 < 1) AS sum
                      FROM int_tbl"""


class aggtst_stdev_samp_emp_test(TstView):
    def __init__(self):
        self.data = [{"stdev_samp": None}]
        self.sql = """CREATE MATERIALIZED VIEW stdev_samp_emp AS SELECT
                      STDDEV_SAMP(c1) FILTER(WHERE c1 < 1) AS stdev_samp
                      FROM int_tbl"""


class aggtst_stdev_pop_emp_test(TstView):
    def __init__(self):
        self.data = [{"stdev_pop": None}]
        self.sql = """CREATE MATERIALIZED VIEW stdev_pop_emp AS SELECT
                      STDDEV_POP(c1) AS stdev_pop
                      FROM int_tbl
                      WHERE c1 = 7"""
