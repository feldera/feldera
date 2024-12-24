from tests.aggregate_tests.aggtst_base import TstView


class aggtst_count_emp_test(TstView):
    def __init__(self):
        self.data = [{"count": 0}]
        self.sql = """CREATE MATERIALIZED VIEW count_emp AS SELECT
                      COUNT (*) AS count
                      FROM array_tbl
                      WHERE FALSE"""


class aggtst_count_col_emp_test(TstView):
    def __init__(self):
        self.data = [{"count_col": 0}]
        self.sql = """CREATE MATERIALIZED VIEW count_col_emp AS SELECT
                      COUNT(c1) AS count_col
                      FROM array_tbl
                      WHERE FALSE"""


class aggtst_max_emp_test(TstView):
    def __init__(self):
        self.data = [{"max": None}]
        self.sql = """CREATE MATERIALIZED VIEW max_emp AS SELECT
                      MAX(c1) FILTER(WHERE c1 IS NULL) AS max
                      FROM array_tbl"""


class aggtst_min_emp_test(TstView):
    def __init__(self):
        self.data = [{"min": None}]
        self.sql = """CREATE MATERIALIZED VIEW min_emp AS SELECT
                      MIN(c1) FILTER(WHERE c1 IS NULL)  AS min
                      FROM array_tbl"""


class aggtst_arg_max_emp_test(TstView):
    def __init__(self):
        self.data = [{"arg_max": None}]
        self.sql = """CREATE MATERIALIZED VIEW arg_max_emp AS SELECT
                      ARG_MAX(c1, c2) FILTER(WHERE c1 IS NULL)  AS arg_max
                      FROM array_tbl"""


class aggtst_arg_min_emp_test(TstView):
    def __init__(self):
        self.data = [{"arg_min": None}]
        self.sql = """CREATE MATERIALIZED VIEW arg_min_emp AS SELECT
                      ARG_MIN(c1, c2) FILTER(WHERE c1 IS NULL)  AS arg_min
                      FROM array_tbl"""


class aggtst_some_emp_test(TstView):
    def __init__(self):
        self.data = [{"sme": None}]
        self.sql = """CREATE MATERIALIZED VIEW some_emp AS SELECT
                      SOME(c1 > c2) AS sme
                      FROM array_tbl
                      WHERE FALSE"""


class aggtst_every_emp_test(TstView):
    def __init__(self):
        self.data = [{"evry": None}]
        self.sql = """CREATE MATERIALIZED VIEW every_emp AS SELECT
                      EVERY(c1 > c2) AS evry
                      FROM array_tbl
                      WHERE FALSE"""


class aggtst_arr_agg_emp_test(TstView):
    def __init__(self):
        self.data = [{"arr_agg": []}]
        self.sql = """CREATE MATERIALIZED VIEW arr_agg_emp AS SELECT
                      ARRAY_AGG(c1) AS arr_agg
                      FROM array_tbl
                      WHERE FALSE"""


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
