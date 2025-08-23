from tests.runtime_aggtest.aggtst_base import TstView


# Array
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


# Map
class aggtst_count_emp_test_map(TstView):
    def __init__(self):
        self.data = [{"count": 0}]
        self.sql = """CREATE MATERIALIZED VIEW count_emp_map AS SELECT
                      COUNT (*) AS count
                      FROM map_tbl
                      WHERE FALSE"""


class aggtst_count_col_emp_test_map(TstView):
    def __init__(self):
        self.data = [{"count_col": 0}]
        self.sql = """CREATE MATERIALIZED VIEW count_col_emp_map AS SELECT
                      COUNT(c1) AS count_col
                      FROM map_tbl
                      WHERE c1 IS NULL"""


class aggtst_max_emp_test_map(TstView):
    def __init__(self):
        self.data = [{"max": None}]
        self.sql = """CREATE MATERIALIZED VIEW max_emp_map AS SELECT
                      MAX(c1) FILTER(WHERE c1 IS NULL) AS max
                      FROM map_tbl"""


class aggtst_min_emp_test_map(TstView):
    def __init__(self):
        self.data = [{"min": None}]
        self.sql = """CREATE MATERIALIZED VIEW min_emp_map AS SELECT
                      MIN(c1) AS min
                      FROM map_tbl
                      WHERE c1 = c2"""


class aggtst_argmin_emp_test_map(TstView):
    def __init__(self):
        self.data = [{"argmin": None}]
        self.sql = """CREATE MATERIALIZED VIEW argmin_emp_test_map AS SELECT
                      ARG_MIN(c1, c2) AS argmin
                      FROM map_tbl
                      WHERE FALSE"""


class aggtst_argmax_emp_test_map(TstView):
    def __init__(self):
        self.data = [{"argmax": None}]
        self.sql = """CREATE MATERIALIZED VIEW argmax_emp_test_map AS SELECT
                      ARG_MAX(c2, c1) AS argmax
                      FROM map_tbl
                      WHERE FALSE"""


class aggtst_arrgg_emp_test_map(TstView):
    def __init__(self):
        self.data = [{"arragg": []}]
        self.sql = """CREATE MATERIALIZED VIEW arrgg_emp_test_map AS SELECT
                      ARRAY_AGG(c1) AS arragg
                      FROM map_tbl
                      WHERE FALSE"""


class aggtst_some_emp_test_map(TstView):
    def __init__(self):
        self.data = [{"sme": None}]
        self.sql = """CREATE MATERIALIZED VIEW some_emp_map AS SELECT
                      SOME(c1 > c2) AS sme
                      FROM map_tbl
                      WHERE FALSE"""


class aggtst_every_emp_test_map(TstView):
    def __init__(self):
        self.data = [{"evry": None}]
        self.sql = """CREATE MATERIALIZED VIEW every_emp_map AS SELECT
                      EVERY(c1 > c2) AS evry
                      FROM map_tbl
                      WHERE FALSE"""
