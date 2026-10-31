from tests.runtime_aggtest.aggtst_base import TstView


class aggtst_interval_mths_argmax(TstView):
    def __init__(self):
        # Result validation is not required for local views
        self.data = []
        self.sql = """CREATE LOCAL VIEW interval_mths_argmax AS SELECT
                      ARG_MAX(c1_minus_c2, c2_minus_c1) AS f_c1,
                      ARG_MAX(c2_minus_c1, c1_minus_c2) AS f_c2,
                      ARG_MAX(c1_minus_c3, c3_minus_c1) AS f_c3,
                      ARG_MAX(c3_minus_c1, c1_minus_c3) AS f_c4,
                      ARG_MAX(c2_minus_c3, c3_minus_c2) AS f_c5,
                      ARG_MAX(c3_minus_c2, c2_minus_c3) AS f_c6
                      FROM atbl_interval_months"""


class aggtst_interval_mths_argmax_res(TstView):
    def __init__(self):
        # checked manually
        self.data = [
            {
                "f_c1": "-558",
                "f_c2": "-605",
                "f_c3": "-217",
                "f_c4": "-88",
                "f_c5": "-639",
                "f_c6": "-341",
            }
        ]
        self.sql = """CREATE MATERIALIZED VIEW interval_mths_argmax_res AS SELECT
                      CAST(f_c1 AS VARCHAR) AS f_c1,
                      CAST(f_c2 AS VARCHAR) AS f_c2,
                      CAST(f_c3 AS VARCHAR) AS f_c3,
                      CAST(f_c4 AS VARCHAR) AS f_c4,
                      CAST(f_c5 AS VARCHAR) AS f_c5,
                      CAST(f_c6 AS VARCHAR) AS f_c6
                      FROM interval_mths_argmax"""


class aggtst_interval_mths_argmax_gby(TstView):
    def __init__(self):
        # Result validation is not required for local views
        self.data = []
        self.sql = """CREATE LOCAL VIEW interval_mths_argmax_gby AS SELECT
                      id,
                      ARG_MAX(c1_minus_c2, c2_minus_c1) AS f_c1,
                      ARG_MAX(c2_minus_c1, c1_minus_c2) AS f_c2,
                      ARG_MAX(c1_minus_c3, c3_minus_c1) AS f_c3,
                      ARG_MAX(c3_minus_c1, c1_minus_c3) AS f_c4,
                      ARG_MAX(c2_minus_c3, c3_minus_c2) AS f_c5,
                      ARG_MAX(c3_minus_c2, c2_minus_c3) AS f_c6
                      FROM atbl_interval_months
                      GROUP BY id"""


class aggtst_interval_mths_argmax_res_gby(TstView):
    def __init__(self):
        # checked manually
        self.data = [
            {
                "id": 0,
                "f_c1": "-121",
                "f_c2": "-605",
                "f_c3": "-64",
                "f_c4": "+34",
                "f_c5": "-639",
                "f_c6": "-57",
            },
            {
                "id": 1,
                "f_c1": "-558",
                "f_c2": "-605",
                "f_c3": "-217",
                "f_c4": "-88",
                "f_c5": "-622",
                "f_c6": "-341",
            },
        ]
        self.sql = """CREATE MATERIALIZED VIEW interval_mths_argmax_res_gby AS SELECT
                      id,
                      CAST(f_c1 AS VARCHAR) AS f_c1,
                      CAST(f_c2 AS VARCHAR) AS f_c2,
                      CAST(f_c3 AS VARCHAR) AS f_c3,
                      CAST(f_c4 AS VARCHAR) AS f_c4,
                      CAST(f_c5 AS VARCHAR) AS f_c5,
                      CAST(f_c6 AS VARCHAR) AS f_c6
                      FROM interval_mths_argmax_gby"""


class aggtst_interval_argmax_distinct_mths(TstView):
    def __init__(self):
        # Result validation is not required for local views
        self.data = []
        self.sql = """CREATE LOCAL VIEW interval_argmax_distinct_mths AS SELECT
                      ARG_MAX(DISTINCT c1_minus_c2, c2_minus_c1) AS f_c1,
                      ARG_MAX(DISTINCT c2_minus_c1, c1_minus_c2) AS f_c2,
                      ARG_MAX(DISTINCT c1_minus_c3, c3_minus_c1) AS f_c3,
                      ARG_MAX(DISTINCT c3_minus_c1, c1_minus_c3) AS f_c4,
                      ARG_MAX(DISTINCT c2_minus_c3, c3_minus_c2) AS f_c5,
                      ARG_MAX(DISTINCT c3_minus_c2, c2_minus_c3) AS f_c6
                      FROM atbl_interval_months"""


class aggtst_interval_argmax_distinct_mths_res(TstView):
    def __init__(self):
        # checked manually
        self.data = [
            {
                "f_c1": "-558",
                "f_c2": "-605",
                "f_c3": "-217",
                "f_c4": "-88",
                "f_c5": "-639",
                "f_c6": "-341",
            }
        ]
        self.sql = """CREATE MATERIALIZED VIEW interval_argmax_distinct_mths_res AS SELECT
                      CAST(f_c1 AS VARCHAR) AS f_c1,
                      CAST(f_c2 AS VARCHAR) AS f_c2,
                      CAST(f_c3 AS VARCHAR) AS f_c3,
                      CAST(f_c4 AS VARCHAR) AS f_c4,
                      CAST(f_c5 AS VARCHAR) AS f_c5,
                      CAST(f_c6 AS VARCHAR) AS f_c6
                      FROM interval_argmax_distinct_mths"""


class aggtst_interval_argmax_distinct_mths_gby(TstView):
    def __init__(self):
        # Result validation is not required for local views
        self.data = []
        self.sql = """CREATE LOCAL VIEW interval_argmax_distinct_mths_gby AS SELECT
                      id,
                      ARG_MAX(DISTINCT c1_minus_c2, c2_minus_c1) AS f_c1,
                      ARG_MAX(DISTINCT c2_minus_c1, c1_minus_c2) AS f_c2,
                      ARG_MAX(DISTINCT c1_minus_c3, c3_minus_c1) AS f_c3,
                      ARG_MAX(DISTINCT c3_minus_c1, c1_minus_c3) AS f_c4,
                      ARG_MAX(DISTINCT c2_minus_c3, c3_minus_c2) AS f_c5,
                      ARG_MAX(DISTINCT c3_minus_c2, c2_minus_c3) AS f_c6
                      FROM atbl_interval_months
                      GROUP BY id"""


class aggtst_interval_argmax_distinct_mths_gby_res(TstView):
    def __init__(self):
        # checked manually
        self.data = [
            {
                "id": 0,
                "f_c1": "-121",
                "f_c2": "-605",
                "f_c3": "-64",
                "f_c4": "+34",
                "f_c5": "-639",
                "f_c6": "-57",
            },
            {
                "id": 1,
                "f_c1": "-558",
                "f_c2": "-605",
                "f_c3": "-217",
                "f_c4": "-88",
                "f_c5": "-622",
                "f_c6": "-341",
            },
        ]
        self.sql = """CREATE MATERIALIZED VIEW interval_argmax_distinct_mths_gby_res AS SELECT
                      id,
                      CAST(f_c1 AS VARCHAR) AS f_c1,
                      CAST(f_c2 AS VARCHAR) AS f_c2,
                      CAST(f_c3 AS VARCHAR) AS f_c3,
                      CAST(f_c4 AS VARCHAR) AS f_c4,
                      CAST(f_c5 AS VARCHAR) AS f_c5,
                      CAST(f_c6 AS VARCHAR) AS f_c6
                      FROM interval_argmax_distinct_mths_gby"""


class aggtst_interval_argmax_where_mths(TstView):
    def __init__(self):
        # Result validation is not required for local views
        self.data = []
        self.sql = """CREATE LOCAL VIEW interval_argmax_where_mths AS SELECT
                      ARG_MAX(c1_minus_c2, c2_minus_c1) FILTER(WHERE c1_minus_c2 > c2_minus_c1) AS f_c1,
                      ARG_MAX(c2_minus_c1, c1_minus_c2) FILTER(WHERE c1_minus_c2 > c2_minus_c1) AS f_c2,
                      ARG_MAX(c1_minus_c3, c3_minus_c1) FILTER(WHERE c1_minus_c3 > c3_minus_c1) AS f_c3,
                      ARG_MAX(c3_minus_c1, c1_minus_c3) FILTER(WHERE c1_minus_c3 > c3_minus_c1) AS f_c4,
                      ARG_MAX(c2_minus_c3, c3_minus_c2) FILTER(WHERE c2_minus_c3 > c3_minus_c2) AS f_c5,
                      ARG_MAX(c3_minus_c2, c2_minus_c3) FILTER(WHERE c2_minus_c3 > c3_minus_c2) AS f_c6
                      FROM atbl_interval_months"""


class aggtst_interval_argmax_where_mths_res(TstView):
    def __init__(self):
        # checked manually
        self.data = [
            {
                "f_c1": "+121",
                "f_c2": "-605",
                "f_c3": "+88",
                "f_c4": "-88",
                "f_c5": "+57",
                "f_c6": "-341",
            }
        ]
        self.sql = """CREATE MATERIALIZED VIEW interval_argmax_where_mths_res AS SELECT
                      CAST(f_c1 AS VARCHAR) AS f_c1,
                      CAST(f_c2 AS VARCHAR) AS f_c2,
                      CAST(f_c3 AS VARCHAR) AS f_c3,
                      CAST(f_c4 AS VARCHAR) AS f_c4,
                      CAST(f_c5 AS VARCHAR) AS f_c5,
                      CAST(f_c6 AS VARCHAR) AS f_c6
                      FROM interval_argmax_where_mths"""


class aggtst_interval_argmax_where_mths_gby(TstView):
    def __init__(self):
        # Result validation is not required for local views
        self.data = []
        self.sql = """CREATE LOCAL VIEW interval_argmax_where_mths_gby AS SELECT
                      id,
                      ARG_MAX(c1_minus_c2, c2_minus_c1) FILTER(WHERE c1_minus_c2 > c2_minus_c1) AS f_c1,
                      ARG_MAX(c2_minus_c1, c1_minus_c2) FILTER(WHERE c1_minus_c2 > c2_minus_c1) AS f_c2,
                      ARG_MAX(c1_minus_c3, c3_minus_c1) FILTER(WHERE c1_minus_c3 > c3_minus_c1) AS f_c3,
                      ARG_MAX(c3_minus_c1, c1_minus_c3) FILTER(WHERE c1_minus_c3 > c3_minus_c1) AS f_c4,
                      ARG_MAX(c2_minus_c3, c3_minus_c2) FILTER(WHERE c2_minus_c3 > c3_minus_c2) AS f_c5,
                      ARG_MAX(c3_minus_c2, c2_minus_c3) FILTER(WHERE c2_minus_c3 > c3_minus_c2) AS f_c6
                      FROM atbl_interval_months
                      GROUP BY id"""


class aggtst_interval_argmax_where_mths_res_gby(TstView):
    def __init__(self):
        # checked manually
        self.data = [
            {
                "id": 0,
                "f_c1": "+605",
                "f_c2": "-605",
                "f_c3": None,
                "f_c4": None,
                "f_c5": "+57",
                "f_c6": "-57",
            },
            {
                "id": 1,
                "f_c1": "+121",
                "f_c2": "-605",
                "f_c3": "+88",
                "f_c4": "-88",
                "f_c5": "+341",
                "f_c6": "-341",
            },
        ]
        self.sql = """CREATE MATERIALIZED VIEW interval_argmax_where_mths_res_gby AS SELECT
                      id,
                      CAST(f_c1 AS VARCHAR) AS f_c1,
                      CAST(f_c2 AS VARCHAR) AS f_c2,
                      CAST(f_c3 AS VARCHAR) AS f_c3,
                      CAST(f_c4 AS VARCHAR) AS f_c4,
                      CAST(f_c5 AS VARCHAR) AS f_c5,
                      CAST(f_c6 AS VARCHAR) AS f_c6
                      FROM interval_argmax_where_mths_gby"""
