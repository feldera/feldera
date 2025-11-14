from tests.runtime_aggtest.aggtst_base import TstView


class aggtst_interval_mths_max(TstView):
    def __init__(self):
        # Result validation is not required for local views
        self.data = []
        self.sql = """CREATE LOCAL VIEW interval_mths_max AS SELECT
                      MAX(c1_minus_c2) AS f_c1,
                      MAX(c2_minus_c1) AS f_c2,
                      MAX(c1_minus_c3) AS f_c3,
                      MAX(c3_minus_c1) AS f_c4,
                      MAX(c2_minus_c3) AS f_c5,
                      MAX(c3_minus_c2) AS f_c6
                      FROM atbl_interval_months"""


class aggtst_interval_mths_max_res(TstView):
    def __init__(self):
        # Validated on Postgres
        self.data = [
            {
                "f_c1": "+605",
                "f_c2": "+557",
                "f_c3": "+87",
                "f_c4": "+217",
                "f_c5": "+340",
                "f_c6": "+639",
            }
        ]
        self.sql = """CREATE MATERIALIZED VIEW interval_mths_max_res AS SELECT
                      CAST(f_c1 AS VARCHAR) AS f_c1,
                      CAST(f_c2 AS VARCHAR) AS f_c2,
                      CAST(f_c3 AS VARCHAR) AS f_c3,
                      CAST(f_c4 AS VARCHAR) AS f_c4,
                      CAST(f_c5 AS VARCHAR) AS f_c5,
                      CAST(f_c6 AS VARCHAR) AS f_c6
                      FROM interval_mths_max"""


class aggtst_interval_mths_max_gby(TstView):
    def __init__(self):
        # Result validation is not required for local views
        self.data = []
        self.sql = """CREATE LOCAL VIEW interval_mths_max_gby AS SELECT
                      id,
                      MAX(c1_minus_c2) AS f_c1,
                      MAX(c2_minus_c1) AS f_c2,
                      MAX(c1_minus_c3) AS f_c3,
                      MAX(c3_minus_c1) AS f_c4,
                      MAX(c2_minus_c3) AS f_c5,
                      MAX(c3_minus_c2) AS f_c6
                      FROM atbl_interval_months
                      GROUP BY id"""


class aggtst_interval_mths_max_res_gby(TstView):
    def __init__(self):
        # Validated on Postgres
        self.data = [
            {
                "id": 0,
                "f_c1": "+605",
                "f_c2": "+121",
                "f_c3": "-33",
                "f_c4": "+64",
                "f_c5": "+56",
                "f_c6": "+639",
            },
            {
                "id": 1,
                "f_c1": "+605",
                "f_c2": "+557",
                "f_c3": "+87",
                "f_c4": "+217",
                "f_c5": "+340",
                "f_c6": "+622",
            },
        ]
        self.sql = """CREATE MATERIALIZED VIEW interval_mths_max_res_gby AS SELECT
                      id,
                      CAST(f_c1 AS VARCHAR) AS f_c1,
                      CAST(f_c2 AS VARCHAR) AS f_c2,
                      CAST(f_c3 AS VARCHAR) AS f_c3,
                      CAST(f_c4 AS VARCHAR) AS f_c4,
                      CAST(f_c5 AS VARCHAR) AS f_c5,
                      CAST(f_c6 AS VARCHAR) AS f_c6
                      FROM interval_mths_max_gby"""


class aggtst_interval_max_distinct_mths(TstView):
    def __init__(self):
        # Result validation is not required for local views
        self.data = []
        self.sql = """CREATE LOCAL VIEW interval_max_distinct_mths AS SELECT
                      MAX(DISTINCT c1_minus_c2) AS f_c1,
                      MAX(DISTINCT c2_minus_c1) AS f_c2,
                      MAX(DISTINCT c1_minus_c3) AS f_c3,
                      MAX(DISTINCT c3_minus_c1) AS f_c4,
                      MAX(DISTINCT c2_minus_c3) AS f_c5,
                      MAX(DISTINCT c3_minus_c2) AS f_c6
                      FROM atbl_interval_months"""


class aggtst_interval_max_distinct_mths_res(TstView):
    def __init__(self):
        # Validated on Postgres
        self.data = [
            {
                "f_c1": "+605",
                "f_c2": "+557",
                "f_c3": "+87",
                "f_c4": "+217",
                "f_c5": "+340",
                "f_c6": "+639",
            }
        ]
        self.sql = """CREATE MATERIALIZED VIEW interval_max_distinct_mths_res AS SELECT
                      CAST(f_c1 AS VARCHAR) AS f_c1,
                      CAST(f_c2 AS VARCHAR) AS f_c2,
                      CAST(f_c3 AS VARCHAR) AS f_c3,
                      CAST(f_c4 AS VARCHAR) AS f_c4,
                      CAST(f_c5 AS VARCHAR) AS f_c5,
                      CAST(f_c6 AS VARCHAR) AS f_c6
                      FROM interval_max_distinct_mths"""


class aggtst_interval_max_distinct_mths_gby(TstView):
    def __init__(self):
        # Result validation is not required for local views
        self.data = []
        self.sql = """CREATE LOCAL VIEW interval_max_distinct_mths_gby AS SELECT
                      id,
                      MAX(DISTINCT c1_minus_c2) AS f_c1,
                      MAX(DISTINCT c2_minus_c1) AS f_c2,
                      MAX(DISTINCT c1_minus_c3) AS f_c3,
                      MAX(DISTINCT c3_minus_c1) AS f_c4,
                      MAX(DISTINCT c2_minus_c3) AS f_c5,
                      MAX(DISTINCT c3_minus_c2) AS f_c6
                      FROM atbl_interval_months
                      GROUP BY id"""


class aggtst_interval_max_distinct_mths_gby_res(TstView):
    def __init__(self):
        # Validated on Postgres
        self.data = [
            {
                "id": 0,
                "f_c1": "+605",
                "f_c2": "+121",
                "f_c3": "-33",
                "f_c4": "+64",
                "f_c5": "+56",
                "f_c6": "+639",
            },
            {
                "id": 1,
                "f_c1": "+605",
                "f_c2": "+557",
                "f_c3": "+87",
                "f_c4": "+217",
                "f_c5": "+340",
                "f_c6": "+622",
            },
        ]
        self.sql = """CREATE MATERIALIZED VIEW interval_max_distinct_mths_gby_res AS SELECT
                      id,
                      CAST(f_c1 AS VARCHAR) AS f_c1,
                      CAST(f_c2 AS VARCHAR) AS f_c2,
                      CAST(f_c3 AS VARCHAR) AS f_c3,
                      CAST(f_c4 AS VARCHAR) AS f_c4,
                      CAST(f_c5 AS VARCHAR) AS f_c5,
                      CAST(f_c6 AS VARCHAR) AS f_c6
                      FROM interval_max_distinct_mths_gby"""


class aggtst_interval_max_where_mths(TstView):
    def __init__(self):
        # Result validation is not required for local views
        self.data = []
        self.sql = """CREATE LOCAL VIEW interval_max_where_mths AS SELECT
                      MAX(c1_minus_c2) FILTER(WHERE c1_minus_c2 > c2_minus_c1) AS f_c1,
                      MAX(c2_minus_c1) FILTER(WHERE c1_minus_c2 > c2_minus_c1) AS f_c2,
                      MAX(c1_minus_c3) FILTER(WHERE c1_minus_c3 > c3_minus_c1) AS f_c3,
                      MAX(c3_minus_c1) FILTER(WHERE c1_minus_c3 > c3_minus_c1) AS f_c4,
                      MAX(c2_minus_c3) FILTER(WHERE c2_minus_c3 > c3_minus_c2) AS f_c5,
                      MAX(c3_minus_c2) FILTER(WHERE c2_minus_c3 > c3_minus_c2) AS f_c6
                      FROM atbl_interval_months"""


class aggtst_interval_max_where_mths_res(TstView):
    def __init__(self):
        # Validated on Postgres
        self.data = [
            {
                "f_c1": "+605",
                "f_c2": "-120",
                "f_c3": "+87",
                "f_c4": "-87",
                "f_c5": "+340",
                "f_c6": "-56",
            }
        ]
        self.sql = """CREATE MATERIALIZED VIEW interval_max_where_mths_res AS SELECT
                      CAST(f_c1 AS VARCHAR) AS f_c1,
                      CAST(f_c2 AS VARCHAR) AS f_c2,
                      CAST(f_c3 AS VARCHAR) AS f_c3,
                      CAST(f_c4 AS VARCHAR) AS f_c4,
                      CAST(f_c5 AS VARCHAR) AS f_c5,
                      CAST(f_c6 AS VARCHAR) AS f_c6
                      FROM interval_max_where_mths"""


class aggtst_interval_max_where_mths_gby(TstView):
    def __init__(self):
        # Result validation is not required for local views
        self.data = []
        self.sql = """CREATE LOCAL VIEW interval_max_where_mths_gby AS SELECT
                      id,
                      MAX(c1_minus_c2) FILTER(WHERE c1_minus_c2 > c2_minus_c1) AS f_c1,
                      MAX(c2_minus_c1) FILTER(WHERE c1_minus_c2 > c2_minus_c1) AS f_c2,
                      MAX(c1_minus_c3) FILTER(WHERE c1_minus_c3 > c3_minus_c1) AS f_c3,
                      MAX(c3_minus_c1) FILTER(WHERE c1_minus_c3 > c3_minus_c1) AS f_c4,
                      MAX(c2_minus_c3) FILTER(WHERE c2_minus_c3 > c3_minus_c2) AS f_c5,
                      MAX(c3_minus_c2) FILTER(WHERE c2_minus_c3 > c3_minus_c2) AS f_c6
                      FROM atbl_interval_months
                      GROUP BY id"""


class aggtst_interval_max_where_mths_res_gby(TstView):
    def __init__(self):
        # Validated on Postgres
        self.data = [
            {
                "id": 0,
                "f_c1": "+605",
                "f_c2": "-605",
                "f_c3": None,
                "f_c4": None,
                "f_c5": "+56",
                "f_c6": "-56",
            },
            {
                "id": 1,
                "f_c1": "+605",
                "f_c2": "-120",
                "f_c3": "+87",
                "f_c4": "-87",
                "f_c5": "+340",
                "f_c6": "-340",
            },
        ]
        self.sql = """CREATE MATERIALIZED VIEW interval_max_where_mths_res_gby AS SELECT
                      id,
                      CAST(f_c1 AS VARCHAR) AS f_c1,
                      CAST(f_c2 AS VARCHAR) AS f_c2,
                      CAST(f_c3 AS VARCHAR) AS f_c3,
                      CAST(f_c4 AS VARCHAR) AS f_c4,
                      CAST(f_c5 AS VARCHAR) AS f_c5,
                      CAST(f_c6 AS VARCHAR) AS f_c6
                      FROM interval_max_where_mths_gby"""
