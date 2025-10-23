from tests.runtime_aggtest.aggtst_base import TstView


class aggtst_interval_min_mths(TstView):
    def __init__(self):
        # Result validation is not required for local views
        self.data = []
        self.sql = """CREATE LOCAL VIEW interval_min_mths AS SELECT
                      MIN(c1_minus_c2) AS f_c1,
                      MIN(c2_minus_c1) AS f_c2,
                      MIN(c1_minus_c3) AS f_c3,
                      MIN(c3_minus_c1) AS f_c4,
                      MIN(c2_minus_c3) AS f_c5,
                      MIN(c3_minus_c2) AS f_c6
                      FROM atbl_interval_months"""


class aggtst_interval_min_mths_res(TstView):
    def __init__(self):
        # Validated on Postgres
        self.data = [
            {
                "f_c1": "-557",
                "f_c2": "-605",
                "f_c3": "-217",
                "f_c4": "-87",
                "f_c5": "-639",
                "f_c6": "-340",
            }
        ]
        self.sql = """CREATE MATERIALIZED VIEW interval_min_mths_res AS SELECT
                      CAST(f_c1 AS VARCHAR) AS f_c1,
                      CAST(f_c2 AS VARCHAR) AS f_c2,
                      CAST(f_c3 AS VARCHAR) AS f_c3,
                      CAST(f_c4 AS VARCHAR) AS f_c4,
                      CAST(f_c5 AS VARCHAR) AS f_c5,
                      CAST(f_c6 AS VARCHAR) AS f_c6
                      FROM interval_min_mths"""


class aggtst_interval_min_gby_mths(TstView):
    def __init__(self):
        # Result validation is not required for local views
        self.data = []
        self.sql = """CREATE LOCAL VIEW interval_min_gby_mths AS SELECT
                      id,
                      MIN(c1_minus_c2) AS f_c1,
                      MIN(c2_minus_c1) AS f_c2,
                      MIN(c1_minus_c3) AS f_c3,
                      MIN(c3_minus_c1) AS f_c4,
                      MIN(c2_minus_c3) AS f_c5,
                      MIN(c3_minus_c2) AS f_c6
                      FROM atbl_interval_months
                      GROUP BY id"""


class aggtst_interval_min_gby_mths_res(TstView):
    def __init__(self):
        # Validated on Postgres
        self.data = [
            {
                "id": 0,
                "f_c1": "-121",
                "f_c2": "-605",
                "f_c3": "-64",
                "f_c4": "+33",
                "f_c5": "-639",
                "f_c6": "-56",
            },
            {
                "id": 1,
                "f_c1": "-557",
                "f_c2": "-605",
                "f_c3": "-217",
                "f_c4": "-87",
                "f_c5": "-622",
                "f_c6": "-340",
            },
        ]
        self.sql = """CREATE MATERIALIZED VIEW interval_min_gby_mths_res AS SELECT
                      id,
                      CAST(f_c1 AS VARCHAR) AS f_c1,
                      CAST(f_c2 AS VARCHAR) AS f_c2,
                      CAST(f_c3 AS VARCHAR) AS f_c3,
                      CAST(f_c4 AS VARCHAR) AS f_c4,
                      CAST(f_c5 AS VARCHAR) AS f_c5,
                      CAST(f_c6 AS VARCHAR) AS f_c6
                      FROM interval_min_gby_mths"""


class aggtst_interval_min_distinct_mths(TstView):
    def __init__(self):
        # Result validation is not required for local views
        self.data = []
        self.sql = """CREATE LOCAL VIEW interval_min_distinct_mths AS SELECT
                      MIN(DISTINCT c1_minus_c2) AS f_c1,
                      MIN(DISTINCT c2_minus_c1) AS f_c2,
                      MIN(DISTINCT c1_minus_c3) AS f_c3,
                      MIN(DISTINCT c3_minus_c1) AS f_c4,
                      MIN(DISTINCT c2_minus_c3) AS f_c5,
                      MIN(DISTINCT c3_minus_c2) AS f_c6
                      FROM atbl_interval_months"""


class aggtst_interval_min_distinct_mths_res(TstView):
    def __init__(self):
        # Validated on Postgres
        self.data = [
            {
                "f_c1": "-557",
                "f_c2": "-605",
                "f_c3": "-217",
                "f_c4": "-87",
                "f_c5": "-639",
                "f_c6": "-340",
            }
        ]
        self.sql = """CREATE MATERIALIZED VIEW interval_min_distinct_mths_res AS SELECT
                      CAST(f_c1 AS VARCHAR) AS f_c1,
                      CAST(f_c2 AS VARCHAR) AS f_c2,
                      CAST(f_c3 AS VARCHAR) AS f_c3,
                      CAST(f_c4 AS VARCHAR) AS f_c4,
                      CAST(f_c5 AS VARCHAR) AS f_c5,
                      CAST(f_c6 AS VARCHAR) AS f_c6
                      FROM interval_min_distinct_mths"""


class aggtst_interval_min_distinct_gby_mths(TstView):
    def __init__(self):
        # Result validation is not required for local views
        self.data = []
        self.sql = """CREATE LOCAL VIEW interval_min_distinct_gby_mths AS SELECT
                      id,
                      MIN(DISTINCT c1_minus_c2) AS f_c1,
                      MIN(DISTINCT c2_minus_c1) AS f_c2,
                      MIN(DISTINCT c1_minus_c3) AS f_c3,
                      MIN(DISTINCT c3_minus_c1) AS f_c4,
                      MIN(DISTINCT c2_minus_c3) AS f_c5,
                      MIN(DISTINCT c3_minus_c2) AS f_c6
                      FROM atbl_interval_months
                      GROUP BY id"""


class aggtst_interval_min_distinct_gby_mths_res(TstView):
    def __init__(self):
        # Validated on Postgres
        self.data = [
            {
                "id": 0,
                "f_c1": "-121",
                "f_c2": "-605",
                "f_c3": "-64",
                "f_c4": "+33",
                "f_c5": "-639",
                "f_c6": "-56",
            },
            {
                "id": 1,
                "f_c1": "-557",
                "f_c2": "-605",
                "f_c3": "-217",
                "f_c4": "-87",
                "f_c5": "-622",
                "f_c6": "-340",
            },
        ]
        self.sql = """CREATE MATERIALIZED VIEW interval_min_distinct_gby_mths_res AS SELECT
                      id,
                      CAST(f_c1 AS VARCHAR) AS f_c1,
                      CAST(f_c2 AS VARCHAR) AS f_c2,
                      CAST(f_c3 AS VARCHAR) AS f_c3,
                      CAST(f_c4 AS VARCHAR) AS f_c4,
                      CAST(f_c5 AS VARCHAR) AS f_c5,
                      CAST(f_c6 AS VARCHAR) AS f_c6
                      FROM interval_min_distinct_gby_mths"""


class aggtst_interval_min_where_mths(TstView):
    def __init__(self):
        # Result validation is not required for local views
        self.data = []
        self.sql = """CREATE LOCAL VIEW interval_min_where_mths AS SELECT
                      MIN(c1_minus_c2) FILTER(WHERE c1_minus_c2 > c2_minus_c1) AS f_c1,
                      MIN(c2_minus_c1) FILTER(WHERE c1_minus_c2 > c2_minus_c1) AS f_c2,
                      MIN(c1_minus_c3) FILTER(WHERE c1_minus_c3 > c3_minus_c1) AS f_c3,
                      MIN(c3_minus_c1) FILTER(WHERE c1_minus_c3 > c3_minus_c1) AS f_c4,
                      MIN(c2_minus_c3) FILTER(WHERE c2_minus_c3 > c3_minus_c2) AS f_c5,
                      MIN(c3_minus_c2) FILTER(WHERE c2_minus_c3 > c3_minus_c2) AS f_c6
                      FROM atbl_interval_months"""


class aggtst_interval_min_where_mths_res(TstView):
    def __init__(self):
        # Validated on Postgres
        self.data = [
            {
                "f_c1": "+120",
                "f_c2": "-605",
                "f_c3": "+87",
                "f_c4": "-87",
                "f_c5": "+56",
                "f_c6": "-340",
            }
        ]
        self.sql = """CREATE MATERIALIZED VIEW interval_min_where_mths_res AS SELECT
                      CAST(f_c1 AS VARCHAR) AS f_c1,
                      CAST(f_c2 AS VARCHAR) AS f_c2,
                      CAST(f_c3 AS VARCHAR) AS f_c3,
                      CAST(f_c4 AS VARCHAR) AS f_c4,
                      CAST(f_c5 AS VARCHAR) AS f_c5,
                      CAST(f_c6 AS VARCHAR) AS f_c6
                      FROM interval_min_where_mths"""


class aggtst_interval_min_where_gby_mths(TstView):
    def __init__(self):
        # Result validation is not required for local views
        self.data = []
        self.sql = """CREATE LOCAL VIEW interval_min_where_gby_mths AS SELECT
                      id,
                      MIN(c1_minus_c2) FILTER(WHERE c1_minus_c2 > c2_minus_c1) AS f_c1,
                      MIN(c2_minus_c1) FILTER(WHERE c1_minus_c2 > c2_minus_c1) AS f_c2,
                      MIN(c1_minus_c3) FILTER(WHERE c1_minus_c3 > c3_minus_c1) AS f_c3,
                      MIN(c3_minus_c1) FILTER(WHERE c1_minus_c3 > c3_minus_c1) AS f_c4,
                      MIN(c2_minus_c3) FILTER(WHERE c2_minus_c3 > c3_minus_c2) AS f_c5,
                      MIN(c3_minus_c2) FILTER(WHERE c2_minus_c3 > c3_minus_c2) AS f_c6
                      FROM atbl_interval_months
                      GROUP BY id"""


class aggtst_interval_min_where_gby_mths_res(TstView):
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
                "f_c1": "+120",
                "f_c2": "-605",
                "f_c3": "+87",
                "f_c4": "-87",
                "f_c5": "+340",
                "f_c6": "-340",
            },
        ]
        self.sql = """CREATE MATERIALIZED VIEW interval_min_where_gby_mths_res AS SELECT
                      id,
                      CAST(f_c1 AS VARCHAR) AS f_c1,
                      CAST(f_c2 AS VARCHAR) AS f_c2,
                      CAST(f_c3 AS VARCHAR) AS f_c3,
                      CAST(f_c4 AS VARCHAR) AS f_c4,
                      CAST(f_c5 AS VARCHAR) AS f_c5,
                      CAST(f_c6 AS VARCHAR) AS f_c6
                      FROM interval_min_where_gby_mths"""
