from tests.runtime_aggtest.aggtst_base import TstView


class aggtst_interval_count_col(TstView):
    def __init__(self):
        # Validated on Postgres
        self.data = [{"f_c1": 5, "f_c2": 5, "f_c3": 5, "f_c4": 5, "f_c5": 5, "f_c6": 5}]
        self.sql = """CREATE MATERIALIZED VIEW interval_count_col AS SELECT
                      COUNT(c1_minus_c2) AS f_c1,
                      COUNT(c2_minus_c1) AS f_c2,
                      COUNT(c1_minus_c3) AS f_c3,
                      COUNT(c3_minus_c1) AS f_c4,
                      COUNT(c2_minus_c3) AS f_c5,
                      COUNT(c3_minus_c2) AS f_c6
                      FROM atbl_interval_seconds"""


class aggtst_interval_count_col_gby(TstView):
    def __init__(self):
        # Validated on Postgres
        self.data = [
            {"id": 0, "f_c1": 2, "f_c2": 2, "f_c3": 2, "f_c4": 2, "f_c5": 2, "f_c6": 2},
            {"id": 1, "f_c1": 3, "f_c2": 3, "f_c3": 3, "f_c4": 3, "f_c5": 3, "f_c6": 3},
        ]
        self.sql = """CREATE MATERIALIZED VIEW interval_count_col_gby AS SELECT
                      id,
                      COUNT(c1_minus_c2) AS f_c1,
                      COUNT(c2_minus_c1) AS f_c2,
                      COUNT(c1_minus_c3) AS f_c3,
                      COUNT(c3_minus_c1) AS f_c4,
                      COUNT(c2_minus_c3) AS f_c5,
                      COUNT(c3_minus_c2) AS f_c6
                      FROM atbl_interval_seconds
                      GROUP BY id"""


class aggtst_interval_count_col_distinct(TstView):
    def __init__(self):
        # Validated on Postgres
        self.data = [{"f_c1": 4, "f_c2": 4, "f_c3": 5, "f_c4": 5, "f_c5": 5, "f_c6": 5}]
        self.sql = """CREATE MATERIALIZED VIEW interval_count_col_distinct AS SELECT
                      COUNT(DISTINCT c1_minus_c2) AS f_c1,
                      COUNT(DISTINCT c2_minus_c1) AS f_c2,
                      COUNT(DISTINCT c1_minus_c3) AS f_c3,
                      COUNT(DISTINCT c3_minus_c1) AS f_c4,
                      COUNT(DISTINCT c2_minus_c3) AS f_c5,
                      COUNT(DISTINCT c3_minus_c2) AS f_c6
                      FROM atbl_interval_seconds"""


class aggtst_interval_count_col_distinct_gby(TstView):
    def __init__(self):
        # Validated on Postgres
        self.data = [
            {"id": 0, "f_c1": 2, "f_c2": 2, "f_c3": 2, "f_c4": 2, "f_c5": 2, "f_c6": 2},
            {"id": 1, "f_c1": 3, "f_c2": 3, "f_c3": 3, "f_c4": 3, "f_c5": 3, "f_c6": 3},
        ]
        self.sql = """CREATE MATERIALIZED VIEW interval_count_col_distinct_gby AS SELECT
                      id,
                      COUNT(DISTINCT c1_minus_c2) AS f_c1,
                      COUNT(DISTINCT c2_minus_c1) AS f_c2,
                      COUNT(DISTINCT c1_minus_c3) AS f_c3,
                      COUNT(DISTINCT c3_minus_c1) AS f_c4,
                      COUNT(DISTINCT c2_minus_c3) AS f_c5,
                      COUNT(DISTINCT c3_minus_c2) AS f_c6
                      FROM atbl_interval_seconds
                      GROUP BY id"""


class aggtst_interval_count_col_where(TstView):
    def __init__(self):
        # Validated on Postgres
        self.data = [{"f_c1": 3, "f_c2": 3, "f_c3": 1, "f_c4": 1, "f_c5": 2, "f_c6": 2}]
        self.sql = """CREATE MATERIALIZED VIEW interval_count_col_where AS SELECT
                      COUNT(c1_minus_c2) FILTER(WHERE c1_minus_c2 > c2_minus_c1) AS f_c1,
                      COUNT(c2_minus_c1) FILTER(WHERE c1_minus_c2 > c2_minus_c1) AS f_c2,
                      COUNT(c1_minus_c3) FILTER(WHERE c1_minus_c3 > c3_minus_c1) AS f_c3,
                      COUNT(c3_minus_c1) FILTER(WHERE c1_minus_c3 > c3_minus_c1) AS f_c4,
                      COUNT(c2_minus_c3) FILTER(WHERE c2_minus_c3 > c3_minus_c2) AS f_c5,
                      COUNT(c3_minus_c2) FILTER(WHERE c2_minus_c3 > c3_minus_c2) AS f_c6
                      FROM atbl_interval_seconds"""


class aggtst_interval_count_col_where_groupby(TstView):
    def __init__(self):
        # Validated on Postgres
        self.data = [
            {"id": 0, "f_c1": 1, "f_c2": 1, "f_c3": 0, "f_c4": 0, "f_c5": 1, "f_c6": 1},
            {"id": 1, "f_c1": 2, "f_c2": 2, "f_c3": 1, "f_c4": 1, "f_c5": 1, "f_c6": 1},
        ]
        self.sql = """CREATE MATERIALIZED VIEW interval_count_col_where_gby AS SELECT
                      id,
                      COUNT(c1_minus_c2) FILTER(WHERE c1_minus_c2 > c2_minus_c1) AS f_c1,
                      COUNT(c2_minus_c1) FILTER(WHERE c1_minus_c2 > c2_minus_c1) AS f_c2,
                      COUNT(c1_minus_c3) FILTER(WHERE c1_minus_c3 > c3_minus_c1) AS f_c3,
                      COUNT(c3_minus_c1) FILTER(WHERE c1_minus_c3 > c3_minus_c1) AS f_c4,
                      COUNT(c2_minus_c3) FILTER(WHERE c2_minus_c3 > c3_minus_c2) AS f_c5,
                      COUNT(c3_minus_c2) FILTER(WHERE c2_minus_c3 > c3_minus_c2) AS f_c6
                      FROM atbl_interval_seconds
                      GROUP BY id"""
