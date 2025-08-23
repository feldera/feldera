from tests.runtime_aggtest.aggtst_base import TstView


class aggtst_int_count_col(TstView):
    def __init__(self):
        # Validated on Postgres
        self.data = [
            {"c1": 2, "c2": 4, "c3": 3, "c4": 4, "c5": 4, "c6": 4, "c7": 2, "c8": 4}
        ]
        self.sql = """CREATE MATERIALIZED VIEW int_count_col AS SELECT
                      COUNT(c1) AS c1, COUNT(c2) AS c2, COUNT(c3) AS c3, COUNT(c4) AS c4, COUNT(c5) AS c5, COUNT(c6) AS c6, COUNT(c7) AS c7, COUNT(c8) AS c8
                      FROM int0_tbl"""


class aggtst_int_count_col_groupby(TstView):
    def __init__(self):
        # Validated on Postgres
        self.data = [
            {
                "id": 0,
                "c1": 1,
                "c2": 2,
                "c3": 1,
                "c4": 2,
                "c5": 2,
                "c6": 2,
                "c7": 1,
                "c8": 2,
            },
            {
                "id": 1,
                "c1": 1,
                "c2": 2,
                "c3": 2,
                "c4": 2,
                "c5": 2,
                "c6": 2,
                "c7": 1,
                "c8": 2,
            },
        ]
        self.sql = """CREATE MATERIALIZED VIEW int_count_col_gby AS SELECT
                      id, COUNT(c1) AS c1, COUNT(c2) AS c2, COUNT(c3) AS c3, COUNT(c4) AS c4, COUNT(c5) AS c5, COUNT(c6) AS c6, COUNT(c7) AS c7, COUNT(c8) AS c8
                      FROM int0_tbl
                      GROUP BY id"""


class aggtst_int_count_col_distinct(TstView):
    def __init__(self):
        # Validated on Postgres
        self.data = [
            {"c1": 2, "c2": 3, "c3": 3, "c4": 3, "c5": 3, "c6": 4, "c7": 2, "c8": 4}
        ]
        self.sql = """CREATE MATERIALIZED VIEW int_count_col_distinct AS SELECT
                      COUNT(DISTINCT c1) AS c1, COUNT(DISTINCT c2) AS c2, COUNT(DISTINCT c3) AS c3, COUNT(DISTINCT c4) AS c4, COUNT(DISTINCT c5) AS c5, COUNT(DISTINCT c6) AS c6, COUNT(DISTINCT c7) AS c7, COUNT(DISTINCT c8) AS c8
                      FROM int0_tbl"""


class aggtst_int_count_col_distinct_groupby(TstView):
    def __init__(self):
        # Validated on Postgres
        self.data = [
            {
                "id": 0,
                "c1": 1,
                "c2": 1,
                "c3": 1,
                "c4": 2,
                "c5": 2,
                "c6": 2,
                "c7": 1,
                "c8": 2,
            },
            {
                "id": 1,
                "c1": 1,
                "c2": 2,
                "c3": 2,
                "c4": 2,
                "c5": 1,
                "c6": 2,
                "c7": 1,
                "c8": 2,
            },
        ]
        self.sql = """CREATE MATERIALIZED VIEW int_count_col_distinct_gby AS SELECT
                      id, COUNT(DISTINCT c1) AS c1, COUNT(DISTINCT c2) AS c2, COUNT(DISTINCT c3) AS c3, COUNT(DISTINCT c4) AS c4, COUNT(DISTINCT c5) AS c5, COUNT(DISTINCT c6) AS c6, COUNT(DISTINCT c7) AS c7, COUNT(DISTINCT c8) AS c8
                      FROM int0_tbl
                      GROUP BY id"""


class aggtst_int_count_col_where(TstView):
    def __init__(self):
        # Validated on Postgres
        self.data = [
            {
                "f_c1": 2,
                "f_c2": 3,
                "f_c3": 2,
                "f_c4": 3,
                "f_c5": 3,
                "f_c6": 3,
                "f_c7": 2,
                "f_c8": 3,
            }
        ]
        self.sql = """CREATE MATERIALIZED VIEW int_count_col_where AS SELECT
                      COUNT(c1) FILTER(WHERE (c5+C6)> 3) AS f_c1, COUNT(c2) FILTER(WHERE (c5+C6)> 3) AS f_c2, COUNT(c3) FILTER(WHERE (c5+C6)> 3) AS f_c3, COUNT(c4) FILTER(WHERE (c5+C6)> 3) AS f_c4, COUNT(c5) FILTER(WHERE (c5+C6)> 3) AS f_c5, COUNT(c6) FILTER(WHERE (c5+C6)> 3) AS f_c6, COUNT(c7) FILTER(WHERE (c5+C6)> 3) AS f_c7, COUNT(c8) FILTER(WHERE (c5+C6)> 3) AS f_c8
                      FROM int0_tbl"""


class aggtst_int_count_col_where_groupby(TstView):
    def __init__(self):
        # Validated on Postgres
        self.data = [
            {
                "id": 0,
                "f_c1": 1,
                "f_c2": 2,
                "f_c3": 1,
                "f_c4": 2,
                "f_c5": 2,
                "f_c6": 2,
                "f_c7": 1,
                "f_c8": 2,
            },
            {
                "id": 1,
                "f_c1": 1,
                "f_c2": 1,
                "f_c3": 1,
                "f_c4": 1,
                "f_c5": 1,
                "f_c6": 1,
                "f_c7": 1,
                "f_c8": 1,
            },
        ]
        self.sql = """CREATE MATERIALIZED VIEW int_count_col_where_gbt AS SELECT
                      id, COUNT(c1) FILTER(WHERE (c5+C6)> 3) AS f_c1, COUNT(c2) FILTER(WHERE (c5+C6)> 3) AS f_c2, COUNT(c3) FILTER(WHERE (c5+C6)> 3) AS f_c3, COUNT(c4) FILTER(WHERE (c5+C6)> 3) AS f_c4, COUNT(c5) FILTER(WHERE (c5+C6)> 3) AS f_c5, COUNT(c6) FILTER(WHERE (c5+C6)> 3) AS f_c6, COUNT(c7) FILTER(WHERE (c5+C6)> 3) AS f_c7, COUNT(c8) FILTER(WHERE (c5+C6)> 3) AS f_c8
                      FROM int0_tbl
                      GROUP BY id"""
