from tests.runtime_aggtest.aggtst_base import TstView


class aggtst_int_avg(TstView):
    def __init__(self):
        # Validated on Postgres
        self.sql = """CREATE MATERIALIZED VIEW int_avg_value AS SELECT
                      AVG(c1) AS c1,AVG(c2) AS c2,AVG(c3) AS c3,AVG(c4) AS c4,AVG(c5) AS c5,AVG(c6) AS c6,AVG(c7) AS c7,AVG(c8) AS c8
                      FROM int0_tbl"""
        self.data = [
            {"c1": 4, "c2": 3, "c3": 4, "c4": 3, "c5": 3, "c6": 3, "c7": 3, "c8": 4}
        ]


class aggtst_int_avg_gby(TstView):
    def __init__(self):
        # Validated on Postgres
        self.sql = """CREATE MATERIALIZED VIEW int_avg_gby AS SELECT
                      id, AVG(c1) AS c1,AVG(c2) AS c2,AVG(c3) AS c3,AVG(c4) AS c4,AVG(c5) AS c5,AVG(c6) AS c6,AVG(c7) AS c7,AVG(c8) AS c8
                      FROM int0_tbl
                      GROUP BY id"""
        self.data = [
            {
                "id": 0,
                "c1": 5,
                "c2": 2,
                "c3": 3,
                "c4": 3,
                "c5": 4,
                "c6": 5,
                "c7": 3,
                "c8": 5,
            },
            {
                "id": 1,
                "c1": 4,
                "c2": 4,
                "c3": 5,
                "c4": 4,
                "c5": 2,
                "c6": 2,
                "c7": 4,
                "c8": 3,
            },
        ]


class aggtst_int_avg_distinct(TstView):
    def __init__(self):
        # Validated on Postgres
        self.sql = """CREATE MATERIALIZED VIEW int_avg_distinct AS SELECT
                      AVG(DISTINCT c1) AS c1, AVG(DISTINCT c2) AS c2, AVG(DISTINCT c3) AS c3, AVG(DISTINCT c4) AS c4, AVG(DISTINCT c5) AS c5, AVG(DISTINCT c6) AS c6, AVG(DISTINCT c7) AS c7, AVG(DISTINCT c8) AS c8
                      FROM int0_tbl"""
        self.data = [
            {"c1": 4, "c2": 3, "c3": 4, "c4": 4, "c5": 3, "c6": 3, "c7": 3, "c8": 4}
        ]


class aggtst_int_avg_distinct_gby(TstView):
    def __init__(self):
        # Validated on Postgres
        self.sql = """CREATE MATERIALIZED VIEW int_avg_distinct_gby AS SELECT
                      id, AVG(DISTINCT c1) AS c1, AVG(DISTINCT c2) AS c2, AVG(DISTINCT c3) AS c3, AVG(DISTINCT c4) AS c4, AVG(DISTINCT c5) AS c5, AVG(DISTINCT c6) AS c6, AVG(DISTINCT c7) AS c7, AVG(DISTINCT c8) AS c8
                      FROM int0_tbl
                      GROUP BY id"""
        self.data = [
            {
                "id": 0,
                "c1": 5,
                "c2": 2,
                "c3": 3,
                "c4": 3,
                "c5": 4,
                "c6": 5,
                "c7": 3,
                "c8": 5,
            },
            {
                "id": 1,
                "c1": 4,
                "c2": 4,
                "c3": 5,
                "c4": 4,
                "c5": 2,
                "c6": 2,
                "c7": 4,
                "c8": 3,
            },
        ]


class aggtst_int_avg_where(TstView):
    def __init__(self):
        # Validated on Postgres
        self.sql = """CREATE MATERIALIZED VIEW int_avg_where AS SELECT
                      AVG(c1) FILTER(WHERE c8>2) AS f_c1, AVG(c2) FILTER(WHERE c8>2) AS f_c2, AVG(c3) FILTER(WHERE c8>2) AS f_c3, AVG(c4) FILTER(WHERE c8>2) AS f_c4, AVG(c5) FILTER(WHERE c8>2) AS f_c5, AVG(c6) FILTER(WHERE c8>2) AS f_c6, AVG(c7) FILTER(WHERE c8>2) AS f_c7, AVG(c8) FILTER(WHERE c8>2) AS f_c8
                      FROM int0_tbl"""
        self.data = [
            {
                "f_c1": 5,
                "f_c2": 3,
                "f_c3": 4,
                "f_c4": 2,
                "f_c5": 3,
                "f_c6": 3,
                "f_c7": 3,
                "f_c8": 5,
            }
        ]


class aggtst_int_avg_where_gby(TstView):
    def __init__(self):
        # Validated on Postgres
        self.sql = """CREATE MATERIALIZED VIEW int_avg_where_gby AS SELECT
                      id, AVG(c1) FILTER(WHERE c8>2) AS f_c1, AVG(c2) FILTER(WHERE c8>2) AS f_c2, AVG(c3) FILTER(WHERE c8>2) AS f_c3, AVG(c4) FILTER(WHERE c8>2) AS f_c4, AVG(c5) FILTER(WHERE c8>2) AS f_c5, AVG(c6) FILTER(WHERE c8>2) AS f_c6, AVG(c7) FILTER(WHERE c8>2) AS f_c7, AVG(c8) FILTER(WHERE c8>2) AS f_c8
                      FROM int0_tbl
                      GROUP BY id"""
        self.data = [
            {
                "id": 0,
                "f_c1": 5,
                "f_c2": 2,
                "f_c3": 3,
                "f_c4": 3,
                "f_c5": 4,
                "f_c6": 5,
                "f_c7": 3,
                "f_c8": 5,
            },
            {
                "id": 1,
                "f_c1": None,
                "f_c2": 5,
                "f_c3": 6,
                "f_c4": 2,
                "f_c5": 2,
                "f_c6": 1,
                "f_c7": None,
                "f_c8": 5,
            },
        ]
