from tests.runtime_aggtest.aggtst_base import TstView


class aggtst_int_max(TstView):
    def __init__(self):
        # Validated on Postgres
        self.data = [
            {"c1": 5, "c2": 5, "c3": 6, "c4": 6, "c5": 5, "c6": 6, "c7": 4, "c8": 8}
        ]
        self.sql = """CREATE MATERIALIZED VIEW int_max_view AS SELECT
                      MAX(c1) AS c1, MAX(c2) AS c2, MAX(c3) AS c3, MAX(c4) AS c4, MAX(c5) AS c5, MAX(c6) AS c6, MAX(c7) AS c7, MAX(c8) AS c8
                      FROM int0_tbl"""


class aggtst_int_max_groupby(TstView):
    def __init__(self):
        # Validated on Postgres
        self.data = [
            {
                "id": 0,
                "c1": 5,
                "c2": 2,
                "c3": 3,
                "c4": 4,
                "c5": 5,
                "c6": 6,
                "c7": 3,
                "c8": 8,
            },
            {
                "id": 1,
                "c1": 4,
                "c2": 5,
                "c3": 6,
                "c4": 6,
                "c5": 2,
                "c6": 3,
                "c7": 4,
                "c8": 5,
            },
        ]
        self.sql = """CREATE MATERIALIZED VIEW int_max_gby AS SELECT
                      id, MAX(c1) AS c1, MAX(c2) AS c2, MAX(c3) AS c3, MAX(c4) AS c4, MAX(c5) AS c5, MAX(c6) AS c6, MAX(c7) AS c7, MAX(c8) AS c8
                      FROM int0_tbl
                      GROUP BY id"""


class aggtst_int_max_distinct(TstView):
    def __init__(self):
        # Validated on Postgres
        self.data = [
            {"c1": 5, "c2": 5, "c3": 6, "c4": 6, "c5": 5, "c6": 6, "c7": 4, "c8": 8}
        ]
        self.sql = """CREATE MATERIALIZED VIEW int_max_distinct AS SELECT
                      MAX(DISTINCT c1) AS c1, MAX(DISTINCT c2) AS c2, MAX(DISTINCT c3) AS c3, MAX(DISTINCT c4) AS c4, MAX(DISTINCT c5) AS c5, MAX(DISTINCT c6) AS c6, MAX(DISTINCT c7) AS c7, MAX(DISTINCT c8) AS c8
                      FROM int0_tbl"""


class aggtst_int_max_distinct_groupby(TstView):
    def __init__(self):
        # Validated on Postgres
        self.data = [
            {
                "id": 0,
                "c1": 5,
                "c2": 2,
                "c3": 3,
                "c4": 4,
                "c5": 5,
                "c6": 6,
                "c7": 3,
                "c8": 8,
            },
            {
                "id": 1,
                "c1": 4,
                "c2": 5,
                "c3": 6,
                "c4": 6,
                "c5": 2,
                "c6": 3,
                "c7": 4,
                "c8": 5,
            },
        ]
        self.sql = """CREATE MATERIALIZED VIEW int_max_distinct_gbt AS SELECT
                      id, MAX(DISTINCT c1) AS c1, MAX(DISTINCT c2) AS c2, MAX(DISTINCT c3) AS c3, MAX(DISTINCT c4) AS c4, MAX(DISTINCT c5) AS c5, MAX(DISTINCT c6) AS c6, MAX(DISTINCT c7) AS c7, MAX(DISTINCT c8) AS c8
                      FROM int0_tbl
                      GROUP BY id"""


class aggtst_int_max_where(TstView):
    def __init__(self):
        # Validated on Postgres
        self.data = [
            {
                "f_c1": 5,
                "f_c2": 3,
                "f_c3": 4,
                "f_c4": 6,
                "f_c5": 5,
                "f_c6": 6,
                "f_c7": 4,
                "f_c8": 8,
            }
        ]
        self.sql = """CREATE MATERIALIZED VIEW int_max_where AS SELECT
                      MAX(c1) FILTER(WHERE c5 + C6>3) AS f_c1, MAX(c2) FILTER(WHERE c5 + C6>3) AS f_c2, MAX(c3) FILTER(WHERE c5 + C6>3) AS f_c3, MAX(c4) FILTER(WHERE c5 + C6>3) AS f_c4, MAX(c5) FILTER(WHERE c5 + C6>3) AS f_c5, MAX(c6) FILTER(WHERE c5 + C6>3) AS f_c6,  MAX(c7) FILTER(WHERE c5 + C6>3) AS f_c7,  MAX(c8) FILTER(WHERE c5 + C6>3) AS f_c8
                      FROM int0_tbl"""


class aggtst_int_max_Where_Groupby(TstView):
    def __init__(self):
        # Validated on Postgres
        self.data = [
            {
                "id": 0,
                "f_c1": 5,
                "f_c2": 2,
                "f_c3": 3,
                "f_c4": 4,
                "f_c5": 5,
                "f_c6": 6,
                "f_c7": 3,
                "f_c8": 8,
            },
            {
                "id": 1,
                "f_c1": 4,
                "f_c2": 3,
                "f_c3": 4,
                "f_c4": 6,
                "f_c5": 2,
                "f_c6": 3,
                "f_c7": 4,
                "f_c8": 2,
            },
        ]
        self.sql = """CREATE MATERIALIZED VIEW int_max_where_gby AS SELECT
                      id, MAX(c1) FILTER(WHERE c5 + C6>3) AS f_c1, MAX(c2) FILTER(WHERE c5 + C6>3) AS f_c2, MAX(c3) FILTER(WHERE c5 + C6>3) AS f_c3, MAX(c4) FILTER(WHERE c5 + C6>3) AS f_c4, MAX(c5) FILTER(WHERE c5 + C6>3) AS f_c5, MAX(c6) FILTER(WHERE c5 + C6>3) AS f_c6,  MAX(c7) FILTER(WHERE c5 + C6>3) AS f_c7,  MAX(c8) FILTER(WHERE c5 + C6>3) AS f_c8
                      FROM int0_tbl
                      GROUP BY id"""
