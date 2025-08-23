from tests.runtime_aggtest.aggtst_base import TstView


class aggtst_int_min(TstView):
    def __init__(self):
        # Validated on Postgres
        self.data = [
            {"c1": 4, "c2": 2, "c3": 3, "c4": 2, "c5": 2, "c6": 1, "c7": 3, "c8": 2}
        ]
        self.sql = """CREATE MATERIALIZED VIEW int_min_view AS SELECT
                      MIN(c1) AS c1, MIN(c2) AS c2, MIN(c3) AS c3, MIN(c4) AS c4, MIN(c5) AS c5, MIN(c6) AS c6, MIN(c7) AS c7, MIN(c8) AS c8
                      FROM int0_tbl"""


class aggtst_int_min_groupby(TstView):
    def __init__(self):
        # Validated on Postgres
        self.data = [
            {
                "id": 0,
                "c1": 5,
                "c2": 2,
                "c3": 3,
                "c4": 2,
                "c5": 3,
                "c6": 4,
                "c7": 3,
                "c8": 3,
            },
            {
                "id": 1,
                "c1": 4,
                "c2": 3,
                "c3": 4,
                "c4": 2,
                "c5": 2,
                "c6": 1,
                "c7": 4,
                "c8": 2,
            },
        ]
        self.sql = """CREATE MATERIALIZED VIEW int_min_gby AS SELECT
                      id, MIN(c1) AS c1, MIN(c2) AS c2, MIN(c3) AS c3, MIN(c4) AS c4, MIN(c5) AS c5, MIN(c6) AS c6, MIN(c7) AS c7, MIN(c8) AS c8
                      FROM int0_tbl
                      GROUP BY id"""


class aggtst_int_min_distinct(TstView):
    def __init__(self):
        # Validated on Postgres
        self.data = [
            {"c1": 4, "c2": 2, "c3": 3, "c4": 2, "c5": 2, "c6": 1, "c7": 3, "c8": 2}
        ]
        self.sql = """CREATE MATERIALIZED VIEW int_min_distinct AS SELECT
                      MIN(DISTINCT c1) AS c1, MIN(DISTINCT c2) AS c2, MIN(DISTINCT c3) AS c3, MIN(DISTINCT c4) AS c4, MIN(DISTINCT c5) AS c5, MIN(DISTINCT c6) AS c6, MIN(DISTINCT c7) AS c7, MIN(DISTINCT c8) AS c8
                      FROM int0_tbl"""


class aggtst_int_min_distinct_groupby(TstView):
    def __init__(self):
        # Validated on Postgres
        self.data = [
            {
                "id": 0,
                "c1": 5,
                "c2": 2,
                "c3": 3,
                "c4": 2,
                "c5": 3,
                "c6": 4,
                "c7": 3,
                "c8": 3,
            },
            {
                "id": 1,
                "c1": 4,
                "c2": 3,
                "c3": 4,
                "c4": 2,
                "c5": 2,
                "c6": 1,
                "c7": 4,
                "c8": 2,
            },
        ]
        self.sql = """CREATE MATERIALIZED VIEW int_min_distinct_gby AS SELECT
                      id, MIN(DISTINCT c1) AS c1, MIN(DISTINCT c2) AS c2, MIN(DISTINCT c3) AS c3, MIN(DISTINCT c4) AS c4, MIN(DISTINCT c5) AS c5, MIN(DISTINCT c6) AS c6, MIN(DISTINCT c7) AS c7, MIN(DISTINCT c8) AS c8
                      FROM int0_tbl
                      GROUP BY id"""


class aggtst_int_min_where(TstView):
    def __init__(self):
        # Validated on Postgres
        self.data = [
            {
                "f_c1": 4,
                "f_c2": 2,
                "f_c3": 3,
                "f_c4": 2,
                "f_c5": 2,
                "f_c6": 3,
                "f_c7": 3,
                "f_c8": 2,
            }
        ]
        self.sql = """CREATE MATERIALIZED VIEW int_min_where AS SELECT
                      MIN(c1) FILTER(WHERE c5 + C6>3) AS f_c1, MIN(c2) FILTER(WHERE c5 + C6>3) AS f_c2, MIN(c3) FILTER(WHERE c5 + C6>3) AS f_c3, MIN(c4) FILTER(WHERE c5 + C6>3) AS f_c4, MIN(c5) FILTER(WHERE c5 + C6>3) AS f_c5, MIN(c6) FILTER(WHERE c5 + C6>3) AS f_c6,  MIN(c7) FILTER(WHERE c5 + C6>3) AS f_c7,  MIN(c8) FILTER(WHERE c5 + C6>3) AS f_c8
                      FROM int0_tbl"""


class aggtst_int_min_where_groupby(TstView):
    def __init__(self):
        # Validated on Postgres
        self.data = [
            {
                "id": 0,
                "f_c1": 5,
                "f_c2": 2,
                "f_c3": 3,
                "f_c4": 2,
                "f_c5": 3,
                "f_c6": 4,
                "f_c7": 3,
                "f_c8": 3,
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
        self.sql = """CREATE MATERIALIZED VIEW int_min_where_gby AS SELECT
                      id, MIN(c1) FILTER(WHERE c5 + C6>3) AS f_c1, MIN(c2) FILTER(WHERE c5 + C6>3) AS f_c2, MIN(c3) FILTER(WHERE c5 + C6>3) AS f_c3, MIN(c4) FILTER(WHERE c5 + C6>3) AS f_c4, MIN(c5) FILTER(WHERE c5 + C6>3) AS f_c5, MIN(c6) FILTER(WHERE c5 + C6>3) AS f_c6,  MIN(c7) FILTER(WHERE c5 + C6>3) AS f_c7,  MIN(c8) FILTER(WHERE c5 + C6>3) AS f_c8
                      FROM int0_tbl
                      GROUP BY id"""
