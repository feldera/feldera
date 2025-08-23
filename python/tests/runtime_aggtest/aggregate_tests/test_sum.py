from tests.runtime_aggtest.aggtst_base import TstView


class aggtst_int_sum(TstView):
    def __init__(self):
        # Validated on Postgres
        self.sql = """CREATE MATERIALIZED VIEW int_sum AS SELECT
                      SUM(c1) AS c1, SUM(c2) AS c2, SUM(c3) AS c3, SUM(c4) AS c4, SUM(c5) AS c5, SUM(c6) AS c6, SUM(c7) AS c7, SUM(c8) AS c8
                      FROM int0_tbl"""
        self.data = [
            {
                "c1": 9,
                "c2": 12,
                "c3": 13,
                "c4": 14,
                "c5": 12,
                "c6": 14,
                "c7": 7,
                "c8": 18,
            }
        ]


class aggtst_int_sum_gby(TstView):
    # Validated on Postgres
    def __init__(self):
        self.sql = """CREATE MATERIALIZED VIEW int_sum_gby AS SELECT
                      id, SUM(c1) AS c1, SUM(c2) AS c2, SUM(c3) AS c3, SUM(c4) AS c4, SUM(c5) AS c5, SUM(c6) AS c6, SUM(c7) AS c7, SUM(c8) AS c8
                      FROM int0_tbl
                      GROUP BY id"""
        self.data = [
            {
                "id": 0,
                "c1": 5,
                "c2": 4,
                "c3": 3,
                "c4": 6,
                "c5": 8,
                "c6": 10,
                "c7": 3,
                "c8": 11,
            },
            {
                "id": 1,
                "c1": 4,
                "c2": 8,
                "c3": 10,
                "c4": 8,
                "c5": 4,
                "c6": 4,
                "c7": 4,
                "c8": 7,
            },
        ]


class aggtst_int_sum_distinct(TstView):
    def __init__(self):
        # Validated on Postgres
        self.sql = """CREATE MATERIALIZED VIEW int_sum_distinct AS SELECT
                      SUM(DISTINCT c1) AS c1, SUM(DISTINCT c2) AS c2, SUM(DISTINCT c3) AS c3, SUM(DISTINCT c4) AS c4, SUM(DISTINCT c5) AS c5, SUM(DISTINCT c6) AS c6, SUM(DISTINCT c7) AS c7, SUM(DISTINCT c8) AS c8
                      FROM int0_tbl"""
        self.data = [
            {
                "c1": 9,
                "c2": 10,
                "c3": 13,
                "c4": 12,
                "c5": 10,
                "c6": 14,
                "c7": 7,
                "c8": 18,
            }
        ]


class aggtst_int_sum_distinct_gby(TstView):
    def __init__(self):
        # Validated on Postgres
        self.sql = """CREATE MATERIALIZED VIEW int_sum_distinct_gby AS SELECT
                      id, SUM(DISTINCT c1) AS c1, SUM(DISTINCT c2) AS c2, SUM(DISTINCT c3) AS c3, SUM(DISTINCT c4) AS c4, SUM(DISTINCT c5) AS c5, SUM(DISTINCT c6) AS c6, SUM(DISTINCT c7) AS c7, SUM(DISTINCT c8) AS c8
                      FROM int0_tbl
                      GROUP BY id"""
        self.data = [
            {
                "id": 0,
                "c1": 5,
                "c2": 2,
                "c3": 3,
                "c4": 6,
                "c5": 8,
                "c6": 10,
                "c7": 3,
                "c8": 11,
            },
            {
                "id": 1,
                "c1": 4,
                "c2": 8,
                "c3": 10,
                "c4": 8,
                "c5": 2,
                "c6": 4,
                "c7": 4,
                "c8": 7,
            },
        ]


class aggtst_int_sum_where(TstView):
    def __init__(self):
        # Validated on Postgres
        self.sql = """CREATE MATERIALIZED VIEW int_sum_where AS SELECT
                      SUM(c1) FILTER(WHERE c8>2) AS f_c1, SUM(c2) FILTER(WHERE c8>2) AS f_c2, SUM(c3) FILTER(WHERE c8>2) AS f_c3, SUM(c4) FILTER(WHERE c8>2) AS f_c4, SUM(c5) FILTER(WHERE c8>2) AS f_c5, SUM(c6) FILTER(WHERE c8>2) AS f_c6,  SUM(c7) FILTER(WHERE c8>2) AS f_c7,  SUM(c8) FILTER(WHERE c8>2) AS f_c8
                      FROM int0_tbl"""
        self.data = [
            {
                "f_c1": 5,
                "f_c2": 9,
                "f_c3": 9,
                "f_c4": 8,
                "f_c5": 10,
                "f_c6": 11,
                "f_c7": 3,
                "f_c8": 16,
            }
        ]


class aggtst_int_sum_where_gby(TstView):
    def __init__(self):
        # Validated on Postgres
        self.sql = """CREATE MATERIALIZED VIEW int_sum_where_gby AS SELECT
                      id, SUM(c1) FILTER(WHERE c8>2) AS f_c1, SUM(c2) FILTER(WHERE c8>2) AS f_c2, SUM(c3) FILTER(WHERE c8>2) AS f_c3, SUM(c4) FILTER(WHERE c8>2) AS f_c4, SUM(c5) FILTER(WHERE c8>2) AS f_c5, SUM(c6) FILTER(WHERE c8>2) AS f_c6,  SUM(c7) FILTER(WHERE c8>2) AS f_c7,  SUM(c8) FILTER(WHERE c8>2) AS f_c8
                      FROM int0_tbl
                      GROUP BY id"""
        self.data = [
            {
                "id": 0,
                "f_c1": 5,
                "f_c2": 4,
                "f_c3": 3,
                "f_c4": 6,
                "f_c5": 8,
                "f_c6": 10,
                "f_c7": 3,
                "f_c8": 11,
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
