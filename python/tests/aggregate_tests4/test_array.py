from tests.aggregate_tests.aggtst_base import TstView


class aggtst_int_array_agg_value(TstView):
    def __init__(self):
        # Validated on Postgres
        self.data = [
            {
                "c1": [None, None, 4, 5],
                "c2": [2, 5, 3, 2],
                "c3": [3, 6, 4, None],
                "c4": [2, 2, 6, 4],
                "c5": [3, 2, 2, 5],
                "c6": [4, 1, 3, 6],
                "c7": [3, None, 4, None],
                "c8": [3, 5, 2, 8],
            }
        ]
        self.sql = """CREATE MATERIALIZED VIEW int_array_agg AS SELECT
                      ARRAY_AGG(c1) AS c1, ARRAY_AGG(c2) AS c2, ARRAY_AGG(c3) AS c3, ARRAY_AGG(c4) AS c4, ARRAY_AGG(c5) AS c5, ARRAY_AGG(c6) AS c6, ARRAY_AGG(c7) AS c7, ARRAY_AGG(c8) AS c8
                      FROM int0_tbl"""


class aggtst_int_array_agg_groupby(TstView):
    def __init__(self):
        # Validated on Postgres
        self.data = [
            {
                "id": 0,
                "c1": [None, 5],
                "c2": [2, 2],
                "c3": [3, None],
                "c4": [2, 4],
                "c5": [3, 5],
                "c6": [4, 6],
                "c7": [3, None],
                "c8": [3, 8],
            },
            {
                "id": 1,
                "c1": [None, 4],
                "c2": [5, 3],
                "c3": [6, 4],
                "c4": [2, 6],
                "c5": [2, 2],
                "c6": [1, 3],
                "c7": [None, 4],
                "c8": [5, 2],
            },
        ]
        self.sql = """CREATE MATERIALIZED VIEW int_array_agg_gby AS SELECT
                      id, ARRAY_AGG(c1) AS c1, ARRAY_AGG(c2) AS c2, ARRAY_AGG(c3) AS c3, ARRAY_AGG(c4) AS c4, ARRAY_AGG(c5) AS c5, ARRAY_AGG(c6) AS c6, ARRAY_AGG(c7) AS c7, ARRAY_AGG(c8) AS c8
                      FROM int0_tbl
                      GROUP BY id"""


class aggtst_int_array_agg_distinct(TstView):
    def __init__(self):
        # Validated on Postgres
        self.data = [
            {
                "c1": [None, 4, 5],
                "c2": [2, 3, 5],
                "c3": [None, 3, 4, 6],
                "c4": [2, 4, 6],
                "c5": [2, 3, 5],
                "c6": [1, 3, 4, 6],
                "c7": [None, 3, 4],
                "c8": [2, 3, 5, 8],
            }
        ]
        self.sql = """CREATE MATERIALIZED VIEW int_array_agg_distinct AS SELECT
                      ARRAY_AGG(DISTINCT c1) AS c1,ARRAY_AGG(DISTINCT c2) AS c2, ARRAY_AGG(DISTINCT c3) AS c3, ARRAY_AGG(DISTINCT c4) AS c4, ARRAY_AGG(DISTINCT c5) AS c5, ARRAY_AGG(DISTINCT c6) AS c6, ARRAY_AGG(DISTINCT c7) AS c7, ARRAY_AGG(DISTINCT c8) AS c8
                      FROM int0_tbl"""


class aggtst_int_array_agg_distinct_groupby(TstView):
    def __init__(self):
        # Validated on Postgres
        self.data = [
            {
                "id": 0,
                "c1": [None, 5],
                "c2": [2],
                "c3": [None, 3],
                "c4": [2, 4],
                "c5": [3, 5],
                "c6": [4, 6],
                "c7": [None, 3],
                "c8": [3, 8],
            },
            {
                "id": 1,
                "c1": [None, 4],
                "c2": [3, 5],
                "c3": [4, 6],
                "c4": [2, 6],
                "c5": [2],
                "c6": [1, 3],
                "c7": [None, 4],
                "c8": [2, 5],
            },
        ]
        self.sql = """CREATE MATERIALIZED VIEW int_array_agg_distinct_groupby AS SELECT
                      id, ARRAY_AGG(DISTINCT c1) AS c1,ARRAY_AGG(DISTINCT c2) AS c2, ARRAY_AGG(DISTINCT c3) AS c3, ARRAY_AGG(DISTINCT c4) AS c4, ARRAY_AGG(DISTINCT c5) AS c5, ARRAY_AGG(DISTINCT c6) AS c6, ARRAY_AGG(DISTINCT c7) AS c7, ARRAY_AGG(DISTINCT c8) AS c8
                      FROM int0_tbl
                      GROUP BY id"""


class aggtst_int_array_agg_where(TstView):
    def __init__(self):
        # Validated on Postgres
        self.data = [
            {
                "f_c1": [None, 4, 5],
                "f_c2": [2, 3, 2],
                "f_c3": [3, 4, None],
                "f_c4": [2, 6, 4],
                "f_c5": [3, 2, 5],
                "f_c6": [4, 3, 6],
                "f_c7": [3, 4, None],
                "f_c8": [3, 2, 8],
            }
        ]
        self.sql = """CREATE MATERIALIZED VIEW int_array_agg_where AS SELECT
                      ARRAY_AGG(c1) FILTER(WHERE (c5+C6)> 3) AS f_c1, ARRAY_AGG(c2) FILTER(WHERE (c5+C6)> 3) AS f_c2, ARRAY_AGG(c3) FILTER(WHERE (c5+C6)> 3) AS f_c3, ARRAY_AGG(c4) FILTER(WHERE (c5+C6)> 3) AS f_c4, ARRAY_AGG(c5) FILTER(WHERE (c5+C6)> 3) AS f_c5, ARRAY_AGG(c6) FILTER(WHERE (c5+C6)> 3) AS f_c6,  ARRAY_AGG(c7) FILTER(WHERE (c5+C6)> 3) AS f_c7,  ARRAY_AGG(c8) FILTER(WHERE (c5+C6)> 3) AS f_c8
                      FROM int0_tbl """


class aggtst_array_agg_where_groupby(TstView):
    def __init__(self):
        # Validated on Postgres
        self.data = [
            {
                "id": 0,
                "f_c1": [None, 5],
                "f_c2": [2, 2],
                "f_c3": [3, None],
                "f_c4": [2, 4],
                "f_c5": [3, 5],
                "f_c6": [4, 6],
                "f_c7": [3, None],
                "f_c8": [3, 8],
            },
            {
                "id": 1,
                "f_c1": [4],
                "f_c2": [3],
                "f_c3": [4],
                "f_c4": [6],
                "f_c5": [2],
                "f_c6": [3],
                "f_c7": [4],
                "f_c8": [2],
            },
        ]
        self.sql = """CREATE MATERIALIZED VIEW int_array_agg_where_gby AS SELECT
                      id, ARRAY_AGG(c1) FILTER(WHERE (c5+C6)> 3) AS f_c1, ARRAY_AGG(c2) FILTER(WHERE (c5+C6)> 3) AS f_c2, ARRAY_AGG(c3) FILTER(WHERE (c5+C6)> 3) AS f_c3, ARRAY_AGG(c4) FILTER(WHERE (c5+C6)> 3) AS f_c4, ARRAY_AGG(c5) FILTER(WHERE (c5+C6)> 3) AS f_c5, ARRAY_AGG(c6) FILTER(WHERE (c5+C6)> 3) AS f_c6,  ARRAY_AGG(c7) FILTER(WHERE (c5+C6)> 3) AS f_c7,  ARRAY_AGG(c8) FILTER(WHERE (c5+C6)> 3) AS f_c8
                      FROM int0_tbl
                      GROUP BY id"""
