from tests.runtime_aggtest.aggtst_base import TstView


class aggtst_int_some(TstView):
    def __init__(self):
        # Validated on Postgres
        self.data = [
            {
                "c1": True,
                "c2": True,
                "c3": True,
                "c4": True,
                "c5": True,
                "c6": True,
                "c7": True,
                "c8": True,
            }
        ]
        self.sql = """CREATE MATERIALIZED VIEW int_some AS SELECT
                      SOME(c1 = 4) AS c1, SOME(c2 > 1) AS c2, SOME(c3>3) AS c3, SOME(c4>1) AS c4, SOME(c5>1) AS c5, SOME(c6 % 2 = 1) AS c6, SOME(c7>2) AS c7, SOME(c8>2) AS c8
                      FROM int0_tbl"""


class aggtst_int_some_groupby(TstView):
    def __init__(self):
        # Validated on Postgres
        self.data = [
            {
                "id": 0,
                "c1": False,
                "c2": True,
                "c3": False,
                "c4": True,
                "c5": True,
                "c6": False,
                "c7": True,
                "c8": True,
            },
            {
                "id": 1,
                "c1": True,
                "c2": True,
                "c3": True,
                "c4": True,
                "c5": True,
                "c6": True,
                "c7": True,
                "c8": True,
            },
        ]
        self.sql = """CREATE MATERIALIZED VIEW int_some_gby AS SELECT
                      id, SOME(c1 = 4) AS c1, SOME(c2 > 1) AS c2, SOME(c3>3) AS c3, SOME(c4>1) AS c4, SOME(c5>1) AS c5, SOME(c6 % 2 = 1) AS c6, SOME(c7>2) AS c7, SOME(c8>2) AS c8
                      FROM int0_tbl
                      GROUP BY id"""


class aggtst_int_some_distinct(TstView):
    def __init__(self):
        # Validated on Postgres
        self.sql = """CREATE MATERIALIZED VIEW int_some_distinct AS SELECT
                      SOME(DISTINCT(c1 = 4)) AS c1, SOME(DISTINCT(c2 > 1)) AS c2, SOME(DISTINCT(c3>3)) AS c3, SOME(DISTINCT(c4>1)) AS c4, SOME(DISTINCT(c5>1)) AS c5, SOME(DISTINCT(c6 % 2 = 1)) AS c6, SOME(DISTINCT(c7>2)) AS c7, SOME(DISTINCT(c8>2)) AS c8
                      FROM int0_tbl"""
        self.data = [
            {
                "c1": True,
                "c2": True,
                "c3": True,
                "c4": True,
                "c5": True,
                "c6": True,
                "c7": True,
                "c8": True,
            }
        ]


class aggtst_int_some_distinct_gby(TstView):
    def __init__(self):
        # Validated on Postgres
        self.sql = """CREATE MATERIALIZED VIEW int_some_distinct_gby AS SELECT
                      id, SOME(DISTINCT(c1 = 4)) AS c1, SOME(DISTINCT(c2 > 1)) AS c2, SOME(DISTINCT(c3>3)) AS c3, SOME(DISTINCT(c4>1)) AS c4, SOME(DISTINCT(c5>1)) AS c5, SOME(DISTINCT(c6 % 2 = 1)) AS c6, SOME(DISTINCT(c7>2)) AS c7, SOME(DISTINCT(c8>2)) AS c8
                      FROM int0_tbl
                      GROUP BY id"""
        self.data = [
            {
                "id": 0,
                "c1": False,
                "c2": True,
                "c3": False,
                "c4": True,
                "c5": True,
                "c6": False,
                "c7": True,
                "c8": True,
            },
            {
                "id": 1,
                "c1": True,
                "c2": True,
                "c3": True,
                "c4": True,
                "c5": True,
                "c6": True,
                "c7": True,
                "c8": True,
            },
        ]


class aggtst_int_some_where(TstView):
    def __init__(self):
        # Validated on Postgres
        self.data = [
            {
                "c1": True,
                "c2": True,
                "c3": True,
                "c4": True,
                "c5": True,
                "c6": True,
                "c7": True,
                "c8": True,
            }
        ]
        self.sql = """CREATE MATERIALIZED VIEW int_some_where AS SELECT
                      SOME(c1 = 4) FILTER (WHERE c1 > 0) AS c1, SOME(c2 > 1) FILTER (WHERE c1 > 0) AS c2, SOME(c3 > 3) FILTER (WHERE c1 > 0) AS c3, SOME(c4 > 1) FILTER (WHERE c1 > 0) AS c4, SOME(c5 > 1) FILTER (WHERE c1 > 0) AS c5, SOME(c6 % 2 = 1) FILTER (WHERE c1 > 0) AS c6, SOME(c7 > 2) FILTER (WHERE c1 > 0) AS c7, SOME(c8 > 2) FILTER (WHERE c1 > 0) AS c8
                      FROM int0_tbl"""


class aggtst_int_some_where_groupby(TstView):
    def __init__(self):
        # Validated on Postgres
        self.data = [
            {
                "id": 0,
                "c1": False,
                "c2": True,
                "c3": None,
                "c4": True,
                "c5": True,
                "c6": False,
                "c7": None,
                "c8": True,
            },
            {
                "id": 1,
                "c1": True,
                "c2": True,
                "c3": True,
                "c4": True,
                "c5": True,
                "c6": True,
                "c7": True,
                "c8": False,
            },
        ]
        self.sql = """CREATE MATERIALIZED VIEW int_some_where_gby AS SELECT
                      id, SOME(c1 = 4) FILTER (WHERE c1 > 0) AS c1, SOME(c2 > 1) FILTER (WHERE c1 > 0) AS c2, SOME(c3 > 3) FILTER (WHERE c1 > 0) AS c3, SOME(c4 > 1) FILTER (WHERE c1 > 0) AS c4, SOME(c5 > 1) FILTER (WHERE c1 > 0) AS c5, SOME(c6 % 2 = 1) FILTER (WHERE c1 > 0) AS c6, SOME(c7 > 2) FILTER (WHERE c1 > 0) AS c7, SOME(c8 > 2) FILTER (WHERE c1 > 0) AS c8
                      FROM int0_tbl
                      GROUP BY id"""
