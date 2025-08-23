from tests.runtime_aggtest.aggtst_base import TstView


class aggtst_binary_some(TstView):
    def __init__(self):
        # Validated on Postgres
        self.data = [{"c1": True, "c2": True}]
        self.sql = """CREATE MATERIALIZED VIEW binary_some AS SELECT
                      SOME(c1 > c2) AS c1, SOME(c2 > c1) AS c2
                      FROM binary_tbl"""


class aggtst_binary_some_groupby(TstView):
    def __init__(self):
        # Validated on Postgres
        self.data = [
            {"id": 0, "c1": False, "c2": True},
            {"id": 1, "c1": True, "c2": True},
        ]
        self.sql = """CREATE MATERIALIZED VIEW binary_some_gby AS SELECT
                      id, SOME(c1 > c2) AS c1, SOME(c2 > c1) AS c2
                      FROM binary_tbl
                      GROUP BY id"""


class aggtst_binary_some_distinct(TstView):
    def __init__(self):
        # Validated on Postgres
        self.data = [{"c1": True, "c2": True}]
        self.sql = """CREATE MATERIALIZED VIEW binary_some_distinct AS SELECT
                      SOME(DISTINCT c1 > c2) AS c1, SOME(DISTINCT c2 > c1) AS c2
                      FROM binary_tbl"""


class aggtst_binary_some_distinct_gby(TstView):
    def __init__(self):
        # Validated on Postgres
        self.data = [
            {"id": 0, "c1": False, "c2": True},
            {"id": 1, "c1": True, "c2": True},
        ]
        self.sql = """CREATE MATERIALIZED VIEW binary_some_distinct_gby AS SELECT
                      id, SOME(DISTINCT c1 > c2) AS c1, SOME(DISTINCT c2 > c1) AS c2
                      FROM binary_tbl
                      GROUP BY id"""


class aggtst_binary_some_where(TstView):
    def __init__(self):
        # Validated on Postgres
        self.data = [{"c1": True, "c2": True}]
        self.sql = """CREATE MATERIALIZED VIEW binary_some_where AS SELECT
                      SOME(c1 > c2) FILTER(WHERE c2 IS NOT NULL) AS c1, SOME(c2 > c1) FILTER(WHERE c2 IS NOT NULL) AS c2
                      FROM binary_tbl"""


class aggtst_binary_some_where_groupby(TstView):
    def __init__(self):
        # Validated on Postgres
        self.data = [
            {"id": 0, "c1": False, "c2": True},
            {"id": 1, "c1": True, "c2": True},
        ]
        self.sql = """CREATE MATERIALIZED VIEW binary_some_where_gby AS SELECT
                      id, SOME(c1 > c2) FILTER(WHERE c2 IS NOT NULL) AS c1, SOME(c2 > c1) FILTER(WHERE c2 IS NOT NULL) AS c2
                      FROM binary_tbl
                      GROUP BY id"""
