from tests.runtime_aggtest.aggtst_base import TstView


class aggtst_varchar_min_value(TstView):
    def __init__(self):
        # Validated on Postgres
        self.data = [{"c1": "@abc", "c2": "abc   d"}]
        self.sql = """CREATE MATERIALIZED VIEW varchar_min AS SELECT
                      MIN(c1) AS c1, MIN(c2) AS c2
                      FROM varchar_tbl"""


class aggtst_varchar_min_gby(TstView):
    def __init__(self):
        # Validated on Postgres
        self.data = [
            {"id": 0, "c1": "hello", "c2": "abc   d"},
            {"id": 1, "c1": "@abc", "c2": "exampl e"},
        ]
        self.sql = """CREATE MATERIALIZED VIEW varchar_min_gby AS SELECT
                      id, MIN(c1) AS c1, MIN(c2) AS c2
                      FROM varchar_tbl
                      GROUP BY id"""


class aggtst_varchar_min_distinct(TstView):
    def __init__(self):
        # Validated on Postgres
        self.data = [{"c1": "@abc", "c2": "abc   d"}]
        self.sql = """CREATE MATERIALIZED VIEW varchar_min_distinct AS SELECT
                      MIN(DISTINCT c1) AS c1, MIN(DISTINCT c2) AS c2
                      FROM varchar_tbl"""


class aggtst_varchar_min_distinct_gby(TstView):
    def __init__(self):
        # Validated on Postgres
        self.data = [
            {"id": 0, "c1": "hello", "c2": "abc   d"},
            {"id": 1, "c1": "@abc", "c2": "exampl e"},
        ]
        self.sql = """CREATE MATERIALIZED VIEW varchar_min_distinct_gby AS SELECT
                      id, MIN(DISTINCT c1) AS c1, MIN(DISTINCT c2) AS c2
                      FROM varchar_tbl
                      GROUP BY id"""


class aggtst_varchar_min_where(TstView):
    def __init__(self):
        # Validated on Postgres
        self.data = [{"c1": "hello", "c2": "exampl e"}]
        self.sql = """CREATE MATERIALIZED VIEW varchar_min_where AS SELECT
                      MIN(c1) FILTER(WHERE len(c1)>4) AS c1, MIN(c2) FILTER(WHERE len(c1)>4) AS c2
                      FROM varchar_tbl"""


class aggtst_varchar_min_where_gby(TstView):
    def __init__(self):
        # Validated on Postgres
        self.data = [
            {"id": 0, "c1": "hello", "c2": "fred"},
            {"id": 1, "c1": "hello", "c2": "exampl e"},
        ]
        self.sql = """CREATE MATERIALIZED VIEW varchar_min_where_gby AS SELECT
                      id, MIN(c1) FILTER(WHERE len(c1)>4) AS c1, MIN(c2) FILTER(WHERE len(c1)>4) AS c2
                      FROM varchar_tbl
                      GROUP BY id"""
