from tests.runtime_aggtest.aggtst_base import TstView


class aggtst_varchar_max_value(TstView):
    def __init__(self):
        # Validated on Postgres
        self.data = [{"c1": "hello", "c2": "variable-length"}]
        self.sql = """CREATE MATERIALIZED VIEW varchar_max AS SELECT
                      MAX(c1) AS c1, MAX(c2) AS c2
                      FROM varchar_tbl"""


class aggtst_varchar_max_gby(TstView):
    def __init__(self):
        # Validated on Postgres
        self.data = [
            {"id": 0, "c1": "hello", "c2": "fred"},
            {"id": 1, "c1": "hello", "c2": "variable-length"},
        ]
        self.sql = """CREATE MATERIALIZED VIEW varchar_max_gby AS SELECT
                      id, MAX(c1) AS c1, MAX(c2) AS c2
                      FROM varchar_tbl
                      GROUP BY id"""


class aggtst_varchar_max_distinct(TstView):
    def __init__(self):
        # Validated on Postgres
        self.data = [{"c1": "hello", "c2": "variable-length"}]
        self.sql = """CREATE MATERIALIZED VIEW varchar_max_distinct AS SELECT
                      MAX(DISTINCT c1) AS c1, MAX(DISTINCT c2) AS c2
                      FROM varchar_tbl"""


class aggtst_varchar_max_distinct_gby(TstView):
    def __init__(self):
        # Validated on Postgres
        self.data = [
            {"id": 0, "c1": "hello", "c2": "fred"},
            {"id": 1, "c1": "hello", "c2": "variable-length"},
        ]
        self.sql = """CREATE MATERIALIZED VIEW varchar_max_distinct_gby AS SELECT
                      id, MAX(DISTINCT c1) AS c1, MAX(DISTINCT c2) AS c2
                      FROM varchar_tbl
                      GROUP BY id"""


class aggtst_varchar_max_where(TstView):
    def __init__(self):
        # Validated on Postgres
        self.data = [{"c1": "hello", "c2": "fred"}]
        self.sql = """CREATE MATERIALIZED VIEW varchar_max_where AS SELECT
                      MAX(c1) FILTER(WHERE len(c1)>4) AS c1, MAX(c2) FILTER(WHERE len(c1)>4) AS c2
                      FROM varchar_tbl"""


class aggtst_varchar_max_where_gby(TstView):
    def __init__(self):
        # Validated on Postgres
        self.data = [
            {"id": 0, "c1": "hello", "c2": "fred"},
            {"id": 1, "c1": "hello", "c2": "exampl e"},
        ]
        self.sql = """CREATE MATERIALIZED VIEW varchar_max_where_gby AS SELECT
                      id, MAX(c1) FILTER(WHERE len(c1)>4) AS c1, MAX(c2) FILTER(WHERE len(c1)>4) AS c2
                      FROM varchar_tbl
                      GROUP BY id"""
