from tests.runtime_aggtest.aggtst_base import TstView


class aggtst_time_some(TstView):
    def __init__(self):
        # Validated on Postgres
        self.data = [{"c1": True, "c2": True}]
        self.sql = """CREATE MATERIALIZED VIEW time_some AS SELECT
                      SOME(c1 > '08:30:00') AS c1, SOME(c2 > '12:45:00') AS c2
                      FROM time_tbl"""


class aggtst_time_some_groupby(TstView):
    def __init__(self):
        # Validated on Postgres
        self.data = [
            {"id": 0, "c1": True, "c2": False},
            {"id": 1, "c1": True, "c2": True},
        ]
        self.sql = """CREATE MATERIALIZED VIEW time_some_gby AS SELECT
                      id, SOME(c1 > '08:30:00') AS c1, SOME(c2 > '12:45:00') AS c2
                      FROM time_tbl
                      GROUP BY id"""


class aggtst_time_some_distinct(TstView):
    def __init__(self):
        # Validated on Postgres
        self.data = [{"c1": True, "c2": True}]
        self.sql = """CREATE MATERIALIZED VIEW time_some_distinct AS SELECT
                      SOME(DISTINCT c1 > '08:30:00') AS c1, SOME(DISTINCT c2 > '12:45:00') AS c2
                      FROM time_tbl"""


class aggtst_time_some_distinct_gby(TstView):
    def __init__(self):
        # Validated on Postgres
        self.data = [
            {"id": 0, "c1": True, "c2": False},
            {"id": 1, "c1": True, "c2": True},
        ]
        self.sql = """CREATE MATERIALIZED VIEW time_some_distinct_gby AS SELECT
                      id, SOME(DISTINCT c1 > '08:30:00') AS c1, SOME(DISTINCT c2 > '12:45:00') AS c2
                      FROM time_tbl
                      GROUP BY id"""


class aggtst_time_some_where(TstView):
    def __init__(self):
        # Validated on Postgres
        self.data = [{"c1": True, "c2": True}]
        self.sql = """CREATE MATERIALIZED VIEW time_some_where AS SELECT
                      SOME(c1 > '08:30:00') FILTER(WHERE c2 IS NOT NULL) AS c1, SOME(c2 > '12:45:00') FILTER(WHERE c2 IS NOT NULL) AS c2
                      FROM time_tbl"""


class aggtst_time_some_where_groupby(TstView):
    def __init__(self):
        # Validated on Postgres
        self.data = [
            {"id": 0, "c1": False, "c2": False},
            {"id": 1, "c1": True, "c2": True},
        ]
        self.sql = """CREATE MATERIALIZED VIEW time_some_where_gby AS SELECT
                      id, SOME(c1 > '08:30:00') FILTER(WHERE c2 IS NOT NULL) AS c1, SOME(c2 > '12:45:00') FILTER(WHERE c2 IS NOT NULL) AS c2
                      FROM time_tbl
                      GROUP BY id"""
