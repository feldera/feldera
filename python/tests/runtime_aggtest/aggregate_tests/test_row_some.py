from tests.runtime_aggtest.aggtst_base import TstView


class aggtst_row_some(TstView):
    def __init__(self):
        # Validated on Postgres
        self.data = [{"c1": True}]
        self.sql = """CREATE MATERIALIZED VIEW row_some AS SELECT
                      SOME(ROW(c3, c2) !=  ROW(c2, c3)) AS c1
                      FROM row_tbl"""


class aggtst_row_some_groupby(TstView):
    def __init__(self):
        # Validated on Postgres
        self.data = [
            {"id": 0, "c1": True},
            {"id": 1, "c1": True},
        ]
        self.sql = """CREATE MATERIALIZED VIEW row_some_gby AS SELECT
                      id, SOME(ROW(c3, c2) !=  ROW(c2, c3)) AS c1
                      FROM row_tbl
                      GROUP BY id"""


class aggtst_row_some_distinct(TstView):
    def __init__(self):
        # Validated on Postgres
        self.data = [{"c1": False}]
        self.sql = """CREATE MATERIALIZED VIEW row_some_distinct AS SELECT
                      SOME(DISTINCT ROW(c3, c2) !=  ROW(c3, c2)) AS c1
                      FROM row_tbl"""


class aggtst_row_some_distinct_gby(TstView):
    def __init__(self):
        # Validated on Postgres
        self.data = [
            {"id": 0, "c1": False},
            {"id": 1, "c1": False},
        ]
        self.sql = """CREATE MATERIALIZED VIEW row_some_distinct_gby AS SELECT
                      id, SOME(DISTINCT ROW(c3, c2) !=  ROW(c3, c2)) AS c1
                      FROM row_tbl
                      GROUP BY id"""


class aggtst_row_some_where(TstView):
    def __init__(self):
        # Validated on Postgres
        self.data = [{"c1": None}]
        self.sql = """CREATE MATERIALIZED VIEW row_some_where AS SELECT
                      SOME(ROW(c3, c2) =  ROW(c2, c3)) FILTER(WHERE c2 IS NULL) AS c1
                      FROM row_tbl"""


class aggtst_row_some_where_groupby(TstView):
    def __init__(self):
        # Validated on Postgres
        self.data = [
            {"id": 0, "c1": None},
            {"id": 1, "c1": None},
        ]
        self.sql = """CREATE MATERIALIZED VIEW row_some_where_gby AS SELECT
                      id, SOME(ROW(c3, c2) =  ROW(c2, c3)) FILTER(WHERE c2 IS NULL) AS c1
                      FROM row_tbl
                      GROUP BY id"""
