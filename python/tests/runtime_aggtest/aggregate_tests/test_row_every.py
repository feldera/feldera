from tests.runtime_aggtest.aggtst_base import TstView


class aggtst_row_every(TstView):
    def __init__(self):
        # Validated on Postgres
        self.data = [{"c1": True}]
        self.sql = """CREATE MATERIALIZED VIEW row_every AS SELECT
                      EVERY(ROW(c3, c2) !=  ROW(c2, c3)) AS c1
                      FROM row_tbl"""


class aggtst_row_every_groupby(TstView):
    def __init__(self):
        # Validated on Postgres
        self.data = [
            {"id": 0, "c1": True},
            {"id": 1, "c1": True},
        ]
        self.sql = """CREATE MATERIALIZED VIEW row_every_gby AS SELECT
                      id, EVERY(ROW(c3, c2) !=  ROW(c2, c3)) AS c1
                      FROM row_tbl
                      GROUP BY id"""


class aggtst_row_every_distinct(TstView):
    def __init__(self):
        # Validated on Postgres
        self.data = [{"c1": False}]
        self.sql = """CREATE MATERIALIZED VIEW row_every_distinct AS SELECT
                      EVERY(DISTINCT ROW(c3, c2) !=  ROW(c3, c2)) AS c1
                      FROM row_tbl"""


class aggtst_row_every_distinct_gby(TstView):
    def __init__(self):
        # Validated on Postgres
        self.data = [
            {"id": 0, "c1": False},
            {"id": 1, "c1": False},
        ]
        self.sql = """CREATE MATERIALIZED VIEW row_every_distinct_gby AS SELECT
                      id, EVERY(DISTINCT ROW(c3, c2) !=  ROW(c3, c2)) AS c1
                      FROM row_tbl
                      GROUP BY id"""


class aggtst_row_every_where(TstView):
    def __init__(self):
        # Validated on Postgres
        self.data = [{"c1": None}]
        self.sql = """CREATE MATERIALIZED VIEW row_every_where AS SELECT
                      EVERY(ROW(c3, c2) =  ROW(c2, c3)) FILTER(WHERE c2 IS NULL) AS c1
                      FROM row_tbl"""


class aggtst_row_every_where_groupby(TstView):
    def __init__(self):
        # Validated on Postgres
        self.data = [
            {"id": 0, "c1": None},
            {"id": 1, "c1": None},
        ]
        self.sql = """CREATE MATERIALIZED VIEW row_every_where_gby AS SELECT
                      id, EVERY(ROW(c3, c2) =  ROW(c2, c3)) FILTER(WHERE c2 IS NULL) AS c1
                      FROM row_tbl
                      GROUP BY id"""
