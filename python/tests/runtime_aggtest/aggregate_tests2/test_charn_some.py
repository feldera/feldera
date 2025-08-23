from tests.runtime_aggtest.aggtst_base import TstView


class aggtst_charn_some(TstView):
    def __init__(self):
        # Validated on Postgres
        self.data = [{"c1": True, "c2": True}]
        self.sql = """CREATE MATERIALIZED VIEW charn_some AS SELECT
                      SOME(f_c1 != '%hello  %') AS c1, SOME(f_c2 LIKE '%a%') AS c2
                      FROM atbl_charn"""


class aggtst_charn_some_groupby(TstView):
    def __init__(self):
        # Validated on Postgres
        self.data = [
            {"id": 0, "c1": True, "c2": True},
            {"id": 1, "c1": True, "c2": True},
        ]
        self.sql = """CREATE MATERIALIZED VIEW charn_some_gby AS SELECT
                      id, SOME(f_c1 != '%hello  %') AS c1, SOME(f_c2 LIKE '%a%') AS c2
                      FROM atbl_charn
                      GROUP BY id"""


class aggtst_charn_some_distinct(TstView):
    def __init__(self):
        # Validated on Postgres
        self.data = [{"c1": True, "c2": True}]
        self.sql = """CREATE MATERIALIZED VIEW charn_some_distinct AS SELECT
                      SOME(DISTINCT f_c1 != '%hello  %') AS c1, SOME(DISTINCT f_c2 LIKE '%a%') AS c2
                      FROM atbl_charn"""


class aggtst_charn_some_distinct_gby(TstView):
    def __init__(self):
        # Validated on Postgres
        self.data = [
            {"id": 0, "c1": True, "c2": True},
            {"id": 1, "c1": True, "c2": True},
        ]
        self.sql = """CREATE MATERIALIZED VIEW charn_some_distinct_gby AS SELECT
                      id, SOME(DISTINCT f_c1 != '%hello  %') AS c1, SOME(DISTINCT f_c2 LIKE '%a%') AS c2
                      FROM atbl_charn
                      GROUP BY id"""


class aggtst_charn_some_where(TstView):
    def __init__(self):
        # Validated on Postgres
        self.data = [{"c1": True, "c2": True}]
        self.sql = """CREATE MATERIALIZED VIEW charn_some_where AS SELECT
                      SOME(f_c1 != '%hello  %') FILTER (WHERE f_c1 IS NOT NULL) AS c1, SOME(f_c2 LIKE '%a%') FILTER (WHERE f_c1 IS NOT NULL) AS c2
                      FROM atbl_charn"""


class aggtst_charn_some_where_groupby(TstView):
    def __init__(self):
        # Validated on Postgres
        self.data = [
            {"id": 0, "c1": True, "c2": False},
            {"id": 1, "c1": True, "c2": True},
        ]
        self.sql = """CREATE MATERIALIZED VIEW charn_some_where_gby AS SELECT
                      id, SOME(f_c1 != '%hello  %') FILTER (WHERE f_c1 IS NOT NULL) AS c1, SOME(f_c2 LIKE '%a%') FILTER (WHERE f_c1 IS NOT NULL) AS c2
                      FROM atbl_charn
                      GROUP BY id"""
