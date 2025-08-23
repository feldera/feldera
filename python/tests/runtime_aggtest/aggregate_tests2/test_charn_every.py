from tests.runtime_aggtest.aggtst_base import TstView


class aggtst_charn_every(TstView):
    def __init__(self):
        # Validated on Postgres
        self.data = [{"c1": True, "c2": False}]
        self.sql = """CREATE MATERIALIZED VIEW charn_every AS SELECT
                      EVERY(f_c1 != '%hello  %') AS c1, EVERY(f_c2 LIKE '%a%') AS c2
                      FROM atbl_charn"""


class aggtst_charn_every_groupby(TstView):
    def __init__(self):
        # Validated on Postgres
        self.data = [
            {"id": 0, "c1": True, "c2": False},
            {"id": 1, "c1": True, "c2": True},
        ]
        self.sql = """CREATE MATERIALIZED VIEW charn_every_gby AS SELECT
                      id, EVERY(f_c1 != '%hello  %') AS c1, EVERY(f_c2 LIKE '%a%') AS c2
                      FROM atbl_charn
                      GROUP BY id"""


class aggtst_charn_every_distinct(TstView):
    def __init__(self):
        # Validated on Postgres
        self.data = [{"c1": True, "c2": False}]
        self.sql = """CREATE MATERIALIZED VIEW charn_every_distinct AS SELECT
                      EVERY(DISTINCT f_c1 != '%hello  %') AS c1, EVERY(DISTINCT f_c2 LIKE '%a%') AS c2
                      FROM atbl_charn"""


class aggtst_charn_every_distinct_gby(TstView):
    def __init__(self):
        # Validated on Postgres
        self.data = [
            {"id": 0, "c1": True, "c2": False},
            {"id": 1, "c1": True, "c2": True},
        ]
        self.sql = """CREATE MATERIALIZED VIEW charn_every_distinct_gby AS SELECT
                      id, EVERY(DISTINCT f_c1 != '%hello  %') AS c1, EVERY(DISTINCT f_c2 LIKE '%a%') AS c2
                      FROM atbl_charn
                      GROUP BY id"""


class aggtst_charn_every_where(TstView):
    def __init__(self):
        # Validated on Postgres
        self.data = [{"c1": True, "c2": False}]
        self.sql = """CREATE MATERIALIZED VIEW charn_every_where AS SELECT
                      EVERY(f_c1 != '%hello  %') FILTER (WHERE f_c1 IS NOT NULL) AS c1, EVERY(f_c2 LIKE '%a%') FILTER (WHERE f_c1 IS NOT NULL) AS c2
                      FROM atbl_charn"""


class aggtst_charn_every_where_groupby(TstView):
    def __init__(self):
        # Validated on Postgres
        self.data = [
            {"id": 0, "c1": True, "c2": False},
            {"id": 1, "c1": True, "c2": True},
        ]
        self.sql = """CREATE MATERIALIZED VIEW charn_every_where_gby AS SELECT
                      id, EVERY(f_c1 != '%hello  %') FILTER (WHERE f_c1 IS NOT NULL) AS c1, EVERY(f_c2 LIKE '%a%') FILTER (WHERE f_c1 IS NOT NULL) AS c2
                      FROM atbl_charn
                      GROUP BY id"""
