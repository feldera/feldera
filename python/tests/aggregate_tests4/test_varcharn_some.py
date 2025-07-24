from tests.aggregate_tests.aggtst_base import TstView


class aggtst_varcharn_some(TstView):
    def __init__(self):
        # Validated on Postgres
        self.data = [{"c1": True, "c2": True}]
        self.sql = """CREATE MATERIALIZED VIEW varcharn_some AS SELECT
                      SOME(f_c1 != '%hello%') AS c1, SOME(f_c2 LIKE '%a%') AS c2
                      FROM atbl_varcharn"""


class aggtst_varcharn_some_groupby(TstView):
    def __init__(self):
        # Validated on Postgres
        self.data = [
            {"id": 0, "c1": True, "c2": True},
            {"id": 1, "c1": True, "c2": True},
        ]
        self.sql = """CREATE MATERIALIZED VIEW varcharn_some_gby AS SELECT
                      id, SOME(f_c1 != '%hello%') AS c1, SOME(f_c2 LIKE '%a%') AS c2
                      FROM atbl_varcharn
                      GROUP BY id"""


class aggtst_varcharn_some_distinct(TstView):
    def __init__(self):
        # Validated on Postgres
        self.data = [{"c1": True, "c2": True}]
        self.sql = """CREATE MATERIALIZED VIEW varcharn_some_distinct AS SELECT
                      SOME(DISTINCT f_c1 != '%hello%') AS c1, SOME(DISTINCT f_c2 LIKE '%a%') AS c2
                      FROM atbl_varcharn"""


class aggtst_varcharn_some_distinct_gby(TstView):
    def __init__(self):
        # Validated on Postgres
        self.data = [
            {"id": 0, "c1": True, "c2": True},
            {"id": 1, "c1": True, "c2": True},
        ]
        self.sql = """CREATE MATERIALIZED VIEW varcharn_some_distinct_gby AS SELECT
                      id, SOME(DISTINCT f_c1 != '%hello%') AS c1, SOME(DISTINCT f_c2 LIKE '%a%') AS c2
                      FROM atbl_varcharn
                      GROUP BY id"""


class aggtst_varcharn_some_where(TstView):
    def __init__(self):
        # Validated on Postgres
        self.data = [{"c1": True, "c2": False}]
        self.sql = """CREATE MATERIALIZED VIEW varcharn_some_where AS SELECT
                      SOME(f_c1 != '%hello%') FILTER(WHERE len(f_c2)=4) AS c1, SOME(f_c2 LIKE '%a%') FILTER(WHERE len(f_c2)=4) AS c2
                      FROM atbl_varcharn"""


class aggtst_varcharn_some_where_groupby(TstView):
    def __init__(self):
        # Validated on Postgres
        self.data = [
            {"id": 0, "c1": True, "c2": False},
            {"id": 1, "c1": None, "c2": None},
        ]
        self.sql = """CREATE MATERIALIZED VIEW varcharn_some_where_gby AS SELECT
                      id, SOME(f_c1 != '%hello%') FILTER(WHERE len(f_c2)=4) AS c1, SOME(f_c2 LIKE '%a%') FILTER(WHERE len(f_c2)=4) AS c2
                      FROM atbl_varcharn
                      GROUP BY id"""
