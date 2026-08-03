from tests.runtime_aggtest.aggtst_base import TstView


# Feldera rejects '=' and '<>' between ROW values, so these tests use
# IS [NOT] DISTINCT FROM.  Unlike '=', it never returns NULL, which is why the
# results of the FILTER variants below differ from the Postgres ones.


class aggtst_row_every(TstView):
    def __init__(self):
        # checked manually
        self.data = [{"c1": True}]
        self.sql = """CREATE MATERIALIZED VIEW row_every AS SELECT
                      EVERY(ROW(c3, c2) IS DISTINCT FROM ROW(c2, c3)) AS c1
                      FROM row_tbl"""


class aggtst_row_every_groupby(TstView):
    def __init__(self):
        # checked manually
        self.data = [
            {"id": 0, "c1": True},
            {"id": 1, "c1": True},
        ]
        self.sql = """CREATE MATERIALIZED VIEW row_every_gby AS SELECT
                      id, EVERY(ROW(c3, c2) IS DISTINCT FROM ROW(c2, c3)) AS c1
                      FROM row_tbl
                      GROUP BY id"""


class aggtst_row_every_distinct(TstView):
    def __init__(self):
        # checked manually
        self.data = [{"c1": False}]
        self.sql = """CREATE MATERIALIZED VIEW row_every_distinct AS SELECT
                      EVERY(DISTINCT ROW(c3, c2) IS DISTINCT FROM ROW(c3, c2)) AS c1
                      FROM row_tbl"""


class aggtst_row_every_distinct_gby(TstView):
    def __init__(self):
        # checked manually
        self.data = [
            {"id": 0, "c1": False},
            {"id": 1, "c1": False},
        ]
        self.sql = """CREATE MATERIALIZED VIEW row_every_distinct_gby AS SELECT
                      id, EVERY(DISTINCT ROW(c3, c2) IS DISTINCT FROM ROW(c3, c2)) AS c1
                      FROM row_tbl
                      GROUP BY id"""


class aggtst_row_every_where(TstView):
    def __init__(self):
        # checked manually: the only row with a NULL c2 has c3 = 'adios', and
        # ROW('adios', NULL) IS NOT DISTINCT FROM ROW(NULL, 'adios') is false
        self.data = [{"c1": False}]
        self.sql = """CREATE MATERIALIZED VIEW row_every_where AS SELECT
                      EVERY(ROW(c3, c2) IS NOT DISTINCT FROM ROW(c2, c3)) FILTER(WHERE c2 IS NULL) AS c1
                      FROM row_tbl"""


class aggtst_row_every_where_groupby(TstView):
    def __init__(self):
        # checked manually: no row of group 1 passes the filter, so its aggregate is NULL
        self.data = [
            {"id": 0, "c1": False},
            {"id": 1, "c1": None},
        ]
        self.sql = """CREATE MATERIALIZED VIEW row_every_where_gby AS SELECT
                      id, EVERY(ROW(c3, c2) IS NOT DISTINCT FROM ROW(c2, c3)) FILTER(WHERE c2 IS NULL) AS c1
                      FROM row_tbl
                      GROUP BY id"""
