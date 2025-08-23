from tests.runtime_aggtest.aggtst_base import TstView


class aggtst_int_count(TstView):
    def __init__(self):
        # Validated on Postgres
        self.data = [{"count": 4}]
        self.sql = """CREATE MATERIALIZED VIEW int_count AS SELECT
                      COUNT(*) AS count
                      FROM int0_tbl"""


class aggtst_int_count_groupby(TstView):
    def __init__(self):
        # Validated on Postgres
        self.data = [{"id": 0, "count": 2}, {"id": 1, "count": 2}]
        self.sql = """CREATE MATERIALIZED VIEW int_count_gby AS SELECT
                      id, COUNT(*) AS count
                      FROM int0_tbl
                      GROUP BY id"""


class aggtst_int_count_where(TstView):
    def __init__(self):
        # Validated on Postgres
        self.data = [{"count": 3}]
        self.sql = """CREATE MATERIALIZED VIEW int_count_where AS SELECT
                      COUNT(*) FILTER(WHERE (c5+C6)> 3) AS count
                      FROM int0_tbl"""


class aggtst_int_count_where_groupby(TstView):
    def __init__(self):
        # Validated on Postgres
        self.data = [{"id": 0, "count": 2}, {"id": 1, "count": 1}]
        self.sql = """CREATE MATERIALIZED VIEW int_count_where_gby AS SELECT
                      id, COUNT(*) FILTER(WHERE (c5+C6)> 3) AS count
                      FROM int0_tbl
                      GROUP BY id"""
