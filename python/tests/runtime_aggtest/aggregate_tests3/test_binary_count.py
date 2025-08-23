from tests.runtime_aggtest.aggtst_base import TstView


class aggtst_binary_count(TstView):
    def __init__(self):
        # Validated on Postgres
        self.data = [{"count": 4}]
        self.sql = """CREATE MATERIALIZED VIEW binary_count AS SELECT
                      COUNT(*) AS count
                      FROM binary_tbl"""


class aggtst_binary_count_groupby(TstView):
    def __init__(self):
        # Validated on Postgres
        self.data = [{"id": 0, "count": 2}, {"id": 1, "count": 2}]
        self.sql = """CREATE MATERIALIZED VIEW binary_count_gby AS SELECT
                      id, COUNT(*) AS count
                      FROM binary_tbl
                      GROUP BY id"""


class aggtst_binary_count_where(TstView):
    def __init__(self):
        # Validated on Postgres
        self.data = [{"count": 2}]
        self.sql = """CREATE MATERIALIZED VIEW binary_count_where AS SELECT
                      COUNT(*) FILTER(WHERE c1 < c2) AS count
                      FROM binary_tbl"""


class aggtst_binary_count_where_groupby(TstView):
    def __init__(self):
        # Validated on Postgres
        self.data = [{"id": 0, "count": 1}, {"id": 1, "count": 1}]
        self.sql = """CREATE MATERIALIZED VIEW binary_count_where_gby AS SELECT
                      id, COUNT(*) FILTER(WHERE c1 < c2) AS count
                      FROM binary_tbl
                      GROUP BY id"""
