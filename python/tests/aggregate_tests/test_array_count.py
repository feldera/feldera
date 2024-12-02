from .aggtst_base import TstView


class aggtst_array_count(TstView):
    def __init__(self):
        # Validated on Postgres
        self.data = [{"count": 4}]
        self.sql = """CREATE MATERIALIZED VIEW array_count AS SELECT
                      COUNT(*) AS count
                      FROM array_tbl"""


class aggtst_array_count_groupby(TstView):
    def __init__(self):
        # Validated on Postgres
        self.data = [{"id": 0, "count": 2}, {"id": 1, "count": 2}]
        self.sql = """CREATE MATERIALIZED VIEW array_count_gby AS SELECT
                      id, COUNT(*) AS count
                      FROM array_tbl
                      GROUP BY id"""


class aggtst_array_count_where(TstView):
    def __init__(self):
        # Validated on Postgres
        self.data = [{"count": 2}]
        self.sql = """CREATE MATERIALIZED VIEW array_count_where AS SELECT
                      COUNT(*) FILTER(WHERE c1 < c2) AS count
                      FROM array_tbl"""


class aggtst_array_count_where_groupby(TstView):
    def __init__(self):
        # Validated on Postgres
        self.data = [{"id": 0, "count": 1}, {"id": 1, "count": 1}]
        self.sql = """CREATE MATERIALIZED VIEW array_count_where_gby AS SELECT
                      id, COUNT(*) FILTER(WHERE c1 < c2) AS count
                      FROM array_tbl
                      GROUP BY id"""
