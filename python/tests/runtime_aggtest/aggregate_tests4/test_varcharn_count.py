from tests.runtime_aggtest.aggtst_base import TstView


class aggtst_varcharn_count(TstView):
    def __init__(self):
        # Validated on Postgres
        self.data = [{"count": 4}]
        self.sql = """CREATE MATERIALIZED VIEW varcharn_count AS SELECT
                      COUNT(*) AS count
                      FROM atbl_varcharn"""


class aggtst_varcharn_count_gby(TstView):
    def __init__(self):
        # Validated on Postgres
        self.data = [{"id": 0, "count": 2}, {"id": 1, "count": 2}]
        self.sql = """CREATE MATERIALIZED VIEW varcharn_count_gby AS SELECT
                      id, COUNT(*) AS count
                      FROM atbl_varcharn
                      GROUP BY id"""


class aggtst_varcharn_count_where(TstView):
    def __init__(self):
        # Validated on Postgres
        self.data = [{"count": 2}]
        self.sql = """CREATE MATERIALIZED VIEW varcharn_count_where AS SELECT
                      COUNT(*) FILTER(WHERE len(f_c1)>4) AS count
                      FROM atbl_varcharn"""


class aggtst_varcharn_count_where_groupby(TstView):
    def __init__(self):
        # Validated on Postgres
        self.data = [{"id": 0, "count": 1}, {"id": 1, "count": 1}]
        self.sql = """CREATE MATERIALIZED VIEW varcharn_count_where_gby AS SELECT
                      id, COUNT(*) FILTER(WHERE len(f_c1)>4) AS count
                      FROM atbl_varcharn
                      GROUP BY id"""
