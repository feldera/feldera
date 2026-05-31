from tests.runtime_aggtest.aggtst_base import TstView


class aggtst_float_count(TstView):
    def __init__(self):
        # checked manually
        self.data = [{"count": 4}]
        self.sql = """CREATE MATERIALIZED VIEW float_count AS SELECT
                      COUNT(*) AS count
                      FROM float_tbl"""


class aggtst_float_count_groupby(TstView):
    def __init__(self):
        # checked manually
        self.data = [{"id": 0, "count": 2}, {"id": 1, "count": 2}]
        self.sql = """CREATE MATERIALIZED VIEW float_count_gby AS SELECT
                      id, COUNT(*) AS count
                      FROM float_tbl
                      GROUP BY id"""


class aggtst_float_count_where(TstView):
    def __init__(self):
        # checked manually
        self.data = [{"count": 1}]
        self.sql = """CREATE MATERIALIZED VIEW float_count_where AS SELECT
                      COUNT(*) FILTER(WHERE c1 IS NULL) AS count
                      FROM float_tbl"""


class aggtst_float_count_where_groupby(TstView):
    def __init__(self):
        # checked manually
        self.data = [{"id": 0, "count": 1}, {"id": 1, "count": 0}]
        self.sql = """CREATE MATERIALIZED VIEW float_count_where_gby AS SELECT
                      id, COUNT(*) FILTER(WHERE c1 IS NULL) AS count
                      FROM float_tbl
                      GROUP BY id"""
