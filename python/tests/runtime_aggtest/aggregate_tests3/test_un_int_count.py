from tests.runtime_aggtest.aggtst_base import TstView


class aggtst_un_int_count(TstView):
    def __init__(self):
        # Validated on MySQL
        self.data = [{"count": 4}]
        self.sql = """CREATE MATERIALIZED VIEW un_int_count AS SELECT
                      COUNT(*) AS count
                      FROM un_int_tbl"""


class aggtst_un_int_count_groupby(TstView):
    def __init__(self):
        # Validated on MySQL
        self.data = [{"id": 0, "count": 2}, {"id": 1, "count": 2}]
        self.sql = """CREATE MATERIALIZED VIEW un_int_count_gby AS SELECT
                      id, COUNT(*) AS count
                      FROM un_int_tbl
                      GROUP BY id"""


class aggtst_un_int_count_where(TstView):
    def __init__(self):
        # Validated on MySQL
        self.data = [{"count": 3}]
        self.sql = """CREATE MATERIALIZED VIEW un_int_count_where AS SELECT
                      COUNT(*) FILTER(WHERE c2 > 45) AS count
                      FROM un_int_tbl"""


class aggtst_un_int_count_where_groupby(TstView):
    def __init__(self):
        # Validated on MySQL
        self.data = [{"id": 0, "count": 1}, {"id": 1, "count": 2}]
        self.sql = """CREATE MATERIALIZED VIEW un_int_count_where_gby AS SELECT
                      id, COUNT(*) FILTER(WHERE c2 > 45) AS count
                      FROM un_int_tbl
                      GROUP BY id"""
