from tests.runtime_aggtest.aggtst_base import TstView


class aggtst_decimal_count(TstView):
    def __init__(self):
        # Validated on Postgres
        self.data = [{"count": 4}]
        self.sql = """CREATE MATERIALIZED VIEW decimal_count AS SELECT
                      COUNT(*) AS count
                      FROM decimal_tbl"""


class aggtst_decimal_count_groupby(TstView):
    def __init__(self):
        # Validated on Postgres
        self.data = [{"id": 0, "count": 2}, {"id": 1, "count": 2}]
        self.sql = """CREATE MATERIALIZED VIEW decimal_count_gby AS SELECT
                      id, COUNT(*) AS count
                      FROM decimal_tbl
                      GROUP BY id"""


class aggtst_decimal_count_where(TstView):
    def __init__(self):
        # Validated on Postgres
        self.data = [{"count": 3}]
        self.sql = """CREATE MATERIALIZED VIEW decimal_count_where AS SELECT
                      COUNT(*) FILTER(WHERE c2!= 2231.90) AS count
                      FROM decimal_tbl"""


class aggtst_decimal_count_where_groupby(TstView):
    def __init__(self):
        # Validated on Postgres
        self.data = [{"id": 0, "count": 1}, {"id": 1, "count": 2}]
        self.sql = """CREATE MATERIALIZED VIEW decimal_count_where_gby AS SELECT
                      id, COUNT(*) FILTER(WHERE c2!= 2231.90) AS count
                      FROM decimal_tbl
                      GROUP BY id"""
