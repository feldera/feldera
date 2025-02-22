from tests.aggregate_tests.aggtst_base import TstView


class aggtst_interval_count(TstView):
    def __init__(self):
        # checked manually
        self.data = [{"count": 5}]
        self.sql = """CREATE MATERIALIZED VIEW interval_count AS SELECT
                      COUNT(*) AS count
                      FROM atbl_interval_seconds"""


class aggtst_interval_count_groupby(TstView):
    def __init__(self):
        # checked manually
        self.data = [{"id": 0, "count": 2}, {"id": 1, "count": 3}]
        self.sql = """CREATE MATERIALIZED VIEW interval_count_gby AS SELECT
                      id, COUNT(*) AS count
                      FROM atbl_interval_seconds
                      GROUP BY id"""


class aggtst_interval_count_where(TstView):
    def __init__(self):
        # checked manually
        self.data = [{"count": 2}]
        self.sql = """CREATE MATERIALIZED VIEW interval_count_where AS SELECT
                      COUNT(*) FILTER(WHERE c2_minus_c1 > c1_minus_c2) AS count
                      FROM atbl_interval_seconds"""


class aggtst_interval_count_where_groupby(TstView):
    def __init__(self):
        # checked manually
        self.data = [{"id": 0, "count": 1}, {"id": 1, "count": 1}]
        self.sql = """CREATE MATERIALIZED VIEW interval_count_where_gby AS SELECT
                      id, COUNT(*) FILTER(WHERE c2_minus_c1 > c1_minus_c2) AS count
                      FROM atbl_interval_seconds
                      GROUP BY id"""
