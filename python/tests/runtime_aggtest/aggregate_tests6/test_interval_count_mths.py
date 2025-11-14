from tests.runtime_aggtest.aggtst_base import TstView


class aggtst_interval_count_mths(TstView):
    def __init__(self):
        # checked manually
        self.data = [{"count": 5}]
        self.sql = """CREATE MATERIALIZED VIEW interval_count_mths AS SELECT
                      COUNT(*) AS count
                      FROM atbl_interval_months"""


class aggtst_interval_count_groupby_mths(TstView):
    def __init__(self):
        # checked manually
        self.data = [{"id": 0, "count": 2}, {"id": 1, "count": 3}]
        self.sql = """CREATE MATERIALIZED VIEW interval_count_gby_mths AS SELECT
                      id, COUNT(*) AS count
                      FROM atbl_interval_months
                      GROUP BY id"""


class aggtst_interval_count_where_mths(TstView):
    def __init__(self):
        # checked manually
        self.data = [{"count": 2}]
        self.sql = """CREATE MATERIALIZED VIEW interval_count_where_mths AS SELECT
                      COUNT(*) FILTER(WHERE c2_minus_c1 > c1_minus_c2) AS count
                      FROM atbl_interval_months"""


class aggtst_interval_count_where_groupby_mths(TstView):
    def __init__(self):
        # checked manually
        self.data = [{"id": 0, "count": 1}, {"id": 1, "count": 1}]
        self.sql = """CREATE MATERIALIZED VIEW interval_count_where_gby_mths AS SELECT
                      id, COUNT(*) FILTER(WHERE c2_minus_c1 > c1_minus_c2) AS count
                      FROM atbl_interval_months
                      GROUP BY id"""
