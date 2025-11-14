from tests.runtime_aggtest.aggtst_base import TstView


class aggtst_interval_months_some(TstView):
    def __init__(self):
        # Validated on Postgres
        self.data = [{"f_c1": True, "f_c3": True, "f_c5": True}]
        self.sql = """CREATE MATERIALIZED VIEW interval_months_some AS SELECT
                      SOME(c1_minus_c2 > c2_minus_c1) AS f_c1,
                      SOME(c1_minus_c3 > c3_minus_c1) AS f_c3,
                      SOME(c2_minus_c3 > c3_minus_c2) AS f_c5
                      FROM atbl_interval_months"""


class aggtst_interval_months_some_gby(TstView):
    def __init__(self):
        # Validated on Postgres
        self.data = [
            {"id": 0, "f_c1": True, "f_c3": False, "f_c5": True},
            {"id": 1, "f_c1": True, "f_c3": True, "f_c5": True},
        ]
        self.sql = """CREATE MATERIALIZED VIEW interval_months_some_gby AS SELECT
                      id,
                      SOME(c1_minus_c2 > c2_minus_c1) AS f_c1,
                      SOME(c1_minus_c3 > c3_minus_c1) AS f_c3,
                      SOME(c2_minus_c3 > c3_minus_c2) AS f_c5
                      FROM atbl_interval_months
                      GROUP BY id"""


class aggtst_interval_months_some_distinct(TstView):
    def __init__(self):
        # Validated on Postgres
        self.data = [{"f_c1": True, "f_c3": True, "f_c5": True}]
        self.sql = """CREATE MATERIALIZED VIEW interval_months_some_distinct AS SELECT
                      SOME(DISTINCT c1_minus_c2 > c2_minus_c1) AS f_c1,
                      SOME(DISTINCT c1_minus_c3 > c3_minus_c1) AS f_c3,
                      SOME(DISTINCT c2_minus_c3 > c3_minus_c2) AS f_c5
                      FROM atbl_interval_months"""


class aggtst_interval_months_some_distinct_gby(TstView):
    def __init__(self):
        # Validated on Postgres
        self.data = [
            {"id": 0, "f_c1": True, "f_c3": False, "f_c5": True},
            {"id": 1, "f_c1": True, "f_c3": True, "f_c5": True},
        ]
        self.sql = """CREATE MATERIALIZED VIEW interval_months_some_distinct_gby AS SELECT
                      id,
                      SOME(DISTINCT c1_minus_c2 > c2_minus_c1) AS f_c1,
                      SOME(DISTINCT c1_minus_c3 > c3_minus_c1) AS f_c3,
                      SOME(DISTINCT c2_minus_c3 > c3_minus_c2) AS f_c5
                      FROM atbl_interval_months
                      GROUP BY id"""


class aggtst_interval_months_some_where(TstView):
    def __init__(self):
        # checked manually
        self.data = [{"f_c1": True, "f_c3": False, "f_c5": False}]
        self.sql = """CREATE MATERIALIZED VIEW interval_months_some_where AS SELECT
                      SOME(c1_minus_c2 > c2_minus_c1) FILTER(WHERE c1_minus_c2::BIGINT > +121) AS f_c1,
                      SOME(c1_minus_c3 > c3_minus_c1) FILTER(WHERE c1_minus_c2::BIGINT > +121) AS f_c3,
                      SOME(c2_minus_c3 > c3_minus_c2) FILTER(WHERE c1_minus_c2::BIGINT > +121) AS f_c5
                      FROM atbl_interval_months"""


class aggtst_interval_months_some_where_groupby(TstView):
    def __init__(self):
        # checked manually
        self.data = [
            {"id": 0, "f_c1": True, "f_c3": False, "f_c5": False},
            {"id": 1, "f_c1": True, "f_c3": False, "f_c5": False},
        ]
        self.sql = """CREATE MATERIALIZED VIEW interval_months_some_where_gby AS SELECT
                      id,
                      SOME(c1_minus_c2 > c2_minus_c1) FILTER(WHERE c1_minus_c2::BIGINT > +121) AS f_c1,
                      SOME(c1_minus_c3 > c3_minus_c1) FILTER(WHERE c1_minus_c2::BIGINT > +121) AS f_c3,
                      SOME(c2_minus_c3 > c3_minus_c2) FILTER(WHERE c1_minus_c2::BIGINT > +121) AS f_c5
                      FROM atbl_interval_months
                      GROUP BY id"""
