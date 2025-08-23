from tests.runtime_aggtest.aggtst_base import TstView


class aggtst_interval_every(TstView):
    def __init__(self):
        # Validated on Postgres
        self.data = [{"f_c1": False, "f_c3": False, "f_c5": False}]
        self.sql = """CREATE MATERIALIZED VIEW interval_every AS SELECT
                      EVERY(c1_minus_c2 > c2_minus_c1) AS f_c1,
                      EVERY(c1_minus_c3 < c3_minus_c1) AS f_c3,
                      EVERY(c2_minus_c3 > c3_minus_c2) AS f_c5
                      FROM atbl_interval_seconds"""


class aggtst_interval_every_gby(TstView):
    def __init__(self):
        # Validated on Postgres
        self.data = [
            {"id": 0, "f_c1": False, "f_c3": True, "f_c5": False},
            {"id": 1, "f_c1": False, "f_c3": False, "f_c5": False},
        ]
        self.sql = """CREATE MATERIALIZED VIEW interval_every_gby AS SELECT
                      id,
                      EVERY(c1_minus_c2 > c2_minus_c1) AS f_c1,
                      EVERY(c1_minus_c3 < c3_minus_c1) AS f_c3,
                      EVERY(c2_minus_c3 > c3_minus_c2) AS f_c5
                      FROM atbl_interval_seconds
                      GROUP BY id"""


class aggtst_interval_every_distinct(TstView):
    def __init__(self):
        # Validated on Postgres
        self.data = [{"f_c1": False, "f_c3": False, "f_c5": False}]
        self.sql = """CREATE MATERIALIZED VIEW interval_every_distinct AS SELECT
                      EVERY(DISTINCT c1_minus_c2 > c2_minus_c1) AS f_c1,
                      EVERY(DISTINCT c1_minus_c3 < c3_minus_c1) AS f_c3,
                      EVERY(DISTINCT c2_minus_c3 > c3_minus_c2) AS f_c5
                      FROM atbl_interval_seconds"""


class aggtst_interval_every_distinct_gby(TstView):
    def __init__(self):
        # Validated on Postgres
        self.data = [
            {"id": 0, "f_c1": False, "f_c3": True, "f_c5": False},
            {"id": 1, "f_c1": False, "f_c3": False, "f_c5": False},
        ]
        self.sql = """CREATE MATERIALIZED VIEW interval_every_distinct_gby AS SELECT
                      id,
                      EVERY(DISTINCT c1_minus_c2 > c2_minus_c1) AS f_c1,
                      EVERY(DISTINCT c1_minus_c3 < c3_minus_c1) AS f_c3,
                      EVERY(DISTINCT c2_minus_c3 > c3_minus_c2) AS f_c5
                      FROM atbl_interval_seconds
                      GROUP BY id"""


class aggtst_interval_every_where(TstView):
    def __init__(self):
        # checked manually
        self.data = [{"f_c1": True, "f_c3": True, "f_c5": False}]
        self.sql = """CREATE MATERIALIZED VIEW interval_every_where AS SELECT
                      EVERY(c1_minus_c2 > c2_minus_c1) FILTER(WHERE c1_minus_c2::BIGINT > 318185100) AS f_c1,
                      EVERY(c1_minus_c3 < c3_minus_c1) FILTER(WHERE c1_minus_c2::BIGINT > 318185100) AS f_c3,
                      EVERY(c2_minus_c3 > c3_minus_c2) FILTER(WHERE c1_minus_c2::BIGINT > 318185100) AS f_c5
                      FROM atbl_interval_seconds"""


class aggtst_interval_every_where_groupby(TstView):
    def __init__(self):
        # checked manually
        self.data = [
            {"id": 0, "f_c1": True, "f_c3": True, "f_c5": False},
            {"id": 1, "f_c1": True, "f_c3": True, "f_c5": False},
        ]
        self.sql = """CREATE MATERIALIZED VIEW interval_every_where_gby AS SELECT
                      id,
                      EVERY(c1_minus_c2 > c2_minus_c1) FILTER(WHERE c1_minus_c2::BIGINT > 318185100) AS f_c1,
                      EVERY(c1_minus_c3 < c3_minus_c1) FILTER(WHERE c1_minus_c2::BIGINT > 318185100) AS f_c3,
                      EVERY(c2_minus_c3 > c3_minus_c2) FILTER(WHERE c1_minus_c2::BIGINT > 318185100) AS f_c5
                      FROM atbl_interval_seconds
                      GROUP BY id"""
