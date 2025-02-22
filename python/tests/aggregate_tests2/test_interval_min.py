from tests.aggregate_tests.aggtst_base import TstView


class aggtst_interval_min(TstView):
    def __init__(self):
        # Result validation is not required for local views
        self.data = []
        self.sql = """CREATE LOCAL VIEW interval_min AS SELECT
                      MIN(c1_minus_c2) AS f_c1,
                      MIN(c2_minus_c1) AS f_c2,
                      MIN(c1_minus_c3) AS f_c3,
                      MIN(c3_minus_c1) AS f_c4,
                      MIN(c2_minus_c3) AS f_c5,
                      MIN(c3_minus_c2) AS f_c6
                      FROM atbl_interval_seconds"""


class aggtst_interval_min_seconds(TstView):
    def __init__(self):
        # Validated on Postgres
        self.data = [
            {
                "m_c1": -1466619540,
                "m_c2": -1592695620,
                "m_c3": -571691580,
                "m_c4": -229712400,
                "m_c5": -1681851120,
                "m_c6": -894927960,
            }
        ]
        self.sql = """CREATE MATERIALIZED VIEW interval_min_seconds AS SELECT
                      TIMESTAMPDIFF(SECOND, d(), d() + f_c1) AS m_c1,
                      TIMESTAMPDIFF(SECOND, d(), d() + f_c2) AS m_c2,
                      TIMESTAMPDIFF(SECOND, d(), d() + f_c3) AS m_c3,
                      TIMESTAMPDIFF(SECOND, d(), d() + f_c4) AS m_c4,
                      TIMESTAMPDIFF(SECOND, d(), d() + f_c5) AS m_c5,
                      TIMESTAMPDIFF(SECOND, d(), d() + f_c6) AS m_c6
                      FROM interval_min"""


class aggtst_interval_min_gby(TstView):
    def __init__(self):
        # Result validation is not required for local views
        self.data = []
        self.sql = """CREATE LOCAL VIEW interval_min_gby AS SELECT
                      id,
                      MIN(c1_minus_c2) AS f_c1,
                      MIN(c2_minus_c1) AS f_c2,
                      MIN(c1_minus_c3) AS f_c3,
                      MIN(c3_minus_c1) AS f_c4,
                      MIN(c2_minus_c3) AS f_c5,
                      MIN(c3_minus_c2) AS f_c6
                      FROM atbl_interval_seconds
                      GROUP BY id"""


class aggtst_interval_min_gby_seconds(TstView):
    def __init__(self):
        # Validated on Postgres
        self.data = [
            {
                "id": 0,
                "m_c1": -318226680,
                "m_c2": -1592695620,
                "m_c3": -169024440,
                "m_c4": 89155500,
                "m_c5": -1681851120,
                "m_c6": -149202240,
            },
            {
                "id": 1,
                "m_c1": -1466619540,
                "m_c2": -1592695620,
                "m_c3": -571691580,
                "m_c4": -229712400,
                "m_c5": -1635868800,
                "m_c6": -894927960,
            },
        ]
        self.sql = """CREATE MATERIALIZED VIEW interval_min_gby_seconds AS SELECT
                      id,
                      TIMESTAMPDIFF(SECOND, d(), d() + f_c1) AS m_c1,
                      TIMESTAMPDIFF(SECOND, d(), d() + f_c2) AS m_c2,
                      TIMESTAMPDIFF(SECOND, d(), d() + f_c3) AS m_c3,
                      TIMESTAMPDIFF(SECOND, d(), d() + f_c4) AS m_c4,
                      TIMESTAMPDIFF(SECOND, d(), d() + f_c5) AS m_c5,
                      TIMESTAMPDIFF(SECOND, d(), d() + f_c6) AS m_c6
                      FROM interval_min_gby"""


class aggtst_interval_min_distinct(TstView):
    def __init__(self):
        # Result validation is not required for local views
        self.data = []
        self.sql = """CREATE LOCAL VIEW interval_min_distinct AS SELECT
                      MIN(DISTINCT c1_minus_c2) AS f_c1,
                      MIN(DISTINCT c2_minus_c1) AS f_c2,
                      MIN(DISTINCT c1_minus_c3) AS f_c3,
                      MIN(DISTINCT c3_minus_c1) AS f_c4,
                      MIN(DISTINCT c2_minus_c3) AS f_c5,
                      MIN(DISTINCT c3_minus_c2) AS f_c6
                      FROM atbl_interval_seconds"""


class aggtst_interval_min_distinct_seconds(TstView):
    def __init__(self):
        # Validated on Postgres
        self.data = [
            {
                "m_c1": -1466619540,
                "m_c2": -1592695620,
                "m_c3": -571691580,
                "m_c4": -229712400,
                "m_c5": -1681851120,
                "m_c6": -894927960,
            }
        ]
        self.sql = """CREATE MATERIALIZED VIEW interval_min_distinct_seconds AS SELECT
                      TIMESTAMPDIFF(SECOND, d(), d() + f_c1) AS m_c1,
                      TIMESTAMPDIFF(SECOND, d(), d() + f_c2) AS m_c2,
                      TIMESTAMPDIFF(SECOND, d(), d() + f_c3) AS m_c3,
                      TIMESTAMPDIFF(SECOND, d(), d() + f_c4) AS m_c4,
                      TIMESTAMPDIFF(SECOND, d(), d() + f_c5) AS m_c5,
                      TIMESTAMPDIFF(SECOND, d(), d() + f_c6) AS m_c6
                      FROM interval_min_distinct"""


class aggtst_interval_min_distinct_gby(TstView):
    def __init__(self):
        # Result validation is not required for local views
        self.data = []
        self.sql = """CREATE LOCAL VIEW interval_min_distinct_gby AS SELECT
                      id,
                      MIN(DISTINCT c1_minus_c2) AS f_c1,
                      MIN(DISTINCT c2_minus_c1) AS f_c2,
                      MIN(DISTINCT c1_minus_c3) AS f_c3,
                      MIN(DISTINCT c3_minus_c1) AS f_c4,
                      MIN(DISTINCT c2_minus_c3) AS f_c5,
                      MIN(DISTINCT c3_minus_c2) AS f_c6
                      FROM atbl_interval_seconds
                      GROUP BY id"""


class aggtst_interval_min_distinct_gby_seconds(TstView):
    def __init__(self):
        # Validated on Postgres
        self.data = [
            {
                "id": 0,
                "m_c1": -318226680,
                "m_c2": -1592695620,
                "m_c3": -169024440,
                "m_c4": 89155500,
                "m_c5": -1681851120,
                "m_c6": -149202240,
            },
            {
                "id": 1,
                "m_c1": -1466619540,
                "m_c2": -1592695620,
                "m_c3": -571691580,
                "m_c4": -229712400,
                "m_c5": -1635868800,
                "m_c6": -894927960,
            },
        ]
        self.sql = """CREATE MATERIALIZED VIEW interval_min_distinct_gby_seconds AS SELECT
                      id,
                      TIMESTAMPDIFF(SECOND, d(), d() + f_c1) AS m_c1,
                      TIMESTAMPDIFF(SECOND, d(), d() + f_c2) AS m_c2,
                      TIMESTAMPDIFF(SECOND, d(), d() + f_c3) AS m_c3,
                      TIMESTAMPDIFF(SECOND, d(), d() + f_c4) AS m_c4,
                      TIMESTAMPDIFF(SECOND, d(), d() + f_c5) AS m_c5,
                      TIMESTAMPDIFF(SECOND, d(), d() + f_c6) AS m_c6
                      FROM interval_min_distinct_gby"""


class aggtst_interval_min_where(TstView):
    def __init__(self):
        # Result validation is not required for local views
        self.data = []
        self.sql = """CREATE LOCAL VIEW interval_min_where AS SELECT
                      MIN(c1_minus_c2) FILTER(WHERE c1_minus_c2 > c2_minus_c1) AS f_c1,
                      MIN(c2_minus_c1) FILTER(WHERE c1_minus_c2 > c2_minus_c1) AS f_c2,
                      MIN(c1_minus_c3) FILTER(WHERE c1_minus_c3 > c3_minus_c1) AS f_c3,
                      MIN(c3_minus_c1) FILTER(WHERE c1_minus_c3 > c3_minus_c1) AS f_c4,
                      MIN(c2_minus_c3) FILTER(WHERE c2_minus_c3 > c3_minus_c2) AS f_c5,
                      MIN(c3_minus_c2) FILTER(WHERE c2_minus_c3 > c3_minus_c2) AS f_c6
                      FROM atbl_interval_seconds"""


class aggtst_interval_min_where_seconds(TstView):
    def __init__(self):
        # Validated on Postgres
        self.data = [
            {
                "m_c1": 318185100,
                "m_c2": -1592695620,
                "m_c3": 229712400,
                "m_c4": -229712400,
                "m_c5": 149202240,
                "m_c6": -894927960,
            }
        ]
        self.sql = """CREATE MATERIALIZED VIEW interval_min_where_seconds AS SELECT
                      TIMESTAMPDIFF(SECOND, d(), d() + f_c1) AS m_c1,
                      TIMESTAMPDIFF(SECOND, d(), d() + f_c2) AS m_c2,
                      TIMESTAMPDIFF(SECOND, d(), d() + f_c3) AS m_c3,
                      TIMESTAMPDIFF(SECOND, d(), d() + f_c4) AS m_c4,
                      TIMESTAMPDIFF(SECOND, d(), d() + f_c5) AS m_c5,
                      TIMESTAMPDIFF(SECOND, d(), d() + f_c6) AS m_c6
                      FROM interval_min_where"""


class aggtst_interval_min_where_gby(TstView):
    def __init__(self):
        # Result validation is not required for local views
        self.data = []
        self.sql = """CREATE LOCAL VIEW interval_min_where_gby AS SELECT
                      id,
                      MIN(c1_minus_c2) FILTER(WHERE c1_minus_c2 > c2_minus_c1) AS f_c1,
                      MIN(c2_minus_c1) FILTER(WHERE c1_minus_c2 > c2_minus_c1) AS f_c2,
                      MIN(c1_minus_c3) FILTER(WHERE c1_minus_c3 > c3_minus_c1) AS f_c3,
                      MIN(c3_minus_c1) FILTER(WHERE c1_minus_c3 > c3_minus_c1) AS f_c4,
                      MIN(c2_minus_c3) FILTER(WHERE c2_minus_c3 > c3_minus_c2) AS f_c5,
                      MIN(c3_minus_c2) FILTER(WHERE c2_minus_c3 > c3_minus_c2) AS f_c6
                      FROM atbl_interval_seconds
                      GROUP BY id"""


class aggtst_interval_min_where_gby_seconds(TstView):
    def __init__(self):
        # Validated on Postgres
        self.data = [
            {
                "id": 0,
                "m_c1": 1592695620,
                "m_c2": -1592695620,
                "m_c3": None,
                "m_c4": None,
                "m_c5": 149202240,
                "m_c6": -149202240,
            },
            {
                "id": 1,
                "m_c1": 318185100,
                "m_c2": -1592695620,
                "m_c3": 229712400,
                "m_c4": -229712400,
                "m_c5": 894927960,
                "m_c6": -894927960,
            },
        ]
        self.sql = """CREATE MATERIALIZED VIEW interval_min_where_gby_seconds AS SELECT
                      id,
                      TIMESTAMPDIFF(SECOND, d(), d() + f_c1) AS m_c1,
                      TIMESTAMPDIFF(SECOND, d(), d() + f_c2) AS m_c2,
                      TIMESTAMPDIFF(SECOND, d(), d() + f_c3) AS m_c3,
                      TIMESTAMPDIFF(SECOND, d(), d() + f_c4) AS m_c4,
                      TIMESTAMPDIFF(SECOND, d(), d() + f_c5) AS m_c5,
                      TIMESTAMPDIFF(SECOND, d(), d() + f_c6) AS m_c6
                      FROM interval_min_where_gby"""
