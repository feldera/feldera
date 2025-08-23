from tests.runtime_aggtest.aggtst_base import TstView


class aggtst_interval_arg_max(TstView):
    def __init__(self):
        # Result validation is not required for local views
        self.data = []
        self.sql = """CREATE LOCAL VIEW interval_arg_max AS SELECT
                      ARG_MAX(c1_minus_c2, c2_minus_c1) AS f_c1,
                      ARG_MAX(c1_minus_c3, c3_minus_c1) AS f_c3,
                      ARG_MAX(c2_minus_c3, c3_minus_c2) AS f_c5
                      FROM atbl_interval_seconds"""


class aggtst_interval_arg_max_seconds(TstView):
    def __init__(self):
        # checked manually
        self.data = [{"m_c1": -1466619540, "m_c3": -571691580, "m_c5": -1681851120}]
        self.sql = """CREATE MATERIALIZED VIEW interval_arg_max_seconds AS SELECT
                      TIMESTAMPDIFF(SECOND, d(), d() + f_c1) AS m_c1,
                      TIMESTAMPDIFF(SECOND, d(), d() + f_c3) AS m_c3,
                      TIMESTAMPDIFF(SECOND, d(), d() + f_c5) AS m_c5
                      FROM interval_arg_max"""


class aggtst_interval_arg_max_gby(TstView):
    def __init__(self):
        # Result validation is not required for local views
        self.data = []
        self.sql = """CREATE LOCAL VIEW interval_arg_max_gby AS SELECT
                      id,
                      ARG_MAX(c1_minus_c2, c2_minus_c1) AS f_c1,
                      ARG_MAX(c1_minus_c3, c3_minus_c1) AS f_c3,
                      ARG_MAX(c2_minus_c3, c3_minus_c2) AS f_c5
                      FROM atbl_interval_seconds
                      GROUP BY id"""


class aggtst_interval_arg_max_gby_seconds(TstView):
    def __init__(self):
        # checked manually
        self.data = [
            {
                "id": 0,
                "m_c1": -318226680,
                "m_c3": -169024440,
                "m_c5": -1681851120,
            },
            {"id": 1, "m_c1": -1466619540, "m_c3": -571691580, "m_c5": -1635868800},
        ]
        self.sql = """CREATE MATERIALIZED VIEW interval_arg_max_gby_seconds AS SELECT
                      id,
                      TIMESTAMPDIFF(SECOND, d(), d() + f_c1) AS m_c1,
                      TIMESTAMPDIFF(SECOND, d(), d() + f_c3) AS m_c3,
                      TIMESTAMPDIFF(SECOND, d(), d() + f_c5) AS m_c5
                      FROM interval_arg_max_gby"""


class aggtst_interval_arg_max_distinct(TstView):
    def __init__(self):
        # Result validation is not required for local views
        self.data = []
        self.sql = """CREATE LOCAL VIEW interval_arg_max_distinct AS SELECT
                      ARG_MAX(DISTINCT c1_minus_c2, c2_minus_c1) AS f_c1,
                      ARG_MAX(DISTINCT c1_minus_c3, c3_minus_c1) AS f_c3,
                      ARG_MAX(DISTINCT c2_minus_c3, c3_minus_c2) AS f_c5
                      FROM atbl_interval_seconds"""


class aggtst_interval_arg_max_distinct_seconds(TstView):
    def __init__(self):
        # checked manually
        self.data = [{"m_c1": -1466619540, "m_c3": -571691580, "m_c5": -1681851120}]
        self.sql = """CREATE MATERIALIZED VIEW interval_arg_max_distinct_seconds AS SELECT
                      TIMESTAMPDIFF(SECOND, d(), d() + f_c1) AS m_c1,
                      TIMESTAMPDIFF(SECOND, d(), d() + f_c3) AS m_c3,
                      TIMESTAMPDIFF(SECOND, d(), d() + f_c5) AS m_c5
                      FROM interval_arg_max_distinct"""


class aggtst_interval_arg_max_distinct_gby(TstView):
    def __init__(self):
        # Result validation is not required for local views
        self.data = []
        self.sql = """CREATE LOCAL VIEW interval_arg_max_distinct_gby AS SELECT
                      id,
                      ARG_MAX(DISTINCT c1_minus_c2, c2_minus_c1) AS f_c1,
                      ARG_MAX(DISTINCT c1_minus_c3, c3_minus_c1) AS f_c3,
                      ARG_MAX(DISTINCT c2_minus_c3, c3_minus_c2) AS f_c5
                      FROM atbl_interval_seconds
                      GROUP BY id"""


class aggtst_interval_arg_max_distinct_gby_seconds(TstView):
    def __init__(self):
        # checked manually
        self.data = [
            {
                "id": 0,
                "m_c1": -318226680,
                "m_c3": -169024440,
                "m_c5": -1681851120,
            },
            {"id": 1, "m_c1": -1466619540, "m_c3": -571691580, "m_c5": -1635868800},
        ]
        self.sql = """CREATE MATERIALIZED VIEW interval_arg_max_distinct_gby_seconds AS SELECT
                      id,
                      TIMESTAMPDIFF(SECOND, d(), d() + f_c1) AS m_c1,
                      TIMESTAMPDIFF(SECOND, d(), d() + f_c3) AS m_c3,
                      TIMESTAMPDIFF(SECOND, d(), d() + f_c5) AS m_c5
                      FROM interval_arg_max_distinct_gby"""


class aggtst_interval_arg_max_where(TstView):
    def __init__(self):
        # Result validation is not required for local views
        self.data = []
        self.sql = """CREATE LOCAL VIEW interval_arg_max_where AS SELECT
                      ARG_MAX(c1_minus_c2, c2_minus_c1) FILTER(WHERE c1_minus_c2 > c2_minus_c1) AS f_c1,
                      ARG_MAX(c1_minus_c3, c3_minus_c1) FILTER(WHERE c1_minus_c3 > c3_minus_c1) AS f_c3,
                      ARG_MAX(c2_minus_c3, c3_minus_c2) FILTER(WHERE c2_minus_c3 > c3_minus_c2) AS f_c5
                      FROM atbl_interval_seconds"""


class aggtst_interval_arg_max_where_seconds(TstView):
    def __init__(self):
        # checked manually
        self.data = [{"m_c1": 318185100, "m_c3": 229712400, "m_c5": 149202240}]
        self.sql = """CREATE MATERIALIZED VIEW interval_arg_max_where_seconds AS SELECT
                      TIMESTAMPDIFF(SECOND, d(), d() + f_c1) AS m_c1,
                      TIMESTAMPDIFF(SECOND, d(), d() + f_c3) AS m_c3,
                      TIMESTAMPDIFF(SECOND, d(), d() + f_c5) AS m_c5
                      FROM interval_arg_max_where"""


class aggtst_interval_arg_max_where_gby(TstView):
    def __init__(self):
        # Result validation is not required for local views
        self.data = []
        self.sql = """CREATE LOCAL VIEW interval_arg_max_where_gby AS SELECT
                      id,
                      ARG_MAX(c1_minus_c2, c2_minus_c1) FILTER(WHERE c1_minus_c2 > c2_minus_c1) AS f_c1,
                      ARG_MAX(c1_minus_c3, c3_minus_c1) FILTER(WHERE c1_minus_c3 > c3_minus_c1) AS f_c3,
                      ARG_MAX(c2_minus_c3, c3_minus_c2) FILTER(WHERE c2_minus_c3 > c3_minus_c2) AS f_c5
                      FROM atbl_interval_seconds
                      GROUP BY id"""


class aggtst_interval_arg_max_where_gby_seconds(TstView):
    def __init__(self):
        # checked manually
        self.data = [
            {"id": 0, "m_c1": 1592695620, "m_c3": None, "m_c5": 149202240},
            {"id": 1, "m_c1": 318185100, "m_c3": 229712400, "m_c5": 894927960},
        ]
        self.sql = """CREATE MATERIALIZED VIEW interval_arg_max_where_gby_seconds AS SELECT
                      id,
                      TIMESTAMPDIFF(SECOND, d(), d() + f_c1) AS m_c1,
                      TIMESTAMPDIFF(SECOND, d(), d() + f_c3) AS m_c3,
                      TIMESTAMPDIFF(SECOND, d(), d() + f_c5) AS m_c5
                      FROM interval_arg_max_where_gby"""
