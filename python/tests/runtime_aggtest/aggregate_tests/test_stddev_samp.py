from tests.runtime_aggtest.aggtst_base import TstView


class aggtst_int_stddev(TstView):
    def __init__(self):
        # Validated on Postgres
        self.data = [
            {
                "c1": 1,
                "c2": 1,
                "c3": None,
                "c4": 1,
                "c5": 2,
                "c6": 2,
                "c7": None,
                "c8": 4,
            }
        ]
        self.sql = """CREATE MATERIALIZED VIEW int_stddev_samp AS SELECT
                      STDDEV_SAMP(c1) AS c1, STDDEV_SAMP(c2) AS c2, STDDEV_SAMP(c3) AS c3, STDDEV_SAMP(c4) AS c4, STDDEV_SAMP(c5) AS c5, STDDEV_SAMP(c6) AS c6, STDDEV_SAMP(c7) AS c7, STDDEV_SAMP(c8) AS c8
                      FROM stddev_tbl"""


class aggtst_int_stddev_groupby(TstView):
    def __init__(self):
        # Validated on Postgres
        self.data = [
            {
                "id": 0,
                "c1": None,
                "c2": 0,
                "c3": None,
                "c4": 1,
                "c5": 1,
                "c6": 1,
                "c7": None,
                "c8": 3,
            },
            {
                "id": 1,
                "c1": None,
                "c2": 1,
                "c3": 1,
                "c4": 2,
                "c5": 0,
                "c6": 1,
                "c7": None,
                "c8": 2,
            },
        ]
        self.sql = """CREATE MATERIALIZED VIEW int_stddev_samp_gby AS SELECT
                      id, STDDEV_SAMP(c1) AS c1, STDDEV_SAMP(c2) AS c2, STDDEV_SAMP(c3) AS c3, STDDEV_SAMP(c4) AS c4, STDDEV_SAMP(c5) AS c5, STDDEV_SAMP(c6) AS c6, STDDEV_SAMP(c7) AS c7, STDDEV_SAMP(c8) AS c8
                      FROM int0_tbl
                      GROUP BY id"""


class aggtst_int_stddev_distinct(TstView):
    def __init__(self):
        # Validated on Postgres
        self.data = [
            {"c1": 1, "c2": 1, "c3": 1, "c4": 2, "c5": 1, "c6": 2, "c7": 1, "c8": 2}
        ]
        self.sql = """CREATE MATERIALIZED VIEW int_stddev_samp_distinct AS SELECT
                      STDDEV_SAMP(DISTINCT c1) AS c1, STDDEV_SAMP(DISTINCT c2) AS c2, STDDEV_SAMP(DISTINCT c3) AS c3, STDDEV_SAMP(DISTINCT c4) AS c4, STDDEV_SAMP(DISTINCT c5) AS c5, STDDEV_SAMP(DISTINCT c6) AS c6, STDDEV_SAMP(DISTINCT c7) AS c7, STDDEV_SAMP(DISTINCT c8) AS c8
                      FROM int0_tbl"""


class aggtst_int_stddev_distinct_groupby(TstView):
    def __init__(self):
        # Validated on Postgres
        self.data = [
            {
                "id": 0,
                "c1": None,
                "c2": None,
                "c3": None,
                "c4": 1,
                "c5": 1,
                "c6": 1,
                "c7": None,
                "c8": 3,
            },
            {
                "id": 1,
                "c1": None,
                "c2": 1,
                "c3": 1,
                "c4": 2,
                "c5": None,
                "c6": 1,
                "c7": None,
                "c8": 2,
            },
        ]
        self.sql = """CREATE MATERIALIZED VIEW int_stddev_samp_distinct_gby AS SELECT
                      id, STDDEV_SAMP(DISTINCT c1) AS c1, STDDEV_SAMP(DISTINCT c2) AS c2, STDDEV_SAMP(DISTINCT c3) AS c3, STDDEV_SAMP(DISTINCT c4) AS c4, STDDEV_SAMP(DISTINCT c5) AS c5, STDDEV_SAMP(DISTINCT c6) AS c6, STDDEV_SAMP(DISTINCT c7) AS c7, STDDEV_SAMP(DISTINCT c8) AS c8
                      FROM int0_tbl
                      GROUP BY id"""


class aggtst_int_stddev_where(TstView):
    def __init__(self):
        # Validated on Postgres
        self.data = [
            {
                "c1": 1,
                "c2": 1,
                "c3": None,
                "c4": 1,
                "c5": 2,
                "c6": 2,
                "c7": None,
                "c8": 4,
            }
        ]
        self.sql = """CREATE MATERIALIZED VIEW int_stddev_where AS SELECT
                      STDDEV_SAMP(c1) FILTER (WHERE c1 > 0) AS c1, STDDEV_SAMP(c2) FILTER (WHERE c1 > 0) AS c2, STDDEV_SAMP(c3) FILTER (WHERE c1 > 0) AS c3, STDDEV_SAMP(c4) FILTER (WHERE c1 > 0) AS c4, STDDEV_SAMP(c5) FILTER (WHERE c1 > 0) AS c5, STDDEV_SAMP(c6) FILTER (WHERE c1 > 0) AS c6, STDDEV_SAMP(c7) FILTER (WHERE c1 > 0) AS c7, STDDEV_SAMP(c8) FILTER (WHERE c1 > 0) AS c8
                      FROM int0_tbl"""


class aggtst_int_stddev_where_groupby1(TstView):
    def __init__(self):
        # Validated on Postgres
        self.data = [
            {
                "id": 0,
                "c1": None,
                "c2": 0,
                "c3": None,
                "c4": 1,
                "c5": 1,
                "c6": 1,
                "c7": None,
                "c8": 3,
            },
            {
                "id": 1,
                "c1": None,
                "c2": None,
                "c3": None,
                "c4": None,
                "c5": None,
                "c6": None,
                "c7": None,
                "c8": None,
            },
        ]
        self.sql = """CREATE MATERIALIZED VIEW int_stddev_where_gby1 AS SELECT
                      id, STDDEV_SAMP(c1) FILTER (WHERE c8>2) AS c1, STDDEV_SAMP(c2) FILTER (WHERE c8>2) AS c2, STDDEV_SAMP(c3) FILTER (WHERE c8>2) AS c3, STDDEV_SAMP(c4) FILTER (WHERE c8>2) AS c4, STDDEV_SAMP(c5) FILTER (WHERE c8>2) AS c5, STDDEV_SAMP(c6) FILTER (WHERE c8>2) AS c6, STDDEV_SAMP(c7) FILTER (WHERE c8>2) AS c7, STDDEV_SAMP(c8) FILTER (WHERE c8>2) AS c8
                      FROM int0_tbl
                      GROUP BY id"""
