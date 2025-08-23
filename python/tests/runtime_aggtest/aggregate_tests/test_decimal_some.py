from tests.runtime_aggtest.aggtst_base import TstView


class aggtst_decimal_some(TstView):
    def __init__(self):
        # Validated on Postgres
        self.data = [{"c1": True, "c2": True}]
        self.sql = """CREATE MATERIALIZED VIEW decimal_some AS SELECT
                      SOME(c1 > 1111.52) AS c1, SOME(c2 > 3802.71) AS c2
                      FROM decimal_tbl"""


class aggtst_decimal_some_groupby(TstView):
    def __init__(self):
        # Validated on Postgres
        self.data = [
            {"id": 0, "c1": False, "c2": False},
            {"id": 1, "c1": True, "c2": True},
        ]
        self.sql = """CREATE MATERIALIZED VIEW decimal_some_gby AS SELECT
                      id, SOME(c1 > 1111.52) AS c1, SOME(c2 > 3802.71) AS c2
                      FROM decimal_tbl
                      GROUP BY id"""


class aggtst_decimal_some_distinct(TstView):
    def __init__(self):
        # Validated on Postgres
        self.data = [{"c1": True, "c2": True}]
        self.sql = """CREATE MATERIALIZED VIEW decimal_some_distinct AS SELECT
                      SOME(DISTINCT c1 > 1111.52) AS c1, SOME(DISTINCT c2 > 3802.71) AS c2
                      FROM decimal_tbl"""


class aggtst_decimal_some_distinct_gby(TstView):
    def __init__(self):
        # Validated on Postgres
        self.data = [
            {"id": 0, "c1": False, "c2": False},
            {"id": 1, "c1": True, "c2": True},
        ]
        self.sql = """CREATE MATERIALIZED VIEW decimal_some_distinct_gby AS SELECT
                      id, SOME(DISTINCT c1 > 1111.52) AS c1, SOME(DISTINCT c2 > 3802.71) AS c2
                      FROM decimal_tbl
                      GROUP BY id"""


class aggtst_decimal_some_where(TstView):
    def __init__(self):
        # Validated on Postgres
        self.data = [{"c1": True, "c2": True}]
        self.sql = """CREATE MATERIALIZED VIEW decimal_some_where AS SELECT
                      SOME(c1 > 1111.52) FILTER(WHERE c1 IS NOT NULL) AS c1,
                      SOME(c2 > 3802.71) FILTER(WHERE c1 IS NOT NULL) AS c2
                      FROM decimal_tbl"""


class aggtst_decimal_some_where_groupby(TstView):
    def __init__(self):
        # Validated on Postgres
        self.data = [
            {"id": 0, "c1": False, "c2": False},
            {"id": 1, "c1": True, "c2": True},
        ]
        self.sql = """CREATE MATERIALIZED VIEW decimal_some_where_gby AS SELECT
                      id,
                      SOME(c1 > 1111.52) FILTER(WHERE c1 IS NOT NULL) AS c1,
                      SOME(c2 > 3802.71) FILTER(WHERE c1 IS NOT NULL) AS c2
                      FROM decimal_tbl
                      GROUP BY id"""
