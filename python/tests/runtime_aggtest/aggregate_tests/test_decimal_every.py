from tests.runtime_aggtest.aggtst_base import TstView


class aggtst_decimal_every(TstView):
    def __init__(self):
        # Validated on Postgres
        self.data = [{"c1": False, "c2": False}]
        self.sql = """CREATE MATERIALIZED VIEW decimal_every AS SELECT
                      EVERY(c1 > 1111.52) AS c1, EVERY(c2 > 3802.71) AS c2
                      FROM decimal_tbl"""


class aggtst_decimal_every_groupby(TstView):
    def __init__(self):
        # Validated on Postgres
        self.data = [
            {"id": 0, "c1": False, "c2": False},
            {"id": 1, "c1": True, "c2": True},
        ]
        self.sql = """CREATE MATERIALIZED VIEW decimal_every_gby AS SELECT
                      id, EVERY(c1 > 1111.52) AS c1, EVERY(c2 > 3802.71) AS c2
                      FROM decimal_tbl
                      GROUP BY id"""


class aggtst_decimal_every_distinct(TstView):
    def __init__(self):
        # Validated on Postgres
        self.data = [{"c1": False, "c2": False}]
        self.sql = """CREATE MATERIALIZED VIEW decimal_every_distinct AS SELECT
                      EVERY(DISTINCT c1 > 1111.52) AS c1, EVERY(DISTINCT c2 > 3802.71) AS c2
                      FROM decimal_tbl"""


class aggtst_decimal_every_distinct_gby(TstView):
    def __init__(self):
        # Validated on Postgres
        self.data = [
            {"id": 0, "c1": False, "c2": False},
            {"id": 1, "c1": True, "c2": True},
        ]
        self.sql = """CREATE MATERIALIZED VIEW decimal_every_distinct_gby AS SELECT
                      id, EVERY(DISTINCT c1 > 1111.52) AS c1, EVERY(DISTINCT c2 > 3802.71) AS c2
                      FROM decimal_tbl
                      GROUP BY id"""


class aggtst_decimal_every_where(TstView):
    def __init__(self):
        # Validated on Postgres
        self.data = [{"c1": False, "c2": False}]
        self.sql = """CREATE MATERIALIZED VIEW decimal_every_where AS SELECT
                      EVERY(c1 > 1111.52) FILTER(WHERE c1 IS NOT NULL) AS c1,
                      EVERY(c2 > 3802.71) FILTER(WHERE c1 IS NOT NULL) AS c2
                      FROM decimal_tbl"""


class aggtst_decimal_every_where_groupby(TstView):
    def __init__(self):
        # Validated on Postgres
        self.data = [
            {"id": 0, "c1": False, "c2": False},
            {"id": 1, "c1": True, "c2": True},
        ]
        self.sql = """CREATE MATERIALIZED VIEW decimal_every_where_gby AS SELECT
                      id,
                      EVERY(c1 > 1111.52) FILTER(WHERE c1 IS NOT NULL) AS c1,
                      EVERY(c2 > 3802.71) FILTER(WHERE c1 IS NOT NULL) AS c2
                      FROM decimal_tbl
                      GROUP BY id"""
