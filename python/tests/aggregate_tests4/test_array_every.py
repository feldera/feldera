from tests.aggregate_tests.aggtst_base import TstView


class aggtst_array_every(TstView):
    def __init__(self):
        # Validated on Postgres
        self.data = [{"c1": False, "c2": False}]
        self.sql = """CREATE MATERIALIZED VIEW array_every AS SELECT
                      EVERY(c1 > c2) AS c1, EVERY(c2 > c1) AS c2
                      FROM array_tbl"""


class aggtst_array_every_groupby(TstView):
    def __init__(self):
        # Validated on Postgres
        self.data = [
            {"id": 0, "c1": False, "c2": True},
            {"id": 1, "c1": False, "c2": False},
        ]
        self.sql = """CREATE MATERIALIZED VIEW array_every_gby AS SELECT
                      id, EVERY(c1 > c2) AS c1, EVERY(c2 > c1) AS c2
                      FROM array_tbl
                      GROUP BY id"""


class aggtst_array_every_distinct(TstView):
    def __init__(self):
        # Validated on Postgres
        self.data = [{"c1": False, "c2": False}]
        self.sql = """CREATE MATERIALIZED VIEW array_every_distinct AS SELECT
                      EVERY(DISTINCT c1 > c2) AS c1, EVERY(DISTINCT c2 > c1) AS c2
                      FROM array_tbl"""


class aggtst_array_every_distinct_gby(TstView):
    def __init__(self):
        # Validated on Postgres
        self.data = [
            {"id": 0, "c1": False, "c2": True},
            {"id": 1, "c1": False, "c2": False},
        ]
        self.sql = """CREATE MATERIALIZED VIEW array_every_distinct_gby AS SELECT
                      id, EVERY(DISTINCT c1 > c2) AS c1, EVERY(DISTINCT c2 > c1) AS c2
                      FROM array_tbl
                      GROUP BY id"""


class aggtst_array_every_where(TstView):
    def __init__(self):
        # Validated on Postgres
        self.data = [{"c1": False, "c2": False}]
        self.sql = """CREATE MATERIALIZED VIEW array_every_where AS SELECT
                      EVERY(c1 > c2) FILTER(WHERE c2 IS NOT NULL) AS c1, EVERY(c2 > c1) FILTER(WHERE c2 IS NOT NULL) AS c2
                      FROM array_tbl"""


class aggtst_array_every_where_groupby(TstView):
    def __init__(self):
        # Validated on Postgres
        self.data = [
            {"id": 0, "c1": False, "c2": True},
            {"id": 1, "c1": False, "c2": False},
        ]
        self.sql = """CREATE MATERIALIZED VIEW array_every_where_gby AS SELECT
                      id, EVERY(c1 > c2) FILTER(WHERE c2 IS NOT NULL) AS c1, EVERY(c2 > c1) FILTER(WHERE c2 IS NOT NULL) AS c2
                      FROM array_tbl
                      GROUP BY id"""
