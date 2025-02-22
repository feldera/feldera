from tests.aggregate_tests.aggtst_base import TstView


class aggtst_date_every(TstView):
    def __init__(self):
        # Validated on Postgres
        self.data = [{"c1": False, "c2": False}]
        self.sql = """CREATE MATERIALIZED VIEW date_every AS SELECT
                      EVERY(c1 > '1969-03-17') AS c1, EVERY(c2 < '2024-12-05') AS c2
                      FROM date_tbl"""


class aggtst_date_every_groupby(TstView):
    def __init__(self):
        # Validated on Postgres
        self.data = [
            {"id": 0, "c1": True, "c2": False},
            {"id": 1, "c1": False, "c2": True},
        ]
        self.sql = """CREATE MATERIALIZED VIEW date_every_gby AS SELECT
                      id, EVERY(c1 > '1969-03-17') AS c1, EVERY(c2 < '2024-12-05') AS c2
                      FROM date_tbl
                      GROUP BY id"""


class aggtst_date_every_distinct(TstView):
    def __init__(self):
        # Validated on Postgres
        self.data = [{"c1": False, "c2": False}]
        self.sql = """CREATE MATERIALIZED VIEW date_every_distinct AS SELECT
                      EVERY(DISTINCT c1 > '1969-03-17') AS c1, EVERY(DISTINCT c2 < '2024-12-05') AS c2
                      FROM date_tbl"""


class aggtst_date_every_distinct_gby(TstView):
    def __init__(self):
        # Validated on Postgres
        self.data = [
            {"id": 0, "c1": True, "c2": False},
            {"id": 1, "c1": False, "c2": True},
        ]
        self.sql = """CREATE MATERIALIZED VIEW date_every_distinct_gby AS SELECT
                      id, EVERY(DISTINCT c1 > '1969-03-17') AS c1, EVERY(DISTINCT c2 < '2024-12-05') AS c2
                      FROM date_tbl
                      GROUP BY id"""


class aggtst_date_every_where(TstView):
    def __init__(self):
        # Validated on Postgres
        self.data = [{"c1": True, "c2": False}]
        self.sql = """CREATE MATERIALIZED VIEW date_every_where AS SELECT
                      EVERY(c1 > '1969-03-17') FILTER(WHERE c1 > '2014-11-05') AS c1, EVERY(c2 < '2024-12-05') FILTER(WHERE c2 > '2015-09-07') AS c2
                      FROM date_tbl"""


class aggtst_date_every_where_groupby(TstView):
    def __init__(self):
        # Validated on Postgres
        self.data = [
            {"id": 0, "c1": True, "c2": False},
            {"id": 1, "c1": True, "c2": True},
        ]
        self.sql = """CREATE MATERIALIZED VIEW date_every_where_gby AS SELECT
                      id, EVERY(c1 > '1969-03-17') FILTER(WHERE c1 > '2014-11-05') AS c1, EVERY(c2 < '2024-12-05') FILTER(WHERE c2 > '2015-09-07') AS c2
                      FROM date_tbl
                      GROUP BY id"""
