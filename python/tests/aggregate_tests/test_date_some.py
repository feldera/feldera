from .aggtst_base import TstView


class aggtst_date_some(TstView):
    def __init__(self):
        # Validated on Postgres
        self.data = [{"c1": True, "c2": True}]
        self.sql = """CREATE MATERIALIZED VIEW date_some AS SELECT
                      SOME(c1 > '1969-03-17') AS c1, SOME(c2 < '2024-12-05') AS c2
                      FROM date_tbl"""


class aggtst_date_some_groupby(TstView):
    def __init__(self):
        # Validated on Postgres
        self.data = [
            {"id": 0, "c1": True, "c2": False},
            {"id": 1, "c1": True, "c2": True},
        ]
        self.sql = """CREATE MATERIALIZED VIEW date_some_gby AS SELECT
                      id, SOME(c1 > '1969-03-17') AS c1, SOME(c2 < '2024-12-05') AS c2
                      FROM date_tbl
                      GROUP BY id"""


class aggtst_date_some_distinct(TstView):
    def __init__(self):
        # Validated on Postgres
        self.data = [{"c1": True, "c2": True}]
        self.sql = """CREATE MATERIALIZED VIEW date_some_distinct AS SELECT
                      SOME(DISTINCT c1 > '1969-03-17') AS c1, SOME(DISTINCT c2 < '2024-12-05') AS c2
                      FROM date_tbl"""


class aggtst_date_some_distinct_gby(TstView):
    def __init__(self):
        # Validated on Postgres
        self.data = [
            {"id": 0, "c1": True, "c2": False},
            {"id": 1, "c1": True, "c2": True},
        ]
        self.sql = """CREATE MATERIALIZED VIEW date_some_distinct_gby AS SELECT
                      id, SOME(DISTINCT c1 > '1969-03-17') AS c1, SOME(DISTINCT c2 < '2024-12-05') AS c2
                      FROM date_tbl
                      GROUP BY id"""


class aggtst_date_some_where(TstView):
    def __init__(self):
        # Validated on Postgres
        self.data = [{"c1": True, "c2": True}]
        self.sql = """CREATE MATERIALIZED VIEW date_some_where AS SELECT
                      SOME(c1 > '1969-03-17') FILTER(WHERE c1 > '2014-11-05') AS c1, SOME(c2 < '2024-12-05') FILTER(WHERE c2 > '2015-09-07') AS c2
                      FROM date_tbl"""


class aggtst_date_some_where_groupby(TstView):
    def __init__(self):
        # Validated on Postgres
        self.data = [
            {"id": 0, "c1": True, "c2": False},
            {"id": 1, "c1": True, "c2": True},
        ]
        self.sql = """CREATE MATERIALIZED VIEW date_some_where_gby AS SELECT
                      id, SOME(c1 > '1969-03-17') FILTER(WHERE c1 > '2014-11-05') AS c1, SOME(c2 < '2024-12-05') FILTER(WHERE c2 > '2015-09-07') AS c2
                      FROM date_tbl
                      GROUP BY id"""