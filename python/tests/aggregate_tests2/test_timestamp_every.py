from tests.aggregate_tests.aggtst_base import TstView


class aggtst_timestamp_every(TstView):
    def __init__(self):
        # Validated on Postgres
        self.data = [{"c1": False, "c2": False}]
        self.sql = """CREATE MATERIALIZED VIEW timestamp_every AS SELECT
                      EVERY(c1 > '2014-11-05 08:27:00') AS c1, EVERY(c2 > '2024-12-05 12:45:00') AS c2
                      FROM timestamp_tbl"""


class aggtst_timestamp_every_groupby(TstView):
    def __init__(self):
        # Validated on Postgres
        self.data = [
            {"id": 0, "c1": False, "c2": False},
            {"id": 1, "c1": True, "c2": False},
        ]
        self.sql = """CREATE MATERIALIZED VIEW timestamp_every_gby AS SELECT
                      id, EVERY(c1 > '2014-11-05 08:27:00') AS c1, EVERY(c2 > '2024-12-05 12:45:00') AS c2
                      FROM timestamp_tbl
                      GROUP BY id"""


class aggtst_timestamp_every_distinct(TstView):
    def __init__(self):
        # Validated on Postgres
        self.data = [{"c1": False, "c2": False}]
        self.sql = """CREATE MATERIALIZED VIEW timestamp_every_distinct AS SELECT
                      EVERY(DISTINCT c1 > '2014-11-05 08:27:00') AS c1, EVERY(DISTINCT c2 > '2024-12-05 12:45:00') AS c2
                      FROM timestamp_tbl"""


class aggtst_timestamp_every_distinct_gby(TstView):
    def __init__(self):
        # Validated on Postgres
        self.data = [
            {"id": 0, "c1": False, "c2": False},
            {"id": 1, "c1": True, "c2": False},
        ]
        self.sql = """CREATE MATERIALIZED VIEW timestamp_every_distinct_gby AS SELECT
                      id, EVERY(DISTINCT c1 > '2014-11-05 08:27:00') AS c1, EVERY(DISTINCT c2 > '2024-12-05 12:45:00') AS c2
                      FROM timestamp_tbl
                      GROUP BY id"""


class aggtst_timestamp_every_where(TstView):
    def __init__(self):
        # Validated on Postgres
        self.data = [{"c1": False, "c2": False}]
        self.sql = """CREATE MATERIALIZED VIEW timestamp_every_where AS SELECT
                      EVERY(c1 > '2014-11-05 08:27:00') FILTER(WHERE c2 IS NOT NULL) AS c1, EVERY(c2 > '2024-12-05 12:45:00') FILTER(WHERE c2 IS NOT NULL) AS c2
                      FROM timestamp_tbl"""


class aggtst_timestamp_every_where_groupby(TstView):
    def __init__(self):
        # Validated on Postgres
        self.data = [
            {"id": 0, "c1": False, "c2": False},
            {"id": 1, "c1": True, "c2": False},
        ]
        self.sql = """CREATE MATERIALIZED VIEW timestamp_every_where_gby AS SELECT
                      id, EVERY(c1 > '2014-11-05 08:27:00') FILTER(WHERE c2 IS NOT NULL) AS c1, EVERY(c2 > '2024-12-05 12:45:00') FILTER(WHERE c2 IS NOT NULL) AS c2
                      FROM timestamp_tbl
                      GROUP BY id"""


class aggtst_timestamp_every_where1_groupby(TstView):
    def __init__(self):
        # Validated on Postgres
        self.data = [
            {"id": 0, "c1": True, "c2": True},
            {"id": 1, "c1": False, "c2": False},
        ]
        self.sql = """CREATE MATERIALIZED VIEW timestamp_every_where1_gby AS SELECT
                      id, EVERY(c1 < '2020-06-21 14:00:00') FILTER(WHERE c2 IS NOT NULL) AS c1, EVERY(c2 > '2014-11-05 16:30:00') FILTER(WHERE c2 IS NOT NULL) AS c2
                      FROM timestamp1_tbl
                      GROUP BY id"""
