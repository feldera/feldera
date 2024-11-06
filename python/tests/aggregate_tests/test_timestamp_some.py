from .aggtst_base import TstView


class aggtst_timestamp_some(TstView):
    def __init__(self):
        # Validated on Postgres
        self.data = [{'c1': True, 'c2': False}]
        self.sql = """CREATE MATERIALIZED VIEW timestamp_some AS SELECT
                      SOME(c1 > '2014-11-05 08:27:00') AS c1, SOME(c2 > '2024-12-05 12:45:00') AS c2
                      FROM timestamp_tbl"""


class aggtst_timestamp_some_groupby(TstView):
    def __init__(self):
        # Validated on Postgres
        self.data = [
            {'id': 0, 'c1': True, 'c2': False},
            {'id': 1, 'c1': True, 'c2': False}
        ]
        self.sql = """CREATE MATERIALIZED VIEW timestamp_some_gby AS SELECT
                      id, SOME(c1 > '2014-11-05 08:27:00') AS c1, SOME(c2 > '2024-12-05 12:45:00') AS c2
                      FROM timestamp_tbl
                      GROUP BY id"""


class aggtst_timestamp_some_distinct(TstView):
    def __init__(self):
        # Validated on Postgres
        self.data = [{'c1': True, 'c2': False}]
        self.sql = """CREATE MATERIALIZED VIEW timestamp_some_distinct AS SELECT
                      SOME(DISTINCT c1 > '2014-11-05 08:27:00') AS c1, SOME(DISTINCT c2 > '2024-12-05 12:45:00') AS c2
                      FROM timestamp_tbl"""


class aggtst_timestamp_some_distinct_gby(TstView):
    def __init__(self):
        # Validated on Postgres
        self.data = [
            {'id': 0, 'c1': True, 'c2': False},
            {'id': 1, 'c1': True, 'c2': False}
        ]
        self.sql = """CREATE MATERIALIZED VIEW timestamp_some_distinct_gby AS SELECT
                      id, SOME(DISTINCT c1 > '2014-11-05 08:27:00') AS c1, SOME(DISTINCT c2 > '2024-12-05 12:45:00') AS c2
                      FROM timestamp_tbl
                      GROUP BY id"""


class aggtst_timestamp_some_where(TstView):
    def __init__(self):
        # Validated on Postgres
        self.data = [{'c1': True, 'c2': False}]
        self.sql = """CREATE MATERIALIZED VIEW timestamp_some_where AS SELECT
                      SOME(c1 > '2014-11-05 08:27:00') FILTER(WHERE c2 IS NOT NULL) AS c1, SOME(c2 > '2024-12-05 12:45:00') FILTER(WHERE c2 IS NOT NULL) AS c2
                      FROM timestamp_tbl"""


class aggtst_timestamp_some_where_groupby(TstView):
    def __init__(self):
        # Validated on Postgres
        self.data = [
            {'id': 0, 'c1': False, 'c2': False},
            {'id': 1, 'c1': True, 'c2': False}
        ]
        self.sql = """CREATE MATERIALIZED VIEW timestamp_some_where_gby AS SELECT
                      id, SOME(c1 > '2014-11-05 08:27:00') FILTER(WHERE c2 IS NOT NULL) AS c1, SOME(c2 > '2024-12-05 12:45:00') FILTER(WHERE c2 IS NOT NULL) AS c2
                      FROM timestamp_tbl
                      GROUP BY id"""
