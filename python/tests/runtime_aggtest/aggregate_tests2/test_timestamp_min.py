from tests.runtime_aggtest.aggtst_base import TstView


class aggtst_timestamp_min_value(TstView):
    def __init__(self):
        # Validated on Postgres
        self.data = [{"c1": "2014-11-05T08:27:00", "c2": "2014-11-05T16:30:00"}]
        self.sql = """CREATE MATERIALIZED VIEW timestamp_min AS SELECT
                      MIN(c1) AS c1, MIN(c2) AS c2
                      FROM timestamp_tbl"""


class aggtst_timestamp_min1_value(TstView):
    def __init__(self):
        # Validated on Postgres
        self.data = [{"c1": "1969-03-17T11:32:00", "c2": "2014-11-05T16:30:00"}]
        self.sql = """CREATE MATERIALIZED VIEW timestamp_min1 AS SELECT
                      MIN(c1) AS c1, MIN(c2) AS c2
                      FROM timestamp1_tbl"""


class aggtst_timestamp_min_gby(TstView):
    def __init__(self):
        # Validated on Postgres
        self.data = [
            {"id": 0, "c1": "2014-11-05T08:27:00", "c2": "2024-12-05T12:45:00"},
            {"id": 1, "c1": "2020-06-21T14:00:00", "c2": "2014-11-05T16:30:00"},
        ]
        self.sql = """CREATE MATERIALIZED VIEW timestamp_min_gby AS SELECT
                      id, MIN(c1) AS c1, MIN(c2) AS c2
                      FROM timestamp_tbl
                      GROUP BY id"""


class aggtst_timestamp_min_distinct(TstView):
    def __init__(self):
        # Validated on Postgres
        self.data = [{"c1": "2014-11-05T08:27:00", "c2": "2014-11-05T16:30:00"}]
        self.sql = """CREATE MATERIALIZED VIEW timestamp_min_distinct AS SELECT
                      MIN(DISTINCT c1) AS c1, MIN(DISTINCT c2) AS c2
                      FROM timestamp_tbl"""


class aggtst_timestamp_min_distinct_gby(TstView):
    def __init__(self):
        # Validated on Postgres
        self.data = [
            {"id": 0, "c1": "2014-11-05T08:27:00", "c2": "2024-12-05T12:45:00"},
            {"id": 1, "c1": "2020-06-21T14:00:00", "c2": "2014-11-05T16:30:00"},
        ]
        self.sql = """CREATE MATERIALIZED VIEW timestamp_min_distinct_gby AS SELECT
                      id, MIN(DISTINCT c1) AS c1, MIN(DISTINCT c2) AS c2
                      FROM timestamp_tbl
                      GROUP BY id"""


class aggtst_timestamp_min_where(TstView):
    def __init__(self):
        # Validated on Postgres
        self.data = [{"f_c1": "2020-06-21T14:00:00", "f_c2": "2014-11-05T16:30:00"}]
        self.sql = """CREATE MATERIALIZED VIEW timestamp_min_where AS SELECT
                      MIN(c1) FILTER (WHERE c1 > '2014-11-05 08:27:00') AS f_c1, MIN(c2) FILTER (WHERE c1 > '2014-11-05 08:27:00') AS f_c2
                      FROM timestamp_tbl"""


class aggtst_timestamp_min_where_gby(TstView):
    def __init__(self):
        # Validated on Postgres
        self.data = [
            {"id": 0, "f_c1": "2020-06-21T14:00:00", "f_c2": None},
            {"id": 1, "f_c1": "2020-06-21T14:00:00", "f_c2": "2014-11-05T16:30:00"},
        ]
        self.sql = """CREATE MATERIALIZED VIEW timestamp_min_where_gby AS SELECT
                      id, MIN(c1) FILTER (WHERE c1 > '2014-11-05 08:27:00') AS f_c1, MIN(c2) FILTER (WHERE c1 > '2014-11-05 08:27:00') AS f_c2
                      FROM timestamp_tbl
                      GROUP BY id"""


class aggtst_timestamp_min_where1_gby(TstView):
    def __init__(self):
        # Validated on Postgres
        self.data = [
            {"id": 0, "f_c1": "2014-11-05T08:27:00", "f_c2": "2024-12-05T12:45:00"},
            {"id": 1, "f_c1": "1969-03-17T11:32:00", "f_c2": "2015-09-07T18:57:00"},
        ]
        self.sql = """CREATE MATERIALIZED VIEW timestamp_min_where1_gby AS SELECT
                      id, MIN(c1) FILTER (WHERE c1 < '2020-06-21 14:00:00') AS f_c1, MIN(c2) FILTER (WHERE c2 > '2014-11-05 16:30:00') AS f_c2
                      FROM timestamp1_tbl
                      GROUP BY id"""
