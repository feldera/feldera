from tests.runtime_aggtest.aggtst_base import TstView


class aggtst_time_min_value(TstView):
    def __init__(self):
        # Validated on Postgres
        self.data = [{"c1": "08:30:00", "c2": "12:45:00"}]
        self.sql = """CREATE MATERIALIZED VIEW time_min AS SELECT
                      MIN(c1) AS c1, MIN(c2) AS c2
                      FROM time_tbl"""


class aggtst_time_min_gby(TstView):
    def __init__(self):
        # Validated on Postgres
        self.data = [
            {"id": 0, "c1": "08:30:00", "c2": "12:45:00"},
            {"id": 1, "c1": "09:15:00", "c2": "16:30:00"},
        ]
        self.sql = """CREATE MATERIALIZED VIEW time_min_gby AS SELECT
                      id, MIN(c1) AS c1, MIN(c2) AS c2
                      FROM time_tbl
                      GROUP BY id"""


class aggtst_time_min_distinct(TstView):
    def __init__(self):
        # Validated on Postgres
        self.data = [{"c1": "08:30:00", "c2": "12:45:00"}]
        self.sql = """CREATE MATERIALIZED VIEW time_min_distinct AS SELECT
                      MIN(DISTINCT c1) AS c1, MIN(DISTINCT c2) AS c2
                      FROM time_tbl"""


class aggtst_time_min_distinct_gby(TstView):
    def __init__(self):
        # Validated on Postgres
        self.data = [
            {"id": 0, "c1": "08:30:00", "c2": "12:45:00"},
            {"id": 1, "c1": "09:15:00", "c2": "16:30:00"},
        ]
        self.sql = """CREATE MATERIALIZED VIEW time_min_distinct_gby AS SELECT
                      id, MIN(DISTINCT c1) AS c1, MIN(DISTINCT c2) AS c2
                      FROM time_tbl
                      GROUP BY id"""


class aggtst_time_min_where(TstView):
    def __init__(self):
        # Validated on Postgres
        self.data = [{"f_c1": "09:15:00", "f_c2": "16:30:00"}]
        self.sql = """CREATE MATERIALIZED VIEW time_min_where AS SELECT
                      MIN(c1) FILTER (WHERE c1 > '08:30:00') AS f_c1, MIN(c2) FILTER (WHERE c1 > '08:30:00') AS f_c2
                      FROM time_tbl"""


class aggtst_time_min_where_gby(TstView):
    def __init__(self):
        # Validated on Postgres
        self.data = [
            {"id": 0, "f_c1": "14:00:00", "f_c2": None},
            {"id": 1, "f_c1": "09:15:00", "f_c2": "16:30:00"},
        ]
        self.sql = """CREATE MATERIALIZED VIEW time_min_where_gby AS SELECT
                      id, MIN(c1) FILTER (WHERE c1 > '08:30:00') AS f_c1, MIN(c2) FILTER (WHERE c1 > '08:30:00') AS f_c2
                      FROM time_tbl
                      GROUP BY id"""
