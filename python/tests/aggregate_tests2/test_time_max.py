from tests.aggregate_tests.aggtst_base import TstView


class aggtst_time_max_value(TstView):
    def __init__(self):
        # Validated on Postgres
        self.data = [{"c1": "14:00:00", "c2": "18:00:00"}]
        self.sql = """CREATE MATERIALIZED VIEW time_max AS SELECT
                      MAX(c1) AS c1, MAX(c2) AS c2
                      FROM time_tbl"""


class aggtst_time_max_gby(TstView):
    def __init__(self):
        # Validated on Postgres
        self.data = [
            {"id": 0, "c1": "14:00:00", "c2": "12:45:00"},
            {"id": 1, "c1": "14:00:00", "c2": "18:00:00"},
        ]
        self.sql = """CREATE MATERIALIZED VIEW time_max_gby AS SELECT
                      id, MAX(c1) AS c1, MAX(c2) AS c2
                      FROM time_tbl
                      GROUP BY id"""


class aggtst_time_max_distinct(TstView):
    def __init__(self):
        # Validated on Postgres
        self.data = [{"c1": "14:00:00", "c2": "18:00:00"}]
        self.sql = """CREATE MATERIALIZED VIEW time_max_distinct AS SELECT
                      MAX(DISTINCT c1) AS c1, MAX(DISTINCT c2) AS c2
                      FROM time_tbl"""


class aggtst_time_max_distinct_gby(TstView):
    def __init__(self):
        # Validated on Postgres
        self.data = [
            {"id": 0, "c1": "14:00:00", "c2": "12:45:00"},
            {"id": 1, "c1": "14:00:00", "c2": "18:00:00"},
        ]
        self.sql = """CREATE MATERIALIZED VIEW time_max_distinct_gby AS SELECT
                      id, MAX(DISTINCT c1) AS c1, MAX(DISTINCT c2) AS c2
                      FROM time_tbl
                      GROUP BY id"""


class aggtst_time_max_where(TstView):
    def __init__(self):
        # Validated on Postgres
        self.data = [{"f_c1": "14:00:00", "f_c2": "18:00:00"}]
        self.sql = """CREATE MATERIALIZED VIEW time_max_where AS SELECT
                      MAX(c1) FILTER (WHERE c1 > '08:30:00') AS f_c1, MAX(c2) FILTER (WHERE c1 > '08:30:00') AS f_c2
                      FROM time_tbl"""


class aggtst_time_max_where_gby(TstView):
    def __init__(self):
        # Validated on Postgres
        self.data = [
            {"id": 0, "f_c1": "14:00:00", "f_c2": None},
            {"id": 1, "f_c1": "14:00:00", "f_c2": "18:00:00"},
        ]
        self.sql = """CREATE MATERIALIZED VIEW time_max_where_gby AS SELECT
                      id, MAX(c1) FILTER (WHERE c1 > '08:30:00') AS f_c1, MAX(c2) FILTER (WHERE c1 > '08:30:00') AS f_c2
                      FROM time_tbl
                      GROUP BY id"""
