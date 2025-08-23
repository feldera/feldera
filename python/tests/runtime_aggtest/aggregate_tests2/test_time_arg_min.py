from tests.runtime_aggtest.aggtst_base import TstView


class aggtst_time_arg_min_value(TstView):
    def __init__(self):
        # checked manually
        self.data = [{"c1": "08:30:00", "c2": "12:45:00"}]
        self.sql = """CREATE MATERIALIZED VIEW time_arg_min AS SELECT
                      ARG_MIN(c1, c2) AS c1, ARG_MIN(c2, c1) AS c2
                      FROM time_tbl"""


class aggtst_time_arg_min_gby(TstView):
    def __init__(self):
        # checked manually
        self.data = [
            {"id": 0, "c1": "08:30:00", "c2": "12:45:00"},
            {"id": 1, "c1": "09:15:00", "c2": "16:30:00"},
        ]
        self.sql = """CREATE MATERIALIZED VIEW time_arg_min_gby AS SELECT
                      id, ARG_MIN(c1, c2) AS c1, ARG_MIN(c2, c1) AS c2
                      FROM time_tbl
                      GROUP BY id"""


class aggtst_time_arg_min_distinct(TstView):
    def __init__(self):
        # checked manually
        self.data = [{"c1": "08:30:00", "c2": "12:45:00"}]
        self.sql = """CREATE MATERIALIZED VIEW time_arg_min_distinct AS SELECT
                      ARG_MIN(DISTINCT c1, c2) AS c1, ARG_MIN(DISTINCT c2, c1) AS c2
                      FROM time_tbl"""


class aggtst_time_arg_min_distinct_gby(TstView):
    def __init__(self):
        # checked manually
        self.data = [
            {"id": 0, "c1": "08:30:00", "c2": "12:45:00"},
            {"id": 1, "c1": "09:15:00", "c2": "16:30:00"},
        ]
        self.sql = """CREATE MATERIALIZED VIEW time_arg_min_distinct_gby AS SELECT
                      id, ARG_MIN(DISTINCT c1, c2) AS c1, ARG_MIN(DISTINCT c2, c1) AS c2
                      FROM time_tbl
                      GROUP BY id"""


class aggtst_time_arg_min_where(TstView):
    def __init__(self):
        # checked manually
        self.data = [{"c1": "09:15:00", "c2": "16:30:00"}]
        self.sql = """CREATE MATERIALIZED VIEW time_arg_min_where AS SELECT
                      ARG_MIN(c1, c2) FILTER(WHERE c1 > '08:30:00') AS c1, ARG_MIN(c2, c1) FILTER(WHERE c1 > '08:30:00') AS c2
                      FROM time_tbl"""


class aggtst_time_arg_min_where_gby(TstView):
    def __init__(self):
        # checked manually
        self.data = [
            {"id": 0, "c1": "14:00:00", "c2": None},
            {"id": 1, "c1": "09:15:00", "c2": "16:30:00"},
        ]
        self.sql = """CREATE MATERIALIZED VIEW time_arg_min_where_gby AS SELECT
                      id, ARG_MIN(c1, c2) FILTER(WHERE c1 > '08:30:00') AS c1, ARG_MIN(c2, c1) FILTER(WHERE c1 > '08:30:00') AS c2
                      FROM time_tbl
                      GROUP BY id"""
