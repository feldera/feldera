from tests.runtime_aggtest.aggtst_base import TstView
import datetime


class aggtst_time_arg_min_value(TstView):
    def __init__(self):
        # checked manually
        self.data = [{"c1": datetime.time(8, 30, 0), "c2": datetime.time(12, 45, 0)}]
        self.sql = """CREATE MATERIALIZED VIEW time_arg_min AS SELECT
                      ARG_MIN(c1, c2) AS c1, ARG_MIN(c2, c1) AS c2
                      FROM time_tbl"""


class aggtst_time_arg_min_gby(TstView):
    def __init__(self):
        # checked manually
        self.data = [
            {"id": 0, "c1": datetime.time(8, 30, 0), "c2": datetime.time(12, 45, 0)},
            {"id": 1, "c1": datetime.time(9, 15, 0), "c2": datetime.time(16, 30, 0)},
        ]
        self.sql = """CREATE MATERIALIZED VIEW time_arg_min_gby AS SELECT
                      id, ARG_MIN(c1, c2) AS c1, ARG_MIN(c2, c1) AS c2
                      FROM time_tbl
                      GROUP BY id"""


class aggtst_time_arg_min_distinct(TstView):
    def __init__(self):
        # checked manually
        self.data = [{"c1": datetime.time(8, 30, 0), "c2": datetime.time(12, 45, 0)}]
        self.sql = """CREATE MATERIALIZED VIEW time_arg_min_distinct AS SELECT
                      ARG_MIN(DISTINCT c1, c2) AS c1, ARG_MIN(DISTINCT c2, c1) AS c2
                      FROM time_tbl"""


class aggtst_time_arg_min_distinct_gby(TstView):
    def __init__(self):
        # checked manually
        self.data = [
            {"id": 0, "c1": datetime.time(8, 30, 0), "c2": datetime.time(12, 45, 0)},
            {"id": 1, "c1": datetime.time(9, 15, 0), "c2": datetime.time(16, 30, 0)},
        ]
        self.sql = """CREATE MATERIALIZED VIEW time_arg_min_distinct_gby AS SELECT
                      id, ARG_MIN(DISTINCT c1, c2) AS c1, ARG_MIN(DISTINCT c2, c1) AS c2
                      FROM time_tbl
                      GROUP BY id"""


class aggtst_time_arg_min_where(TstView):
    def __init__(self):
        # checked manually
        self.data = [{"c1": datetime.time(9, 15, 00), "c2": datetime.time(16, 30, 0)}]
        self.sql = """CREATE MATERIALIZED VIEW time_arg_min_where AS SELECT
                      ARG_MIN(c1, c2) FILTER(WHERE c1 > '08:30:00') AS c1, ARG_MIN(c2, c1) FILTER(WHERE c1 > '08:30:00') AS c2
                      FROM time_tbl"""


class aggtst_time_arg_min_where_gby(TstView):
    def __init__(self):
        # checked manually
        self.data = [
            {"id": 0, "c1": datetime.time(14, 0, 0), "c2": None},
            {"id": 1, "c1": datetime.time(9, 15, 0), "c2": datetime.time(16, 30, 0)},
        ]
        self.sql = """CREATE MATERIALIZED VIEW time_arg_min_where_gby AS SELECT
                      id, ARG_MIN(c1, c2) FILTER(WHERE c1 > '08:30:00') AS c1, ARG_MIN(c2, c1) FILTER(WHERE c1 > '08:30:00') AS c2
                      FROM time_tbl
                      GROUP BY id"""
