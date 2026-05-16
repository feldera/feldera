from tests.runtime_aggtest.aggtst_base import TstView
import datetime

class aggtst_timestamp_arg_min_value(TstView):
    def __init__(self):
        # checked manually
        self.data = [{"c1": datetime.datetime(2024, 12, 5, 9, 15, 0), "c2": datetime.datetime(2024, 12, 5, 12, 45, 0)}]
        self.sql = """CREATE MATERIALIZED VIEW timestamp_arg_min AS SELECT
                      ARG_MIN(c1, c2) AS c1, ARG_MIN(c2, c1) AS c2
                      FROM timestamp_tbl"""


class aggtst_timestamp_arg_min1_value(TstView):
    def __init__(self):
        # checked manually
        self.data = [{"c1": datetime.datetime(2024, 12, 5, 9, 15, 0), "c2": datetime.datetime(2015, 9, 7, 18, 57, 0)}]
        self.sql = """CREATE MATERIALIZED VIEW timestamp_arg_min1 AS SELECT
                      ARG_MIN(c1, c2) AS c1, ARG_MIN(c2, c1) AS c2
                      FROM timestamp1_tbl"""


class aggtst_timestamp_arg_min_gby(TstView):
    def __init__(self):
        # checked manually
        self.data = [
            {"id": 0, "c1": datetime.datetime(2014, 11, 5, 8, 27, 0), "c2": datetime.datetime(2024, 12, 5, 12, 45, 0)},
            {"id": 1, "c1": datetime.datetime(2024, 12, 5, 9, 15, 0), "c2": datetime.datetime(2023, 2, 26, 18, 0, 0)},
        ]
        self.sql = """CREATE MATERIALIZED VIEW timestamp_arg_min_gby AS SELECT
                      id, ARG_MIN(c1, c2) AS c1, ARG_MIN(c2, c1) AS c2
                      FROM timestamp_tbl
                      GROUP BY id"""


class aggtst_timestamp_arg_min_distinct(TstView):
    def __init__(self):
        # checked manually
        self.data = [{"c1": datetime.datetime(2024, 12, 5, 9, 15, 0), "c2": datetime.datetime(2024, 12, 5, 12, 45, 0)}]
        self.sql = """CREATE MATERIALIZED VIEW timestamp_arg_min_distinct AS SELECT
                      ARG_MIN(DISTINCT c1, c2) AS c1, ARG_MIN(DISTINCT c2, c1) AS c2
                      FROM timestamp_tbl"""


class aggtst_timestamp_arg_min_distinct_gby(TstView):
    def __init__(self):
        # checked manually
        self.data = [
            {"id": 0, "c1": datetime.datetime(2014, 11, 5, 8, 27, 0), "c2": datetime.datetime(2024, 12, 5, 12, 45, 0)},
            {"id": 1, "c1": datetime.datetime(2024, 12, 5, 9, 15, 0), "c2": datetime.datetime(2023, 2, 26, 18, 0, 0)},
        ]
        self.sql = """CREATE MATERIALIZED VIEW timestamp_arg_min_distinct_gby AS SELECT
                      id, ARG_MIN(DISTINCT c1, c2) AS c1, ARG_MIN(DISTINCT c2, c1) AS c2
                      FROM timestamp_tbl
                      GROUP BY id"""


class aggtst_timestamp_arg_min_where(TstView):
    def __init__(self):
        # checked manually
        self.data = [{"c1": datetime.datetime(2024, 12, 5, 9, 15, 0), "c2": None}]
        self.sql = """CREATE MATERIALIZED VIEW timestamp_arg_min_where AS SELECT
                      ARG_MIN(c1, c2) FILTER(WHERE c1 > '2014-11-05 08:27:00') AS c1, ARG_MIN(c2, c1) FILTER(WHERE c1 > '2014-11-05 08:27:00') AS c2
                      FROM timestamp_tbl"""


class aggtst_timestamp_arg_min_where_gby(TstView):
    def __init__(self):
        # checked manually
        self.data = [
            {"id": 0, "c1": datetime.datetime(2020, 6, 21, 14, 0, 0), "c2": None},
            {"id": 1, "c1": datetime.datetime(2024, 12, 5, 9, 15, 0), "c2": datetime.datetime(2023, 2, 26, 18, 0, 0)},
        ]
        self.sql = """CREATE MATERIALIZED VIEW timestamp_arg_min_where_gby AS SELECT
                      id, ARG_MIN(c1, c2) FILTER(WHERE c1 > '2014-11-05 08:27:00') AS c1, ARG_MIN(c2, c1) FILTER(WHERE c1 > '2014-11-05 08:27:00') AS c2
                      FROM timestamp_tbl
                      GROUP BY id"""


class aggtst_timestamp_arg_min_where1_gby(TstView):
    def __init__(self):
        # checked manually
        self.data = [
            {"id": 0, "c1": datetime.datetime(2014, 11, 5, 8, 27, 0), "c2": datetime.datetime(2024, 12, 5, 12, 45, 0)},
            {"id": 1, "c1": datetime.datetime(1969, 3, 17, 11, 32, 0), "c2": datetime.datetime(2015, 9, 7, 18, 57, 0)},
        ]
        self.sql = """CREATE MATERIALIZED VIEW timestamp_arg_min_where1_gby AS SELECT
                      id, ARG_MIN(c1, c2) FILTER(WHERE c1 < '2020-06-21 14:00:00') AS c1, ARG_MIN(c2, c1) FILTER(WHERE c2 > '2014-11-05 16:30:00') AS c2
                      FROM timestamp1_tbl
                      GROUP BY id"""
