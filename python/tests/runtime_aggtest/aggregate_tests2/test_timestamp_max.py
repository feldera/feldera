from tests.runtime_aggtest.aggtst_base import TstView
import datetime

class aggtst_timestamp_max_value(TstView):
    def __init__(self):
        # Validated on Postgres
        self.data = [{"c1": datetime.datetime(2024, 12, 5, 9, 15, 0), "c2": datetime.datetime(2024, 12, 5, 12, 45, 0)}]
        self.sql = """CREATE MATERIALIZED VIEW timestamp_max AS SELECT
                      MAX(c1) AS c1, MAX(c2) AS c2
                      FROM timestamp_tbl"""


class aggtst_timestamp_max_gby(TstView):
    def __init__(self):
        # Validated on Postgres
        self.data = [
            {"id": 0, "c1": datetime.datetime(2020, 6, 21, 14, 0, 0), "c2": datetime.datetime(2024, 12, 5, 12, 45, 0)},
            {"id": 1, "c1": datetime.datetime(2024, 12, 5, 9, 15, 0), "c2": datetime.datetime(2023, 2, 26, 18, 0, 0)},
        ]
        self.sql = """CREATE MATERIALIZED VIEW timestamp_max_gby AS SELECT
                      id, MAX(c1) AS c1, MAX(c2) AS c2
                      FROM timestamp_tbl
                      GROUP BY id"""


class aggtst_timestamp_max_distinct(TstView):
    def __init__(self):
        # Validated on Postgres
        self.data = [{"c1": datetime.datetime(2024, 12, 5, 9, 15, 0), "c2": datetime.datetime(2024, 12, 5, 12, 45, 0)}]
        self.sql = """CREATE MATERIALIZED VIEW timestamp_max_distinct AS SELECT
                      MAX(DISTINCT c1) AS c1, MAX(DISTINCT c2) AS c2
                      FROM timestamp_tbl"""


class aggtst_timestamp_max_distinct_gby(TstView):
    def __init__(self):
        # Validated on Postgres
        self.data = [
            {"id": 0, "c1": datetime.datetime(2020, 6, 21, 14, 0, 0), "c2": datetime.datetime(2024, 12, 5, 12, 45, 0)},
            {"id": 1, "c1": datetime.datetime(2024, 12, 5, 9, 15, 0), "c2": datetime.datetime(2023, 2, 26, 18, 0, 0)},
        ]
        self.sql = """CREATE MATERIALIZED VIEW timestamp_max_distinct_gby AS SELECT
                      id, MAX(DISTINCT c1) AS c1, MAX(DISTINCT c2) AS c2
                      FROM timestamp_tbl
                      GROUP BY id"""


class aggtst_timestamp_max_where(TstView):
    def __init__(self):
        # Validated on Postgres
        self.data = [{"f_c1": datetime.datetime(2024, 12, 5, 9, 15, 0), "f_c2": datetime.datetime(2023, 2, 26, 18, 0, 0)}]
        self.sql = """CREATE MATERIALIZED VIEW timestamp_max_where AS SELECT
                      MAX(c1) FILTER (WHERE c1 > '2014-11-05 08:27:00') AS f_c1, MAX(c2) FILTER (WHERE c1 > '2014-11-05 08:27:00') AS f_c2
                      FROM timestamp_tbl"""


class aggtst_timestamp_max_where_gby(TstView):
    def __init__(self):
        # Validated on Postgres
        self.data = [
            {"id": 0, "f_c1": datetime.datetime(2020, 6, 21, 14, 0, 0), "f_c2": None},
            {"id": 1, "f_c1": datetime.datetime(2024, 12, 5, 9, 15, 0), "f_c2": datetime.datetime(2023, 2, 26, 18, 0, 0)},
        ]
        self.sql = """CREATE MATERIALIZED VIEW timestamp_max_where_gby AS SELECT
                      id, MAX(c1) FILTER (WHERE c1 > '2014-11-05 08:27:00') AS f_c1, MAX(c2) FILTER (WHERE c1 > '2014-11-05 08:27:00') AS f_c2
                      FROM timestamp_tbl
                      GROUP BY id"""


class aggtst_timestamp_max_where1_gby(TstView):
    def __init__(self):
        # Validated on Postgres
        self.data = [
            {"id": 0, "f_c1": datetime.datetime(2014, 11, 5, 8, 27, 0), "f_c2": datetime.datetime(2024, 12, 5, 12, 45, 0)},
            {"id": 1, "f_c1": datetime.datetime(1969, 3, 17, 11, 32, 0), "f_c2": datetime.datetime(2023, 2, 26, 18, 0, 0)},
        ]
        self.sql = """CREATE MATERIALIZED VIEW timestamp_max_where1_gby AS SELECT
                      id, MAX(c1) FILTER (WHERE c1 < '2020-06-21 14:00:00') AS f_c1, MAX(c2) FILTER (WHERE c2 > '2014-11-05 16:30:00') AS f_c2
                      FROM timestamp1_tbl
                      GROUP BY id"""
