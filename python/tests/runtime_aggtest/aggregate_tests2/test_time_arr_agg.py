from tests.runtime_aggtest.aggtst_base import TstView
import datetime

class aggtst_time_array_agg_value(TstView):
    def __init__(self):
        # Validated on Postgres
        self.data = [
            {
                "c1": [datetime.time(8, 30, 0), datetime.time(9, 15, 0), datetime.time(14, 0, 0), datetime.time(14, 0, 0)],
                "c2": [datetime.time(12, 45, 0), datetime.time(16, 30, 0), None, datetime.time(18, 0, 0)],
            }
        ]
        self.sql = """CREATE MATERIALIZED VIEW time_array_agg AS SELECT
                      ARRAY_AGG(c1) AS c1, ARRAY_AGG(c2) AS c2
                      FROM time_tbl"""


class aggtst_time_array_agg_gby(TstView):
    def __init__(self):
        # Validated on Postgres
        self.data = [
            {"id": 0, "c1": [datetime.time(8, 30, 0), datetime.time(14, 0, 0)], "c2": [datetime.time(12, 45, 0), None]},
            {"id": 1, "c1": [datetime.time(9, 15, 0), datetime.time(14, 0, 0)], "c2": [datetime.time(16, 30, 0), datetime.time(18, 0, 0)]},
        ]
        self.sql = """CREATE MATERIALIZED VIEW time_array_agg_gby AS SELECT
                      id, ARRAY_AGG(c1) AS c1, ARRAY_AGG(c2) AS c2
                      FROM time_tbl
                      GROUP BY id"""


class aggtst_time_array_agg_distinct(TstView):
    def __init__(self):
        # Validated on Postgres
        self.data = [
            {
                "c1": [datetime.time(8, 30, 0), datetime.time(9, 15, 0), datetime.time(14, 0, 0)],
                "c2": [None, datetime.time(12, 45, 0), datetime.time(16, 30, 0), datetime.time(18, 0, 0)],
            }
        ]
        self.sql = """CREATE MATERIALIZED VIEW time_array_agg_distinct AS SELECT
                      ARRAY_AGG(DISTINCT c1) AS c1, ARRAY_AGG(DISTINCT c2) AS c2
                      FROM time_tbl"""


class aggtst_time_array_agg_distinct_gby(TstView):
    def __init__(self):
        # Validated on Postgres
        self.data = [
            {"id": 0, "c1": [datetime.time(8, 30, 0), datetime.time(14, 0, 0)], "c2": [None, datetime.time(12, 45, 0)]},
            {"id": 1, "c1": [datetime.time(9, 15, 0), datetime.time(14, 0, 0)], "c2": [datetime.time(16, 30, 0), datetime.time(18, 0, 0)]},
        ]
        self.sql = """CREATE MATERIALIZED VIEW time_array_agg_distinct_gby AS SELECT
                      id, ARRAY_AGG(DISTINCT c1) AS c1, ARRAY_AGG(DISTINCT c2) AS c2
                      FROM time_tbl
                      GROUP BY id"""


class aggtst_time_array_agg_where(TstView):
    def __init__(self):
        # Validated on Postgres
        self.data = [
            {
                "f_c1": [datetime.time(9, 15, 0), datetime.time(14, 0, 0), datetime.time(14, 0, 0)],
                "f_c2": [datetime.time(16, 30, 0), None, datetime.time(18, 0, 0)],
            }
        ]
        self.sql = """CREATE MATERIALIZED VIEW time_array_where AS SELECT
                      ARRAY_AGG(c1) FILTER(WHERE c1 > '08:30:00') AS f_c1, ARRAY_AGG(c2) FILTER(WHERE c1 > '08:30:00') AS f_c2
                      FROM time_tbl"""


class aggtst_time_array_agg_where_gby(TstView):
    def __init__(self):
        # Validated on Postgres
        self.data = [
            {"id": 0, "f_c1": [datetime.time(14, 0, 0)], "f_c2": [None]},
            {
                "id": 1,
                "f_c1": [datetime.time(9, 15, 0), datetime.time(14, 0, 0)],
                "f_c2": [datetime.time(16, 30, 0), datetime.time(18, 0, 0)],
            },
        ]
        self.sql = """CREATE MATERIALIZED VIEW time_array_where_gby AS SELECT
                      id, ARRAY_AGG(c1) FILTER(WHERE c1 > '08:30:00') AS f_c1, ARRAY_AGG(c2) FILTER(WHERE c1 > '08:30:00') AS f_c2
                      FROM time_tbl
                      GROUP BY id"""
