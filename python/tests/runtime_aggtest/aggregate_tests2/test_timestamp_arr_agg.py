from tests.runtime_aggtest.aggtst_base import TstView
import datetime


class aggtst_timestamp_array_agg_value(TstView):
    def __init__(self):
        # Validated on Postgres
        self.data = [
            {
                "c1": [
                    datetime.datetime(2014, 11, 5, 8, 27, 0),
                    datetime.datetime(2020, 6, 21, 14, 0, 0),
                    datetime.datetime(2020, 6, 21, 14, 0, 0),
                    datetime.datetime(2024, 12, 5, 9, 15, 0),
                ],
                "c2": [
                    datetime.datetime(2024, 12, 5, 12, 45, 0),
                    None,
                    datetime.datetime(2023, 2, 26, 18, 0, 0),
                    datetime.datetime(2014, 11, 5, 16, 30, 0),
                ],
            }
        ]
        self.sql = """CREATE MATERIALIZED VIEW timestamp_array_agg AS SELECT
                      ARRAY_AGG(c1) AS c1, ARRAY_AGG(c2) AS c2
                      FROM timestamp_tbl"""


class aggtst_timestamp_array_agg_gby(TstView):
    def __init__(self):
        # Validated on Postgres
        self.data = [
            {
                "id": 0,
                "c1": [
                    datetime.datetime(2014, 11, 5, 8, 27, 0),
                    datetime.datetime(2020, 6, 21, 14, 0, 0),
                ],
                "c2": [datetime.datetime(2024, 12, 5, 12, 45, 0), None],
            },
            {
                "id": 1,
                "c1": [
                    datetime.datetime(2020, 6, 21, 14, 0, 0),
                    datetime.datetime(2024, 12, 5, 9, 15, 0),
                ],
                "c2": [
                    datetime.datetime(2023, 2, 26, 18, 0, 0),
                    datetime.datetime(2014, 11, 5, 16, 30, 0),
                ],
            },
        ]
        self.sql = """CREATE MATERIALIZED VIEW timestamp_array_agg_gby AS SELECT
                      id, ARRAY_AGG(c1) AS c1, ARRAY_AGG(c2) AS c2
                      FROM timestamp_tbl
                      GROUP BY id"""


class aggtst_timestamp_array_agg_distinct(TstView):
    def __init__(self):
        # Validated on Postgres
        self.data = [
            {
                "c1": [
                    datetime.datetime(2014, 11, 5, 8, 27, 0),
                    datetime.datetime(2020, 6, 21, 14, 0, 0),
                    datetime.datetime(2024, 12, 5, 9, 15, 0),
                ],
                "c2": [
                    None,
                    datetime.datetime(2014, 11, 5, 16, 30, 0),
                    datetime.datetime(2023, 2, 26, 18, 0, 0),
                    datetime.datetime(2024, 12, 5, 12, 45, 0),
                ],
            }
        ]
        self.sql = """CREATE MATERIALIZED VIEW timestamp_array_agg_distinct AS SELECT
                      ARRAY_AGG(DISTINCT c1) AS c1, ARRAY_AGG(DISTINCT c2) AS c2
                      FROM timestamp_tbl"""


class aggtst_timestamp_array_agg_distinct_gby(TstView):
    def __init__(self):
        # Validated on Postgres
        self.data = [
            {
                "id": 0,
                "c1": [
                    datetime.datetime(2014, 11, 5, 8, 27, 0),
                    datetime.datetime(2020, 6, 21, 14, 0, 0),
                ],
                "c2": [None, datetime.datetime(2024, 12, 5, 12, 45, 0)],
            },
            {
                "id": 1,
                "c1": [
                    datetime.datetime(2020, 6, 21, 14, 0, 0),
                    datetime.datetime(2024, 12, 5, 9, 15, 0),
                ],
                "c2": [
                    datetime.datetime(2014, 11, 5, 16, 30, 0),
                    datetime.datetime(2023, 2, 26, 18, 0, 0),
                ],
            },
        ]
        self.sql = """CREATE MATERIALIZED VIEW timestamp_array_agg_distinct_gby AS SELECT
                      id, ARRAY_AGG(DISTINCT c1) AS c1, ARRAY_AGG(DISTINCT c2) AS c2
                      FROM timestamp_tbl
                      GROUP BY id"""


class aggtst_timestamp_array_agg_where(TstView):
    def __init__(self):
        # Validated on Postgres
        self.data = [
            {
                "f_c1": [
                    datetime.datetime(2020, 6, 21, 14, 0, 0),
                    datetime.datetime(2020, 6, 21, 14, 0, 0),
                    datetime.datetime(2024, 12, 5, 9, 15, 0),
                ],
                "f_c2": [
                    None,
                    datetime.datetime(2023, 2, 26, 18, 0, 0),
                    datetime.datetime(2014, 11, 5, 16, 30, 0),
                ],
            }
        ]
        self.sql = """CREATE MATERIALIZED VIEW timestamp_array_where AS SELECT
                      ARRAY_AGG(c1) FILTER(WHERE c1 > '2014-11-05 08:27:00') AS f_c1, ARRAY_AGG(c2) FILTER(WHERE c1 > '2014-11-05 08:27:00') AS f_c2
                      FROM timestamp_tbl"""


class aggtst_timestamp_array_agg_where_gby(TstView):
    def __init__(self):
        # Validated on Postgres
        self.data = [
            {
                "id": 0,
                "f_c1": [datetime.datetime(2020, 6, 21, 14, 0, 0)],
                "f_c2": [None],
            },
            {
                "id": 1,
                "f_c1": [
                    datetime.datetime(2020, 6, 21, 14, 0, 0),
                    datetime.datetime(2024, 12, 5, 9, 15, 0),
                ],
                "f_c2": [
                    datetime.datetime(2023, 2, 26, 18, 0, 0),
                    datetime.datetime(2014, 11, 5, 16, 30, 0),
                ],
            },
        ]
        self.sql = """CREATE MATERIALIZED VIEW timestamp_array_where_gby AS SELECT
                      id, ARRAY_AGG(c1) FILTER(WHERE c1 > '2014-11-05 08:27:00') AS f_c1, ARRAY_AGG(c2) FILTER(WHERE c1 > '2014-11-05 08:27:00') AS f_c2
                      FROM timestamp_tbl
                      GROUP BY id"""
