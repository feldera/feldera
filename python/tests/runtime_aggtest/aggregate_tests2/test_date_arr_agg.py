from tests.runtime_aggtest.aggtst_base import TstView
import datetime

class aggtst_date_array_agg_value(TstView):
    def __init__(self):
        # Validated on Postgres
        self.data = [
            {
                "c1": [
                    datetime.date(1969, 3, 17),
                    datetime.date(2014, 11, 5),
                    datetime.date(2020, 6, 21),
                    datetime.date(2020, 6, 21),
                    datetime.date(2024, 12, 5),
                ],
                "c2": [datetime.date(2015, 9, 7), datetime.date(2024, 12, 5), None, datetime.date(2023, 2, 26), datetime.date(2014, 11, 5)],
            }
        ]
        self.sql = """CREATE MATERIALIZED VIEW date_array_agg AS SELECT
                      ARRAY_AGG(c1) AS c1, ARRAY_AGG(c2) AS c2
                      FROM date_tbl"""


class aggtst_date_array_agg_gby(TstView):
    def __init__(self):
        # Validated on Postgres
        self.data = [
            {"id": 0, "c1": [datetime.date(2014, 11, 5), datetime.date(2020, 6, 21)], "c2": [datetime.date(2024, 12, 5), None]},
            {
                "id": 1,
                "c1": [datetime.date(1969, 3, 17), datetime.date(2020, 6, 21), datetime.date(2024, 12, 5)],
                "c2": [datetime.date(2015, 9, 7), datetime.date(2023, 2, 26), datetime.date(2014, 11, 5)],
            },
        ]
        self.sql = """CREATE MATERIALIZED VIEW date_array_agg_gby AS SELECT
                      id, ARRAY_AGG(c1) AS c1, ARRAY_AGG(c2) AS c2
                      FROM date_tbl
                      GROUP BY id"""


class aggtst_date_array_agg_distinct(TstView):
    def __init__(self):
        # Validated on Postgres
        self.data = [
            {
                "c1": [datetime.date(1969, 3, 17), datetime.date(2014, 11, 5), datetime.date(2020, 6, 21), datetime.date(2024, 12, 5)],
                "c2": [None, datetime.date(2014, 11, 5), datetime.date(2015, 9, 7), datetime.date(2023, 2, 26), datetime.date(2024, 12, 5)],
            }
        ]
        self.sql = """CREATE MATERIALIZED VIEW date_array_agg_distinct AS SELECT
                      ARRAY_AGG(DISTINCT c1) AS c1, ARRAY_AGG(DISTINCT c2) AS c2
                      FROM date_tbl"""


class aggtst_date_array_agg_distinct_gby(TstView):
    def __init__(self):
        # Validated on Postgres
        self.data = [
            {"id": 0, "c1": [datetime.date(2014, 11, 5), datetime.date(2020, 6, 21)], "c2": [None, datetime.date(2024, 12, 5)]},
            {
                "id": 1,
                "c1": [datetime.date(1969, 3, 17), datetime.date(2020, 6, 21), datetime.date(2024, 12, 5)],
                "c2": [datetime.date(2014, 11, 5), datetime.date(2015, 9, 7), datetime.date(2023, 2, 26)],
            },
        ]
        self.sql = """CREATE MATERIALIZED VIEW date_array_agg_distinct_gby AS SELECT
                      id, ARRAY_AGG(DISTINCT c1) AS c1, ARRAY_AGG(DISTINCT c2) AS c2
                      FROM date_tbl
                      GROUP BY id"""


class aggtst_date_array_agg_where(TstView):
    def __init__(self):
        # Validated on Postgres
        self.data = [
            {
                "f_c1": [datetime.date(2020, 6, 21), datetime.date(2020, 6, 21), datetime.date(2024, 12, 5)],
                "f_c2": [datetime.date(2015, 9, 7), datetime.date(2024, 12, 5), datetime.date(2023, 2, 26)],
            }
        ]
        self.sql = """CREATE MATERIALIZED VIEW date_array_where AS SELECT
                      ARRAY_AGG(c1) FILTER(WHERE c1 > '2014-11-05') AS f_c1, ARRAY_AGG(c2) FILTER(WHERE c2 > '2014-11-05') AS f_c2
                      FROM date_tbl"""


class aggtst_date_array_agg_where_gby(TstView):
    def __init__(self):
        # Validated on Postgres
        self.data = [
            {"id": 0, "f_c1": [datetime.date(2020, 6, 21)], "f_c2": [datetime.date(2024, 12, 5)]},
            {
                "id": 1,
                "f_c1": [datetime.date(2020, 6, 21), datetime.date(2024, 12, 5)],
                "f_c2": [datetime.date(2015, 9, 7), datetime.date(2023, 2, 26)],
            },
        ]
        self.sql = """CREATE MATERIALIZED VIEW date_array_where_gby AS SELECT
                      id, ARRAY_AGG(c1) FILTER(WHERE c1 > '2014-11-05') AS f_c1, ARRAY_AGG(c2) FILTER(WHERE c2 > '2014-11-05') AS f_c2
                      FROM date_tbl
                      GROUP BY id"""
