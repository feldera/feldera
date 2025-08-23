from tests.runtime_aggtest.aggtst_base import TstView


class aggtst_date_array_agg_value(TstView):
    def __init__(self):
        # Validated on Postgres
        self.data = [
            {
                "c1": [
                    "1969-03-17",
                    "2014-11-05",
                    "2020-06-21",
                    "2020-06-21",
                    "2024-12-05",
                ],
                "c2": ["2015-09-07", "2024-12-05", None, "2023-02-26", "2014-11-05"],
            }
        ]
        self.sql = """CREATE MATERIALIZED VIEW date_array_agg AS SELECT
                      ARRAY_AGG(c1) AS c1, ARRAY_AGG(c2) AS c2
                      FROM date_tbl"""


class aggtst_date_array_agg_gby(TstView):
    def __init__(self):
        # Validated on Postgres
        self.data = [
            {"id": 0, "c1": ["2014-11-05", "2020-06-21"], "c2": ["2024-12-05", None]},
            {
                "id": 1,
                "c1": ["1969-03-17", "2020-06-21", "2024-12-05"],
                "c2": ["2015-09-07", "2023-02-26", "2014-11-05"],
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
                "c1": ["1969-03-17", "2014-11-05", "2020-06-21", "2024-12-05"],
                "c2": [None, "2014-11-05", "2015-09-07", "2023-02-26", "2024-12-05"],
            }
        ]
        self.sql = """CREATE MATERIALIZED VIEW date_array_agg_distinct AS SELECT
                      ARRAY_AGG(DISTINCT c1) AS c1, ARRAY_AGG(DISTINCT c2) AS c2
                      FROM date_tbl"""


class aggtst_date_array_agg_distinct_gby(TstView):
    def __init__(self):
        # Validated on Postgres
        self.data = [
            {"id": 0, "c1": ["2014-11-05", "2020-06-21"], "c2": [None, "2024-12-05"]},
            {
                "id": 1,
                "c1": ["1969-03-17", "2020-06-21", "2024-12-05"],
                "c2": ["2014-11-05", "2015-09-07", "2023-02-26"],
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
                "f_c1": ["2020-06-21", "2020-06-21", "2024-12-05"],
                "f_c2": ["2015-09-07", "2024-12-05", "2023-02-26"],
            }
        ]
        self.sql = """CREATE MATERIALIZED VIEW date_array_where AS SELECT
                      ARRAY_AGG(c1) FILTER(WHERE c1 > '2014-11-05') AS f_c1, ARRAY_AGG(c2) FILTER(WHERE c2 > '2014-11-05') AS f_c2
                      FROM date_tbl"""


class aggtst_date_array_agg_where_gby(TstView):
    def __init__(self):
        # Validated on Postgres
        self.data = [
            {"id": 0, "f_c1": ["2020-06-21"], "f_c2": ["2024-12-05"]},
            {
                "id": 1,
                "f_c1": ["2020-06-21", "2024-12-05"],
                "f_c2": ["2015-09-07", "2023-02-26"],
            },
        ]
        self.sql = """CREATE MATERIALIZED VIEW date_array_where_gby AS SELECT
                      id, ARRAY_AGG(c1) FILTER(WHERE c1 > '2014-11-05') AS f_c1, ARRAY_AGG(c2) FILTER(WHERE c2 > '2014-11-05') AS f_c2
                      FROM date_tbl
                      GROUP BY id"""
