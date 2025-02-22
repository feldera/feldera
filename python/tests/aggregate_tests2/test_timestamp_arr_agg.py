from tests.aggregate_tests.aggtst_base import TstView


class aggtst_timestamp_array_agg_value(TstView):
    def __init__(self):
        # Validated on Postgres
        self.data = [
            {
                "c1": [
                    "2014-11-05T08:27:00",
                    "2020-06-21T14:00:00",
                    "2020-06-21T14:00:00",
                    "2024-12-05T09:15:00",
                ],
                "c2": [
                    "2024-12-05T12:45:00",
                    None,
                    "2023-02-26T18:00:00",
                    "2014-11-05T16:30:00",
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
                "c1": ["2014-11-05T08:27:00", "2020-06-21T14:00:00"],
                "c2": ["2024-12-05T12:45:00", None],
            },
            {
                "id": 1,
                "c1": ["2020-06-21T14:00:00", "2024-12-05T09:15:00"],
                "c2": ["2023-02-26T18:00:00", "2014-11-05T16:30:00"],
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
                    "2014-11-05T08:27:00",
                    "2020-06-21T14:00:00",
                    "2024-12-05T09:15:00",
                ],
                "c2": [
                    None,
                    "2014-11-05T16:30:00",
                    "2023-02-26T18:00:00",
                    "2024-12-05T12:45:00",
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
                "c1": ["2014-11-05T08:27:00", "2020-06-21T14:00:00"],
                "c2": [None, "2024-12-05T12:45:00"],
            },
            {
                "id": 1,
                "c1": ["2020-06-21T14:00:00", "2024-12-05T09:15:00"],
                "c2": ["2014-11-05T16:30:00", "2023-02-26T18:00:00"],
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
                    "2020-06-21T14:00:00",
                    "2020-06-21T14:00:00",
                    "2024-12-05T09:15:00",
                ],
                "f_c2": [None, "2023-02-26T18:00:00", "2014-11-05T16:30:00"],
            }
        ]
        self.sql = """CREATE MATERIALIZED VIEW timestamp_array_where AS SELECT
                      ARRAY_AGG(c1) FILTER(WHERE c1 > '2014-11-05 08:27:00') AS f_c1, ARRAY_AGG(c2) FILTER(WHERE c1 > '2014-11-05 08:27:00') AS f_c2
                      FROM timestamp_tbl"""


class aggtst_timestamp_array_agg_where_gby(TstView):
    def __init__(self):
        # Validated on Postgres
        self.data = [
            {"id": 0, "f_c1": ["2020-06-21T14:00:00"], "f_c2": [None]},
            {
                "id": 1,
                "f_c1": ["2020-06-21T14:00:00", "2024-12-05T09:15:00"],
                "f_c2": ["2023-02-26T18:00:00", "2014-11-05T16:30:00"],
            },
        ]
        self.sql = """CREATE MATERIALIZED VIEW timestamp_array_where_gby AS SELECT
                      id, ARRAY_AGG(c1) FILTER(WHERE c1 > '2014-11-05 08:27:00') AS f_c1, ARRAY_AGG(c2) FILTER(WHERE c1 > '2014-11-05 08:27:00') AS f_c2
                      FROM timestamp_tbl
                      GROUP BY id"""
