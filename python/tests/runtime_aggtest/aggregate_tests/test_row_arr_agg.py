from tests.runtime_aggtest.aggtst_base import TstView


class aggtst_row_array_agg_value(TstView):
    def __init__(self):
        # Validated on Postgres
        self.data = [
            {
                "c1": [
                    {"EXPR$0": None, "EXPR$1": "adios"},
                    {"EXPR$0": "elo", "EXPR$1": "ciao"},
                    {"EXPR$0": "elo", "EXPR$1": "ciao"},
                    {"EXPR$0": "hi", "EXPR$1": "hiya"},
                    {"EXPR$0": "ola", "EXPR$1": "ciao"},
                ],
                "c2": [4, 2, 2, 7, 3],
            }
        ]
        self.sql = """CREATE MATERIALIZED VIEW row_array_agg AS SELECT
                      ARRAY_AGG(ROW(c2, c3)) AS c1, ARRAY_AGG(c1) AS c2
                      FROM row_tbl"""


class aggtst_row_array_agg_gby(TstView):
    def __init__(self):
        # Validated on Postgres
        self.data = [
            {
                "id": 0,
                "c1": [
                    {"EXPR$0": None, "EXPR$1": "adios"},
                    {"EXPR$0": "ola", "EXPR$1": "ciao"},
                ],
                "c2": [4, 3],
            },
            {
                "id": 1,
                "c1": [
                    {"EXPR$0": "elo", "EXPR$1": "ciao"},
                    {"EXPR$0": "elo", "EXPR$1": "ciao"},
                    {"EXPR$0": "hi", "EXPR$1": "hiya"},
                ],
                "c2": [2, 2, 7],
            },
        ]
        self.sql = """CREATE MATERIALIZED VIEW row_array_agg_gby AS SELECT
                      id, ARRAY_AGG(ROW(c2, c3)) AS c1, ARRAY_AGG(c1) AS c2
                      FROM row_tbl
                      GROUP BY id"""


class aggtst_row_array_agg_distinct(TstView):
    def __init__(self):
        # Validated on Postgres
        self.data = [
            {
                "c1": [
                    {"EXPR$0": None, "EXPR$1": "adios"},
                    {"EXPR$0": "elo", "EXPR$1": "ciao"},
                    {"EXPR$0": "hi", "EXPR$1": "hiya"},
                    {"EXPR$0": "ola", "EXPR$1": "ciao"},
                ],
                "c2": [2, 3, 4, 7],
            }
        ]
        self.sql = """CREATE MATERIALIZED VIEW time_row_agg_distinct AS SELECT
                      ARRAY_AGG(DISTINCT ROW(c2, c3)) AS c1, ARRAY_AGG(DISTINCT c1) AS c2
                      FROM row_tbl"""


class aggtst_row_array_agg_distinct_gby(TstView):
    def __init__(self):
        # Validated on Postgres
        self.data = [
            {
                "id": 0,
                "c1": [
                    {"EXPR$0": None, "EXPR$1": "adios"},
                    {"EXPR$0": "ola", "EXPR$1": "ciao"},
                ],
                "c2": [3, 4],
            },
            {
                "id": 1,
                "c1": [
                    {"EXPR$0": "elo", "EXPR$1": "ciao"},
                    {"EXPR$0": "hi", "EXPR$1": "hiya"},
                ],
                "c2": [2, 7],
            },
        ]
        self.sql = """CREATE MATERIALIZED VIEW row_array_agg_distinct_gby AS SELECT
                      id, ARRAY_AGG(DISTINCT ROW(c2, c3)) AS c1, ARRAY_AGG(DISTINCT c1) AS c2
                      FROM row_tbl
                      GROUP BY id"""


class aggtst_row_array_agg_where(TstView):
    def __init__(self):
        # Validated on Postgres
        self.data = [{"f_c1": [{"EXPR$0": "hi", "EXPR$1": "hiya"}], "f_c2": [7]}]
        self.sql = """CREATE MATERIALIZED VIEW row_array_where AS SELECT
                      ARRAY_AGG(ROW(c2, c3)) FILTER(WHERE c2 < c3) AS f_c1,
                      ARRAY_AGG(c1) FILTER(WHERE c2 < c3) AS f_c2
                      FROM row_tbl"""


class aggtst_row_array_agg_where_gby(TstView):
    def __init__(self):
        # Validated on Postgres
        self.data = [
            {"id": 0, "f_c1": [], "f_c2": []},
            {"id": 1, "f_c1": [{"EXPR$0": "hi", "EXPR$1": "hiya"}], "f_c2": [7]},
        ]
        self.sql = """CREATE MATERIALIZED VIEW row_array_where_gby AS SELECT
                      id,
                      ARRAY_AGG(ROW(c2, c3)) FILTER(WHERE c2 < c3) AS f_c1,
                      ARRAY_AGG(c1) FILTER(WHERE c2 < c3) AS f_c2
                      FROM row_tbl
                      GROUP BY id"""
