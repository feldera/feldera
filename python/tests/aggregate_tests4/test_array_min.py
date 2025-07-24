from tests.aggregate_tests.aggtst_base import TstView


class aggtst_array_min_value(TstView):
    def __init__(self):
        # Validated on Postgres
        self.data = [{"c1": [12, 22], "c2": [32, 34, 22, 12]}]
        self.sql = """CREATE MATERIALIZED VIEW array_min AS SELECT
                      MIN(c1) AS c1, MIN(c2) AS c2
                      FROM array_tbl"""


class aggtst_array_min_gby(TstView):
    def __init__(self):
        # Validated on Postgres
        self.data = [
            {"id": 0, "c1": [12, 22], "c2": [55, 66, None]},
            {"id": 1, "c1": [23, 56, 16], "c2": [32, 34, 22, 12]},
        ]
        self.sql = """CREATE MATERIALIZED VIEW array_min_gby AS SELECT
                      id, MIN(c1) AS c1, MIN(c2) AS c2
                      FROM array_tbl
                      GROUP BY id"""


class aggtst_array_min_distinct(TstView):
    def __init__(self):
        # Validated on Postgres
        self.data = [{"c1": [12, 22], "c2": [32, 34, 22, 12]}]
        self.sql = """CREATE MATERIALIZED VIEW array_min_distinct AS SELECT
                      MIN(DISTINCT c1) AS c1, MIN(DISTINCT c2) AS c2
                      FROM array_tbl"""


class aggtst_array_min_distinct_gby(TstView):
    def __init__(self):
        # Validated on Postgres
        self.data = [
            {"id": 0, "c1": [12, 22], "c2": [55, 66, None]},
            {"id": 1, "c1": [23, 56, 16], "c2": [32, 34, 22, 12]},
        ]
        self.sql = """CREATE MATERIALIZED VIEW array_min_distinct_gby AS SELECT
                      id, MIN(DISTINCT c1) AS c1, MIN(DISTINCT c2) AS c2
                      FROM array_tbl
                      GROUP BY id"""


class aggtst_array_min_where(TstView):
    def __init__(self):
        # Validated on Postgres
        self.data = [{"c1": [23, 56, 16], "c2": [55, 66, None]}]
        self.sql = """CREATE MATERIALIZED VIEW array_min_where AS SELECT
                      MIN(c1) FILTER(WHERE c1 < c2) AS c1, MIN(c2) FILTER(WHERE c1 < c2) AS c2
                      FROM array_tbl"""


class aggtst_array_min_where_gby(TstView):
    def __init__(self):
        # Validated on Postgres
        self.data = [
            {"id": 0, "c1": [23, 56, 16], "c2": [55, 66, None]},
            {"id": 1, "c1": [23, 56, 16], "c2": [99]},
        ]
        self.sql = """CREATE MATERIALIZED VIEW array_min_where_gby AS SELECT
                      id, MIN(c1) FILTER(WHERE c1 < c2) AS c1, MIN(c2) FILTER(WHERE c1 < c2) AS c2
                      FROM array_tbl
                      GROUP BY id"""
