from tests.aggregate_tests.aggtst_base import TstView


class aggtst_array_max_value(TstView):
    def __init__(self):
        # Validated on Postgres
        self.data = [{"c1": [49], "c2": [99]}]
        self.sql = """CREATE MATERIALIZED VIEW array_max AS SELECT
                      MAX(c1) AS c1, MAX(c2) AS c2
                      FROM array_tbl"""


class aggtst_array_max_gby(TstView):
    def __init__(self):
        # Validated on Postgres
        self.data = [
            {"id": 0, "c1": [23, 56, 16], "c2": [55, 66, None]},
            {"id": 1, "c1": [49], "c2": [99]},
        ]
        self.sql = """CREATE MATERIALIZED VIEW array_max_gby AS SELECT
                      id, MAX(c1) AS c1, MAX(c2) AS c2
                      FROM array_tbl
                      GROUP BY id"""


class aggtst_array_max_distinct(TstView):
    def __init__(self):
        # Validated on Postgres
        self.data = [{"c1": [49], "c2": [99]}]
        self.sql = """CREATE MATERIALIZED VIEW array_max_distinct AS SELECT
                      MAX(DISTINCT c1) AS c1, MAX(DISTINCT c2) AS c2
                      FROM array_tbl"""


class aggtst_array_max_distinct_gby(TstView):
    def __init__(self):
        # Validated on Postgres
        self.data = [
            {"id": 0, "c1": [23, 56, 16], "c2": [55, 66, None]},
            {"id": 1, "c1": [49], "c2": [99]},
        ]
        self.sql = """CREATE MATERIALIZED VIEW array_max_distinct_gby AS SELECT
                      id, MAX(DISTINCT c1) AS c1, MAX(DISTINCT c2) AS c2
                      FROM array_tbl
                      GROUP BY id"""


class aggtst_array_max_where(TstView):
    def __init__(self):
        # Validated on Postgres
        self.data = [{"c1": [23, 56, 16], "c2": [99]}]
        self.sql = """CREATE MATERIALIZED VIEW array_max_where AS SELECT
                      MAX(c1) FILTER(WHERE c1 < c2) AS c1, MAX(c2) FILTER(WHERE c1 < c2) AS c2
                      FROM array_tbl"""


class aggtst_array_max_where_gby(TstView):
    def __init__(self):
        # Validated on Postgres
        self.data = [
            {"id": 0, "c1": [23, 56, 16], "c2": [55, 66, None]},
            {"id": 1, "c1": [23, 56, 16], "c2": [99]},
        ]
        self.sql = """CREATE MATERIALIZED VIEW array_max_where_gby AS SELECT
                      id, MAX(c1) FILTER(WHERE c1 < c2) AS c1, MAX(c2) FILTER(WHERE c1 < c2) AS c2
                      FROM array_tbl
                      GROUP BY id"""
