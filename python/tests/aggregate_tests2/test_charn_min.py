from tests.aggregate_tests.aggtst_base import TstView


class aggtst_charn_min_value(TstView):
    def __init__(self):
        # Validated on Postgres
        self.data = [{"c1": "@abc   ", "c2": "abc   d"}]
        self.sql = """CREATE MATERIALIZED VIEW charn_min AS SELECT
                      MIN(f_c1) AS c1, MIN(f_c2) AS c2
                      FROM atbl_charn"""


class aggtst_charn_min_gby(TstView):
    def __init__(self):
        # Validated on Postgres
        self.data = [
            {"id": 0, "c1": "hello  ", "c2": "abc   d"},
            {"id": 1, "c1": "@abc   ", "c2": "exampl "},
        ]
        self.sql = """CREATE MATERIALIZED VIEW charn_min_gby AS SELECT
                      id, MIN(f_c1) AS c1, MIN(f_c2) AS c2
                      FROM atbl_charn
                      GROUP BY id"""


class aggtst_charn_min_distinct(TstView):
    def __init__(self):
        # Validated on Postgres
        self.data = [{"c1": "@abc   ", "c2": "abc   d"}]
        self.sql = """CREATE MATERIALIZED VIEW charn_min_distinct AS SELECT
                      MIN(DISTINCT f_c1) AS c1, MIN(DISTINCT f_c2) AS c2
                      FROM atbl_charn"""


class aggtst_charn_min_distinct_gby(TstView):
    def __init__(self):
        # Validated on Postgres
        self.data = [
            {"id": 0, "c1": "hello  ", "c2": "abc   d"},
            {"id": 1, "c1": "@abc   ", "c2": "exampl "},
        ]
        self.sql = """CREATE MATERIALIZED VIEW charn_min_distinct_gby AS SELECT
                      id, MIN(DISTINCT f_c1) AS c1, MIN(DISTINCT f_c2) AS c2
                      FROM atbl_charn
                      GROUP BY id"""


class aggtst_charn_min_where(TstView):
    def __init__(self):
        # Validated on Postgres
        self.data = [{"c1": "@abc   ", "c2": "exampl "}]
        self.sql = """CREATE MATERIALIZED VIEW charn_min_where AS SELECT
                      MIN(f_c1) FILTER(WHERE f_c1 != '%hello  %') AS c1, MIN(f_c2) FILTER(WHERE f_c1 != '%hello  %') AS c2
                      FROM atbl_charn"""


class aggtst_charn_min_where_gby(TstView):
    def __init__(self):
        # Validated on Postgres
        self.data = [
            {"id": 0, "c1": "hello  ", "c2": "fred   "},
            {"id": 1, "c1": "@abc   ", "c2": "exampl "},
        ]
        self.sql = """CREATE MATERIALIZED VIEW charn_min_where_gby AS SELECT
                      id, MIN(f_c1) FILTER(WHERE f_c1 != '%hello  %') AS c1, MIN(f_c2) FILTER(WHERE f_c1 != '%hello  %') AS c2
                      FROM atbl_charn
                      GROUP BY id"""
