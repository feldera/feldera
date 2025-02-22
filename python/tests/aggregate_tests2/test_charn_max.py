from tests.aggregate_tests.aggtst_base import TstView


class aggtst_charn_max_value(TstView):
    def __init__(self):
        # Validated on Postgres
        self.data = [{"c1": "hello  ", "c2": "variabl"}]
        self.sql = """CREATE MATERIALIZED VIEW charn_max AS SELECT
                      MAX(f_c1) AS c1, MAX(f_c2) AS c2
                      FROM atbl_charn"""


class aggtst_charn_max_gby(TstView):
    def __init__(self):
        # Validated on Postgres
        self.data = [
            {"id": 0, "c1": "hello  ", "c2": "fred   "},
            {"id": 1, "c1": "hello  ", "c2": "variabl"},
        ]
        self.sql = """CREATE MATERIALIZED VIEW charn_max_gby AS SELECT
                      id, MAX(f_c1) AS c1, MAX(f_c2) AS c2
                      FROM atbl_charn
                      GROUP BY id"""


class aggtst_charn_max_distinct(TstView):
    def __init__(self):
        # Validated on Postgres
        self.data = [{"c1": "hello  ", "c2": "variabl"}]
        self.sql = """CREATE MATERIALIZED VIEW charn_max_distinct AS SELECT
                      MAX(DISTINCT f_c1) AS c1, MAX(DISTINCT f_c2) AS c2
                      FROM atbl_charn"""


class aggtst_charn_max_distinct_gby(TstView):
    def __init__(self):
        # Validated on Postgres
        self.data = [
            {"id": 0, "c1": "hello  ", "c2": "fred   "},
            {"id": 1, "c1": "hello  ", "c2": "variabl"},
        ]
        self.sql = """CREATE MATERIALIZED VIEW charn_max_distinct_gby AS SELECT
                      id, MAX(DISTINCT f_c1) AS c1, MAX(DISTINCT f_c2) AS c2
                      FROM atbl_charn
                      GROUP BY id"""


class aggtst_charn_max_where(TstView):
    def __init__(self):
        # Validated on Postgres
        self.data = [{"c1": "hello  ", "c2": "variabl"}]
        self.sql = """CREATE MATERIALIZED VIEW charn_max_where AS SELECT
                      MAX(f_c1) FILTER(WHERE f_c1 != '%hello  %') AS c1, MAX(f_c2) FILTER(WHERE f_c1 != '%hello  %') AS c2
                      FROM atbl_charn"""


class aggtst_charn_max_where_gby(TstView):
    def __init__(self):
        # Validated on Postgres
        self.data = [
            {"id": 0, "c1": "hello  ", "c2": "fred   "},
            {"id": 1, "c1": "hello  ", "c2": "variabl"},
        ]
        self.sql = """CREATE MATERIALIZED VIEW charn_max_where_gby AS SELECT
                      id, MAX(f_c1) FILTER(WHERE f_c1 != '%hello  %') AS c1, MAX(f_c2) FILTER(WHERE f_c1 != '%hello  %') AS c2
                      FROM atbl_charn
                      GROUP BY id"""
