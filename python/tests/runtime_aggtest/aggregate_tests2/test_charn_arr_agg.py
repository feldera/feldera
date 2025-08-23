from tests.runtime_aggtest.aggtst_base import TstView


class aggtst_charn_array_agg_value(TstView):
    def __init__(self):
        # Validated on Postgres
        self.data = [
            {
                "c1": [None, "@abc   ", "hello  ", "hello  "],
                "c2": ["abc   d", "variabl", "exampl ", "fred   "],
            }
        ]
        self.sql = """CREATE MATERIALIZED VIEW charn_array_agg AS SELECT
                      ARRAY_AGG(f_c1) AS c1, ARRAY_AGG(f_c2) AS c2
                      FROM atbl_charn"""


class aggtst_charn_array_agg_gby(TstView):
    def __init__(self):
        # Validated on Postgres
        self.data = [
            {"id": 0, "c1": [None, "hello  "], "c2": ["abc   d", "fred   "]},
            {"id": 1, "c1": ["@abc   ", "hello  "], "c2": ["variabl", "exampl "]},
        ]
        self.sql = """CREATE MATERIALIZED VIEW chan_array_agg_gby AS SELECT
                      id, ARRAY_AGG(f_c1) AS c1, ARRAY_AGG(f_c2) AS c2
                      FROM atbl_charn
                      GROUP BY id"""


class aggtst_charn_array_agg_distinct(TstView):
    def __init__(self):
        # Validated on Postgres
        self.data = [
            {
                "c1": [None, "@abc   ", "hello  "],
                "c2": ["abc   d", "exampl ", "fred   ", "variabl"],
            }
        ]
        self.sql = """CREATE MATERIALIZED VIEW charn_array_agg_distinct AS SELECT
                      ARRAY_AGG(DISTINCT f_c1) AS c1, ARRAY_AGG(DISTINCT f_c2) AS c2
                      FROM atbl_charn"""


class aggtst_charn_array_agg_distinct_gby(TstView):
    def __init__(self):
        # Validated on Postgres
        self.data = [
            {"id": 0, "c1": [None, "hello  "], "c2": ["abc   d", "fred   "]},
            {"id": 1, "c1": ["@abc   ", "hello  "], "c2": ["exampl ", "variabl"]},
        ]
        self.sql = """CREATE MATERIALIZED VIEW charn_array_agg_distinct_gby AS SELECT
                      id, ARRAY_AGG(DISTINCT f_c1) AS c1, ARRAY_AGG(DISTINCT f_c2) AS c2
                      FROM atbl_charn
                      GROUP BY id"""


class aggtst_charn_array_agg_where(TstView):
    def __init__(self):
        # Validated on Postgres
        self.data = [
            {
                "c1": ["@abc   ", "hello  ", "hello  "],
                "c2": ["variabl", "exampl ", "fred   "],
            }
        ]
        self.sql = """CREATE MATERIALIZED VIEW charn_array_where AS SELECT
                      ARRAY_AGG(f_c1) FILTER(WHERE f_c1 != '%hello  %') AS c1, ARRAY_AGG(f_c2) FILTER(WHERE f_c1 != '%hello  %') AS c2
                      FROM atbl_charn"""


class aggtst_charn_array_agg_where_gby(TstView):
    def __init__(self):
        # Validated on Postgres
        self.data = [
            {"id": 0, "c1": ["hello  "], "c2": ["fred   "]},
            {"id": 1, "c1": ["@abc   ", "hello  "], "c2": ["variabl", "exampl "]},
        ]
        self.sql = """CREATE MATERIALIZED VIEW charn_array_where_gby AS SELECT
                      id, ARRAY_AGG(f_c1) FILTER(WHERE f_c1 != '%hello  %') AS c1, ARRAY_AGG(f_c2) FILTER(WHERE f_c1 != '%hello  %') AS c2
                      FROM atbl_charn
                      GROUP BY id"""
