from tests.aggregate_tests.aggtst_base import TstView


class aggtst_varchar_array_agg_value(TstView):
    def __init__(self):
        # Validated on Postgres
        self.data = [
            {
                "c1": [None, "@abc", "hello", "hello"],
                "c2": ["abc   d", "variable-length", "exampl e", "fred"],
            }
        ]
        self.sql = """CREATE MATERIALIZED VIEW varchar_array_agg AS SELECT
                      ARRAY_AGG(c1) AS c1, ARRAY_AGG(c2) AS c2
                      FROM varchar_tbl"""


class aggtst_varchar_array_agg_gby(TstView):
    def __init__(self):
        # Validated on Postgres
        self.data = [
            {"id": 0, "c1": [None, "hello"], "c2": ["abc   d", "fred"]},
            {"id": 1, "c1": ["@abc", "hello"], "c2": ["variable-length", "exampl e"]},
        ]
        self.sql = """CREATE MATERIALIZED VIEW varchar_array_agg_gby AS SELECT
                      id, ARRAY_AGG(c1) AS c1, ARRAY_AGG(c2) AS c2
                      FROM varchar_tbl
                      GROUP BY id"""


class aggtst_varchar_array_agg_distinct(TstView):
    def __init__(self):
        # Validated on Postgres
        self.data = [
            {
                "c1": [None, "@abc", "hello"],
                "c2": ["abc   d", "exampl e", "fred", "variable-length"],
            }
        ]
        self.sql = """CREATE MATERIALIZED VIEW varchar_array_agg_distinct AS SELECT
                      ARRAY_AGG(DISTINCT c1) AS c1, ARRAY_AGG(DISTINCT c2) AS c2
                      FROM varchar_tbl"""


class aggtst_varchar_array_agg_distinct_gby(TstView):
    def __init__(self):
        # Validated on Postgres
        self.data = [
            {"id": 0, "c1": [None, "hello"], "c2": ["abc   d", "fred"]},
            {"id": 1, "c1": ["@abc", "hello"], "c2": ["exampl e", "variable-length"]},
        ]
        self.sql = """CREATE MATERIALIZED VIEW varchar_array_agg_distinct_gby AS SELECT
                      id, ARRAY_AGG(DISTINCT c1) AS c1, ARRAY_AGG(DISTINCT c2) AS c2
                      FROM varchar_tbl
                      GROUP BY id"""


class aggtst_varchar_array_agg_where(TstView):
    def __init__(self):
        # Validated on Postgres
        self.data = [
            {
                "f_c1": [None, "@abc", "hello"],
                "f_c2": ["abc   d", "variable-length", "exampl e"],
            }
        ]
        self.sql = """CREATE MATERIALIZED VIEW varchar_array_where AS SELECT
                      ARRAY_AGG(c1) FILTER(WHERE len(c2)>4) AS f_c1, ARRAY_AGG(c2) FILTER(WHERE len(c2)>4) AS f_c2
                      FROM varchar_tbl"""


class aggtst_varchar_array_agg_where_gby(TstView):
    def __init__(self):
        # Validated on Postgres
        self.data = [
            {"id": 0, "f_c1": [None], "f_c2": ["abc   d"]},
            {
                "id": 1,
                "f_c1": ["@abc", "hello"],
                "f_c2": ["variable-length", "exampl e"],
            },
        ]
        self.sql = """CREATE MATERIALIZED VIEW varchar_array_where_gby AS SELECT
                      id, ARRAY_AGG(c1) FILTER(WHERE len(c2)>4) AS f_c1, ARRAY_AGG(c2) FILTER(WHERE len(c2)>4) AS f_c2
                      FROM varchar_tbl
                      GROUP BY id"""
