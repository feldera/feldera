from tests.runtime_aggtest.aggtst_base import TstView


class aggtst_varcharn_array_agg_value(TstView):
    def __init__(self):
        # Validated on Postgres
        self.data = [
            {
                "c1": [None, "@abc", "hello", "hello"],
                "c2": ["abc  ", "varia", "examp", "fred"],
            }
        ]
        self.sql = """CREATE MATERIALIZED VIEW varcharn_array_agg AS SELECT
                      ARRAY_AGG(f_c1) AS c1, ARRAY_AGG(f_c2) AS c2
                      FROM atbl_varcharn"""


class aggtst_varcharn_array_agg_gby(TstView):
    def __init__(self):
        # Validated on Postgres
        self.data = [
            {"id": 0, "c1": [None, "hello"], "c2": ["abc  ", "fred"]},
            {"id": 1, "c1": ["@abc", "hello"], "c2": ["varia", "examp"]},
        ]
        self.sql = """CREATE MATERIALIZED VIEW varchan_array_agg_gby AS SELECT
                      id, ARRAY_AGG(f_c1) AS c1, ARRAY_AGG(f_c2) AS c2
                      FROM atbl_varcharn
                      GROUP BY id"""


class aggtst_varcharn_array_agg_distinct(TstView):
    def __init__(self):
        # Validated on Postgres
        self.data = [
            {"c1": [None, "@abc", "hello"], "c2": ["abc  ", "examp", "fred", "varia"]}
        ]
        self.sql = """CREATE MATERIALIZED VIEW varcharn_array_agg_distinct AS SELECT
                      ARRAY_AGG(DISTINCT f_c1) AS c1, ARRAY_AGG(DISTINCT f_c2) AS c2
                      FROM atbl_varcharn"""


class aggtst_varcharn_array_agg_distinct_gby(TstView):
    def __init__(self):
        # Validated on Postgres
        self.data = [
            {"id": 0, "c1": [None, "hello"], "c2": ["abc  ", "fred"]},
            {"id": 1, "c1": ["@abc", "hello"], "c2": ["examp", "varia"]},
        ]
        self.sql = """CREATE MATERIALIZED VIEW varcharn_array_agg_distinct_gby AS SELECT
                      id, ARRAY_AGG(DISTINCT f_c1) AS c1, ARRAY_AGG(DISTINCT f_c2) AS c2
                      FROM atbl_varcharn
                      GROUP BY id"""


class aggtst_varcharn_array_agg_where(TstView):
    def __init__(self):
        # Validated on Postgres
        self.data = [{"c1": [None, "@abc", "hello"], "c2": ["abc  ", "varia", "examp"]}]
        self.sql = """CREATE MATERIALIZED VIEW varcharn_array_where AS SELECT
                      ARRAY_AGG(f_c1) FILTER(WHERE len(f_c2)>4) AS c1, ARRAY_AGG(f_c2) FILTER(WHERE len(f_c2)>4) AS c2
                      FROM atbl_varcharn"""


class aggtst_varcharn_array_agg_where_gby(TstView):
    def __init__(self):
        # Validated on Postgres
        self.data = [
            {"id": 0, "c1": [None], "c2": ["abc  "]},
            {"id": 1, "c1": ["@abc", "hello"], "c2": ["varia", "examp"]},
        ]
        self.sql = """CREATE MATERIALIZED VIEW varcharn_array_where_gby AS SELECT
                      id, ARRAY_AGG(f_c1) FILTER(WHERE len(f_c2)>4) AS c1, ARRAY_AGG(f_c2) FILTER(WHERE len(f_c2)>4) AS c2
                      FROM atbl_varcharn
                      GROUP BY id"""
