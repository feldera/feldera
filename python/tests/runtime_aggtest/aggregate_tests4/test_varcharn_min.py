from tests.runtime_aggtest.aggtst_base import TstView


class aggtst_varcharn_min_value(TstView):
    def __init__(self):
        # Validated on Postgres
        self.data = [{"c1": "@abc", "c2": "abc  "}]
        self.sql = """CREATE MATERIALIZED VIEW varcharn_min AS SELECT
                      MIN(f_c1) AS c1, MIN(f_c2) AS c2
                      FROM atbl_varcharn"""


class aggtst_varcharn_min_gby(TstView):
    def __init__(self):
        # Validated on Postgres
        self.data = [
            {"id": 0, "c1": "hello", "c2": "abc  "},
            {"id": 1, "c1": "@abc", "c2": "examp"},
        ]
        self.sql = """CREATE MATERIALIZED VIEW varcharn_min_gby AS SELECT
                      id, MIN(f_c1) AS c1, MIN(f_c2) AS c2
                      FROM atbl_varcharn
                      GROUP BY id"""


class aggtst_varcharn_min_distinct(TstView):
    def __init__(self):
        # Validated on Postgres
        self.data = [{"c1": "@abc", "c2": "abc  "}]
        self.sql = """CREATE MATERIALIZED VIEW varcharn_min_distinct AS SELECT
                      MIN(DISTINCT f_c1) AS c1, MIN(DISTINCT f_c2) AS c2
                      FROM atbl_varcharn"""


class aggtst_varcharn_min_distinct_gby(TstView):
    def __init__(self):
        # Validated on Postgres
        self.data = [
            {"id": 0, "c1": "hello", "c2": "abc  "},
            {"id": 1, "c1": "@abc", "c2": "examp"},
        ]
        self.sql = """CREATE MATERIALIZED VIEW varcharn_min_distinct_gby AS SELECT
                      id, MIN(DISTINCT f_c1) AS c1, MIN(DISTINCT f_c2) AS c2
                      FROM atbl_varcharn
                      GROUP BY id"""


class aggtst_varcharn_min_where(TstView):
    def __init__(self):
        # Validated on Postgres
        self.data = [{"c1": "hello", "c2": "examp"}]
        self.sql = """CREATE MATERIALIZED VIEW varcharn_min_where AS SELECT
                      MIN(f_c1) FILTER(WHERE len(f_c1)>4) AS c1, MIN(f_c2) FILTER(WHERE len(f_c1)>4) AS c2
                      FROM atbl_varcharn"""


class aggtst_varcharn_min_where_gby(TstView):
    def __init__(self):
        # Validated on Postgres
        self.data = [
            {"id": 0, "c1": "hello", "c2": "fred"},
            {"id": 1, "c1": "hello", "c2": "examp"},
        ]
        self.sql = """CREATE MATERIALIZED VIEW varcharn_min_where_gby AS SELECT
                      id, MIN(f_c1) FILTER(WHERE len(f_c1)>4) AS c1, MIN(f_c2) FILTER(WHERE len(f_c1)>4) AS c2
                      FROM atbl_varcharn
                      GROUP BY id"""
