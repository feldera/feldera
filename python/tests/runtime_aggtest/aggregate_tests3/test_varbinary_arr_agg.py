from tests.runtime_aggtest.aggtst_base import TstView


class aggtst_varbinary_array_agg_value(TstView):
    def __init__(self):
        # Validated on Postgres
        self.data = [
            {
                "c1": ["0c1620", "17382115", "17382115", "312b541d0b"],
                "c2": [None, "37424d58", "63141f4d", "2022"],
            }
        ]
        self.sql = """CREATE MATERIALIZED VIEW varbinary_array_agg AS SELECT
                      ARRAY_AGG(c1) AS c1, ARRAY_AGG(c2) AS c2
                      FROM varbinary_tbl"""


class aggtst_varbinary_array_agg_gby(TstView):
    def __init__(self):
        # Validated on Postgres
        self.data = [
            {"id": 0, "c1": ["0c1620", "17382115"], "c2": [None, "37424d58"]},
            {"id": 1, "c1": ["17382115", "312b541d0b"], "c2": ["63141f4d", "2022"]},
        ]
        self.sql = """CREATE MATERIALIZED VIEW varbinary_array_agg_gby AS SELECT
                      id, ARRAY_AGG(c1) AS c1, ARRAY_AGG(c2) AS c2
                      FROM varbinary_tbl
                      GROUP BY id"""


class aggtst_varbinary_array_agg_distinct(TstView):
    def __init__(self):
        # Validated on Postgres
        self.data = [
            {
                "c1": ["0c1620", "17382115", "312b541d0b"],
                "c2": [None, "2022", "37424d58", "63141f4d"],
            }
        ]
        self.sql = """CREATE MATERIALIZED VIEW varbinary_array_agg_distinct AS SELECT
                      ARRAY_AGG(DISTINCT c1) AS c1, ARRAY_AGG(DISTINCT c2) AS c2
                      FROM varbinary_tbl"""


class aggtst_varbinary_array_agg_distinct_gby(TstView):
    def __init__(self):
        # Validated on Postgres
        self.data = [
            {"id": 0, "c1": ["0c1620", "17382115"], "c2": [None, "37424d58"]},
            {"id": 1, "c1": ["17382115", "312b541d0b"], "c2": ["2022", "63141f4d"]},
        ]
        self.sql = """CREATE MATERIALIZED VIEW varbinary_array_agg_distinct_gby AS SELECT
                      id, ARRAY_AGG(DISTINCT c1) AS c1, ARRAY_AGG(DISTINCT c2) AS c2
                      FROM varbinary_tbl
                      GROUP BY id"""


class aggtst_varbinary_array_agg_where(TstView):
    def __init__(self):
        # Validated on Postgres
        self.data = [
            {"f_c1": ["17382115", "17382115"], "f_c2": ["37424d58", "63141f4d"]}
        ]
        self.sql = """CREATE MATERIALIZED VIEW varbinary_array_where AS SELECT
                      ARRAY_AGG(c1) FILTER(WHERE c1 < c2) AS f_c1, ARRAY_AGG(c2) FILTER(WHERE c1 < c2) AS f_c2
                      FROM varbinary_tbl"""


class aggtst_varbinary_array_agg_where_gby(TstView):
    def __init__(self):
        # Validated on Postgres
        self.data = [
            {"id": 0, "f_c1": ["17382115"], "f_c2": ["37424d58"]},
            {"id": 1, "f_c1": ["17382115"], "f_c2": ["63141f4d"]},
        ]
        self.sql = """CREATE MATERIALIZED VIEW varbinary_array_where_gby AS SELECT
                      id, ARRAY_AGG(c1) FILTER(WHERE c1 < c2) AS f_c1, ARRAY_AGG(c2) FILTER(WHERE c1 < c2) AS f_c2
                      FROM varbinary_tbl
                      GROUP BY id"""
