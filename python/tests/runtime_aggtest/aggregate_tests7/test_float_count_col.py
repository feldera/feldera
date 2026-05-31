from tests.runtime_aggtest.aggtst_base import TstView


class aggtst_float_count_col(TstView):
    def __init__(self):
        # Validated on Postgres
        self.data = [{"c1": 3, "c2": 4, "c3": 3, "c4": 4}]
        self.sql = """CREATE MATERIALIZED VIEW float_count_col AS SELECT
                      COUNT(c1) AS c1, COUNT(c2) AS c2, COUNT(c3) AS c3, COUNT(c4) AS c4
                      FROM float_tbl"""


class aggtst_float_count_col_groupby(TstView):
    def __init__(self):
        # Validated on Postgres
        self.data = [
            {
                "id": 0,
                "c1": 1,
                "c2": 2,
                "c3": 2,
                "c4": 2,
            },
            {"id": 1, "c1": 2, "c2": 2, "c3": 1, "c4": 2},
        ]
        self.sql = """CREATE MATERIALIZED VIEW float_count_col_gby AS SELECT
                      id, COUNT(c1) AS c1, COUNT(c2) AS c2, COUNT(c3) AS c3, COUNT(c4) AS c4
                      FROM float_tbl
                      GROUP BY id"""


class aggtst_float_count_col_distinct(TstView):
    def __init__(self):
        # Validated on Postgres
        self.data = [{"c1": 3, "c2": 4, "c3": 3, "c4": 4}]
        self.sql = """CREATE MATERIALIZED VIEW float_count_col_distinct AS SELECT
                      COUNT(DISTINCT c1) AS c1, COUNT(DISTINCT c2) AS c2, COUNT(DISTINCT c3) AS c3, COUNT(DISTINCT c4) AS c4
                      FROM float_tbl"""


class aggtst_float_count_col_distinct_groupby(TstView):
    def __init__(self):
        # Validated on Postgres
        self.data = [
            {
                "id": 0,
                "c1": 1,
                "c2": 2,
                "c3": 2,
                "c4": 2,
            },
            {"id": 1, "c1": 2, "c2": 2, "c3": 1, "c4": 2},
        ]
        self.sql = """CREATE MATERIALIZED VIEW float_count_col_distinct_gby AS SELECT
                      id, COUNT(DISTINCT c1) AS c1, COUNT(DISTINCT c2) AS c2, COUNT(DISTINCT c3) AS c3, COUNT(DISTINCT c4) AS c4
                      FROM float_tbl
                      GROUP BY id"""


class aggtst_float_count_col_where(TstView):
    def __init__(self):
        # Validated on Postgres
        self.data = [{"f_c1": 0, "f_c2": 1, "f_c3": 1, "f_c4": 1}]

        self.sql = """CREATE MATERIALIZED VIEW float_count_col_where AS SELECT
                      COUNT(c1) FILTER(WHERE c1 IS NULL) AS f_c1,
                      COUNT(c2) FILTER(WHERE c1 IS NULL) AS f_c2,
                      COUNT(c3) FILTER(WHERE c1 IS NULL) AS f_c3,
                      COUNT(c4) FILTER(WHERE c1 IS NULL) AS f_c4
                      FROM float_tbl"""


class aggtst_float_count_col_where_groupby(TstView):
    def __init__(self):
        # Validated on Postgres
        self.data = [
            {"id": 0, "f_c1": 0, "f_c2": 1, "f_c3": 1, "f_c4": 1},
            {"id": 1, "f_c1": 0, "f_c2": 0, "f_c3": 0, "f_c4": 0},
        ]
        self.sql = """CREATE MATERIALIZED VIEW float_count_col_where_gby AS SELECT
                      id,
                      COUNT(c1) FILTER(WHERE c1 IS NULL) AS f_c1,
                      COUNT(c2) FILTER(WHERE c1 IS NULL) AS f_c2,
                      COUNT(c3) FILTER(WHERE c1 IS NULL) AS f_c3,
                      COUNT(c4) FILTER(WHERE c1 IS NULL) AS f_c4
                      FROM float_tbl
                      GROUP BY id"""
