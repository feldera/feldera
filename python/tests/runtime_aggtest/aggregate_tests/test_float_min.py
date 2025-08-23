from tests.runtime_aggtest.aggtst_base import TstView


# REAL tests (considering precision of 7 digits)
class aggtst_real_min_value(TstView):
    def __init__(self):
        # Validated on Postgres
        self.data = [{"c1": "-1111.567", "c2": "-38.27112"}]
        self.sql = """CREATE MATERIALIZED VIEW real_min AS SELECT
                      CAST(MIN(c1) AS VARCHAR) AS c1,
                      CAST(MIN(c2) AS VARCHAR) AS c2
                      FROM real_tbl"""


class aggtst_real_min_gby(TstView):
    def __init__(self):
        # Validated on Postgres
        self.data = [
            {"id": 0, "c1": "57681.18", "c2": "-38.27112"},
            {"id": 1, "c1": "-1111.567", "c2": "71689.88"},
        ]
        self.sql = """CREATE MATERIALIZED VIEW real_min_gby AS SELECT
                      id,
                      CAST(MIN(c1) AS VARCHAR) AS c1,
                      CAST(MIN(c2) AS VARCHAR) AS c2
                      FROM real_tbl
                      GROUP BY id"""


class aggtst_real_min_distinct(TstView):
    def __init__(self):
        # Validated on Postgres
        self.data = [{"c1": "-1111.567", "c2": "-38.27112"}]
        self.sql = """CREATE MATERIALIZED VIEW real_min_distinct AS SELECT
                      CAST(MIN(DISTINCT c1) AS VARCHAR) AS c1,
                      CAST(MIN(DISTINCT c2) AS VARCHAR) AS c2
                      FROM real_tbl"""


class aggtst_real_min_distinct_gby(TstView):
    def __init__(self):
        # Validated on Postgres
        self.data = [
            {"id": 0, "c1": "57681.18", "c2": "-38.27112"},
            {"id": 1, "c1": "-1111.567", "c2": "71689.88"},
        ]
        self.sql = """CREATE MATERIALIZED VIEW real_min_distinct_gby AS SELECT
                      id,
                      CAST(MIN(DISTINCT c1) AS VARCHAR) AS c1,
                      CAST(MIN(DISTINCT c2) AS VARCHAR) AS c2
                      FROM real_tbl
                      GROUP BY id"""


class aggtst_real_min_where(TstView):
    def __init__(self):
        # Validated on Postgres
        self.data = [{"f_c1": "57681.18", "f_c2": "-38.27112"}]
        self.sql = """CREATE MATERIALIZED VIEW real_min_where AS SELECT
                      CAST(MIN(c1) FILTER (WHERE c1 > c2) AS VARCHAR) AS f_c1,
                      CAST(MIN(c2) FILTER (WHERE c1 > c2) AS VARCHAR) AS f_c2
                      FROM real_tbl"""


class aggtst_time_min_where_gby(TstView):
    def __init__(self):
        # Validated on Postgres
        self.data = [
            {"id": 0, "f_c1": "57681.18", "f_c2": "-38.27112"},
            {"id": 1, "f_c1": None, "f_c2": None},
        ]
        self.sql = """CREATE MATERIALIZED VIEW real_min_where_gby AS SELECT
                      id,
                      CAST(MIN(c1) FILTER (WHERE c1 > c2) AS VARCHAR) AS f_c1,
                      CAST(MIN(c2) FILTER (WHERE c1 > c2) AS VARCHAR) AS f_c2
                      FROM real_tbl
                      GROUP BY id"""
