from .aggtst_base import TstView


# REAL tests (considering precision of 7 digits)
class aggtst_real_max_value(TstView):
    def __init__(self):
        # Validated on Postgres
        self.data = [{"c1": "57681.18", "c2": "873315.8"}]
        self.sql = """CREATE MATERIALIZED VIEW real_max AS SELECT
                      CAST(MAX(c1) AS VARCHAR) AS c1,
                      CAST(MAX(c2) AS VARCHAR) AS c2
                      FROM real_tbl"""


class aggtst_real_max_gby(TstView):
    def __init__(self):
        # Validated on Postgres
        self.data = [
            {"id": 0, "c1": "57681.18", "c2": "2231.791"},
            {"id": 1, "c1": "57681.18", "c2": "873315.8"},
        ]
        self.sql = """CREATE MATERIALIZED VIEW real_max_gby AS SELECT
                      id,
                      CAST(MAX(c1) AS VARCHAR) AS c1,
                      CAST(MAX(c2) AS VARCHAR) AS c2
                      FROM real_tbl
                      GROUP BY id"""


class aggtst_real_max_distinct(TstView):
    def __init__(self):
        # Validated on Postgres
        self.data = [{"c1": "57681.18", "c2": "873315.8"}]
        self.sql = """CREATE MATERIALIZED VIEW real_max_distinct AS SELECT
                      CAST(MAX(DISTINCT c1) AS VARCHAR) AS c1,
                      CAST(MAX(DISTINCT c2) AS VARCHAR) AS c2
                      FROM real_tbl"""


class aggtst_real_max_distinct_gby(TstView):
    def __init__(self):
        # Validated on Postgres
        self.data = [
            {"id": 0, "c1": "57681.18", "c2": "2231.791"},
            {"id": 1, "c1": "57681.18", "c2": "873315.8"},
        ]
        self.sql = """CREATE MATERIALIZED VIEW real_max_distinct_gby AS SELECT
                      id,
                      CAST(MAX(DISTINCT c1) AS VARCHAR) AS c1,
                      CAST(MAX(DISTINCT c2) AS VARCHAR) AS c2
                      FROM real_tbl
                      GROUP BY id"""


class aggtst_real_max_where(TstView):
    def __init__(self):
        # Validated on Postgres
        self.data = [{"f_c1": "57681.18", "f_c2": "-38.27112"}]
        self.sql = """CREATE MATERIALIZED VIEW real_max_where AS SELECT
                      CAST(MAX(c1) FILTER (WHERE c1 > c2) AS VARCHAR) AS f_c1,
                      CAST(MAX(c2) FILTER (WHERE c1 > c2) AS VARCHAR) AS f_c2
                      FROM real_tbl"""


class aggtst_real_max_where_gby(TstView):
    def __init__(self):
        # Validated on Postgres
        self.data = [
            {"id": 0, "f_c1": "57681.18", "f_c2": "-38.27112"},
            {"id": 1, "f_c1": None, "f_c2": None},
        ]
        self.sql = """CREATE MATERIALIZED VIEW real_max_where_gby AS SELECT
                      id,
                      CAST(MAX(c1) FILTER (WHERE c1 > c2) AS VARCHAR) AS f_c1,
                      CAST(MAX(c2) FILTER (WHERE c1 > c2) AS VARCHAR) AS f_c2
                      FROM real_tbl
                      GROUP BY id"""
