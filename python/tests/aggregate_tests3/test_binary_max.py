from tests.aggregate_tests.aggtst_base import TstView


class aggtst_binary_max_value(TstView):
    def __init__(self):
        # checked manually
        self.data = [{"c1": "312b541d", "c2": "63141f4d"}]
        self.sql = """CREATE MATERIALIZED VIEW binary_max AS SELECT
                      MAX(c1) AS c1, MAX(c2) AS c2
                      FROM binary_tbl"""


class aggtst_binary_max_gby(TstView):
    def __init__(self):
        # checked manually
        self.data = [
            {"id": 0, "c1": "17382115", "c2": "37424d58"},
            {"id": 1, "c1": "312b541d", "c2": "63141f4d"},
        ]
        self.sql = """CREATE MATERIALIZED VIEW binary_max_gby AS SELECT
                      id, MAX(c1) AS c1, MAX(c2) AS c2
                      FROM binary_tbl
                      GROUP BY id"""


class aggtst_binary_max_distinct(TstView):
    def __init__(self):
        # checked manually
        self.data = [{"c1": "312b541d", "c2": "63141f4d"}]
        self.sql = """CREATE MATERIALIZED VIEW binary_max_distinct AS SELECT
                      MAX(DISTINCT c1) AS c1, MAX(DISTINCT c2) AS c2
                      FROM binary_tbl"""


class aggtst_binary_max_distinct_gby(TstView):
    def __init__(self):
        # checked manually
        self.data = [
            {"id": 0, "c1": "17382115", "c2": "37424d58"},
            {"id": 1, "c1": "312b541d", "c2": "63141f4d"},
        ]
        self.sql = """CREATE MATERIALIZED VIEW binary_max_distinct_gby AS SELECT
                      id, MAX(DISTINCT c1) AS c1, MAX(DISTINCT c2) AS c2
                      FROM binary_tbl
                      GROUP BY id"""


class aggtst_binary_max_where(TstView):
    def __init__(self):
        # checked manually
        self.data = [{"c1": "17382115", "c2": "63141f4d"}]
        self.sql = """CREATE MATERIALIZED VIEW binary_max_where AS SELECT
                      MAX(c1) FILTER(WHERE c1 < c2) AS c1, MAX(c2) FILTER(WHERE c1 < c2) AS c2
                      FROM binary_tbl"""


class aggtst_binary_max_where_gby(TstView):
    def __init__(self):
        # checked manually
        self.data = [
            {"id": 0, "c1": "17382115", "c2": "37424d58"},
            {"id": 1, "c1": "17382115", "c2": "63141f4d"},
        ]
        self.sql = """CREATE MATERIALIZED VIEW binary_max_where_gby AS SELECT
                      id, MAX(c1) FILTER(WHERE c1 < c2) AS c1, MAX(c2) FILTER(WHERE c1 < c2) AS c2
                      FROM binary_tbl
                      GROUP BY id"""
