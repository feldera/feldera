from tests.aggregate_tests.aggtst_base import TstView


class aggtst_binary_min_value(TstView):
    def __init__(self):
        # checked manually
        self.data = [{"c1": "0c1620", "c2": "2022160c"}]
        self.sql = """CREATE MATERIALIZED VIEW binary_min AS SELECT
                      MIN(c1) AS c1, MIN(c2) AS c2
                      FROM binary_tbl"""


class aggtst_binary_min_gby(TstView):
    def __init__(self):
        # checked manually
        self.data = [
            {"id": 0, "c1": "0c1620", "c2": "37424d58"},
            {"id": 1, "c1": "17382115", "c2": "2022160c"},
        ]
        self.sql = """CREATE MATERIALIZED VIEW binary_min_gby AS SELECT
                      id, MIN(c1) AS c1, MIN(c2) AS c2
                      FROM binary_tbl
                      GROUP BY id"""


class aggtst_binary_min_distinct(TstView):
    def __init__(self):
        # checked manually
        self.data = [{"c1": "0c1620", "c2": "2022160c"}]
        self.sql = """CREATE MATERIALIZED VIEW binary_min_distinct AS SELECT
                      MIN(DISTINCT c1) AS c1, MIN(DISTINCT c2) AS c2
                      FROM binary_tbl"""


class aggtst_binary_min_distinct_gby(TstView):
    def __init__(self):
        # checked manually
        self.data = [
            {"id": 0, "c1": "0c1620", "c2": "37424d58"},
            {"id": 1, "c1": "17382115", "c2": "2022160c"},
        ]
        self.sql = """CREATE MATERIALIZED VIEW binary_min_distinct_gby AS SELECT
                      id, MIN(DISTINCT c1) AS c1, MIN(DISTINCT c2) AS c2
                      FROM binary_tbl
                      GROUP BY id"""


class aggtst_binary_min_where(TstView):
    def __init__(self):
        # checked manually
        self.data = [{"c1": "17382115", "c2": "37424d58"}]
        self.sql = """CREATE MATERIALIZED VIEW binary_min_where AS SELECT
                      MIN(c1) FILTER(WHERE c1 < c2) AS c1, MIN(c2) FILTER(WHERE c1 < c2) AS c2
                      FROM binary_tbl"""


class aggtst_binary_min_where_gby(TstView):
    def __init__(self):
        # checked manually
        self.data = [
            {"id": 0, "c1": "17382115", "c2": "37424d58"},
            {"id": 1, "c1": "17382115", "c2": "63141f4d"},
        ]
        self.sql = """CREATE MATERIALIZED VIEW binary_min_where_gby AS SELECT
                      id, MIN(c1) FILTER(WHERE c1 < c2) AS c1, MIN(c2) FILTER(WHERE c1 < c2) AS c2
                      FROM binary_tbl
                      GROUP BY id"""
