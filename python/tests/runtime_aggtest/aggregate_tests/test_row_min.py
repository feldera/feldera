from tests.runtime_aggtest.aggtst_base import TstView


class aggtst_row_min_value(TstView):
    def __init__(self):
        # checked manually
        self.data = [{"c1": {"EXPR$0": 2, "EXPR$1": "elo", "EXPR$2": "ciao"}}]
        self.sql = """CREATE MATERIALIZED VIEW row_min AS SELECT
                      MIN(ROW(c1, c2, c3)) AS c1
                      FROM row_tbl"""


class aggtst_row_min_gby(TstView):
    def __init__(self):
        # checked manually
        self.data = [
            {"id": 0, "c1": {"EXPR$0": 3, "EXPR$1": "ola", "EXPR$2": "ciao"}},
            {"id": 1, "c1": {"EXPR$0": 2, "EXPR$1": "elo", "EXPR$2": "ciao"}},
        ]
        self.sql = """CREATE MATERIALIZED VIEW row_min_gby AS SELECT
                      id,
                      MIN(ROW(c1, c2, c3)) AS c1
                      FROM row_tbl
                      GROUP BY id"""


class aggtst_row_min_distinct(TstView):
    def __init__(self):
        # checked manually
        self.data = [{"c1": {"EXPR$0": 2, "EXPR$1": "elo", "EXPR$2": "ciao"}}]
        self.sql = """CREATE MATERIALIZED VIEW row_min_distinct AS SELECT
                      MIN(DISTINCT ROW(c1, c2, c3)) AS c1
                      FROM row_tbl"""


class aggtst_row_min_distinct_gby(TstView):
    def __init__(self):
        # checked manually
        self.data = [
            {"id": 0, "c1": {"EXPR$0": 3, "EXPR$1": "ola", "EXPR$2": "ciao"}},
            {"id": 1, "c1": {"EXPR$0": 2, "EXPR$1": "elo", "EXPR$2": "ciao"}},
        ]
        self.sql = """CREATE MATERIALIZED VIEW row_min_distinct_gby AS SELECT
                      id,
                      MIN(DISTINCT ROW(c1, c2, c3)) AS c1
                      FROM row_tbl
                      GROUP BY id"""


class aggtst_row_min_where(TstView):
    def __init__(self):
        # checked manually
        self.data = [{"c1": {"EXPR$0": 7, "EXPR$1": "hi", "EXPR$2": "hiya"}}]
        self.sql = """CREATE MATERIALIZED VIEW row_min_where AS SELECT
                      MIN(ROW(c1, c2, c3)) FILTER(WHERE c2 < c3) AS c1
                      FROM row_tbl"""


class aggtst_row_min_where_gby(TstView):
    def __init__(self):
        # checked manually
        self.data = [
            {"id": 0, "c1": None},
            {"id": 1, "c1": {"EXPR$0": 7, "EXPR$1": "hi", "EXPR$2": "hiya"}},
        ]
        self.sql = """CREATE MATERIALIZED VIEW row_min_where_gby AS SELECT
                      id, MIN(ROW(c1, c2, c3)) FILTER(WHERE c2 < c3) AS c1
                      FROM row_tbl
                      GROUP BY id"""
