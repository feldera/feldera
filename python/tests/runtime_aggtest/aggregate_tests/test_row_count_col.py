from tests.runtime_aggtest.aggtst_base import TstView


class aggtst_row_count_col(TstView):
    def __init__(self):
        # checked manually
        self.data = [{"c1": 5}]
        self.sql = """CREATE MATERIALIZED VIEW row_count_col AS SELECT
                      COUNT(ROW(c1, c3)) AS c1
                      FROM row_tbl"""


class aggtst_row_count_col_gby(TstView):
    def __init__(self):
        # checked manually
        self.data = [{"id": 0, "c1": 2}, {"id": 1, "c1": 3}]
        self.sql = """CREATE MATERIALIZED VIEW row_count_col_gby AS SELECT
                      id, COUNT(ROW(c1, c3)) AS c1
                      FROM row_tbl
                      GROUP BY id"""


class aggtst_row_count_col_distinct(TstView):
    def __init__(self):
        # checked manually
        self.data = [{"c1": 4}]
        self.sql = """CREATE MATERIALIZED VIEW row_count_col_distinct AS SELECT
                      COUNT(DISTINCT ROW(c1, c2, c3)) AS c1
                      FROM row_tbl"""


class aggtst_row_count_col_distinct_gby(TstView):
    def __init__(self):
        # checked manually
        self.data = [{"id": 0, "c1": 2}, {"id": 1, "c1": 2}]
        self.sql = """CREATE MATERIALIZED VIEW row_count_col_distinct_gby AS SELECT
                      id, COUNT(DISTINCT ROW(c1, c2, c3)) AS c1
                      FROM row_tbl
                      GROUP BY id"""


class aggtst_row_count_col_where(TstView):
    def __init__(self):
        # checked manually
        self.data = [{"c1": 1}]
        self.sql = """CREATE MATERIALIZED VIEW row_count_col_where AS SELECT
                      COUNT(ROW(c1, c2, c3)) FILTER(WHERE c2 < c3) AS c1
                      FROM row_tbl"""


class aggtst_row_count_where_groupby(TstView):
    def __init__(self):
        # checked manually
        self.data = [{"id": 0, "c1": 0}, {"id": 1, "c1": 1}]
        self.sql = """CREATE MATERIALIZED VIEW row_count_col_where_gby AS SELECT
                      id, COUNT(ROW(c1, c2, c3)) FILTER(WHERE c2 < c3) AS c1
                      FROM row_tbl
                      GROUP BY id"""
