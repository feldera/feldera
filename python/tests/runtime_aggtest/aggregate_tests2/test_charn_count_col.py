from tests.runtime_aggtest.aggtst_base import TstView


class aggtst_charn_count_col(TstView):
    def __init__(self):
        # Validated on Postgres
        self.data = [{"c1": 3, "c2": 4}]
        self.sql = """CREATE MATERIALIZED VIEW charn_count_col AS SELECT
                      COUNT(f_c1) AS c1, COUNT(f_c2) AS c2
                      FROM atbl_charn"""


class aggtst_charn_count_col_gby(TstView):
    def __init__(self):
        # Validated on Postgres
        self.data = [{"id": 0, "c1": 1, "c2": 2}, {"id": 1, "c1": 2, "c2": 2}]
        self.sql = """CREATE MATERIALIZED VIEW charn_count_col_gby AS SELECT
                      id, COUNT(f_c1) AS c1, COUNT(f_c2) AS c2
                      FROM atbl_charn
                      GROUP BY id"""


class aggtst_charn_count_col_distinct(TstView):
    def __init__(self):
        # Validated on Postgres
        self.data = [{"c1": 2, "c2": 4}]
        self.sql = """CREATE MATERIALIZED VIEW charn_count_col_distinct AS SELECT
                      COUNT(DISTINCT f_c1) AS c1, COUNT(DISTINCT f_c2) AS c2
                      FROM atbl_charn"""


class aggtst_charn_count_col_distinct_gby(TstView):
    def __init__(self):
        # Validated on Postgres
        self.data = [{"id": 0, "c1": 1, "c2": 2}, {"id": 1, "c1": 2, "c2": 2}]
        self.sql = """CREATE MATERIALIZED VIEW charn_count_col_distinct_gby AS SELECT
                      id, COUNT(DISTINCT f_c1) AS c1, COUNT(DISTINCT f_c2) AS c2
                      FROM atbl_charn
                      GROUP BY id"""


# Checked manually since Postgres disagrees with us here
# While the length for CHAR is set to 7, for CHAR(N), the length function in Postgres disregards trailing spaces at the end of table values
class aggtst_charn_count_col_where(TstView):
    def __init__(self):
        self.data = [{"c1": 3, "c2": 3}]
        self.sql = """CREATE MATERIALIZED VIEW charn_count_col_where AS SELECT
                      COUNT(f_c1) FILTER(WHERE length(f_c1)>4) AS c1, COUNT(f_c2) FILTER(WHERE length(f_c1)>4) AS c2
                      FROM atbl_charn"""


class aggtst_charn_count_where_groupby(TstView):
    def __init__(self):
        self.data = [{"id": 0, "c1": 1, "c2": 1}, {"id": 1, "c1": 2, "c2": 2}]
        self.sql = """CREATE MATERIALIZED VIEW charn_count_col_where_gby AS SELECT
                      id, COUNT(f_c1) FILTER(WHERE length(f_c1)>4) AS c1, COUNT(f_c2) FILTER(WHERE length(f_c1)>4) AS c2
                      FROM atbl_charn
                      GROUP BY id"""
