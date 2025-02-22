from tests.aggregate_tests.aggtst_base import TstView


class aggtst_charn_count(TstView):
    def __init__(self):
        # Validated on Postgres
        self.data = [{"count": 4}]
        self.sql = """CREATE MATERIALIZED VIEW charn_count AS SELECT
                      COUNT(*) AS count
                      FROM atbl_charn"""


class aggtst_charn_count_gby(TstView):
    def __init__(self):
        # Validated on Postgres
        self.data = [{"id": 0, "count": 2}, {"id": 1, "count": 2}]
        self.sql = """CREATE MATERIALIZED VIEW charn_count_gby AS SELECT
                      id, COUNT(*) AS count
                      FROM atbl_charn
                      GROUP BY id"""


# Checked manually since Postgres disagrees with us here
# While the length for CHAR is set to 7, for CHAR(N), the length function in Postgres disregards trailing spaces at the end of table values
class aggtst_charn_count_where(TstView):
    def __init__(self):
        # Validated on Postgres
        self.data = [{"count": 3}]
        self.sql = """CREATE MATERIALIZED VIEW charn_count_where AS SELECT
                      COUNT(*) FILTER(WHERE length(f_c1)>4) AS count
                      FROM atbl_charn"""


class aggtst_varcharn_count_where_groupby(TstView):
    def __init__(self):
        # Validated on Postgres
        self.data = [{"id": 0, "count": 1}, {"id": 1, "count": 2}]
        self.sql = """CREATE MATERIALIZED VIEW charn_count_where_gby AS SELECT
                      id, COUNT(*) FILTER(WHERE length(f_c1)>4) AS count
                      FROM atbl_charn
                      GROUP BY id"""
