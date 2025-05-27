from tests.aggregate_tests.aggtst_base import TstView, TstTable


class orderby_tbl(TstTable):
    def __init__(self):
        self.sql = """CREATE TABLE tbl(
                      c3 INT)"""
        self.data = [
            {"c3": 3},
            {"c3": 2},
            {"c3": None},
            {"c3": 2},
        ]


class orderby_v(TstView):
    def __init__(self):
        self.data = []
        self.sql = """CREATE MATERIALIZED VIEW v AS
                       SELECT c3 FROM tbl
                       ORDER BY c3 ASC NULLS LAST
                       LIMIT 3"""
