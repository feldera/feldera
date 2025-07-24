from tests.aggregate_tests.aggtst_base import TstView


class aggtst_atbl_charn(TstView):
    def __init__(self):
        # Validated on Postgres
        self.data = [
            {"id": 0, "f_c1": None, "f_c2": "abc   d"},
            {"id": 0, "f_c1": "hello  ", "f_c2": "fred   "},
            {"id": 1, "f_c1": "@abc   ", "f_c2": "variabl"},
            {"id": 1, "f_c1": "hello  ", "f_c2": "exampl "},
        ]
        self.sql = """CREATE MATERIALIZED VIEW atbl_charn AS SELECT
                      id,
                      CAST(c1 AS CHAR(7)) AS f_c1,
                      CAST(c2 AS CHAR(7)) AS f_c2
                      FROM varchar_tbl"""
