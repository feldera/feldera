from tests.runtime_aggtest.aggtst_base import TstView


class aggtst_atbl_varcharn(TstView):
    def __init__(self):
        # Validated on Postgres
        self.data = [
            {"id": 0, "f_c1": None, "f_c2": "abc  "},
            {"id": 0, "f_c1": "hello", "f_c2": "fred"},
            {"id": 1, "f_c1": "@abc", "f_c2": "varia"},
            {"id": 1, "f_c1": "hello", "f_c2": "examp"},
        ]
        self.sql = """CREATE MATERIALIZED VIEW atbl_varcharn AS SELECT
                      id,
                      CAST(c1 AS VARCHAR(5)) AS f_c1,
                      CAST(c2 AS VARCHAR(5)) AS f_c2
                      FROM varchar_tbl"""
