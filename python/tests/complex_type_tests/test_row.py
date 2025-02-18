from tests.aggregate_tests.aggtst_base import TstView, TstTable


# Depth 1 tests
class cmpxtst_row_int_var_tbl(TstTable):
    """Define the table used by the ROW of int and varchar tests"""

    def __init__(self):
        self.sql = """CREATE TABLE row_int_var_tbl(
                      id INT,
                      c1 ROW(i1 INT NOT NULL, i2 INT NULL),
                      c2 ROW(v1 VARCHAR NOT NULL, v2 VARCHAR NULL))"""
        self.data = [
            {"id": 0, "c1": {"i1": 20, "i2": None}, "c2": {"v1": "hi", "v2": "bye"}},
            {"id": 1, "c1": {"i1": 10, "i2": 12}, "c2": {"v1": "hello", "v2": None}},
        ]


class cmpxtst_row_field_access(TstView):
    def __init__(self):
        # checked manually
        self.data = [
            {"id": 0, "i1": 20, "i2": None, "v1": "hi", "v2": "bye"},
            {"id": 1, "i1": 10, "i2": 12, "v1": "hello", "v2": None},
        ]
        self.sql = """CREATE MATERIALIZED VIEW row_field_access AS SELECT
                      id,
                      row_int_var_tbl.c1.i1,
                      row_int_var_tbl.c1.i2,
                      row_int_var_tbl.c2.v1,
                      row_int_var_tbl.c2.v2
                      FROM row_int_var_tbl"""
