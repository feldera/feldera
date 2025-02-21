from tests.aggregate_tests.aggtst_base import TstView, TstTable


# Depth 1 tests
class cmpxtst_userdef_int_var_tbl(TstTable):
    """Define the table used by the user defined type tests"""

    def __init__(self):
        self.sql = """CREATE TYPE int_type AS(i1 INT NOT NULL, i2 INT);
                      CREATE TYPE var_type AS(v1 VARCHAR NOT NULL, v2 VARCHAR);
                      \nCREATE TABLE userdef_int_var_tbl(
                      id INT,
                      c1 int_type,
                      c2 var_type)"""
        self.data = [
            {"id": 0, "c1": {"i1": 1, "i2": None}, "c2": {"v1": "hi", "v2": None}},
            {"id": 1, "c1": {"i1": 1, "i2": 2}, "c2": {"v1": "hello", "v2": "bye"}},
        ]


class cmpxtst_userdef_field_access(TstView):
    def __init__(self):
        # checked manually
        self.data = [
            {"id": 0, "i1": 1, "i2": None, "v1": "hi", "v2": None},
            {"id": 1, "i1": 1, "i2": 2, "v1": "hello", "v2": "bye"},
        ]
        self.sql = """CREATE MATERIALIZED VIEW userdef_field_access AS SELECT
                      id,
                      userdef_int_var_tbl.c1.i1,
                      userdef_int_var_tbl.c1.i2,
                      userdef_int_var_tbl.c2.v1,
                      userdef_int_var_tbl.c2.v2
                      FROM userdef_int_var_tbl"""
