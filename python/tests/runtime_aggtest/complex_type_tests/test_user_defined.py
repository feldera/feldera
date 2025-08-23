from tests.runtime_aggtest.aggtst_base import TstView, TstTable


# Depth 1 tests
class cmpxtst_userdef_int_var_tbl(TstTable):
    """Define the table used by the user defined type tests"""

    def __init__(self):
        self.sql = """CREATE TYPE int_type AS(i1 INT NOT NULL, i2 INT);
                      CREATE TYPE var_type AS(v1 VARCHAR NOT NULL, v2 VARCHAR);
                      CREATE TABLE userdef_int_var_tbl(
                      id INT,
                      c1 int_type NOT NULL,
                      c2 int_type,
                      c3 var_type NOT NULL,
                      c4 var_type)"""
        self.data = [
            {
                "id": 0,
                "c1": {"i1": 1, "i2": None},
                "c2": {"i1": 5, "i2": 6},
                "c3": {"v1": "hi", "v2": None},
                "c4": {"v1": "hello", "v2": None},
            },
            {
                "id": 1,
                "c1": {"i1": 1, "i2": 2},
                "c2": {"i1": 5, "i2": None},
                "c3": {"v1": "bye", "v2": None},
                "c4": {"v1": "ciao", "v2": "adios"},
            },
        ]


class cmpxtst_userdef_field_access(TstView):
    def __init__(self):
        # checked manually
        self.data = [
            {
                "id": 0,
                "i1": 1,
                "i2": None,
                "i10": 5,
                "i20": 6,
                "v1": "hi",
                "v2": None,
                "v10": "hello",
                "v20": None,
            },
            {
                "id": 1,
                "i1": 1,
                "i2": 2,
                "i10": 5,
                "i20": None,
                "v1": "bye",
                "v2": None,
                "v10": "ciao",
                "v20": "adios",
            },
        ]
        self.sql = """CREATE MATERIALIZED VIEW userdef_field_access AS SELECT
                      id,
                      userdef_int_var_tbl.c1.i1,
                      userdef_int_var_tbl.c1.i2,
                      userdef_int_var_tbl.c2.i1,
                      userdef_int_var_tbl.c2.i2,
                      userdef_int_var_tbl.c3.v1,
                      userdef_int_var_tbl.c3.v2,
                      userdef_int_var_tbl.c4.v1,
                      userdef_int_var_tbl.c4.v2
                      FROM userdef_int_var_tbl"""
