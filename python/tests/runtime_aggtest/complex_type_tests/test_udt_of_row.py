from tests.runtime_aggtest.aggtst_base import TstView, TstTable


# Depth 2 tests => User Defined Type of Row Type
class cmpxtst_udt_of_row_tbl(TstTable):
    """Define the table used by the user defined type of Row tests"""

    def __init__(self):
        self.sql = """CREATE TYPE row_type1 AS(r1_int ROW(i11 INT NOT NULL, i12 INT NULL), r1_var ROW(v11 VARCHAR NOT NULL, v12 VARCHAR NULL));
                      CREATE TYPE row_type2 AS(r2_int ROW(i21 INT NULL, i22 INT NOT NULL), r2_var ROW(v21 VARCHAR NULL, v22 VARCHAR NOT NULL));
                      CREATE TABLE udt_of_row_tbl(
                      id INT,
                      c1 row_type1,
                      c2 row_type2 NOT NULL)"""
        self.data = [
            {
                "id": 0,
                "c1": None,
                "c2": {
                    "r2_int": {"i21": None, "i22": 21},
                    "r2_var": {"v21": None, "v22": "hi"},
                },
            },
            {
                "id": 1,
                "c1": {
                    "r1_int": {"i11": 20, "i12": None},
                    "r1_var": {"v11": "hello", "v12": None},
                },
                "c2": {"r2_int": None, "r2_var": None},
            },
            {
                "id": 2,
                "c1": {
                    "r1_int": {"i11": 11, "i12": 12},
                    "r1_var": {"v11": "bye", "v12": "see you!"},
                },
                "c2": {
                    "r2_int": {"i21": 14, "i22": 13},
                    "r2_var": {"v21": None, "v22": "hi"},
                },
            },
        ]


class cmpxtst_udt_of_row_field_access(TstView):
    def __init__(self):
        # checked manually
        self.data = [
            {
                "id": 0,
                "c1r1_int": None,
                "c1r1_var": None,
                "c2r2_int": {"i21": None, "i22": 21},
                "c2r2_var": {"v21": None, "v22": "hi"},
            },
            {
                "id": 1,
                "c1r1_int": {"i11": 20, "i12": None},
                "c1r1_var": {"v11": "hello", "v12": None},
                "c2r2_int": None,
                "c2r2_var": None,
            },
            {
                "id": 2,
                "c1r1_int": {"i11": 11, "i12": 12},
                "c1r1_var": {"v11": "bye", "v12": "see you!"},
                "c2r2_int": {"i21": 14, "i22": 13},
                "c2r2_var": {"v21": None, "v22": "hi"},
            },
        ]
        self.sql = """CREATE MATERIALIZED VIEW udt_of_row_field_access AS SELECT
                      id,
                      udt_of_row_tbl.c1.r1_int AS c1r1_int,
                      udt_of_row_tbl.c1.r1_var AS c1r1_var,
                      udt_of_row_tbl.c2.r2_int AS c2r2_int,
                      udt_of_row_tbl.c2.r2_var AS c2r2_var
                      FROM udt_of_row_tbl"""


class cmpxtst_udt_of_row_element_access(TstView):
    def __init__(self):
        # checked manually
        self.data = [
            {"id": 0, "c1r1_int_i11": None, "c1r1_var_v11": None},
            {"id": 1, "c1r1_int_i11": 20, "c1r1_var_v11": "hello"},
            {"id": 2, "c1r1_int_i11": 11, "c1r1_var_v11": "bye"},
        ]
        self.sql = """CREATE MATERIALIZED VIEW udt_of_row_element_access AS SELECT
                      id,
                      udt_of_row_tbl.c1.r1_int.i11 AS c1r1_int_i11,
                      udt_of_row_tbl.c1.r1_var.v11 AS c1r1_var_v11
                      FROM udt_of_row_tbl"""


class cmpxtst_udt_of_row_elmnt_nexist(TstView):
    def __init__(self):
        # checked manually
        self.data = [
            {"id": 0, "c2r2_var_v21": None},
            {"id": 1, "c2r2_var_v21": None},
            {"id": 2, "c2r2_var_v21": None},
        ]
        self.sql = """CREATE MATERIALIZED VIEW udt_of_row_elmnt_nexist AS SELECT
                      id,
                      udt_of_row_tbl.c2.r2_var.v21 AS c2r2_var_v21
                      FROM udt_of_row_tbl"""
