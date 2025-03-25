from tests.aggregate_tests.aggtst_base import TstView, TstTable


# Depth 2 tests => User Defined Type of Array Type
class cmpxtst_udt_of_arr_tbl(TstTable):
    """Define the table used by the user defined type of array tests"""

    def __init__(self):
        self.sql = """CREATE TYPE arr_type1 AS(arr_int_type1 INT ARRAY NOT NULL, arr_int_type2 INT ARRAY NULL);
                      CREATE TYPE arr_type2 AS(arr_var_type1 VARCHAR ARRAY NOT NULL, arr_var_type2 VARCHAR ARRAY NULL);
                      CREATE TABLE udt_of_arr_tbl(
                      id INT,
                      c1 arr_type1,
                      c2 arr_type2 NOT NULL)"""
        self.data = [
            {
                "id": 0,
                "c1": {"arr_int_type1": [1, 2, None, 4], "arr_int_type2": None},
                "c2": {"arr_var_type1": ["hi", "hello", None], "arr_var_type2": None},
            },
            {
                "id": 1,
                "c1": None,
                "c2": {"arr_var_type1": ["bye", None], "arr_var_type2": ["ciao", None]},
            },
        ]


class cmpxtst_udt_of_arr_unnest(TstView):
    def __init__(self):
        # checked manually
        self.data = [
            {"id": 0, "arr11_val": 1, "idx": 1},
            {"id": 0, "arr11_val": 2, "idx": 2},
            {"id": 0, "arr11_val": 4, "idx": 4},
            {"id": 0, "arr11_val": None, "idx": 3},
        ]
        self.sql = """CREATE MATERIALIZED VIEW udt_of_arr_unnest AS SELECT
                      id, arr11_val, idx
                      FROM udt_of_arr_tbl,
                      UNNEST(udt_of_arr_tbl.c1.arr_int_type1) WITH ORDINALITY AS t (arr11_val, idx)"""


class cmpxtst_udt_of_arr_field_access(TstView):
    def __init__(self):
        # checked manually
        self.data = [
            {
                "id": 0,
                "arr_int_type1": [1, 2, None, 4],
                "arr_var_type1": ["hi", "hello", None],
            },
            {"id": 1, "arr_int_type1": None, "arr_var_type1": ["bye", None]},
        ]
        self.sql = """CREATE MATERIALIZED VIEW udt_of_arr_field_access AS SELECT
                      id,
                      udt_of_arr_tbl.c1.arr_int_type1,
                      udt_of_arr_tbl.c2.arr_var_type1
                      FROM udt_of_arr_tbl"""


class cmpxtst_udt_of_arr_element_access(TstView):
    def __init__(self):
        # checked manually
        self.data = [
            {"id": 0, "int12": 2, "var11": "hi"},
            {"id": 1, "int12": None, "var11": "bye"},
        ]
        self.sql = """CREATE MATERIALIZED VIEW udt_of_arr_element_access AS SELECT
                      id,
                      udt_of_arr_tbl.c1.arr_int_type1[2] int12,
                      udt_of_arr_tbl.c2.arr_var_type1[1] var11
                      FROM udt_of_arr_tbl"""


class cmpxtst_udt_of_arr_idx_outbound(TstView):
    def __init__(self):
        # checked manually
        self.data = [{"id": 0, "int15": None}, {"id": 1, "int15": None}]
        self.sql = """CREATE MATERIALIZED VIEW udt_of_arr_idx_outbound AS SELECT
                          id,
                          udt_of_arr_tbl.c1.arr_int_type1[5] AS int15
                          FROM udt_of_arr_tbl"""


class cmpxtst_udt_of_arr_elmnt_nexist(TstView):
    def __init__(self):
        # checked manually
        self.data = [{"id": 0, "int13": None}, {"id": 1, "int13": None}]
        self.sql = """CREATE MATERIALIZED VIEW udt_of_arr_elmnt_nexist AS SELECT
                          id,
                          udt_of_arr_tbl.c1.arr_int_type1[3] AS int13
                          FROM udt_of_arr_tbl"""
