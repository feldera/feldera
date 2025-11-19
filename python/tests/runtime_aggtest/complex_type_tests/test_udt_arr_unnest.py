from tests.runtime_aggtest.aggtst_base import TstView, TstTable


class cmpxtst_udt_of_arr_unnest_tbl(TstTable):
    """Define the table used by the user defined type of array UNNEST tests"""

    def __init__(self):
        self.sql = """CREATE TYPE arr_type_unnest AS(arr_int_type1 INT ARRAY NULL, arr_int_type2 INT ARRAY NOT NULL);
                      CREATE TABLE udt_of_arr_unnest_tbl(
                      id INT,
                      c1 arr_type_unnest,
                      c2 arr_type_unnest NOT NULL)"""
        self.data = [
            {
                "id": 0,
                "c1": {"arr_int_type1": [1, None], "arr_int_type2": [1, 2, None, 4]},
                "c2": {"arr_int_type1": None, "arr_int_type2": [2, 3, None]},
            },
            {
                "id": 1,
                "c1": None,
                "c2": {"arr_int_type1": [5, None], "arr_int_type2": [6, None]},
            },
        ]


class cmpxtst_udt_of_arr_unnest11(TstView):
    def __init__(self):
        # checked manually
        self.data = [
            {"id": 0, "arr11_val": 2, "idx": 1},
            {"id": 0, "arr11_val": None, "idx": 2},
        ]
        self.sql = """CREATE MATERIALIZED VIEW udt_of_arr_unnest11 AS SELECT
                      id, arr11_val + 1  AS arr11_val, idx
                      FROM udt_of_arr_unnest_tbl,
                      UNNEST(udt_of_arr_unnest_tbl.c1.arr_int_type1) WITH ORDINALITY AS t (arr11_val, idx)"""


class cmpxtst_udt_of_arr_unnest12(TstView):
    def __init__(self):
        # checked manually
        self.data = [
            {"id": 0, "arr12_val": 2, "idx": 1},
            {"id": 0, "arr12_val": 3, "idx": 2},
            {"id": 0, "arr12_val": 5, "idx": 4},
            {"id": 0, "arr12_val": None, "idx": 3},
        ]
        self.sql = """CREATE MATERIALIZED VIEW udt_of_arr_unnest12 AS SELECT
                      id, arr12_val + 1 AS arr12_val, idx
                      FROM udt_of_arr_unnest_tbl,
                      UNNEST(udt_of_arr_unnest_tbl.c1.arr_int_type2) WITH ORDINALITY AS t (arr12_val, idx)"""


class cmpxtst_udt_of_arr_unnest21(TstView):
    def __init__(self):
        # checked manually
        self.data = [
            {"id": 1, "arr21_val": 6, "idx": 1},
            {"id": 1, "arr21_val": None, "idx": 2},
        ]
        self.sql = """CREATE MATERIALIZED VIEW udt_of_arr_unnest21 AS SELECT
                      id, arr21_val + 1  AS arr21_val, idx
                      FROM udt_of_arr_unnest_tbl,
                      UNNEST(udt_of_arr_unnest_tbl.c2.arr_int_type1) WITH ORDINALITY AS t (arr21_val, idx)"""


class cmpxtst_udt_of_arr_unnest22(TstView):
    def __init__(self):
        # checked manually
        self.data = [
            {"id": 0, "arr22_val": 3, "idx": 1},
            {"id": 0, "arr22_val": 4, "idx": 2},
            {"id": 0, "arr22_val": None, "idx": 3},
            {"id": 1, "arr22_val": 7, "idx": 1},
            {"id": 1, "arr22_val": None, "idx": 2},
        ]
        self.sql = """CREATE MATERIALIZED VIEW udt_of_arr_unnest22 AS SELECT
                      id, arr22_val + 1 AS arr22_val, idx
                      FROM udt_of_arr_unnest_tbl,
                      UNNEST(udt_of_arr_unnest_tbl.c2.arr_int_type2) WITH ORDINALITY AS t (arr22_val, idx)"""
