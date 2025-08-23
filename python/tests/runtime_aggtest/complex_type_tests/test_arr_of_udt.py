from tests.runtime_aggtest.aggtst_base import TstView, TstTable


# Depth 2 tests => Array of User Defined Type
class cmpxtst_array_of_udt_tbl(TstTable):
    """Define the table used by the array of user defined type tests"""

    def __init__(self):
        self.sql = """CREATE TABLE arr_of_udt_tbl(
                      id INT,
                      c1_arr int_type ARRAY,
                      c2_arr var_type ARRAY NOT NULL)"""
        self.data = [
            {
                "id": 0,
                "c1_arr": [{"i1": 1, "i2": None}, {"i1": 5, "i2": 6}, None],
                "c2_arr": [{"v1": "hi", "v2": None}, {"v1": "hello", "v2": None}],
            },
            {"id": 1, "c1_arr": [None], "c2_arr": [{"v1": "ciao", "v2": "adios"}]},
        ]


class cmpxtst_arr_of_udt_unnest(TstView):
    def __init__(self):
        # checked manually
        self.data = [
            {"id": 0, "i1_val": 1, "i2_val": None, "idx": 1},
            {"id": 0, "i1_val": 5, "i2_val": 6, "idx": 2},
            {"id": 0, "i1_val": None, "i2_val": None, "idx": 3},
            {"id": 1, "i1_val": None, "i2_val": None, "idx": 1},
        ]
        self.sql = """CREATE MATERIALIZED VIEW arr_of_udt_unnest AS SELECT
                      id,  i1_val, i2_val,  idx
                      FROM arr_of_udt_tbl,
                      UNNEST(c1_arr) WITH ORDINALITY AS t (i1_val, i2_val, idx)"""


class cmpxtst_arr_of_udt_field_access(TstView):
    def __init__(self):
        # checked manually
        self.data = [
            {
                "id": 0,
                "c1_val1": {"i1": 1, "i2": None},
                "c1_val2": {"i1": 5, "i2": 6},
                "c2_val1": {"v1": "hi", "v2": None},
                "c2_val2": {"v1": "hello", "v2": None},
            },
            {
                "id": 1,
                "c1_val1": None,
                "c1_val2": None,
                "c2_val1": {"v1": "ciao", "v2": "adios"},
                "c2_val2": None,
            },
        ]
        self.sql = """CREATE MATERIALIZED VIEW arr_of_udt_field_access AS SELECT
                      id,
                      c1_arr[1] AS c1_val1,
                      c1_arr[2] AS c1_val2,
                      c2_arr[1] AS c2_val1,
                      c2_arr[2] AS c2_val2
                      FROM arr_of_udt_tbl"""


class cmpxtst_arr_of_udt_elmnt_access(TstView):
    def __init__(self):
        # checked manually
        self.data = [
            {"id": 0, "c12_elmnt": None, "c21_elmnt": "hello"},
            {"id": 1, "c12_elmnt": None, "c21_elmnt": None},
        ]
        self.sql = """CREATE MATERIALIZED VIEW arr_of_udt_elmnt_access AS SELECT
                      id,
                      c1_arr[1][2] AS c12_elmnt,
                      c2_arr[2][1] AS c21_elmnt
                      FROM arr_of_udt_tbl"""


class cmpxtst_arr_of_udt_idx_outbound(TstView):
    def __init__(self):
        # checked manually
        self.data = [
            {"id": 0, "c15_val": None, "c151_elmnt": None},
            {"id": 1, "c15_val": None, "c151_elmnt": None},
        ]
        self.sql = """CREATE MATERIALIZED VIEW arr_of_udt_idx_outbound AS SELECT
                      id,
                      c1_arr[5] AS c15_val,
                      c1_arr[5].i1 AS c151_elmnt
                      FROM arr_of_udt_tbl"""


class cmpxtst_arr_of_udt_elmnt_nexist(TstView):
    def __init__(self):
        # checked manually
        self.data = [{"id": 1, "c11_val": None}]
        self.sql = """CREATE MATERIALIZED VIEW arr_of_udt_elmnt_nexist AS SELECT
                      id,
                      c1_arr[1] AS c11_val
                      FROM arr_of_udt_tbl
                      WHERE id = 1"""
