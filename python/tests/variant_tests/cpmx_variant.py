from tests.aggregate_tests.aggtst_base import TstView, TstTable


# Complex Types(Array, Map, Row)
class varnttst_cmpx_var_tbl(TstTable):
    """Define the table used by the complex type tests"""

    def __init__(self):
        self.sql = """CREATE TABLE varnt_cmpx_var_tbl(
                      id INT,
                      arr VARIANT ARRAY,
                      mapp MAP<VARCHAR, VARIANT>,
                      roww ROW(v1 VARIANT, v2 VARIANT)
                      )"""
        self.data = [
            {
                "id": 0,
                "arr": ["12, 22", "hello, hi, bye"],
                "mapp": {"a": "ciao!", "b": "hello 45 234"},
                "roww": {"v1": "20", "v2": "bye bye!"},
            },
            {"id": 1, "arr": None, "map": None, "roww": None},
            {
                "id": 2,
                "arr": [None, None],
                "map": {"a": None, "b": None},
                "roww": {"v1": None, "v2": None},
            },
        ]


class varnttst_read_cmpx_var(TstView):
    def __init__(self):
        # checked manually
        self.data = [
            {
                "id": 0,
                "arr": ['"12, 22"', '"hello, hi, bye"'],
                "mapp": {"a": '"ciao!"', "b": '"hello 45 234"'},
                "roww": {"v1": '"20"', "v2": '"bye bye!"'},
            },
            {"id": 1, "arr": None, "mapp": None, "roww": None},
            {
                "id": 2,
                "arr": [None, None],
                "mapp": None,
                "roww": {"v1": "null", "v2": "null"},
            },
        ]
        self.sql = """CREATE MATERIALIZED VIEW read_cmpx_var AS SELECT
                      *
                      FROM varnt_cmpx_var_tbl"""


# Array
class varnttst_arr_unnest_varnt(TstView):
    def __init__(self):
        # checked manually
        self.data = [
            {"id": 0, "arr_val": '"12, 22"', "idx": 1},
            {"id": 0, "arr_val": '"hello, hi, bye"', "idx": 2},
            {"id": 2, "arr_val": None, "idx": 1},
            {"id": 2, "arr_val": None, "idx": 2},
        ]
        self.sql = """CREATE MATERIALIZED VIEW arr_unnest_varnt AS SELECT
                      id,  arr_val, idx
                      FROM varnt_cmpx_var_tbl,
                      UNNEST(arr) WITH ORDINALITY AS t (arr_val, idx)"""


class varnttst_arr_field_access_varnt(TstView):
    def __init__(self):
        # checked manually
        self.data = [
            {"id": 0, "arr_val1": '"12, 22"', "arr_val2": '"hello, hi, bye"'},
            {"id": 1, "arr_val1": None, "arr_val2": None},
            {"id": 2, "arr_val1": None, "arr_val2": None},
        ]
        self.sql = """CREATE MATERIALIZED VIEW arr_field_access_varnt AS SELECT
                      id,
                      arr[1] AS arr_val1,
                      arr[2] AS arr_val2
                      FROM varnt_cmpx_var_tbl"""


# Map
class varnttst_map_field_access_varnt(TstView):
    def __init__(self):
        # checked manually
        self.data = [
            {"id": 0, "a": '"ciao!"', "b": '"hello 45 234"'},
            {"id": 1, "a": None, "b": None},
            {"id": 2, "a": None, "b": None},
        ]
        self.sql = """CREATE MATERIALIZED VIEW map_field_access_varnt AS SELECT
                      id,
                      mapp['a'] AS a,
                      mapp['b'] AS b
                      FROM varnt_cmpx_var_tbl"""


# Row
class varnttst_roww_field_access_varnt(TstView):
    def __init__(self):
        # checked manually
        self.data = [
            {"id": 0, "v1": '"20"', "v2": '"bye bye!"'},
            {"id": 1, "v1": None, "v2": None},
            {"id": 2, "v1": "null", "v2": "null"},
        ]
        self.sql = """CREATE MATERIALIZED VIEW roww_field_access_varnt AS SELECT
                      id,
                      varnt_cmpx_var_tbl.roww.v1,
                      varnt_cmpx_var_tbl.roww.v2
                      FROM varnt_cmpx_var_tbl"""
