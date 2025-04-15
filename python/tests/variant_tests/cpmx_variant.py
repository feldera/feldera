from tests.aggregate_tests.aggtst_base import TstView, TstTable


# Complex Types(Array, Map, Row, UDT)
class varnttst_cmpx_var_tbl(TstTable):
    """Define the table used by the complex type tests"""

    def __init__(self):
        self.sql = """CREATE TYPE varnt_type AS(v1 VARIANT, v2 VARIANT);
                      CREATE TABLE varnt_cmpx_var_tbl(
                      id INT,
                      arr VARIANT ARRAY,
                      mapp MAP<VARCHAR, VARIANT>,
                      mapp1 MAP<VARIANT, VARIANT>,
                      roww ROW(v1 VARIANT, v2 VARIANT),
                      udt varnt_type
                      )"""
        self.data = [
            {
                "id": 0,
                "arr": ["12, 22", "hello, hi, bye"],
                "mapp": {"a": "ciao!", "b": "hello 45 234"},
                "mapp1": {"c": "olaa,", "d": "friends!!"},
                "roww": {"v1": "20", "v2": "bye bye!"},
                "udt": {"v1": "ferris", "v2": "flies away?"},
            },
            {
                "id": 1,
                "arr": None,
                "mapp": None,
                "mapp1": None,
                "roww": None,
                "udt": None,
            },
            {
                "id": 2,
                "arr": [None, None],
                "mapp": {"a": None, "b": None},
                "mapp1": {"c": None, "d": None},
                "roww": {"v1": None, "v2": None},
                "udt": {"v1": None, "v2": None},
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
                "mapp1": {'"c"': '"olaa,"', '"d"': '"friends!!"'},
                "roww": {"v1": '"20"', "v2": '"bye bye!"'},
                "udt": {"v1": '"ferris"', "v2": '"flies away?"'},
            },
            {
                "id": 1,
                "arr": None,
                "mapp": None,
                "mapp1": None,
                "roww": None,
                "udt": None,
            },
            {
                "id": 2,
                "arr": [None, None],
                "mapp": {"a": None, "b": None},
                "mapp1": {'"c"': None, '"d"': None},
                "roww": {"v1": "null", "v2": "null"},
                "udt": {"v1": None, "v2": None},
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


class varnttst_arr_field_nexist_varnt(TstView):
    def __init__(self):
        # checked manually
        self.data = [
            {"id": 0, "arr_val3": None},
            {"id": 1, "arr_val3": None},
            {"id": 2, "arr_val3": None},
        ]
        self.sql = """CREATE MATERIALIZED VIEW arr_field_nexist_varnt AS SELECT
                      id,
                      arr[3] AS arr_val3
                      FROM varnt_cmpx_var_tbl"""


# Map
class varnttst_map_field_access_varnt_var_key(TstView):
    def __init__(self):
        # checked manually
        self.data = [
            {"id": 0, "a": '"ciao!"', "b": '"hello 45 234"'},
            {"id": 1, "a": None, "b": None},
            {"id": 2, "a": None, "b": None},
        ]
        self.sql = """CREATE MATERIALIZED VIEW map_field_access_varnt_var_key AS SELECT
                      id,
                      mapp['a'] AS a,
                      mapp['b'] AS b
                      FROM varnt_cmpx_var_tbl"""


class varnttst_map_field_access_varnt_varnt_key(TstView):
    def __init__(self):
        # checked manually
        self.data = [
            {"id": 0, "c": '"olaa,"', "d": '"friends!!"'},
            {"id": 1, "c": None, "d": None},
            {"id": 2, "c": None, "d": None},
        ]
        self.sql = """CREATE MATERIALIZED VIEW map_field_access_varnt_varnt_key AS SELECT
                      id,
                      mapp1[CAST('c' AS VARIANT)] AS c,
                      mapp1[CAST('d' AS VARIANT)] AS d
                      FROM varnt_cmpx_var_tbl"""


class varnttst_map_field_nexist_varnt(TstView):
    def __init__(self):
        # checked manually
        self.data = [
            {"id": 0, "q": None, "y": None},
            {"id": 1, "q": None, "y": None},
            {"id": 2, "q": None, "y": None},
        ]
        self.sql = """CREATE MATERIALIZED VIEW map_field_nexist_varnt AS SELECT
                      id,
                      mapp['q'] AS q,
                      mapp['y'] AS y
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


# UDT
class varnttst_udt_field_access_varnt(TstView):
    def __init__(self):
        # checked manually
        self.data = [
            {"id": 0, "v1": '"ferris"', "v2": '"flies away?"'},
            {"id": 1, "v1": None, "v2": None},
            {"id": 2, "v1": None, "v2": None},
        ]
        self.sql = """CREATE MATERIALIZED VIEW udt_field_access_varnt AS SELECT
                      id,
                      varnt_cmpx_var_tbl.udt.v1,
                      varnt_cmpx_var_tbl.udt.v2
                      FROM varnt_cmpx_var_tbl"""
