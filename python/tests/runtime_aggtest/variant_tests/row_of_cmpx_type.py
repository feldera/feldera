from tests.runtime_aggtest.aggtst_base import TstView, TstTable


# Row of Complex Type Tests
class varnttst_row_of_cmpx_tbl(TstTable):
    """Define the table used by the row of complex type tests"""

    def __init__(self):
        self.sql = """CREATE TABLE varnt_row_of_cmpx_tbl(
                      id INT,
                      row_arr ROW(v1 VARIANT ARRAY NULL, v2 VARIANT ARRAY) NULL,
                      row_map ROW(v1 MAP<VARIANT, VARIANT> NULL, v2 MAP<VARIANT, VARIANT>),
                      row_row ROW(v1 ROW(v11 VARIANT NULL, v12 VARIANT NULL) NULL, v2 ROW(v21 VARIANT NULL, v22 VARIANT NULL)),
                      row_udt ROW(var1 varnt_type NULL, var2 varnt_type)
                      )"""
        self.data = [
            {
                "id": 0,
                "row_arr": {
                    "v1": ["12, 13, 10", "17, 14"],
                    "v2": ["hi ,", "how are you?"],
                },
                "row_map": {
                    "v1": {"x": "bella", "y": "ciao"},
                    "v2": {"a": "good-bye", "b": "for now!"},
                },
                "row_row": {
                    "v1": {"v11": "the quick", "v12": "brown fox"},
                    "v2": {"v21": "jumps over", "v22": "the lazy dog."},
                },
                "row_udt": {"var1": ("ferris", "flies"), "var2": ("away", "????")},
            },
            {
                "id": 1,
                "row_arr": None,
                "row_map": None,
                "row_row": None,
                "row_udt": None,
            },
            {
                "id": 2,
                "row_arr": {"v1": None, "v2": ["check1"]},
                "row_map": {"v1": None, "v2": {"c": "check2"}},
                "row_row": {
                    "v1": {"v11": None, "v12": None},
                    "v2": {"v21": None, "v22": "check3"},
                },
                "row_udt": {"var1": (None, None), "var2": (None, "check4")},
            },
        ]


class varnttst_read_row_of_cmpx(TstView):
    def __init__(self):
        # checked manually
        self.data = [
            {
                "id": 0,
                "row_arr": {
                    "v1": ['"12, 13, 10"', '"17, 14"'],
                    "v2": ['"hi ,"', '"how are you?"'],
                },
                "row_map": {
                    "v1": {'"x"': '"bella"', '"y"': '"ciao"'},
                    "v2": {'"a"': '"good-bye"', '"b"': '"for now!"'},
                },
                "row_row": {
                    "v1": {"v11": '"the quick"', "v12": '"brown fox"'},
                    "v2": {"v21": '"jumps over"', "v22": '"the lazy dog."'},
                },
                "row_udt": {
                    "var1": {"v1": '"ferris"', "v2": '"flies"'},
                    "var2": {"v1": '"away"', "v2": '"????"'},
                },
            },
            {
                "id": 1,
                "row_arr": None,
                "row_map": None,
                "row_row": None,
                "row_udt": None,
            },
            {
                "id": 2,
                "row_arr": {"v1": None, "v2": ['"check1"']},
                "row_map": {"v1": None, "v2": {'"c"': '"check2"'}},
                "row_row": {
                    "v1": {"v11": None, "v12": None},
                    "v2": {"v21": None, "v22": '"check3"'},
                },
                "row_udt": {
                    "var1": {"v1": None, "v2": None},
                    "var2": {"v1": None, "v2": '"check4"'},
                },
            },
        ]
        self.sql = """CREATE MATERIALIZED VIEW read_row_of_cmpx AS SELECT
                      *
                      FROM varnt_row_of_cmpx_tbl"""


# Array
class varnttst_row_of_arr_unnest_varnt_elmnt(TstView):
    def __init__(self):
        # checked manually
        self.data = [
            {"id": 0, "row1_elmnt": '"12, 13, 10"', "idx": 1},
            {"id": 0, "row1_elmnt": '"17, 14"', "idx": 2},
        ]
        self.sql = """CREATE MATERIALIZED VIEW row_of_arr_unnest_varnt_elmnt AS SELECT
                      id,  row1_elmnt, idx
                      FROM varnt_row_of_cmpx_tbl,
                      UNNEST(varnt_row_of_cmpx_tbl.row_arr[1]) WITH ORDINALITY AS t (row1_elmnt, idx)"""


class varnttst_row_of_arr_field_access_varnt(TstView):
    def __init__(self):
        # checked manually
        self.data = [
            {
                "id": 0,
                "row_arr1": ['"12, 13, 10"', '"17, 14"'],
                "row_arr2": ['"hi ,"', '"how are you?"'],
            },
            {"id": 1, "row_arr1": None, "row_arr2": None},
            {"id": 2, "row_arr1": None, "row_arr2": ['"check1"']},
        ]
        self.sql = """CREATE MATERIALIZED VIEW row_of_arr_field_access_varnt AS SELECT
                      id,
                      varnt_row_of_cmpx_tbl.row_arr[1] AS row_arr1,
                      varnt_row_of_cmpx_tbl.row_arr.v2 AS row_arr2
                      FROM varnt_row_of_cmpx_tbl"""


class varnttst_row_of_arr_elmnt_access_varnt(TstView):
    def __init__(self):
        # checked manually
        self.data = [
            {"id": 0, "row_arr11": '"12, 13, 10"', "row_arr21": '"hi ,"'},
            {"id": 1, "row_arr11": None, "row_arr21": None},
            {"id": 2, "row_arr11": None, "row_arr21": '"check1"'},
        ]
        self.sql = """CREATE MATERIALIZED VIEW row_of_arr_elmnt_access_varnt AS SELECT
                      id,
                      varnt_row_of_cmpx_tbl.row_arr[1][1] AS row_arr11,
                      varnt_row_of_cmpx_tbl.row_arr.v2[1] AS row_arr21
                      FROM varnt_row_of_cmpx_tbl"""


class varnttst_row_of_arr_nexist_varnt(TstView):
    def __init__(self):
        # checked manually
        self.data = [
            {"id": 0, "row_arr25": None},
            {"id": 1, "row_arr25": None},
            {"id": 2, "row_arr25": None},
        ]
        self.sql = """CREATE MATERIALIZED VIEW row_of_arr_nexist_varnt AS SELECT
                      id,
                      varnt_row_of_cmpx_tbl.row_arr[2][5] AS row_arr25
                      FROM varnt_row_of_cmpx_tbl"""


# Map
class varnttst_row_of_map_field_access_varnt(TstView):
    def __init__(self):
        # checked manually
        self.data = [
            {
                "id": 0,
                "row_map1": {'"x"': '"bella"', '"y"': '"ciao"'},
                "row_map2": {'"a"': '"good-bye"', '"b"': '"for now!"'},
            },
            {"id": 1, "row_map1": None, "row_map2": None},
            {"id": 2, "row_map1": None, "row_map2": {'"c"': '"check2"'}},
        ]
        self.sql = """CREATE MATERIALIZED VIEW row_of_map_field_access_varnt AS SELECT
                      id,
                      varnt_row_of_cmpx_tbl.row_map.v1 AS row_map1,
                      row_map[2] AS row_map2
                      FROM varnt_row_of_cmpx_tbl"""


class varnttst_row_of_map_elmnt_access_varnt(TstView):
    def __init__(self):
        # checked manually
        self.data = [
            {"id": 0, "x": '"bella"', "a": '"good-bye"'},
            {"id": 1, "x": None, "a": None},
            {"id": 2, "x": None, "a": None},
        ]
        self.sql = """CREATE MATERIALIZED VIEW row_of_map_elmnt_access_varnt AS SELECT
                      id,
                      varnt_row_of_cmpx_tbl.row_map.v1[CAST('x' AS VARIANT)] AS x,
                      row_map[2][CAST('a' AS VARIANT)] AS a
                      FROM varnt_row_of_cmpx_tbl"""


class varnttst_row_of_map_nexist_varnt(TstView):
    def __init__(self):
        # checked manually
        self.data = [{"id": 0, "b": None}, {"id": 1, "b": None}, {"id": 2, "b": None}]
        self.sql = """CREATE MATERIALIZED VIEW row_of_map_nexist_varnt AS SELECT
                      id,
                      row_map[1][CAST('b' AS VARIANT)] AS b
                      FROM varnt_row_of_cmpx_tbl"""


# Row
class varnttst_row_of_row_field_access_varnt(TstView):
    def __init__(self):
        # checked manually
        self.data = [
            {
                "id": 0,
                "row_row1": {"v11": '"the quick"', "v12": '"brown fox"'},
                "row_row2": {"v21": '"jumps over"', "v22": '"the lazy dog."'},
            },
            {"id": 1, "row_row1": None, "row_row2": None},
            {
                "id": 2,
                "row_row1": {"v11": None, "v12": None},
                "row_row2": {"v21": None, "v22": '"check3"'},
            },
        ]
        self.sql = """CREATE MATERIALIZED VIEW row_of_row_field_access_varnt AS SELECT
                      id,
                      varnt_row_of_cmpx_tbl.row_row.v1 AS row_row1,
                      varnt_row_of_cmpx_tbl.row_row[2] AS row_row2
                      FROM varnt_row_of_cmpx_tbl"""


class varnttst_row_of_row_elmnt_access_varnt(TstView):
    def __init__(self):
        # checked manually
        self.data = [
            {"id": 0, "row_row12": '"brown fox"', "row_row21": '"jumps over"'},
            {"id": 1, "row_row12": None, "row_row21": None},
            {"id": 2, "row_row12": None, "row_row21": None},
        ]
        self.sql = """CREATE MATERIALIZED VIEW row_of_row_elmnt_access_varnt AS SELECT
                      id,
                      varnt_row_of_cmpx_tbl.row_row.v1.v12 AS row_row12,
                      varnt_row_of_cmpx_tbl.row_row[2][1] AS row_row21
                      FROM varnt_row_of_cmpx_tbl"""


# UDT
class varnttst_row_of_udt_field_access_varnt(TstView):
    def __init__(self):
        # checked manually
        self.data = [
            {
                "id": 0,
                "row_udt1": {"v1": '"ferris"', "v2": '"flies"'},
                "row_udt2": {"v1": '"away"', "v2": '"????"'},
            },
            {"id": 1, "row_udt1": None, "row_udt2": None},
            {
                "id": 2,
                "row_udt1": {"v1": None, "v2": None},
                "row_udt2": {"v1": None, "v2": '"check4"'},
            },
        ]
        self.sql = """CREATE MATERIALIZED VIEW row_of_udt_field_access_varnt AS SELECT
                      id,
                      varnt_row_of_cmpx_tbl.row_udt.var1 AS row_udt1,
                      varnt_row_of_cmpx_tbl.row_udt[2] AS row_udt2
                      FROM varnt_row_of_cmpx_tbl"""


class varnttst_row_of_udt_elmnt_access_varnt(TstView):
    def __init__(self):
        # checked manually
        self.data = [
            {"id": 0, "row_udt12": '"flies"', "row_udt21": '"away"'},
            {"id": 1, "row_udt12": None, "row_udt21": None},
            {"id": 2, "row_udt12": None, "row_udt21": None},
        ]
        self.sql = """CREATE MATERIALIZED VIEW row_of_udt_elmnt_access_varnt AS SELECT
                      id,
                      varnt_row_of_cmpx_tbl.row_udt.var1.v2 AS row_udt12,
                      varnt_row_of_cmpx_tbl.row_udt[2][1] AS row_udt21
                      FROM varnt_row_of_cmpx_tbl"""
