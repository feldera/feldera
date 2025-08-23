from tests.runtime_aggtest.aggtst_base import TstView, TstTable


# Array of Complex Type Tests
class varnttst_arr_of_cmpx_tbl(TstTable):
    """Define the table used by the array of complex type tests"""

    def __init__(self):
        self.sql = """CREATE TABLE varnt_arr_of_cmpx_tbl(
                      id INT,
                      arr_arr VARIANT ARRAY ARRAY,
                      arr_map MAP<VARIANT, VARIANT> ARRAY,
                      arr_row ROW(v1 VARIANT NULL, v2 VARIANT NULL) ARRAY,
                      arr_udt varnt_type ARRAY
                      )"""
        self.data = [
            {
                "id": 0,
                "arr_arr": [["12, 22", "hello, hi, bye"], ["are you okay,", "mate?"]],
                "arr_map": [
                    {"p": "12 friends", "q": "paint on"},
                    {"r": "48 canvases", "s": "each!?"},
                ],
                "arr_row": [
                    {"v1": "345", "v2": "bye"},
                    {"v1": "b4 breakfast", "v2": "456 ciao"},
                ],
                "arr_udt": [
                    {"v1": "ferris", "v2": "flies"},
                    {"v1": "away", "v2": "?????"},
                ],
            },
            {
                "id": 1,
                "arr_arr": None,
                "arr_map": None,
                "arr_row": None,
                "arr_udt": None,
            },
            {
                "id": 2,
                "arr_arr": [[None, None], [None]],
                "arr_map": [{"p": None, "q": None}, {"r": None, "s": None}],
                "arr_row": [{"v1": None, "v2": None}, {"v1": None, "v2": None}],
                "arr_udt": [{"v1": None, "v2": None}, {"v1": None, "v2": None}],
            },
        ]


class varnttst_read_arr_of_cmpx(TstView):
    def __init__(self):
        # checked manually
        self.data = [
            {
                "id": 0,
                "arr_arr": [
                    ['"12, 22"', '"hello, hi, bye"'],
                    ['"are you okay,"', '"mate?"'],
                ],
                "arr_map": [
                    {'"p"': '"12 friends"', '"q"': '"paint on"'},
                    {'"r"': '"48 canvases"', '"s"': '"each!?"'},
                ],
                "arr_row": [
                    {"v1": '"345"', "v2": '"bye"'},
                    {"v1": '"b4 breakfast"', "v2": '"456 ciao"'},
                ],
                "arr_udt": [
                    {"v1": '"ferris"', "v2": '"flies"'},
                    {"v1": '"away"', "v2": '"?????"'},
                ],
            },
            {
                "id": 1,
                "arr_arr": None,
                "arr_map": None,
                "arr_row": None,
                "arr_udt": None,
            },
            {
                "id": 2,
                "arr_arr": [[None, None], [None]],
                "arr_map": [{'"p"': None, '"q"': None}, {'"r"': None, '"s"': None}],
                "arr_row": [{"v1": None, "v2": None}, {"v1": None, "v2": None}],
                "arr_udt": [{"v1": None, "v2": None}, {"v1": None, "v2": None}],
            },
        ]
        self.sql = """CREATE MATERIALIZED VIEW read_arr_of_cmpx AS SELECT
                      *
                      FROM varnt_arr_of_cmpx_tbl"""


# Array
class varnttst_arr_of_arr_unnest_varnt(TstView):
    def __init__(self):
        # checked manually
        self.data = [
            {"id": 0, "arr_val": ['"12, 22"', '"hello, hi, bye"'], "idx": 1},
            {"id": 0, "arr_val": ['"are you okay,"', '"mate?"'], "idx": 2},
            {"id": 2, "arr_val": [None, None], "idx": 1},
            {"id": 2, "arr_val": [None], "idx": 2},
        ]
        self.sql = """CREATE MATERIALIZED VIEW arr_of_arr_unnest_varnt AS SELECT
                      id,  arr_val, idx
                      FROM varnt_arr_of_cmpx_tbl,
                      UNNEST(arr_arr) WITH ORDINALITY AS t (arr_val, idx)"""


class varnttst_arr_of_arr_unnest_varnt_elmnt(TstView):
    def __init__(self):
        # checked manually
        self.data = [
            {"id": 0, "arr1_elmnt": '"12, 22"', "idx": 1},
            {"id": 0, "arr1_elmnt": '"hello, hi, bye"', "idx": 2},
            {"id": 2, "arr1_elmnt": None, "idx": 1},
            {"id": 2, "arr1_elmnt": None, "idx": 2},
        ]
        self.sql = """CREATE MATERIALIZED VIEW arr_of_arr_unnest_varnt_elmnt AS SELECT
                      id,  arr1_elmnt, idx
                      FROM varnt_arr_of_cmpx_tbl,
                      UNNEST(arr_arr[1]) WITH ORDINALITY AS t (arr1_elmnt, idx)"""


class varnttst_arr_of_arr_field_access_varnt(TstView):
    def __init__(self):
        # checked manually
        self.data = [
            {
                "id": 0,
                "arr_arr1": ['"12, 22"', '"hello, hi, bye"'],
                "arr_arr2": ['"are you okay,"', '"mate?"'],
            },
            {"id": 1, "arr_arr1": None, "arr_arr2": None},
            {"id": 2, "arr_arr1": [None, None], "arr_arr2": [None]},
        ]
        self.sql = """CREATE MATERIALIZED VIEW arr_of_arr_field_access_varnt AS SELECT
                      id,
                      arr_arr[1] AS arr_arr1,
                      arr_arr[2] AS arr_arr2
                      FROM varnt_arr_of_cmpx_tbl"""


class varnttst_arr_of_arr_elmnt_access_varnt(TstView):
    def __init__(self):
        # checked manually
        self.data = [
            {"id": 0, "arr_arr11": '"12, 22"', "arr_arr21": '"are you okay,"'},
            {"id": 1, "arr_arr11": None, "arr_arr21": None},
            {"id": 2, "arr_arr11": None, "arr_arr21": None},
        ]
        self.sql = """CREATE MATERIALIZED VIEW arr_of_arr_elmnt_access_varnt AS SELECT
                      id,
                      arr_arr[1][1] AS arr_arr11,
                      arr_arr[2][1] AS arr_arr21
                      FROM varnt_arr_of_cmpx_tbl"""


class varnttst_arr_of_arr_nexist_varnt(TstView):
    def __init__(self):
        # checked manually
        self.data = [
            {"id": 0, "arr_arr3": None, "arr_arr25": None},
            {"id": 1, "arr_arr3": None, "arr_arr25": None},
            {"id": 2, "arr_arr3": None, "arr_arr25": None},
        ]
        self.sql = """CREATE MATERIALIZED VIEW arr_of_arr_nexist_varnt AS SELECT
                      id,
                      arr_arr[3] AS arr_arr3,
                      arr_arr[2][5] AS arr_arr25
                      FROM varnt_arr_of_cmpx_tbl"""


# Map
class varnttst_arr_of_map_unnest_varnt(TstView):
    def __init__(self):
        # checked manually
        self.data = [
            {
                "id": 0,
                "map_val": {'"p"': '"12 friends"', '"q"': '"paint on"'},
                "idx": 1,
            },
            {"id": 0, "map_val": {'"r"': '"48 canvases"', '"s"': '"each!?"'}, "idx": 2},
            {"id": 2, "map_val": {'"p"': None, '"q"': None}, "idx": 1},
            {"id": 2, "map_val": {'"r"': None, '"s"': None}, "idx": 2},
        ]
        self.sql = """CREATE MATERIALIZED VIEW arr_of_map_unnest_varnt AS SELECT
                      id,  map_val, idx
                      FROM varnt_arr_of_cmpx_tbl,
                      UNNEST(arr_map) WITH ORDINALITY AS t (map_val, idx)"""


class varnttst_arr_of_map_field_access_varnt(TstView):
    def __init__(self):
        # checked manually
        self.data = [
            {
                "id": 0,
                "arr_map1": {'"p"': '"12 friends"', '"q"': '"paint on"'},
                "arr_map2": {'"r"': '"48 canvases"', '"s"': '"each!?"'},
            },
            {"id": 1, "arr_map1": None, "arr_map2": None},
            {
                "id": 2,
                "arr_map1": {'"p"': None, '"q"': None},
                "arr_map2": {'"r"': None, '"s"': None},
            },
        ]
        self.sql = """CREATE MATERIALIZED VIEW arr_of_map_field_access_varnt AS SELECT
                      id,
                      arr_map[1] AS arr_map1,
                      arr_map[2] AS arr_map2
                      FROM varnt_arr_of_cmpx_tbl"""


class varnttst_arr_of_map_elmnt_access_varnt(TstView):
    def __init__(self):
        # checked manually
        self.data = [
            {"id": 0, "p": '"12 friends"', "r": '"48 canvases"'},
            {"id": 1, "p": None, "r": None},
            {"id": 2, "p": None, "r": None},
        ]
        self.sql = """CREATE MATERIALIZED VIEW arr_of_map_elmnt_access_varnt AS SELECT
                      id,
                      arr_map[1][CAST('p' AS VARIANT)] AS p,
                      arr_map[2][CAST('r' AS VARIANT)] AS r
                      FROM varnt_arr_of_cmpx_tbl"""


class varnttst_arr_of_map_nexist_varnt(TstView):
    def __init__(self):
        # checked manually
        self.data = [
            {"id": 0, "arr_map4": None, "b": None},
            {"id": 1, "arr_map4": None, "b": None},
            {"id": 2, "arr_map4": None, "b": None},
        ]
        self.sql = """CREATE MATERIALIZED VIEW arr_of_map_nexist_varnt AS SELECT
                      id,
                      arr_map[4] AS arr_map4,
                      arr_map[1][CAST('a' AS VARIANT)] AS b
                      FROM varnt_arr_of_cmpx_tbl"""


# Row
class varnttst_arr_of_row_unnest_varnt(TstView):
    def __init__(self):
        # checked manually
        self.data = [
            {"id": 0, "v1_val": '"345"', "v2_val": '"bye"', "idx": 1},
            {"id": 0, "v1_val": '"b4 breakfast"', "v2_val": '"456 ciao"', "idx": 2},
            {"id": 2, "v1_val": None, "v2_val": None, "idx": 1},
            {"id": 2, "v1_val": None, "v2_val": None, "idx": 2},
        ]
        self.sql = """CREATE MATERIALIZED VIEW arr_of_row_unnest_varnt AS SELECT
                      id,  v1_val, v2_val, idx
                      FROM varnt_arr_of_cmpx_tbl,
                      UNNEST(arr_row) WITH ORDINALITY AS t (v1_val, v2_val, idx)"""


class varnttst_arr_of_row_field_access_varnt(TstView):
    def __init__(self):
        # checked manually
        self.data = [
            {
                "id": 0,
                "arr_row1": {"v1": '"345"', "v2": '"bye"'},
                "arr_row2": {"v1": '"b4 breakfast"', "v2": '"456 ciao"'},
            },
            {"id": 1, "arr_row1": None, "arr_row2": None},
            {
                "id": 2,
                "arr_row1": {"v1": None, "v2": None},
                "arr_row2": {"v1": None, "v2": None},
            },
        ]
        self.sql = """CREATE MATERIALIZED VIEW arr_of_row_field_access_varnt AS SELECT
                      id,
                      arr_row[1] AS arr_row1,
                      arr_row[2] AS arr_row2
                      FROM varnt_arr_of_cmpx_tbl"""


class varnttst_arr_of_row_elmnt_access_varnt(TstView):
    def __init__(self):
        # checked manually
        self.data = [
            {"id": 0, "arr_row12": '"bye"', "arr_row21": '"b4 breakfast"'},
            {"id": 1, "arr_row12": None, "arr_row21": None},
            {"id": 2, "arr_row12": None, "arr_row21": None},
        ]
        self.sql = """CREATE MATERIALIZED VIEW arr_of_row_elmnt_access_varnt AS SELECT
                      id,
                      arr_row[1][2] AS arr_row12,
                      arr_row[2][1] AS arr_row21
                      FROM varnt_arr_of_cmpx_tbl"""


class varnttst_arr_of_row_nexist_varnt(TstView):
    def __init__(self):
        # checked manually
        self.data = [
            {"id": 0, "arr_row5": None},
            {"id": 1, "arr_row5": None},
            {"id": 2, "arr_row5": None},
        ]
        self.sql = """CREATE MATERIALIZED VIEW arr_of_row_nexist_varnt AS SELECT
                      id,
                      arr_row[5] AS arr_row5
                      FROM varnt_arr_of_cmpx_tbl"""


# UDT
class varnttst_arr_of_udt_unnest_varnt(TstView):
    def __init__(self):
        # checked manually
        self.data = [
            {"id": 0, "v1_val": '"ferris"', "v2_val": '"flies"', "idx": 1},
            {"id": 0, "v1_val": '"away"', "v2_val": '"?????"', "idx": 2},
            {"id": 2, "v1_val": None, "v2_val": None, "idx": 1},
            {"id": 2, "v1_val": None, "v2_val": None, "idx": 2},
        ]
        self.sql = """CREATE MATERIALIZED VIEW arr_of_udt_unnest_varnt AS SELECT
                      id,  v1_val, v2_val, idx
                      FROM varnt_arr_of_cmpx_tbl,
                      UNNEST(arr_udt) WITH ORDINALITY AS t (v1_val, v2_val, idx)"""


class varnttst_arr_of_udt_field_access_varnt(TstView):
    def __init__(self):
        # checked manually
        self.data = [
            {
                "id": 0,
                "arr_udt1": {"v1": '"ferris"', "v2": '"flies"'},
                "arr_udt2": {"v1": '"away"', "v2": '"?????"'},
            },
            {"id": 1, "arr_udt1": None, "arr_udt2": None},
            {
                "id": 2,
                "arr_udt1": {"v1": None, "v2": None},
                "arr_udt2": {"v1": None, "v2": None},
            },
        ]
        self.sql = """CREATE MATERIALIZED VIEW arr_of_udt_field_access_varnt AS SELECT
                      id,
                      arr_udt[1] AS arr_udt1,
                      arr_udt[2] AS arr_udt2
                      FROM varnt_arr_of_cmpx_tbl"""


class varnttst_arr_of_udt_elmnt_access_varnt(TstView):
    def __init__(self):
        # checked manually
        self.data = [
            {"id": 0, "arr_udt12": '"flies"', "arr_udt21": '"away"'},
            {"id": 1, "arr_udt12": None, "arr_udt21": None},
            {"id": 2, "arr_udt12": None, "arr_udt21": None},
        ]
        self.sql = """CREATE MATERIALIZED VIEW arr_of_udt_elmnt_access_varnt AS SELECT
                      id,
                      arr_udt[1][2] AS arr_udt12,
                      arr_udt[2][1] AS arr_udt21
                      FROM varnt_arr_of_cmpx_tbl"""


class varnttst_arr_of_udt_nexist_varnt(TstView):
    def __init__(self):
        # checked manually
        self.data = [
            {"id": 0, "arr_udt5": None},
            {"id": 1, "arr_udt5": None},
            {"id": 2, "arr_udt5": None},
        ]
        self.sql = """CREATE MATERIALIZED VIEW arr_of_udt_nexist_varnt AS SELECT
                      id,
                      arr_udt[5] AS arr_udt5
                      FROM varnt_arr_of_cmpx_tbl"""
