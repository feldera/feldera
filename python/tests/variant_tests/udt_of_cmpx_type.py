from tests.aggregate_tests.aggtst_base import TstView, TstTable


# UDT of Complex Type Tests
class varnttst_udt_of_cmpx_tbl(TstTable):
    """Define the table used by the UDT of complex type tests"""

    def __init__(self):
        self.sql = """CREATE TYPE arr_varnt_type AS(arr_vnt VARIANT ARRAY);
                      CREATE TYPE map_varnt_type AS(map_vnt MAP<VARIANT, VARIANT>);
                      CREATE TYPE row_varnt_type AS(row_vnt ROW(v1 VARIANT, v2 VARIANT));
                      CREATE TYPE udt_varnt_type AS(udt_vnt varnt_type);
                      CREATE TABLE varnt_udt_of_cmpx_tbl(
                      id INT,
                      udt_arr arr_varnt_type,
                      udt_map map_varnt_type,
                      udt_row row_varnt_type,
                      udt_udt udt_varnt_type
                      )"""
        self.data = [
            {
                "id": 0,
                "udt_arr": {"arr_vnt": ["45, 62", "bye, bye, friends!!"]},
                "udt_map": {"map_vnt": {"c": "sayonara,", "d": "everyone!"}},
                "udt_row": {"row_vnt": {"v1": "24, 25", "v2": "adios"}},
                "udt_udt": {"udt_vnt": {"v1": "are you", "v2": "alright, mate?"}},
            },
            {
                "id": 1,
                "udt_arr": None,
                "udt_map": None,
                "udt_row": None,
                "udt_udt": None,
            },
            {
                "id": 2,
                "udt_arr": {"arr_vnt": [None]},
                "udt_map": {"map_vnt": {"c": None, "d": None}},
                "udt_row": {"row_vnt": {"v1": None, "v2": None}},
                "udt_udt": {"udt_vnt": {"v1": None, "v2": None}},
            },
        ]


class varnttst_read_udt_of_cmpx(TstView):
    def __init__(self):
        # checked manually
        self.data = [
            {
                "id": 0,
                "udt_arr": {"arr_vnt": ['"45, 62"', '"bye, bye, friends!!"']},
                "udt_map": {"map_vnt": {'"c"': '"sayonara,"', '"d"': '"everyone!"'}},
                "udt_row": {"row_vnt": {"v1": '"24, 25"', "v2": '"adios"'}},
                "udt_udt": {"udt_vnt": {"v1": '"are you"', "v2": '"alright, mate?"'}},
            },
            {
                "id": 1,
                "udt_arr": None,
                "udt_map": None,
                "udt_row": None,
                "udt_udt": None,
            },
            {
                "id": 2,
                "udt_arr": {"arr_vnt": [None]},
                "udt_map": {"map_vnt": {'"c"': None, '"d"': None}},
                "udt_row": {"row_vnt": {"v1": "null", "v2": "null"}},
                "udt_udt": {"udt_vnt": {"v1": None, "v2": None}},
            },
        ]
        self.sql = """CREATE MATERIALIZED VIEW read_udt_of_cmpx AS SELECT
                      *
                      FROM varnt_udt_of_cmpx_tbl"""


# Array
class varnttst_udt_of_arr_unnest_varnt_elmnt(TstView):
    def __init__(self):
        # checked manually
        self.data = [
            {"id": 0, "udt1_elmnt": '"45, 62"', "idx": 1},
            {"id": 0, "udt1_elmnt": '"bye, bye, friends!!"', "idx": 2},
            {"id": 2, "udt1_elmnt": None, "idx": 1},
        ]
        self.sql = """CREATE MATERIALIZED VIEW udt_of_arr_unnest_varnt_elmnt AS SELECT
                      id,  udt1_elmnt, idx
                      FROM varnt_udt_of_cmpx_tbl,
                      UNNEST(varnt_udt_of_cmpx_tbl.udt_arr[1]) WITH ORDINALITY AS t (udt1_elmnt, idx)"""


class varnttst_udt_of_arr_field_access_varnt(TstView):
    def __init__(self):
        # checked manually
        self.data = [
            {
                "id": 0,
                "udt_arr1": ['"45, 62"', '"bye, bye, friends!!"'],
                "arr_vnt": ['"45, 62"', '"bye, bye, friends!!"'],
            },
            {"id": 1, "udt_arr1": None, "arr_vnt": None},
            {"id": 2, "udt_arr1": [None], "arr_vnt": [None]},
        ]
        self.sql = """CREATE MATERIALIZED VIEW udt_of_arr_field_access_varnt AS SELECT
                      id,
                      varnt_udt_of_cmpx_tbl.udt_arr[1] AS udt_arr1,
                      varnt_udt_of_cmpx_tbl.udt_arr.arr_vnt AS arr_vnt
                      FROM varnt_udt_of_cmpx_tbl"""


class varnttst_udt_of_arr_elmnt_access_varnt(TstView):
    def __init__(self):
        # checked manually
        self.data = [
            {"id": 0, "udt_arr11": '"45, 62"', "arr_vnt": '"45, 62"'},
            {"id": 1, "udt_arr11": None, "arr_vnt": None},
            {"id": 2, "udt_arr11": None, "arr_vnt": None},
        ]
        self.sql = """CREATE MATERIALIZED VIEW udt_of_arr_elmnt_access_varnt AS SELECT
                      id,
                      varnt_udt_of_cmpx_tbl.udt_arr[1][1] AS udt_arr11,
                      varnt_udt_of_cmpx_tbl.udt_arr.arr_vnt[1] AS arr_vnt
                      FROM varnt_udt_of_cmpx_tbl"""


class varnttst_udt_of_arr_nexist_varnt(TstView):
    def __init__(self):
        # checked manually
        self.data = [
            {"id": 0, "udt_arr25": None},
            {"id": 1, "udt_arr25": None},
            {"id": 2, "udt_arr25": None},
        ]
        self.sql = """CREATE MATERIALIZED VIEW udt_of_arr_nexist_varnt AS SELECT
                      id,
                      varnt_udt_of_cmpx_tbl.udt_arr[1][3] AS udt_arr25
                      FROM varnt_udt_of_cmpx_tbl"""


# Map
class varnttst_udt_of_map_field_access_varnt(TstView):
    def __init__(self):
        # checked manually
        self.data = [
            {
                "id": 0,
                "udt_map1": {'"c"': '"sayonara,"', '"d"': '"everyone!"'},
                "map_vnt": {'"c"': '"sayonara,"', '"d"': '"everyone!"'},
            },
            {"id": 1, "udt_map1": None, "map_vnt": None},
            {
                "id": 2,
                "udt_map1": {'"c"': None, '"d"': None},
                "map_vnt": {'"c"': None, '"d"': None},
            },
        ]
        self.sql = """CREATE MATERIALIZED VIEW udt_of_map_field_access_varnt AS SELECT
                      id,
                      udt_map[1] AS udt_map1,
                      varnt_udt_of_cmpx_tbl.udt_map.map_vnt AS map_vnt
                      FROM varnt_udt_of_cmpx_tbl"""


class varnttst_udt_of_map_elmnt_access_varnt(TstView):
    def __init__(self):
        # checked manually
        self.data = [
            {"id": 0, "c": '"sayonara,"', "d": '"everyone!"'},
            {"id": 1, "c": None, "d": None},
            {"id": 2, "c": None, "d": None},
        ]
        self.sql = """CREATE MATERIALIZED VIEW udt_of_map_elmnt_access_varnt AS SELECT
                      id,
                      udt_map[1][CAST('c' AS VARIANT)] AS c,
                      varnt_udt_of_cmpx_tbl.udt_map.map_vnt[CAST('d' AS VARIANT)] AS d
                      FROM varnt_udt_of_cmpx_tbl"""


class varnttst_udt_of_map_nexist_varnt(TstView):
    def __init__(self):
        # checked manually
        self.data = [{"id": 0, "b": None}, {"id": 1, "b": None}, {"id": 2, "b": None}]
        self.sql = """CREATE MATERIALIZED VIEW udt_of_map_nexist_varnt AS SELECT
                      id,
                      udt_map[1][CAST('b' AS VARIANT)] AS b
                      FROM varnt_udt_of_cmpx_tbl"""


# Row
class varnttst_udt_of_row_field_access_varnt(TstView):
    def __init__(self):
        # checked manually
        self.data = [
            {
                "id": 0,
                "udt_row1": {"v1": '"24, 25"', "v2": '"adios"'},
                "row_vnt": {"v1": '"24, 25"', "v2": '"adios"'},
            },
            {"id": 1, "udt_row1": None, "row_vnt": None},
            {
                "id": 2,
                "udt_row1": {"v1": "null", "v2": "null"},
                "row_vnt": {"v1": "null", "v2": "null"},
            },
        ]
        self.sql = """CREATE MATERIALIZED VIEW udt_of_row_field_access_varnt AS SELECT
                      id,
                      varnt_udt_of_cmpx_tbl.udt_row[1] AS udt_row1,
                      varnt_udt_of_cmpx_tbl.udt_row.row_vnt AS row_vnt
                      FROM varnt_udt_of_cmpx_tbl"""


class varnttst_udt_of_row_elmnt_access_varnt(TstView):
    def __init__(self):
        # checked manually
        self.data = [
            {"id": 0, "udt_row11": '"24, 25"', "udt_row12": '"adios"'},
            {"id": 1, "udt_row11": None, "udt_row12": None},
            {"id": 2, "udt_row11": "null", "udt_row12": "null"},
        ]
        self.sql = """CREATE MATERIALIZED VIEW udt_of_row_elmnt_access_varnt AS SELECT
                      id,
                      varnt_udt_of_cmpx_tbl.udt_row[1][1] AS udt_row11,
                      varnt_udt_of_cmpx_tbl.udt_row.row_vnt.v2 AS udt_row12
                      FROM varnt_udt_of_cmpx_tbl"""


# UDT
class varnttst_udt_of_udt_field_access_varnt(TstView):
    def __init__(self):
        # checked manually
        self.data = [
            {
                "id": 0,
                "udt_udt1": {"v1": '"are you"', "v2": '"alright, mate?"'},
                "udt_vnt": {"v1": '"are you"', "v2": '"alright, mate?"'},
            },
            {"id": 1, "udt_udt1": None, "udt_vnt": None},
            {
                "id": 2,
                "udt_udt1": {"v1": None, "v2": None},
                "udt_vnt": {"v1": None, "v2": None},
            },
        ]
        self.sql = """CREATE MATERIALIZED VIEW udt_of_udt_field_access_varnt AS SELECT
                      id,
                      varnt_udt_of_cmpx_tbl.udt_udt[1] AS udt_udt1,
                      varnt_udt_of_cmpx_tbl.udt_udt.udt_vnt AS udt_vnt
                      FROM varnt_udt_of_cmpx_tbl"""


class varnttst_udt_of_udt_elmnt_access_varnt(TstView):
    def __init__(self):
        # checked manually
        self.data = [
            {"id": 0, "udt_udt11": '"are you"', "udt_udt12": '"alright, mate?"'},
            {"id": 1, "udt_udt11": None, "udt_udt12": None},
            {"id": 2, "udt_udt11": None, "udt_udt12": None},
        ]
        self.sql = """CREATE MATERIALIZED VIEW udt_of_udt_elmnt_access_varnt AS SELECT
                      id,
                      varnt_udt_of_cmpx_tbl.udt_udt[1][1] AS udt_udt11,
                      varnt_udt_of_cmpx_tbl.udt_udt.udt_vnt.v2 AS udt_udt12
                      FROM varnt_udt_of_cmpx_tbl"""
