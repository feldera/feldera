from tests.runtime_aggtest.aggtst_base import TstView


class cmpxtst_map_var_cmpx_v(TstView):
    def __init__(self):
        # checked manually
        self.data = [
            {
                "id": 0,
                "arr": {"a": ["hello", "bye"], "b": ["see you"]},
                "arr1": {"a": ["hi"]},
                "mapp": {"a": {"a": 12}, "b": {"b": 17}},
                "mapp1": {"x": {"y": 1}},
                "roww": {"a": {"i1": 4, "v1": "cat"}, "b": {"i1": 7, "v1": "dog"}},
                "roww1": {"a": {"i1": None, "v1": "ok"}},
                "udt": {"a": {"i1": 4, "v1": "cat"}},
                "udt1": {"a": {"i1": None, "v1": "dog"}},
            },
            {
                "id": 1,
                "arr": {"a": [None, "bye"], "b": None},
                "arr1": {"a": []},
                "mapp": {"a": None, "b": {"b": None}},
                "mapp1": {"a": {"b": None}},
                "roww": {"a": None, "b": {"i1": 5, "v1": None}},
                "roww1": {"a": {"i1": None, "v1": "x"}},
                "udt": {"a": None, "b": {"i1": 3, "v1": None}},
                "udt1": {"a": {"i1": None, "v1": "y"}},
            },
            {
                "id": 2,
                "arr": {"a": ["x", None]},
                "arr1": {"a": ["z"]},
                "mapp": {"a": {"x": 1}},
                "mapp1": {"a": {"x": None}},
                "roww": {"a": {"i1": 1, "v1": None}},
                "roww1": {"a": {"i1": None, "v1": "valid"}},
                "udt": {"a": {"i1": 9, "v1": None}},
                "udt1": {"a": {"i1": None, "v1": "valid"}},
            },
            {
                "id": 3,
                "arr": None,
                "arr1": {},
                "mapp": None,
                "mapp1": {},
                "roww": None,
                "roww1": {},
                "udt": None,
                "udt1": {},
            },
        ]
        self.sql = """CREATE MATERIALIZED VIEW map_var_cmpx_v AS SELECT
                      *
                      FROM map_var_cmpx_tbl"""


class cmpxtst_map_var_arr_unnest_a(TstView):
    def __init__(self):
        # checked manually
        self.data = [
            {"id": 0, "val": "hello", "idx": 1},
            {"id": 0, "val": "bye", "idx": 2},
            {"id": 1, "val": None, "idx": 1},
            {"id": 1, "val": "bye", "idx": 2},
            {"id": 2, "val": "x", "idx": 1},
            {"id": 2, "val": None, "idx": 2},
        ]
        self.sql = """CREATE MATERIALIZED VIEW map_var_arr_unnest_a AS SELECT
                      id, val, idx
                      FROM map_var_cmpx_tbl,
                      UNNEST(arr['a']) WITH ORDINALITY AS t (val, idx)"""


class cmpxtst_map_var_arr_unnest_b(TstView):
    def __init__(self):
        # checked manually
        self.data = [
            {"id": 0, "val": "see you", "idx": 1},
        ]
        self.sql = """CREATE MATERIALIZED VIEW map_var_arr_unnest_b AS SELECT
                      id, val, idx
                      FROM map_var_cmpx_tbl,
                      UNNEST(arr['b']) WITH ORDINALITY AS t (val, idx)"""


class cmpxtst_map_var_arr1_unnest_a(TstView):
    def __init__(self):
        # checked manually
        self.data = [
            {"id": 0, "val": "hi", "idx": 1},
            {"id": 2, "val": "z", "idx": 1},
        ]
        self.sql = """CREATE MATERIALIZED VIEW map_var_arr1_unnest_a AS SELECT
                      id, val, idx
                      FROM map_var_cmpx_tbl,
                      UNNEST(arr1['a']) WITH ORDINALITY AS t (val, idx)"""


# Field Access
class cmpxtst_map_var_arr_field_access(TstView):
    def __init__(self):
        # checked manually
        self.data = [
            {
                "id": 0,
                "arr_a": ["hello", "bye"],
                "arr_b": ["see you"],
                "arr_a1": ["hi"],
                "arr_b1": None,
            },
            {
                "id": 1,
                "arr_a": [None, "bye"],
                "arr_b": None,
                "arr_a1": [],
                "arr_b1": None,
            },
            {
                "id": 2,
                "arr_a": ["x", None],
                "arr_b": None,
                "arr_a1": ["z"],
                "arr_b1": None,
            },
            {"id": 3, "arr_a": None, "arr_b": None, "arr_a1": None, "arr_b1": None},
        ]
        self.sql = """CREATE MATERIALIZED VIEW map_var_arr_field_access AS SELECT
                      id,
                      arr['a']  AS arr_a,
                      arr['b']  AS arr_b,
                      arr1['a'] AS arr_a1,
                      arr1['b'] AS arr_b1
                      FROM map_var_cmpx_tbl"""


class cmpxtst_map_var_mapp_field_access(TstView):
    def __init__(self):
        # checked manually
        self.data = [
            {
                "id": 0,
                "mapp_a": {"a": 12},
                "mapp_b": {"b": 17},
                "mapp_a1": None,
                "mapp_x1": {"y": 1},
            },
            {
                "id": 1,
                "mapp_a": None,
                "mapp_b": {"b": None},
                "mapp_a1": {"b": None},
                "mapp_x1": None,
            },
            {
                "id": 2,
                "mapp_a": {"x": 1},
                "mapp_b": None,
                "mapp_a1": {"x": None},
                "mapp_x1": None,
            },
            {"id": 3, "mapp_a": None, "mapp_b": None, "mapp_a1": None, "mapp_x1": None},
        ]
        self.sql = """CREATE MATERIALIZED VIEW map_var_mapp_field_access AS SELECT
                      id,
                      mapp['a']  AS mapp_a,
                      mapp['b']  AS mapp_b,
                      mapp1['a'] AS mapp_a1,
                      mapp1['x'] AS mapp_x1
                      FROM map_var_cmpx_tbl"""


class cmpxtst_map_var_roww_field_access(TstView):
    def __init__(self):
        # checked manually
        self.data = [
            {
                "id": 0,
                "roww_a": {"i1": 4, "v1": "cat"},
                "roww_b": {"i1": 7, "v1": "dog"},
                "roww_a1": {"i1": None, "v1": "ok"},
                "roww_b1": None,
            },
            {
                "id": 1,
                "roww_a": None,
                "roww_b": {"i1": 5, "v1": None},
                "roww_a1": {"i1": None, "v1": "x"},
                "roww_b1": None,
            },
            {
                "id": 2,
                "roww_a": {"i1": 1, "v1": None},
                "roww_b": None,
                "roww_a1": {"i1": None, "v1": "valid"},
                "roww_b1": None,
            },
            {"id": 3, "roww_a": None, "roww_b": None, "roww_a1": None, "roww_b1": None},
        ]
        self.sql = """CREATE MATERIALIZED VIEW map_var_roww_field_access AS SELECT
                      id,
                      roww['a']  AS roww_a,
                      roww['b']  AS roww_b,
                      roww1['a'] AS roww_a1,
                      roww1['b'] AS roww_b1
                      FROM map_var_cmpx_tbl"""


class cmpxtst_map_var_udt_field_access(TstView):
    def __init__(self):
        # checked manually
        self.data = [
            {
                "id": 0,
                "udt_a": {"i1": 4, "v1": "cat"},
                "udt_b": None,
                "udt_a1": {"i1": None, "v1": "dog"},
                "udt_b1": None,
            },
            {
                "id": 1,
                "udt_a": None,
                "udt_b": {"i1": 3, "v1": None},
                "udt_a1": {"i1": None, "v1": "y"},
                "udt_b1": None,
            },
            {
                "id": 2,
                "udt_a": {"i1": 9, "v1": None},
                "udt_b": None,
                "udt_a1": {"i1": None, "v1": "valid"},
                "udt_b1": None,
            },
            {"id": 3, "udt_a": None, "udt_b": None, "udt_a1": None, "udt_b1": None},
        ]
        self.sql = """CREATE MATERIALIZED VIEW map_var_udt_field_access AS SELECT
                      id,
                      udt['a']  AS udt_a,
                      udt['b']  AS udt_b,
                      udt1['a'] AS udt_a1,
                      udt1['b'] AS udt_b1
                      FROM map_var_cmpx_tbl"""


# Element Access
class cmpxtst_map_var_arr_element_access(TstView):
    def __init__(self):
        # checked manually
        self.data = [
            {
                "id": 0,
                "arr_a1": "hello",
                "arr_a2": "bye",
                "arr_b1": "see you",
                "arr_b2": None,
                "arr_a1_1": "hi",
                "arr_a1_2": None,
                "arr_b1_1": None,
                "arr_b1_2": None,
            },
            {
                "id": 1,
                "arr_a1": None,
                "arr_a2": "bye",
                "arr_b1": None,
                "arr_b2": None,
                "arr_a1_1": None,
                "arr_a1_2": None,
                "arr_b1_1": None,
                "arr_b1_2": None,
            },
            {
                "id": 2,
                "arr_a1": "x",
                "arr_a2": None,
                "arr_b1": None,
                "arr_b2": None,
                "arr_a1_1": "z",
                "arr_a1_2": None,
                "arr_b1_1": None,
                "arr_b1_2": None,
            },
            {
                "id": 3,
                "arr_a1": None,
                "arr_a2": None,
                "arr_b1": None,
                "arr_b2": None,
                "arr_a1_1": None,
                "arr_a1_2": None,
                "arr_b1_1": None,
                "arr_b1_2": None,
            },
        ]
        self.sql = """CREATE MATERIALIZED VIEW map_var_arr_element_access AS SELECT
                      id,
                      arr['a'][1]   AS arr_a1,
                      arr['a'][2]   AS arr_a2,
                      arr['b'][1]   AS arr_b1,
                      arr['b'][2]   AS arr_b2,
                      arr1['a'][1]  AS arr_a1_1,
                      arr1['a'][2]  AS arr_a1_2,
                      arr1['b'][1]  AS arr_b1_1,
                      arr1['b'][2]  AS arr_b1_2
                      FROM map_var_cmpx_tbl"""


class cmpxtst_map_var_mapp_element_access(TstView):
    def __init__(self):
        # checked manually
        self.data = [
            {
                "id": 0,
                "mapp_a_a": 12,
                "mapp_a_x": None,
                "mapp_b_b": 17,
                "mapp_x1_y": 1,
                "mapp_a1_b": None,
                "mapp_a1_x": None,
            },
            {
                "id": 1,
                "mapp_a_a": None,
                "mapp_a_x": None,
                "mapp_b_b": None,
                "mapp_x1_y": None,
                "mapp_a1_b": None,
                "mapp_a1_x": None,
            },
            {
                "id": 2,
                "mapp_a_a": None,
                "mapp_a_x": 1,
                "mapp_b_b": None,
                "mapp_x1_y": None,
                "mapp_a1_b": None,
                "mapp_a1_x": None,
            },
            {
                "id": 3,
                "mapp_a_a": None,
                "mapp_a_x": None,
                "mapp_b_b": None,
                "mapp_x1_y": None,
                "mapp_a1_b": None,
                "mapp_a1_x": None,
            },
        ]
        self.sql = """CREATE MATERIALIZED VIEW map_var_mapp_element_access AS SELECT
                      id,
                      mapp['a']['a']  AS mapp_a_a,
                      mapp['a']['x']  AS mapp_a_x,
                      mapp['b']['b']  AS mapp_b_b,
                      mapp1['x']['y'] AS mapp_x1_y,
                      mapp1['a']['b'] AS mapp_a1_b,
                      mapp1['a']['x'] AS mapp_a1_x
                      FROM map_var_cmpx_tbl"""


class cmpxtst_map_var_roww_field_element_access(TstView):
    def __init__(self):
        # checked manually
        self.data = [
            {
                "id": 0,
                "roww_a_i1": 4,
                "roww_a_v1": "cat",
                "roww_b_i1": 7,
                "roww_b_v1": "dog",
                "roww_a1_i1": None,
                "roww_a1_v1": "ok",
                "roww_b1_i1": None,
                "roww_b1_v1": None,
            },
            {
                "id": 1,
                "roww_a_i1": None,
                "roww_a_v1": None,
                "roww_b_i1": 5,
                "roww_b_v1": None,
                "roww_a1_i1": None,
                "roww_a1_v1": "x",
                "roww_b1_i1": None,
                "roww_b1_v1": None,
            },
            {
                "id": 2,
                "roww_a_i1": 1,
                "roww_a_v1": None,
                "roww_b_i1": None,
                "roww_b_v1": None,
                "roww_a1_i1": None,
                "roww_a1_v1": "valid",
                "roww_b1_i1": None,
                "roww_b1_v1": None,
            },
            {
                "id": 3,
                "roww_a_i1": None,
                "roww_a_v1": None,
                "roww_b_i1": None,
                "roww_b_v1": None,
                "roww_a1_i1": None,
                "roww_a1_v1": None,
                "roww_b1_i1": None,
                "roww_b1_v1": None,
            },
        ]
        self.sql = """CREATE MATERIALIZED VIEW map_var_roww_field_element_access AS SELECT
                      id,
                      roww['a'].i1   AS roww_a_i1,
                      roww['a'].v1   AS roww_a_v1,
                      roww['b'].i1   AS roww_b_i1,
                      roww['b'].v1   AS roww_b_v1,
                      roww1['a'].i1  AS roww_a1_i1,
                      roww1['a'].v1  AS roww_a1_v1,
                      roww1['b'].i1  AS roww_b1_i1,
                      roww1['b'].v1  AS roww_b1_v1
                      FROM map_var_cmpx_tbl"""


class cmpxtst_map_var_udt_field_element_access(TstView):
    def __init__(self):
        # checked manually
        self.data = [
            {
                "id": 0,
                "udt_a_i1": 4,
                "udt_a_v1": "cat",
                "udt_b_i1": None,
                "udt_b_v1": None,
                "udt_a1_i1": None,
                "udt_a1_v1": "dog",
                "udt_b1_i1": None,
                "udt_b1_v1": None,
            },
            {
                "id": 1,
                "udt_a_i1": None,
                "udt_a_v1": None,
                "udt_b_i1": 3,
                "udt_b_v1": None,
                "udt_a1_i1": None,
                "udt_a1_v1": "y",
                "udt_b1_i1": None,
                "udt_b1_v1": None,
            },
            {
                "id": 2,
                "udt_a_i1": 9,
                "udt_a_v1": None,
                "udt_b_i1": None,
                "udt_b_v1": None,
                "udt_a1_i1": None,
                "udt_a1_v1": "valid",
                "udt_b1_i1": None,
                "udt_b1_v1": None,
            },
            {
                "id": 3,
                "udt_a_i1": None,
                "udt_a_v1": None,
                "udt_b_i1": None,
                "udt_b_v1": None,
                "udt_a1_i1": None,
                "udt_a1_v1": None,
                "udt_b1_i1": None,
                "udt_b1_v1": None,
            },
        ]
        self.sql = """CREATE MATERIALIZED VIEW map_var_udt_field_element_access AS SELECT
                      id,
                      udt['a'].i1   AS udt_a_i1,
                      udt['a'].v1   AS udt_a_v1,
                      udt['b'].i1   AS udt_b_i1,
                      udt['b'].v1   AS udt_b_v1,
                      udt1['a'].i1  AS udt_a1_i1,
                      udt1['a'].v1  AS udt_a1_v1,
                      udt1['b'].i1  AS udt_b1_i1,
                      udt1['b'].v1  AS udt_b1_v1
                      FROM map_var_cmpx_tbl"""


class cmpxtst_map_var_arr_outbound_index(TstView):
    def __init__(self):
        # checked manually
        self.data = [
            {"id": 0, "arr_a3": None, "arr_b2": None},
            {"id": 1, "arr_a3": None, "arr_b2": None},
            {"id": 2, "arr_a3": None, "arr_b2": None},
            {"id": 3, "arr_a3": None, "arr_b2": None},
        ]
        self.sql = """CREATE MATERIALIZED VIEW map_var_arr_outbound_index AS SELECT
                      id,
                      arr['a'][3] AS arr_a3,
                      arr['b'][2] AS arr_b2
                      FROM map_var_cmpx_tbl"""


class cmpxtst_map_var_arr_nonexistent_key(TstView):
    def __init__(self):
        # checked manually
        self.data = [
            {"id": 0, "arr_z": None, "arr_z1": None},
            {"id": 1, "arr_z": None, "arr_z1": None},
            {"id": 2, "arr_z": None, "arr_z1": None},
            {"id": 3, "arr_z": None, "arr_z1": None},
        ]
        self.sql = """CREATE MATERIALIZED VIEW map_var_arr_nonexistent_key AS SELECT
                      id,
                      arr['z']  AS arr_z,
                      arr1['z'] AS arr_z1
                      FROM map_var_cmpx_tbl"""


class cmpxtst_map_var_mapp_nonexistent_key(TstView):
    def __init__(self):
        # checked manually
        self.data = [
            {
                "id": 0,
                "mapp_z": None,
                "mapp_a_z": None,
                "mapp_z1": None,
                "mapp_a1_z": None,
            },
            {
                "id": 1,
                "mapp_z": None,
                "mapp_a_z": None,
                "mapp_z1": None,
                "mapp_a1_z": None,
            },
            {
                "id": 2,
                "mapp_z": None,
                "mapp_a_z": None,
                "mapp_z1": None,
                "mapp_a1_z": None,
            },
            {
                "id": 3,
                "mapp_z": None,
                "mapp_a_z": None,
                "mapp_z1": None,
                "mapp_a1_z": None,
            },
        ]
        self.sql = """CREATE MATERIALIZED VIEW map_var_mapp_nonexistent_key AS SELECT
                      id,
                      mapp['z']       AS mapp_z,
                      mapp['a']['z']  AS mapp_a_z,
                      mapp1['z']      AS mapp_z1,
                      mapp1['a']['z'] AS mapp_a1_z
                      FROM map_var_cmpx_tbl"""


class cmpxtst_map_var_roww_nonexistent_key(TstView):
    def __init__(self):
        # checked manually
        self.data = [
            {"id": 0, "roww_z": None, "roww_z1": None},
            {"id": 1, "roww_z": None, "roww_z1": None},
            {"id": 2, "roww_z": None, "roww_z1": None},
            {"id": 3, "roww_z": None, "roww_z1": None},
        ]
        self.sql = """CREATE MATERIALIZED VIEW map_var_roww_nonexistent_key AS SELECT
                      id,
                      roww['z']  AS roww_z,
                      roww1['z'] AS roww_z1
                      FROM map_var_cmpx_tbl"""


class cmpxtst_map_var_udt_nonexistent_key(TstView):
    def __init__(self):
        # checked manually
        self.data = [
            {"id": 0, "udt_z": None, "udt_z1": None},
            {"id": 1, "udt_z": None, "udt_z1": None},
            {"id": 2, "udt_z": None, "udt_z1": None},
            {"id": 3, "udt_z": None, "udt_z1": None},
        ]
        self.sql = """CREATE MATERIALIZED VIEW map_var_udt_nonexistent_key AS SELECT
                      id,
                      udt['z']  AS udt_z,
                      udt1['z'] AS udt_z1
                      FROM map_var_cmpx_tbl"""
