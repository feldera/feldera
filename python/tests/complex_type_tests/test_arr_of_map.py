from tests.aggregate_tests.aggtst_base import TstView, TstTable


# Depth 2 tests => Array of Map
class cmpxtst_arr_of_map_tbl(TstTable):
    """Define the table used by the array of map tests"""

    def __init__(self):
        self.sql = """CREATE TABLE arr_of_map_tbl(
                      id INT,
                      c1_arr MAP<VARCHAR, VARCHAR> ARRAY NOT NULL,
                      c2_arr MAP<VARCHAR, INT> ARRAY)"""
        self.data = [
            {
                "id": 0,
                "c1_arr": [{"p": "hi", "q": "hello"}, {"r": "bye", "s": "ciao"}],
                "c2_arr": [{"a": 23, "b": 45}],
            },
            {
                "id": 1,
                "c1_arr": [
                    {"u": "elo", "v": "konichiwa"},
                    {"w": "bye", "z": "sayonara"},
                ],
                "c2_arr": [{"x": 22, "y": 44}],
            },
        ]


class cmpxtst_arr_of_map_unnest(TstView):
    def __init__(self):
        self.data = [
            {"id": 0, "c1_val": {"p": "hi", "q": "hello"}, "idx": 1},
            {"id": 0, "c1_val": {"r": "bye", "s": "ciao"}, "idx": 2},
            {"id": 1, "c1_val": {"u": "elo", "v": "konichiwa"}, "idx": 1},
            {"id": 1, "c1_val": {"w": "bye", "z": "sayonara"}, "idx": 2},
        ]
        self.sql = """CREATE MATERIALIZED VIEW arr_of_map_unnest AS SELECT
                      id,  c1_val, idx
                      FROM arr_of_map_tbl,
                      UNNEST(c1_arr) WITH ORDINALITY AS t (c1_val, idx)"""


class cmpxtst_arr_of_map_field_access(TstView):
    def __init__(self):
        # checked manually
        self.data = [
            {
                "id": 0,
                "c1_val1": {"p": "hi", "q": "hello"},
                "c1_val2": {"r": "bye", "s": "ciao"},
                "c2_val1": {"a": 23, "b": 45},
            },
            {
                "id": 1,
                "c1_val1": {"u": "elo", "v": "konichiwa"},
                "c1_val2": {"w": "bye", "z": "sayonara"},
                "c2_val1": {"x": 22, "y": 44},
            },
        ]
        self.sql = """CREATE MATERIALIZED VIEW arr_of_map_field_access AS SELECT
                      id,
                      c1_arr[1] AS c1_val1,
                      c1_arr[2] AS c1_val2,
                      c2_arr[1] AS c2_val1
                      FROM arr_of_map_tbl"""


class cmpxtst_arr_of_map_element_access(TstView):
    def __init__(self):
        # checked manually
        self.data = [
            {"id": 0, "p": "hi", "v": None},
            {"id": 1, "p": None, "v": "konichiwa"},
        ]
        self.sql = """CREATE MATERIALIZED VIEW arr_of_map_elmnt_access AS SELECT
                      id,
                      c1_arr[1]['p'] AS p,
                      c1_arr[1]['v'] AS v
                      FROM arr_of_map_tbl"""
