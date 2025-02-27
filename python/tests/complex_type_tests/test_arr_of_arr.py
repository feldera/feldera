from tests.aggregate_tests.aggtst_base import TstView, TstTable


# Depth 2 tests =-> Array of Array
class cmpxtst_arr_of_arr_tbl(TstTable):
    """Define the table used by the array of array tests"""

    def __init__(self):
        self.sql = """CREATE TABLE arr_of_arr_tbl(
                      id INT,
                      c1_arr INT ARRAY ARRAY,
                      c2_arr INT ARRAY ARRAY NOT NULL,
                      c3_arr VARCHAR ARRAY ARRAY,
                      c4_arr VARCHAR ARRAY ARRAY NOT NULL)"""
        self.data = [
            {
                "id": 0,
                "c1_arr": [[1, 2, None], [None]],
                "c2_arr": [[1, 2]],
                "c3_arr": [["bye", None], ["ciao"], [None]],
                "c4_arr": [["hi", "hello"]],
            },
            {
                "id": 1,
                "c1_arr": [[4, 5], [8]],
                "c2_arr": [[3]],
                "c3_arr": [[None], None],
                "c4_arr": [["ola"]],
            },
        ]


class cmpxtst_arr_of_arr_unnest_int(TstView):
    def __init__(self):
        # checked manually
        self.data = [
            {"id": 0, "c1_val": [1, 2, None], "idx": 1},
            {"id": 0, "c1_val": [None], "idx": 2},
            {"id": 1, "c1_val": [4, 5], "idx": 1},
            {"id": 1, "c1_val": [8], "idx": 2},
        ]
        self.sql = """CREATE MATERIALIZED VIEW arr_of_arr_unnest_int AS SELECT
                      id,  c1_val, idx
                      FROM arr_of_arr_tbl,
                      UNNEST(c1_arr) WITH ORDINALITY AS t (c1_val, idx)"""


class cmpxtst_arr_of_arr_unnest_element_int(TstView):
    def __init__(self):
        # checked manually
        self.data = [
            {"id": 0, "c11_elmnt": 1, "idx": 1},
            {"id": 0, "c11_elmnt": 2, "idx": 2},
            {"id": 0, "c11_elmnt": None, "idx": 3},
            {"id": 1, "c11_elmnt": 4, "idx": 1},
            {"id": 1, "c11_elmnt": 5, "idx": 2},
        ]
        self.sql = """CREATE MATERIALIZED VIEW arr_of_arr_unnest_element_int AS SELECT
                      id,  c11_elmnt, idx
                      FROM arr_of_arr_tbl,
                      UNNEST(c1_arr[1]) WITH ORDINALITY AS t (c11_elmnt, idx)"""


class cmpxtst_arr_of_arr_unnest_varchar(TstView):
    def __init__(self):
        # checked manually
        self.data = [
            {"id": 0, "c3_val": ["bye", None], "idx": 1},
            {"id": 0, "c3_val": ["ciao"], "idx": 2},
            {"id": 0, "c3_val": [None], "idx": 3},
            {"id": 1, "c3_val": [None], "idx": 1},
            {"id": 1, "c3_val": None, "idx": 2},
        ]
        self.sql = """CREATE MATERIALIZED VIEW arr_of_arr_unnest_varchar AS SELECT
                      id,  c3_val, idx
                      FROM arr_of_arr_tbl,
                      UNNEST(c3_arr) WITH ORDINALITY AS t (c3_val, idx)"""


class cmpxtst_arr_of_arr_unnest_element_varchar(TstView):
    def __init__(self):
        # checked manually
        self.data = [
            {"id": 0, "c31_elmnt": "bye", "idx": 1},
            {"id": 0, "c31_elmnt": None, "idx": 2},
            {"id": 1, "c31_elmnt": None, "idx": 1},
        ]
        self.sql = """CREATE MATERIALIZED VIEW arr_of_arr_unnest_element_varchar AS SELECT
                      id,  c31_elmnt, idx
                      FROM arr_of_arr_tbl,
                      UNNEST(c3_arr[1]) WITH ORDINALITY AS t (c31_elmnt, idx)"""


class cmpxtst_arr_of_arr_elmnt_idx_outbound(TstView):
    def __init__(self):
        # checked manually
        self.data = []  # does not return anything since c1_arr does not have index = 3
        self.sql = """CREATE MATERIALIZED VIEW arr_of_arr_elmnt_idx_outbound AS SELECT
                      id,  c13_elmnt, idx
                      FROM arr_of_arr_tbl,
                      UNNEST(c1_arr[3]) WITH ORDINALITY AS t (c13_elmnt, idx)"""


class cmpxtst_arr_of_arr_field_access(TstView):
    def __init__(self):
        # checked manually
        self.data = [
            {
                "id": 0,
                "c1_val1": [1, 2, None],
                "c1_val2": [None],
                "c2_val1": [1, 2],
                "c3_val1": ["bye", None],
                "c3_val2": ["ciao"],
                "c4_val1": ["hi", "hello"],
            },
            {
                "id": 1,
                "c1_val1": [4, 5],
                "c1_val2": [8],
                "c2_val1": [3],
                "c3_val1": [None],
                "c3_val2": None,
                "c4_val1": ["ola"],
            },
        ]
        self.sql = """CREATE MATERIALIZED VIEW arr_of_arr_field_access AS SELECT
                      id,
                      c1_arr[1] AS c1_val1,
                      c1_arr[2] AS c1_val2,
                      c2_arr[1] AS c2_val1,
                      c3_arr[1] AS c3_val1,
                      c3_arr[2] AS c3_val2,
                      c4_arr[1] AS c4_val1
                      FROM arr_of_arr_tbl"""


class cmpxtst_arr_of_arr_elmnt_access(TstView):
    def __init__(self):
        # checked manually
        self.data = [
            {
                "id": 0,
                "c11_elmnt": 1,
                "c21_elmnt": 1,
                "c31_elmnt": "bye",
                "c41_elmnt": "hi",
            },
            {
                "id": 1,
                "c11_elmnt": 4,
                "c21_elmnt": 3,
                "c31_elmnt": None,
                "c41_elmnt": "ola",
            },
        ]
        self.sql = """CREATE MATERIALIZED VIEW arr_of_arr_elmnt_access AS SELECT
                      id,
                      c1_arr[1][1] AS c11_elmnt,
                      c2_arr[1][1] AS c21_elmnt,
                      c3_arr[1][1] AS c31_elmnt,
                      c4_arr[1][1] AS c41_elmnt
                      FROM arr_of_arr_tbl"""


class cmpxtst_arr_of_arr_elmnt_access_idx_outbound(TstView):
    def __init__(self):
        # checked manually
        self.data = [
            {"c1_val4": None, "c14_elmnt": None},
            {"c1_val4": None, "c14_elmnt": None},
        ]
        self.sql = """CREATE MATERIALIZED VIEW arr_of_arr_elmnt_access_idx_outbound AS SELECT
                      c1_arr[4] AS c1_val4,
                      c1_arr[1][4] AS c14_elmnt
                      FROM arr_of_arr_tbl"""
