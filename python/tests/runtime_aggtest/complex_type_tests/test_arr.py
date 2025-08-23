from tests.runtime_aggtest.aggtst_base import TstView, TstTable


# Depth 1 tests
class cmpxtst_array_int_var_tbl(TstTable):
    """Define the table used by the array of int and varchar tests"""

    def __init__(self):
        self.sql = """CREATE TABLE array_int_var_tbl(
                      id INT,
                      c1_arr INT ARRAY,
                      c2_arr INT ARRAY NOT NULL,
                      c3_arr VARCHAR ARRAY,
                      c4_arr VARCHAR ARRAY NOT NULL)"""
        self.data = [
            {
                "id": 0,
                "c1_arr": [1, 2, None],
                "c2_arr": [4, 5, 6],
                "c3_arr": ["Ciao", None],
                "c4_arr": ["Hello", "Hi"],
            },
            {
                "id": 1,
                "c1_arr": [None],
                "c2_arr": [7, 8, 9],
                "c3_arr": [None],
                "c4_arr": ["Bye"],
            },
        ]


class cmpxtst_arr_unnest_int(TstView):
    def __init__(self):
        # Validated on Postgres
        self.data = [
            {"id": 0, "c1_val": 1, "idx": 1},
            {"id": 0, "c1_val": 2, "idx": 2},
            {"id": 0, "c1_val": None, "idx": 3},
            {"id": 1, "c1_val": None, "idx": 1},
        ]
        self.sql = """CREATE MATERIALIZED VIEW arr_unnest_int AS SELECT
                      id,  c1_val, idx
                      FROM array_int_var_tbl,
                      UNNEST(c1_arr) WITH ORDINALITY AS t (c1_val, idx)"""


class cmpxtst_arr_unnest_varchar(TstView):
    def __init__(self):
        # Validated on Postgres
        self.data = [
            {"id": 0, "c3_val": "Ciao", "idx": 1},
            {"id": 0, "c3_val": None, "idx": 2},
            {"id": 1, "c3_val": None, "idx": 1},
        ]
        self.sql = """CREATE MATERIALIZED VIEW arr_unnest_varchar AS SELECT
                      id,  c3_val, idx
                      FROM array_int_var_tbl,
                      UNNEST(c3_arr) WITH ORDINALITY AS t (c3_val, idx)"""


class cmpxtst_arr_field_access(TstView):
    def __init__(self):
        # Validated on Postgres
        self.data = [
            {
                "id": 0,
                "c1_val1": 1,
                "c1_val3": None,
                "c2_val1": 4,
                "c2_val2": 5,
                "c3_val1": "Ciao",
                "c4_val2": "Hi",
            },
            {
                "id": 1,
                "c1_val1": None,
                "c1_val3": None,
                "c2_val1": 7,
                "c2_val2": 8,
                "c3_val1": None,
                "c4_val2": None,
            },
        ]
        self.sql = """CREATE MATERIALIZED VIEW arr_field_access AS SELECT
                      id,
                      c1_arr[1] AS c1_val1,
                      c1_arr[3] AS c1_val3,
                      c2_arr[1] AS c2_val1,
                      c2_arr[2] AS c2_val2,
                      c3_arr[1] AS c3_val1,
                      c4_arr[2] AS c4_val2
                      FROM array_int_var_tbl"""
