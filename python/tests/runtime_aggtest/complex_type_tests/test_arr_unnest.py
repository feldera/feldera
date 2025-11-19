from tests.runtime_aggtest.aggtst_base import TstView, TstTable


class cmpxtst_arr_unnest_tbl(TstTable):
    """Define the table used by the array UNNEST tests"""

    def __init__(self):
        self.sql = """CREATE TABLE arr_unnest_tbl(
                      id INT,
                      c1_arr INT ARRAY,
                      c2_arr INT ARRAY NOT NULL)"""
        self.data = [
            {
                "id": 0,
                "c1_arr": [1, 2, None],
                "c2_arr": [4, 5, None],
            },
            {
                "id": 1,
                "c1_arr": [None, None],
                "c2_arr": [None, None, None],
            },
            {
                "id": 2,
                "c1_arr": None,
                "c2_arr": [7, 8],
            },
        ]


class cmpxtst_arr_unnest_int1(TstView):
    def __init__(self):
        # checked manually
        self.data = [
            {"id": 0, "c1": 2, "idx": 1},
            {"id": 0, "c1": 3, "idx": 2},
            {"id": 0, "c1": None, "idx": 3},
            {"id": 1, "c1": None, "idx": 1},
            {"id": 1, "c1": None, "idx": 2},
        ]
        self.sql = """CREATE MATERIALIZED VIEW arr_unnest_int1 AS SELECT
                      id,  c1_val + 1 AS c1, idx
                      FROM arr_unnest_tbl,
                      UNNEST(c1_arr) WITH ORDINALITY AS t (c1_val, idx)"""


class cmpxtst_arr_unnest_int2(TstView):
    def __init__(self):
        # checked manually
        self.data = [
            {"id": 0, "c2": 5, "idx": 1},
            {"id": 0, "c2": 6, "idx": 2},
            {"id": 0, "c2": None, "idx": 3},
            {"id": 1, "c2": None, "idx": 1},
            {"id": 1, "c2": None, "idx": 2},
            {"id": 1, "c2": None, "idx": 3},
            {"id": 2, "c2": 8, "idx": 1},
            {"id": 2, "c2": 9, "idx": 2},
        ]
        self.sql = """CREATE MATERIALIZED VIEW arr_unnest_int2 AS SELECT
                      id,  c2_val + 1 AS c2, idx
                      FROM arr_unnest_tbl,
                      UNNEST(c2_arr) WITH ORDINALITY AS t (c2_val, idx)"""
