from tests.runtime_aggtest.aggtst_base import TstView, TstTable


class cmpxtst_arr_of_arr_unnest_tbl(TstTable):
    """Define the table used by the array of array UNNEST tests"""

    def __init__(self):
        self.sql = """CREATE TABLE arr_of_arr_unnest_tbl(
                      id INT,
                      c1_arr INT ARRAY ARRAY,
                      c2_arr INT ARRAY ARRAY NOT NULL)"""
        self.data = [
            {
                "id": 0,
                "c1_arr": [[1, 2, None], [None]],
                "c2_arr": [[1, 2]],
            },
            {
                "id": 1,
                "c1_arr": [[4, 5], [8]],
                "c2_arr": [[3], [None]],
            },
            {
                "id": 2,
                "c1_arr": None,
                "c2_arr": [[None], None],
            },
        ]


class cmpxtst_arr_of_arr_unnest_int11(TstView):
    def __init__(self):
        # checked manually
        self.data = [
            {"id": 0, "c11": 2, "idx": 1},
            {"id": 0, "c11": 3, "idx": 2},
            {"id": 0, "c11": None, "idx": 3},
            {"id": 1, "c11": 5, "idx": 1},
            {"id": 1, "c11": 6, "idx": 2},
        ]
        self.sql = """CREATE MATERIALIZED VIEW arr_of_arr_unnest_int11 AS SELECT
                      id,  c1_val + 1 AS c11, idx
                      FROM arr_of_arr_unnest_tbl,
                      UNNEST(c1_arr[1]) WITH ORDINALITY AS t (c1_val, idx)"""


class cmpxtst_arr_of_arr_unnest_int12(TstView):
    def __init__(self):
        # checked manually
        self.data = [{"id": 0, "c12": None, "idx": 1}, {"id": 1, "c12": 9, "idx": 1}]
        self.sql = """CREATE MATERIALIZED VIEW arr_of_arr_unnest_int12 AS SELECT
                      id,  c1_val + 1 AS c12, idx
                      FROM arr_of_arr_unnest_tbl,
                      UNNEST(c1_arr[2]) WITH ORDINALITY AS t (c1_val, idx)"""


class cmpxtst_arr_of_arr_unnest_int21(TstView):
    def __init__(self):
        # checked manually
        self.data = [
            {"id": 0, "c21": 2, "idx": 1},
            {"id": 0, "c21": 3, "idx": 2},
            {"id": 1, "c21": 4, "idx": 1},
            {"id": 2, "c21": None, "idx": 1},
        ]
        self.sql = """CREATE MATERIALIZED VIEW arr_of_arr_unnest_int21 AS SELECT
                      id,  c2_val + 1 AS c21, idx
                      FROM arr_of_arr_unnest_tbl,
                      UNNEST(c2_arr[1]) WITH ORDINALITY AS t (c2_val, idx)"""


class cmpxtst_arr_of_arr_unnest_int22(TstView):
    def __init__(self):
        # checked manually
        self.data = [{"id": 1, "c21": None, "idx": 1}]
        self.sql = """CREATE MATERIALIZED VIEW arr_of_arr_unnest_int22 AS SELECT
                      id,  c2_val + 1 AS c21, idx
                      FROM arr_of_arr_unnest_tbl,
                      UNNEST(c2_arr[2]) WITH ORDINALITY AS t (c2_val, idx)"""
