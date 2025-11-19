from tests.runtime_aggtest.aggtst_base import TstView, TstTable


class cmpxtst_array_of_row_unnest_tbl(TstTable):
    """Define the table used by the array of row UNNEST tests"""

    def __init__(self):
        self.sql = """CREATE TABLE arr_of_row_unnest_tbl(
                      id INT,
                      c1_arr ROW(i1 INT NOT NULL, i2 INT NULL)  ARRAY NOT NULL,
                      c2_arr ROW(i1 INT NULL, i2 INT NOT NULL)  ARRAY)"""
        self.data = [
            {
                "id": 0,
                "c1_arr": [{"i1": 1, "i2": None}, {"i1": 2, "i2": 3}],
                "c2_arr": None,
            },
            {
                "id": 1,
                "c1_arr": [{"i1": 3, "i2": 4}, {"i1": 4, "i2": 5}],
                "c2_arr": [{"i1": None, "i2": 6}, {"i1": 3, "i2": 7}, None],
            },
        ]


class cmpxtst_arr_of_row_unnest1(TstView):
    def __init__(self):
        # checked manually
        self.data = [
            {"id": 0, "i1": 2, "i2": None, "idx": 1},
            {"id": 0, "i1": 3, "i2": 4, "idx": 2},
            {"id": 1, "i1": 4, "i2": 5, "idx": 1},
            {"id": 1, "i1": 5, "i2": 6, "idx": 2},
        ]
        self.sql = """CREATE MATERIALIZED VIEW arr_of_row_unnest1 AS SELECT
                      id,  i1_val + 1 AS i1, i2_val + 1 AS i2,  idx
                      FROM arr_of_row_unnest_tbl,
                      UNNEST(c1_arr) WITH ORDINALITY AS t (i1_val, i2_val, idx)"""


class cmpxtst_arr_of_row_unnest2(TstView):
    def __init__(self):
        # checked manually
        self.data = [
            {"id": 1, "i1": None, "i2": 7, "idx": 1},
            {"id": 1, "i1": 4, "i2": 8, "idx": 2},
            {"id": 1, "i1": None, "i2": None, "idx": 3},
        ]
        self.sql = """CREATE MATERIALIZED VIEW arr_of_row_unnest2 AS SELECT
                      id,  i1_val + 1 AS i1, i2_val + 1 AS i2,  idx
                      FROM arr_of_row_unnest_tbl,
                      UNNEST(c2_arr) WITH ORDINALITY AS t (i1_val, i2_val, idx)"""
