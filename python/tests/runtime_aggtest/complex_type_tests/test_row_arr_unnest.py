from tests.runtime_aggtest.aggtst_base import TstView, TstTable


class cmpxtst_row_of_arr_unnest_tbl(TstTable):
    """Define the table used by the ROW of Array UNNEST tests"""

    def __init__(self):
        self.sql = """CREATE TABLE row_of_arr_unnest_tbl(
                      id INT,
                      c1 ROW(i1 INT ARRAY NOT NULL, i2 INT ARRAY NULL) NOT NULL,
                      c2 ROW(i1 INT ARRAY NULL, i2 INT ARRAY NOT NULL))"""
        self.data = [
            {"id": 0, "c1": {"i1": [2, 3, None], "i2": [None]}, "c2": None},
            {
                "id": 1,
                "c1": {"i1": [1, None], "i2": [3, None]},
                "c2": {"i1": None, "i2": [6]},
            },
        ]


class cmpxtst_row_of_arr_unnest11(TstView):
    def __init__(self):
        # checked manually
        self.data = [
            {"id": 0, "c11": 3, "idx": 1},
            {"id": 0, "c11": 4, "idx": 2},
            {"id": 0, "c11": None, "idx": 3},
            {"id": 1, "c11": 2, "idx": 1},
            {"id": 1, "c11": None, "idx": 2},
        ]
        self.sql = """CREATE MATERIALIZED VIEW row_of_arr_unnest11 AS SELECT
                      id, i1_val + 1 AS c11, idx
                      FROM row_of_arr_unnest_tbl,
                      UNNEST(row_of_arr_unnest_tbl.c1.i1) WITH ORDINALITY AS t (i1_val, idx)"""


class cmpxtst_row_of_arr_unnest12(TstView):
    def __init__(self):
        # checked manually
        self.data = [
            {"id": 0, "c12": None, "idx": 1},
            {"id": 1, "c12": 4, "idx": 1},
            {"id": 1, "c12": None, "idx": 2},
        ]
        self.sql = """CREATE MATERIALIZED VIEW row_of_arr_unnest12 AS SELECT
                      id, i2_val + 1 AS c12, idx
                      FROM row_of_arr_unnest_tbl,
                      UNNEST(row_of_arr_unnest_tbl.c1.i2) WITH ORDINALITY AS t (i2_val, idx)"""


class cmpxtst_row_of_arr_unnest21(TstView):
    def __init__(self):
        # checked manually
        self.data = []  # empty because either c2 is a NULL ROW or i1 is a NULL ARRAY
        self.sql = """CREATE MATERIALIZED VIEW row_of_arr_unnest21 AS SELECT
                      id, i1_val + 1 AS c21, idx
                      FROM row_of_arr_unnest_tbl,
                      UNNEST(row_of_arr_unnest_tbl.c2.i1) WITH ORDINALITY AS t (i1_val, idx)"""


class cmpxtst_row_of_arr_unnest22(TstView):
    def __init__(self):
        # checked manually
        self.data = [{"id": 1, "c22": 7, "idx": 1}]
        self.sql = """CREATE MATERIALIZED VIEW row_of_arr_unnest22 AS SELECT
                      id, i2_val + 1 AS c22, idx
                      FROM row_of_arr_unnest_tbl,
                      UNNEST(row_of_arr_unnest_tbl.c2.i2) WITH ORDINALITY AS t (i2_val, idx)"""
