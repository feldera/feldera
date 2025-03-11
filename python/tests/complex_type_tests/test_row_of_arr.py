from tests.aggregate_tests.aggtst_base import TstView, TstTable


# Depth 2 tests => Row of Array
class cmpxtst_row_of_arr_tbl(TstTable):
    """Define the table used by the ROW of Array tests"""

    def __init__(self):
        self.sql = """CREATE TABLE row_of_arr_tbl(
                      id INT,
                      c1 ROW(i1 INT ARRAY NOT NULL, v1 VARCHAR ARRAY NULL) NOT NULL,
                      c2 ROW(i2 INT ARRAY NULL, v2 VARCHAR ARRAY NOT NULL))"""
        self.data = [
            {"id": 0, "c1": {"i1": [20, 22, 23], "v1": [None]}, "c2": None},
            {
                "id": 1,
                "c1": {
                    "i1": [12, 13, 77, 10, 2],
                    "v1": ["hi", "how are you?", None],
                },
                "c2": {"i2": [1, 2, None], "v2": ["bye", "see you later!"]},
            },
            {
                "id": 2,
                "c1": {
                    "i1": [5],
                    "v1": ["are", "you", "good?"],
                },
                "c2": {"i2": [None], "v2": ["adios"]},
            },
        ]


class cmpxtst_row_of_arr_unnest(TstView):
    def __init__(self):
        # checked manually
        self.data = [
            {"id": 0, "v1_val": None, "idx": 1},
            {"id": 1, "v1_val": "hi", "idx": 1},
            {"id": 1, "v1_val": "how are you?", "idx": 2},
            {"id": 1, "v1_val": None, "idx": 3},
            {"id": 2, "v1_val": "are", "idx": 1},
            {"id": 2, "v1_val": "you", "idx": 2},
            {"id": 2, "v1_val": "good?", "idx": 3},
        ]
        self.sql = """CREATE MATERIALIZED VIEW row_of_arr_unnest AS SELECT
                      id, v1_val, idx
                      FROM row_of_arr_tbl,
                      UNNEST(row_of_arr_tbl.c1.v1) WITH ORDINALITY AS t (v1_val, idx)"""


class cmpxtst_row_of_arr_field_access(TstView):
    def __init__(self):
        # checked manually
        self.data = [
            {"id": 0, "i1": [20, 22, 23], "v1": [None], "i2": None, "v2": None},
            {
                "id": 1,
                "i1": [12, 13, 77, 10, 2],
                "v1": ["hi", "how are you?", None],
                "i2": [1, 2, None],
                "v2": ["bye", "see you later!"],
            },
            {
                "id": 2,
                "i1": [5],
                "v1": ["are", "you", "good?"],
                "i2": [None],
                "v2": ["adios"],
            },
        ]
        self.sql = """CREATE MATERIALIZED VIEW row_of_arr_field_access AS SELECT
                      id,
                      row_of_arr_tbl.c1.i1,
                      row_of_arr_tbl.c1.v1,
                      row_of_arr_tbl.c2.i2,
                      row_of_arr_tbl.c2.v2
                      FROM row_of_arr_tbl"""


class cmpxtst_row_of_arr_elmnt_access(TstView):
    def __init__(self):
        # checked manually
        self.data = [
            {"id": 0, "v2_val2": None, "v2_val2_alt": None},
            {"id": 1, "v2_val2": "see you later!", "v2_val2_alt": "see you later!"},
            {"id": 2, "v2_val2": None, "v2_val2_alt": None},
        ]
        self.sql = """CREATE MATERIALIZED VIEW row_of_arr_elmnt_access AS SELECT
                      id,
                      row_of_arr_tbl.c2.v2[2] AS v2_val2,
                      row_of_arr_tbl.c2[2][2] AS v2_val2_alt
                      FROM row_of_arr_tbl"""


class cmpxtst_row_of_arr_idx_outbound(TstView):
    def __init__(self):
        # checked manually
        self.data = [
            {"id": 0, "v2_val5": None},
            {"id": 1, "v2_val5": None},
            {"id": 2, "v2_val5": None},
        ]
        self.sql = """CREATE MATERIALIZED VIEW row_of_arr_idx_outbound AS SELECT
                          id,
                          row_of_arr_tbl.c2.v2[5] AS v2_val5
                          FROM row_of_arr_tbl"""


class cmpxtst_row_of_arr_elmnt_nexist(TstView):
    def __init__(self):
        # checked manually
        self.data = [{"id": 2, "c22_val": [None]}]
        self.sql = """CREATE MATERIALIZED VIEW row_of_arr_elmnt_nexist AS SELECT
                      id,
                      row_of_arr_tbl.c2.i2 AS c22_val
                      FROM row_of_arr_tbl
                      WHERE id = 2"""
