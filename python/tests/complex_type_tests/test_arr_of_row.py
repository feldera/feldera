from tests.aggregate_tests.aggtst_base import TstView, TstTable


# Depth 2 tests => Array of Row
class cmpxtst_array_of_row_tbl(TstTable):
    """Define the table used by the array of row tests"""

    def __init__(self):
        self.sql = """CREATE TABLE arr_of_row_tbl(
                      id INT,
                      c1_arr ROW(i1 INT NOT NULL, v1 VARCHAR NULL)  ARRAY NOT NULL,
                      c2_arr ROW(i2 INT NULL, v2 VARCHAR NOT NULL)  ARRAY)"""
        self.data = [
            {
                "id": 0,
                "c1_arr": [{"i1": 1, "v1": None}, {"i1": 2, "v1": "hello"}],
                "c2_arr": None,
            },
            {
                "id": 1,
                "c1_arr": [{"i1": 3, "v1": "bye"}, {"i1": 4, "v1": "adios"}],
                "c2_arr": [{"i2": None, "v2": "hi"}, {"i2": 3, "v2": "ciao"}, None],
            },
        ]


class cmpxtst_arr_of_row_unnest(TstView):
    def __init__(self):
        # checked manually
        self.data = [
            {"id": 0, "i1_val": 1, "v1_val": None, "idx": 1},
            {"id": 0, "i1_val": 2, "v1_val": "hello", "idx": 2},
            {"id": 1, "i1_val": 3, "v1_val": "bye", "idx": 1},
            {"id": 1, "i1_val": 4, "v1_val": "adios", "idx": 2},
        ]
        self.sql = """CREATE MATERIALIZED VIEW arr_of_row_unnest AS SELECT
                      id,  i1_val, v1_val,  idx
                      FROM arr_of_row_tbl,
                      UNNEST(c1_arr) WITH ORDINALITY AS t (i1_val, v1_val, idx)"""


class cmpxtst_arr_of_row_field_access(TstView):
    def __init__(self):
        # checked manually
        self.data = [
            {
                "id": 0,
                "c1_val1": {"i1": 1, "v1": None},
                "c1_val2": {"i1": 2, "v1": "hello"},
                "c2_val1": None,
                "c2_val3": None,
            },
            {
                "id": 1,
                "c1_val1": {"i1": 3, "v1": "bye"},
                "c1_val2": {"i1": 4, "v1": "adios"},
                "c2_val1": {"i2": None, "v2": "hi"},
                "c2_val3": None,
            },
        ]
        self.sql = """CREATE MATERIALIZED VIEW arr_of_row_field_access AS SELECT
                      id,
                      c1_arr[1] AS c1_val1,
                      c1_arr[2] AS c1_val2,
                      c2_arr[1] AS c2_val1,
                      c2_arr[3] AS c2_val3
                      FROM arr_of_row_tbl"""


class cmpxtst_arr_of_row_elmnt_access(TstView):
    def __init__(self):
        # checked manually
        self.data = [
            {"id": 0, "c12_elmnt": None, "c21_elmnt": None},
            {"id": 1, "c12_elmnt": "bye", "c21_elmnt": 3},
        ]
        self.sql = """CREATE MATERIALIZED VIEW arr_of_row_elmnt_access AS SELECT
                      id,
                      c1_arr[1][2] AS c12_elmnt,
                      c2_arr[2][1] AS c21_elmnt
                      FROM arr_of_row_tbl"""


class cmpxtst_arr_of_row_arr_idx_outbound(TstView):
    def __init__(self):
        # checked manually
        self.data = [
            {"id": 0, "c15_val": None, "c151_elmnt": None},
            {"id": 1, "c15_val": None, "c151_elmnt": None},
        ]
        self.sql = """CREATE MATERIALIZED VIEW arr_of_row_arr_idx_outbound AS SELECT
                      id,
                      c1_arr[5] AS c15_val,
                      c1_arr[5].i1 AS c151_elmnt
                      FROM arr_of_row_tbl"""


# ignore => error: Unknown field 'i5' when inner row column does not exist
class ignoretst_arr_of_row_idx_nexist(TstView):
    def __init__(self):
        # no result because of SQL error
        self.data = []
        self.sql = """CREATE MATERIALIZED VIEW arr_of_row_idx_nexist AS SELECT
                      id,
                      c1_arr[2].i5 AS c12_val
                      FROM arr_of_row_tbl"""
