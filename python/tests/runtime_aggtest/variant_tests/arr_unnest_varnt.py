from tests.runtime_aggtest.aggtst_base import TstView, TstTable


class varnttst_varnt_unnest_tbl(TstTable):
    """Define the table used by the complex type UNNEST tests"""

    def __init__(self):
        self.sql = """
                      CREATE TABLE varnt_unnest_tbl(
                      id INT,
                      arr VARIANT ARRAY,
                      arr2 VARIANT ARRAY NOT NULL
                      )"""
        self.data = [
            {
                "id": 0,
                "arr": [{"x": 1}, {"x": 2}, None],
                "arr2": [{"y": 4}, {"y": 5}, None],
            },
            {"id": 1, "arr": None, "arr2": [{"y": 8}, {"y": 9}]},
            {"id": 2, "arr": [None, None], "arr2": [None, None]},
        ]


class varnttst_arr_of_arrunnest_varnt(TstView):
    def __init__(self):
        # checked manually
        self.data = [
            {"id": 0, "arr_val": "1", "idx": 1},
            {"id": 0, "arr_val": "2", "idx": 2},
            {"id": 0, "arr_val": None, "idx": 3},
            {"id": 2, "arr_val": None, "idx": 1},
            {"id": 2, "arr_val": None, "idx": 2},
        ]
        self.sql = """CREATE MATERIALIZED VIEW arrunnest_varnt AS SELECT
                      id,  TO_JSON(arr_val['x']) AS arr_val, idx
                      FROM varnt_unnest_tbl,
                      UNNEST(arr) WITH ORDINALITY AS t (arr_val, idx)"""


class varnttst_arr_of_arrunnest_varnt2(TstView):
    def __init__(self):
        # checked manually
        self.data = [
            {"id": 0, "arr2_val": "4", "idx": 1},
            {"id": 0, "arr2_val": "5", "idx": 2},
            {"id": 0, "arr2_val": None, "idx": 3},
            {"id": 1, "arr2_val": "8", "idx": 1},
            {"id": 1, "arr2_val": "9", "idx": 2},
            {"id": 2, "arr2_val": None, "idx": 1},
            {"id": 2, "arr2_val": None, "idx": 2},
        ]
        self.sql = """CREATE MATERIALIZED VIEW arrunnest_varnt2 AS SELECT
                      id,  TO_JSON(arr2_val['y']) AS arr2_val, idx
                      FROM varnt_unnest_tbl,
                      UNNEST(arr2) WITH ORDINALITY AS t (arr2_val, idx)"""
