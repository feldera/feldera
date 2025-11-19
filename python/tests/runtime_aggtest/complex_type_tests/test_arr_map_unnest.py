from tests.runtime_aggtest.aggtst_base import TstView, TstTable


class cmpxtst_arr_of_map_unnest_tbl(TstTable):
    """Define the table used by the array of map UNNEST tests"""

    def __init__(self):
        self.sql = """CREATE TABLE arr_of_map_unnest_tbl(
                      id INT,
                      c1_arr MAP<VARCHAR, INT> ARRAY,
                      c2_arr MAP<VARCHAR, INT> ARRAY NOT NULL)"""
        self.data = [
            {
                "id": 0,
                "c1_arr": [{"p": 1, "q": 2}, {"p": None, "q": None}],
                "c2_arr": [{"p": 1, "q": 2}],
            },
            {
                "id": 1,
                "c1_arr": [{"p": 4, "q": 5}, {"q": None}],
                "c2_arr": [{"p": 3}, None],
            },
            {
                "id": 2,
                "c1_arr": None,
                "c2_arr": [None],
            },
        ]


class cmpxtst_arr_of_map_unnest1(TstView):
    def __init__(self):
        # checked manually
        self.data = [
            {"id": 0, "c1": 2, "idx": 1},
            {"id": 0, "c1": 2, "idx": 2},
            {"id": 1, "c1": 1, "idx": 2},
            {"id": 1, "c1": 2, "idx": 1},
        ]
        self.sql = """CREATE MATERIALIZED VIEW arr_of_map_unnest1 AS SELECT
                      id,  CARDINALITY(c1_val) AS c1, idx
                      FROM arr_of_map_unnest_tbl,
                      UNNEST(c1_arr) WITH ORDINALITY AS t (c1_val, idx)"""


class cmpxtst_arr_of_map_unnest2(TstView):
    def __init__(self):
        # checked manually
        self.data = [
            {"id": 0, "c1": 2, "idx": 1},
            {"id": 1, "c1": 1, "idx": 1},
            {"id": 1, "c1": None, "idx": 2},
            {"id": 2, "c1": None, "idx": 1},
        ]
        self.sql = """CREATE MATERIALIZED VIEW arr_of_map_unnest2 AS SELECT
                      id,  CARDINALITY(c2_val) AS c1, idx
                      FROM arr_of_map_unnest_tbl,
                      UNNEST(c2_arr) WITH ORDINALITY AS t (c2_val, idx)"""
