from tests.runtime_aggtest.aggtst_base import TstView, TstTable


class cmpxtst_arr_of_udt_unnest_tbl(TstTable):
    """Define the table used by the array of UDT UNNEST tests"""

    def __init__(self):
        self.sql = """CREATE TYPE int_unnest_type AS(i1 INT NOT NULL, i2 INT);
                      CREATE TABLE arr_of_udt_unnest_tbl(
                      id INT,
                      c1_arr int_unnest_type ARRAY,
                      c2_arr int_unnest_type ARRAY NOT NULL)"""
        self.data = [
            {
                "id": 0,
                "c1_arr": [{"i1": 1, "i2": None}, {"i1": 5, "i2": 6}, None],
                "c2_arr": [{"i1": 3, "i2": None}, {"i1": 4, "i2": None}],
            },
            {"id": 1, "c1_arr": [None], "c2_arr": [{"i1": 5, "i2": 6}]},
            {"id": 2, "c1_arr": None, "c2_arr": [{"i1": 7, "i2": 8}]},
        ]


class cmpxtst_arr_of_udt_unnest1(TstView):
    def __init__(self):
        # checked manually
        self.data = [
            {"id": 0, "i1": 2, "i2": None, "idx": 1},
            {"id": 0, "i1": 6, "i2": 7, "idx": 2},
            {"id": 0, "i1": None, "i2": None, "idx": 3},
            {"id": 1, "i1": None, "i2": None, "idx": 1},
        ]
        self.sql = """CREATE MATERIALIZED VIEW arr_of_udt_unnest1 AS SELECT
                      id,  i1_val + 1 AS i1, i2_val + 1 AS i2,  idx
                      FROM arr_of_udt_unnest_tbl,
                      UNNEST(c1_arr) WITH ORDINALITY AS t (i1_val, i2_val, idx)"""


class cmpxtst_arr_of_udt_unnest2(TstView):
    def __init__(self):
        # checked manually
        self.data = [
            {"id": 0, "i1": 4, "i2": None, "idx": 1},
            {"id": 0, "i1": 5, "i2": None, "idx": 2},
            {"id": 1, "i1": 6, "i2": 7, "idx": 1},
            {"id": 2, "i1": 8, "i2": 9, "idx": 1},
        ]
        self.sql = """CREATE MATERIALIZED VIEW arr_of_udt_unnest2 AS SELECT
                      id,  i1_val + 1 AS i1, i2_val + 1 AS i2,  idx
                      FROM arr_of_udt_unnest_tbl,
                      UNNEST(c2_arr) WITH ORDINALITY AS t (i1_val, i2_val, idx)"""
