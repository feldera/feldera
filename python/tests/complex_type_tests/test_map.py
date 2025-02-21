from tests.aggregate_tests.aggtst_base import TstView, TstTable


# Depth 1 tests
class cmpxtst_map_int_var_tbl(TstTable):
    """Define the table used by the MAP of int and varchar tests"""

    def __init__(self):
        self.sql = """CREATE TABLE map_int_var_tbl(
                      id INT,
                      c1 MAP<INT, INT> NOT NULL,
                      c2 MAP<INT, INT>,
                      c3 MAP<VARCHAR, VARCHAR>)"""
        self.data = [
            {
                "id": 0,
                "c1": {1: 22, 2: 44},
                "c2": None,
                "c3": {"x": "hi", "y": "hello"},
            },
            {
                "id": 1,
                "c1": {1: 33, 2: 66},
                "c2": {1: None, 2: 88},
                "c3": {"a": "bye", "b": None},
            },
        ]


class cmpxtst_map_access_int_by_key(TstView):
    def __init__(self):
        # checked manually
        self.data = [
            {"id": 0, "val1": 22, "val2": None},
            {"id": 1, "val1": 33, "val2": 88},
        ]
        self.sql = """CREATE MATERIALIZED VIEW map_access_int_by_key AS SELECT
                      id,
                      c1[1] AS val1,
                      c2[2] AS val2
                      FROM map_int_var_tbl"""


class cmpxtst_map_access_var_by_key(TstView):
    def __init__(self):
        # checked manually
        self.data = [{"id": 0, "x": "hi", "a": None}, {"id": 1, "x": None, "a": "bye"}]
        self.sql = """CREATE MATERIALIZED VIEW map_access_by_key AS SELECT
                      id,
                      c3['x'] AS x,
                      c3['a'] AS a
                      FROM map_int_var_tbl"""
