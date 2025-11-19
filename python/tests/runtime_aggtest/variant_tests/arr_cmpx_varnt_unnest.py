from tests.runtime_aggtest.aggtst_base import TstView, TstTable


class varnttst_arr_of_cmpx_unnest_tbl(TstTable):
    """Define the table used by the array of complex type UNNEST tests"""

    def __init__(self):
        self.sql = """CREATE TYPE varnt_unnest_type AS(i1 VARIANT NOT NULL, i2 VARIANT);
                      CREATE TABLE varnt_arr_of_cmpx_unnest_tbl(
                      id INT,
                      arr_arr VARIANT ARRAY ARRAY NULL,
                      arr_arr1 VARIANT ARRAY ARRAY NOT NULL,
                      arr_map MAP<VARIANT , VARIANT> ARRAY NULL,
                      arr_map1 MAP<VARIANT , VARIANT> ARRAY NOT NULL,
                      arr_row ROW(i1 VARIANT NULL, i2 VARIANT NOT NULL) ARRAY NOT NULL,
                      arr_row1 ROW(i1 VARIANT NOT NULL, i2 VARIANT NULL) ARRAY,
                      arr_udt varnt_unnest_type ARRAY NULL,
                      arr_udt1 varnt_unnest_type ARRAY NOT NULL
                      )"""
        self.data = [
            {
                "id": 0,
                "arr_arr": [["1, 2", None], [None]],
                "arr_arr1": [["1, 2"]],
                "arr_map": [{"p": "1", "q": "2"}, {"p": None, "q": None}],
                "arr_map1": [{"p": "1", "q": "2"}],
                "arr_row": [{"i1": "1", "i2": None}, {"i1": "2", "i2": "3"}],
                "arr_row1": None,
                "arr_udt": [{"i1": "1", "i2": None}, {"i1": "5", "i2": "6"}, None],
                "arr_udt1": [{"i1": "3", "i2": None}, {"i1": "4", "i2": None}],
            },
            {
                "id": 1,
                "arr_arr": [["4, 5"], [8]],
                "arr_arr1": [["3"], [None]],
                "arr_map": [{"p": "4", "q": "5"}, {"q": None}],
                "arr_map1": [{"p": "3"}, None],
                "arr_row": [{"i1": "3", "i2": "4"}, {"i1": "4", "i2": "5"}],
                "arr_row1": [{"i1": None, "i2": "6"}, {"i1": "3", "i2": "7"}, None],
                "arr_udt": None,
                "arr_udt1": [None],
            },
        ]


# Array
class varnttst_arr_of_arr_cmpxunnest_varnt(TstView):
    def __init__(self):
        # checked manually
        self.data = [
            {"id": 0, "arr_val": "VARCHAR", "idx": 1},
            {"id": 0, "arr_val": "NULL", "idx": 2},
            {"id": 1, "arr_val": "VARCHAR", "idx": 1},
        ]
        self.sql = """CREATE MATERIALIZED VIEW arr_of_arr_cmpxunnest_varnt AS SELECT
                      id,  TYPEOF(arr_val) AS arr_val, idx
                      FROM varnt_arr_of_cmpx_unnest_tbl,
                      UNNEST(arr_arr[1]) WITH ORDINALITY AS t (arr_val, idx)"""


class varnttst_arr_of_arr_cmpxunnest_varnt2(TstView):
    def __init__(self):
        # checked manually
        self.data = [
            {"id": 0, "arr_val": "NULL", "idx": 1},
            {"id": 1, "arr_val": "BIGINT UNSIGNED", "idx": 1},
        ]
        self.sql = """CREATE MATERIALIZED VIEW arr_of_arr_cmpxunnest_varnt2 AS SELECT
                      id,  TYPEOF(arr_val) AS arr_val, idx
                      FROM varnt_arr_of_cmpx_unnest_tbl,
                      UNNEST(arr_arr[2]) WITH ORDINALITY AS t (arr_val, idx)"""


class varnttst_arr_of_arr1_cmpxunnest_varnt(TstView):
    def __init__(self):
        # checked manually
        self.data = [
            {"id": 0, "arr_val": "VARCHAR", "idx": 1},
            {"id": 1, "arr_val": "VARCHAR", "idx": 1},
        ]
        self.sql = """CREATE MATERIALIZED VIEW arr_of_arr1_cmpxunnest_varnt AS SELECT
                      id,  TYPEOF(arr_val) AS arr_val, idx
                      FROM varnt_arr_of_cmpx_unnest_tbl,
                      UNNEST(arr_arr1[1]) WITH ORDINALITY AS t (arr_val, idx)"""


class varnttst_arr_of_arr1_cmpxunnest_varnt2(TstView):
    def __init__(self):
        # checked manually
        self.data = [{"id": 1, "arr_val": "NULL", "idx": 1}]
        self.sql = """CREATE MATERIALIZED VIEW arr_of_arr1_cmpxunnest_varnt2 AS SELECT
                      id,  TYPEOF(arr_val) AS arr_val, idx
                      FROM varnt_arr_of_cmpx_unnest_tbl,
                      UNNEST(arr_arr1[2]) WITH ORDINALITY AS t (arr_val, idx)"""


# Map
class varnttst_arr_of_map_cmpxunnest_varnt(TstView):
    def __init__(self):
        # checked manually
        self.data = [
            {"id": 0, "map_p": "NULL", "map_q": "NULL", "idx": 2},
            {"id": 0, "map_p": "VARCHAR", "map_q": "VARCHAR", "idx": 1},
            {"id": 1, "map_p": "NULL", "map_q": "NULL", "idx": 2},
            {"id": 1, "map_p": "VARCHAR", "map_q": "VARCHAR", "idx": 1},
        ]
        self.sql = """CREATE MATERIALIZED VIEW arr_of_map_cmpxunnest_varnt AS SELECT
                      id,  TYPEOF(map_val['p']) AS map_p, TYPEOF(map_val['q']) AS map_q, idx
                      FROM varnt_arr_of_cmpx_unnest_tbl,
                      UNNEST(arr_map) WITH ORDINALITY AS t (map_val, idx)"""


# Row
class varnttst_arr_of_row_cmpxunnest_varnt(TstView):
    def __init__(self):
        # checked manually
        self.data = [
            {"id": 0, "i1": "VARCHAR", "i2": "VARIANT", "idx": 1},
            {"id": 0, "i1": "VARCHAR", "i2": "VARCHAR", "idx": 2},
            {"id": 1, "i1": "VARCHAR", "i2": "VARCHAR", "idx": 1},
            {"id": 1, "i1": "VARCHAR", "i2": "VARCHAR", "idx": 2},
        ]
        self.sql = """CREATE MATERIALIZED VIEW arr_of_row_cmpxunnest_varnt AS SELECT
                      id,  TYPEOF(i1_val) AS i1, TYPEOF(i2_val) AS i2, idx
                      FROM varnt_arr_of_cmpx_unnest_tbl,
                      UNNEST(arr_row) WITH ORDINALITY AS t (i1_val, i2_val, idx)"""


class varnttst_arr_of_row_cmpxunnest_varnt1(TstView):
    def __init__(self):
        # checked manually
        self.data = [
            {"id": 1, "i1": "VARIANT", "i2": "VARCHAR", "idx": 1},
            {"id": 1, "i1": "VARCHAR", "i2": "VARCHAR", "idx": 2},
            {"id": 1, "i1": "NULL", "i2": "NULL", "idx": 3},
        ]
        self.sql = """CREATE MATERIALIZED VIEW arr_of_row_cmpxunnest_varnt1 AS SELECT
                      id,  TYPEOF(i1_val) AS i1, TYPEOF(i2_val) AS i2, idx
                      FROM varnt_arr_of_cmpx_unnest_tbl,
                      UNNEST(arr_row1) WITH ORDINALITY AS t (i1_val, i2_val, idx)"""


# UDT
class varnttst_arr_of_udt_cmpxunnest_varnt(TstView):
    def __init__(self):
        # checked manually
        self.data = [
            {"id": 0, "i1": "NULL", "i2": "NULL", "idx": 3},
            {"id": 0, "i1": "VARCHAR", "i2": "NULL", "idx": 1},
            {"id": 0, "i1": "VARCHAR", "i2": "VARCHAR", "idx": 2},
        ]
        self.sql = """CREATE MATERIALIZED VIEW arr_of_udt_cmpxunnest_varnt AS SELECT
                      id,  TYPEOF(i1_val) AS i1, TYPEOF(i2_val) AS i2, idx
                      FROM varnt_arr_of_cmpx_unnest_tbl,
                      UNNEST(arr_udt) WITH ORDINALITY AS t (i1_val, i2_val, idx)"""


class varnttst_arr_of_udt_cmpxunnest2_varnt(TstView):
    def __init__(self):
        # checked manually
        self.data = [
            {"id": 0, "i1": "VARCHAR", "i2": "NULL", "idx": 1},
            {"id": 0, "i1": "VARCHAR", "i2": "NULL", "idx": 2},
            {"id": 1, "i1": "NULL", "i2": "NULL", "idx": 1},
        ]
        self.sql = """CREATE MATERIALIZED VIEW arr_of_udt_cmpxunnest2_varnt AS SELECT
                      id,  TYPEOF(i1_val) AS i1, TYPEOF(i2_val) AS i2,  idx
                      FROM varnt_arr_of_cmpx_unnest_tbl,
                      UNNEST(arr_udt1) WITH ORDINALITY AS t (i1_val, i2_val, idx)"""
