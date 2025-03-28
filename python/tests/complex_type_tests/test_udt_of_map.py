from tests.aggregate_tests.aggtst_base import TstView, TstTable


# Depth 2 tests => User Defined Type of Map Type
class cmpxtst_udt_of_map_tbl(TstTable):
    """Define the table used by the user defined type of Map type tests"""

    def __init__(self):
        self.sql = """CREATE TYPE map_type1 AS(m1_int MAP<VARCHAR, INT> NOT NULL, m2_int MAP<VARCHAR, INT>);
                      CREATE TYPE map_type2 AS(m1_var MAP<VARCHAR, VARCHAR> NOT NULL, m2_var MAP<VARCHAR, VARCHAR>);
                      CREATE TABLE udt_of_map_tbl(
                      id INT,
                      c1 map_type1,
                      c2 map_type2 NOT NULL)"""
        self.data = [
            {
                "id": 0,
                "c1": None,
                "c2": {
                    "m1_var": {"q": "hi", "r": None},
                    "m2_var": {"s": None, "t": "ciao"},
                },
            },
            {
                "id": 1,
                "c1": {"m1_int": {"1": None, "2": 44}, "m2_int": {"1": 22, "2": None}},
                "c2": {"m1_var": {"a": "hello", "b": None}, "m2_var": None},
            },
        ]


class cmpxtst_udt_of_map_field_access(TstView):
    def __init__(self):
        # checked manually
        self.data = [
            {
                "id": 0,
                "m1_int": None,
                "m2_int": None,
                "m1_var": {"q": "hi", "r": None},
                "m2_var": {"s": None, "t": "ciao"},
            },
            {
                "id": 1,
                "m1_int": {"1": None, "2": 44},
                "m2_int": {"1": 22, "2": None},
                "m1_var": {"a": "hello", "b": None},
                "m2_var": None,
            },
        ]
        self.sql = """CREATE MATERIALIZED VIEW udt_of_map_field_access AS SELECT
                      id,
                      c1[1] AS m1_int,
                      c1[2] AS m2_int,
                      c2[1] AS m1_var,
                      c2[2] AS m2_var
                      FROM udt_of_map_tbl"""


class cmpxtst_udt_of_map_element_access(TstView):
    def __init__(self):
        # checked manually
        self.data = [{"id": 0, "b": None}]
        self.sql = """CREATE MATERIALIZED VIEW udt_of_map_element_access AS SELECT
                      id,
                      c2[1]['b'] AS b
                      FROM udt_of_map_tbl
                      WHERE id = 0"""


class cmpxtst_udt_of_map_idx_outbound(TstView):
    def __init__(self):
        # checked manually
        self.data = [{"id": 0, "c": None}, {"id": 1, "c": None}]
        self.sql = """CREATE MATERIALIZED VIEW udt_of_map_idx_outbound AS SELECT
                      id,
                      c2[1]['c'] AS c
                      FROM udt_of_map_tbl"""


class cmpxtst_udt_of_map_elmnt_nexist(TstView):
    def __init__(self):
        # checked manually
        self.data = [{"id": 0, "m1_int": None}]
        self.sql = """CREATE MATERIALIZED VIEW udt_of_map_elmnt_nexist AS SELECT
                      id,
                      c1[1] AS m1_int
                      FROM udt_of_map_tbl
                      WHERE id = 0"""
