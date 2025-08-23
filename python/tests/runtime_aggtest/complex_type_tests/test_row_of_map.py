from tests.runtime_aggtest.aggtst_base import TstView, TstTable


# Depth 2 tests => Row of Map
class cmpxtst_row_of_map_tbl(TstTable):
    """Define the table used by the ROW of MAP tests"""

    def __init__(self):
        self.sql = """CREATE TABLE row_of_map_tbl(
                      id INT,
                      c1 ROW(m1_int MAP<VARCHAR, INT> NOT NULL, m2_int MAP<VARCHAR, INT> NULL),
                      c2 ROW(m1_var MAP<VARCHAR, VARCHAR> NOT NULL, m2_var MAP<VARCHAR, VARCHAR> NULL))"""
        self.data = [
            {
                "id": 0,
                "c1": None,
                "c2": {
                    "m1_var": {"x": "hi", "y": "hello"},
                    "m2_var": {"a": "ciao", "b": "bye"},
                },
            },
            {
                "id": 1,
                "c1": {"m1_int": {"1": None, "2": 66}, "m2_int": None},
                "c2": {"m1_var": {"q": None, "r": "adios"}, "m2_var": None},
            },
            {
                "id": 2,
                "c1": {"m1_int": {"3": 22, "4": 55}, "m2_int": {"5": 67, "6": 56}},
                "c2": None,
            },
        ]


class cmpxtst_row_of_map_field_access(TstView):
    def __init__(self):
        # checked manually
        self.data = [
            {
                "id": 0,
                "m1_int": None,
                "m2_int": None,
                "m1_var": {"x": "hi", "y": "hello"},
                "m2_var": {"a": "ciao", "b": "bye"},
            },
            {
                "id": 1,
                "m1_int": {"1": None, "2": 66},
                "m2_int": None,
                "m1_var": {"q": None, "r": "adios"},
                "m2_var": None,
            },
            {
                "id": 2,
                "m1_int": {"3": 22, "4": 55},
                "m2_int": {"5": 67, "6": 56},
                "m1_var": None,
                "m2_var": None,
            },
        ]
        self.sql = """CREATE MATERIALIZED VIEW row_of_map_field_access AS SELECT
                      id,
                      c1[1] AS m1_int,
                      c1[2] AS m2_int,
                      c2[1] AS m1_var,
                      c2[2] AS m2_var
                      FROM row_of_map_tbl"""


class cmpxtst_row_of_map_element_access(TstView):
    def __init__(self):
        # checked manually
        self.data = [
            {"id": 0, "c1_two": None, "r": None},
            {"id": 1, "c1_two": 66, "r": "adios"},
            {"id": 2, "c1_two": None, "r": None},
        ]
        self.sql = """CREATE MATERIALIZED VIEW row_of_map_tbl_element_access AS SELECT
                      id,
                      c1[1]['2'] AS c1_two,
                      c2[1]['r'] AS r
                      FROM row_of_map_tbl"""


class cmpxtst_row_of_map_idx_outbound(TstView):
    def __init__(self):
        # checked manually
        self.data = [{"id": 0, "u": None}, {"id": 1, "u": None}, {"id": 2, "u": None}]
        self.sql = """CREATE MATERIALIZED VIEW row_of_map_idx_outbound AS SELECT
                      id,
                      c1[2]['u'] AS u
                      FROM row_of_map_tbl"""


# IGNORE: https://github.com/feldera/feldera/issues/3770
class ignore_row_of_map_idx_outbound(TstView):
    def __init__(self):
        # checked manually
        self.data = []
        self.sql = """CREATE MATERIALIZED VIEW row_of_map_idx_outbound AS SELECT
                      id,
                      c1[3] AS c13_val
                      FROM row_of_map_tbl"""


class cmpxtst_row_of_map_elmnt_nexist(TstView):
    def __init__(self):
        # checked manually
        self.data = [{"id": 0, "q": None}, {"id": 1, "q": None}, {"id": 2, "q": None}]
        self.sql = """CREATE MATERIALIZED VIEW row_of_map_elmnt_nexist AS SELECT
                      id,
                      c1[2]['q'] AS q
                      FROM row_of_map_tbl"""
