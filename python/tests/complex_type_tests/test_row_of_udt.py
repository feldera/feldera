from tests.aggregate_tests.aggtst_base import TstView, TstTable


# Depth 2 tests => Row of User Defined Type
class cmpxtst_row_of_udt_tbl(TstTable):
    """Define the table used by the ROW of User Defined Type tests"""

    def __init__(self):
        self.sql = """CREATE TABLE row_of_udt_tbl(
                      id INT,
                      c1 ROW(c1_int int_type NOT NULL, c1_var var_type NULL) NOT NULL,
                      c2 ROW(c2_int int_type NULL, c2_var var_type NOT NULL))"""
        self.data = [
            {
                "id": 0,
                "c1": {"c1_int": (1, None), "c1_var": ("bye", None)},
                "c2": {"c2_int": None, "c2_var": ("hi", "konichiwa")},
            },
            {
                "id": 1,
                "c1": {"c1_int": (12, 18), "c1_var": None},
                "c2": None,
            },
            {
                "id": 2,
                "c1": {"c1_int": (15, 17), "c1_var": ("ciao", "adios")},
                "c2": {"c2_int": (19, None), "c2_var": ("bye", None)},
            },
        ]


class cmpxtst_row_of_udt_field_access(TstView):
    def __init__(self):
        # checked manually
        self.data = [
            {
                "id": 0,
                "c1_int": {"i1": 1, "i2": None},
                "c1_var": {"v1": "bye", "v2": None},
                "c2_int": None,
                "c2_var": {"v1": "hi", "v2": "konichiwa"},
            },
            {
                "id": 1,
                "c1_int": {"i1": 12, "i2": 18},
                "c1_var": None,
                "c2_int": None,
                "c2_var": None,
            },
            {
                "id": 2,
                "c1_int": {"i1": 15, "i2": 17},
                "c1_var": {"v1": "ciao", "v2": "adios"},
                "c2_int": {"i1": 19, "i2": None},
                "c2_var": {"v1": "bye", "v2": None},
            },
        ]
        self.sql = """CREATE MATERIALIZED VIEW row_of_udt_field_access AS SELECT
                      id,
                      row_of_udt_tbl.c1.c1_int,
                      row_of_udt_tbl.c1.c1_var,
                      row_of_udt_tbl.c2.c2_int,
                      row_of_udt_tbl.c2.c2_var
                      FROM row_of_udt_tbl"""


class cmpxtst_row_of_udt_elmnt_access(TstView):
    def __init__(self):
        # checked manually
        self.data = [
            {"id": 0, "v1": "bye", "i1": 1},
            {"id": 1, "v1": None, "i1": 12},
            {"id": 2, "v1": "ciao", "i1": 15},
        ]
        self.sql = """CREATE MATERIALIZED VIEW row_of_udt_elmnt_access AS SELECT
                      id,
                      row_of_udt_tbl.c1.c1_var.v1,
                      row_of_udt_tbl.c1.c1_int.i1
                      FROM row_of_udt_tbl"""


class cmpxtst_row_of_udt_elmnt_nexist(TstView):
    def __init__(self):
        # checked manually
        self.data = [
            {"id": 0, "c22_val2": None},
            {"id": 1, "c22_val2": None},
            {"id": 2, "c22_val2": None},
        ]
        self.sql = """CREATE MATERIALIZED VIEW row_of_udt_elmnt_nexist AS SELECT
                      id,
                      row_of_udt_tbl.c2.c2_int.i2 AS c22_val2
                      FROM row_of_udt_tbl"""
