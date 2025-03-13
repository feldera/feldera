from tests.aggregate_tests.aggtst_base import TstView, TstTable


# Depth 2 tests => Row of User Defined Type
class cmpxtst_row_of_udt_tbl(TstTable):
    """Define the table used by the ROW of User Defined Type tests"""

    def __init__(self):
        self.sql = """CREATE TABLE row_of_udt_tbl(
                      id INT,
                      c1 ROW(c1_int int_type NOT NULL, c1_var var_type) NOT NULL,
                      c2 ROW(c2_int int_type, c2_var var_type NOT NULL))"""
        self.data = [
            {
                "id": 0,
                "c1": {"c1_int": (1, 2), "c1_var": ("bye", None)},
                "c2": {"c2_int": (5, None), "c2_var": ("hi", "konichiwa")},
            },
            {
                "id": 1,
                "c1": {"c1_int": (12, 18), "c1_var": ("ciao", "adios")},
                "c2": None,
            },
        ]


class cmpxtst_row_of_udt_field_access(TstView):
    def __init__(self):
        # checked manually
        self.data = [
            {
                "id": 0,
                "c1_int": {"i1": 1, "i2": 2},
                "c1_var": {"v1": "bye", "v2": None},
                "c2_int": {"i1": 5, "i2": None},
                "c2_var": {"v1": "hi", "v2": "konichiwa"},
            },
            {
                "id": 1,
                "c1_int": {"i1": 12, "i2": 18},
                "c1_var": {"v1": "ciao", "v2": "adios"},
                "c2_int": None,
                "c2_var": None,
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
            {"id": 0, "v2": None, "i1": 5},
            {"id": 1, "v2": "adios", "i1": None},
        ]
        self.sql = """CREATE MATERIALIZED VIEW row_of_udt_elmnt_access AS SELECT
                      id,
                      row_of_udt_tbl.c1.c1_var.v2,
                      row_of_udt_tbl.c2.c2_int.i1
                      FROM row_of_udt_tbl"""


class cmpxtst_row_of_udt_elmnt_nexist(TstView):
    def __init__(self):
        # checked manually
        self.data = [{"id": 0, "c12_val": None}]
        self.sql = """CREATE MATERIALIZED VIEW row_of_udt_elmnt_nexist AS SELECT
                      id,
                      row_of_udt_tbl.c1.c1_var.v2 AS c12_val
                      FROM row_of_udt_tbl
                      WHERE id = 0"""
