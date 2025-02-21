from tests.aggregate_tests.aggtst_base import TstView, TstTable


class cmpxtst_row_int_var_tbl(TstTable):
    """Define the table used by the ROW of int and varchar tests"""

    def __init__(self):
        self.sql = """CREATE TABLE row_int_var_tbl(
                      id INT,
                      c1 ROW(i1 INT NOT NULL, i2 INT NULL) NOT NULL,
                      c2 ROW(i1 INT NOT NULL, i2 INT NULL),
                      c3 ROW(v1 VARCHAR NOT NULL, v2 VARCHAR NULL) NOT NULL,
                      c4 ROW(v1 VARCHAR NOT NULL, v2 VARCHAR NULL)
                      )"""
        self.data = [
            {"id": 0, "c1": {"i1": 20, "i2": None}, "c2": {"i1": 22, "i2": 44}, "c3": {"v1": "hi", "v2": "bye"}, "c4": {"v1": "hello", "v2": None}},
            {"id": 1, "c1": {"i1": 10, "i2": None}, "c2": {"i1": 33, "i2": None}, "c3": {"v1": "bye", "v2": "adios"}, "c4": {"v1": "ciao", "v2": None}}
        ]


class cmpxtst_row_field_access(TstView):
    def __init__(self):
        # checked manually
        self.data = [
            {'id': 0, 'i1': 20, 'i2': None, 'i10': 22, 'i20': 44, 'v1': 'hi', 'v2': 'bye', 'v10': 'hello', 'v20': None},
            {'id': 1, 'i1': 10, 'i2': None, 'i10': 33, 'i20': None, 'v1': 'bye', 'v2': 'adios', 'v10': 'ciao', 'v20': None}
        ]
        self.sql = """CREATE MATERIALIZED VIEW row_field_access AS SELECT
                      id,
                      row_int_var_tbl.c1.i1,
                      row_int_var_tbl.c1.i2,
                      row_int_var_tbl.c2.i1,
                      row_int_var_tbl.c2.i2,
                      row_int_var_tbl.c3.v1,
                      row_int_var_tbl.c3.v2,
                      row_int_var_tbl.c4.v1,
                      row_int_var_tbl.c4.v2
                      FROM row_int_var_tbl"""
