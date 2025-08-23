from tests.runtime_aggtest.aggtst_base import TstView, TstTable


class cmpxtst_row_of_row_tbl(TstTable):
    """Define the table used by the ROW of ROW tests"""

    def __init__(self):
        self.sql = """CREATE TABLE row_of_row_tbl(
                      id INT,
                      c1 ROW(c11 ROW(i11 INT NOT NULL, i12 INT NULL) NOT NULL, c12 ROW(v11 VARCHAR NOT NULL, v12 VARCHAR NULL) NULL) NOT NULL,
                      c2 ROW(c21 ROW(i21 INT NULL, i22 INT NOT NULL) NULL, c22 ROW(v21 VARCHAR NULL, v22 VARCHAR NOT NULL) NULL)
                      )"""
        self.data = [
            {
                "id": 0,
                "c1": {
                    "c11": {"i11": 12, "i12": None},
                    "c12": {"v11": "ola!!", "v12": None},
                },
                "c2": None,
            },
            {
                "id": 1,
                "c1": {
                    "c11": {"i11": 20, "i12": None},
                    "c12": {"v11": "hi", "v12": None},
                },
                "c2": {"c21": None, "c22": None},
            },
            {
                "id": 2,
                "c1": {"c11": {"i11": 19, "i12": 14}, "c12": None},
                "c2": {
                    "c21": {"i21": 14, "i22": 17},
                    "c22": {"v21": None, "v22": "hiya!"},
                },
            },
        ]


class cmpxtst_row_of_row_field_access(TstView):
    def __init__(self):
        # checked manually
        self.data = [
            {
                "id": 0,
                "c11": {"i11": 12, "i12": None},
                "c12": {"v11": "ola!!", "v12": None},
                "c21": None,
                "c22": None,
            },
            {
                "id": 1,
                "c11": {"i11": 20, "i12": None},
                "c12": {"v11": "hi", "v12": None},
                "c21": None,
                "c22": None,
            },
            {
                "id": 2,
                "c11": {"i11": 19, "i12": 14},
                "c12": None,
                "c21": {"i21": 14, "i22": 17},
                "c22": {"v21": None, "v22": "hiya!"},
            },
        ]
        self.sql = """CREATE MATERIALIZED VIEW row_of_row_field_access AS SELECT
                      id,
                      row_of_row_tbl.c1.c11 AS c11,
                      row_of_row_tbl.c1.c12 AS c12,
                      row_of_row_tbl.c2.c21 AS c21,
                      row_of_row_tbl.c2.c22 AS c22
                      FROM row_of_row_tbl"""


class cmpxtst_row_of_row_element_access(TstView):
    def __init__(self):
        # checked manually
        self.data = [
            {"id": 0, "c11_i11": 12, "c12_v11": "ola!!"},
            {"id": 1, "c11_i11": 20, "c12_v11": "hi"},
            {"id": 2, "c11_i11": 19, "c12_v11": None},
        ]
        self.sql = """CREATE MATERIALIZED VIEW row_of_row_element_access AS SELECT
                      id,
                      row_of_row_tbl.c1.c11.i11 AS c11_i11,
                      row_of_row_tbl.c1.c12.v11 AS c12_v11
                      FROM row_of_row_tbl"""


class cmpxtst_arow_of_row_elmnt_nexist(TstView):
    def __init__(self):
        # checked manually
        self.data = [
            {"id": 0, "c21_i11": None},
            {"id": 1, "c21_i11": None},
            {"id": 2, "c21_i11": None},
        ]
        self.sql = """CREATE MATERIALIZED VIEW row_of_row_elmnt_nexist AS SELECT
                      id,
                      row_of_row_tbl.c2.c22.v21 AS c21_i11
                      FROM row_of_row_tbl"""
