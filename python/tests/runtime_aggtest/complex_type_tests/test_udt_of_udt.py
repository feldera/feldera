from tests.runtime_aggtest.aggtst_base import TstView, TstTable


# Depth 2 tests => User Defined Type of User Defined Type
class cmpxtst_udt_of_udt_tbl(TstTable):
    """Define the table used by the user defined type of user defined type tests"""

    def __init__(self):
        self.sql = """CREATE TYPE udef_type AS(i1_type int_type NOT NULL, v1_type var_type);
                      CREATE TYPE udef_type2 AS(v2_type var_type NOT NULL, i2_type int_type);
                      CREATE TABLE udt_of_udt_tbl(
                      id INT,
                      c1 udef_type,
                      c2 udef_type2 NOT NULL)"""
        self.data = [
            {
                "id": 0,
                "c1": {"i1_type": {"i1": 1, "i2": None}, "v1_type": None},
                "c2": {"v2_type": {"v1": "hi", "v2": None}, "i2_type": None},
            },
            {
                "id": 1,
                "c1": {
                    "i1_type": {"i1": 5, "i2": 6},
                    "v1_type": {"v1": "hi", "v2": None},
                },
                "c2": {
                    "v2_type": {"v1": "hello", "v2": "how are you?"},
                    "i2_type": {"i1": 7, "i2": None},
                },
            },
        ]


class cmpxtst_udt_of_udt_field_access(TstView):
    def __init__(self):
        # checked manually
        self.data = [
            {
                "id": 0,
                "c1i1_type": {"i1": 1, "i2": None},
                "c1v1_type": None,
                "c2v2_type": {"v1": "hi", "v2": None},
                "c2i2_type": None,
            },
            {
                "id": 1,
                "c1i1_type": {"i1": 5, "i2": 6},
                "c1v1_type": {"v1": "hi", "v2": None},
                "c2v2_type": {"v1": "hello", "v2": "how are you?"},
                "c2i2_type": {"i1": 7, "i2": None},
            },
        ]
        self.sql = """CREATE MATERIALIZED VIEW udt_of_udt_field_access AS SELECT
                      id,
                      udt_of_udt_tbl.c1.i1_type AS c1i1_type,
                      udt_of_udt_tbl.c1.v1_type AS c1v1_type,
                      udt_of_udt_tbl.c2.v2_type AS c2v2_type,
                      udt_of_udt_tbl.c2.i2_type AS c2i2_type
                      FROM udt_of_udt_tbl"""


class cmpxtst_udt_of_udt_element_access(TstView):
    def __init__(self):
        # checked manually
        self.data = [
            {"id": 0, "c1i1_type_i2_elmnt": None, "c2v2_type_v2_elmnt": None},
            {"id": 1, "c1i1_type_i2_elmnt": 6, "c2v2_type_v2_elmnt": "how are you?"},
        ]
        self.sql = """CREATE MATERIALIZED VIEW udt_of_udt_element_access AS SELECT
                      id,
                      udt_of_udt_tbl.c1.i1_type.i2 AS c1i1_type_i2_elmnt,
                      udt_of_udt_tbl.c2.v2_type.v2 AS c2v2_type_v2_elmnt
                      FROM udt_of_udt_tbl"""


class cmpxtst_udt_of_udt_elmnt_nexist(TstView):
    def __init__(self):
        # checked manually
        self.data = [{"id": 0, "c1v1_type": None, "c1i1_type_i2_elmnt": None}]
        self.sql = """CREATE MATERIALIZED VIEW udt_of_udt_elmnt_nexist AS SELECT
                      id,
                      udt_of_udt_tbl.c1.v1_type AS c1v1_type,
                      udt_of_udt_tbl.c1.i1_type.i2 AS c1i1_type_i2_elmnt
                      FROM udt_of_udt_tbl
                      WHERE id = 0"""
