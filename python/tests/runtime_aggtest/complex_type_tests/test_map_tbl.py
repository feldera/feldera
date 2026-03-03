from tests.runtime_aggtest.aggtst_base import TstTable


class cmpxtst_map_var_cmpx_tbl(TstTable):
    """Define the table used by the MAP tests that use MAP with scalar keys and complex values"""

    def __init__(self):
        self.sql = """CREATE TYPE user_def AS (i1 INT NOT NULL, v1 VARCHAR NULL);
                      CREATE TYPE user_def1 AS (i1 INT NULL, v1 VARCHAR NOT NULL);
                      CREATE TABLE map_var_cmpx_tbl(
                      id INT,
                      arr MAP<VARCHAR, VARCHAR ARRAY> NULL,
                      arr1 MAP<VARCHAR, VARCHAR ARRAY> NOT NULL,
                      mapp MAP<VARCHAR, MAP<VARCHAR, INT>> NULL,
                      mapp1 MAP<VARCHAR, MAP<VARCHAR, INT>> NOT NULL,
                      roww MAP<VARCHAR, ROW(i1 INT NOT NULL, v1 VARCHAR NULL)> NULL,
                      roww1 MAP<VARCHAR, ROW(i1 INT NULL, v1 VARCHAR NOT NULL)> NOT NULL,
                      udt MAP<VARCHAR, user_def> NULL,
                      udt1 MAP<VARCHAR, user_def1> NOT NULL)"""
        self.data = [
            {
                "id": 0,
                "arr": {"a": ["hello", "bye"], "b": ["see you"]},
                "arr1": {"a": ["hi"]},
                "mapp": {"a": {"a": 12}, "b": {"b": 17}},
                "mapp1": {"x": {"y": 1}},
                "roww": {"a": {"i1": 4, "v1": "cat"}, "b": {"i1": 7, "v1": "dog"}},
                "roww1": {"a": {"i1": None, "v1": "ok"}},
                "udt": {"a": {"i1": 4, "v1": "cat"}},
                "udt1": {"a": {"i1": None, "v1": "dog"}},
            },
            {
                "id": 1,
                "arr": {"a": [None, "bye"], "b": None},
                "arr1": {"a": []},
                "mapp": {"a": None, "b": {"b": None}},
                "mapp1": {"a": {"b": None}},
                "roww": {"a": None, "b": {"i1": 5, "v1": None}},
                "roww1": {"a": {"i1": None, "v1": "x"}},
                "udt": {"a": None, "b": {"i1": 3, "v1": None}},
                "udt1": {"a": {"i1": None, "v1": "y"}},
            },
            {
                "id": 2,
                "arr": {"a": ["x", None]},
                "arr1": {"a": ["z"]},
                "mapp": {"a": {"x": 1}},
                "mapp1": {"a": {"x": None}},
                "roww": {"a": {"i1": 1, "v1": None}},
                "roww1": {"a": {"i1": None, "v1": "valid"}},
                "udt": {"a": {"i1": 9, "v1": None}},
                "udt1": {"a": {"i1": None, "v1": "valid"}},
            },
            {
                "id": 3,
                "arr": None,
                "mapp": None,
                "roww": None,
                "udt": None,
                "arr1": {},
                "mapp1": {},
                "roww1": {},
                "udt1": {},
            },
        ]
