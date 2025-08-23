from tests.runtime_aggtest.aggtst_base import TstTable


class aggtst_map_tbl(TstTable):
    """Define the table used by the MAP tests"""

    def __init__(self):
        self.sql = """CREATE TABLE map_tbl(
                      id INT,
                      c1 MAP<VARCHAR, INT> NOT NULL,
                      c2 MAP<VARCHAR, INT>)"""
        self.data = [
            {"id": 0, "c1": {"a": 75, "b": 66}, "c2": None},
            {"id": 0, "c1": {"q": 11, "v": 66}, "c2": {"q": 22}},
            {"id": 1, "c1": {"x": 8, "y": 6}, "c2": {"i": 5, "j": 66}},
            {"id": 1, "c1": {"f": 45, "h": 66}, "c2": {"f": 1}},
            {"id": 1, "c1": {"q": 11, "v": 66}, "c2": {"q": 11, "v": 66, "x": None}},
        ]
