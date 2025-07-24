from tests.aggregate_tests.aggtst_base import TstTable


class aggtst_array_tbl(TstTable):
    """Define the table used by the array tests"""

    def __init__(self):
        self.sql = """CREATE TABLE array_tbl(
                      id INT,
                      c1 INT ARRAY NOT NULL,
                      c2 INT ARRAY,
                      c3 MAP<VARCHAR, INT> ARRAY)"""
        self.data = [
            {"id": 0, "c1": [12, 22], "c2": None, "c3": [{"a": 5, "b": 66}]},
            {"id": 0, "c1": [23, 56, 16], "c2": [55, 66, None], "c3": [{"c": 2}]},
            {"id": 1, "c1": [23, 56, 16], "c2": [99], "c3": None},
            {"id": 1, "c1": [49], "c2": [32, 34, 22, 12], "c3": [{"x": 1}]},
        ]
