from tests.aggregate_tests.aggtst_base import TstTable


class aggtst_varchar_table(TstTable):
    """Define the table used by the varchar tests"""

    def __init__(self):
        self.sql = """CREATE TABLE varchar_tbl(
                      id INT,
                      c1 VARCHAR,
                      c2 VARCHAR NULL)"""
        self.data = [
            {"id": 0, "c1": None, "c2": "abc   d"},
            {"id": 0, "c1": "hello", "c2": "fred"},
            {"id": 1, "c1": "@abc", "c2": "variable-length"},
            {"id": 1, "c1": "hello", "c2": "exampl e"},
        ]
