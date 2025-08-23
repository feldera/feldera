from tests.runtime_aggtest.aggtst_base import TstTable


class aggtst_row_tbl(TstTable):
    """Define the table used by the ROW tests"""

    def __init__(self):
        self.sql = """CREATE TABLE row_tbl(
                      id INT,
                      c1 INT NOT NULL,
                      c2 VARCHAR,
                      c3 VARCHAR)"""
        self.data = [
            {"id": 0, "c1": 4, "c2": None, "c3": "adios"},
            {"id": 0, "c1": 3, "c2": "ola", "c3": "ciao"},
            {"id": 1, "c1": 7, "c2": "hi", "c3": "hiya"},
            {"id": 1, "c1": 2, "c2": "elo", "c3": "ciao"},
            {"id": 1, "c1": 2, "c2": "elo", "c3": "ciao"},
        ]
