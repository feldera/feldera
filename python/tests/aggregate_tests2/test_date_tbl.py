from tests.aggregate_tests.aggtst_base import TstTable


class aggtst_date_table(TstTable):
    """Define the table used by DATE tests"""

    def __init__(self):
        self.sql = """CREATE TABLE date_tbl(
                      id INT,
                      c1 DATE NOT NULL,
                      c2 DATE)"""
        self.data = [
            {"id": 0, "c1": "2014-11-05", "c2": "2024-12-05"},
            {"id": 0, "c1": "2020-06-21", "c2": None},
            {"id": 1, "c1": "2024-12-05", "c2": "2014-11-05"},
            {"id": 1, "c1": "2020-06-21", "c2": "2023-02-26"},
            {"id": 1, "c1": "1969-03-17", "c2": "2015-09-07"},
        ]
