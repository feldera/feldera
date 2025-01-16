from .aggtst_base import TstTable


class aggtst_real_table(TstTable):
    """Define the table used by REAL tests"""

    def __init__(self):
        self.sql = """CREATE FUNCTION RF(R REAL) RETURNS REAL AS ROUND(R, 6);
                        CREATE TABLE real_tbl(
                        id INT,
                        c1 REAL,
                        c2 REAL NOT NULL);"""
        self.data = [
            {"id": 0, "c1": None, "c2": 2231.791},
            {"id": 0, "c1": 57681.18, "c2": -38.27112},
            {"id": 1, "c1": -1111.567, "c2": 71689.88},
            {"id": 1, "c1": 57681.18, "c2": 873315.8},
        ]
