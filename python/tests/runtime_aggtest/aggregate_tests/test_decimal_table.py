from tests.runtime_aggtest.aggtst_base import TstTable


class aggtst_decimal_table(TstTable):
    """Define the table used by all decimal tests"""

    def __init__(self):
        self.sql = """CREATE TABLE decimal_tbl(
                      id INT, c1 DECIMAL(6,2), c2 DECIMAL(6,2) NOT NULL
                      )"""
        self.data = [
            {"id": 0, "c1": 1111.52, "c2": 2231.90},
            {"id": 0, "c1": None, "c2": 3802.71},
            {"id": 1, "c1": 5681.08, "c2": 7689.88},
            {"id": 1, "c1": 5681.08, "c2": 7335.88},
        ]
