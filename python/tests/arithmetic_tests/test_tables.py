from tests.aggregate_tests.aggtst_base import TstTable


class arithtst_date_table(TstTable):
    """Define the table used by DATE tests"""

    def __init__(self):
        self.sql = """CREATE FUNCTION d()
                      RETURNS DATE NOT NULL AS
                      CAST('1970-01-01' AS DATE);
                      
                      CREATE TABLE date_tbl(
                      id INT,
                      c1 DATE NOT NULL,
                      c2 DATE)"""
        self.data = [
            {"id": 0, "c1": "2024-12-05", "c2": "2014-11-05"},
            {"id": 1, "c1": "2020-06-21", "c2": "2023-02-26"},
        ]
