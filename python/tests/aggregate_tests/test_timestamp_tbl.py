from .aggtst_base import TstTable


class aggtst_timestamp_table(TstTable):
    """Define the table used by the timestamp tests"""

    def __init__(self):
        self.sql = """CREATE TABLE timestamp_tbl(
                      id INT,
                      c1 TIMESTAMP NOT NULL,
                      c2 TIMESTAMP)"""
        self.data = [
            {"id": 0, "c1": "2014-11-05 08:27:00", "c2": "2024-12-05 12:45:00"},
            {"id": 0, "c1": "2020-06-21 14:00:00", "c2": None},
            {"id": 1, "c1": "2024-12-05 09:15:00", "c2": "2014-11-05 16:30:00"},
            {"id": 1, "c1": "2020-06-21 14:00:00", "c2": "2023-02-26 18:00:00"},
        ]
