from tests.runtime_aggtest.aggtst_base import TstTable


class aggtst_time_table(TstTable):
    """Define the table used by the time tests"""

    def __init__(self):
        self.sql = """CREATE TABLE time_tbl(
                      id INT,
                      c1 TIME NOT NULL,
                      c2 TIME)"""
        self.data = [
            {"id": 0, "c1": "08:30:00", "c2": "12:45:00"},
            {"id": 0, "c1": "14:00:00", "c2": None},
            {"id": 1, "c1": "09:15:00", "c2": "16:30:00"},
            {"id": 1, "c1": "14:00:00", "c2": "18:00:00"},
        ]
