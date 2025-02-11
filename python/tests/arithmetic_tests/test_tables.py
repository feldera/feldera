from tests.aggregate_tests.aggtst_base import TstTable


# Left to add dates before the Gregorian calendar
class arithtst_date_table(TstTable):
    """Define the table used by DATE tests"""

    def __init__(self):
        self.sql = """CREATE TABLE date_tbl(
                      id INT,
                      c1 DATE NOT NULL,
                      c2 DATE)"""
        self.data = [
            {"id": 0, "c1": "2024-12-05", "c2": "2014-11-05"},
            {"id": 1, "c1": "2020-06-21", "c2": "2023-02-26"},
            {"id": 2, "c1": "1969-06-21", "c2": "1948-12-02"},
        ]


class arithtst_timestamp_table(TstTable):
    """Define the table used by TIMESTAMP tests"""

    def __init__(self):
        self.sql = """CREATE TABLE timestamp_tbl(
                      id INT,
                      c1 TIMESTAMP NOT NULL,
                      c2 TIMESTAMP)"""
        self.data = [
            {"id": 0, "c1": "2019-12-05 08:27:00", "c2": "2014-11-05 12:45:00"},
            {"id": 1, "c1": "2020-06-21 14:00:00", "c2": "2023-02-26 18:00:00"},
            {"id": 2, "c1": "1959-06-21 11:32:00", "c2": "1948-12-02 09:15:00"},
        ]


class arithtst_time_table(TstTable):
    """Define the table used by TIME tests"""

    def __init__(self):
        self.sql = """CREATE TABLE time_tbl(
                      id INT,
                      c1 TIME NOT NULL,
                      c2 TIME)"""
        self.data = [
            {"id": 0, "c1": "18:30:45", "c2": "12:45:12"},
            {"id": 1, "c1": "08:23:55", "c2": "14:17:09"},
        ]
