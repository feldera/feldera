## Contains tables definition for tables containing columns with data types supported only in Feldera

from tests.aggregate_tests.aggtst_base import TstTable, TstView


class orderby_tbl_manual_binary_ts(TstTable):
    """Define the table used by the order by/limit tests with Binary and Timestamp values"""

    def __init__(self):
        self.sql = """CREATE TABLE orderby_tbl_manual_binary_ts(
                      c1 BINARY(4),
                      c2 TIMESTAMP)"""
        self.data = [
            {"c1": [12, 22, 32], "c2": None},
            {"c1": [12, 22, 32], "c2": "1987-06-05 06:43:00"},
            {"c1": None, "c2": "2020-06-21 14:00:00"},
            {"c1": [23, 56, 33, 21], "c2": "2014-11-05 08:27:00"},
            {"c1": [23, 56, 33, 21], "c2": "2020-06-21 14:00:00"},
            {"c1": [55, 66, 77, 88], "c2": "2024-12-05 12:45:00"},
            {"c1": [99, 20, 31, 77], "c2": None},
            {"c1": [49, 43, 84, 29], "c2": "2014-11-15 23:45:00"},
            {"c1": [32, 34, 22, 12], "c2": "2014-11-05 08:27:00"},
            {"c1": None, "c2": "1965-12-11 17:22:00"},
            {"c1": [10, 12, 28, 14], "c2": "2007-12-15 20:20:00"},
        ]


class orderby_binary_ts_v(TstView):
    def __init__(self):
        self.data = [
            {"c1": "0a0c1c0e", "c2": "2007-12-15T20:20:00"},
            {"c1": "0c1620", "c2": "1987-06-05T06:43:00"},
            {"c1": "0c1620", "c2": None},
            {"c1": "17382115", "c2": "2020-06-21T14:00:00"},
            {"c1": "17382115", "c2": "2014-11-05T08:27:00"},
            {"c1": "2022160c", "c2": "2014-11-05T08:27:00"},
            {"c1": "312b541d", "c2": "2014-11-15T23:45:00"},
            {"c1": "37424d58", "c2": "2024-12-05T12:45:00"},
            {"c1": "63141f4d", "c2": None},
            {"c1": None, "c2": "1965-12-11T17:22:00"},
            {"c1": None, "c2": "2020-06-21T14:00:00"},
        ]
        self.sql = """CREATE MATERIALIZED VIEW orderby_binary_ts_v AS
                       SELECT *
                       FROM orderby_tbl_manual_binary_ts"""


class orderby_tbl_manual_arr_time(TstTable):
    """Define the table used by the order by/limit tests with Array and Time values"""

    def __init__(self):
        self.sql = """CREATE TABLE orderby_tbl_manual_arr_time(
                      c1 INT ARRAY,
                      c2 TIME)"""
        self.data = [
            {"c1": [12, 22, 32], "c2": "06:43:00"},
            {"c1": [12, 22, 32], "c2": "06:43:00"},
            {"c1": [12, 22, 32], "c2": "06:43:00"},
            {"c1": None, "c2": "08:27:00"},
            {"c1": [11, 56, 33, 21], "c2": "14:00:00"},
            {"c1": [55, 66, 77, 88], "c2": "12:45:00"},
            {"c1": [99, 20, 31, 77], "c2": None},
            {"c1": [49, 43, 84, 29], "c2": "23:45:00"},
            {"c1": [32, 34, 22, 12], "c2": None},
            {"c1": None, "c2": "17:22:00"},
            {"c1": [13, 12, 28, 14], "c2": "20:20:00"},
        ]
