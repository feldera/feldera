## Contains tables definition for tables containing columns with data types supported in Feldera AND SQLITE
from tests.runtime_aggtest.aggtst_base import TstTable


class orderby_tbl_sqlite_int_varchar(TstTable):
    """Define the table used by the order by/limit tests with INT and VARCHAR values"""

    def __init__(self):
        self.sql = """CREATE TABLE orderby_tbl_sqlite_int_varchar(
                      c1 INT,
                      c2 VARCHAR)"""
        self.data = [
            {"c1": 3, "c2": "hello"},
            {"c1": 2, "c2": "bye bye, friend!!"},
            {"c1": None, "c2": "see you later"},
            {"c1": 2, "c2": None},
            {"c1": 1, "c2": "ferris says ciao"},
            {"c1": 14, "c2": "meet you @ 5"},
            {"c1": 3, "c2": None},
            {"c1": 6, "c2": "see you"},
            {"c1": 1, "c2": "hello"},
            {"c1": None, "c2": "fred says konichiwa"},
            {"c1": 4, "c2": "see you"},
        ]


class orderby_tbl_sqlite_smallint_varchar(TstTable):
    """Define the table used by the order by/limit tests with SMALLINT and VARCHAR values"""

    def __init__(self):
        self.sql = """CREATE TABLE orderby_tbl_sqlite_smallint_varchar(
                      c1 SMALLINT,
                      c2 VARCHAR)"""
        self.data = [
            {"c1": 3, "c2": "hello"},
            {"c1": 2, "c2": "bye bye, friend!!"},
            {"c1": None, "c2": "see you later"},
            {"c1": 2, "c2": None},
            {"c1": 1, "c2": "ferris says ciao"},
            {"c1": 14, "c2": "meet you @ 5"},
            {"c1": 3, "c2": None},
            {"c1": 6, "c2": "see you"},
            {"c1": 1, "c2": "hello"},
            {"c1": None, "c2": "fred says konichiwa"},
            {"c1": 4, "c2": "see you"},
        ]


class orderby_tbl_sqlite_char_tinyint(TstTable):
    """Define the table used by the order by/limit tests with CHAR and TINYINT values"""

    def __init__(self):
        self.sql = """CREATE TABLE orderby_tbl_sqlite_char_tinyint(
                      c1 CHAR,
                      c2 TINYINT)"""
        self.data = [
            {"c1": "hello", "c2": 3},
            {"c1": "bye bye, friend!!", "c2": 2},
            {"c1": "see you later", "c2": None},
            {"c1": None, "c2": 2},
            {"c1": "ferris says ciao", "c2": 1},
            {"c1": "meet you @ 5", "c2": 14},
            {"c1": None, "c2": 3},
            {"c1": "see you", "c2": 6},
            {"c1": "hello", "c2": 1},
            {"c1": "fred says konichiwa", "c2": None},
            {"c1": "see you", "c2": 4},
        ]


class orderby_tbl_sqlite_decimal_char(TstTable):
    """Define the table used by the order by/limit tests with Decimal and CHAR values"""

    def __init__(self):
        self.sql = """CREATE TABLE orderby_tbl_sqlite_decimal_char(
                      c1 DECIMAL(6,2),
                      c2 CHAR)"""
        self.data = [
            {"c1": 1111.52, "c2": "hello"},
            {"c1": 4567.17, "c2": "bye bye, friend!!"},
            {"c1": None, "c2": "see you later"},
            {"c1": 4567.17, "c2": None},
            {"c1": 1111.52, "c2": "ferris says ciao"},
            {"c1": 1445.19, "c2": "meet you @ 5"},
            {"c1": 3902.44, "c2": None},
            {"c1": 9831.66, "c2": "see you"},
            {"c1": 1616.45, "c2": "hello"},
            {"c1": None, "c2": "fred says konichiwa"},
            {"c1": 4538.22, "c2": "see you"},
        ]


class orderby_tbl_sqlite_real_date(TstTable):
    """Define the table used by the order by/limit tests with REAL and DATE values"""

    def __init__(self):
        self.sql = """CREATE TABLE orderby_tbl_sqlite_real_date(
                      c1 REAL,
                      c2 DATE)"""
        self.data = [
            {"c1": 57681.18, "c2": "2014-11-05"},
            {"c1": 34561.22, "c2": "2014-11-05"},
            {"c1": None, "c2": "1999-01-25"},
            {"c1": 34561.22, "c2": None},
            {"c1": 11111.23, "c2": "1999-01-25"},
            {"c1": 12145.67, "c2": "2026-10-14"},
            {"c1": 57681.18, "c2": None},
            {"c1": 17879.12, "c2": "2009-08-19"},
            {"c1": -1234.34, "c2": "1965-04-21"},
            {"c1": None, "c2": "2018-01-12"},
            {"c1": 88890.23, "c2": "2011-11-18"},
        ]


class orderby_tbl_sqlite_double_date(TstTable):
    """Define the table used by the order by/limit tests with DOUBLE and DATE values"""

    def __init__(self):
        self.sql = """CREATE TABLE orderby_tbl_sqlite_double_date(
                      c1 DOUBLE,
                      c2 DATE)"""
        self.data = [
            {"c1": 57681.1800001234, "c2": "2014-11-05"},
            {"c1": 34561.2200006789, "c2": "2014-11-05"},
            {"c1": None, "c2": "1999-01-25"},
            {"c1": 34561.2200006789, "c2": None},
            {"c1": 11111.2300001111, "c2": "1999-01-25"},
            {"c1": 12145.6700002222, "c2": "2026-10-14"},
            {"c1": 57681.1800001234, "c2": None},
            {"c1": 17879.1200003333, "c2": "2009-08-19"},
            {"c1": -1234.3400004444, "c2": "1965-04-21"},
            {"c1": None, "c2": "2018-01-12"},
            {"c1": 88890.2300005555, "c2": "2011-11-18"},
        ]
