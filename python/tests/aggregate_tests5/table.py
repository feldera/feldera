from tests.aggregate_tests.aggtst_base import TstTable, TstView


class aggtst_int0_table(TstTable):
    def __init__(self):
        self.sql = """CREATE TABLE int0_tbl(
                      id INT NOT NULL,
                      c1 TINYINT,
                      c2 TINYINT NOT NULL,
                      c3 INT2,
                      c4 INT2 NOT NULL,
                      c5 INT,
                      c6 INT NOT NULL,
                      c7 BIGINT,
                      c8 BIGINT NOT NULL)WITH ('append_only' = 'true')"""

        self.data = [
            {
                "id": 0,
                "c1": 5,
                "c2": 2,
                "c3": None,
                "c4": 4,
                "c5": 5,
                "c6": 6,
                "c7": None,
                "c8": 8,
            },
            {
                "id": 1,
                "c1": 4,
                "c2": 3,
                "c3": 4,
                "c4": 6,
                "c5": 2,
                "c6": 3,
                "c7": 4,
                "c8": 2,
            },
            {
                "id": 0,
                "c1": None,
                "c2": 2,
                "c3": 3,
                "c4": 2,
                "c5": 3,
                "c6": 4,
                "c7": 3,
                "c8": 3,
            },
            {
                "id": 1,
                "c1": None,
                "c2": 5,
                "c3": 6,
                "c4": 2,
                "c5": 2,
                "c6": 1,
                "c7": None,
                "c8": 5,
            },
        ]


class aggtst_decimal_table(TstTable):
    """Define the table used by all decimal tests"""

    def __init__(self):
        self.sql = """CREATE TABLE decimal_tbl(
                      id INT, c1 DECIMAL(6,2), c2 DECIMAL(6,2) NOT NULL
                      )WITH ('append_only' = 'true')"""
        self.data = [
            {"id": 0, "c1": 1111.52, "c2": 2231.90},
            {"id": 0, "c1": None, "c2": 3802.71},
            {"id": 1, "c1": 5681.08, "c2": 7689.88},
            {"id": 1, "c1": 5681.08, "c2": 7335.88},
        ]


class aggtst_row_tbl(TstTable):
    """Define the table used by the ROW tests"""

    def __init__(self):
        self.sql = """CREATE TABLE row_tbl(
                      id INT,
                      c1 INT NOT NULL,
                      c2 VARCHAR,
                      c3 VARCHAR)WITH ('append_only' = 'true')"""
        self.data = [
            {"id": 0, "c1": 4, "c2": None, "c3": "adios"},
            {"id": 0, "c1": 3, "c2": "ola", "c3": "ciao"},
            {"id": 1, "c1": 7, "c2": "hi", "c3": "hiya"},
            {"id": 1, "c1": 2, "c2": "elo", "c3": "ciao"},
            {"id": 1, "c1": 2, "c2": "elo", "c3": "ciao"},
        ]


class aggtst_varchar_table(TstTable):
    """Define the table used by the varchar tests"""

    def __init__(self):
        self.sql = """CREATE TABLE varchar_tbl(
                      id INT,
                      c1 VARCHAR,
                      c2 VARCHAR NULL)WITH ('append_only' = 'true')"""
        self.data = [
            {"id": 0, "c1": None, "c2": "abc   d"},
            {"id": 0, "c1": "hello", "c2": "fred"},
            {"id": 1, "c1": "@abc", "c2": "variable-length"},
            {"id": 1, "c1": "hello", "c2": "exampl e"},
        ]


class aggtst_atbl_varcharn(TstView):
    def __init__(self):
        # Validated on Postgres
        self.data = [
            {"id": 0, "f_c1": None, "f_c2": "abc  "},
            {"id": 0, "f_c1": "hello", "f_c2": "fred"},
            {"id": 1, "f_c1": "@abc", "f_c2": "varia"},
            {"id": 1, "f_c1": "hello", "f_c2": "examp"},
        ]
        self.sql = """CREATE MATERIALIZED VIEW atbl_varcharn AS SELECT
                      id,
                      CAST(c1 AS VARCHAR(5)) AS f_c1,
                      CAST(c2 AS VARCHAR(5)) AS f_c2
                      FROM varchar_tbl"""


class aggtst_atbl_charn(TstView):
    def __init__(self):
        # Validated on Postgres
        self.data = [
            {"id": 0, "f_c1": None, "f_c2": "abc   d"},
            {"id": 0, "f_c1": "hello  ", "f_c2": "fred   "},
            {"id": 1, "f_c1": "@abc   ", "f_c2": "variabl"},
            {"id": 1, "f_c1": "hello  ", "f_c2": "exampl "},
        ]
        self.sql = """CREATE MATERIALIZED VIEW atbl_charn AS SELECT
                      id,
                      CAST(c1 AS CHAR(7)) AS f_c1,
                      CAST(c2 AS CHAR(7)) AS f_c2
                      FROM varchar_tbl"""


class aggtst_date_table(TstTable):
    """Define the table used by DATE tests"""

    def __init__(self):
        self.sql = """CREATE TABLE date_tbl(
                      id INT,
                      c1 DATE NOT NULL,
                      c2 DATE)WITH ('append_only' = 'true')"""
        self.data = [
            {"id": 0, "c1": "2014-11-05", "c2": "2024-12-05"},
            {"id": 0, "c1": "2020-06-21", "c2": None},
            {"id": 1, "c1": "2024-12-05", "c2": "2014-11-05"},
            {"id": 1, "c1": "2020-06-21", "c2": "2023-02-26"},
            {"id": 1, "c1": "1969-03-17", "c2": "2015-09-07"},
        ]


class aggtst_time_table(TstTable):
    """Define the table used by the time tests"""

    def __init__(self):
        self.sql = """CREATE TABLE time_tbl(
                      id INT,
                      c1 TIME NOT NULL,
                      c2 TIME)WITH ('append_only' = 'true')"""
        self.data = [
            {"id": 0, "c1": "08:30:00", "c2": "12:45:00"},
            {"id": 0, "c1": "14:00:00", "c2": None},
            {"id": 1, "c1": "09:15:00", "c2": "16:30:00"},
            {"id": 1, "c1": "14:00:00", "c2": "18:00:00"},
        ]


class aggtst_timestamp_table(TstTable):
    """Define the table used by the timestamp tests"""

    def __init__(self):
        self.sql = """CREATE TABLE timestamp_tbl(
                      id INT,
                      c1 TIMESTAMP NOT NULL,
                      c2 TIMESTAMP)WITH ('append_only' = 'true')"""
        self.data = [
            {"id": 0, "c1": "2014-11-05 08:27:00", "c2": "2024-12-05 12:45:00"},
            {"id": 0, "c1": "2020-06-21 14:00:00", "c2": None},
            {"id": 1, "c1": "2024-12-05 09:15:00", "c2": "2014-11-05 16:30:00"},
            {"id": 1, "c1": "2020-06-21 14:00:00", "c2": "2023-02-26 18:00:00"},
        ]


class aggtst_timestamp1_table(TstTable):
    """Define the table used by some of the timestamp tests"""

    def __init__(self):
        self.sql = """CREATE TABLE timestamp1_tbl(
                      id INT,
                      c1 TIMESTAMP NOT NULL,
                      c2 TIMESTAMP)WITH ('append_only' = 'true')"""
        self.data = [
            {"id": 0, "c1": "2014-11-05 08:27:00", "c2": "2024-12-05 12:45:00"},
            {"id": 0, "c1": "2020-06-21 14:00:00", "c2": None},
            {"id": 1, "c1": "2024-12-05 09:15:00", "c2": "2014-11-05 16:30:00"},
            {"id": 1, "c1": "2020-06-21 14:00:00", "c2": "2023-02-26 18:00:00"},
            {"id": 1, "c1": "1969-03-17 11:32:00", "c2": "2015-09-07 18:57:00"},
        ]
