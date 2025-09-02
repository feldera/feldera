from tests.runtime_aggtest.aggtst_base import TstTable


class neg_numeric_tbl(TstTable):
    """Define the table used by negative numeric tests"""

    def __init__(self):
        self.sql = """CREATE TABLE numeric_tbl(
                      id INT,
                      tiny_int TINYINT,
                      tiny_int2 TINYINT,
                      small_int SMALLINT,
                      small_int2 SMALLINT,
                      intt INTEGER,
                      intt2 INTEGER,
                      big_int BIGINT,
                      big_int2 BIGINT
                      )"""
        self.data = [
            {
                "id": 0,
                "tiny_int": 120,
                "tiny_int2": 100,
                "small_int": 32750,
                "small_int2": 32700,
                "intt": 2147483647,
                "intt2": 2147483000,
                "big_int": 8123302036854775807,
                "big_int2": 9223372036854775807,
            },
            {
                "id": 1,
                "tiny_int": -128,
                "tiny_int2": -1,
                "small_int": -32768,
                "small_int2": -1,
                "intt": -2147483648,
                "intt2": -1,
                "big_int": -9223372036854775808,
                "big_int2": -1,
            },
        ]


class neg_numeric_tbl1(TstTable):
    """Define the table used by negative numeric tests"""

    def __init__(self):
        self.sql = """CREATE TABLE numeric_tbl1(
                      id INT,
                      tiny_int TINYINT,
                      small_int SMALLINT,
                      intt INTEGER,
                      big_int BIGINT
                      )"""
        self.data = [
            {
                "id": 0,
                "tiny_int": 127,
                "small_int": 32767,
                "intt": 2147483647,
                "big_int": 9223372036854775807,
            },
            {"id": 1, "tiny_int": 1, "small_int": 1, "intt": 1, "big_int": 1},
        ]


class neg_numeric_unsigned_tbl(TstTable):
    """Define the table used by negative integer unsigned tests"""

    def __init__(self):
        self.sql = """CREATE TABLE numeric_un_tbl(
                      id INT,
                      tiny_int TINYINT UNSIGNED,
                      tiny_int2 TINYINT UNSIGNED,
                      small_int SMALLINT UNSIGNED,
                      small_int2 SMALLINT UNSIGNED,
                      intt INTEGER UNSIGNED,
                      intt2 INTEGER UNSIGNED,
                      big_int BIGINT UNSIGNED,
                      big_int2 BIGINT UNSIGNED
                      )"""
        self.data = [
            {
                "id": 0,
                "tiny_int": 250,
                "tiny_int2": 200,
                "small_int": 65430,
                "small_int2": 65435,
                "intt": 4294966290,
                "intt2": 4292966290,
                "big_int": 12446742073709541615,
                "big_int2": 18446742073709541615,
            },
        ]


class neg_numeric_unsigned_tbl1(TstTable):
    """Define the table used by negative integer unsigned tests"""

    def __init__(self):
        self.sql = """CREATE TABLE numeric_un_tbl1(
                      id INT,
                      tiny_int TINYINT UNSIGNED,
                      small_int SMALLINT UNSIGNED,
                      intt INTEGER UNSIGNED,
                      big_int BIGINT UNSIGNED
                      )"""
        self.data = [
            {
                "id": 0,
                "tiny_int": 255,
                "small_int": 65535,
                "intt": 4294967295,
                "big_int": 18446744073709551615,
            },
            {"id": 1, "tiny_int": 1, "small_int": 1, "intt": 1, "big_int": 1},
        ]
