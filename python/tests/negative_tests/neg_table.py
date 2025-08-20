from tests.aggregate_tests.aggtst_base import TstTable


class neg_numeric_tbl(TstTable):
    """Define the table used by negative numeric tests"""

    def __init__(self):
        self.sql = """CREATE TABLE numeric_tbl(
                      id INT,
                      tiny_int TINYINT,
                      small_int SMALLINT,
                      intt INTEGER,
                      big_int BIGINT
                      )"""
        self.data = [
            {
                "id": 0,
                "tiny_int": 120,
                "small_int": 32750,
                "intt": 2147483647,
                "big_int": 8123302036854775807,
            },
            {
                "id": 1,
                "tiny_int": 100,
                "small_int": 32700,
                "intt": 2147483000,
                "big_int": 9223372036854775807,
            },
        ]


class neg_numeric_unsigned_tbl(TstTable):
    """Define the table used by negative integer unsigned tests"""

    def __init__(self):
        self.sql = """CREATE TABLE numeric_un_tbl(
                      id INT,
                      tiny_int TINYINT UNSIGNED,
                      small_int SMALLINT UNSIGNED,
                      intt INTEGER UNSIGNED,
                      big_int BIGINT UNSIGNED
                      )"""
        self.data = [
            {
                "id": 0,
                "tiny_int": 250,
                "small_int": 65430,
                "intt": 4294966290,
                "big_int": 12446742073709541615,
            },
            {
                "id": 1,
                "tiny_int": 200,
                "small_int": 65435,
                "intt": 4292966290,
                "big_int": 18446742073709541615,
            },
        ]
