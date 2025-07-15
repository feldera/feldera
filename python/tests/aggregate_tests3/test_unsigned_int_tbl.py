from tests.aggregate_tests.aggtst_base import TstTable


class aggtst_unsigned_int_table(TstTable):
    def __init__(self):
        self.sql = """CREATE TABLE un_int_tbl(
                      id INT NOT NULL,
                      c1 TINYINT UNSIGNED,
                      c2 TINYINT UNSIGNED NOT NULL,
                      c3 SMALLINT UNSIGNED,
                      c4 SMALLINT UNSIGNED NOT NULL,
                      c5 INT UNSIGNED,
                      c6 INT UNSIGNED NOT NULL,
                      c7 BIGINT UNSIGNED,
                      c8 BIGINT UNSIGNED NOT NULL)"""

        self.data = [
            {
                "id": 0,
                "c1": 72,
                "c2": 64,
                "c3": None,
                "c4": 16002,
                "c5": 781245123,
                "c6": 651238977,
                "c7": None,
                "c8": 284792878783,
            },
            {
                "id": 1,
                "c1": 61,
                "c2": 64,
                "c3": 14257,
                "c4": 15342,
                "c5": 963218731,
                "c6": 749321014,
                "c7": 367192837461,
                "c8": 265928374652,
            },
            {
                "id": 0,
                "c1": None,
                "c2": 45,
                "c3": 13450,
                "c4": 14123,
                "c5": 812347981,
                "c6": 698123417,
                "c7": 419283746512,
                "c8": 283746512983,
            },
            {
                "id": 1,
                "c1": None,
                "c2": 48,
                "c3": 12876,
                "c4": 13532,
                "c5": 709123456,
                "c6": 786452310,
                "c7": None,
                "c8": 274839201928,
            },
        ]
