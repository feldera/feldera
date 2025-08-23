from tests.runtime_aggtest.aggtst_base import TstTable


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


class aggtst_unsigned_int_table1(TstTable):
    """Table used by STDDEV aggregate to avoid overflow caused by input"""

    def __init__(self):
        self.sql = """CREATE TABLE un_int_tbl1(
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
                "c1": 12,
                "c2": 14,
                "c3": None,
                "c4": 1200,
                "c5": 123456,
                "c6": 234567,
                "c7": None,
                "c8": 1234567,
            },
            {
                "id": 1,
                "c1": 11,
                "c2": 13,
                "c3": 1100,
                "c4": 1300,
                "c5": 223456,
                "c6": 334567,
                "c7": 1123456,
                "c8": 2123456,
            },
            {
                "id": 0,
                "c1": 10,
                "c2": 12,
                "c3": 1000,
                "c4": 1250,
                "c5": 323456,
                "c6": 434567,
                "c7": 2123456,
                "c8": 3123456,
            },
            {
                "id": 1,
                "c1": 13,
                "c2": 15,
                "c3": 1150,
                "c4": 1350,
                "c5": 423456,
                "c6": 534567,
                "c7": None,
                "c8": 4123456,
            },
        ]
