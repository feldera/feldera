from tests.runtime_aggtest.aggtst_base import TstTable


class asof_table1(TstTable):
    """Define the table used by some integer tests"""

    def __init__(self):
        self.sql = """CREATE TABLE asof_tbl1(
                      id INT, intt INT, str VARCHAR, decimall DECIMAL(6, 2), reall REAL, dbl DOUBLE, booll BOOL, bin BINARY,
                      tmestmp TIMESTAMP,
                      datee DATE,
                      tme TIME)"""
        self.data = [
            {
                "id": 1,
                "intt": 10,
                "str": "apple",
                "decimall": -1111.52,
                "reall": -57681.18,
                "dbl": -38.2711234601246,
                "booll": False,
                "bin": [10, 22, 32],
                "tme": "13:23:44.456",
                "tmestmp": "2000-06-21 14:23:44.123",
                "datee": "2000-06-21",
            },
            {
                "id": 2,
                "intt": 15,
                "str": "cat",
                "decimall": -0.52,
                "reall": 2.56,
                "dbl": -0.82711234601246,
                "booll": True,
                "bin": [16, 23, 44],
                "tme": "19:23:44.456",
                "tmestmp": "2019-06-21 14:23:44.123",
                "datee": "2019-06-21",
            },
            {
                "id": 3,
                "intt": 20,
                "str": "dog",
                "decimall": -123.45,
                "reall": 0.5,
                "dbl": 0.125,
                "booll": False,
                "bin": [17, 23, 44],
                "tme": "01:23:44.456",
                "tmestmp": "1978-06-21 14:23:44.123",
                "datee": "1978-06-21",
            },
            {
                "id": 4,
                "intt": 25,
                "str": "firefly",
                "decimall": 0.00,
                "reall": 0.0,
                "dbl": 0.0,
                "booll": True,
                "bin": [22, 23, 44],
                "tme": "23:23:44.456",
                "tmestmp": "2002-06-21 14:23:44.123",
                "datee": "2002-06-21",
            },
            {
                "id": 5,
                "intt": None,
                "str": None,
                "decimall": None,
                "reall": None,
                "dbl": None,
                "booll": None,
                "bin": None,
                "tme": None,
                "tmestmp": None,
                "datee": None,
            },
        ]


class asof_table2(TstTable):
    """Define the table used by some integer tests"""

    def __init__(self):
        self.sql = """CREATE TABLE asof_tbl2(
                      id INT, intt INT, str VARCHAR, decimall DECIMAL(6, 2), reall REAL, dbl DOUBLE, booll BOOL, bin BINARY,
                      tmestmp TIMESTAMP,
                      datee DATE,
                      tme TIME)"""
        self.data = [
            {
                "id": 1,
                "intt": 5,
                "str": "bye",
                "decimall": 10.10,
                "reall": 10.001,
                "dbl": 10.000001,
                "booll": True,
                "bin": [11, 22, 32],
                "tme": "14:23:44.456",
                "tmestmp": "2020-06-21 14:23:44.123",
                "datee": "2020-06-21",
            },
            {
                "id": 2,
                "intt": 16,
                "str": "hi",
                "decimall": -256.25,
                "reall": -0.1234567,
                "dbl": -0.00256,
                "booll": False,
                "bin": [15, 55, 22],
                "tme": "20:23:44.456",
                "tmestmp": "2021-06-21 14:23:44.123",
                "datee": "2021-06-21",
            },
            {
                "id": 3,
                "intt": 70,
                "str": "ciao",
                "decimall": 64.32,
                "reall": -987,
                "dbl": -999.9999999,
                "booll": True,
                "bin": [12, 16, 55],
                "tme": "00:23:44.456",
                "tmestmp": "1977-06-21 14:23:44.123",
                "datee": "1977-06-21",
            },
            {
                "id": 4,
                "intt": 12,
                "str": "c you!",
                "decimall": 0.01,
                "reall": 1.618,
                "dbl": 3.14159265358979,
                "booll": False,
                "bin": [44, 88, 99],
                "tme": "22:23:44.456",
                "tmestmp": "2001-06-21 14:23:44.123",
                "datee": "2001-06-21",
            },
            {
                "id": 5,
                "intt": None,
                "str": None,
                "decimall": None,
                "reall": None,
                "dbl": None,
                "booll": None,
                "bin": None,
                "tme": None,
                "tmestmp": None,
                "datee": None,
            },
        ]
