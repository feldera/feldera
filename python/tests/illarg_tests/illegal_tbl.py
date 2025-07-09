from tests.aggregate_tests.aggtst_base import TstTable
from decimal import Decimal


class illarg_tbl(TstTable):
    """Define the table used by illegal argument tests"""

    def __init__(self):
        self.sql = """CREATE TABLE illegal_tbl(
                      id INT,
                      intt INT,
                      decimall DECIMAL(6, 2),
                      reall REAL,
                      dbl DOUBLE,
                      booll BOOL,
                      str VARCHAR,
                      bin BINARY,
                      tmestmp TIMESTAMP,
                      uuidd UUID,
                      arr VARCHAR ARRAY
                      )"""
        self.data = [
            {
                "id": 0,
                "intt": -12,
                "decimall": -1111.52,
                "reall": -57681.18,
                "dbl": -38.2711234601246,
                "booll": True,
                "str": "hello ",
                "bin": [11, 22, 32],
                "tmestmp": "2020-06-21 14:23:44",
                "uuidd": "42b8fec7-c7a3-4531-9611-4bde80f9cb4c",
                "arr": ["bye", "14", "See you!", "-0.52"],
            },
            {
                "id": 1,
                "intt": -1,
                "decimall": -0.52,
                "reall": -0.1234567,
                "dbl": -0.82711234601246,
                "booll": False,
                "str": "0.12",
                "bin": [11, 22, 32],
                "tmestmp": "2020-06-21 14:23:44",
                "uuidd": "42b8fec7-c7a3-4531-9611-4bde80f9cb4c",
                "arr": ["-0.14", "friends", "See you!"],
            },
            {
                "id": 2,
                "intt": None,
                "decimall": Decimal("Infinity"),
                "reall": -2.4434967,
                "dbl": None,
                "booll": None,
                "str": None,
                "bin": None,
                "tmestmp": None,
                "uuidd": None,
                "arr": [None],
            },
            {
                "id": 3,
                "intt": None,
                "decimall": Decimal("NaN"),
                "reall": None,
                "dbl": None,
                "booll": None,
                "str": None,
                "bin": None,
                "tmestmp": None,
                "uuidd": None,
                "arr": [None],
            },
        ]
