from tests.runtime_aggtest.aggtst_base import TstTable, TstView


class aggtst_float_table(TstTable):
    """Define the table used by the REAL and DOUBLE tests"""

    def __init__(self):
        self.sql = """CREATE TABLE float_tbl(
                      id INT,
                      c1 REAL,
                      c2 REAL NOT NULL,
                      c3 DOUBLE PRECISION,
                      c4 DOUBLE PRECISION NOT NULL)"""

        self.data = [
            {
                "id": 0,
                "c1": None,
                "c2": 12345.6789012,
                "c3": 1.618033988749894,
                "c4": 9876543210.123456789012345,
            },
            {
                "id": 0,
                "c1": -76543.2109876,
                "c2": -0.0000123,
                "c3": -123456789.987654321012345,
                "c4": -9999999999.999999999999999,
            },
            {
                "id": 1,
                "c1": 3.1415926,
                "c2": 2.7182818,
                "c3": None,
                "c4": 0.577215664901532,
            },
            {
                "id": 1,
                "c1": -9.8765432,
                "c2": -8.7654321,
                "c3": -7.654321098765432,
                "c4": -6.543210987654321,
            },
        ]


class aggtst_float_view(TstView):
    """The result of SELECT * on float table"""

    def __init__(self):
        self.sql = """CREATE MATERIALIZED VIEW float_view AS SELECT
                      id, c1, c2, c3, c4
                      FROM float_tbl"""
        self.data = [
            {
                "id": 0,
                "c1": -76543.2109375,
                "c2": -1.2299999980314169e-05,
                "c3": -123456789.98765431,
                "c4": -10000000000.0,
            },
            {
                "id": 0,
                "c1": None,
                "c2": 12345.6787109375,
                "c3": 1.618033988749894,
                "c4": 9876543210.123455,
            },
            {
                "id": 1,
                "c1": -9.876543045043945,
                "c2": -8.765432357788086,
                "c3": -7.654321098765432,
                "c4": -6.543210987654321,
            },
            {
                "id": 1,
                "c1": 3.141592502593994,
                "c2": 2.7182817459106445,
                "c3": None,
                "c4": 0.577215664901532,
            },
        ]


class aggtst_float_decimal_table(TstTable):
    """Define the table used by the REAL and DOUBLE tests"""

    def __init__(self):
        self.sql = """CREATE TABLE float_decimal_tbl(
                      id INT,
                      c1 DECIMAL(18, 8),
                      c2 DECIMAL(18, 8),
                      c3 DECIMAL(38,18),
                      c4 DECIMAL(38,18))"""

        self.data = [
            {
                "id": 0,
                "c1": None,
                "c2": 12345.6789012,
                "c3": 1.618033988749894,
                "c4": 9876543210.123456789012345,
            },
            {
                "id": 0,
                "c1": -76543.2109876,
                "c2": -0.0000123,
                "c3": -123456789.987654321012345,
                "c4": -9999999999.999999999999999,
            },
            {
                "id": 1,
                "c1": 3.1415926,
                "c2": 2.7182818,
                "c3": None,
                "c4": 0.577215664901532,
            },
            {
                "id": 1,
                "c1": -9.8765432,
                "c2": -8.7654321,
                "c3": -7.654321098765432,
                "c4": -6.543210987654321,
            },
        ]
