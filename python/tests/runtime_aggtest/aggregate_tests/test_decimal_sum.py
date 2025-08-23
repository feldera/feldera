from tests.runtime_aggtest.aggtst_base import TstView
from decimal import Decimal


class aggtst_decimal_sum(TstView):
    def __init__(self):
        # Validated on Postgres
        self.sql = """CREATE MATERIALIZED VIEW decimal_sum AS SELECT
                      SUM(c1) AS c1, SUM(c2) AS c2
                      FROM decimal_tbl"""
        self.data = [{"c1": Decimal("12473.68"), "c2": Decimal("21060.37")}]


class aggtst_decimal_sum_gby(TstView):
    def __init__(self):
        # Validated on Postgres
        self.sql = """CREATE MATERIALIZED VIEW decimal_sum_gby AS SELECT
                      id, SUM(c1) AS c1, SUM(c2) AS c2
                      FROM decimal_tbl
                      GROUP BY id"""
        self.data = [
            {"id": 0, "c1": Decimal("1111.52"), "c2": Decimal("6034.61")},
            {"id": 1, "c1": Decimal("11362.16"), "c2": Decimal("15025.76")},
        ]


class aggtst_decimal_sum_distinct(TstView):
    def __init__(self):
        # Validated on Postgres
        self.sql = """CREATE MATERIALIZED VIEW decimal_sum_distinct AS SELECT
                      SUM(DISTINCT c1) AS c1, SUM(DISTINCT c2) AS c2
                      FROM decimal_tbl"""
        self.data = [{"c1": Decimal("6792.60"), "c2": Decimal("21060.37")}]


class aggtst_decimal_sum_distinct_gby(TstView):
    def __init__(self):
        # Validated on Postgres
        self.sql = """CREATE MATERIALIZED VIEW decimal_sum_distinct_gby AS SELECT
                      id, SUM(DISTINCT c1) AS c1, SUM(DISTINCT c2) AS c2
                      FROM decimal_tbl
                      GROUP BY id"""
        self.data = [
            {"id": 0, "c1": Decimal("1111.52"), "c2": Decimal("6034.61")},
            {"id": 1, "c1": Decimal("5681.08"), "c2": Decimal("15025.76")},
        ]


class aggtst_decimal_sum_where(TstView):
    def __init__(self):
        # Validated on Postgres
        self.sql = """CREATE MATERIALIZED VIEW decimal_sum_where AS SELECT
                           SUM(c1) FILTER(WHERE c2>2231.90) AS f_c1, SUM(c2) FILTER(WHERE c2>2231.90) AS f_c2
                        FROM decimal_tbl"""
        self.data = [{"f_c1": Decimal("11362.16"), "f_c2": Decimal("18828.47")}]


class aggtst_decimal_sum_where_gby(TstView):
    def __init__(self):
        # Validated on Postgres
        self.sql = """CREATE MATERIALIZED VIEW decimal_sum_where_gby AS SELECT
                      id, SUM(c1) FILTER(WHERE c2>2231.90) AS f_c1, SUM(c2) FILTER(WHERE c2>2231.90) AS f_c2
                      FROM decimal_tbl
                      GROUP BY id"""
        self.data = [
            {"id": 0, "f_c1": None, "f_c2": Decimal("3802.71")},
            {"id": 1, "f_c1": Decimal("11362.16"), "f_c2": Decimal("15025.76")},
        ]
