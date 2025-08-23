from tests.runtime_aggtest.aggtst_base import TstView
from decimal import Decimal


class aggtst_decimal_avg(TstView):
    def __init__(self):
        # Validated on Postgres
        self.sql = """CREATE MATERIALIZED VIEW decimal_avg AS
                      SELECT AVG(c1) AS c1, AVG(c2) AS c2
                      FROM decimal_tbl"""
        self.data = [{"c1": Decimal("4157.89"), "c2": Decimal("5265.09")}]


class aggtst_decimal_avg_gby(TstView):
    def __init__(self):
        # Validated on Postgres
        self.sql = """CREATE MATERIALIZED VIEW decimal_avg_gby AS SELECT
                      id, AVG(c1) AS c1, AVG(c2) AS c2
                      FROM decimal_tbl
                      GROUP BY id"""
        self.data = [
            {"id": 0, "c1": Decimal("1111.52"), "c2": Decimal("3017.30")},
            {"id": 1, "c1": Decimal("5681.08"), "c2": Decimal("7512.88")},
        ]


class aggtst_decimal_avg_distinct(TstView):
    def __init__(self):
        # Validated on Postgres
        self.sql = """CREATE MATERIALIZED VIEW decimal_avg_distinct AS SELECT
                      AVG(DISTINCT c1) AS c1, AVG(DISTINCT c2) AS c2
                      FROM decimal_tbl"""
        self.data = [{"c1": Decimal("3396.30"), "c2": Decimal("5265.09")}]


class aggtst_decimal_avg_distinct_gby(TstView):
    def __init__(self):
        # Validated on Postgres
        self.sql = """CREATE MATERIALIZED VIEW decimal_avg_distinct_gby AS SELECT
                      id, AVG(DISTINCT c1) AS c1, AVG(DISTINCT c2) AS c2
                      FROM decimal_tbl
                      GROUP BY id"""
        self.data = [
            {"id": 0, "c1": Decimal("1111.52"), "c2": Decimal("3017.30")},
            {"id": 1, "c1": Decimal("5681.08"), "c2": Decimal("7512.88")},
        ]


class aggtst_decimal_avg_where(TstView):
    def __init__(self):
        # Validated on Postgres
        self.sql = """CREATE MATERIALIZED VIEW decimal_avg_where AS SELECT
                      AVG(c1) FILTER(WHERE c2>2231.90) AS f_c1, AVG(c2) FILTER(WHERE c2>2231.90) AS f_c2
                      FROM decimal_tbl"""
        self.data = [{"f_c1": Decimal("5681.08"), "f_c2": Decimal("6276.15")}]


class aggtst_decimal_avg_where_gby(TstView):
    def __init__(self):
        # Validated on Postgres
        self.sql = """CREATE MATERIALIZED VIEW decimal_avg_where_gby AS SELECT
                      id, AVG(c1) FILTER(WHERE c2>2231.90) AS f_c1, AVG(c2) FILTER(WHERE c2>2231.90) AS f_c2
                      FROM decimal_tbl
                      GROUP BY id"""
        self.data = [
            {"id": 0, "f_c1": None, "f_c2": Decimal("3802.71")},
            {"id": 1, "f_c1": Decimal("5681.08"), "f_c2": Decimal("7512.88")},
        ]
