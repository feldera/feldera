from tests.aggregate_tests.aggtst_base import TstView
from decimal import Decimal


class aggtst_decimal_min_value(TstView):
    def __init__(self):
        # Validated on Postgres
        self.data = [{"c1": Decimal("1111.52"), "c2": Decimal("2231.90")}]
        self.sql = """CREATE MATERIALIZED VIEW decimal_min AS SELECT
                      MIN(c1) AS c1,
                      MIN(c2) AS c2
                      FROM decimal_tbl"""


class aggtst_decimal_min_gby(TstView):
    def __init__(self):
        # Validated on Postgres
        self.data = [
            {"id": 0, "c1": Decimal("1111.52"), "c2": Decimal("2231.90")},
            {"id": 1, "c1": Decimal("5681.08"), "c2": Decimal("7335.88")},
        ]
        self.sql = """CREATE MATERIALIZED VIEW decimal_min_gby AS SELECT
                      id,
                      MIN(c1) AS c1,
                      MIN(c2) AS c2
                      FROM decimal_tbl
                      GROUP BY id"""


class aggtst_decimal_min_distinct(TstView):
    def __init__(self):
        # Validated on Postgres
        self.data = [{"c1": Decimal("1111.52"), "c2": Decimal("2231.90")}]
        self.sql = """CREATE MATERIALIZED VIEW decimal_min_distinct AS SELECT
                      MIN(DISTINCT c1) AS c1,
                      MIN(DISTINCT c2) AS c2
                      FROM decimal_tbl"""


class aggtst_decimal_min_distinct_gby(TstView):
    def __init__(self):
        # Validated on Postgres
        self.data = [
            {"id": 0, "c1": Decimal("1111.52"), "c2": Decimal("2231.90")},
            {"id": 1, "c1": Decimal("5681.08"), "c2": Decimal("7335.88")},
        ]
        self.sql = """CREATE MATERIALIZED VIEW decimal_min_distinct_gby AS SELECT
                      id,
                      MIN(DISTINCT c1) AS c1,
                      MIN(DISTINCT c2) AS c2
                      FROM decimal_tbl
                      GROUP BY id"""


class aggtst_decimal_min_where(TstView):
    def __init__(self):
        # Validated on Postgres
        self.data = [{"f_c1": Decimal("5681.08"), "f_c2": Decimal("3802.71")}]
        self.sql = """CREATE MATERIALIZED VIEW decimal_min_where AS SELECT
                      MIN(c1) FILTER (WHERE c2!= 2231.90) AS f_c1,
                      MIN(c2) FILTER (WHERE c2!= 2231.90) AS f_c2
                      FROM decimal_tbl"""


class aggtst_decimal_min_where_gby(TstView):
    def __init__(self):
        # Validated on Postgres
        self.data = [
            {"id": 0, "f_c1": None, "f_c2": Decimal("3802.71")},
            {"id": 1, "f_c1": Decimal("5681.08"), "f_c2": Decimal("7335.88")},
        ]
        self.sql = """CREATE MATERIALIZED VIEW decimal_min_where_gby AS SELECT
                      id,
                      MIN(c1) FILTER (WHERE c2!= 2231.90) AS f_c1,
                      MIN(c2) FILTER (WHERE c2!= 2231.90) AS f_c2
                      FROM decimal_tbl
                      GROUP BY id"""
