from tests.aggregate_tests.aggtst_base import TstView
from decimal import Decimal


class aggtst_decimal_max_value(TstView):
    def __init__(self):
        # Validated on Postgres
        self.data = [{"c1": Decimal("5681.08"), "c2": Decimal("7689.88")}]
        self.sql = """CREATE MATERIALIZED VIEW decimal_max AS SELECT
                      MAX(c1) AS c1,
                      MAX(c2) AS c2
                      FROM decimal_tbl"""


class aggtst_decimal_max_gby(TstView):
    def __init__(self):
        # Validated on Postgres
        self.data = [
            {"id": 0, "c1": Decimal("1111.52"), "c2": Decimal("3802.71")},
            {"id": 1, "c1": Decimal("5681.08"), "c2": Decimal("7689.88")},
        ]
        self.sql = """CREATE MATERIALIZED VIEW decimal_max_gby AS SELECT
                      id,
                      MAX(c1) AS c1,
                      MAX(c2) AS c2
                      FROM decimal_tbl
                      GROUP BY id"""


class aggtst_decimal_max_distinct(TstView):
    def __init__(self):
        # Validated on Postgres
        self.data = [{"c1": Decimal("5681.08"), "c2": Decimal("7689.88")}]
        self.sql = """CREATE MATERIALIZED VIEW decimal_max_distinct AS SELECT
                      MAX(DISTINCT c1) AS c1,
                      MAX(DISTINCT c2) AS c2
                      FROM decimal_tbl"""


class aggtst_decimal_max_distinct_gby(TstView):
    def __init__(self):
        # Validated on Postgres
        self.data = [
            {"id": 0, "c1": Decimal("1111.52"), "c2": Decimal("3802.71")},
            {"id": 1, "c1": Decimal("5681.08"), "c2": Decimal("7689.88")},
        ]
        self.sql = """CREATE MATERIALIZED VIEW decimal_max_distinct_gby AS SELECT
                      id,
                      MAX(DISTINCT c1) AS c1,
                      MAX(DISTINCT c2) AS c2
                      FROM decimal_tbl
                      GROUP BY id"""


class aggtst_decimal_max_where(TstView):
    def __init__(self):
        # Validated on Postgres
        self.data = [{"f_c1": Decimal("5681.08"), "f_c2": Decimal("7335.88")}]
        self.sql = """CREATE MATERIALIZED VIEW decimal_max_where AS SELECT
                      MAX(c1) FILTER (WHERE c2!= 7689.88) AS f_c1,
                      MAX(c2) FILTER (WHERE c2!= 7689.88) AS f_c2
                      FROM decimal_tbl"""


class aggtst_decimal_max_where_gby(TstView):
    def __init__(self):
        # Validated on Postgres
        self.data = [
            {"id": 0, "f_c1": Decimal("1111.52"), "f_c2": Decimal("3802.71")},
            {"id": 1, "f_c1": Decimal("5681.08"), "f_c2": Decimal("7335.88")},
        ]
        self.sql = """CREATE MATERIALIZED VIEW decimal_max_where_gby AS SELECT
                      id,
                      MAX(c1) FILTER (WHERE c2!= 7689.88) AS f_c1,
                      MAX(c2) FILTER (WHERE c2!= 7689.88) AS f_c2
                      FROM decimal_tbl
                      GROUP BY id"""
