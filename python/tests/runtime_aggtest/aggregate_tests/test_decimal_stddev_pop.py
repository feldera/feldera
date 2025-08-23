from tests.runtime_aggtest.aggtst_base import TstView
from decimal import Decimal


class aggtst_decimal_stddev_pop(TstView):
    def __init__(self):
        # Validated on Postgres
        self.data = [{"c1": Decimal("2154.11"), "c2": Decimal("2318.75")}]
        self.sql = """CREATE MATERIALIZED VIEW decimal_stddev_pop AS SELECT
                      STDDEV_POP(c1) AS c1,
                      STDDEV_POP(c2) AS c2
                      FROM decimal_tbl"""


class aggtst_decimal_stddev_pop_groupby(TstView):
    def __init__(self):
        # Validated on Postgres
        self.data = [
            {"id": 0, "c1": Decimal("0.00"), "c2": Decimal("785.40")},
            {"id": 1, "c1": Decimal("0.00"), "c2": Decimal("177.00")},
        ]
        self.sql = """CREATE MATERIALIZED VIEW decimal_stddev_pop_gby AS SELECT
                      id,
                      STDDEV_POP(c1) AS c1,
                      STDDEV_POP(c2) AS c2
                      FROM decimal_tbl
                      GROUP BY id"""


class aggtst_decimal_stddev_pop_distinct(TstView):
    def __init__(self):
        # Validated on Postgres
        self.data = [{"c1": Decimal("2284.78"), "c2": Decimal("2318.75")}]
        self.sql = """CREATE MATERIALIZED VIEW decimal_stddev_pop_distinct AS SELECT
                      STDDEV_POP(DISTINCT c1) AS c1,
                      STDDEV_POP(DISTINCT c2) AS c2
                      FROM decimal_tbl"""


class aggtst_decimal_stddev_pop_distinct_groupby(TstView):
    def __init__(self):
        # Validated on Postgres
        self.data = [
            {"id": 0, "c1": Decimal("0.00"), "c2": Decimal("785.40")},
            {"id": 1, "c1": Decimal("0.00"), "c2": Decimal("177.00")},
        ]
        self.sql = """CREATE MATERIALIZED VIEW decimal_stddev_pop_distinct_gby AS SELECT
                      id,
                      STDDEV_POP(DISTINCT c1) AS c1,
                      STDDEV_POP(DISTINCT c2) AS c2
                      FROM decimal_tbl
                      GROUP BY id"""


class aggtst_decimal_stddev_pop_where(TstView):
    def __init__(self):
        # Validated on Postgres
        self.data = [{"c1": Decimal("0.00"), "c2": Decimal("1754.95")}]
        self.sql = """CREATE MATERIALIZED VIEW decimal_stddev_pop_where AS SELECT
                      STDDEV_POP(c1) FILTER (WHERE c2 > 2231.90) AS c1,
                      STDDEV_POP(c2) FILTER (WHERE c2 > 2231.90) AS c2
                      FROM decimal_tbl"""


class aggtst_decimal_stddev_pop_where_groupby(TstView):
    def __init__(self):
        # Validated on Postgres
        self.data = [
            {"id": 0, "c1": None, "c2": Decimal("0.00")},
            {"id": 1, "c1": Decimal("0.00"), "c2": Decimal("177.00")},
        ]
        self.sql = """CREATE MATERIALIZED VIEW decimal_stddev_pop_where_gby AS SELECT
                      id,
                      STDDEV_POP(c1) FILTER (WHERE c2 > 2231.90) AS c1,
                      STDDEV_POP(c2) FILTER (WHERE c2 > 2231.90) AS c2
                      FROM decimal_tbl
                      GROUP BY id"""
