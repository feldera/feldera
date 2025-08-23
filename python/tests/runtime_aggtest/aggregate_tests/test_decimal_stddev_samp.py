from tests.runtime_aggtest.aggtst_base import TstView
from decimal import Decimal


class aggtst_decimal_stddev(TstView):
    def __init__(self):
        # Validated on Postgres
        self.data = [{"c1": Decimal("2638.23"), "c2": Decimal("2677.47")}]
        self.sql = """CREATE MATERIALIZED VIEW decimal_stddev_samp AS SELECT
                      STDDEV_SAMP(c1) AS c1,
                      STDDEV_SAMP(c2) AS c2
                      FROM decimal_tbl"""


class aggtst_decimal_stddev_groupby(TstView):
    def __init__(self):
        # Validated on Postgres
        self.data = [
            {"id": 0, "c1": None, "c2": Decimal("1110.73")},
            {"id": 1, "c1": Decimal("0.00"), "c2": Decimal("250.31")},
        ]
        self.sql = """CREATE MATERIALIZED VIEW decimal_stddev_samp_gby AS SELECT
                      id,
                      STDDEV_SAMP(c1) AS c1,
                      STDDEV_SAMP(c2) AS c2
                      FROM decimal_tbl
                      GROUP BY id"""


class aggtst_decimal_stddev_distinct(TstView):
    def __init__(self):
        # Validated on Postgres
        self.data = [{"c1": Decimal("3231.16"), "c2": Decimal("2677.47")}]
        self.sql = """CREATE MATERIALIZED VIEW decimal_stddev_samp_distinct AS SELECT
                      STDDEV_SAMP(DISTINCT c1) AS c1,
                      STDDEV_SAMP(DISTINCT c2) AS c2
                      FROM decimal_tbl"""


class aggtst_decimal_stddev_distinct_groupby(TstView):
    def __init__(self):
        # Validated on Postgres
        self.data = [
            {"id": 0, "c1": None, "c2": Decimal("1110.73")},
            {"id": 1, "c1": None, "c2": Decimal("250.31")},
        ]
        self.sql = """CREATE MATERIALIZED VIEW decimal_stddev_samp_distinct_gby AS SELECT
                      id,
                      STDDEV_SAMP(DISTINCT c1) AS c1,
                      STDDEV_SAMP(DISTINCT c2) AS c2
                      FROM decimal_tbl
                      GROUP BY id"""


class aggtst_decimal_stddev_where(TstView):
    def __init__(self):
        # Validated on Postgres
        self.data = [{"c1": Decimal("0.00"), "c2": Decimal("2149.36")}]
        self.sql = """CREATE MATERIALIZED VIEW decimal_stddev_where AS SELECT
                      STDDEV_SAMP(c1) FILTER (WHERE c2 > 2231.90) AS c1,
                      STDDEV_SAMP(c2) FILTER (WHERE c2 > 2231.90) AS c2
                      FROM decimal_tbl"""


class aggtst_decimal_stddev_where_groupby(TstView):
    def __init__(self):
        # Validated on Postgres
        self.data = [
            {"id": 0, "c1": None, "c2": None},
            {"id": 1, "c1": Decimal("0.00"), "c2": Decimal("250.31")},
        ]
        self.sql = """CREATE MATERIALIZED VIEW decimal_stddev_where_gby AS SELECT
                      id,
                      STDDEV_SAMP(c1) FILTER (WHERE c2 > 2231.90) AS c1,
                      STDDEV_SAMP(c2) FILTER (WHERE c2 > 2231.90) AS c2
                      FROM decimal_tbl
                      GROUP BY id"""
