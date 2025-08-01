from .aggtst_base import TstView
from decimal import Decimal


class aggtst_decimal_arg_min_value(TstView):
    def __init__(self):
        # checked manually
        self.data = [{"c1": Decimal("1111.52"), "c2": Decimal("2231.90")}]
        self.sql = """CREATE MATERIALIZED VIEW decimal_arg_min AS SELECT
                      ARG_MIN(c1, c2) AS c1,
                      ARG_MIN(c2, c1) AS c2
                      FROM decimal_tbl"""


class aggtst_decimal_arg_min_gby(TstView):
    def __init__(self):
        # checked manually
        self.data = [
            {"id": 0, "c1": Decimal("1111.52"), "c2": Decimal("2231.90")},
            {"id": 1, "c1": Decimal("5681.08"), "c2": Decimal("7335.88")},
        ]
        self.sql = """CREATE MATERIALIZED VIEW decimal_arg_min_gby AS SELECT
                      id,
                      ARG_MIN(c1, c2) AS c1,
                      ARG_MIN(c2, c1) AS c2
                      FROM decimal_tbl
                      GROUP BY id"""


class aggtst_decimal_arg_min_distinct(TstView):
    def __init__(self):
        # checked manually
        self.data = [{"c1": Decimal("1111.52"), "c2": Decimal("2231.90")}]
        self.sql = """CREATE MATERIALIZED VIEW decimal_arg_min_distinct AS SELECT
                      ARG_MIN(DISTINCT c1, c2) AS c1,
                      ARG_MIN(DISTINCT c2, c1) AS c2
                      FROM decimal_tbl"""


class aggtst_decimal_arg_min_distinct_gby(TstView):
    def __init__(self):
        # checked manually
        self.data = [
            {"id": 0, "c1": Decimal("1111.52"), "c2": Decimal("2231.90")},
            {"id": 1, "c1": Decimal("5681.08"), "c2": Decimal("7335.88")},
        ]
        self.sql = """CREATE MATERIALIZED VIEW decimal_arg_min_distinct_gby AS SELECT
                      id,
                      ARG_MIN(DISTINCT c1, c2) AS c1,
                      ARG_MIN(DISTINCT c2, c1) AS c2
                      FROM decimal_tbl
                      GROUP BY id"""


class aggtst_decimal_arg_min_where(TstView):
    def __init__(self):
        # checked manually
        self.data = [{"c1": None, "c2": Decimal("7335.88")}]
        self.sql = """CREATE MATERIALIZED VIEW decimal_arg_min_where AS SELECT
                      ARG_MIN(c1, c2) FILTER(WHERE c2!= 2231.90) AS c1,
                      ARG_MIN(c2, c1) FILTER(WHERE c2!= 2231.90) AS c2
                      FROM decimal_tbl"""


class aggtst_decimal_arg_min_where_gby(TstView):
    def __init__(self):
        # checked manually
        self.data = [
            {"id": 0, "c1": None, "c2": Decimal("3802.71")},
            {"id": 1, "c1": Decimal("5681.08"), "c2": Decimal("7335.88")},
        ]
        self.sql = """CREATE MATERIALIZED VIEW decimal_arg_min_where_gby AS SELECT
                      id,
                      ARG_MIN(c1, c2) FILTER(WHERE c2!= 2231.90) AS c1,
                      ARG_MIN(c2, c1) FILTER(WHERE c2!= 2231.90) AS c2
                      FROM decimal_tbl
                      GROUP BY id"""
