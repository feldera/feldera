from tests.runtime_aggtest.aggtst_base import TstView
from decimal import Decimal


class aggtst_decimal_arg_max_value(TstView):
    def __init__(self):
        # checked manually
        self.data = [{"c1": Decimal("5681.08"), "c2": Decimal("7689.88")}]
        self.sql = """CREATE MATERIALIZED VIEW decimal_arg_max AS SELECT
                      ARG_MAX(c1, c2) AS c1,
                      ARG_MAX(c2, c1) AS c2
                      FROM decimal_tbl"""


class aggtst_decimal_arg_max_gby(TstView):
    def __init__(self):
        # checked manually
        self.data = [
            {"id": 0, "c1": None, "c2": Decimal("2231.90")},
            {"id": 1, "c1": Decimal("5681.08"), "c2": Decimal("7689.88")},
        ]
        self.sql = """CREATE MATERIALIZED VIEW decimal_arg_max_gby AS SELECT
                      id,
                      ARG_MAX(c1, c2) AS c1,
                      ARG_MAX(c2, c1) AS c2
                      FROM decimal_tbl
                      GROUP BY id"""


class aggtst_decimal_arg_max_distinct(TstView):
    def __init__(self):
        # checked manually
        self.data = [{"c1": Decimal("5681.08"), "c2": Decimal("7689.88")}]
        self.sql = """CREATE MATERIALIZED VIEW decimal_arg_max_distinct AS SELECT
                      ARG_MAX(DISTINCT c1, c2) AS c1,
                      ARG_MAX(DISTINCT c2, c1) AS c2
                      FROM decimal_tbl"""


class aggtst_decimal_arg_max_distinct_gby(TstView):
    def __init__(self):
        # checked manually
        self.data = [
            {"id": 0, "c1": None, "c2": Decimal("2231.90")},
            {"id": 1, "c1": Decimal("5681.08"), "c2": Decimal("7689.88")},
        ]
        self.sql = """CREATE MATERIALIZED VIEW decimal_arg_max_distinct_gby AS SELECT
                      id,
                      ARG_MAX(DISTINCT c1, c2) AS c1,
                      ARG_MAX(DISTINCT c2, c1) AS c2
                      FROM decimal_tbl
                      GROUP BY id"""


class aggtst_decimal_arg_max_where(TstView):
    def __init__(self):
        # checked manually
        self.data = [{"c1": Decimal("5681.08"), "c2": Decimal("7335.88")}]
        self.sql = """CREATE MATERIALIZED VIEW decimal_arg_max_where AS SELECT
                      ARG_MAX(c1, c2) FILTER(WHERE c2!= 7689.88) AS c1,
                      ARG_MAX(c2, c1) FILTER(WHERE c2!= 7689.88) AS c2
                      FROM decimal_tbl"""


class aggtst_decimal_arg_max_where_gby(TstView):
    def __init__(self):
        # checked manually
        self.data = [
            {"id": 0, "c1": None, "c2": Decimal("2231.90")},
            {"id": 1, "c1": Decimal("5681.08"), "c2": Decimal("7335.88")},
        ]
        self.sql = """CREATE MATERIALIZED VIEW decimal_arg_max_where_gby AS SELECT
                      id,
                      ARG_MAX(c1, c2) FILTER(WHERE c2!= 7689.88) AS c1,
                      ARG_MAX(c2, c1) FILTER(WHERE c2!= 7689.88) AS c2
                      FROM decimal_tbl
                      GROUP BY id"""
