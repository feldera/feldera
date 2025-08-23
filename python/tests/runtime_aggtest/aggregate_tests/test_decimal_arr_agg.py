from tests.runtime_aggtest.aggtst_base import TstView
from decimal import Decimal


class aggtst_decimal_array_agg_value(TstView):
    def __init__(self):
        # Validated on Postgres
        self.data = [
            {
                "c1": [
                    None,
                    Decimal("1111.52"),
                    Decimal("5681.08"),
                    Decimal("5681.08"),
                ],
                "c2": [
                    Decimal("3802.71"),
                    Decimal("2231.90"),
                    Decimal("7335.88"),
                    Decimal("7689.88"),
                ],
            }
        ]
        self.sql = """CREATE MATERIALIZED VIEW decimal_array_agg AS SELECT
                      ARRAY_AGG(c1) AS c1, ARRAY_AGG(c2) AS c2
                      FROM decimal_tbl"""


class aggtst_decimal_array_agg_gby(TstView):
    def __init__(self):
        # Validated on Postgres
        self.data = [
            {
                "id": 0,
                "c1": [None, Decimal("1111.52")],
                "c2": [Decimal("3802.71"), Decimal("2231.90")],
            },
            {
                "id": 1,
                "c1": [Decimal("5681.08"), Decimal("5681.08")],
                "c2": [Decimal("7335.88"), Decimal("7689.88")],
            },
        ]
        self.sql = """CREATE MATERIALIZED VIEW decimal_array_agg_gby AS SELECT
                      id, ARRAY_AGG(c1) AS c1, ARRAY_AGG(c2) AS c2
                      FROM decimal_tbl
                      GROUP BY id"""


class aggtst_decimal_array_agg_distinct(TstView):
    def __init__(self):
        # Validated on Postgres
        self.data = [
            {
                "c1": [None, Decimal("1111.52"), Decimal("5681.08")],
                "c2": [
                    Decimal("2231.90"),
                    Decimal("3802.71"),
                    Decimal("7335.88"),
                    Decimal("7689.88"),
                ],
            }
        ]
        self.sql = """CREATE MATERIALIZED VIEW decimal_array_agg_distinct AS SELECT
                      ARRAY_AGG(DISTINCT c1) AS c1, ARRAY_AGG(DISTINCT c2) AS c2
                      FROM decimal_tbl"""


class aggtst_decimal_array_agg_distinct_gby(TstView):
    def __init__(self):
        # Validated on Postgres
        self.data = [
            {
                "id": 0,
                "c1": [None, Decimal("1111.52")],
                "c2": [Decimal("2231.90"), Decimal("3802.71")],
            },
            {
                "id": 1,
                "c1": [Decimal("5681.08")],
                "c2": [Decimal("7335.88"), Decimal("7689.88")],
            },
        ]
        self.sql = """CREATE MATERIALIZED VIEW decimal_array_agg_distinct_gby AS SELECT
                      id, ARRAY_AGG(DISTINCT c1) AS c1, ARRAY_AGG(DISTINCT c2) AS c2
                      FROM decimal_tbl
                      GROUP BY id"""


class aggtst_decimal_array_agg_where(TstView):
    def __init__(self):
        # Validated on Postgres
        self.data = [
            {
                "f_c1": [None, Decimal("5681.08"), Decimal("5681.08")],
                "f_c2": [Decimal("3802.71"), Decimal("7335.88"), Decimal("7689.88")],
            }
        ]
        self.sql = """CREATE MATERIALIZED VIEW decimal_array_where AS SELECT
                      ARRAY_AGG(c1) FILTER(WHERE c2 > 2231.90) AS f_c1, ARRAY_AGG(c2) FILTER(WHERE c2 > 2231.90) AS f_c2
                      FROM decimal_tbl"""


class aggtst_decimal_array_agg_where_gby(TstView):
    def __init__(self):
        # Validated on Postgres
        self.data = [
            {"id": 0, "f_c1": [None], "f_c2": [Decimal("3802.71")]},
            {
                "id": 1,
                "f_c1": [Decimal("5681.08"), Decimal("5681.08")],
                "f_c2": [Decimal("7335.88"), Decimal("7689.88")],
            },
        ]
        self.sql = """CREATE MATERIALIZED VIEW decimal_array_where_gby AS SELECT
                      id, ARRAY_AGG(c1) FILTER(WHERE c2 > 2231.90) AS f_c1, ARRAY_AGG(c2) FILTER(WHERE c2 > 2231.90) AS f_c2
                      FROM decimal_tbl
                      GROUP BY id"""
