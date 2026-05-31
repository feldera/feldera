from tests.runtime_aggtest.aggtst_base import TstView


class aggtst_float_sum(TstView):
    def __init__(self):
        # Validated on Postgres
        self.sql = """
        CREATE MATERIALIZED VIEW float_sum_value AS
        WITH f AS (
            SELECT
                SUM(c1) AS f1,
                SUM(c2) AS f2,
                SUM(c3) AS f3,
                SUM(c4) AS f4
            FROM float_tbl
        ),
        d AS (
            SELECT
                ROUND(SUM(c1), 8) AS d1,
                ROUND(SUM(c2), 8) AS d2,
                ROUND(SUM(c3), 18) AS d3,
                ROUND(SUM(c4), 18) AS d4
            FROM float_decimal_tbl
        )
        SELECT
            ABS(f1 - d1) < 1e-3 AS c1,
            ABS(f2 - d2) < 1e-3 AS c2,
            ABS(f3 - d3) < 1e-7 AS c3,
            ABS(f4 - d4) < 1e-5 AS c4
        FROM f, d
        """
        self.data = [
            {
                "c1": True,
                "c2": True,
                "c3": True,
                "c4": True,
            }
        ]


class aggtst_float_sum_distinct(TstView):
    def __init__(self):
        # Validated on Postgres
        self.sql = """
        CREATE MATERIALIZED VIEW float_sum_distinct AS
        WITH f AS (
            SELECT
                SUM(DISTINCT c1) AS f1,
                SUM(DISTINCT c2) AS f2,
                SUM(DISTINCT c3) AS f3,
                SUM(DISTINCT c4) AS f4
            FROM float_tbl
        ),
        d AS (
            SELECT
                ROUND(SUM(DISTINCT c1), 8) AS d1,
                ROUND(SUM(DISTINCT c2), 8) AS d2,
                ROUND(SUM(DISTINCT c3), 18) AS d3,
                ROUND(SUM(DISTINCT c4), 18) AS d4
            FROM float_decimal_tbl
        )
        SELECT
            ABS(f1 - d1) < 1e-3 AS c1,
            ABS(f2 - d2) < 1e-3 AS c2,
            ABS(f3 - d3) < 1e-7 AS c3,
            ABS(f4 - d4) < 1e-5 AS c4
        FROM f, d
        """
        self.data = [
            {
                "c1": True,
                "c2": True,
                "c3": True,
                "c4": True,
            }
        ]


class aggtst_float_sum_filter_where(TstView):
    def __init__(self):
        # Validated on Postgres
        self.sql = """
        CREATE MATERIALIZED VIEW float_sum_filter_where AS
        WITH f AS (
            SELECT
                SUM(c1) FILTER (WHERE c3 IS NOT NULL) AS f1,
                SUM(c2) FILTER (WHERE c3 IS NOT NULL) AS f2,
                SUM(c3) FILTER (WHERE c3 IS NOT NULL) AS f3,
                SUM(c4) FILTER (WHERE c3 IS NOT NULL) AS f4
            FROM float_tbl
        ),
        d AS (
            SELECT
                ROUND(SUM(c1) FILTER (WHERE c3 IS NOT NULL), 8) AS d1,
                ROUND(SUM(c2) FILTER (WHERE c3 IS NOT NULL), 8) AS d2,
                ROUND(SUM(c3) FILTER (WHERE c3 IS NOT NULL), 18) AS d3,
                ROUND(SUM(c4) FILTER (WHERE c3 IS NOT NULL), 18) AS d4
            FROM float_decimal_tbl
        )
        SELECT
            ABS(f1 - d1) < 1e-2 AS c1,
            ABS(f2 - d2) < 1e-3 AS c2,
            ABS(f3 - d3) < 1e-7 AS c3,
            ABS(f4 - d4) < 1e-5 AS c4
        FROM f, d
        """
        self.data = [
            {
                "c1": True,
                "c2": True,
                "c3": True,
                "c4": True,
            }
        ]
