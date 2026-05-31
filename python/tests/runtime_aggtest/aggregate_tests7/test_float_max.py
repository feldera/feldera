from tests.runtime_aggtest.aggtst_base import TstView


class aggtst_float_max(TstView):
    def __init__(self):
        # Validated on Postgres
        self.sql = """
        CREATE MATERIALIZED VIEW float_max_value AS
        WITH f AS (
            SELECT
                MAX(c1) AS f1,
                MAX(c2) AS f2,
                MAX(c3) AS f3,
                MAX(c4) AS f4
            FROM float_tbl
        ),
        d AS (
            SELECT
                ROUND(MAX(c1), 8)  AS d1,
                ROUND(MAX(c2), 8)  AS d2,
                ROUND(MAX(c3), 18) AS d3,
                ROUND(MAX(c4), 18) AS d4
            FROM float_decimal_tbl
        )
        SELECT
            ABS(f1 - d1) < 1e-5 AS c1,
            ABS(f2 - d2) < 1e-3 AS c2,
            ABS(f3 - d3) < 1e-10 AS c3,
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


class aggtst_float_max_distinct(TstView):
    def __init__(self):
        # Validated on Postgres
        self.sql = """
        CREATE MATERIALIZED VIEW float_max_distinct AS
        WITH f AS (
            SELECT
                MAX(DISTINCT c1) AS f1,
                MAX(DISTINCT c2) AS f2,
                MAX(DISTINCT c3) AS f3,
                MAX(DISTINCT c4) AS f4
            FROM float_tbl
        ),
        d AS (
            SELECT
                ROUND(MAX(DISTINCT c1), 8)  AS d1,
                ROUND(MAX(DISTINCT c2), 8)  AS d2,
                ROUND(MAX(DISTINCT c3), 18) AS d3,
                ROUND(MAX(DISTINCT c4), 18) AS d4
            FROM float_decimal_tbl
        )
        SELECT
            ABS(f1 - d1) < 1e-5 AS c1,
            ABS(f2 - d2) < 1e-3 AS c2,
            ABS(f3 - d3) < 1e-10 AS c3,
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


class aggtst_float_max_filter_where(TstView):
    def __init__(self):
        # Validated on Postgres
        self.sql = """
        CREATE MATERIALIZED VIEW float_max_filter_where AS
        WITH f AS (
            SELECT
                MAX(c1) FILTER (WHERE c3 IS NOT NULL) AS f1,
                MAX(c2) FILTER (WHERE c3 IS NOT NULL) AS f2,
                MAX(c3) FILTER (WHERE c3 IS NOT NULL) AS f3,
                MAX(c4) FILTER (WHERE c3 IS NOT NULL) AS f4
            FROM float_tbl
        ),
        d AS (
            SELECT
                ROUND(MAX(c1) FILTER (WHERE c3 IS NOT NULL), 8)  AS d1,
                ROUND(MAX(c2) FILTER (WHERE c3 IS NOT NULL), 8)  AS d2,
                ROUND(MAX(c3) FILTER (WHERE c3 IS NOT NULL), 18) AS d3,
                ROUND(MAX(c4) FILTER (WHERE c3 IS NOT NULL), 18) AS d4
            FROM float_decimal_tbl
        )
        SELECT
            ABS(f1 - d1) < 1e-5 AS c1,
            ABS(f2 - d2) < 1e-3 AS c2,
            ABS(f3 - d3) < 1e-10 AS c3,
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
