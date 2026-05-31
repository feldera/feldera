from tests.runtime_aggtest.aggtst_base import TstView


class aggtst_float_min(TstView):
    def __init__(self):
        # Validated on Postgres
        self.sql = """
        CREATE MATERIALIZED VIEW float_min AS
        WITH f AS (
            SELECT
                MIN(c1) AS f1,
                MIN(c2) AS f2,
                MIN(c3) AS f3,
                MIN(c4) AS f4
            FROM float_tbl
        ),
        d AS (
            SELECT
                MIN(c1) AS d1,
                MIN(c2) AS d2,
                MIN(c3) AS d3,
                MIN(c4) AS d4
            FROM float_decimal_tbl
        )
        SELECT
            ABS(f1 - d1) < 1e-4 AS c1,
            ABS(f2 - d2) < 1e-3 AS c2,
            ABS(f3 - d3) < 1e-6 AS c3,
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


class aggtst_float_min_distinct(TstView):
    def __init__(self):
        # Validated on Postgres
        self.sql = """
        CREATE MATERIALIZED VIEW float_min_distinct AS
        WITH f AS (
            SELECT
                MIN(DISTINCT c1) AS f1,
                MIN(DISTINCT c2) AS f2,
                MIN(DISTINCT c3) AS f3,
                MIN(DISTINCT c4) AS f4
            FROM float_tbl
        ),
        d AS (
            SELECT
                MIN(DISTINCT c1) AS d1,
                MIN(DISTINCT c2) AS d2,
                MIN(DISTINCT c3) AS d3,
                MIN(DISTINCT c4) AS d4
            FROM float_decimal_tbl
        )
        SELECT
            ABS(f1 - d1) < 1e-4 AS c1,
            ABS(f2 - d2) < 1e-3 AS c2,
            ABS(f3 - d3) < 1e-6 AS c3,
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


class aggtst_float_min_filter_where(TstView):
    def __init__(self):
        # Validated on Postgres
        self.sql = """
        CREATE MATERIALIZED VIEW float_min_filter_where AS
        WITH f AS (
            SELECT
                MIN(c1) FILTER (WHERE c3 IS NOT NULL) AS f1,
                MIN(c2) FILTER (WHERE c3 IS NOT NULL) AS f2,
                MIN(c3) FILTER (WHERE c3 IS NOT NULL) AS f3,
                MIN(c4) FILTER (WHERE c3 IS NOT NULL) AS f4
            FROM float_tbl
        ),
        d AS (
            SELECT
                MIN(c1) FILTER (WHERE c3 IS NOT NULL) AS d1,
                MIN(c2) FILTER (WHERE c3 IS NOT NULL) AS d2,
                MIN(c3) FILTER (WHERE c3 IS NOT NULL) AS d3,
                MIN(c4) FILTER (WHERE c3 IS NOT NULL) AS d4
            FROM float_decimal_tbl
        )
        SELECT
            ABS(f1 - d1) < 1e-4 AS c1,
            ABS(f2 - d2) < 1e-3 AS c2,
            ABS(f3 - d3) < 1e-6 AS c3,
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
