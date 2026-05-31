from tests.runtime_aggtest.aggtst_base import TstView


class aggtst_float_arg_min(TstView):
    def __init__(self):
        # checked manually
        self.sql = """
        CREATE MATERIALIZED VIEW float_arg_min AS
        WITH f AS (
            SELECT
                ARG_MIN(c1, c2) AS f1,
                ARG_MIN(c2, c1) AS f2,
                ARG_MIN(c3, c4) AS f3,
                ARG_MIN(c4, c3) AS f4
            FROM float_tbl
        ),
        d AS (
            SELECT
                ARG_MIN(c1, c2) AS d1,
                ARG_MIN(c2, c1) AS d2,
                ARG_MIN(c3, c4) AS d3,
                ARG_MIN(c4, c3) AS d4
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


class aggtst_float_arg_min_distinct(TstView):
    def __init__(self):
        # checked manually
        self.sql = """
        CREATE MATERIALIZED VIEW float_arg_min_distinct AS
        WITH f AS (
            SELECT
                ARG_MIN(DISTINCT c1, c2) AS f1,
                ARG_MIN(DISTINCT c2, c1) AS f2,
                ARG_MIN(DISTINCT c3, c4) AS f3,
                ARG_MIN(DISTINCT c4, c3) AS f4
            FROM float_tbl
        ),
        d AS (
            SELECT
                ARG_MIN(DISTINCT c1, c2) AS d1,
                ARG_MIN(DISTINCT c2, c1) AS d2,
                ARG_MIN(DISTINCT c3, c4) AS d3,
                ARG_MIN(DISTINCT c4, c3) AS d4
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


class aggtst_float_arg_min_filter_where(TstView):
    def __init__(self):
        # checked manually
        self.sql = """
        CREATE MATERIALIZED VIEW float_arg_min_filter_where AS
        WITH f AS (
            SELECT
                ARG_MIN(c1, c2) FILTER (WHERE c3 IS NOT NULL) AS f1,
                ARG_MIN(c2, c1) FILTER (WHERE c3 IS NOT NULL) AS f2,
                ARG_MIN(c3, c4) FILTER (WHERE c3 IS NOT NULL) AS f3,
                ARG_MIN(c4, c3) FILTER (WHERE c3 IS NOT NULL) AS f4
            FROM float_tbl
        ),
        d AS (
            SELECT
                ARG_MIN(c1, c2) FILTER (WHERE c3 IS NOT NULL) AS d1,
                ARG_MIN(c2, c1) FILTER (WHERE c3 IS NOT NULL) AS d2,
                ARG_MIN(c3, c4) FILTER (WHERE c3 IS NOT NULL) AS d3,
                ARG_MIN(c4, c3) FILTER (WHERE c3 IS NOT NULL) AS d4
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
