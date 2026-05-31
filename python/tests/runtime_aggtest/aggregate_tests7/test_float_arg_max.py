from tests.runtime_aggtest.aggtst_base import TstView


class aggtst_float_arg_max(TstView):
    def __init__(self):
        # checked manually
        self.sql = """
        CREATE MATERIALIZED VIEW float_arg_max_value AS
        WITH f AS (
            SELECT
                ARG_MAX(c1, c2) AS f1,
                ARG_MAX(c2, c1) AS f2,
                ARG_MAX(c3, c4) AS f3,
                ARG_MAX(c4, c3) AS f4
            FROM float_tbl
        ),
        d AS (
            SELECT
                ROUND(ARG_MAX(c1, c2), 8)  AS d1,
                ROUND(ARG_MAX(c2, c1), 8)  AS d2,
                ROUND(ARG_MAX(c3, c4), 18) AS d3,
                ROUND(ARG_MAX(c4, c3), 18) AS d4
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
                "c1": None,
                "c2": True,
                "c3": True,
                "c4": True,
            }
        ]


class aggtst_float_arg_max_distinct(TstView):
    def __init__(self):
        # checked manually
        self.sql = """
        CREATE MATERIALIZED VIEW float_arg_max_distinct AS
        WITH f AS (
            SELECT
                ARG_MAX(DISTINCT c1, c2) AS f1,
                ARG_MAX(DISTINCT c2, c1) AS f2,
                ARG_MAX(DISTINCT c3, c4) AS f3,
                ARG_MAX(DISTINCT c4, c3) AS f4
            FROM float_tbl
        ),
        d AS (
            SELECT
                ROUND(ARG_MAX(DISTINCT c1, c2), 8)  AS d1,
                ROUND(ARG_MAX(DISTINCT c2, c1), 8)  AS d2,
                ROUND(ARG_MAX(DISTINCT c3, c4), 18) AS d3,
                ROUND(ARG_MAX(DISTINCT c4, c3), 18) AS d4
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
                "c1": None,
                "c2": True,
                "c3": True,
                "c4": True,
            }
        ]


class aggtst_float_arg_max_filter_where(TstView):
    def __init__(self):
        # checked manually
        self.sql = """
        CREATE MATERIALIZED VIEW float_arg_max_filter_where AS
        WITH f AS (
            SELECT
                ARG_MAX(c1, c2) FILTER (WHERE c3 IS NOT NULL) AS f1,
                ARG_MAX(c2, c1) FILTER (WHERE c3 IS NOT NULL) AS f2,
                ARG_MAX(c3, c4) FILTER (WHERE c3 IS NOT NULL) AS f3,
                ARG_MAX(c4, c3) FILTER (WHERE c3 IS NOT NULL) AS f4
            FROM float_tbl
        ),
        d AS (
            SELECT
                ROUND(ARG_MAX(c1, c2) FILTER (WHERE c3 IS NOT NULL), 8)  AS d1,
                ROUND(ARG_MAX(c2, c1) FILTER (WHERE c3 IS NOT NULL), 8)  AS d2,
                ROUND(ARG_MAX(c3, c4) FILTER (WHERE c3 IS NOT NULL), 18) AS d3,
                ROUND(ARG_MAX(c4, c3) FILTER (WHERE c3 IS NOT NULL), 18) AS d4
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
                "c1": None,
                "c2": True,
                "c3": True,
                "c4": True,
            }
        ]
