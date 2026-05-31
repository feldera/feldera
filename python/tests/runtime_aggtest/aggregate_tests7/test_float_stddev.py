from tests.runtime_aggtest.aggtst_base import TstTable, TstView


class aggtst_float_table_safe(TstTable):
    """Define the table used by the REAL and DOUBLE tests"""

    def __init__(self):
        self.sql = """CREATE TABLE float_tbl_safe(
                      id INT,
                      c1 REAL,
                      c2 REAL NOT NULL,
                      c3 DOUBLE PRECISION,
                      c4 DOUBLE PRECISION NOT NULL)"""
        self.data = [
            {
                "id": 0,
                "c1": None,
                "c2": 123.4562,
                "c3": 1.61803398,
                "c4": 980.1234567890,
            },
            {
                "id": 1,
                "c1": -9.8765432,
                "c2": -8.7654,
                "c3": -7.65432109,
                "c4": -6.54321098,
            },
        ]


class aggtst_float_decimal_table_safe(TstTable):
    """Define the table used by the REAL and DOUBLE tests"""

    def __init__(self):
        self.sql = """CREATE TABLE float_decimal_tbl_safe(
                      id INT,
                      c1 DECIMAL(18, 8),
                      c2 DECIMAL(18, 8),
                      c3 DECIMAL(38,18),
                      c4 DECIMAL(38,18))"""
        self.data = [
            {
                "id": 0,
                "c1": None,
                "c2": 123.4562,
                "c3": 1.61803398,
                "c4": 980.1234567890,
            },
            {
                "id": 1,
                "c1": -9.8765432,
                "c2": -8.7654,
                "c3": -7.65432109,
                "c4": -6.54321098,
            },
        ]


class aggtst_float_stddev(TstView):
    def __init__(self):
        # Validated on Postgres
        self.sql = """
        CREATE MATERIALIZED VIEW float_stddev AS
        WITH f AS (
            SELECT
                STDDEV_SAMP(c1) AS f1,
                STDDEV_SAMP(c2) AS f2,
                STDDEV_SAMP(c3) AS f3,
                STDDEV_SAMP(c4) AS f4
            FROM float_tbl_safe
        ),
        d AS (
            SELECT
                STDDEV_SAMP(c1) AS d1,
                STDDEV_SAMP(c2) AS d2,
                STDDEV_SAMP(c3) AS d3,
                STDDEV_SAMP(c4) AS d4
            FROM float_decimal_tbl_safe
        )
        SELECT
            ABS(f1 - d1) < 1e-5 AS c1,
            ABS(f2 - d2) < 1e-5 AS c2,
            ABS(f3 - d3) < 1e-10 AS c3,
            ABS(f4 - d4) < 1e-10 AS c4
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


class aggtst_float_stddev_distinct(TstView):
    def __init__(self):
        # Validated on Postgres
        self.sql = """
        CREATE MATERIALIZED VIEW float_stddev_distinct AS
        WITH f AS (
            SELECT
                STDDEV_SAMP(DISTINCT c1) AS f1,
                STDDEV_SAMP(DISTINCT c2) AS f2,
                STDDEV_SAMP(DISTINCT c3) AS f3,
                STDDEV_SAMP(DISTINCT c4) AS f4
            FROM float_tbl_safe
        ),
        d AS (
            SELECT
                STDDEV_SAMP(DISTINCT c1) AS d1,
                STDDEV_SAMP(DISTINCT c2) AS d2,
                STDDEV_SAMP(DISTINCT c3) AS d3,
                STDDEV_SAMP(DISTINCT c4) AS d4
            FROM float_decimal_tbl_safe
        )
        SELECT
            ABS(f1 - d1) < 1e-5 AS c1,
            ABS(f2 - d2) < 1e-5 AS c2,
            ABS(f3 - d3) < 1e-10 AS c3,
            ABS(f4 - d4) < 1e-10 AS c4
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


class aggtst_float_stddev_filter_where(TstView):
    def __init__(self):
        # Validated on Postgres
        self.sql = """
        CREATE MATERIALIZED VIEW float_stddev_filter_where AS
        WITH f AS (
            SELECT
                STDDEV_SAMP(c1) FILTER (WHERE c3 IS NOT NULL) AS f1,
                STDDEV_SAMP(c2) FILTER (WHERE c3 IS NOT NULL) AS f2,
                STDDEV_SAMP(c3) FILTER (WHERE c3 IS NOT NULL) AS f3,
                STDDEV_SAMP(c4) FILTER (WHERE c3 IS NOT NULL) AS f4
            FROM float_tbl_safe
        ),
        d AS (
            SELECT
                STDDEV_SAMP(c1) FILTER (WHERE c3 IS NOT NULL) AS d1,
                STDDEV_SAMP(c2) FILTER (WHERE c3 IS NOT NULL) AS d2,
                STDDEV_SAMP(c3) FILTER (WHERE c3 IS NOT NULL) AS d3,
                STDDEV_SAMP(c4) FILTER (WHERE c3 IS NOT NULL) AS d4
            FROM float_decimal_tbl_safe
        )
        SELECT
            ABS(f1 - d1) < 1e-5 AS c1,
            ABS(f2 - d2) < 1e-5 AS c2,
            ABS(f3 - d3) < 1e-10 AS c3,
            ABS(f4 - d4) < 1e-10 AS c4
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


class aggtst_float_stddev_pop(TstView):
    def __init__(self):
        # Validated on Postgres
        self.sql = """
        CREATE MATERIALIZED VIEW float_stddev_pop_cmp AS
        WITH f AS (
            SELECT
                STDDEV_POP(c1) AS f1,
                STDDEV_POP(c2) AS f2,
                STDDEV_POP(c3) AS f3,
                STDDEV_POP(c4) AS f4
            FROM float_tbl_safe
        ),
        d AS (
            SELECT
                STDDEV_POP(c1) AS d1,
                STDDEV_POP(c2) AS d2,
                STDDEV_POP(c3) AS d3,
                STDDEV_POP(c4) AS d4
            FROM float_decimal_tbl_safe
        )
        SELECT
            (f1 = d1) AS c1, -- they are both NULL
            ABS(f2 - d2) < 1e-5 AS c2,
            ABS(f3 - d3) < 1e-9 AS c3,
            ABS(f4 - d4) < 1e-10 AS c4
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


class aggtst_float_stddev_pop_distinct(TstView):
    def __init__(self):
        # Validated on Postgres
        self.sql = """
        CREATE MATERIALIZED VIEW float_stddev_pop_distinct AS
        WITH f AS (
            SELECT
                STDDEV_POP(DISTINCT c1) AS f1,
                STDDEV_POP(DISTINCT c2) AS f2,
                STDDEV_POP(DISTINCT c3) AS f3,
                STDDEV_POP(DISTINCT c4) AS f4
            FROM float_tbl_safe
        ),
        d AS (
            SELECT
                STDDEV_POP(DISTINCT c1) AS d1,
                STDDEV_POP(DISTINCT c2) AS d2,
                STDDEV_POP(DISTINCT c3) AS d3,
                STDDEV_POP(DISTINCT c4) AS d4
            FROM float_decimal_tbl_safe
        )
        SELECT
            (f1 = d1) AS c1, -- they are both NULL
            ABS(f2 - d2) < 1e-5 AS c2,
            ABS(f3 - d3) < 1e-9 AS c3,
            ABS(f4 - d4) < 1e-10 AS c4
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


class aggtst_float_stddev_pop_filter_where(TstView):
    def __init__(self):
        # Validated on Postgres
        self.sql = """
        CREATE MATERIALIZED VIEW float_stddev_pop_filter_where AS
        WITH f AS (
            SELECT
                STDDEV_POP(c1) FILTER (WHERE c3 IS NOT NULL) AS f1,
                STDDEV_POP(c2) FILTER (WHERE c3 IS NOT NULL) AS f2,
                STDDEV_POP(c3) FILTER (WHERE c3 IS NOT NULL) AS f3,
                STDDEV_POP(c4) FILTER (WHERE c3 IS NOT NULL) AS f4
            FROM float_tbl_safe
        ),
        d AS (
            SELECT
                STDDEV_POP(c1) FILTER (WHERE c3 IS NOT NULL) AS d1,
                STDDEV_POP(c2) FILTER (WHERE c3 IS NOT NULL) AS d2,
                STDDEV_POP(c3) FILTER (WHERE c3 IS NOT NULL) AS d3,
                STDDEV_POP(c4) FILTER (WHERE c3 IS NOT NULL) AS d4
            FROM float_decimal_tbl_safe
        )
        SELECT
            (f1 = d1) AS c1, -- they are both NULL
            ABS(f2 - d2) < 1e-5 AS c2,
            ABS(f3 - d3) < 1e-9 AS c3,
            ABS(f4 - d4) < 1e-10 AS c4
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
