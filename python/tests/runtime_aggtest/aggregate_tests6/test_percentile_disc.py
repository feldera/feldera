from decimal import Decimal

from tests.runtime_aggtest.aggtst_base import TstView


class aggtst_percentile_disc(TstView):
    def __init__(self):
        # PERCENTILE_DISC returns discrete (actual) value from dataset
        self.sql = """CREATE MATERIALIZED VIEW percentile_disc AS SELECT
                      PERCENTILE_DISC(0.5) WITHIN GROUP (ORDER BY value) AS median
                      FROM percentile_tbl"""
        self.data = [{"median": 5.0}]


class aggtst_percentile_disc_gby(TstView):
    def __init__(self):
        # PERCENTILE_DISC with GROUP BY
        self.sql = """CREATE MATERIALIZED VIEW percentile_disc_gby AS SELECT
                      id,
                      PERCENTILE_DISC(0.5) WITHIN GROUP (ORDER BY value) AS median
                      FROM percentile_tbl
                      GROUP BY id"""
        self.data = [
            {"id": 0, "median": 3.0},
            {"id": 1, "median": 30.0},
        ]


class aggtst_percentile_disc_quartiles(TstView):
    def __init__(self):
        # Test multiple percentiles
        self.sql = """CREATE MATERIALIZED VIEW percentile_disc_quartiles AS SELECT
                      PERCENTILE_DISC(0.0) WITHIN GROUP (ORDER BY value) AS p0,
                      PERCENTILE_DISC(0.25) WITHIN GROUP (ORDER BY value) AS p25,
                      PERCENTILE_DISC(0.5) WITHIN GROUP (ORDER BY value) AS p50,
                      PERCENTILE_DISC(0.75) WITHIN GROUP (ORDER BY value) AS p75,
                      PERCENTILE_DISC(1.0) WITHIN GROUP (ORDER BY value) AS p100
                      FROM percentile_tbl"""
        self.data = [
            {
                "p0": Decimal("1.0"),
                "p25": Decimal("3.0"),
                "p50": Decimal("5.0"),
                "p75": Decimal("30.0"),
                "p100": Decimal("50.0"),
            }
        ]


class aggtst_percentile_disc_quartiles_gby(TstView):
    def __init__(self):
        # Test multiple percentiles with GROUP BY
        self.sql = """CREATE MATERIALIZED VIEW percentile_disc_quartiles_gby AS SELECT
                      id,
                      PERCENTILE_DISC(0.0) WITHIN GROUP (ORDER BY value) AS p0,
                      PERCENTILE_DISC(0.25) WITHIN GROUP (ORDER BY value) AS p25,
                      PERCENTILE_DISC(0.5) WITHIN GROUP (ORDER BY value) AS p50,
                      PERCENTILE_DISC(0.75) WITHIN GROUP (ORDER BY value) AS p75,
                      PERCENTILE_DISC(1.0) WITHIN GROUP (ORDER BY value) AS p100
                      FROM percentile_tbl
                      GROUP BY id"""
        self.data = [
            {
                "id": 0,
                "p0": Decimal("1.0"),
                "p25": Decimal("2.0"),
                "p50": Decimal("3.0"),
                "p75": Decimal("4.0"),
                "p100": Decimal("5.0"),
            },
            {
                "id": 1,
                "p0": Decimal("10.0"),
                "p25": Decimal("20.0"),
                "p50": Decimal("30.0"),
                "p75": Decimal("40.0"),
                "p100": Decimal("50.0"),
            },
        ]


class aggtst_percentile_disc_desc(TstView):
    def __init__(self):
        # Test PERCENTILE_DISC with DESC order
        self.sql = """CREATE MATERIALIZED VIEW percentile_disc_desc AS SELECT
                      PERCENTILE_DISC(0.5) WITHIN GROUP (ORDER BY value DESC) AS median_desc
                      FROM percentile_tbl"""
        self.data = [{"median_desc": Decimal("10.0")}]


class aggtst_percentile_disc_desc_gby(TstView):
    def __init__(self):
        # Test PERCENTILE_DISC with DESC order and GROUP BY
        self.sql = """CREATE MATERIALIZED VIEW percentile_disc_desc_gby AS SELECT
                      id,
                      PERCENTILE_DISC(0.5) WITHIN GROUP (ORDER BY value DESC) AS median_desc
                      FROM percentile_tbl
                      GROUP BY id"""
        self.data = [
            {"id": 0, "median_desc": 3.0},
            {"id": 1, "median_desc": 30.0},
        ]


# Edge case tests for PERCENTILE_DISC


class aggtst_percentile_disc_single_value(TstView):
    def __init__(self):
        # Test PERCENTILE_DISC with a single value
        self.sql = """CREATE MATERIALIZED VIEW percentile_disc_single AS SELECT
                      PERCENTILE_DISC(0.0) WITHIN GROUP (ORDER BY value) AS p0,
                      PERCENTILE_DISC(0.5) WITHIN GROUP (ORDER BY value) AS p50,
                      PERCENTILE_DISC(1.0) WITHIN GROUP (ORDER BY value) AS p100
                      FROM percentile_single_tbl"""
        self.data = [
            {"p0": Decimal("42.0"), "p50": Decimal("42.0"), "p100": Decimal("42.0")}
        ]


class aggtst_percentile_disc_two_values(TstView):
    def __init__(self):
        # Test PERCENTILE_DISC with exactly two values
        self.sql = """CREATE MATERIALIZED VIEW percentile_disc_two AS SELECT
                      PERCENTILE_DISC(0.0) WITHIN GROUP (ORDER BY value) AS p0,
                      PERCENTILE_DISC(0.25) WITHIN GROUP (ORDER BY value) AS p25,
                      PERCENTILE_DISC(0.5) WITHIN GROUP (ORDER BY value) AS p50,
                      PERCENTILE_DISC(0.75) WITHIN GROUP (ORDER BY value) AS p75,
                      PERCENTILE_DISC(1.0) WITHIN GROUP (ORDER BY value) AS p100
                      FROM percentile_two_tbl"""
        # PERCENTILE_DISC returns actual values from dataset (no interpolation)
        # For n=2: [0.0, 100.0]
        # p0 returns first value, p100 returns last value
        # Other percentiles return the value at or before the percentile position
        self.data = [
            {
                "p0": Decimal("0.0"),
                "p25": Decimal("0.0"),
                "p50": Decimal("0.0"),
                "p75": Decimal("100.0"),
                "p100": Decimal("100.0"),
            }
        ]


class aggtst_percentile_disc_duplicates(TstView):
    def __init__(self):
        # Test PERCENTILE_DISC with duplicate values
        self.sql = """CREATE MATERIALIZED VIEW percentile_disc_dups AS SELECT
                      PERCENTILE_DISC(0.5) WITHIN GROUP (ORDER BY value) AS median
                      FROM percentile_dup_tbl"""
        # Values: [5, 5, 5, 10, 10] -> sorted, middle value
        self.data = [{"median": Decimal("5.0")}]


class aggtst_percentile_disc_negative(TstView):
    def __init__(self):
        # Test PERCENTILE_DISC with negative values
        self.sql = """CREATE MATERIALIZED VIEW percentile_disc_neg AS SELECT
                      PERCENTILE_DISC(0.0) WITHIN GROUP (ORDER BY value) AS p0,
                      PERCENTILE_DISC(0.5) WITHIN GROUP (ORDER BY value) AS p50,
                      PERCENTILE_DISC(1.0) WITHIN GROUP (ORDER BY value) AS p100
                      FROM percentile_neg_tbl"""
        # Values: [-20, -10, 0, 10, 20]
        self.data = [
            {"p0": Decimal("-20.0"), "p50": Decimal("0.0"), "p100": Decimal("20.0")}
        ]


class aggtst_percentile_disc_with_nulls(TstView):
    def __init__(self):
        # Test PERCENTILE_DISC ignores NULL values
        self.sql = """CREATE MATERIALIZED VIEW percentile_disc_nulls AS SELECT
                      PERCENTILE_DISC(0.5) WITHIN GROUP (ORDER BY value) AS median
                      FROM percentile_null_tbl"""
        # Values with nulls removed: [1, 2, 3] -> median is 2
        self.data = [{"median": Decimal("2.0")}]


class aggtst_percentile_disc_near_extremes(TstView):
    def __init__(self):
        # Test PERCENTILE_DISC with percentiles very close to 0 and 1
        self.sql = """CREATE MATERIALIZED VIEW percentile_disc_extremes AS SELECT
                      PERCENTILE_DISC(0.01) WITHIN GROUP (ORDER BY value) AS p1,
                      PERCENTILE_DISC(0.99) WITHIN GROUP (ORDER BY value) AS p99
                      FROM percentile_tbl"""
        # PERCENTILE_DISC returns actual values, not interpolated
        # For n=10 values [1,2,3,4,5,10,20,30,40,50], p1 returns first value, p99 returns last
        self.data = [{"p1": Decimal("1.0"), "p99": Decimal("50.0")}]


class aggtst_percentile_disc_all_same(TstView):
    def __init__(self):
        # Test PERCENTILE_DISC when all values are identical
        self.sql = """CREATE MATERIALIZED VIEW percentile_disc_same AS SELECT
                      PERCENTILE_DISC(0.0) WITHIN GROUP (ORDER BY value) AS p0,
                      PERCENTILE_DISC(0.5) WITHIN GROUP (ORDER BY value) AS p50,
                      PERCENTILE_DISC(1.0) WITHIN GROUP (ORDER BY value) AS p100
                      FROM percentile_same_tbl"""
        self.data = [
            {"p0": Decimal("7.0"), "p50": Decimal("7.0"), "p100": Decimal("7.0")}
        ]


class aggtst_percentile_disc_three_values(TstView):
    def __init__(self):
        # Test PERCENTILE_DISC with exactly three values (odd count)
        self.sql = """CREATE MATERIALIZED VIEW percentile_disc_three AS SELECT
                      PERCENTILE_DISC(0.0) WITHIN GROUP (ORDER BY value) AS p0,
                      PERCENTILE_DISC(0.25) WITHIN GROUP (ORDER BY value) AS p25,
                      PERCENTILE_DISC(0.5) WITHIN GROUP (ORDER BY value) AS p50,
                      PERCENTILE_DISC(0.75) WITHIN GROUP (ORDER BY value) AS p75,
                      PERCENTILE_DISC(1.0) WITHIN GROUP (ORDER BY value) AS p100
                      FROM percentile_three_tbl"""
        # Values: [10, 20, 30]
        # PERCENTILE_DISC returns actual values
        self.data = [
            {
                "p0": Decimal("10.0"),
                "p25": Decimal("10.0"),
                "p50": Decimal("20.0"),
                "p75": Decimal("30.0"),
                "p100": Decimal("30.0"),
            }
        ]


class aggtst_percentile_disc_four_values(TstView):
    def __init__(self):
        # Test PERCENTILE_DISC with exactly four values (even count)
        self.sql = """CREATE MATERIALIZED VIEW percentile_disc_four AS SELECT
                      PERCENTILE_DISC(0.0) WITHIN GROUP (ORDER BY value) AS p0,
                      PERCENTILE_DISC(0.25) WITHIN GROUP (ORDER BY value) AS p25,
                      PERCENTILE_DISC(0.5) WITHIN GROUP (ORDER BY value) AS p50,
                      PERCENTILE_DISC(0.75) WITHIN GROUP (ORDER BY value) AS p75,
                      PERCENTILE_DISC(1.0) WITHIN GROUP (ORDER BY value) AS p100
                      FROM percentile_four_tbl"""
        # Values: [0, 10, 20, 30]
        self.data = [
            {
                "p0": Decimal("0.0"),
                "p25": Decimal("0.0"),
                "p50": Decimal("10.0"),
                "p75": Decimal("20.0"),
                "p100": Decimal("30.0"),
            }
        ]


class aggtst_percentile_disc_mixed_gby(TstView):
    def __init__(self):
        # Test GROUP BY with groups of different sizes
        self.sql = """CREATE MATERIALIZED VIEW percentile_disc_mixed_gby AS SELECT
                      grp,
                      PERCENTILE_DISC(0.5) WITHIN GROUP (ORDER BY value) AS median
                      FROM percentile_mixed_tbl
                      GROUP BY grp"""
        # grp 'A': [10, 20, 30] -> median = 20 (middle value)
        # grp 'B': [100] -> median = 100
        # grp 'C': [1, 2, 3, 4, 5] -> median = 3 (middle value)
        self.data = [
            {"grp": "A", "median": 20.0},
            {"grp": "B", "median": 100.0},
            {"grp": "C", "median": 3.0},
        ]
