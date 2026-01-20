from decimal import Decimal

from tests.runtime_aggtest.aggtst_base import TstView


class aggtst_percentile_cont(TstView):
    def __init__(self):
        # PERCENTILE_CONT returns continuous (interpolated) percentile
        self.sql = """CREATE MATERIALIZED VIEW percentile_cont AS SELECT
                      PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY value) AS median
                      FROM percentile_tbl"""
        self.data = [{"median": 7.5}]


class aggtst_percentile_cont_gby(TstView):
    def __init__(self):
        # PERCENTILE_CONT with GROUP BY
        self.sql = """CREATE MATERIALIZED VIEW percentile_cont_gby AS SELECT
                      id,
                      PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY value) AS median
                      FROM percentile_tbl
                      GROUP BY id"""
        self.data = [
            {"id": 0, "median": 3.0},
            {"id": 1, "median": 30.0},
        ]


class aggtst_percentile_cont_quartiles(TstView):
    def __init__(self):
        # Test multiple percentiles
        self.sql = """CREATE MATERIALIZED VIEW percentile_cont_quartiles AS SELECT
                      PERCENTILE_CONT(0.0) WITHIN GROUP (ORDER BY value) AS p0,
                      PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY value) AS p25,
                      PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY value) AS p50,
                      PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY value) AS p75,
                      PERCENTILE_CONT(1.0) WITHIN GROUP (ORDER BY value) AS p100
                      FROM percentile_tbl"""
        self.data = [{"p0": 1.0, "p25": 3.25, "p50": 7.5, "p75": 27.5, "p100": 50.0}]


class aggtst_percentile_cont_quartiles_gby(TstView):
    def __init__(self):
        # Test multiple percentiles with GROUP BY
        self.sql = """CREATE MATERIALIZED VIEW percentile_cont_quartiles_gby AS SELECT
                      id,
                      PERCENTILE_CONT(0.0) WITHIN GROUP (ORDER BY value) AS p0,
                      PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY value) AS p25,
                      PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY value) AS p50,
                      PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY value) AS p75,
                      PERCENTILE_CONT(1.0) WITHIN GROUP (ORDER BY value) AS p100
                      FROM percentile_tbl
                      GROUP BY id"""
        self.data = [
            {"id": 0, "p0": 1.0, "p25": 2.0, "p50": 3.0, "p75": 4.0, "p100": 5.0},
            {"id": 1, "p0": 10.0, "p25": 20.0, "p50": 30.0, "p75": 40.0, "p100": 50.0},
        ]


class aggtst_percentile_cont_desc(TstView):
    def __init__(self):
        # Test PERCENTILE_CONT with DESC order
        self.sql = """CREATE MATERIALIZED VIEW percentile_cont_desc AS SELECT
                      PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY value DESC) AS median_desc
                      FROM percentile_tbl"""
        self.data = [{"median_desc": 7.5}]


class aggtst_percentile_cont_desc_gby(TstView):
    def __init__(self):
        # Test PERCENTILE_CONT with DESC order and GROUP BY
        self.sql = """CREATE MATERIALIZED VIEW percentile_cont_desc_gby AS SELECT
                      id,
                      PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY value DESC) AS median_desc
                      FROM percentile_tbl
                      GROUP BY id"""
        self.data = [
            {"id": 0, "median_desc": 3.0},
            {"id": 1, "median_desc": 30.0},
        ]


# Edge case tests for PERCENTILE_CONT


class aggtst_percentile_cont_single_value(TstView):
    def __init__(self):
        # Test PERCENTILE_CONT with a single value
        self.sql = """CREATE MATERIALIZED VIEW percentile_cont_single AS SELECT
                      PERCENTILE_CONT(0.0) WITHIN GROUP (ORDER BY value) AS p0,
                      PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY value) AS p50,
                      PERCENTILE_CONT(1.0) WITHIN GROUP (ORDER BY value) AS p100
                      FROM percentile_single_tbl"""
        self.data = [{"p0": 42.0, "p50": 42.0, "p100": 42.0}]


class aggtst_percentile_cont_two_values(TstView):
    def __init__(self):
        # Test PERCENTILE_CONT interpolation with exactly two values (0 and 100)
        self.sql = """CREATE MATERIALIZED VIEW percentile_cont_two AS SELECT
                      PERCENTILE_CONT(0.0) WITHIN GROUP (ORDER BY value) AS p0,
                      PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY value) AS p25,
                      PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY value) AS p50,
                      PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY value) AS p75,
                      PERCENTILE_CONT(1.0) WITHIN GROUP (ORDER BY value) AS p100
                      FROM percentile_two_tbl"""
        self.data = [{"p0": 0.0, "p25": 25.0, "p50": 50.0, "p75": 75.0, "p100": 100.0}]


class aggtst_percentile_cont_duplicates(TstView):
    def __init__(self):
        # Test PERCENTILE_CONT with duplicate values
        self.sql = """CREATE MATERIALIZED VIEW percentile_cont_dups AS SELECT
                      PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY value) AS median
                      FROM percentile_dup_tbl"""
        # Values: [5, 5, 5, 10, 10] -> sorted, median should be 5 (middle value)
        self.data = [{"median": 5.0}]


class aggtst_percentile_cont_negative(TstView):
    def __init__(self):
        # Test PERCENTILE_CONT with negative values
        self.sql = """CREATE MATERIALIZED VIEW percentile_cont_neg AS SELECT
                      PERCENTILE_CONT(0.0) WITHIN GROUP (ORDER BY value) AS p0,
                      PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY value) AS p50,
                      PERCENTILE_CONT(1.0) WITHIN GROUP (ORDER BY value) AS p100
                      FROM percentile_neg_tbl"""
        # Values: [-20, -10, 0, 10, 20] -> median is 0
        self.data = [{"p0": -20.0, "p50": 0.0, "p100": 20.0}]


class aggtst_percentile_cont_with_nulls(TstView):
    def __init__(self):
        # Test PERCENTILE_CONT ignores NULL values
        self.sql = """CREATE MATERIALIZED VIEW percentile_cont_nulls AS SELECT
                      PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY value) AS median
                      FROM percentile_null_tbl"""
        # Values with nulls removed: [1, 2, 3] -> median is 2
        self.data = [{"median": 2.0}]


class aggtst_percentile_cont_near_extremes(TstView):
    def __init__(self):
        # Test PERCENTILE_CONT with percentiles very close to 0 and 1
        self.sql = """CREATE MATERIALIZED VIEW percentile_cont_extremes AS SELECT
                      PERCENTILE_CONT(0.01) WITHIN GROUP (ORDER BY value) AS p1,
                      PERCENTILE_CONT(0.99) WITHIN GROUP (ORDER BY value) AS p99
                      FROM percentile_tbl"""
        # With 10 values from 1-50, p1 and p99 should be near the extremes
        # p1 = 1 + 0.01 * 9 * (2-1) = 1.09 for id=0 part... but we have all 10 values
        # Formula: p = (1-f)*val[floor(idx)] + f*val[ceil(idx)] where idx = percentile*(n-1)
        # For n=10, idx for 0.01 = 0.01*9 = 0.09, so floor=0, ceil=1, f=0.09
        # p1 = (1-0.09)*1 + 0.09*2 = 0.91 + 0.18 = 1.09
        # For p99: idx = 0.99*9 = 8.91, floor=8, ceil=9, f=0.91
        # p99 = (1-0.91)*40 + 0.91*50 = 3.6 + 45.5 = 49.1
        self.data = [{"p1": Decimal("1.09"), "p99": Decimal("49.1")}]


class aggtst_percentile_cont_all_same(TstView):
    def __init__(self):
        # Test PERCENTILE_CONT when all values are identical
        self.sql = """CREATE MATERIALIZED VIEW percentile_cont_same AS SELECT
                      PERCENTILE_CONT(0.0) WITHIN GROUP (ORDER BY value) AS p0,
                      PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY value) AS p50,
                      PERCENTILE_CONT(1.0) WITHIN GROUP (ORDER BY value) AS p100
                      FROM percentile_same_tbl"""
        self.data = [{"p0": 7.0, "p50": 7.0, "p100": 7.0}]


class aggtst_percentile_cont_three_values(TstView):
    def __init__(self):
        # Test PERCENTILE_CONT with exactly three values (odd count)
        self.sql = """CREATE MATERIALIZED VIEW percentile_cont_three AS SELECT
                      PERCENTILE_CONT(0.0) WITHIN GROUP (ORDER BY value) AS p0,
                      PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY value) AS p25,
                      PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY value) AS p50,
                      PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY value) AS p75,
                      PERCENTILE_CONT(1.0) WITHIN GROUP (ORDER BY value) AS p100
                      FROM percentile_three_tbl"""
        # Values: [10, 20, 30]
        # p0 = 10, p100 = 30
        # p25: idx = 0.25*2 = 0.5, floor=0, ceil=1, f=0.5 -> (1-0.5)*10 + 0.5*20 = 15
        # p50: idx = 0.5*2 = 1.0, floor=1, ceil=1, f=0 -> 20
        # p75: idx = 0.75*2 = 1.5, floor=1, ceil=2, f=0.5 -> (1-0.5)*20 + 0.5*30 = 25
        self.data = [{"p0": 10.0, "p25": 15.0, "p50": 20.0, "p75": 25.0, "p100": 30.0}]


class aggtst_percentile_cont_four_values(TstView):
    def __init__(self):
        # Test PERCENTILE_CONT with exactly four values (even count)
        self.sql = """CREATE MATERIALIZED VIEW percentile_cont_four AS SELECT
                      PERCENTILE_CONT(0.0) WITHIN GROUP (ORDER BY value) AS p0,
                      PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY value) AS p25,
                      PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY value) AS p50,
                      PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY value) AS p75,
                      PERCENTILE_CONT(1.0) WITHIN GROUP (ORDER BY value) AS p100
                      FROM percentile_four_tbl"""
        # Values: [0, 10, 20, 30]
        # n=4, so max_idx = 3
        # p0 = 0, p100 = 30
        # p25: idx = 0.25*3 = 0.75, floor=0, ceil=1, f=0.75 -> (1-0.75)*0 + 0.75*10 = 7.5
        # p50: idx = 0.5*3 = 1.5, floor=1, ceil=2, f=0.5 -> (1-0.5)*10 + 0.5*20 = 15
        # p75: idx = 0.75*3 = 2.25, floor=2, ceil=3, f=0.25 -> (1-0.25)*20 + 0.25*30 = 22.5
        self.data = [{"p0": 0.0, "p25": 7.5, "p50": 15.0, "p75": 22.5, "p100": 30.0}]


class aggtst_percentile_cont_mixed_gby(TstView):
    def __init__(self):
        # Test GROUP BY with groups of different sizes
        self.sql = """CREATE MATERIALIZED VIEW percentile_cont_mixed_gby AS SELECT
                      grp,
                      PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY value) AS median
                      FROM percentile_mixed_tbl
                      GROUP BY grp"""
        # grp 'A': [10, 20, 30] -> median = 20
        # grp 'B': [100] -> median = 100
        # grp 'C': [1, 2, 3, 4, 5] -> median = 3
        self.data = [
            {"grp": "A", "median": 20.0},
            {"grp": "B", "median": 100.0},
            {"grp": "C", "median": 3.0},
        ]
