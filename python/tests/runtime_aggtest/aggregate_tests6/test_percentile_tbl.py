from tests.runtime_aggtest.aggtst_base import TstTable


class aggtst_percentile_table(TstTable):
    """Define the table used by percentile tests"""

    def __init__(self):
        self.sql = """CREATE TABLE percentile_tbl(
                      id INT, value DOUBLE
                      )"""
        self.data = [
            {"id": 0, "value": 1.0},
            {"id": 0, "value": 2.0},
            {"id": 0, "value": 3.0},
            {"id": 0, "value": 4.0},
            {"id": 0, "value": 5.0},
            {"id": 1, "value": 10.0},
            {"id": 1, "value": 20.0},
            {"id": 1, "value": 30.0},
            {"id": 1, "value": 40.0},
            {"id": 1, "value": 50.0},
        ]


# Edge case tables for percentile tests


class aggtst_percentile_single_table(TstTable):
    """Table with a single value for edge case testing"""

    def __init__(self):
        self.sql = """CREATE TABLE percentile_single_tbl(value DOUBLE)"""
        self.data = [{"value": 42.0}]


class aggtst_percentile_two_table(TstTable):
    """Table with exactly two values for interpolation edge case"""

    def __init__(self):
        self.sql = """CREATE TABLE percentile_two_tbl(value DOUBLE)"""
        self.data = [
            {"value": 0.0},
            {"value": 100.0},
        ]


class aggtst_percentile_three_table(TstTable):
    """Table with exactly three values (odd count)"""

    def __init__(self):
        self.sql = """CREATE TABLE percentile_three_tbl(value DOUBLE)"""
        self.data = [
            {"value": 10.0},
            {"value": 20.0},
            {"value": 30.0},
        ]


class aggtst_percentile_four_table(TstTable):
    """Table with exactly four values (even count)"""

    def __init__(self):
        self.sql = """CREATE TABLE percentile_four_tbl(value DOUBLE)"""
        self.data = [
            {"value": 0.0},
            {"value": 10.0},
            {"value": 20.0},
            {"value": 30.0},
        ]


class aggtst_percentile_dup_table(TstTable):
    """Table with duplicate values"""

    def __init__(self):
        self.sql = """CREATE TABLE percentile_dup_tbl(value DOUBLE)"""
        self.data = [
            {"value": 5.0},
            {"value": 5.0},
            {"value": 5.0},
            {"value": 10.0},
            {"value": 10.0},
        ]


class aggtst_percentile_neg_table(TstTable):
    """Table with negative values"""

    def __init__(self):
        self.sql = """CREATE TABLE percentile_neg_tbl(value DOUBLE)"""
        self.data = [
            {"value": -20.0},
            {"value": -10.0},
            {"value": 0.0},
            {"value": 10.0},
            {"value": 20.0},
        ]


class aggtst_percentile_null_table(TstTable):
    """Table with NULL values mixed in"""

    def __init__(self):
        self.sql = """CREATE TABLE percentile_null_tbl(value DOUBLE)"""
        self.data = [
            {"value": None},
            {"value": 1.0},
            {"value": None},
            {"value": 2.0},
            {"value": 3.0},
            {"value": None},
        ]


class aggtst_percentile_same_table(TstTable):
    """Table with all identical values"""

    def __init__(self):
        self.sql = """CREATE TABLE percentile_same_tbl(value DOUBLE)"""
        self.data = [
            {"value": 7.0},
            {"value": 7.0},
            {"value": 7.0},
            {"value": 7.0},
            {"value": 7.0},
        ]


class aggtst_percentile_mixed_table(TstTable):
    """Table with groups of different sizes for GROUP BY testing"""

    def __init__(self):
        self.sql = """CREATE TABLE percentile_mixed_tbl(grp VARCHAR, value DOUBLE)"""
        self.data = [
            # Group A: 3 values
            {"grp": "A", "value": 10.0},
            {"grp": "A", "value": 20.0},
            {"grp": "A", "value": 30.0},
            # Group B: 1 value
            {"grp": "B", "value": 100.0},
            # Group C: 5 values
            {"grp": "C", "value": 1.0},
            {"grp": "C", "value": 2.0},
            {"grp": "C", "value": 3.0},
            {"grp": "C", "value": 4.0},
            {"grp": "C", "value": 5.0},
        ]
