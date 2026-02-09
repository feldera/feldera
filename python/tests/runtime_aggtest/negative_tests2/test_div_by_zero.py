from tests.runtime_aggtest.aggtst_base import TstView


# Modulo
class neg_mod_tiny_int(TstView):
    def __init__(self):
        # checked manually
        self.sql = """CREATE MATERIALIZED VIEW mod_tiny_int AS SELECT
                      tiny_int % 0 AS tiny_int
                      FROM numeric_tbl
                      WHERE id = 1"""
        self.expected_error = (
            "attempt to calculate the remainder with a divisor of zero"
        )


class neg_mod_small_int(TstView):
    def __init__(self):
        # checked manually
        self.sql = """CREATE MATERIALIZED VIEW mod_small_int AS SELECT
                      small_int % 0 AS small_int
                      FROM numeric_tbl
                      WHERE id = 1"""
        self.expected_error = (
            "attempt to calculate the remainder with a divisor of zero"
        )


class neg_mod_big_int(TstView):
    def __init__(self):
        # checked manually
        self.sql = """CREATE MATERIALIZED VIEW mod_big_int AS SELECT
                      big_int % 0 AS big_int
                      FROM numeric_tbl
                      WHERE id = 1"""
        self.expected_error = (
            "attempt to calculate the remainder with a divisor of zero"
        )


class neg_mod_intt(TstView):
    def __init__(self):
        # checked manually
        self.sql = """CREATE MATERIALIZED VIEW mod_intt AS SELECT
                      intt % 0 AS intt
                      FROM numeric_tbl
                      WHERE id = 1"""
        self.expected_error = (
            "attempt to calculate the remainder with a divisor of zero"
        )


class neg_mod_tiny_un_int(TstView):
    def __init__(self):
        # checked manually
        self.sql = """CREATE MATERIALIZED VIEW mod_tiny_un_int AS SELECT
                      tiny_int % 0 AS tiny_un_int
                      FROM numeric_un_tbl"""
        self.expected_error = (
            "attempt to calculate the remainder with a divisor of zero"
        )


class neg_mod_small_un_int(TstView):
    def __init__(self):
        # checked manually
        self.sql = """CREATE MATERIALIZED VIEW mod_small_int AS SELECT
                      small_int % 0 AS small_un_int
                      FROM numeric_un_tbl"""
        self.expected_error = (
            "attempt to calculate the remainder with a divisor of zero"
        )


class neg_mod_big_un_int(TstView):
    def __init__(self):
        # checked manually
        self.sql = """CREATE MATERIALIZED VIEW mod_big_un_int AS SELECT
                      big_int % 0 AS big_un_int
                      FROM numeric_un_tbl"""
        self.expected_error = (
            "attempt to calculate the remainder with a divisor of zero"
        )


class neg_mod_un_intt(TstView):
    def __init__(self):
        # checked manually
        self.sql = """CREATE MATERIALIZED VIEW mod_un_intt AS SELECT
                      intt % 0 AS un_intt
                      FROM numeric_un_tbl"""
        self.expected_error = (
            "attempt to calculate the remainder with a divisor of zero"
        )
