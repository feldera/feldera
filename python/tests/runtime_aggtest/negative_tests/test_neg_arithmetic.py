from tests.runtime_aggtest.aggtst_base import TstView


# Overflow in Arithmetic Operations


# Addition
class neg_add_tiny_int(TstView):
    def __init__(self):
        # checked manually
        self.sql = """CREATE MATERIALIZED VIEW add_tiny_int AS SELECT
                      tiny_int + tiny_int2 AS tiny_int
                      FROM numeric_tbl
                      WHERE id = 0"""
        self.expected_error = "'120 + 100' causes overflow for type TINYINT"


class neg_add_small_int(TstView):
    def __init__(self):
        # checked manually
        self.sql = """CREATE MATERIALIZED VIEW add_small_int AS SELECT
                      small_int + small_int2 AS small_int
                      FROM numeric_tbl
                      WHERE id = 0"""
        self.expected_error = "'32750 + 32700' causes overflow for type SMALLINT"


class neg_add_big_int(TstView):
    def __init__(self):
        # checked manually
        self.sql = """CREATE MATERIALIZED VIEW add_big_int AS SELECT
                      big_int + big_int2 AS big_int
                      FROM numeric_tbl
                      WHERE id = 0"""
        self.expected_error = "'8123302036854775807 + 9223372036854775807' causes overflow for type BIGINT"


class neg_add_intt(TstView):
    def __init__(self):
        # checked manually
        self.sql = """CREATE MATERIALIZED VIEW add_intt AS SELECT
                      intt + intt2 AS intt
                      FROM numeric_tbl
                      WHERE id = 0"""
        self.expected_error = (
            "'2147483647 + 2147483000' causes overflow for type INTEGER"
        )


class neg_add_tiny_un_int(TstView):
    def __init__(self):
        # checked manually
        self.sql = """CREATE MATERIALIZED VIEW add_tiny_un_int AS SELECT
                      tiny_int + tiny_int2 AS tiny_un_int
                      FROM numeric_un_tbl
                      WHERE id = 0"""
        self.expected_error = "'250 + 200' causes overflow for type TINYINT UNSIGNED"


class neg_add_small_un_int(TstView):
    def __init__(self):
        # checked manually
        self.sql = """CREATE MATERIALIZED VIEW add_small_int AS SELECT
                      small_int + small_int2 AS small_un_int
                      FROM numeric_un_tbl
                      WHERE id = 0"""
        self.expected_error = (
            "'65430 + 65435' causes overflow for type SMALLINT UNSIGNED"
        )


class neg_add_big_un_int(TstView):
    def __init__(self):
        # checked manually
        self.sql = """CREATE MATERIALIZED VIEW add_big_un_int AS SELECT
                      big_int + big_int2 AS big_un_int
                      FROM numeric_un_tbl
                      WHERE id = 0"""
        self.expected_error = "'12446742073709541615 + 18446742073709541615' causes overflow for type BIGINT UNSIGNED"


class neg_add_un_intt(TstView):
    def __init__(self):
        # checked manually
        self.sql = """CREATE MATERIALIZED VIEW add_un_intt AS SELECT
                      intt + intt2 AS un_intt
                      FROM numeric_un_tbl
                      WHERE id = 0"""
        self.expected_error = (
            "'4294966290 + 4292966290' causes overflow for type INTEGER UNSIGNED"
        )


# Multiplication
class neg_mul_tiny_int(TstView):
    def __init__(self):
        # checked manually
        self.sql = """CREATE MATERIALIZED VIEW mul_tiny_int AS SELECT
                      tiny_int * tiny_int2 AS tiny_int
                      FROM numeric_tbl
                      WHERE id = 0"""
        self.expected_error = "'120 * 100' causes overflow for type TINYINT"


class neg_mul_small_int(TstView):
    def __init__(self):
        # checked manually
        self.sql = """CREATE MATERIALIZED VIEW mul_small_int AS SELECT
                      small_int * small_int2 AS small_int
                      FROM numeric_tbl
                      WHERE id = 0"""
        self.expected_error = "'32750 * 32700' causes overflow for type SMALLINT"


class neg_mul_big_int(TstView):
    def __init__(self):
        # checked manually
        self.sql = """CREATE MATERIALIZED VIEW mul_big_int AS SELECT
                      big_int * big_int2 AS big_int
                      FROM numeric_tbl
                      WHERE id = 0"""
        self.expected_error = "'8123302036854775807 * 9223372036854775807' causes overflow for type BIGINT"


class neg_mul_intt(TstView):
    def __init__(self):
        # checked manually
        self.sql = """CREATE MATERIALIZED VIEW mul_intt AS SELECT
                      intt * intt2 AS intt
                      FROM numeric_tbl
                      WHERE id = 0"""
        self.expected_error = (
            "'2147483647 * 2147483000' causes overflow for type INTEGER"
        )


class neg_mul_tiny_un_int(TstView):
    def __init__(self):
        # checked manually
        self.sql = """CREATE MATERIALIZED VIEW mul_tiny_un_int AS SELECT
                      tiny_int * tiny_int2 AS tiny_un_int
                      FROM numeric_un_tbl
                      WHERE id = 0"""
        self.expected_error = "'250 * 200' causes overflow for type TINYINT UNSIGNED"


class neg_mul_small_un_int(TstView):
    def __init__(self):
        # checked manually
        self.sql = """CREATE MATERIALIZED VIEW mul_small_int AS SELECT
                      small_int * small_int2 AS small_un_int
                      FROM numeric_un_tbl
                      WHERE id = 0"""
        self.expected_error = (
            "'65430 * 65435' causes overflow for type SMALLINT UNSIGNED"
        )


class neg_mul_big_un_int(TstView):
    def __init__(self):
        # checked manually
        self.sql = """CREATE MATERIALIZED VIEW mul_big_un_int AS SELECT
                      big_int * big_int2 AS big_un_int
                      FROM numeric_un_tbl
                      WHERE id = 0"""
        self.expected_error = "'12446742073709541615 * 18446742073709541615' causes overflow for type BIGINT UNSIGNED"


class neg_mul_un_intt(TstView):
    def __init__(self):
        # checked manually
        self.sql = """CREATE MATERIALIZED VIEW mul_un_intt AS SELECT
                      intt * intt2 AS un_intt
                      FROM numeric_un_tbl
                      WHERE id = 0"""
        self.expected_error = (
            "'4294966290 * 4292966290' causes overflow for type INTEGER UNSIGNED"
        )


# Division
class neg_div_tiny_int(TstView):
    def __init__(self):
        # checked manually
        self.sql = """CREATE MATERIALIZED VIEW div_tiny_int AS SELECT
                      tiny_int / tiny_int2 AS tiny_int
                      FROM numeric_tbl
                      WHERE id = 1"""
        self.expected_error = "'-128 / -1' causes overflow for type TINYINT"


class neg_div_small_int(TstView):
    def __init__(self):
        # checked manually
        self.sql = """CREATE MATERIALIZED VIEW div_small_int AS SELECT
                      small_int / small_int2 AS small_int
                      FROM numeric_tbl
                      WHERE id = 1"""
        self.expected_error = "'-32768 / -1' causes overflow for type SMALLINT"


class neg_div_big_int(TstView):
    def __init__(self):
        # checked manually
        self.sql = """CREATE MATERIALIZED VIEW div_big_int AS SELECT
                      big_int / big_int2 AS big_int
                      FROM numeric_tbl
                      WHERE id = 1"""
        self.expected_error = (
            "'-9223372036854775808 / -1' causes overflow for type BIGINT"
        )


class neg_div_intt(TstView):
    def __init__(self):
        # checked manually
        self.sql = """CREATE MATERIALIZED VIEW div_intt AS SELECT
                      intt / intt2 AS intt
                      FROM numeric_tbl
                      WHERE id = 1"""
        self.expected_error = "'-2147483648 / -1' causes overflow for type INTEGER"


class neg_div_tiny_un_int(TstView):
    def __init__(self):
        # checked manually
        self.sql = """CREATE MATERIALIZED VIEW div_tiny_un_int AS SELECT
                      tiny_int / 0::TINYINT UNSIGNED AS tiny_un_int
                      FROM numeric_un_tbl"""
        self.expected_error = "'250 / 0' causes overflow for type TINYINT UNSIGNED"


class neg_div_small_un_int(TstView):
    def __init__(self):
        # checked manually
        self.sql = """CREATE MATERIALIZED VIEW div_small_un_int AS SELECT
                      small_int / 0::SMALLINT UNSIGNED AS small_un_int
                      FROM numeric_un_tbl"""
        self.expected_error = "'65430 / 0' causes overflow for type SMALLINT UNSIGNED"


class neg_div_big_un_int(TstView):
    def __init__(self):
        # checked manually
        self.sql = """CREATE MATERIALIZED VIEW div_big_un_int AS SELECT
                      big_int / 0::BIGINT UNSIGNED AS big_un_int
                      FROM numeric_un_tbl"""
        self.expected_error = (
            "'12446742073709541615 / 0' causes overflow for type BIGINT UNSIGNED"
        )


class neg_div_un_intt(TstView):
    def __init__(self):
        # checked manually
        self.sql = """CREATE MATERIALIZED VIEW div_un_intt AS SELECT
                      intt / 0::INTEGER UNSIGNED AS un_intt
                      FROM numeric_un_tbl"""
        self.expected_error = (
            "'4294966290 / 0' causes overflow for type INTEGER UNSIGNED"
        )


# Subtraction
class neg_sub_tiny_int(TstView):
    def __init__(self):
        # checked manually
        self.sql = """CREATE MATERIALIZED VIEW sub_tiny_int AS SELECT
                      - tiny_int - tiny_int2 AS tiny_int
                      FROM numeric_tbl
                      WHERE id = 0"""
        self.expected_error = "'-120 - 100' causes overflow for type TINYINT"


class neg_sub_small_int(TstView):
    def __init__(self):
        # checked manually
        self.sql = """CREATE MATERIALIZED VIEW sub_small_int AS SELECT
                      - small_int - small_int2 AS small_int
                      FROM numeric_tbl
                      WHERE id = 0"""
        self.expected_error = "'-32750 - 32700' causes overflow for type SMALLINT"


class neg_sub_big_int(TstView):
    def __init__(self):
        # checked manually
        self.sql = """CREATE MATERIALIZED VIEW sub_big_int AS SELECT
                      - big_int - big_int2 AS big_int
                      FROM numeric_tbl
                      WHERE id = 0"""
        self.expected_error = "'-8123302036854775807 - 9223372036854775807' causes overflow for type BIGINT"


class neg_sub_intt(TstView):
    def __init__(self):
        # checked manually
        self.sql = """CREATE MATERIALIZED VIEW sub_intt AS SELECT
                      - intt - intt2 AS intt
                      FROM numeric_tbl
                      WHERE id = 0"""
        self.expected_error = (
            "'-2147483647 - 2147483000' causes overflow for type INTEGER"
        )


class neg_sub_tiny_un_int(TstView):
    def __init__(self):
        # checked manually
        self.sql = """CREATE MATERIALIZED VIEW sub_tiny_un_int AS SELECT
                      tiny_int2 - tiny_int AS tiny_un_int
                      FROM numeric_un_tbl
                      WHERE id = 0"""
        self.expected_error = "'200 - 250' causes overflow for type TINYINT UNSIGNED"


class neg_sub_small_un_int(TstView):
    def __init__(self):
        # checked manually
        self.sql = """CREATE MATERIALIZED VIEW sub_small_int AS SELECT
                      small_int - small_int2 AS small_un_int
                      FROM numeric_un_tbl
                      WHERE id = 0"""
        self.expected_error = (
            "'65430 - 65435' causes overflow for type SMALLINT UNSIGNED"
        )


class neg_sub_big_un_int(TstView):
    def __init__(self):
        # checked manually
        self.sql = """CREATE MATERIALIZED VIEW sub_big_un_int AS SELECT
                      big_int - big_int2 AS big_un_int
                      FROM numeric_un_tbl
                      WHERE id = 0"""
        self.expected_error = "'12446742073709541615 - 18446742073709541615' causes overflow for type BIGINT UNSIGNED"


class neg_sub_un_intt(TstView):
    def __init__(self):
        # checked manually
        self.sql = """CREATE MATERIALIZED VIEW sub_un_intt AS SELECT
                      intt2 - intt AS un_intt
                      FROM numeric_un_tbl
                      WHERE id = 0"""
        self.expected_error = (
            "'4292966290 - 4294966290' causes overflow for type INTEGER UNSIGNED"
        )


# MOD
class neg_mod_tiny_int(TstView):
    def __init__(self):
        # checked manually
        self.sql = """CREATE MATERIALIZED VIEW mod_tiny_int AS SELECT
                      tiny_int % 0::TINYINT AS tiny_int
                      FROM numeric_tbl
                      WHERE id = 1"""
        self.expected_error = (
            "attempt to calculate the remainder with a divisor of zero"
        )


class neg_mod_small_int(TstView):
    def __init__(self):
        # checked manually
        self.sql = """CREATE MATERIALIZED VIEW mod_small_int AS SELECT
                      small_int % 0::SMALLINT AS small_int
                      FROM numeric_tbl
                      WHERE id = 1"""
        self.expected_error = (
            "attempt to calculate the remainder with a divisor of zero"
        )


class neg_mod_big_int(TstView):
    def __init__(self):
        # checked manually
        self.sql = """CREATE MATERIALIZED VIEW mod_big_int AS SELECT
                      big_int % 0::BIGINT AS big_int
                      FROM numeric_tbl
                      WHERE id = 1"""
        self.expected_error = (
            "attempt to calculate the remainder with a divisor of zero"
        )


class neg_mod_intt(TstView):
    def __init__(self):
        # checked manually
        self.sql = """CREATE MATERIALIZED VIEW mod_intt AS SELECT
                      intt % 0::INTEGER AS intt
                      FROM numeric_tbl
                      WHERE id = 1"""
        self.expected_error = (
            "attempt to calculate the remainder with a divisor of zero"
        )


class neg_mod_tiny_un_int(TstView):
    def __init__(self):
        # checked manually
        self.sql = """CREATE MATERIALIZED VIEW mod_tiny_un_int AS SELECT
                      tiny_int % 0::TINYINT UNSIGNED AS tiny_un_int
                      FROM numeric_un_tbl"""
        self.expected_error = (
            "attempt to calculate the remainder with a divisor of zero"
        )


class neg_mod_small_un_int(TstView):
    def __init__(self):
        # checked manually
        self.sql = """CREATE MATERIALIZED VIEW mod_small_un_int AS SELECT
                      small_int % 0::SMALLINT UNSIGNED AS small_un_int
                      FROM numeric_un_tbl"""
        self.expected_error = (
            "attempt to calculate the remainder with a divisor of zero"
        )


class neg_mod_big_un_int(TstView):
    def __init__(self):
        # checked manually
        self.sql = """CREATE MATERIALIZED VIEW mod_big_un_int AS SELECT
                      big_int % 0::BIGINT UNSIGNED AS big_un_int
                      FROM numeric_un_tbl"""
        self.expected_error = (
            "attempt to calculate the remainder with a divisor of zero"
        )


class neg_mod_un_intt(TstView):
    def __init__(self):
        # checked manually
        self.sql = """CREATE MATERIALIZED VIEW mod_un_intt AS SELECT
                      intt % 0::INTEGER UNSIGNED AS un_intt
                      FROM numeric_un_tbl"""
        self.expected_error = (
            "attempt to calculate the remainder with a divisor of zero"
        )
