from tests.aggregate_tests.aggtst_base import TstView


# Overfllow in Arithmetic Operations


# Addition
class neg_add_tiny_int(TstView):
    def __init__(self):
        # checked manually
        self.sql = """CREATE MATERIALIZED VIEW add_tiny_int AS SELECT
                      a.tiny_int + b.tiny_int AS tiny_int
                      FROM numeric_tbl a
                      JOIN numeric_tbl b
                      ON a.id = 0 AND b.id = 1"""
        self.expected_error = "panic message: '120 + 100' causes overflow"


class neg_add_small_int(TstView):
    def __init__(self):
        # checked manually
        self.sql = """CREATE MATERIALIZED VIEW add_small_int AS SELECT
                      a.small_int + b.small_int AS small_int
                      FROM numeric_tbl a
                      JOIN numeric_tbl b
                      ON a.id = 0 AND b.id = 1"""
        self.expected_error = "panic message: '32750 + 32700' causes overflow"


class neg_add_big_int(TstView):
    def __init__(self):
        # checked manually
        self.sql = """CREATE MATERIALIZED VIEW add_big_int AS SELECT
                      a.big_int + b.big_int AS big_int
                      FROM numeric_tbl a
                      JOIN numeric_tbl b
                      ON a.id = 0 AND b.id = 1"""
        self.expected_error = (
            "panic message: '8123302036854775807 + 9223372036854775807' causes overflow"
        )


class neg_add_intt(TstView):
    def __init__(self):
        # checked manually
        self.sql = """CREATE MATERIALIZED VIEW add_intt AS SELECT
                      a.intt + b.intt AS intt
                      FROM numeric_tbl a
                      JOIN numeric_tbl b
                      ON a.id = 0 AND b.id = 1"""
        self.expected_error = "panic message: '2147483647 + 2147483000' causes overflow"


class neg_add_tiny_un_int(TstView):
    def __init__(self):
        # checked manually
        self.sql = """CREATE MATERIALIZED VIEW add_tiny_un_int AS SELECT
                      a.tiny_int + b.tiny_int AS tiny_un_int
                      FROM numeric_un_tbl a
                      JOIN numeric_un_tbl b
                      ON a.id = 0 AND b.id = 1"""
        self.expected_error = "panic message: '250 + 200' causes overflow"


class neg_add_small_un_int(TstView):
    def __init__(self):
        # checked manually
        self.sql = """CREATE MATERIALIZED VIEW add_small_int AS SELECT
                      a.small_int + b.small_int AS small_un_int
                      FROM numeric_un_tbl a
                      JOIN numeric_un_tbl b
                      ON a.id = 0 AND b.id = 1"""
        self.expected_error = "panic message: '65430 + 65435' causes overflow"


class neg_add_big_un_int(TstView):
    def __init__(self):
        # checked manually
        self.sql = """CREATE MATERIALIZED VIEW add_big_un_int AS SELECT
                      a.big_int + b.big_int AS big_un_int
                      FROM numeric_un_tbl a
                      JOIN numeric_un_tbl b
                      ON a.id = 0 AND b.id = 1"""
        self.expected_error = "panic message: '12446742073709541615 + 18446742073709541615' causes overflow"


class neg_add_un_intt(TstView):
    def __init__(self):
        # checked manually
        self.sql = """CREATE MATERIALIZED VIEW add_un_intt AS SELECT
                      a.intt + b.intt AS un_intt
                      FROM numeric_un_tbl a
                      JOIN numeric_un_tbl b
                      ON a.id = 0 AND b.id = 1"""
        self.expected_error = "panic message: '4294966290 + 4292966290' causes overflow"


# Multiplication
class neg_mul_tiny_int(TstView):
    def __init__(self):
        # checked manually
        self.sql = """CREATE MATERIALIZED VIEW mul_tiny_int AS SELECT
                      a.tiny_int * b.tiny_int AS tiny_int
                      FROM numeric_tbl a
                      JOIN numeric_tbl b
                      ON a.id = 0 AND b.id = 1"""
        self.expected_error = "panic message: '120 * 100' causes overflow"


class neg_mul_small_int(TstView):
    def __init__(self):
        # checked manually
        self.sql = """CREATE MATERIALIZED VIEW mul_small_int AS SELECT
                      a.small_int * b.small_int AS small_int
                      FROM numeric_tbl a
                      JOIN numeric_tbl b
                      ON a.id = 0 AND b.id = 1"""
        self.expected_error = "panic message: '32750 * 32700' causes overflow"


class neg_mul_big_int(TstView):
    def __init__(self):
        # checked manually
        self.sql = """CREATE MATERIALIZED VIEW mul_big_int AS SELECT
                      a.big_int * b.big_int AS big_int
                      FROM numeric_tbl a
                      JOIN numeric_tbl b
                      ON a.id = 0 AND b.id = 1"""
        self.expected_error = (
            "panic message: '8123302036854775807 * 9223372036854775807' causes overflow"
        )


class neg_mul_intt(TstView):
    def __init__(self):
        # checked manually
        self.sql = """CREATE MATERIALIZED VIEW mul_intt AS SELECT
                      a.intt * b.intt AS intt
                      FROM numeric_tbl a
                      JOIN numeric_tbl b
                      ON a.id = 0 AND b.id = 1"""
        self.expected_error = "panic message: '2147483647 * 2147483000' causes overflow"


class neg_mul_tiny_un_int(TstView):
    def __init__(self):
        # checked manually
        self.sql = """CREATE MATERIALIZED VIEW mul_tiny_un_int AS SELECT
                      a.tiny_int * b.tiny_int AS tiny_un_int
                      FROM numeric_un_tbl a
                      JOIN numeric_un_tbl b
                      ON a.id = 0 AND b.id = 1"""
        self.expected_error = "panic message: '250 * 200' causes overflow"


class neg_mul_small_un_int(TstView):
    def __init__(self):
        # checked manually
        self.sql = """CREATE MATERIALIZED VIEW mul_small_int AS SELECT
                      a.small_int * b.small_int AS small_un_int
                      FROM numeric_un_tbl a
                      JOIN numeric_un_tbl b
                      ON a.id = 0 AND b.id = 1"""
        self.expected_error = "panic message: '65430 * 65435' causes overflow"


class neg_mul_big_un_int(TstView):
    def __init__(self):
        # checked manually
        self.sql = """CREATE MATERIALIZED VIEW mul_big_un_int AS SELECT
                      a.big_int * b.big_int AS big_un_int
                      FROM numeric_un_tbl a
                      JOIN numeric_un_tbl b
                      ON a.id = 0 AND b.id = 1"""
        self.expected_error = "panic message: '12446742073709541615 * 18446742073709541615' causes overflow"


class neg_mul_un_intt(TstView):
    def __init__(self):
        # checked manually
        self.sql = """CREATE MATERIALIZED VIEW mul_un_intt AS SELECT
                      a.intt * b.intt AS un_intt
                      FROM numeric_un_tbl a
                      JOIN numeric_un_tbl b
                      ON a.id = 0 AND b.id = 1"""
        self.expected_error = "panic message: '4294966290 * 4292966290' causes overflow"
