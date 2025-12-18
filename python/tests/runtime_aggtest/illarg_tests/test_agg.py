from tests.runtime_aggtest.aggtst_base import TstView


# ARRAY_AGG
class illarg_arr_agg_illegal(TstView):
    def __init__(self):
        # checked manually
        self.sql = """CREATE MATERIALIZED VIEW illarg_arr_agg_illegal AS SELECT
                      ARRAY_AGG(intt, intt) AS arr
                      FROM illegal_tbl
                      WHERE id = 0"""
        self.expected_error = "Invalid number of arguments to function 'ARRAY_AGG'. Was expecting 1 arguments"


# AVG
class illarg_avg_illegal(TstView):
    def __init__(self):
        # checked manually
        self.sql = """CREATE MATERIALIZED VIEW illarg_avg_illegal AS SELECT
                      AVG(booll) AS arr
                      FROM illegal_tbl
                      WHERE id = 0"""
        self.expected_error = "Cannot apply 'AVG' to arguments of type"


# ARG_MAX
class illarg_arg_max_illegal(TstView):
    def __init__(self):
        # checked manually
        self.sql = """CREATE MATERIALIZED VIEW illarg_arg_max_illegal AS SELECT
                      ARG_MAX(udt) AS udt
                      FROM illegal_tbl"""
        self.expected_error = "Invalid number of arguments to function 'ARG_MAX'. Was expecting 2 arguments"


# ARG_MIN
class illarg_arg_min_illegal(TstView):
    def __init__(self):
        # checked manually
        self.sql = """CREATE MATERIALIZED VIEW illarg_arg_min_illegal AS SELECT
                      ARG_MIN(roww, roww, roww) AS roww
                      FROM illegal_tbl"""
        self.expected_error = "Invalid number of arguments to function 'ARG_MIN'. Was expecting 2 arguments"


# BIT_AND
class illarg_bit_and_illegal(TstView):
    def __init__(self):
        # checked manually
        self.sql = """CREATE MATERIALIZED VIEW illarg_bit_and_illegal AS SELECT
                      BIT_AND(roww) AS roww
                      FROM illegal_tbl"""
        self.expected_error = "Cannot apply 'BIT_AND' to arguments of type "


# BIT_OR
class illarg_bit_or_illegal(TstView):
    def __init__(self):
        self.sql = """CREATE MATERIALIZED VIEW illarg_bit_or_illegal AS SELECT
                      BIT_OR(udt) AS udt
                      FROM illegal_tbl"""
        self.expected_error = "Cannot apply 'BIT_OR' to arguments of type "


# BIT_XOR
class illarg_bit_xor_illegal(TstView):
    def __init__(self):
        self.sql = """CREATE MATERIALIZED VIEW illarg_bit_xor_illegal AS SELECT
                      BIT_XOR(udt) AS udt
                      FROM illegal_tbl"""
        self.expected_error = "Cannot apply 'BIT_XOR' to arguments of type "


# COUNT
class illarg_count_illegal(TstView):
    def __init__(self):
        # checked manually
        self.sql = """CREATE MATERIALIZED VIEW illarg_count_illegal AS SELECT
                      COUNT(udt, COUNT(intt)) AS udt
                      FROM illegal_tbl"""
        self.expected_error = "Aggregate expressions cannot be nested"


# COUNTIF
class illarg_countif_illegal(TstView):
    def __init__(self):
        # checked manually
        self.sql = """CREATE MATERIALIZED VIEW illarg_countif_illegal AS SELECT
                      COUNTIF(reall < 0.2, decimall > 2) AS reall
                      FROM illegal_tbl"""
        self.expected_error = "invalid number of arguments to function 'countif'"


# EVERY
class illarg_every_illegal(TstView):
    def __init__(self):
        # checked manually
        self.sql = """CREATE MATERIALIZED VIEW illarg_every_illegal AS SELECT
                      EVERY(intt) AS intt
                      FROM illegal_tbl"""
        self.expected_error = "Cannot apply 'EVERY' to arguments of type 'EVERY(<INTEGER>)'. Supported form(s): 'EVERY(<BOOLEAN>)'"


# SOME
class illarg_some_illegal(TstView):
    def __init__(self):
        # checked manually
        self.sql = """CREATE MATERIALIZED VIEW illarg_some_illegal AS SELECT
                      SOME(intt) AS intt
                      FROM illegal_tbl"""
        self.expected_error = "Cannot apply 'SOME' to arguments of type 'SOME(<INTEGER>)'. Supported form(s): 'SOME(<BOOLEAN>)'"


# BOOL_AND
class illarg_bool_and_illegal(TstView):
    def __init__(self):
        # checked manually
        self.sql = """CREATE MATERIALIZED VIEW illarg_bool_and_illegal AS SELECT
                      BOOL_AND(udt[2] IS NOT NULL, arr[2] IS NOT NULL) AS udt
                      FROM illegal_tbl"""
        self.expected_error = "Invalid number of arguments to function 'BOOL_AND'. Was expecting 1 arguments"


# BOOL_OR
class illarg_bool_or_illegal(TstView):
    def __init__(self):
        # checked manually
        self.sql = """CREATE MATERIALIZED VIEW illarg_bool_or_illegal AS SELECT
                      BOOL_OR(arr[2] IS NOT NULL, arr[2] IS NOT NULL) AS arr
                      FROM illegal_tbl"""
        self.expected_error = "Invalid number of arguments to function 'BOOL_OR'. Was expecting 1 arguments"


# MAX
class illarg_max_illegal(TstView):
    def __init__(self):
        # checked manually
        self.sql = """CREATE MATERIALIZED VIEW illarg_max_illegal AS SELECT
                      MAX(udt[2], arr[2]) AS udt
                      FROM illegal_tbl"""
        self.expected_error = (
            "Invalid number of arguments to function 'MAX'. Was expecting 1 arguments"
        )


# MIN
class illarg_min_illegal(TstView):
    def __init__(self):
        # checked manually
        self.sql = """CREATE MATERIALIZED VIEW illarg_min_illegal AS SELECT
                      MIN(udt[2], arr[2]) AS udt
                      FROM illegal_tbl"""
        self.expected_error = (
            "Invalid number of arguments to function 'MIN'. Was expecting 1 arguments"
        )


# SUM
class illarg_sum_illegal(TstView):
    def __init__(self):
        # checked manually
        self.sql = """CREATE MATERIALIZED VIEW illarg_sum_illegal AS SELECT
                      SUM(udt) AS udt
                      FROM illegal_tbl"""
        self.expected_error = "Cannot apply 'SUM' to arguments of type "


class illarg_sum_illegal1(TstView):
    def __init__(self):
        # checked manually
        self.sql = """CREATE MATERIALIZED VIEW illarg_sum_illegal1 AS SELECT
                      SUM(str) AS str
                      FROM illegal_tbl"""
        self.expected_error = "Cannot parse hello  into a DECIMAL(38, 19)"


# STDDEV
class illarg_stddev_illegal(TstView):
    def __init__(self):
        # checked manually
        self.sql = """CREATE MATERIALIZED VIEW illarg_stddev_illegal AS SELECT
                      STDDEV(roww) AS roww
                      FROM illegal_tbl"""
        self.expected_error = "Cannot apply 'STDDEV' to arguments of type "


# STDDEV_POP
class illarg_stddev_pop_illegal(TstView):
    def __init__(self):
        # checked manually
        self.sql = """CREATE MATERIALIZED VIEW illarg_stddev_pop_illegal AS SELECT
                      STDDEV_POP(roww) AS roww
                      FROM illegal_tbl"""
        self.expected_error = "Cannot apply 'STDDEV_POP' to arguments of type "
