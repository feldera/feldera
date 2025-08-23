from tests.runtime_aggtest.aggtst_base import TstView
from decimal import Decimal


# Float and Decimal type tests
# ABS function
class illarg_abs_legal(TstView):
    def __init__(self):
        # checked manually
        self.data = [{"dbl": Decimal("0.82711234601246"), "decimall": Decimal("0.52")}]
        self.sql = """CREATE MATERIALIZED VIEW abs_legal AS SELECT
                      ABS(dbl) AS dbl,
                      ABS(decimall) AS decimall
                      FROM illegal_tbl
                      WHERE id = 1"""


class illarg_abs_cast_legal(TstView):
    def __init__(self):
        # checked manually
        self.data = [{"arr": Decimal("0.14")}]
        self.sql = """CREATE MATERIALIZED VIEW abs_cast_legal AS SELECT
                      ABS(ARR[1]) AS arr
                      FROM illegal_tbl
                      WHERE id = 1"""


# Negative Test
class illarg_abs_illegal(TstView):
    def __init__(self):
        # checked manually
        self.sql = """CREATE MATERIALIZED VIEW abs_illegal AS SELECT
                      ABS(booll) AS booll
                      FROM illegal_tbl"""
        self.expected_error = "Cannot apply 'ABS' to arguments of type"


# ACOS function
class illarg_acos_legal(TstView):
    def __init__(self):
        # Validated on Postgres
        self.data = [{"dbl": "2.544746576714120"}]
        self.sql = """CREATE MATERIALIZED VIEW aocs_legal AS SELECT
                      CAST(ACOS(dbl) AS VARCHAR(17)) AS dbl
                      FROM illegal_tbl
                      WHERE id = 1"""


class illarg_acos_cast_legal(TstView):
    def __init__(self):
        # Validated on Postgres
        self.data = [{"intt": "3.141592653589793"}]
        self.sql = """CREATE MATERIALIZED VIEW acos_cast_legal AS SELECT
                      CAST(ACOS(intt) AS VARCHAR(17)) AS intt
                      FROM illegal_tbl
                      WHERE id = 1"""


# Negative Test
class illarg_acos_illegal(TstView):
    def __init__(self):
        # checked manually
        self.sql = """CREATE MATERIALIZED VIEW acos_illegal AS SELECT
                      ACOS(bin) AS bin
                      FROM illegal_tbl"""
        self.expected_error = "Cannot apply 'ACOS' to arguments of type"


# ACOSH function
class illarg_acosh_legal(TstView):
    def __init__(self):
        # Validated on Postgres
        self.data = [{"dbl": "4.337672106130182"}]
        self.sql = """CREATE MATERIALIZED VIEW acosh_legal AS SELECT
                      CAST(ACOSH(- dbl) AS VARCHAR(17)) AS dbl
                      FROM illegal_tbl
                      WHERE id = 0"""


class illarg_acosh_cast_legal(TstView):
    def __init__(self):
        # Validated on Postgres
        self.data = [{"arr": "3.330926552641251"}]
        self.sql = """CREATE MATERIALIZED VIEW acosh_cast_legal AS SELECT
                      CAST(ACOSH(ARR[2]) AS VARCHAR(17)) AS arr
                      FROM illegal_tbl
                      WHERE id = 0"""


# Negative Test
class illarg_acosh_illegal(TstView):
    def __init__(self):
        # checked manually
        self.sql = """CREATE MATERIALIZED VIEW acosh_illegal AS SELECT
                      ACOSH(tmestmp) AS tmestmp
                      FROM illegal_tbl"""
        self.expected_error = "Cannot apply 'ACOSH' to arguments of type"


# ASIN function
class illarg_asin_legal(TstView):
    def __init__(self):
        # Validated on Postgres
        self.data = [{"dbl": "-0.973950249919223"}]
        self.sql = """CREATE MATERIALIZED VIEW asin_legal AS SELECT
                      CAST(ASIN(dbl) AS VARCHAR(18)) AS dbl
                      FROM illegal_tbl
                      WHERE id = 1"""


class illarg_asin_cast_legal(TstView):
    def __init__(self):
        # Validated on Postgres
        self.data = [{"intt": "-1.570796326794896"}]
        self.sql = """CREATE MATERIALIZED VIEW asin_cast_legal AS SELECT
                      CAST(ASIN(intt) AS VARCHAR(18)) AS intt
                      FROM illegal_tbl
                      WHERE id = 1"""


# Negative Test
class illarg_asin_illegal(TstView):
    def __init__(self):
        # checked manually
        self.sql = """CREATE MATERIALIZED VIEW asin_illegal AS SELECT
                      ASIN(booll) AS booll
                      FROM illegal_tbl"""
        self.expected_error = "Cannot apply 'ASIN' to arguments of type"


# ASINH function
class illarg_asinh_legal(TstView):
    def __init__(self):
        # Validated on Postgres
        self.data = [{"dbl": "4.338013477915945"}]
        self.sql = """CREATE MATERIALIZED VIEW asinh_legal AS SELECT
                      CAST(ASINH(- dbl) AS VARCHAR(17)) AS dbl
                      FROM illegal_tbl
                      WHERE id = 0"""


class illarg_asinhh_cast_legal(TstView):
    def __init__(self):
        # Validated on Postgres
        self.data = [{"arr": "3.333477586883992"}]
        self.sql = """CREATE MATERIALIZED VIEW asinh_cast_legal AS SELECT
                      CAST(ASINH(ARR[2]) AS VARCHAR(17)) AS arr
                      FROM illegal_tbl
                      WHERE id = 0"""


# Negative Test
class illarg_asinh_illegal(TstView):
    def __init__(self):
        # checked manually
        self.sql = """CREATE MATERIALIZED VIEW asinh_illegal AS SELECT
                      ASINH(uuidd) AS uuidd
                      FROM illegal_tbl"""
        self.expected_error = "Cannot apply 'ASINH' to arguments of type"


# ATAN function
class illarg_atan_legal(TstView):
    def __init__(self):
        # Validated on Postgres
        self.data = [{"dbl": "-0.691055623356933"}]
        self.sql = """CREATE MATERIALIZED VIEW atan_legal AS SELECT
                      CAST(ATAN(dbl) AS VARCHAR(18)) AS dbl
                      FROM illegal_tbl
                      WHERE id = 1"""


class illarg_atan_cast_legal(TstView):
    def __init__(self):
        # Validated on Postgres
        self.data = [{"arr": "-0.139095941482071"}]
        self.sql = """CREATE MATERIALIZED VIEW atan_cast_legal AS SELECT
                      CAST(ATAN(ARR[1]) AS VARCHAR(18)) AS arr
                      FROM illegal_tbl
                      WHERE id = 1"""


# Negative Test
class illarg_atan_illegal(TstView):
    def __init__(self):
        # checked manually
        self.sql = """CREATE MATERIALIZED VIEW atan_illegal AS SELECT
                      ATAN(tmestmp) AS tmestmp
                      FROM illegal_tbl"""
        self.expected_error = "Cannot apply 'ATAN' to arguments of type"


# ATANH function
class illarg_atanh_legal(TstView):
    def __init__(self):
        # Validated on Postgres
        self.data = [{"dbl": "-1.178925030963371"}]
        self.sql = """CREATE MATERIALIZED VIEW atanh_legal AS SELECT
                      CAST(ATANH(dbl) AS VARCHAR(18)) AS dbl
                      FROM illegal_tbl
                      WHERE id = 1"""


class illarg_atanh_cast_legal(TstView):
    def __init__(self):
        # Validated on Postgres
        self.data = [{"str": "0.120581028408444"}]
        self.sql = """CREATE MATERIALIZED VIEW atanh_cast_legal AS SELECT
                      CAST(ATANH(str) AS VARCHAR(17)) AS str
                      FROM illegal_tbl
                      WHERE id = 1"""


# Negative Test
class illarg_atanh_illegal(TstView):
    def __init__(self):
        # checked manually
        self.sql = """CREATE MATERIALIZED VIEW atanh_illegal AS SELECT
                      ATANH(bin) AS bin
                      FROM illegal_tbl"""
        self.expected_error = "Cannot apply 'ATANH' to arguments of type"


# ATAN2 function
class illarg_atan2_legal(TstView):
    def __init__(self):
        # Validated on Postgres
        self.data = [{"dbl": "1.962934161183153"}]
        self.sql = """CREATE MATERIALIZED VIEW atan2_legal AS SELECT
                      CAST(ATAN2(2, dbl) AS VARCHAR(17)) AS dbl
                      FROM illegal_tbl
                      WHERE id = 1"""


class illarg_atan2_cast_legal(TstView):
    def __init__(self):
        # Validated on Postgres
        self.data = [{"arr": "1.640682328429539"}]
        self.sql = """CREATE MATERIALIZED VIEW atan2_cast_legal AS SELECT
                      CAST(ATAN2(2, ARR[1]) AS VARCHAR(17)) AS arr
                      FROM illegal_tbl
                      WHERE id = 1"""


# Negative Test
class illarg_atan2_illegal(TstView):
    def __init__(self):
        # checked manually
        self.sql = """CREATE MATERIALIZED VIEW atan2_illegal AS SELECT
                      ATAN2(2, bin) AS bin
                      FROM illegal_tbl"""
        self.expected_error = "Cannot apply 'ATAN2' to arguments of type"


# CBRT function
class illarg_cbrt_legal(TstView):
    def __init__(self):
        # Validated on Postgres
        self.data = [{"dbl": "-0.938688508314015"}]
        self.sql = """CREATE MATERIALIZED VIEW cbrt_legal AS SELECT
                      CAST(CBRT(dbl) AS VARCHAR(18)) AS dbl
                      FROM illegal_tbl
                      WHERE id = 1"""


class illarg_cbrt_cast_legal(TstView):
    def __init__(self):
        # checked manually
        self.data = [{"intt": Decimal("-1")}]
        self.sql = """CREATE MATERIALIZED VIEW cbrt_cast_legal AS SELECT
                      CBRT(intt) AS intt
                      FROM illegal_tbl
                      WHERE id = 1"""


# Negative Test
class illarg_cbrt_illegal(TstView):
    def __init__(self):
        # checked manually
        self.sql = """CREATE MATERIALIZED VIEW cbrt_illegal AS SELECT
                      CBRT(booll) AS booll
                      FROM illegal_tbl"""
        self.expected_error = "Cannot apply 'CBRT' to arguments of type"


# CEIL function
class illarg_ceil_legal(TstView):
    def __init__(self):
        # Validated on Postgres
        self.data = [{"dbl": Decimal("-38.0"), "decimall": -1111}]
        self.sql = """CREATE MATERIALIZED VIEW ceil_legal AS SELECT
                      CEIL(dbl) AS dbl,
                      CEIL(decimall) AS decimall
                      FROM illegal_tbl
                      WHERE id = 0"""


class illarg_ceil_cast_legal(TstView):
    def __init__(self):
        # Validated on Postgres
        self.data = [{"arr": 0}]
        self.sql = """CREATE MATERIALIZED VIEW ceil_cast_legal AS SELECT
                      CEIL(ARR[4]) AS arr
                      FROM illegal_tbl
                      WHERE id = 0"""


# Negative Test
class illarg_ceil_illegal(TstView):
    def __init__(self):
        # checked manually
        self.sql = """CREATE MATERIALIZED VIEW ceil_illegal AS SELECT
                      CEIL(bin) AS bin
                      FROM illegal_tbl"""
        self.expected_error = "Cannot apply 'CEIL' to arguments of type"


# COS function
class illarg_cos_legal(TstView):
    def __init__(self):
        # Validated on Postgres
        self.data = [{"dbl": "0.677003833836985"}]
        self.sql = """CREATE MATERIALIZED VIEW ocs_legal AS SELECT
                      CAST(COS(dbl) AS VARCHAR(17)) AS dbl
                      FROM illegal_tbl
                      WHERE id = 1"""


class illarg_cos_cast_legal(TstView):
    def __init__(self):
        # Validated on Postgres
        self.data = [{"intt": "0.540302305868139"}]
        self.sql = """CREATE MATERIALIZED VIEW cos_cast_legal AS SELECT
                      CAST(COS(intt) AS VARCHAR(17)) AS intt
                      FROM illegal_tbl
                      WHERE id = 1"""


# Negative Test
class illarg_cos_illegal(TstView):
    def __init__(self):
        # checked manually
        self.sql = """CREATE MATERIALIZED VIEW cos_illegal AS SELECT
                      COS(tmestmp) AS tmestmp
                      FROM illegal_tbl"""
        self.expected_error = "Cannot apply 'COS' to arguments of type"


# COSH function
class illarg_cosh_legal(TstView):
    def __init__(self):
        # Validated on Postgres
        self.data = [{"dbl": "1.362008123537668"}]
        self.sql = """CREATE MATERIALIZED VIEW cosh_legal AS SELECT
                      CAST(COSH(dbl) AS VARCHAR(17)) AS dbl
                      FROM illegal_tbl
                      WHERE id = 1"""


class illarg_cosh_cast_legal(TstView):
    def __init__(self):
        # Validated on Postgres
        self.data = [{"arr": "1.009816017128016"}]
        self.sql = """CREATE MATERIALIZED VIEW cosh_cast_legal AS SELECT
                      CAST(COSH(ARR[1]) AS VARCHAR(17)) AS arr
                      FROM illegal_tbl
                      WHERE id = 1"""


# Negative Test
class illarg_cosh_illegal(TstView):
    def __init__(self):
        # checked manually
        self.sql = """CREATE MATERIALIZED VIEW cosh_illegal AS SELECT
                      COSH(tmestmp) AS tmestmp
                      FROM illegal_tbl"""
        self.expected_error = "Cannot apply 'COSH' to arguments of type"


# CSC function
class illarg_csc_legal(TstView):
    def __init__(self):
        # Validated on Postgres
        self.data = [{"dbl": "-1.358733516710411"}]
        self.sql = """CREATE MATERIALIZED VIEW csc_legal AS SELECT
                      CAST(CSC(dbl) AS VARCHAR(18)) AS dbl
                      FROM illegal_tbl
                      WHERE id = 1"""


class illarg_csc_cast_legal(TstView):
    def __init__(self):
        # Validated on Postgres
        self.data = [{"arr": "-7.166243942235975"}]
        self.sql = """CREATE MATERIALIZED VIEW csc_cast_legal AS SELECT
                      CAST(CSC(ARR[1]) AS VARCHAR(18)) AS arr
                      FROM illegal_tbl
                      WHERE id = 1"""


# Negative Test
class illarg_csc_illegal(TstView):
    def __init__(self):
        # checked manually
        self.sql = """CREATE MATERIALIZED VIEW csc_illegal AS SELECT
                      CSC(bin) AS bin
                      FROM illegal_tbl"""
        self.expected_error = "Cannot apply 'CSC' to arguments of type"


# CSCH function
class illarg_csch_legal(TstView):
    def __init__(self):
        # Validated on Postgres
        self.data = [{"dbl": "-1.081434320792168"}]
        self.sql = """CREATE MATERIALIZED VIEW csch_legal AS SELECT
                      CAST(CSCH(dbl) AS VARCHAR(18)) AS dbl
                      FROM illegal_tbl
                      WHERE id = 1"""


class illarg_csch_cast_legal(TstView):
    def __init__(self):
        # Validated on Postgres
        self.data = [{"str": "8.31336688239132"}]
        self.sql = """CREATE MATERIALIZED VIEW csch_cast_legal AS SELECT
                      CAST(CSCH(str) AS VARCHAR(16)) AS str
                      FROM illegal_tbl
                      WHERE id = 1"""


# Negative Test
class illarg_csch_illegal(TstView):
    def __init__(self):
        # checked manually
        self.sql = """CREATE MATERIALIZED VIEW csch_illegal AS SELECT
                      CSCH(booll) AS booll
                      FROM illegal_tbl"""
        self.expected_error = "Cannot apply 'CSCH' to arguments of type"


# COT function
class illarg_cot_legal(TstView):
    def __init__(self):
        # Validated on Postgres
        self.data = [{"dbl": "-0.919867799975758"}]
        self.sql = """CREATE MATERIALIZED VIEW cot_legal AS SELECT
                      CAST(COT(dbl) AS VARCHAR(18)) AS dbl
                      FROM illegal_tbl
                      WHERE id = 1"""


class illarg_cot_cast_legal(TstView):
    def __init__(self):
        # Validated on Postgres
        self.data = [{"intt": "-0.642092615934330"}]
        self.sql = """CREATE MATERIALIZED VIEW cot_cast_legal AS SELECT
                      CAST(COT(intt) AS VARCHAR(18)) AS intt
                      FROM illegal_tbl
                      WHERE id = 1"""


# Negative Test
class illarg_cot_illegal(TstView):
    def __init__(self):
        # checked manually
        self.sql = """CREATE MATERIALIZED VIEW cot_illegal AS SELECT
                      COT(bin) AS bin
                      FROM illegal_tbl"""
        self.expected_error = "Cannot apply 'COT' to arguments of type"


# COTH function
class illarg_coth_legal(TstView):
    def __init__(self):
        # Validated on Postgres
        self.data = [{"dbl": "-1.472922329991374"}]
        self.sql = """CREATE MATERIALIZED VIEW coth_legal AS SELECT
                      CAST(COTH(dbl) AS VARCHAR(18)) AS dbl
                      FROM illegal_tbl
                      WHERE id = 1"""


class illarg_coth_cast_legal(TstView):
    def __init__(self):
        # Validated on Postgres
        self.data = [{"intt": "-1.313035285499331"}]
        self.sql = """CREATE MATERIALIZED VIEW coth_cast_legal AS SELECT
                      CAST(COTH(intt) AS VARCHAR(18)) AS intt
                      FROM illegal_tbl
                      WHERE id = 1"""


# Negative Test
class illarg_coth_illegal(TstView):
    def __init__(self):
        # Validated on Postgres
        self.sql = """CREATE MATERIALIZED VIEW coth_illegal AS SELECT
                      COTH(bin) AS bin
                      FROM illegal_tbl"""
        self.expected_error = "Cannot apply 'COTH' to arguments of type"


# Degrees function
class illarg_degrees_legal(TstView):
    def __init__(self):
        # Validated on Postgres
        self.data = [{"dbl": "-47.3900466096781"}]
        self.sql = """CREATE MATERIALIZED VIEW degrees_legal AS SELECT
                      CAST(DEGREES(dbl) AS VARCHAR(17)) AS dbl
                      FROM illegal_tbl
                      WHERE id = 1"""


class illarg_degrees_cast_legal(TstView):
    def __init__(self):
        # Validated on Postgres
        self.data = [{"intt": "-57.2957795130823"}]
        self.sql = """CREATE MATERIALIZED VIEW degrees_cast_legal AS SELECT
                      CAST(DEGREES(intt) AS VARCHAR(17)) AS intt
                      FROM illegal_tbl
                      WHERE id = 1"""


# Negative Test
class illarg_degrees_illegal(TstView):
    def __init__(self):
        # checked manually
        self.sql = """CREATE MATERIALIZED VIEW degrees_illegal AS SELECT
                      DEGREES(booll) AS booll
                      FROM illegal_tbl"""
        self.expected_error = "Cannot apply 'DEGREES' to arguments of type"


# EXP function
class illarg_exp_legal(TstView):
    def __init__(self):
        # Validated on Postgres
        self.data = [{"dbl": "0.437310265541554"}]
        self.sql = """CREATE MATERIALIZED VIEW exp_legal AS SELECT
                      CAST(EXP(dbl) AS VARCHAR(17)) AS dbl
                      FROM illegal_tbl
                      WHERE id = 1"""


class illarg_exp_cast_legal(TstView):
    def __init__(self):
        # Validated on Postgres
        self.data = [{"arr": "0.869358235398805"}]
        self.sql = """CREATE MATERIALIZED VIEW exp_cast_legal AS SELECT
                      CAST(EXP(ARR[1]) AS VARCHAR(17)) AS arr
                      FROM illegal_tbl
                      WHERE id = 1"""


# Negative Test
class illarg_exp_illegal(TstView):
    def __init__(self):
        # checked manually
        self.sql = """CREATE MATERIALIZED VIEW exp_illegal AS SELECT
                      EXP(bin) AS bin
                      FROM illegal_tbl"""
        self.expected_error = "Cannot apply 'EXP' to arguments of type"


# Floor function
class illarg_floor_legal(TstView):
    def __init__(self):
        # Validated on Postgres
        self.data = [{"reall": Decimal("-1"), "decimall": -1}]
        self.sql = """CREATE MATERIALIZED VIEW floor_legal AS SELECT
                      FLOOR(reall) AS reall,
                      FLOOR(decimall) AS decimall
                      FROM illegal_tbl
                      WHERE id = 1"""


class illarg_floor_cast_legal(TstView):
    def __init__(self):
        # Validated on Postgres
        self.data = [{"arr": -1}]
        self.sql = """CREATE MATERIALIZED VIEW floor_cast_legal AS SELECT
                      FLOOR(ARR[1]) AS arr
                      FROM illegal_tbl
                      WHERE id = 1"""


# Negative Test
class illarg_floor_illegal(TstView):
    def __init__(self):
        # checked manually
        self.sql = """CREATE MATERIALIZED VIEW floor_illegal AS SELECT
                      FLOOR(tmestmp) AS tmestmp
                      FROM illegal_tbl"""
        self.expected_error = "Cannot apply 'FLOOR' to arguments of type"


# LN function
class illarg_ln_legal(TstView):
    def __init__(self):
        # Validated on Postgres
        self.data = [{"dbl": "3.644695655163628"}]
        self.sql = """CREATE MATERIALIZED VIEW ln_legal AS SELECT
                      CAST(LN(- dbl) AS VARCHAR(17)) AS dbl
                      FROM illegal_tbl
                      WHERE id = 0"""


class illarg_ln_cast_legal(TstView):
    def __init__(self):
        # Validated on Postgres
        self.data = [{"intt": "2.484906649788000"}]
        self.sql = """CREATE MATERIALIZED VIEW ln_cast_legal AS SELECT
                      CAST(LN(- intt) AS VARCHAR(17)) AS intt
                      FROM illegal_tbl
                      WHERE id = 0"""


# Negative Test
class illarg_ln_illegal(TstView):
    def __init__(self):
        # checked manually
        self.sql = """CREATE MATERIALIZED VIEW ln_illegal AS SELECT
                      LN(booll) AS booll
                      FROM illegal_tbl"""
        self.expected_error = "Cannot apply 'LN' to arguments of type"


# LOG function
class illarg_log_legal(TstView):
    def __init__(self):
        # Validated on Postgres
        self.data = [{"dbl": "1.582871211254320"}]
        self.sql = """CREATE MATERIALIZED VIEW log_legal AS SELECT
                      CAST(LOG(- dbl, 10) AS VARCHAR(17)) AS dbl
                      FROM illegal_tbl
                      WHERE id = 0"""


class illarg_log_cast_legal(TstView):
    def __init__(self):
        # Validated on Postgres
        self.data = [{"intt": "1.079181246047624"}]
        self.sql = """CREATE MATERIALIZED VIEW log_cast_legal AS SELECT
                      CAST(LOG(- intt, 10) AS VARCHAR(17)) AS intt
                      FROM illegal_tbl
                      WHERE id = 0"""


# Negative Test
class illarg_log_illegal(TstView):
    def __init__(self):
        # checked manually
        self.sql = """CREATE MATERIALIZED VIEW log_illegal AS SELECT
                      LOG(booll) AS booll
                      FROM illegal_tbl"""
        self.expected_error = "Cannot apply 'LOG' to arguments of type"


# LOG10 function
class illarg_log10_legal(TstView):
    def __init__(self):
        # checked manually
        self.data = [{"dbl": "1.582871211254320"}]
        self.sql = """CREATE MATERIALIZED VIEW log10_legal AS SELECT
                      CAST(LOG10(- dbl) AS VARCHAR(17)) AS dbl
                      FROM illegal_tbl
                      WHERE id = 0"""


class illarg_log10_cast_legal(TstView):
    def __init__(self):
        # Validated on Postgres
        self.data = [{"intt": "1.079181246047624"}]
        self.sql = """CREATE MATERIALIZED VIEW log10_cast_legal AS SELECT
                      CAST(LOG10(- intt) AS VARCHAR(17)) AS intt
                      FROM illegal_tbl
                      WHERE id = 0"""


# Negative Test
class illarg_log10_illegal(TstView):
    def __init__(self):
        # Validated on Postgres
        self.sql = """CREATE MATERIALIZED VIEW log10_illegal AS SELECT
                      LOG10(bin) AS bin
                      FROM illegal_tbl"""
        self.expected_error = "Cannot apply 'LOG10' to arguments of type"


# Power function
class illarg_pow_legal(TstView):
    def __init__(self):
        # Validated on Postgres
        self.data = [{"dbl": "1464.678890900099"}]
        self.sql = """CREATE MATERIALIZED VIEW pow_legal AS SELECT
                      CAST(POWER(dbl, 2) AS VARCHAR(17)) AS dbl
                      FROM illegal_tbl
                      WHERE id = 0"""


class illarg_pow_cast_legal(TstView):
    def __init__(self):
        # Validated on Postgres
        self.data = [{"arr": Decimal("196")}]
        self.sql = """CREATE MATERIALIZED VIEW pow_cast_legal AS SELECT
                      POWER(ARR[2], 2) AS arr
                      FROM illegal_tbl
                      WHERE id = 0"""


# Negative Test
class illarg_pow_illegal(TstView):
    def __init__(self):
        # checked manually
        self.sql = """CREATE MATERIALIZED VIEW pow_illegal AS SELECT
                      POWER(tmestmp, 2) AS tmestmp
                      FROM illegal_tbl"""
        self.expected_error = "Cannot apply 'POWER' to arguments of type"


# RADIANS function
class illarg_rad_legal(TstView):
    def __init__(self):
        # Validated on Postgres
        self.data = [{"dbl": "-0.66795711281641"}]
        self.sql = """CREATE MATERIALIZED VIEW rad_legal AS SELECT
                      CAST(RADIANS(dbl) AS VARCHAR(17)) AS dbl
                      FROM illegal_tbl
                      WHERE id = 0"""


class illarg_rad_cast_legal(TstView):
    def __init__(self):
        # checked manually
        self.data = [{"intt": "-0.209439510239319"}]
        self.sql = """CREATE MATERIALIZED VIEW rad_cast_legal AS SELECT
                      CAST(RADIANS(intt) AS VARCHAR(18)) AS intt
                      FROM illegal_tbl
                      WHERE id = 0"""


# Negative Test
class illarg_rad_illegal(TstView):
    def __init__(self):
        # checked manually
        self.sql = """CREATE MATERIALIZED VIEW rad_illegal AS SELECT
                      RADIANS(booll) AS booll
                      FROM illegal_tbl"""
        self.expected_error = "Cannot apply 'RADIANS' to arguments of type"


# ROUND function
class illarg_round_legal(TstView):
    def __init__(self):
        # Validated on Postgres
        self.data = [{"decimall": Decimal("-1112")}]
        self.sql = """CREATE MATERIALIZED VIEW round_legal AS SELECT
                      ROUND(decimall) AS decimall
                      FROM illegal_tbl
                      WHERE id = 0"""


class illarg_round_cast_legal(TstView):
    def __init__(self):
        # Validated on Postgres
        self.data = [{"arr": Decimal("14")}]
        self.sql = """CREATE MATERIALIZED VIEW round_cast_legal AS SELECT
                      ROUND(ARR[2]) AS arr
                      FROM illegal_tbl
                      WHERE id = 0"""


# Negative Test
class illarg_round_illegal(TstView):
    def __init__(self):
        # checked manually
        self.sql = """CREATE MATERIALIZED VIEW round_illegal AS SELECT
                      ROUND(booll) AS booll
                      FROM illegal_tbl"""
        self.expected_error = "Cannot apply 'ROUND' to arguments of type"


# ROUND(value, digits) function
class illarg_roundvd_legal(TstView):
    def __init__(self):
        # Validated on Postgres
        self.data = [{"dbl": Decimal("-38.27")}]
        self.sql = """CREATE MATERIALIZED VIEW roundvd_legal AS SELECT
                      ROUND(dbl, 2) AS dbl
                      FROM illegal_tbl
                      WHERE id = 0"""


class illarg_roundvd_cast_legal(TstView):
    def __init__(self):
        # Validated on Postgres
        self.data = [{"arr": Decimal("14")}]
        self.sql = """CREATE MATERIALIZED VIEW roundvd_cast_legal AS SELECT
                      ROUND(ARR[2], 2) AS arr
                      FROM illegal_tbl
                      WHERE id = 0"""


# Negative Test
class illarg_roundvd_illegal(TstView):
    def __init__(self):
        # checked manually
        self.sql = """CREATE MATERIALIZED VIEW roundvd_illegal AS SELECT
                      ROUND(booll, 2) AS booll
                      FROM illegal_tbl"""
        self.expected_error = "Cannot apply 'ROUND' to arguments of type"


# SEC function
class illarg_sec_legal(TstView):
    def __init__(self):
        # Validated on Postgres
        self.data = [{"dbl": "1.189324047746509"}]
        self.sql = """CREATE MATERIALIZED VIEW sec_legal AS SELECT
                      CAST(SEC(dbl) AS VARCHAR(17)) AS dbl
                      FROM illegal_tbl
                      WHERE id = 0"""


class illarg_sec_cast_legal(TstView):
    def __init__(self):
        # Validated on Postgres
        self.data = [{"intt": "1.185039176093985"}]
        self.sql = """CREATE MATERIALIZED VIEW sec_cast_legal AS SELECT
                      CAST(SEC(intt) AS VARCHAR(17)) AS intt
                      FROM illegal_tbl
                      WHERE id = 0"""


# Negative Test
class illarg_sec_illegal(TstView):
    def __init__(self):
        # checked manually
        self.sql = """CREATE MATERIALIZED VIEW sec_illegal AS SELECT
                      SEC(booll) AS booll
                      FROM illegal_tbl"""
        self.expected_error = "Cannot apply 'SEC' to arguments of type"


# SECH function
class illarg_sech_legal(TstView):
    def __init__(self):
        # Validated on Postgres
        self.data = [{"dbl": Decimal("4.78731782390219E-17")}]
        self.sql = """CREATE MATERIALIZED VIEW sech_legal AS SELECT
                      SECH(dbl) AS dbl
                      FROM illegal_tbl
                      WHERE id = 0"""


class illarg_sech_cast_legal(TstView):
    def __init__(self):
        # Validated on Postgres
        self.data = [{"arr": "1.6631e-6"}]
        self.sql = """CREATE MATERIALIZED VIEW sech_cast_legal AS SELECT
                      CAST(ROUND(SECH(ARR[2]), 10) AS VARCHAR(17)) AS arr
                      FROM illegal_tbl
                      WHERE id = 0"""


# Negative Test
class illarg_sech_illegal(TstView):
    def __init__(self):
        # checked manually
        self.sql = """CREATE MATERIALIZED VIEW sech_illegal AS SELECT
                      SECH(booll) AS booll
                      FROM illegal_tbl"""
        self.expected_error = "Cannot apply 'SECH' to arguments of type"


# SIN function
class illarg_sin_legal(TstView):
    def __init__(self):
        # Validated on Postgres
        self.data = [{"dbl": "-0.541324538107345"}]
        self.sql = """CREATE MATERIALIZED VIEW sin_legal AS SELECT
                      CAST(SIN(dbl) AS VARCHAR(18)) AS dbl
                      FROM illegal_tbl
                      WHERE id = 0"""


class illarg_sin_cast_legal(TstView):
    def __init__(self):
        # Validated on Postgres
        self.data = [{"arr": "0.990607355694870"}]
        self.sql = """CREATE MATERIALIZED VIEW sin_cast_legal AS SELECT
                      CAST(SIN(ARR[2]) AS VARCHAR(17)) AS arr
                      FROM illegal_tbl
                      WHERE id = 0"""


# Negative Test
class illarg_sin_illegal(TstView):
    def __init__(self):
        # checked manually
        self.sql = """CREATE MATERIALIZED VIEW sin_illegal AS SELECT
                      SIN(booll) AS booll
                      FROM illegal_tbl"""
        self.expected_error = "Cannot apply 'SIN' to arguments of type"


# SINH function
class illarg_sinh_legal(TstView):
    def __init__(self):
        # Validated on Postgres
        self.data = [{"dbl": Decimal("-2.088852331899055E+16")}]
        self.sql = """CREATE MATERIALIZED VIEW sinh_legal AS SELECT
                      SINH(dbl) AS dbl
                      FROM illegal_tbl
                      WHERE id = 0"""


class illarg_sinh_cast_legal(TstView):
    def __init__(self):
        # Validated on Postgres
        self.data = [{"arr": Decimal("601302.1420819727")}]
        self.sql = """CREATE MATERIALIZED VIEW sinh_cast_legal AS SELECT
                      SINH(ARR[2]) AS arr
                      FROM illegal_tbl
                      WHERE id = 0"""


# Negative Test
class illarg_sinh_illegal(TstView):
    def __init__(self):
        # checked manually
        self.sql = """CREATE MATERIALIZED VIEW sinh_illegal AS SELECT
                      SINH(booll) AS booll
                      FROM illegal_tbl"""
        self.expected_error = "Cannot apply 'SINH' to arguments of type"


# SQRT function
class illarg_sqrt_legal(TstView):
    def __init__(self):
        # Validated on Postgres
        self.data = [{"dbl": "6.186365933253916"}]
        self.sql = """CREATE MATERIALIZED VIEW sqrt_legal AS SELECT
                      CAST(SQRT(- dbl) AS VARCHAR(17)) AS dbl
                      FROM illegal_tbl
                      WHERE id = 0"""


class illarg_sqrt_cast_legal(TstView):
    def __init__(self):
        # Validated on Postgres
        self.data = [{"intt": Decimal("3.4641016151377544")}]
        self.sql = """CREATE MATERIALIZED VIEW sqrt_cast_legal AS SELECT
                      SQRT(- intt) AS intt
                      FROM illegal_tbl
                      WHERE id = 0"""


# Negative Test
class illarg_sqrt_illegal(TstView):
    def __init__(self):
        # checked manually
        self.sql = """CREATE MATERIALIZED VIEW sqrt_illegal AS SELECT
                      SQRT(booll) AS booll
                      FROM illegal_tbl"""
        self.expected_error = "Cannot apply 'SQRT' to arguments of type"


# TAN function
class illarg_tan_legal(TstView):
    def __init__(self):
        # Validated on Postgres
        self.data = [{"dbl": "-0.64381029080633"}]
        self.sql = """CREATE MATERIALIZED VIEW tan_legal AS SELECT
                      CAST(TAN(dbl) AS VARCHAR(17)) AS dbl
                      FROM illegal_tbl
                      WHERE id = 0"""


class illarg_tan_cast_legal(TstView):
    def __init__(self):
        # Validated on Postgres
        self.data = [{"intt": "0.635859928661580"}]
        self.sql = """CREATE MATERIALIZED VIEW tan_cast_legal AS SELECT
                      CAST(TAN(intt) AS VARCHAR(17)) AS intt
                      FROM illegal_tbl
                      WHERE id = 0"""


# Negative Test
class illarg_tan_illegal(TstView):
    def __init__(self):
        # checked manually
        self.sql = """CREATE MATERIALIZED VIEW tan_illegal AS SELECT
                      TAN(booll) AS booll
                      FROM illegal_tbl"""
        self.expected_error = "Cannot apply 'TAN' to arguments of type"


# TANH function
class illarg_tanh_legal(TstView):
    def __init__(self):
        # Validated on Postgres
        self.data = [{"dbl": Decimal("-1.0")}]
        self.sql = """CREATE MATERIALIZED VIEW tanh_legal AS SELECT
                      TANH(dbl) AS dbl
                      FROM illegal_tbl
                      WHERE id = 0"""


class illarg_tanh_cast_legal(TstView):
    def __init__(self):
        # Validated on Postgres
        self.data = [{"arr": "0.999999999998617"}]
        self.sql = """CREATE MATERIALIZED VIEW tanh_cast_legal AS SELECT
                      CAST(TANH(ARR[2]) AS VARCHAR(17)) AS arr
                      FROM illegal_tbl
                      WHERE id = 0"""


# Negative Test
class illarg_tanh_illegal(TstView):
    def __init__(self):
        # checked manually
        self.sql = """CREATE MATERIALIZED VIEW tanh_illegal AS SELECT
                      TANH(booll) AS booll
                      FROM illegal_tbl"""
        self.expected_error = "Cannot apply 'TANH' to arguments of type"


# TRUNCATE function
class illarg_truncate_legal(TstView):
    def __init__(self):
        # Validated on Postgres
        self.data = [{"decimall": Decimal("-1111.00")}]
        self.sql = """CREATE MATERIALIZED VIEW truncate_legal AS SELECT
                      TRUNCATE(decimall) AS decimall
                      FROM illegal_tbl
                      WHERE id = 0"""


class illarg_truncate_cast_legal(TstView):
    def __init__(self):
        # Validated on Postgres
        self.data = [{"intt": -12}]
        self.sql = """CREATE MATERIALIZED VIEW truncate_cast_legal AS SELECT
                      TRUNCATE(intt) AS intt
                      FROM illegal_tbl
                      WHERE id = 0"""


# Negative Test
class illarg_truncate_illegal(TstView):
    def __init__(self):
        # checked manually
        self.sql = """CREATE MATERIALIZED VIEW truncate_illegal AS SELECT
                      TRUNCATE(booll) AS booll
                      FROM illegal_tbl"""
        self.expected_error = "Cannot apply 'TRUNCATE' to arguments of type"


# TRUNCATE(value, digits) function
class illarg_truncatevd_legal(TstView):
    def __init__(self):
        # Validated on Postgres
        self.data = [{"dbl": Decimal("-38.27")}]
        self.sql = """CREATE MATERIALIZED VIEW truncatevd_legal AS SELECT
                      TRUNCATE(dbl, 2) AS dbl
                      FROM illegal_tbl
                      WHERE id = 0"""


class illarg_truncatevvd_cast_legal(TstView):
    def __init__(self):
        # Validated on Postgres
        self.data = [{"intt": -12}]
        self.sql = """CREATE MATERIALIZED VIEW truncatevd_cast_legal AS SELECT
                      TRUNCATE(intt, 2) AS intt
                      FROM illegal_tbl
                      WHERE id = 0"""


# Negative Test
class illarg_truncatevd_illegal(TstView):
    def __init__(self):
        # checked manually
        self.sql = """CREATE MATERIALIZED VIEW truncatevd_illegal AS SELECT
                      TRUNCATE(booll, 2) AS booll
                      FROM illegal_tbl"""
        self.expected_error = "Cannot apply 'TRUNCATE' to arguments of type"


# BROUND function
class illarg_bround_legal(TstView):
    def __init__(self):
        # checked manually
        self.data = [{"decimall": Decimal("-1111.50")}]
        self.sql = """CREATE MATERIALIZED VIEW bround_legal AS SELECT
                      BROUND(decimall, 1) AS decimall
                      FROM illegal_tbl
                      WHERE id = 0"""


class illarg_bround_cast_legal(TstView):
    def __init__(self):
        # checked manually
        self.data = [{"intt": -12}]
        self.sql = """CREATE MATERIALIZED VIEW bround_cast_legal AS SELECT
                      BROUND(intt, 1) AS intt
                      FROM illegal_tbl
                      WHERE id = 0"""


# Negative Test
class illarg_bround_illegal(TstView):
    def __init__(self):
        # checked manually
        self.sql = """CREATE MATERIALIZED VIEW bround_illegal AS SELECT
                      BROUND(str, 1) AS str
                      FROM illegal_tbl"""
        self.expected_error = "Cannot apply 'BROUND' to arguments of type"


# SIGN function
class illarg_sign_legal(TstView):
    def __init__(self):
        # checked manually
        self.data = [{"decimall": Decimal("-1")}]
        self.sql = """CREATE MATERIALIZED VIEW sign_legal AS SELECT
                      SIGN(decimall) AS decimall
                      FROM illegal_tbl
                      WHERE id = 0"""


class illarg_sign_cast_legal(TstView):
    def __init__(self):
        # checked manually
        self.data = [{"arr": Decimal("1")}]
        self.sql = """CREATE MATERIALIZED VIEW sign_cast_legal AS SELECT
                      SIGN(ARR[2]) AS arr
                      FROM illegal_tbl
                      WHERE id = 0"""


# Negative Test
class illarg_sign_illegal(TstView):
    def __init__(self):
        # checked manually
        self.sql = """CREATE MATERIALIZED VIEW sign_illegal AS SELECT
                      SIGN(booll) AS booll
                      FROM illegal_tbl"""
        self.expected_error = "Cannot apply 'SIGN' to arguments of type"


# IGNORE: https://github.com/feldera/feldera/issues/4350

# # IS_INF function
# class illarg_isinf_legal(TstView):
#     def __init__(self):
#         # checked manually
#         self.data = [{"reall": False, "dbl": None, "decimall": True}]
#         self.sql = """CREATE MATERIALIZED VIEW isinf_legal AS SELECT
#                       IS_INF(reall) AS reall,
#                       IS_INF(dbl) AS dbl,
#                       IS_INF(decimall) AS decimall
#                       FROM illegal_tbl
#                       WHERE id = 2"""
#
#
# class illarg_isinf_cast_legal(TstView):
#     def __init__(self):
#         # checked manually
#         self.data = [{"arr": False}]
#         self.sql = """CREATE MATERIALIZED VIEW isinf_cast_legal AS SELECT
#                       IS_INF(ARR[1]) AS arr
#                       FROM illegal_tbl
#                       WHERE id = 1"""
#
#
# # Negative Test
# class illarg_isinf_illegal(TstView):
#     def __init__(self):
#         # checked manually
#         self.sql = """CREATE MATERIALIZED VIEW isinf_illegal AS SELECT
#                       IS_INF(booll) AS booll
#                       FROM illegal_tbl"""
#         self.expected_error = "Cannot apply 'IS_INF' to arguments of type"
#
#
# # IS_NAN function
# class illarg_isnan_legal(TstView):
#     def __init__(self):
#         # checked manually
#         self.data = [{"reall": None, "dbl": None, "decimall": True}]
#         self.sql = """CREATE MATERIALIZED VIEW isnan_legal AS SELECT
#                       IS_NAN(reall) AS reall,
#                       IS_NAN(dbl) AS dbl,
#                       IS_NAN(decimall) AS decimall
#                       FROM illegal_tbl
#                       WHERE id = 3"""
#
#
# class illarg_isnan_cast_legal(TstView):
#     def __init__(self):
#         # checked manually
#         self.data = [{"intt": False}]
#         self.sql = """CREATE MATERIALIZED VIEW isnan_cast_legal AS SELECT
#                       IS_NAN(intt) AS intt
#                       FROM illegal_tbl
#                       WHERE id = 1"""
#
#
# # Negative Test
# class illarg_isnan_illegal(TstView):
#     def __init__(self):
#         # checked manually
#         self.sql = """CREATE MATERIALIZED VIEW isnan_illegal AS SELECT
#                       IS_NAN(booll) AS booll
#                       FROM illegal_tbl"""
#         self.expected_error = "Cannot apply 'IS_NAN' to arguments of type"


# Integer type functions
# MOD function
class illarg_mod_legal(TstView):
    def __init__(self):
        # Validated on Postgres
        self.data = [{"intt": 2, "decimall": Decimal("-1.52")}]
        self.sql = """CREATE MATERIALIZED VIEW mod_legal AS SELECT
                      MOD(-intt, 5) AS intt,
                      MOD(decimall, 3) AS decimall
                      FROM illegal_tbl
                      WHERE id = 0"""


class illarg_mod_cast_legal(TstView):
    def __init__(self):
        # checked manually
        self.data = [{"arr": 2}]
        self.sql = """CREATE MATERIALIZED VIEW mod_cast_legal AS SELECT
                      MOD(ARR[2], 3) AS arr
                      FROM illegal_tbl
                      WHERE id = 0"""


# Negative Test
class illarg_mod_illegal(TstView):
    def __init__(self):
        # checked manually
        self.sql = """CREATE MATERIALIZED VIEW mod_illegal AS SELECT
                      MOD(booll, 2) AS booll
                      FROM illegal_tbl"""
        self.expected_error = "Cannot apply 'MOD' to arguments of type"


# SEQUENCE function
class illarg_sequence_legal(TstView):
    def __init__(self):
        # Validated on Postgres
        self.data = [{"intt": [12, 13, 14, 15, 16, 17, 18, 19, 20]}]
        self.sql = """CREATE MATERIALIZED VIEW sequence_legal AS SELECT
                      SEQUENCE(-intt, 20) AS intt
                      FROM illegal_tbl
                      WHERE id = 0"""


class illarg_sequence_cast_legal(TstView):
    def __init__(self):
        # checked manually
        self.data = [{"arr": [14, 15, 16]}]
        self.sql = """CREATE MATERIALIZED VIEW sequence_cast_legal AS SELECT
                      SEQUENCE(arr[2], 16) AS arr
                      FROM illegal_tbl
                      WHERE id = 0"""


# Negative Test
class illarg_sequence_illegal(TstView):
    def __init__(self):
        # checked manually
        self.sql = """CREATE MATERIALIZED VIEW sequence_illegal AS SELECT
                      SEQUENCE(booll, 2) AS booll
                      FROM illegal_tbl"""
        self.expected_error = "Cannot apply 'SEQUENCE' to arguments of type"
