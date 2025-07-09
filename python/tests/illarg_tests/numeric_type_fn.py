from tests.aggregate_tests.aggtst_base import TstView
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
        self.data = [{"arr": Decimal("0.1400000000000000000")}]
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
        self.data = [{"dbl": Decimal("2.5447465767141204")}]
        self.sql = """CREATE MATERIALIZED VIEW aocs_legal AS SELECT
                      ACOS(dbl) AS dbl
                      FROM illegal_tbl
                      WHERE id = 1"""


class illarg_acos_cast_legal(TstView):
    def __init__(self):
        # Validated on Postgres
        self.data = [{"intt": Decimal("3.141592653589793")}]
        self.sql = """CREATE MATERIALIZED VIEW acos_cast_legal AS SELECT
                      ACOS(intt) AS intt
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
        self.data = [{"dbl": Decimal("4.337672106130182")}]
        self.sql = """CREATE MATERIALIZED VIEW acosh_legal AS SELECT
                      ACOSH(- dbl) AS dbl
                      FROM illegal_tbl
                      WHERE id = 0"""


class illarg_acosh_cast_legal(TstView):
    def __init__(self):
        # Validated on Postgres
        self.data = [{"arr": Decimal("3.3309265526412517")}]
        self.sql = """CREATE MATERIALIZED VIEW acosh_cast_legal AS SELECT
                      ACOSH(ARR[2]) AS arr
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
        self.data = [{"dbl": Decimal("-0.9739502499192239")}]
        self.sql = """CREATE MATERIALIZED VIEW asin_legal AS SELECT
                      ASIN(dbl) AS dbl
                      FROM illegal_tbl
                      WHERE id = 1"""


class illarg_asin_cast_legal(TstView):
    def __init__(self):
        # Validated on Postgres
        self.data = [{"intt": Decimal("-1.5707963267948966")}]
        self.sql = """CREATE MATERIALIZED VIEW asin_cast_legal AS SELECT
                      ASIN(intt) AS intt
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
        self.data = [{"dbl": Decimal("4.338013477915945")}]
        self.sql = """CREATE MATERIALIZED VIEW asinh_legal AS SELECT
                      ASINH(- dbl) AS dbl
                      FROM illegal_tbl
                      WHERE id = 0"""


class illarg_asinhh_cast_legal(TstView):
    def __init__(self):
        # Validated on Postgres
        self.data = [{"arr": Decimal("3.3334775868839923")}]
        self.sql = """CREATE MATERIALIZED VIEW asinh_cast_legal AS SELECT
                      ASINH(ARR[2]) AS arr
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
        self.data = [{"dbl": Decimal("-0.6910556233569336")}]
        self.sql = """CREATE MATERIALIZED VIEW atan_legal AS SELECT
                      ATAN(dbl) AS dbl
                      FROM illegal_tbl
                      WHERE id = 1"""


class illarg_atan_cast_legal(TstView):
    def __init__(self):
        # Validated on Postgres
        self.data = [{"arr": Decimal("-0.13909594148207133")}]
        self.sql = """CREATE MATERIALIZED VIEW atan_cast_legal AS SELECT
                      ATAN(ARR[1]) AS arr
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
        self.data = [{"dbl": Decimal("-1.178925030963371")}]
        self.sql = """CREATE MATERIALIZED VIEW atanh_legal AS SELECT
                      ATANH(dbl) AS dbl
                      FROM illegal_tbl
                      WHERE id = 1"""


class illarg_atanh_cast_legal(TstView):
    def __init__(self):
        # Validated on Postgres
        self.data = [{"str": Decimal("0.12058102840844402")}]
        self.sql = """CREATE MATERIALIZED VIEW atanh_cast_legal AS SELECT
                      ATANH(str) AS str
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
        self.data = [{"dbl": Decimal("1.9629341611831532")}]
        self.sql = """CREATE MATERIALIZED VIEW atan2_legal AS SELECT
                      ATAN2(2, dbl) AS dbl
                      FROM illegal_tbl
                      WHERE id = 1"""


class illarg_atan2_cast_legal(TstView):
    def __init__(self):
        # Validated on Postgres
        self.data = [{"arr": Decimal("1.640682328429539")}]
        self.sql = """CREATE MATERIALIZED VIEW atan2_cast_legal AS SELECT
                      ATAN2(2, ARR[1]) AS arr
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
        self.data = [{"dbl": Decimal("-0.9386885083140153")}]
        self.sql = """CREATE MATERIALIZED VIEW cbrt_legal AS SELECT
                      CBRT(dbl) AS dbl
                      FROM illegal_tbl
                      WHERE id = 1"""


class illarg_cbrt_cast_legal(TstView):
    def __init__(self):
        # checked manually
        self.data = [{"intt": Decimal("-1.0")}]
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
        self.data = [{"dbl": Decimal("0.6770038338369854")}]
        self.sql = """CREATE MATERIALIZED VIEW ocs_legal AS SELECT
                      COS(dbl) AS dbl
                      FROM illegal_tbl
                      WHERE id = 1"""


class illarg_cos_cast_legal(TstView):
    def __init__(self):
        # Validated on Postgres
        self.data = [{"intt": Decimal("0.5403023058681398")}]
        self.sql = """CREATE MATERIALIZED VIEW cos_cast_legal AS SELECT
                      COS(intt) AS intt
                      FROM illegal_tbl
                      WHERE id = 1"""


# Negative Test
class illarg_cos_illegal(TstView):
    def __init__(self):
        # checked manually
        self.sql = """CREATE MATERIALIZED VIEW cos_illegal AS SELECT
                      COS(tmestmp) AS tmestmp
                      FROM illegal_tbl"""
        self.expected_error = "Cannot apply 'ABS' to arguments of type"


# COSH function
class illarg_cosh_legal(TstView):
    def __init__(self):
        # Validated on Postgres
        self.data = [{"dbl": Decimal("1.3620081235376684")}]
        self.sql = """CREATE MATERIALIZED VIEW cosh_legal AS SELECT
                      COSH(dbl) AS dbl
                      FROM illegal_tbl
                      WHERE id = 1"""


class illarg_cosh_cast_legal(TstView):
    def __init__(self):
        # Validated on Postgres
        self.data = [{"arr": Decimal("1.0098160171280166")}]
        self.sql = """CREATE MATERIALIZED VIEW cosh_cast_legal AS SELECT
                      COSH(ARR[1]) AS arr
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
        self.data = [{"dbl": Decimal("-1.358733516710411")}]
        self.sql = """CREATE MATERIALIZED VIEW csc_legal AS SELECT
                      CSC(dbl) AS dbl
                      FROM illegal_tbl
                      WHERE id = 1"""


class illarg_csc_cast_legal(TstView):
    def __init__(self):
        # Validated on Postgres
        self.data = [{"arr": Decimal("-7.166243942235975")}]
        self.sql = """CREATE MATERIALIZED VIEW csc_cast_legal AS SELECT
                      CSC(ARR[1]) AS arr
                      FROM illegal_tbl
                      WHERE id = 1"""


# Negative Test
class illarg_csc_illegal(TstView):
    def __init__(self):
        # checked manually
        self.sql = """CREATE MATERIALIZED VIEW csc_illegal AS SELECT
                      CSC(bin) AS bin
                      FROM illegal_tbl"""
        self.expected_error = "Cannot apply 'ABS' to arguments of type"


# CSCH function
class illarg_csch_legal(TstView):
    def __init__(self):
        # Validated on Postgres
        self.data = [{"dbl": Decimal("-1.0814343207921682")}]
        self.sql = """CREATE MATERIALIZED VIEW csch_legal AS SELECT
                      CSCH(dbl) AS dbl
                      FROM illegal_tbl
                      WHERE id = 1"""


class illarg_csch_cast_legal(TstView):
    def __init__(self):
        # Validated on Postgres
        self.data = [{"str": Decimal("8.313366882391323")}]
        self.sql = """CREATE MATERIALIZED VIEW csch_cast_legal AS SELECT
                      CSCH(str) AS str
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
        self.data = [{"dbl": Decimal("-0.919867799975758")}]
        self.sql = """CREATE MATERIALIZED VIEW cot_legal AS SELECT
                      COT(dbl) AS dbl
                      FROM illegal_tbl
                      WHERE id = 1"""


class illarg_cot_cast_legal(TstView):
    def __init__(self):
        # Validated on Postgres
        self.data = [{"intt": Decimal("-0.6420926159343308")}]
        self.sql = """CREATE MATERIALIZED VIEW cot_cast_legal AS SELECT
                      COT(intt) AS intt
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
        self.data = [{"dbl": Decimal("-1.472922329991374")}]
        self.sql = """CREATE MATERIALIZED VIEW coth_legal AS SELECT
                      COTH(dbl) AS dbl
                      FROM illegal_tbl
                      WHERE id = 1"""


class illarg_coth_cast_legal(TstView):
    def __init__(self):
        # Validated on Postgres
        self.data = [{"intt": Decimal("-1.3130352854993315")}]
        self.sql = """CREATE MATERIALIZED VIEW coth_cast_legal AS SELECT
                      COTH(intt) AS intt
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
        self.data = [{"dbl": Decimal("-47.39004660967816")}]
        self.sql = """CREATE MATERIALIZED VIEW degrees_legal AS SELECT
                      DEGREES(dbl) AS dbl
                      FROM illegal_tbl
                      WHERE id = 1"""


class illarg_degrees_cast_legal(TstView):
    def __init__(self):
        # Validated on Postgres
        self.data = [{"intt": Decimal("-57.29577951308232")}]
        self.sql = """CREATE MATERIALIZED VIEW degrees_cast_legal AS SELECT
                      DEGREES(intt) AS intt
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
        self.data = [{"dbl": Decimal("0.4373102655415548")}]
        self.sql = """CREATE MATERIALIZED VIEW exp_legal AS SELECT
                      EXP(dbl) AS dbl
                      FROM illegal_tbl
                      WHERE id = 1"""


class illarg_exp_cast_legal(TstView):
    def __init__(self):
        # Validated on Postgres
        self.data = [{"arr": Decimal("0.8693582353988059")}]
        self.sql = """CREATE MATERIALIZED VIEW exp_cast_legal AS SELECT
                      EXP(ARR[1]) AS arr
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
        self.data = [{"reall": Decimal("-1.0"), "decimall": -1}]
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
        self.expected_error = "Cannot apply 'ABS' to arguments of type"


# IS_INF function
class illarg_isinf_legal(TstView):
    def __init__(self):
        # checked manually
        self.data = [{"reall": False, "dbl": None, "decimall": True}]
        self.sql = """CREATE MATERIALIZED VIEW isinf_legal AS SELECT
                      IS_INF(reall) AS reall,
                      IS_INF(dbl) AS dbl,
                      IS_INF(decimall) AS decimall
                      FROM illegal_tbl
                      WHERE id = 2"""


class illarg_isinf_cast_legal(TstView):
    def __init__(self):
        # checked manually
        self.data = [{"arr": False}]
        self.sql = """CREATE MATERIALIZED VIEW isinf_cast_legal AS SELECT
                      IS_INF(ARR[1]) AS arr
                      FROM illegal_tbl
                      WHERE id = 1"""


# Negative Test
class illarg_isinf_illegal(TstView):
    def __init__(self):
        # checked manually
        self.sql = """CREATE MATERIALIZED VIEW isinf_illegal AS SELECT
                      IS_INF(booll) AS booll
                      FROM illegal_tbl"""
        self.expected_error = "Cannot apply 'ABS' to arguments of type"


# IS_NAN function
class illarg_isnan_legal(TstView):
    def __init__(self):
        # checked manually
        self.data = [{"reall": None, "dbl": None, "decimall": True}]
        self.sql = """CREATE MATERIALIZED VIEW isnan_legal AS SELECT
                      IS_NAN(reall) AS reall,
                      IS_NAN(dbl) AS dbl,
                      IS_NAN(decimall) AS decimall
                      FROM illegal_tbl
                      WHERE id = 3"""


class illarg_isnan_cast_legal(TstView):
    def __init__(self):
        # checked manually
        self.data = [{"intt": False}]
        self.sql = """CREATE MATERIALIZED VIEW isnan_cast_legal AS SELECT
                      IS_NAN(intt) AS intt
                      FROM illegal_tbl
                      WHERE id = 1"""


# Negative Test
class illarg_isnan_illegal(TstView):
    def __init__(self):
        # checked manually
        self.sql = """CREATE MATERIALIZED VIEW isnan_illegal AS SELECT
                      IS_NAN(booll) AS booll
                      FROM illegal_tbl"""
        self.expected_error = "Cannot apply 'ABS' to arguments of type"
