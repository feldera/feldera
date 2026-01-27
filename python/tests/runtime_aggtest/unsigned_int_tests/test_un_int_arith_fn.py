from tests.runtime_aggtest.aggtst_base import TstView
from decimal import Decimal


# NOTE: SINH, COSH, EXP tests are supplied smaller input due to: https://github.com/feldera/feldera/issues/5520
# ABS
class un_int_abs(TstView):
    def __init__(self):
        self.data = [
            {
                "id": 0,
                "tiny_un_int": 9,
                "small_un_int": 10192,
                "un_intt": 1146171806,
                "big_un_int": 6996571239486628517,
            }
        ]
        self.sql = """CREATE MATERIALIZED VIEW abs_un_intt AS SELECT
                      id,
                      ABS(tiny_int) AS tiny_un_int,
                      ABS(small_int) AS small_un_int,
                      ABS(intt) AS un_intt,
                      ABS(big_int) AS big_un_int
                      FROM un_int_datagen_tbl"""


# ACOS
class un_int_acos(TstView):
    def __init__(self):
        self.data = [
            {
                "id": 0,
                "tiny_un_int": Decimal("1.5707963267948966"),
                "small_un_int": Decimal("1.5707963267948966"),
                "un_intt": Decimal("1.5707963267948966"),
                "big_un_int": Decimal("1.5707963267948966"),
            }
        ]
        self.sql = """CREATE MATERIALIZED VIEW acos_un_intt AS SELECT
                      id,
                      ACOS(tiny_int) AS tiny_un_int,
                      ACOS(small_int) AS small_un_int,
                      ACOS(intt) AS un_intt,
                      ACOS(big_int) AS big_un_int
                      FROM un_int_datagen_tbl1
                      WHERE id = 0"""


# ACOSH
class un_int_acosh(TstView):
    def __init__(self):
        self.data = [
            {
                "id": 0,
                "tiny_un_int": Decimal("2.8872709503576206"),
                "small_un_int": Decimal("9.922505555965195"),
                "un_intt": Decimal("21.552840542537893"),
                "big_un_int": Decimal("44.08509906057516"),
            }
        ]
        self.sql = """CREATE MATERIALIZED VIEW acosh_un_intt AS SELECT
                      id,
                      ACOSH(tiny_int) AS tiny_un_int,
                      ACOSH(small_int) AS small_un_int,
                      ACOSH(intt) AS un_intt,
                      ACOSH(big_int) AS big_un_int
                      FROM un_int_datagen_tbl"""


# ASIN
class un_int_asin(TstView):
    def __init__(self):
        self.data = [
            {
                "id": 1,
                "tiny_un_int": Decimal("1.5707963267948966"),
                "small_un_int": Decimal("1.5707963267948966"),
                "un_intt": Decimal("1.5707963267948966"),
                "big_un_int": Decimal("1.5707963267948966"),
            }
        ]
        self.sql = """CREATE MATERIALIZED VIEW asin_un_intt AS SELECT
                      id,
                      ASIN(tiny_int) AS tiny_un_int,
                      ASIN(small_int) AS small_un_int,
                      ASIN(intt) AS un_intt,
                      ASIN(big_int) AS big_un_int
                      FROM un_int_datagen_tbl1
                      WHERE id = 1"""


# ASINH
class un_int_asinh(TstView):
    def __init__(self):
        self.data = [
            {
                "id": 0,
                "tiny_un_int": Decimal("2.8934439858858716"),
                "small_un_int": Decimal("9.922505560778585"),
                "un_intt": Decimal("21.552840542537893"),
                "big_un_int": Decimal("44.08509906057516"),
            }
        ]
        self.sql = """CREATE MATERIALIZED VIEW asinh_un_intt AS SELECT
                      id,
                      ASINH(tiny_int) AS tiny_un_int,
                      ASINH(small_int) AS small_un_int,
                      ASINH(intt) AS un_intt,
                      ASINH(big_int) AS big_un_int
                      FROM un_int_datagen_tbl"""


# ATAN
class un_int_atan(TstView):
    def __init__(self):
        self.data = [
            {
                "id": 0,
                "tiny_un_int": Decimal("1.460139105621001"),
                "small_un_int": Decimal("1.5706982106256668"),
                "un_intt": Decimal("1.5707963259224271"),
                "big_un_int": Decimal("1.5707963267948966"),
            }
        ]

        self.sql = """CREATE MATERIALIZED VIEW atan_un_intt AS SELECT
                      id,
                      ATAN(tiny_int) AS tiny_un_int,
                      ATAN(small_int) AS small_un_int,
                      ATAN(intt) AS un_intt,
                      ATAN(big_int) AS big_un_int
                      FROM un_int_datagen_tbl"""


# CBRT
class un_int_cbrt(TstView):
    def __init__(self):
        self.data = [
            {
                "id": 0,
                "tiny_un_int": Decimal("2.080083823051904"),
                "small_un_int": Decimal("21.681357558087388"),
                "un_intt": Decimal("1046.5257220023252"),
                "big_un_int": Decimal("1912618.7992433792"),
            }
        ]
        self.sql = """CREATE MATERIALIZED VIEW cbrt_un_intt AS SELECT
                      id,
                      CBRT(tiny_int) AS tiny_un_int,
                      CBRT(small_int) AS small_un_int,
                      CBRT(intt) AS un_intt,
                      CBRT(big_int) AS big_un_int
                      FROM un_int_datagen_tbl"""


# COS
class un_int_cos(TstView):
    def __init__(self):
        self.data = [
            {
                "id": 0,
                "tiny_un_int": Decimal("-0.9111302618846769"),
                "small_un_int": Decimal("0.7816859829714614"),
                "un_intt": Decimal("-0.9355219612196043"),
                "big_un_int": Decimal("-0.7925127445218715"),
            }
        ]
        self.sql = """CREATE MATERIALIZED VIEW cos_un_intt AS SELECT
                      id,
                      COS(tiny_int) AS tiny_un_int,
                      COS(small_int) AS small_un_int,
                      COS(intt) AS un_intt,
                      COS(big_int) AS big_un_int
                      FROM un_int_datagen_tbl"""


# COSH
class un_int_cosh(TstView):
    def __init__(self):
        # checked manually
        self.data = [
            {
                "id": 1,
                "tiny_un_int": Decimal("1.5430806348152437"),
                "small_un_int": Decimal("1.5430806348152437"),
                "un_intt": Decimal("1.5430806348152437"),
                "big_un_int": Decimal("1.5430806348152437"),
            }
        ]
        self.sql = """CREATE MATERIALIZED VIEW cosh_un_intt AS SELECT
                      id,
                      COSH(tiny_int) AS tiny_un_int,
                      COSH(small_int) AS small_un_int,
                      COSH(intt) AS un_intt,
                      COSH(big_int) AS big_un_int
                      FROM un_int_datagen_tbl1
                      WHERE id = 1"""


# DEGREES
class un_int_degrees(TstView):
    def __init__(self):
        self.data = [
            {
                "id": 0,
                "tiny_un_int": Decimal("515.662015617741"),
                "small_un_int": Decimal("583958.5847973351"),
                "un_intt": Decimal("65670807080.68737"),
                "big_un_int": Decimal("4.00874003085199E+20"),
            }
        ]
        self.sql = """CREATE MATERIALIZED VIEW degrees_un_intt AS SELECT
                      id,
                      DEGREES(tiny_int) AS tiny_un_int,
                      DEGREES(small_int) AS small_un_int,
                      DEGREES(intt) AS un_intt,
                      DEGREES(big_int) AS big_un_int
                      FROM un_int_datagen_tbl"""


# EXP
class un_int_exp(TstView):
    def __init__(self):
        self.data = [
            {
                "id": 1,
                "tiny_un_int": Decimal("2.718281828459045"),
                "small_un_int": Decimal("2.718281828459045"),
                "un_intt": Decimal("2.718281828459045"),
                "big_un_int": Decimal("2.718281828459045"),
            }
        ]
        self.sql = """CREATE MATERIALIZED VIEW exp_un_intt AS SELECT
                      id,
                      EXP(tiny_int) AS tiny_un_int,
                      EXP(small_int) AS small_un_int,
                      EXP(intt) AS un_intt,
                      EXP(big_int) AS big_un_int
                      FROM un_int_datagen_tbl1
                      WHERE id = 1"""


# LN
class un_int_ln(TstView):
    def __init__(self):
        self.data = [
            {
                "id": 0,
                "tiny_un_int": Decimal("2.1972245773362196"),
                "small_un_int": Decimal("9.229358377811945"),
                "un_intt": Decimal("20.85969336197795"),
                "big_un_int": Decimal("43.39195188001521"),
            }
        ]
        self.sql = """CREATE MATERIALIZED VIEW ln_un_intt AS SELECT
                      id,
                      LN(tiny_int) AS tiny_un_int,
                      LN(small_int) AS small_un_int,
                      LN(intt) AS un_intt,
                      LN(big_int) AS big_un_int
                      FROM un_int_datagen_tbl"""


# LOG
class un_int_log(TstView):
    def __init__(self):
        self.data = [
            {
                "id": 0,
                "tiny_un_int": Decimal("2.1972245773362196"),
                "small_un_int": Decimal("9.229358377811945"),
                "un_intt": Decimal("20.85969336197795"),
                "big_un_int": Decimal("43.39195188001521"),
            }
        ]
        self.sql = """CREATE MATERIALIZED VIEW log_un_intt AS SELECT
                      id,
                      LOG(tiny_int) AS tiny_un_int,
                      LOG(small_int) AS small_un_int,
                      LOG(intt) AS un_intt,
                      LOG(big_int) AS big_un_int
                      FROM un_int_datagen_tbl"""


# LOG10
class un_int_log10(TstView):
    def __init__(self):
        self.data = [
            {
                "id": 0,
                "tiny_un_int": Decimal("0.9542425094393249"),
                "small_un_int": Decimal("4.008259414991275"),
                "un_intt": Decimal("9.059249721300915"),
                "big_un_int": Decimal("18.844885260502043"),
            }
        ]
        self.sql = """CREATE MATERIALIZED VIEW log10_un_intt AS SELECT
                      id,
                      LOG10(tiny_int) AS tiny_un_int,
                      LOG10(small_int) AS small_un_int,
                      LOG10(intt) AS un_intt,
                      LOG10(big_int) AS big_un_int
                      FROM un_int_datagen_tbl"""


# RADIANS
class un_int_radians(TstView):
    def __init__(self):
        self.data = [
            {
                "id": 0,
                "tiny_un_int": Decimal("0.15707963267948966"),
                "small_un_int": Decimal("177.88395736326206"),
                "un_intt": Decimal("20004471.8082297"),
                "big_un_int": Decimal("1.2211320447938237E+17"),
            }
        ]
        self.sql = """CREATE MATERIALIZED VIEW radians_un_intt AS SELECT
                      id,
                      RADIANS(tiny_int) AS tiny_un_int,
                      RADIANS(small_int) AS small_un_int,
                      RADIANS(intt) AS un_intt,
                      RADIANS(big_int) AS big_un_int
                      FROM un_int_datagen_tbl"""


# SIN
class un_int_sin(TstView):
    def __init__(self):
        self.data = [
            {
                "id": 0,
                "tiny_un_int": Decimal("0.4121184852417566"),
                "small_un_int": Decimal("0.6236722087971696"),
                "un_intt": Decimal("-0.3532685381913101"),
                "big_un_int": Decimal("0.6098553515141201"),
            }
        ]
        self.sql = """CREATE MATERIALIZED VIEW sin_un_intt AS SELECT
                      id,
                      SIN(tiny_int) AS tiny_un_int,
                      SIN(small_int) AS small_un_int,
                      SIN(intt) AS un_intt,
                      SIN(big_int) AS big_un_int
                      FROM un_int_datagen_tbl"""


# SINH
class un_int_sinh(TstView):
    def __init__(self):
        self.data = [
            {
                "id": 1,
                "tiny_un_int": Decimal("1.1752011936438014"),
                "small_un_int": Decimal("1.1752011936438014"),
                "un_intt": Decimal("1.1752011936438014"),
                "big_un_int": Decimal("1.1752011936438014"),
            }
        ]
        self.sql = """CREATE MATERIALIZED VIEW sinh_un_intt AS SELECT
                      id,
                      SINH(tiny_int) AS tiny_un_int,
                      SINH(small_int) AS small_un_int,
                      SINH(intt) AS un_intt,
                      SINH(big_int) AS big_un_int
                      FROM un_int_datagen_tbl1
                      WHERE id = 1"""


# SQRT
class un_int_sqrt(TstView):
    def __init__(self):
        self.data = [
            {
                "id": 0,
                "tiny_un_int": Decimal("3.0"),
                "small_un_int": Decimal("100.9554357129917"),
                "un_intt": Decimal("33855.159222783164"),
                "big_un_int": Decimal("2645103256.866663"),
            }
        ]
        self.sql = """CREATE MATERIALIZED VIEW sqrt_un_intt AS SELECT
                      id,
                      SQRT(tiny_int) AS tiny_un_int,
                      SQRT(small_int) AS small_un_int,
                      SQRT(intt) AS un_intt,
                      SQRT(big_int) AS big_un_int
                      FROM un_int_datagen_tbl"""


# TAN
class un_int_tan(TstView):
    def __init__(self):
        self.data = [
            {
                "id": 0,
                "tiny_un_int": Decimal("-0.4523156594418099"),
                "small_un_int": Decimal("0.7978551776333174"),
                "un_intt": Decimal("0.3776165101787321"),
                "big_un_int": Decimal("-0.7695211915892282"),
            }
        ]
        self.sql = """CREATE MATERIALIZED VIEW tan_un_intt AS SELECT
                      id,
                      TAN(tiny_int) AS tiny_un_int,
                      TAN(small_int) AS small_un_int,
                      TAN(intt) AS un_intt,
                      TAN(big_int) AS big_un_int
                      FROM un_int_datagen_tbl"""


# ATAN2
class un_int_atan2(TstView):
    def __init__(self):
        self.data = [
            {
                "id": 0,
                "tiny_un_int": Decimal("1.3521273809209546"),
                "small_un_int": Decimal("1.5706000944583258"),
                "un_intt": Decimal("1.5707963250499575"),
                "big_un_int": Decimal("1.5707963267948966"),
            }
        ]
        self.sql = """CREATE MATERIALIZED VIEW atan2_un_intt AS SELECT
                      id,
                      ATAN2(tiny_int, 2) AS tiny_un_int,
                      ATAN2(small_int, 2) AS small_un_int,
                      ATAN2(intt, 2) AS un_intt,
                      ATAN2(big_int, 2) AS big_un_int
                      FROM un_int_datagen_tbl"""


# POWER
class un_int_power(TstView):
    def __init__(self):
        self.data = [
            {
                "id": 0,
                "tiny_un_int": Decimal("81.0"),
                "small_un_int": Decimal("103876864.0"),
                "un_intt": Decimal("1.3137098088693018E+18"),
                "big_un_int": Decimal("4.895200910921146E+37"),
            }
        ]
        self.sql = """CREATE MATERIALIZED VIEW power_un_intt AS SELECT
                      id,
                      POWER(tiny_int, 2) AS tiny_un_int,
                      POWER(small_int, 2) AS small_un_int,
                      POWER(intt, 2) AS un_intt,
                      POWER(big_int, 2) AS big_un_int
                      FROM un_int_datagen_tbl"""


# GREATEST
class un_int_greatest(TstView):
    def __init__(self):
        self.data = [{"id": 0, "un_intt": 6996571239486628517}]
        self.sql = """CREATE MATERIALIZED VIEW greatest_un_intt AS SELECT
                      id,
                      GREATEST(tiny_int, small_int, intt, big_int) AS un_intt
                      FROM un_int_datagen_tbl"""


# GREATEST_IGNORE_NULLS
class un_int_greatest_ignore_nulls(TstView):
    def __init__(self):
        self.data = [{"id": 0, "un_intt": 6996571239486628517}]
        self.sql = """CREATE MATERIALIZED VIEW greatest_ignore_nulls_un_intt AS SELECT
                      id,
                      GREATEST_IGNORE_NULLS(tiny_int, small_int, intt, big_int) AS un_intt
                      FROM un_int_datagen_tbl"""


# LEAST
class un_int_least(TstView):
    def __init__(self):
        self.data = [{"id": 0, "un_intt": 9}]
        self.sql = """CREATE MATERIALIZED VIEW least_un_intt AS SELECT
                      id,
                      LEAST(tiny_int, small_int, intt, big_int) AS un_intt
                      FROM un_int_datagen_tbl"""


# LEAST_IGNORE_NULLS
class un_int_least_ignore_nulls(TstView):
    def __init__(self):
        self.data = [{"id": 0, "un_intt": 9}]
        self.sql = """CREATE MATERIALIZED VIEW least_ignore_nulls_un_intt AS SELECT
                      id,
                      LEAST_IGNORE_NULLS(tiny_int, small_int, intt, big_int) AS un_intt
                      FROM un_int_datagen_tbl"""


# BROUND
class un_int_bround(TstView):
    def __init__(self):
        self.data = [
            {
                "id": 0,
                "tiny_un_int": 9,
                "small_un_int": 10192,
                "un_intt": 1146171806,
                "big_un_int": 6996571239486628517,
            }
        ]
        self.sql = """CREATE MATERIALIZED VIEW bround_un_intt AS SELECT
                      id,
                      BROUND(tiny_int, 2) AS tiny_un_int,
                      BROUND(small_int, 2) AS small_un_int,
                      BROUND(intt, 2) AS un_intt,
                      BROUND(big_int, 2) AS big_un_int
                      FROM un_int_datagen_tbl"""


# CEIL
class un_int_ceil(TstView):
    def __init__(self):
        self.data = [
            {
                "id": 0,
                "tiny_un_int": 9,
                "small_un_int": 10192,
                "un_intt": 1146171806,
                "big_un_int": 6996571239486628517,
            }
        ]
        self.sql = """CREATE MATERIALIZED VIEW ceil_un_intt AS SELECT
                      id,
                      CEIL(tiny_int) AS tiny_un_int,
                      CEIL(small_int) AS small_un_int,
                      CEIL(intt) AS un_intt,
                      CEIL(big_int) AS big_un_int
                      FROM un_int_datagen_tbl"""


# FLOOR
class un_int_floor(TstView):
    def __init__(self):
        self.data = [
            {
                "id": 0,
                "tiny_un_int": 9,
                "small_un_int": 10192,
                "un_intt": 1146171806,
                "big_un_int": 6996571239486628517,
            }
        ]
        self.sql = """CREATE MATERIALIZED VIEW floor_un_intt AS SELECT
                      id,
                      FLOOR(tiny_int) AS tiny_un_int,
                      FLOOR(small_int) AS small_un_int,
                      FLOOR(intt) AS un_intt,
                      FLOOR(big_int) AS big_un_int
                      FROM un_int_datagen_tbl"""


# ROUND
class un_int_round(TstView):
    def __init__(self):
        self.data = [
            {
                "id": 0,
                "tiny_un_int": 9,
                "small_un_int": 10192,
                "un_intt": 1146171806,
                "big_un_int": 6996571239486628517,
            }
        ]
        self.sql = """CREATE MATERIALIZED VIEW round_un_intt AS SELECT
                      id,
                      ROUND(tiny_int) AS tiny_un_int,
                      ROUND(small_int) AS small_un_int,
                      ROUND(intt) AS un_intt,
                      ROUND(big_int) AS big_un_int
                      FROM un_int_datagen_tbl"""


# TRUNC
class un_int_trunc(TstView):
    def __init__(self):
        self.data = [
            {
                "id": 0,
                "tiny_un_int": 9,
                "small_un_int": 10192,
                "un_intt": 1146171806,
                "big_un_int": 6996571239486628517,
            }
        ]
        self.sql = """CREATE MATERIALIZED VIEW trunc_un_intt AS SELECT
                      id,
                      TRUNC(tiny_int) AS tiny_un_int,
                      TRUNC(small_int) AS small_un_int,
                      TRUNC(intt) AS un_intt,
                      TRUNC(big_int) AS big_un_int
                      FROM un_int_datagen_tbl"""


# TRUNCATE
class un_int_truncate(TstView):
    def __init__(self):
        self.data = [
            {
                "id": 0,
                "tiny_un_int": 9,
                "small_un_int": 10192,
                "un_intt": 1146171806,
                "big_un_int": 6996571239486628517,
            }
        ]
        self.sql = """CREATE MATERIALIZED VIEW truncate_un_intt AS SELECT
                      id,
                      TRUNCATE(tiny_int) AS tiny_un_int,
                      TRUNCATE(small_int) AS small_un_int,
                      TRUNCATE(intt) AS un_intt,
                      TRUNCATE(big_int) AS big_un_int
                      FROM un_int_datagen_tbl"""


# COT
class un_int_cot(TstView):
    def __init__(self):
        self.data = [
            {
                "id": 0,
                "tiny_un_int": Decimal("-2.2108454109991946"),
                "small_un_int": Decimal("1.2533602939900772"),
                "un_intt": Decimal("2.648189295342737"),
                "big_un_int": Decimal("-1.299509371450555"),
            }
        ]
        self.sql = """CREATE MATERIALIZED VIEW cot_un_intt AS SELECT
                      id,
                      COT(tiny_int) AS tiny_un_int,
                      COT(small_int) AS small_un_int,
                      COT(intt) AS un_intt,
                      COT(big_int) AS big_un_int
                      FROM un_int_datagen_tbl"""


# COTH
class un_int_coth(TstView):
    def __init__(self):
        self.data = [
            {
                "id": 0,
                "tiny_un_int": Decimal("1.0000000304599599"),
                "small_un_int": Decimal("1.0"),
                "un_intt": Decimal("1.0"),
                "big_un_int": Decimal("1.0"),
            }
        ]
        self.sql = """CREATE MATERIALIZED VIEW coth_un_intt AS SELECT
                      id,
                      COTH(tiny_int) AS tiny_un_int,
                      COTH(small_int) AS small_un_int,
                      COTH(intt) AS un_intt,
                      COTH(big_int) AS big_un_int
                      FROM un_int_datagen_tbl"""


# CSC
class un_int_csc(TstView):
    def __init__(self):
        self.data = [
            {
                "id": 0,
                "tiny_un_int": Decimal("2.426486643551989"),
                "small_un_int": Decimal("1.6034063822222029"),
                "un_intt": Decimal("-2.8307077814511095"),
                "big_un_int": Decimal("1.6397330900142915"),
            }
        ]
        self.sql = """CREATE MATERIALIZED VIEW csc_un_intt AS SELECT
                      id,
                      CSC(tiny_int) AS tiny_un_int,
                      CSC(small_int) AS small_un_int,
                      CSC(intt) AS un_intt,
                      CSC(big_int) AS big_un_int
                      FROM un_int_datagen_tbl"""


# CSCH
class un_int_csch(TstView):
    def __init__(self):
        self.data = [
            {
                "id": 0,
                "tiny_un_int": Decimal("0.0002468196119324168"),
                "small_un_int": Decimal("0.0"),
                "un_intt": Decimal("0.0"),
                "big_un_int": Decimal("0.0"),
            }
        ]
        self.sql = """CREATE MATERIALIZED VIEW csch_un_intt AS SELECT
                      id,
                      CSCH(tiny_int) AS tiny_un_int,
                      CSCH(small_int) AS small_un_int,
                      CSCH(intt) AS un_intt,
                      CSCH(big_int) AS big_un_int
                      FROM un_int_datagen_tbl"""


# SEC
class un_int_sec(TstView):
    def __init__(self):
        self.data = [
            {
                "id": 0,
                "tiny_un_int": Decimal("-1.097537906304962"),
                "small_un_int": Decimal("1.2792860839062903"),
                "un_intt": Decimal("-1.068921993767349"),
                "big_un_int": Decimal("-1.2618093613160846"),
            }
        ]
        self.sql = """CREATE MATERIALIZED VIEW sec_un_intt AS SELECT
                      id,
                      SEC(tiny_int) AS tiny_un_int,
                      SEC(small_int) AS small_un_int,
                      SEC(intt) AS un_intt,
                      SEC(big_int) AS big_un_int
                      FROM un_int_datagen_tbl"""


# SECH
class un_int_sech(TstView):
    def __init__(self):
        self.data = [
            {
                "id": 0,
                "tiny_un_int": Decimal("0.00024681960441430155"),
                "small_un_int": Decimal("0.0"),
                "un_intt": Decimal("0.0"),
                "big_un_int": Decimal("0.0"),
            }
        ]
        self.sql = """CREATE MATERIALIZED VIEW sech_un_intt AS SELECT
                      id,
                      SECH(tiny_int) AS tiny_un_int,
                      SECH(small_int) AS small_un_int,
                      SECH(intt) AS un_intt,
                      SECH(big_int) AS big_un_int
                      FROM un_int_datagen_tbl"""


# Addition
class un_int_add(TstView):
    def __init__(self):
        self.data = [
            {
                "tiny_int": 93,
                "small_int": 25714,
                "big_int": 10348816759985322266,
                "intt": 2604097602,
            }
        ]
        self.sql = """CREATE MATERIALIZED VIEW un_int_add AS SELECT
                      tiny_int + tiny_int1 AS tiny_int,
                      small_int + small_int1 AS small_int,
                      big_int + big_int1 AS big_int,
                      intt + intt1 AS intt
                      FROM un_int_datagen_tbl"""


# Subtraction
class un_int_sub(TstView):
    def __init__(self):
        self.data = [
            {
                "tiny_int": 75,
                "small_int": 5330,
                "big_int": 3644325718987934768,
                "intt": 311753990,
            }
        ]
        self.sql = """CREATE MATERIALIZED VIEW un_int_sub AS SELECT
                      tiny_int1 - tiny_int AS tiny_int,
                      small_int1 - small_int AS small_int,
                      big_int - big_int1 AS big_int,
                      intt1 - intt AS intt
                      FROM un_int_datagen_tbl"""


# Multiplication
class un_int_mul(TstView):
    def __init__(self):
        self.data = [
            {
                "tiny_int": 9,
                "small_int": 10192,
                "big_int": 6996571239486628517,
                "intt": 1146171806,
            }
        ]
        self.sql = """CREATE MATERIALIZED VIEW un_int_mul AS SELECT
                      tiny_int * 1::TINYINT UNSIGNED AS tiny_int,
                      small_int * 1::SMALLINT UNSIGNED AS small_int,
                      big_int * 1::BIGINT UNSIGNED AS big_int,
                      intt * 1::INTEGER UNSIGNED AS intt
                      FROM un_int_datagen_tbl"""


# Division
class un_int_div(TstView):
    def __init__(self):
        self.data = [{"tiny_int": 0, "small_int": 0, "big_int": 2, "intt": 0}]
        self.sql = """CREATE MATERIALIZED VIEW un_int_div AS SELECT
                      tiny_int / tiny_int1 AS tiny_int,
                      small_int / small_int1 AS small_int,
                      big_int / big_int1 AS big_int,
                      intt / intt1 AS intt
                      FROM un_int_datagen_tbl"""


# MOD
class un_int_mod(TstView):
    def __init__(self):
        self.data = [
            {
                "tiny_int": 9,
                "small_int": 10192,
                "big_int": 292080198489241019,
                "intt": 1146171806,
            }
        ]
        self.sql = """CREATE MATERIALIZED VIEW un_int_mod AS SELECT
                      tiny_int % tiny_int1 AS tiny_int,
                      small_int % small_int1 AS small_int,
                      big_int % big_int1 AS big_int,
                      intt % intt1 AS intt
                      FROM un_int_datagen_tbl"""
