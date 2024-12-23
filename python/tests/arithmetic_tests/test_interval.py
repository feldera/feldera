from tests.aggregate_tests.aggtst_base import TstView


# LONG INTERVAL
class arithtst_atbl_long_interval(TstView):
    """Define the view used by long interval tests as input"""

    def __init__(self):
        # Result validation is not required for local views
        self.data = []

        self.sql = """CREATE LOCAL VIEW atbl_long_interval AS SELECT
                      id,
                      (c1 - c2)MONTH AS months
                      FROM timestamp_tbl"""


class arithtst_atbl_long_interval_res(TstView):
    def __init__(self):
        # Validated on MySQL
        self.data = [
            {'id': 0, 'months_res': 60},
            {'id': 1, 'months_res': -32},
            {'id': 2, 'months_res': 126}
        ]
        self.sql = """CREATE MATERIALIZED VIEW atbl_long_interval_res AS SELECT
                      id,
                      CAST((months) AS BIGINT) AS months_res
                      FROM atbl_long_interval"""


# Equivalent MySQL program
# SELECT
# 	id,
#     TIMESTAMPDIFF(MONTH, c2, c1) AS f_c1
# FROM timestamp_tbl;


# SHORT INTERVAL
class arithtst_atbl_short_interval(TstView):
    """Define the view used by short interval tests as input"""

    def __init__(self):
        # Result validation is not required for local views
        self.data = []
        self.sql = """CREATE LOCAL VIEW atbl_short_interval AS SELECT
                      id,
                      (c1 - c2)SECOND AS seconds
                      FROM timestamp_tbl"""


class arithtst_atbl_short_interval_res(TstView):
    def __init__(self):
        # Validated on MySQL
        self.data = [
            {'id': 0, 'seconds_res': 160342920},
            {'id': 1, 'seconds_res': -84686400},
            {'id': 2, 'seconds_res': 332907420}
        ]
        self.sql = """CREATE MATERIALIZED VIEW atbl_short_interval_res AS SELECT
                      id,
                      CAST((seconds) AS BIGINT) AS seconds_res
                      FROM atbl_short_interval"""


# Equivalent MySQL program
# SELECT
# 	id,
#     TIMESTAMPDIFF(SECOND, c2, c1) AS f_c1
# FROM timestamp_tbl;


class arithtst_linterval_minus_linterval(TstView):
    def __init__(self):
        # Result validation is not required for local views
        self.data = []
        self.sql = """CREATE LOCAL VIEW linterval_minus_linterval AS SELECT
                      id,
                      months - INTERVAL '10' MONTH  AS f_c1
                      FROM atbl_long_interval"""


class arithtst_linterval_minus_linterval_res(TstView):
    def __init__(self):
        # checked manually
        self.data = [
            {'id': 0, 'f_c1_res': 50},
            {'id': 1, 'f_c1_res': -42},
            {'id': 2, 'f_c1_res': 116}
        ]
        self.sql = """CREATE MATERIALIZED VIEW linterval_minus_linterval_res AS SELECT
                      id,
                      CAST((f_c1) AS BIGINT) AS f_c1_res
                      FROM linterval_minus_linterval"""


class arithtst_sinterval_minus_sinterval(TstView):
    def __init__(self):
        # Result validation is not required for local views
        self.data = []
        self.sql = """CREATE LOCAL VIEW sinterval_minus_sinterval AS SELECT
                      id,
                      seconds - INTERVAL '1567890' SECOND AS f_c1
                      FROM atbl_short_interval"""


class arithtst_sinterval_minus_sinterval_res(TstView):
    def __init__(self):
        # Validated on Postgres
        self.data = [
            {'id': 0, 'f_c1_res': 158775030},
            {'id': 1, 'f_c1_res': -86254290},
            {'id': 2, 'f_c1_res': 331339530}
        ]
        self.sql = """CREATE MATERIALIZED VIEW sinterval_minus_sinterval_res AS SELECT
                      id,
                      CAST((f_c1) AS BIGINT) AS f_c1_res
                      FROM sinterval_minus_sinterval"""

# Equivalent Postgres SQL
# CREATE TABLE atbl_interval AS SELECT
#     id,
#     c1 - c2 AS c1_minus_c2
# FROM timestamp_tbl;
#
# SELECT
# 	id,
# 	EXTRACT(EPOCH FROM (c1_minus_c2 - INTERVAL '1567890 second')) AS f_c1
# FROM atbl_interval;


class arithtst_linterval_plus_linterval(TstView):
    def __init__(self):
        # Result validation is not required for local views
        self.data = []
        self.sql = """CREATE LOCAL VIEW linterval_plus_linterval AS SELECT
                      id,
                      months + INTERVAL '10' MONTH  AS f_c1
                      FROM atbl_long_interval"""


class arithtst_linterval_plus_linterval_res(TstView):
    def __init__(self):
        # checked manually
        self.data = [
            {'id': 0, 'f_c1_res': 70},
            {'id': 1, 'f_c1_res': -22},
            {'id': 2, 'f_c1_res': 136}
        ]
        self.sql = """CREATE MATERIALIZED VIEW linterval_plus_linterval_res AS SELECT
                      id,
                      CAST((f_c1) AS BIGINT) AS f_c1_res
                      FROM linterval_plus_linterval"""


class arithtst_sinterval_plus_sinterval(TstView):
    def __init__(self):
        # Result validation is not required for local views
        self.data = []
        self.sql = """CREATE LOCAL VIEW sinterval_plus_sinterval AS SELECT
                      id,
                      seconds + INTERVAL '1567890' SECOND AS f_c1
                      FROM atbl_short_interval"""


class arithtst_sinterval_plus_sinterval_res(TstView):
    def __init__(self):
        # Validated on Postgres
        self.data = [
            {'id': 0, 'f_c1_res': 161910810},
            {'id': 1, 'f_c1_res': -83118510},
            {'id': 2, 'f_c1_res': 334475310}
        ]
        self.sql = """CREATE MATERIALIZED VIEW sinterval_plus_sinterval_res AS SELECT
                      id,
                      CAST((f_c1) AS BIGINT) AS f_c1_res
                      FROM sinterval_plus_sinterval"""


# Equivalent Postgres SQL
# CREATE TABLE atbl_interval AS SELECT
#     id,
#     c1 - c2 AS c1_minus_c2
# FROM timestamp_tbl;
#
# SELECT
# 	id,
# 	EXTRACT(EPOCH FROM (c1_minus_c2 + INTERVAL '1567890 second')) AS f_c1
# FROM atbl_interval;


class arithtst_linterval_negation(TstView):
    def __init__(self):
        # Validated on Postgres
        self.data = [
            {'id': 0, 'f_c1': '2019-12-05', 'f_c2': '2014-12-05T08:27:00'},
            {'id': 1, 'f_c1': '2027-08-05', 'f_c2': '2022-08-05T08:27:00'},
            {'id': 2, 'f_c1': '2014-06-05', 'f_c2': '2009-06-05T08:27:00'}
        ]
        self.sql = """CREATE MATERIALIZED VIEW linterval_negation AS SELECT
                      id,
                      '2024-12-05':: DATE + (- months) AS f_c1,
                      '2019-12-05 08:27:00'::TIMESTAMP + (- months) AS f_c2
                      FROM atbl_long_interval"""


class arithtst_sinterval_negation(TstView):
    def __init__(self):
        # Validated on Postgres
        self.data = [
            {'id': 0, 'f_c1': '22:48:00', 'f_c2': '2014-11-05T12:45:00'},
            {'id': 1, 'f_c1': '22:30:00', 'f_c2': '2022-08-11T12:27:00'},
            {'id': 2, 'f_c1': '16:13:00', 'f_c2': '2009-05-18T06:10:00'}
        ]
        self.sql = """CREATE MATERIALIZED VIEW sinterval_negation AS SELECT
                       id,
                      '18:30:00':: TIME + (- seconds) AS f_c1,
                      '2019-12-05 08:27:00'::TIMESTAMP + (- seconds) AS f_c2
                      FROM atbl_short_interval"""


# Equivalent Postgres SQL:
# SELECT '2024-12-05':: DATE + (- INTERVAL 'value in months') date_neg;
# SELECT '2019-12-05 08:27:00'::TIMESTAMP + (- INTERVAL 'value in months/seconds') timestamp_neg;
# SELECT '18:30:00'::TIME + (- INTERVAL 'value in seconds') time_neg;


class arithtst_neg(TstView):
    def __init__(self):
        # Result validation is not required for local views
        self.data = [
        ]
        self.sql = """CREATE LOCAL VIEW neg AS SELECT
                      v1.id,
                      (-v1.months) AS months_neg,
                      (-v2.seconds) AS seconds_neg
                      FROM atbl_long_interval v1
                      JOIN atbl_short_interval v2 ON v1.id = v2.id;"""


class arithtst_neg_res(TstView):
    def __init__(self):
        # checked manually
        self.data = [
            {'id': 0, 'f_c1': -60, 'f_c2': -160342920},
            {'id': 1, 'f_c1': 32, 'f_c2': 84686400},
            {'id': 2, 'f_c1': -126, 'f_c2': -332907420}
        ]
        self.sql = """CREATE MATERIALIZED VIEW neg_res AS SELECT
                      id,
                      CAST((months_neg) AS BIGINT) AS f_c1,
                      CAST((seconds_neg) AS BIGINT) AS f_c2
                      FROM neg"""


class arithtst_linterval_mul_double(TstView):
    def __init__(self):
        # Result validation is not required for local views
        self.data = []
        self.sql = """CREATE LOCAL VIEW linterval_mul_double AS SELECT
                      id,
                      months * 2.583 AS f_c1
                      FROM atbl_long_interval"""


class arithtst_linterval_mul_double_res(TstView):
    def __init__(self):
        # checked manually
        self.data = [
            {'id': 0, 'f_c1': '+154'},
            {'id': 1, 'f_c1': '-82'},
            {'id': 2, 'f_c1': '+325'}
        ]
        self.sql = """CREATE MATERIALIZED VIEW linterval_mul_double_res AS SELECT
                      id,
                      CAST((f_c1) AS VARCHAR) AS f_c1
                      FROM linterval_mul_double"""


class arithtst_linterval_div_double(TstView):
    def __init__(self):
        # Result validation is not required for local views
        self.data = []
        self.sql = """CREATE LOCAL VIEW linterval_div_double AS SELECT
                      id,
                      months / 2.583 AS f_c1
                      FROM atbl_long_interval"""


class arithtst_linterval_div_double_res(TstView):
    def __init__(self):
        # checked manually
        self.data = [
            {'id': 0, 'f_c1': '+23'},
            {'id': 1, 'f_c1': '-12'},
            {'id': 2, 'f_c1': '+48'}
        ]
        self.sql = """CREATE MATERIALIZED VIEW linterval_div_double_res AS SELECT
                      id,
                      CAST((f_c1) AS VARCHAR) AS f_c1
                      FROM linterval_div_double"""