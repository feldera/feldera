from tests.aggregate_tests.aggtst_base import TstView


class arithtst_adate_minus_date(TstView):
    def __init__(self):
        # Result validation is not required for local views
        self.data = []
        self.sql = """CREATE LOCAL VIEW adate_minus_date AS SELECT
                      id,
                      (c1-c2)SECOND AS seconds,
                      (c1-c2)MINUTE AS minutes,
                      (c1-c2)HOUR AS hours,
                      (c1-c2)DAY AS days,
                      (c1-c2)MONTH AS months,
                      (c1-c2)YEAR AS years
                      FROM date_tbl"""


class arithtst_date_minus_date_res(TstView):
    def __init__(self):
        # Validated on MySQL
        self.data = [
            {'id': 0, 'sec_res': '+318211200.000000', 'min_res': '+5303520', 'hrs_res': '+88392', 'days_res': '+3683', 'mths_res': '+121', 'yrs_res': '+10'},
            {'id': 1, 'sec_res': '-84672000.000000', 'min_res': '-1411200', 'hrs_res': '-23520', 'days_res': '-980', 'mths_res': '-32', 'yrs_res': '-2'},
            {'id': 2, 'sec_res': '+648518400.000000', 'min_res': '+10808640', 'hrs_res': '+180144', 'days_res': '+7506', 'mths_res': '+246', 'yrs_res': '+20'}
        ]
        self.sql = """CREATE MATERIALIZED VIEW date_minus_date_res AS SELECT
                      id,
                      CAST((seconds) AS VARCHAR) AS sec_res,
                      CAST((minutes) AS VARCHAR) AS min_res,
                      CAST((hours) AS VARCHAR) AS hrs_res,
                      CAST((days) AS VARCHAR) AS days_res,
                      CAST((months) AS VARCHAR) AS mths_res,
                      CAST((years) AS VARCHAR) AS yrs_res
                      FROM adate_minus_date"""


# Equivalent SQL for MySQL
# SELECT
# 	id,
#     TIMESTAMPDIFF(SECOND, c2, c1) AS seconds,
#     TIMESTAMPDIFF(MINUTE, c2, c1) AS minutes,
#     TIMESTAMPDIFF(HOUR, c2, c1) AS hours,
#     TIMESTAMPDIFF(DAY, c2, c1) AS days,
#     TIMESTAMPDIFF(MONTH, c2, c1) AS months,
#     TIMESTAMPDIFF(YEAR, c2, c1) AS years
# FROM date_tbl;


# Using explicit interval types for subtraction and addition
class arithtst_date_minus_sinterval(TstView):
    def __init__(self):
        # Validated on MySQL
        # The subtraction of all interval types produces the same result as subtracting MINUTE type in MySQL
        self.data = [
            {'id': 0, 'seconds_c1': '2014-11-05', 'minutes_c1': '2014-11-05', 'hours_c1': '2014-11-05', 'days_c1': '2014-11-05'},
            {'id': 1, 'seconds_c1': '2023-02-26', 'minutes_c1': '2023-02-26', 'hours_c1': '2023-02-26', 'days_c1': '2023-02-26'},
            {'id': 2, 'seconds_c1': '1948-12-02', 'minutes_c1': '1948-12-02', 'hours_c1': '1948-12-02', 'days_c1': '1948-12-02'}
        ]
        self.sql = """CREATE MATERIALIZED VIEW date_minus_sinterval AS SELECT
                      v1.id,
                      c1 - (v1.seconds) AS seconds_c1,
                      c1 - (v1.minutes) AS minutes_c1,
                      c1 - (v1.hours) AS hours_c1,
                      c1 - (v1.days) AS days_c1
                      FROM adate_minus_date v1
                      JOIN date_tbl v2 ON v1.id = v2.id;"""


class arithtst_date_minus_linterval(TstView):
    def __init__(self):
        # Validated on MySQL
        # The result of subtracting MONTH type interval matches with MySQL
        # whereas YEARS behaves similarly as MONTHS in terms of accuracy
        self.data = [
            {'id': 0, 'months_c1': '2014-11-05', 'years_c1': '2014-11-05'},
            {'id': 1, 'months_c1': '2023-02-21', 'years_c1': '2023-02-21'},
            {'id': 2, 'months_c1': '1948-12-21', 'years_c1': '1948-12-21'}
        ]
        self.sql = """CREATE MATERIALIZED VIEW date_minus_linterval AS SELECT
                      v1.id,
                      c1 - (v1.months) AS months_c1,
                      c1 - (v1.years) AS years_c1
                      FROM adate_minus_date v1
                      JOIN date_tbl v2 ON v1.id = v2.id;"""


class arithtst_date_plus_sinterval(TstView):
    def __init__(self):
        # Validated on MySQL
        self.data = [
            {'id': 0, 'seconds_c1': '2035-01-05', 'minutes_c1': '2035-01-05', 'hours_c1': '2035-01-05', 'days_c1': '2035-01-05'},
            {'id': 1, 'seconds_c1': '2017-10-15', 'minutes_c1': '2017-10-15', 'hours_c1': '2017-10-15', 'days_c1': '2017-10-15'},
            {'id': 2, 'seconds_c1': '1990-01-08', 'minutes_c1': '1990-01-08', 'hours_c1': '1990-01-08', 'days_c1': '1990-01-08'}
        ]
        self.sql = """CREATE MATERIALIZED VIEW date_plus_interval AS SELECT
                      v1.id,
                      c1 + (v1.seconds) AS seconds_c1,
                      c1 + (v1.minutes) AS minutes_c1,
                      c1 + (v1.hours) AS hours_c1,
                      c1 + (v1.days) AS days_c1
                      FROM adate_minus_date v1
                      JOIN date_tbl v2 ON v1.id = v2.id;"""


class arithtst_date_plus_linterval(TstView):
    def __init__(self):
        # Validated on MySQL
        # The result of adding MONTH type interval matches with MySQL
        # whereas YEARS behaves similarly as MONTHS in terms of accuracy
        self.data = [
            {'id': 0, 'months_c1': '2035-01-05', 'years_c1': '2035-01-05'},
            {'id': 1, 'months_c1': '2017-10-21', 'years_c1': '2017-10-21'},
            {'id': 2, 'months_c1': '1989-12-21', 'years_c1': '1989-12-21'}
        ]
        self.sql = """CREATE MATERIALIZED VIEW date_plus_linterval AS SELECT
                      v1.id,
                      c1 + (v1.months) AS months_c1,
                      c1 + (v1.years) AS years_c1
                      FROM adate_minus_date v1
                      JOIN date_tbl v2 ON v1.id = v2.id;"""


# Using interval type cast as VARCHAR for subtraction and addition
class arithtst_dt_sub_sinterval(TstView):
    def __init__(self):
        # Validated on MySQL
        self.data = [
            {'id': 0, 'sec_c1': '2014-11-05', 'min_c1': '2014-11-05', 'hrs_c1': '2014-11-05', 'day_c1': '2014-11-05'},
            {'id': 1, 'sec_c1': '2023-02-26', 'min_c1': '2023-02-26', 'hrs_c1': '2023-02-26', 'day_c1': '2023-02-26'},
            {'id': 2, 'sec_c1': '1948-12-02', 'min_c1': '1948-12-02', 'hrs_c1': '1948-12-02', 'day_c1': '1948-12-02'}
        ]
        self.sql = """CREATE MATERIALIZED VIEW dt_sub_sinterval AS SELECT
                      v1.id,
                      c1 - INTERVAL v1.sec_res SECOND AS sec_c1,
                      c1 - INTERVAL v1.min_res MINUTE AS min_c1,
                      c1 - INTERVAL v1.hrs_res HOUR AS hrs_c1,
                      c1 - INTERVAL v1.days_res DAY AS day_c1
                      FROM date_minus_date_res v1
                      JOIN date_tbl v2 ON v1.id = v2.id;"""


class arithtst_dt_sub_linterval(TstView):
    def __init__(self):
        # Validated on MySQL
        self.data = [
            {'id': 0, 'mths_c1': '2014-11-05', 'yrs_c1': '2014-12-05'},
            {'id': 1, 'mths_c1': '2023-02-21', 'yrs_c1': '2022-06-21'},
            {'id': 2, 'mths_c1': '1948-12-21', 'yrs_c1': '1949-06-21'}
        ]
        self.sql = """CREATE MATERIALIZED VIEW dt_sub_linterval AS SELECT
                      v1.id,
                      c1 - INTERVAL v1.mths_res MONTH AS mths_c1,
                      c1 - INTERVAL v1.yrs_res YEAR AS yrs_c1
                      FROM date_minus_date_res v1
                      JOIN date_tbl v2 ON v1.id = v2.id;"""


class arithtst_dt_add_sinterval(TstView):
    def __init__(self):
        # Validated on MySQL
        self.data = [
            {'id': 0, 'sec_c1': '2035-01-05', 'min_c1': '2035-01-05', 'hrs_c1': '2035-01-05', 'day_c1': '2035-01-05'},
            {'id': 1, 'sec_c1': '2017-10-15', 'min_c1': '2017-10-15', 'hrs_c1': '2017-10-15', 'day_c1': '2017-10-15'},
            {'id': 2, 'sec_c1': '1990-01-08', 'min_c1': '1990-01-08', 'hrs_c1': '1990-01-08', 'day_c1': '1990-01-08'}
        ]
        self.sql = """CREATE MATERIALIZED VIEW dt_add_sinterval AS SELECT
                      v1.id,
                      c1 + INTERVAL v1.sec_res SECOND AS sec_c1,
                      c1 + INTERVAL v1.min_res MINUTE AS min_c1,
                      c1 + INTERVAL v1.hrs_res HOUR AS hrs_c1,
                      c1 + INTERVAL v1.days_res DAY AS day_c1
                      FROM date_minus_date_res v1
                      JOIN date_tbl v2 ON v1.id = v2.id;"""


class arithtst_dt_add_linterval(TstView):
    def __init__(self):
        # Validated on MySQL
        self.data = [
            {'id': 0, 'mths_c1': '2035-01-05', 'yrs_c1': '2034-12-05'},
            {'id': 1, 'mths_c1': '2017-10-21', 'yrs_c1': '2018-06-21'},
            {'id': 2, 'mths_c1': '1989-12-21', 'yrs_c1': '1989-06-21'}
        ]
        self.sql = """CREATE MATERIALIZED VIEW dt_add_linterval AS SELECT
                      v1.id,
                      c1 + INTERVAL v1.mths_res MONTH AS mths_c1,
                      c1 + INTERVAL v1.yrs_res YEAR AS yrs_c1
                      FROM date_minus_date_res v1
                      JOIN date_tbl v2 ON v1.id = v2.id;"""


# Equivalent SQL for MySQL
# CREATE TABLE interval_values AS
# SELECT
#     id,
#     TIMESTAMPDIFF(SECOND, c2, c1) AS seconds,
#     TIMESTAMPDIFF(MINUTE, c2, c1) AS minutes,
#     TIMESTAMPDIFF(HOUR, c2, c1) AS hours,
#     TIMESTAMPDIFF(DAY, c2, c1) AS days,
#     TIMESTAMPDIFF(MONTH, c2, c1) AS months,
#     TIMESTAMPDIFF(YEAR, c2, c1) AS years
# FROM date_tbl;
#
# SELECT
#     v1.id,
#     DATE_SUB(c1, INTERVAL v1.seconds SECOND) AS sec_c1,
#     DATE_SUB(c1, INTERVAL v1.minutes MINUTE) AS min_c1,
#     DATE_SUB(c1, INTERVAL v1.hours HOUR) AS hrs_c1,
#     DATE_SUB(c1, INTERVAL v1.days DAY) AS day_c1,
#     DATE_SUB(c1, INTERVAL v1.months MONTH) AS mths_c1,
#     DATE_SUB(c1, INTERVAL v1.years YEAR) AS yrs_c1
# FROM interval_values v1
# JOIN date_tbl v2 ON v1.id = v2.id;


class arithtst_adt_minus_dt(TstView):
    def __init__(self):
        # Result validation not required for local views
        self.data = []
        self.sql = """CREATE LOCAL VIEW adt_minus_dt AS SELECT
                      id,
                      (c1-c2)YEAR TO MONTH  AS ytm,
                      (c1-c2)DAY TO HOUR AS dth,
                      (c1-c2)DAY TO MINUTE AS dtm,
                      (c1-c2)DAY TO SECOND AS dts,
                      (c1-c2)HOUR TO MINUTE AS htm,
                      (c1-c2)HOUR TO SECOND AS hts,
                      (c1-c2)MINUTE TO SECOND AS mts
                      FROM date_tbl"""


class arithtst_dt_minus_dt_res(TstView):
    def __init__(self):
        # Validated on Postgres
        self.data = [
            {'id': 0, 'ytm_res': '+10-1', 'dth_res': '+3683 00', 'dtm_res': '+3683 00:00', 'dts_res': '+3683 00:00:00.000000', 'htm_res': '+88392:00', 'hts_res': '+88392:00:00.000000', 'mts_res': '+5303520:00.000000'},
            {'id': 1, 'ytm_res': '-2-8', 'dth_res': '-980 00', 'dtm_res': '-980 00:00', 'dts_res': '-980 00:00:00.000000', 'htm_res': '-23520:00', 'hts_res': '-23520:00:00.000000', 'mts_res': '-1411200:00.000000'},
            {'id': 2, 'ytm_res': '+20-6', 'dth_res': '+7506 00', 'dtm_res': '+7506 00:00', 'dts_res': '+7506 00:00:00.000000', 'htm_res': '+180144:00', 'hts_res': '+180144:00:00.000000', 'mts_res': '+10808640:00.000000'}
        ]
        self.sql = """CREATE MATERIALIZED VIEW dt_minus_dt_res AS SELECT
                      id,
                      CAST((ytm) AS VARCHAR) AS ytm_res,
                      CAST((dth) AS VARCHAR) AS dth_res,
                      CAST((dtm) AS VARCHAR) AS dtm_res,
                      CAST((dts) AS VARCHAR) AS dts_res,
                      CAST((htm) AS VARCHAR) AS htm_res,
                      CAST((hts) AS VARCHAR) AS hts_res,
                      CAST((mts) AS VARCHAR) AS mts_res
                      FROM adt_minus_dt"""


# Equivalent SQL for Postgres
# SELECT
#     id,
#     AGE(c1, c2) AS diff,
#     c1 - c2 AS days,
#     (c1 - c2)*24 AS hours,
#     (c1 - c2)*24*60 AS minutes
# FROM date_tbl;

# eg:
# diff = 20 years 6 mons 19 days,
# days = 7506,
# hours = 180144
# minutes = 10808640


# Using explicit interval types for subtraction and addition
class arithtst_dt_minus_interval(TstView):
    def __init__(self):
        # Validated on Postgres
        self.data = [
            {'id': 0, 'ytm': '2014-11-05', 'dth': '2014-11-05', 'dtm': '2014-11-05', 'dts': '2014-11-05', 'htm': '2014-11-05', 'hts': '2014-11-05', 'mts': '2014-11-05'},
            {'id': 1, 'ytm': '2023-02-21', 'dth': '2023-02-26', 'dtm': '2023-02-26', 'dts': '2023-02-26', 'htm': '2023-02-26', 'hts': '2023-02-26', 'mts': '2023-02-26'},
            {'id': 2, 'ytm': '1948-12-21', 'dth': '1948-12-02', 'dtm': '1948-12-02', 'dts': '1948-12-02', 'htm': '1948-12-02', 'hts': '1948-12-02', 'mts': '1948-12-02'}
        ]
        self.sql = """CREATE MATERIALIZED VIEW dt_minus_interval AS SELECT
                              v1.id,
                              c1 - (v1.ytm)  AS ytm,
                              c1 - (v1.dth)  AS dth,
                              c1 - (v1.dtm)  AS dtm,
                              c1 - (v1.dts)  AS dts,
                              c1 - (v1.htm)  AS htm,
                              c1 - (v1.hts)  AS hts,
                              c1 - (v1.mts)  AS mts
                              FROM adt_minus_dt v1
                              JOIN date_tbl v2 ON v1.id = v2.id"""


class arithtst_dt_plus_interval(TstView):
    def __init__(self):
        # Validated on Postgres
        self.data = [
            {'id': 0, 'ytm': '2035-01-05', 'dth': '2035-01-05', 'dtm': '2035-01-05', 'dts': '2035-01-05', 'htm': '2035-01-05', 'hts': '2035-01-05', 'mts': '2035-01-05'},
            {'id': 1, 'ytm': '2017-10-21', 'dth': '2017-10-15', 'dtm': '2017-10-15', 'dts': '2017-10-15', 'htm': '2017-10-15', 'hts': '2017-10-15', 'mts': '2017-10-15'},
            {'id': 2, 'ytm': '1989-12-21', 'dth': '1990-01-08', 'dtm': '1990-01-08', 'dts': '1990-01-08', 'htm': '1990-01-08', 'hts': '1990-01-08', 'mts': '1990-01-08'}
        ]
        self.sql = """CREATE MATERIALIZED VIEW dt_plus_interval AS SELECT
                              v1.id,
                              c1 + (v1.ytm)  AS ytm,
                              c1 + (v1.dth)  AS dth,
                              c1 + (v1.dtm)  AS dtm,
                              c1 + (v1.dts)  AS dts,
                              c1 + (v1.htm)  AS htm,
                              c1 + (v1.hts)  AS hts,
                              c1 + (v1.mts)  AS mts
                              FROM adt_minus_dt v1
                              JOIN date_tbl v2 ON v1.id = v2.id"""


# Using interval type cast as VARCHAR for subtraction and addition
class arithtst_dt_minus_dttinterval(TstView):
    def __init__(self):
        # Validated on Postgres
        self.data = [
            {'id': 0, 'ytm': '2014-11-05', 'dth': '2014-11-05', 'dtm': '2014-11-05', 'dts': '2014-11-05', 'htm': '2014-11-05', 'hts': '2014-11-05', 'mts': '2014-11-05'},
            {'id': 1, 'ytm': '2023-02-21', 'dth': '2023-02-26', 'dtm': '2023-02-26', 'dts': '2023-02-26', 'htm': '2023-02-26', 'hts': '2023-02-26', 'mts': '2023-02-26'},
            {'id': 2, 'ytm': '1948-12-21', 'dth': '1948-12-02', 'dtm': '1948-12-02', 'dts': '1948-12-02', 'htm': '1948-12-02', 'hts': '1948-12-02', 'mts': '1948-12-02'}
        ]
        self.sql = """CREATE MATERIALIZED VIEW dt_minus_dttinterval AS SELECT
                              v1.id,
                              c1 - CAST(v1.ytm_res AS INTERVAL YEAR TO MONTH)  AS ytm,
                              c1 - CAST(v1.dth_res AS INTERVAL DAY TO HOUR)  AS dth,
                              c1 - CAST(v1.dtm_res AS INTERVAL DAY TO MINUTE)  AS dtm,
                              c1 - CAST(v1.dts_res AS INTERVAL DAY TO SECOND)  AS dts,
                              c1 - CAST(v1.htm_res AS INTERVAL HOUR TO MINUTE)  AS htm,
                              c1 - CAST(v1.hts_res AS INTERVAL HOUR TO SECOND)  AS hts,
                              c1 - CAST(v1.mts_res AS INTERVAL MINUTE TO SECOND)  AS mts
                              FROM dt_minus_dt_res v1
                              JOIN date_tbl v2 ON v1.id = v2.id"""


class arithtst_dt_plus_dttinterval(TstView):
    def __init__(self):
        # Validated on Postgres
        self.data = [
            {'id': 0, 'ytm': '2035-01-05', 'dth': '2035-01-05', 'dtm': '2035-01-05', 'dts': '2035-01-05', 'htm': '2035-01-05', 'hts': '2035-01-05', 'mts': '2035-01-05'},
            {'id': 1, 'ytm': '2017-10-21', 'dth': '2017-10-15', 'dtm': '2017-10-15', 'dts': '2017-10-15', 'htm': '2017-10-15', 'hts': '2017-10-15', 'mts': '2017-10-15'},
            {'id': 2, 'ytm': '1989-12-21', 'dth': '1990-01-08', 'dtm': '1990-01-08', 'dts': '1990-01-08', 'htm': '1990-01-08', 'hts': '1990-01-08', 'mts': '1990-01-08'}
        ]
        self.sql = """CREATE MATERIALIZED VIEW dt_plus_dttinterval AS SELECT
                              v1.id,
                              c1 + CAST(v1.ytm_res AS INTERVAL YEAR TO MONTH)  AS ytm,
                              c1 + CAST(v1.dth_res AS INTERVAL DAY TO HOUR)  AS dth,
                              c1 + CAST(v1.dtm_res AS INTERVAL DAY TO MINUTE)  AS dtm,
                              c1 + CAST(v1.dts_res AS INTERVAL DAY TO SECOND)  AS dts,
                              c1 + CAST(v1.htm_res AS INTERVAL HOUR TO MINUTE)  AS htm,
                              c1 + CAST(v1.hts_res AS INTERVAL HOUR TO SECOND)  AS hts,
                              c1 + CAST(v1.mts_res AS INTERVAL MINUTE TO SECOND)  AS mts
                              FROM dt_minus_dt_res v1
                              JOIN date_tbl v2 ON v1.id = v2.id"""


# Equivalent SQL for Postgres
# SELECT
#     '1969-06-21'::DATE - INTERVAL '20 year 6 month' YEAR TO MONTH AS ytm,
#     '1969-06-21'::DATE - INTERVAL '7506 day 00 hour' DAY TO HOUR AS dth,
#     '1969-06-21'::DATE - INTERVAL '7506 day 00:00 minute' DAY TO MINUTE AS dtm,
#     '1969-06-21'::DATE - INTERVAL '7506 day 00:00:00.000000 second' DAY TO SECOND AS dts,
#     '1969-06-21'::DATE - INTERVAL '180144 hour 0 minute' HOUR TO MINUTE AS htm,
#     '1969-06-21'::DATE - INTERVAL '180144 hour 0 second' HOUR TO SECOND AS hts,
#     '1969-06-21'::DATE - INTERVAL '10808640 minute 00.000000 second' MINUTE TO SECOND AS mts;


class arithtst_bneg_date(TstView):
    def __init__(self):
        # Checked manually
        self.data = [
            {'id': 0, 'seconds_neg': '-318211200.000000', 'minutes_neg': '-5303520', 'hours_neg': '-88392', 'days_neg': '-3683', 'months_neg': '-121', 'years_neg': '-10'},
            {'id': 1, 'seconds_neg': '+84672000.000000', 'minutes_neg': '+1411200', 'hours_neg': '+23520', 'days_neg': '+980', 'months_neg': '+32', 'years_neg': '+2'},
            {'id': 2, 'seconds_neg': '-648518400.000000', 'minutes_neg': '-10808640', 'hours_neg': '-180144', 'days_neg': '-7506', 'months_neg': '-246', 'years_neg': '-20'},
        ]
        self.sql = """CREATE  MATERIALIZED VIEW bneg_date AS SELECT
                      id,
                      CAST(-seconds AS VARCHAR) AS seconds_neg,
                      CAST(-minutes AS VARCHAR) AS minutes_neg,
                      CAST(-hours AS VARCHAR) AS hours_neg,
                      CAST(-days AS VARCHAR) AS days_neg,
                      CAST(-months AS VARCHAR) AS months_neg,
                      CAST(-years AS VARCHAR) AS years_neg
                      FROM adate_minus_date;"""


class arithtst_bneg_dtinterval(TstView):
    def __init__(self):
        # Checked manually
        self.data = [
            {'id': 0, 'ytm_neg': '-10-1', 'dth_neg': '-3683 00', 'dtm_neg': '-3683 00:00', 'dts_neg': '-3683 00:00:00.000000', 'htm_neg': '-88392:00', 'hts_neg': '-88392:00:00.000000', 'mts_neg': '-5303520:00.000000'},
            {'id': 1, 'ytm_neg': '+2-8', 'dth_neg': '+980 00', 'dtm_neg': '+980 00:00', 'dts_neg': '+980 00:00:00.000000', 'htm_neg': '+23520:00', 'hts_neg': '+23520:00:00.000000', 'mts_neg': '+1411200:00.000000'},
            {'id': 2, 'ytm_neg': '-20-6', 'dth_neg': '-7506 00', 'dtm_neg': '-7506 00:00', 'dts_neg': '-7506 00:00:00.000000', 'htm_neg': '-180144:00', 'hts_neg': '-180144:00:00.000000', 'mts_neg': '-10808640:00.000000'},
        ]
        self.sql = """CREATE MATERIALIZED VIEW bneg_dtinterval AS SELECT
                      id,
                      CAST(-ytm AS VARCHAR) AS ytm_neg,
                      CAST(-dth AS VARCHAR) AS dth_neg,
                      CAST(-dtm AS VARCHAR) AS dtm_neg,
                      CAST(-dts AS VARCHAR) AS dts_neg,
                      CAST(-htm AS VARCHAR) AS htm_neg,
                      CAST(-hts AS VARCHAR) AS hts_neg,
                      CAST(-mts AS VARCHAR) AS mts_neg
                      FROM adt_minus_dt"""