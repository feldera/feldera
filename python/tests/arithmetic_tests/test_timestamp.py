from tests.aggregate_tests.aggtst_base import TstView


class arithtst_atimestamp_minus_timestamp(TstView):
    def __init__(self):
        # Result validation is not required for local views
        self.data = []
        self.sql = """CREATE LOCAL VIEW atimestamp_minus_timestamp AS SELECT
                      id,
                      (c1-c2)SECOND AS seconds,
                      (c1-c2)MINUTE AS minutes,
                      (c1-c2)HOUR AS hours,
                      (c1-c2)DAY AS days,
                      (c1-c2)MONTH AS months,
                      (c1-c2)YEAR AS years
                      FROM timestamp_tbl"""


class arithtst_timestamp_minus_timestamp_str(TstView):
    def __init__(self):
        # Validated on MySQL
        self.data = [
            {
                "id": 0,
                "sec_str": "+160342920.000000",
                "min_str": "+2672382",
                "hrs_str": "+44539",
                "days_str": "+1855",
                "mths_str": "+60",
                "yrs_str": "+5",
            },
            {
                "id": 1,
                "sec_str": "-84686400.000000",
                "min_str": "-1411440",
                "hrs_str": "-23524",
                "days_str": "-980",
                "mths_str": "-32",
                "yrs_str": "-2",
            },
            {
                "id": 2,
                "sec_str": "+332907420.000000",
                "min_str": "+5548457",
                "hrs_str": "+92474",
                "days_str": "+3853",
                "mths_str": "+126",
                "yrs_str": "+10",
            },
        ]
        self.sql = """CREATE MATERIALIZED VIEW timestamp_minus_timestamp_str AS SELECT
                      id,
                      CAST((seconds) AS VARCHAR) AS sec_str,
                      CAST((minutes) AS VARCHAR) AS min_str,
                      CAST((hours) AS VARCHAR) AS hrs_str,
                      CAST((days) AS VARCHAR) AS days_str,
                      CAST((months) AS VARCHAR) AS mths_str,
                      CAST((years) AS VARCHAR) AS yrs_str
                      FROM atimestamp_minus_timestamp"""


# Equivalent SQL for MySQL
# SELECT
# 	id,
#     TIMESTAMPDIFF(SECOND, c2, c1) AS seconds,
#     TIMESTAMPDIFF(MINUTE, c2, c1) AS minutes,
#     TIMESTAMPDIFF(HOUR, c2, c1) AS hours,
#     TIMESTAMPDIFF(DAY, c2, c1) AS days,
#     TIMESTAMPDIFF(MONTH, c2, c1) AS months,
#     TIMESTAMPDIFF(YEAR, c2, c1) AS years
# FROM timestamp_tbl;


# Using explicit interval types for subtraction and addition
class arithtst_timestamp_minus_sinterval(TstView):
    def __init__(self):
        # Validated on MySQL
        # The subtraction of all interval types produce the same result as subtracting MINUTE type in MySQL
        self.data = [
            {
                "id": 0,
                "seconds": "2014-11-05T12:45:00",
                "minutes": "2014-11-05T12:45:00",
                "hours": "2014-11-05T12:45:00",
                "days": "2014-11-05T12:45:00",
            },
            {
                "id": 1,
                "seconds": "2023-02-26T18:00:00",
                "minutes": "2023-02-26T18:00:00",
                "hours": "2023-02-26T18:00:00",
                "days": "2023-02-26T18:00:00",
            },
            {
                "id": 2,
                "seconds": "1948-12-02T09:15:00",
                "minutes": "1948-12-02T09:15:00",
                "hours": "1948-12-02T09:15:00",
                "days": "1948-12-02T09:15:00",
            },
        ]
        self.sql = """CREATE MATERIALIZED VIEW timestamp_minus_sinterval AS SELECT
                      v1.id,
                      c1 - (v1.seconds) AS seconds,
                      c1 - (v1.minutes) AS minutes,
                      c1 - (v1.hours) AS hours,
                      c1 - (v1.days) AS days
                      FROM atimestamp_minus_timestamp v1
                      JOIN timestamp_tbl v2 ON v1.id = v2.id;"""


class arithtst_timestamp_minus_linterval(TstView):
    def __init__(self):
        # Validated on MySQL
        # The result of subtracting MONTH type interval matches with MySQL
        # whereas YEARS behaves similarly as MONTHS in terms of accuracy
        self.data = [
            {
                "id": 0,
                "months": "2014-12-05T08:27:00",
                "years": "2014-12-05T08:27:00",
            },
            {
                "id": 1,
                "months": "2023-02-21T14:00:00",
                "years": "2023-02-21T14:00:00",
            },
            {
                "id": 2,
                "months": "1948-12-21T11:32:00",
                "years": "1948-12-21T11:32:00",
            },
        ]
        self.sql = """CREATE MATERIALIZED VIEW timestamp_minus_linterval AS SELECT
                      v1.id,
                      c1 - (v1.months) AS months,
                      c1 - (v1.years) AS years
                      FROM atimestamp_minus_timestamp v1
                      JOIN timestamp_tbl v2 ON v1.id = v2.id;"""


class arithtst_timestamp_plus_sinterval(TstView):
    def __init__(self):
        # Validated on MySQL
        # The addition of all interval types produce the same result as adding MINUTE type in MySQL
        self.data = [
            {
                "id": 0,
                "seconds": "2025-01-03T04:09:00",
                "minutes": "2025-01-03T04:09:00",
                "hours": "2025-01-03T04:09:00",
                "days": "2025-01-03T04:09:00",
            },
            {
                "id": 1,
                "seconds": "2017-10-15T10:00:00",
                "minutes": "2017-10-15T10:00:00",
                "hours": "2017-10-15T10:00:00",
                "days": "2017-10-15T10:00:00",
            },
            {
                "id": 2,
                "seconds": "1970-01-07T13:49:00",
                "minutes": "1970-01-07T13:49:00",
                "hours": "1970-01-07T13:49:00",
                "days": "1970-01-07T13:49:00",
            },
        ]
        self.sql = """CREATE MATERIALIZED VIEW timestamp_plus_sinterval AS SELECT
                      v1.id,
                      c1 + (v1.seconds) AS seconds,
                      c1 + (v1.minutes) AS minutes,
                      c1 + (v1.hours) AS hours,
                      c1 + (v1.days) AS days
                      FROM atimestamp_minus_timestamp v1
                      JOIN timestamp_tbl v2 ON v1.id = v2.id;"""


class arithtst_timestamp_plus_linterval(TstView):
    def __init__(self):
        # Validated on MySQL
        # The result of adding MONTH type interval matches with MySQL
        # whereas YEARS behaves similarly as MONTHS in terms of accuracy
        self.data = [
            {
                "id": 0,
                "months": "2024-12-05T08:27:00",
                "years": "2024-12-05T08:27:00",
            },
            {
                "id": 1,
                "months": "2017-10-21T14:00:00",
                "years": "2017-10-21T14:00:00",
            },
            {
                "id": 2,
                "months": "1969-12-21T11:32:00",
                "years": "1969-12-21T11:32:00",
            },
        ]
        self.sql = """CREATE MATERIALIZED VIEW timestamp_plus_linterval AS SELECT
                      v1.id,
                      c1 + (v1.months) AS months,
                      c1 + (v1.years) AS years
                      FROM atimestamp_minus_timestamp v1
                      JOIN timestamp_tbl v2 ON v1.id = v2.id;"""


# Using interval type cast as VARCHAR for subtraction and addition
class arithtst_ts_sub_sinterval(TstView):
    def __init__(self):
        # Validated on MySQL
        self.data = [
            {
                "id": 0,
                "sec_str": "2014-11-05T12:45:00",
                "min_str": "2014-11-05T12:45:00",
                "hrs_str": "2014-11-05T13:27:00",
                "day_str": "2014-11-06T08:27:00",
            },
            {
                "id": 1,
                "sec_str": "2023-02-26T18:00:00",
                "min_str": "2023-02-26T18:00:00",
                "hrs_str": "2023-02-26T18:00:00",
                "day_str": "2023-02-26T14:00:00",
            },
            {
                "id": 2,
                "sec_str": "1948-12-02T09:15:00",
                "min_str": "1948-12-02T09:15:00",
                "hrs_str": "1948-12-02T09:32:00",
                "day_str": "1948-12-02T11:32:00",
            },
        ]
        self.sql = """CREATE MATERIALIZED VIEW ts_sub_sinterval AS SELECT
                      v1.id,
                      c1 - INTERVAL v1.sec_str SECOND AS sec_str,
                      c1 - INTERVAL v1.min_str MINUTE AS min_str,
                      c1 - INTERVAL v1.hrs_str HOUR AS hrs_str,
                      c1 - INTERVAL v1.days_str DAY AS day_str
                      FROM timestamp_minus_timestamp_str v1
                      JOIN timestamp_tbl v2 ON v1.id = v2.id;"""


class arithtst_ts_sub_linterval(TstView):
    def __init__(self):
        # Validated on MySQL
        self.data = [
            {
                "id": 0,
                "mths_str": "2014-12-05T08:27:00",
                "yrs_str": "2014-12-05T08:27:00",
            },
            {
                "id": 1,
                "mths_str": "2023-02-21T14:00:00",
                "yrs_str": "2022-06-21T14:00:00",
            },
            {
                "id": 2,
                "mths_str": "1948-12-21T11:32:00",
                "yrs_str": "1949-06-21T11:32:00",
            },
        ]
        self.sql = """CREATE MATERIALIZED VIEW ts_sub_linterval AS SELECT
                      v1.id,
                      c1 - INTERVAL v1.mths_str MONTH AS mths_str,
                      c1 - INTERVAL v1.yrs_str YEAR AS yrs_str
                      FROM timestamp_minus_timestamp_str v1
                      JOIN timestamp_tbl v2 ON v1.id = v2.id;"""


class arithtst_ts_add_sinterval(TstView):
    def __init__(self):
        # Validated on MySQL
        self.data = [
            {
                "id": 0,
                "sec_str": "2025-01-03T04:09:00",
                "min_str": "2025-01-03T04:09:00",
                "hrs_str": "2025-01-03T03:27:00",
                "day_str": "2025-01-02T08:27:00",
            },
            {
                "id": 1,
                "sec_str": "2017-10-15T10:00:00",
                "min_str": "2017-10-15T10:00:00",
                "hrs_str": "2017-10-15T10:00:00",
                "day_str": "2017-10-15T14:00:00",
            },
            {
                "id": 2,
                "sec_str": "1970-01-07T13:49:00",
                "min_str": "1970-01-07T13:49:00",
                "hrs_str": "1970-01-07T13:32:00",
                "day_str": "1970-01-07T11:32:00",
            },
        ]
        self.sql = """CREATE MATERIALIZED VIEW ts_add_sinterval AS SELECT
                      v1.id,
                      c1 + INTERVAL v1.sec_str SECOND AS sec_str,
                      c1 + INTERVAL v1.min_str MINUTE AS min_str,
                      c1 + INTERVAL v1.hrs_str HOUR AS hrs_str,
                      c1 + INTERVAL v1.days_str DAY AS day_str
                      FROM timestamp_minus_timestamp_str v1
                      JOIN timestamp_tbl v2 ON v1.id = v2.id;"""


class arithtst_ts_add_linterval(TstView):
    def __init__(self):
        # Validated on MySQL
        self.data = [
            {
                "id": 0,
                "mths_str": "2024-12-05T08:27:00",
                "yrs_str": "2024-12-05T08:27:00",
            },
            {
                "id": 1,
                "mths_str": "2017-10-21T14:00:00",
                "yrs_str": "2018-06-21T14:00:00",
            },
            {
                "id": 2,
                "mths_str": "1969-12-21T11:32:00",
                "yrs_str": "1969-06-21T11:32:00",
            },
        ]
        self.sql = """CREATE MATERIALIZED VIEW ts_add_linterval AS SELECT
                      v1.id,
                      c1 + INTERVAL v1.mths_str MONTH AS mths_str,
                      c1 + INTERVAL v1.yrs_str YEAR AS yrs_str
                      FROM timestamp_minus_timestamp_str v1
                      JOIN timestamp_tbl v2 ON v1.id = v2.id;"""


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
# FROM timestamp_tbl;
#
# SELECT
#     v1.id,
#     DATE_SUB(c1, INTERVAL v1.seconds SECOND) AS sec_c1,
#     DATE_SUB(c1, INTERVAL v1.minutes MINUTE) AS min_c1,
#     DATE_SUB(c1, INTERVAL v1.hours HOUR) AS hrs_c1,
#     DATE_SUB(c1, INTERVAL v1.days DAY) AS day_c1,
#     DATE_SUB(c1, INTERVAL v1.months MONTH) AS mths_c1,  -- This is correct
#     DATE_SUB(c1, INTERVAL v1.years YEAR) AS yrs_c1  -- This is correct
# FROM interval_values v1
# JOIN timestamp_tbl v2 ON v1.id = v2.id;


class arithtst_ats_minus_ts(TstView):
    def __init__(self):
        # Result validation not required for local views
        self.data = []
        self.sql = """CREATE LOCAL VIEW ats_minus_ts AS SELECT
                      id,
                      (c1-c2)YEAR TO MONTH  AS ytm,
                      (c1-c2)DAY TO HOUR AS dth,
                      (c1-c2)DAY TO MINUTE AS dtm,
                      (c1-c2)DAY TO SECOND AS dts,
                      (c1-c2)HOUR TO MINUTE AS htm,
                      (c1-c2)HOUR TO SECOND AS hts,
                      (c1-c2)MINUTE TO SECOND AS mts
                      FROM timestamp_tbl"""


class arithtst_ts_minus_ts_str(TstView):
    def __init__(self):
        # Validated on Postgres
        self.data = [
            {
                "id": 0,
                "ytm_str": "+5-0",
                "dth_str": "+1855 19",
                "dtm_str": "+1855 19:42",
                "dts_str": "+1855 19:42:00.000000",
                "htm_str": "+44539:42",
                "hts_str": "+44539:42:00.000000",
                "mts_str": "+2672382:00.000000",
            },
            {
                "id": 1,
                "ytm_str": "-2-8",
                "dth_str": "-980 04",
                "dtm_str": "-980 04:00",
                "dts_str": "-980 04:00:00.000000",
                "htm_str": "-23524:00",
                "hts_str": "-23524:00:00.000000",
                "mts_str": "-1411440:00.000000",
            },
            {
                "id": 2,
                "ytm_str": "+10-6",
                "dth_str": "+3853 02",
                "dtm_str": "+3853 02:17",
                "dts_str": "+3853 02:17:00.000000",
                "htm_str": "+92474:17",
                "hts_str": "+92474:17:00.000000",
                "mts_str": "+5548457:00.000000",
            },
        ]
        self.sql = """CREATE MATERIALIZED VIEW ats_minus_ts_str AS SELECT
                      id,
                      CAST((ytm) AS VARCHAR) AS ytm_str,
                      CAST((dth) AS VARCHAR) AS dth_str,
                      CAST((dtm) AS VARCHAR) AS dtm_str,
                      CAST((dts) AS VARCHAR) AS dts_str,
                      CAST((htm) AS VARCHAR) AS htm_str,
                      CAST((hts) AS VARCHAR) AS hts_str,
                      CAST((mts) AS VARCHAR) AS mts_str
                      FROM ats_minus_ts"""


# Equivalent SQL for Postgres
# SELECT
#     id,
#     AGE(c1, c2) AS diff,
#     c1 - c2 AS day_to_minute,
#     EXTRACT(DAYS FROM c1 -c2)*24 + EXTRACT(HOUR FROM c1-c2) || ' hours ' || EXTRACT(MINUTE FROM c1 - c2) || ' minutes '  AS hour_to_minute,
#     EXTRACT(DAYS FROM c1 -c2)*24*60 + EXTRACT(HOUR FROM c1-c2)*60 + EXTRACT(MINUTE FROM c1-c2) AS minute_to_second
# FROM timestamp_tbl;

# eg:
# diff = 10 years 6 mons 19 days 02:17:00,
# day_to_minute = 3853 days 02:17:00,
# hour_to_minute = 92474 hours 17 minutes
# minute_to_second = 5548457


# Using explicit interval types for subtraction and addition
class arithtst_ts_minus_interval(TstView):
    def __init__(self):
        # Validated on Postgres
        # The result of subtraction with all interval types matches with Postgres except DAY TO HOUR
        # DAY to HOUR behaves similarly as DAY to MONTH/SECOND, HOUR TO MINUTE/SECOND, MINUTE TO SECOND) in terms of accuracy
        self.data = [
            {
                "id": 0,
                "ytm": "2014-12-05T08:27:00",
                "dth": "2014-11-05T12:45:00",
                "dtm": "2014-11-05T12:45:00",
                "dts": "2014-11-05T12:45:00",
                "htm": "2014-11-05T12:45:00",
                "hts": "2014-11-05T12:45:00",
                "mts": "2014-11-05T12:45:00",
            },
            {
                "id": 1,
                "ytm": "2023-02-21T14:00:00",
                "dth": "2023-02-26T18:00:00",
                "dtm": "2023-02-26T18:00:00",
                "dts": "2023-02-26T18:00:00",
                "htm": "2023-02-26T18:00:00",
                "hts": "2023-02-26T18:00:00",
                "mts": "2023-02-26T18:00:00",
            },
            {
                "id": 2,
                "ytm": "1948-12-21T11:32:00",
                "dth": "1948-12-02T09:15:00",
                "dtm": "1948-12-02T09:15:00",
                "dts": "1948-12-02T09:15:00",
                "htm": "1948-12-02T09:15:00",
                "hts": "1948-12-02T09:15:00",
                "mts": "1948-12-02T09:15:00",
            },
        ]
        self.sql = """CREATE MATERIALIZED VIEW ts_minus_interval AS SELECT
                              v1.id,
                              c1 - (v1.ytm)  AS ytm,
                              c1 - (v1.dth)  AS dth,
                              c1 - (v1.dtm)  AS dtm,
                              c1 - (v1.dts)  AS dts,
                              c1 - (v1.htm)  AS htm,
                              c1 - (v1.hts)  AS hts,
                              c1 - (v1.mts)  AS mts
                              FROM ats_minus_ts v1
                              JOIN timestamp_tbl v2 ON v1.id = v2.id"""


class arithtst_ts_plus_interval(TstView):
    def __init__(self):
        # Validated on Postgres
        # The result of addition with all interval types matches with Postgres except DAY TO HOUR
        # DAY to HOUR behaves similarly as DAY to MONTH/SECOND, HOUR TO MINUTE/SECOND, MINUTE TO SECOND in terms of accuracy
        self.data = [
            {
                "id": 0,
                "ytm": "2024-12-05T08:27:00",
                "dth": "2025-01-03T04:09:00",
                "dtm": "2025-01-03T04:09:00",
                "dts": "2025-01-03T04:09:00",
                "htm": "2025-01-03T04:09:00",
                "hts": "2025-01-03T04:09:00",
                "mts": "2025-01-03T04:09:00",
            },
            {
                "id": 1,
                "ytm": "2017-10-21T14:00:00",
                "dth": "2017-10-15T10:00:00",
                "dtm": "2017-10-15T10:00:00",
                "dts": "2017-10-15T10:00:00",
                "htm": "2017-10-15T10:00:00",
                "hts": "2017-10-15T10:00:00",
                "mts": "2017-10-15T10:00:00",
            },
            {
                "id": 2,
                "ytm": "1969-12-21T11:32:00",
                "dth": "1970-01-07T13:49:00",
                "dtm": "1970-01-07T13:49:00",
                "dts": "1970-01-07T13:49:00",
                "htm": "1970-01-07T13:49:00",
                "hts": "1970-01-07T13:49:00",
                "mts": "1970-01-07T13:49:00",
            },
        ]
        self.sql = """CREATE MATERIALIZED VIEW ts_plus_interval AS SELECT
                              v1.id,
                              c1 + (v1.ytm)  AS ytm,
                              c1 + (v1.dth)  AS dth,
                              c1 + (v1.dtm)  AS dtm,
                              c1 + (v1.dts)  AS dts,
                              c1 + (v1.htm)  AS htm,
                              c1 + (v1.hts)  AS hts,
                              c1 + (v1.mts)  AS mts
                              FROM ats_minus_ts v1
                              JOIN timestamp_tbl v2 ON v1.id = v2.id"""


# Using interval type cast as VARCHAR for subtraction and addition
class arithtst_ts_minus_tssinterval(TstView):
    def __init__(self):
        # Validated on Postgres
        self.data = [
            {
                "id": 0,
                "ytm_str": "2014-12-05T08:27:00",
                "dth_str": "2014-11-05T13:27:00",
                "dtm_str": "2014-11-05T12:45:00",
                "dts_str": "2014-11-05T12:45:00",
                "htm_str": "2014-11-05T12:45:00",
                "hts_str": "2014-11-05T12:45:00",
                "mts_str": "2014-11-05T12:45:00",
            },
            {
                "id": 1,
                "ytm_str": "2023-02-21T14:00:00",
                "dth_str": "2023-02-26T18:00:00",
                "dtm_str": "2023-02-26T18:00:00",
                "dts_str": "2023-02-26T18:00:00",
                "htm_str": "2023-02-26T18:00:00",
                "hts_str": "2023-02-26T18:00:00",
                "mts_str": "2023-02-26T18:00:00",
            },
            {
                "id": 2,
                "ytm_str": "1948-12-21T11:32:00",
                "dth_str": "1948-12-02T09:32:00",
                "dtm_str": "1948-12-02T09:15:00",
                "dts_str": "1948-12-02T09:15:00",
                "htm_str": "1948-12-02T09:15:00",
                "hts_str": "1948-12-02T09:15:00",
                "mts_str": "1948-12-02T09:15:00",
            },
        ]
        self.sql = """CREATE MATERIALIZED VIEW ts_minus_tssinterval AS SELECT
                              v1.id,
                              c1 - CAST(v1.ytm_str AS INTERVAL YEAR TO MONTH)  AS ytm_str,
                              c1 - CAST(v1.dth_str AS INTERVAL DAY TO HOUR)  AS dth_str,
                              c1 - CAST(v1.dtm_str AS INTERVAL DAY TO MINUTE)  AS dtm_str,
                              c1 - CAST(v1.dts_str AS INTERVAL DAY TO SECOND)  AS dts_str,
                              c1 - CAST(v1.htm_str AS INTERVAL HOUR TO MINUTE)  AS htm_str,
                              c1 - CAST(v1.hts_str AS INTERVAL HOUR TO SECOND)  AS hts_str,
                              c1 - CAST(v1.mts_str AS INTERVAL MINUTE TO SECOND)  AS mts_str
                              FROM ats_minus_ts_str v1
                              JOIN timestamp_tbl v2 ON v1.id = v2.id"""


class arithtst_ts_plus_tssinterval(TstView):
    def __init__(self):
        # Validated on Postgres
        self.data = [
            {
                "id": 0,
                "ytm_str": "2024-12-05T08:27:00",
                "dth_str": "2025-01-03T03:27:00",
                "dtm_str": "2025-01-03T04:09:00",
                "dts_str": "2025-01-03T04:09:00",
                "htm_str": "2025-01-03T04:09:00",
                "hts_str": "2025-01-03T04:09:00",
                "mts_str": "2025-01-03T04:09:00",
            },
            {
                "id": 1,
                "ytm_str": "2017-10-21T14:00:00",
                "dth_str": "2017-10-15T10:00:00",
                "dtm_str": "2017-10-15T10:00:00",
                "dts_str": "2017-10-15T10:00:00",
                "htm_str": "2017-10-15T10:00:00",
                "hts_str": "2017-10-15T10:00:00",
                "mts_str": "2017-10-15T10:00:00",
            },
            {
                "id": 2,
                "ytm_str": "1969-12-21T11:32:00",
                "dth_str": "1970-01-07T13:32:00",
                "dtm_str": "1970-01-07T13:49:00",
                "dts_str": "1970-01-07T13:49:00",
                "htm_str": "1970-01-07T13:49:00",
                "hts_str": "1970-01-07T13:49:00",
                "mts_str": "1970-01-07T13:49:00",
            },
        ]
        self.sql = """CREATE MATERIALIZED VIEW ts_plus_tssinterval AS SELECT
                              v1.id,
                              c1 + CAST(v1.ytm_str AS INTERVAL YEAR TO MONTH)  AS ytm_str,
                              c1 + CAST(v1.dth_str AS INTERVAL DAY TO HOUR)  AS dth_str,
                              c1 + CAST(v1.dtm_str AS INTERVAL DAY TO MINUTE)  AS dtm_str,
                              c1 + CAST(v1.dts_str AS INTERVAL DAY TO SECOND)  AS dts_str,
                              c1 + CAST(v1.htm_str AS INTERVAL HOUR TO MINUTE)  AS htm_str,
                              c1 + CAST(v1.hts_str AS INTERVAL HOUR TO SECOND)  AS hts_str,
                              c1 + CAST(v1.mts_str AS INTERVAL MINUTE TO SECOND)  AS mts_str
                              FROM ats_minus_ts_str v1
                              JOIN timestamp_tbl v2 ON v1.id = v2.id"""


# Equivalent SQL for Postgres
# SELECT '1959-06-21 11:32:00'::TIMESTAMP +/- INTERVAL '10 year 6 month' YEAR TO MONTH AS diff
# SELECT '1959-06-21 11:32:00'::TIMESTAMP +/- INTERVAL '3853 day 02 hour' DAY TO HOUR AS diff
# SELECT '1959-06-21 11:32:00'::TIMESTAMP +/- INTERVAL '3853 day 02:17 minute' DAY TO MINUTE AS diff
# SELECT '1959-06-21 11:32:00'::TIMESTAMP +/- INTERVAL '3853 day 02:17:00.000000 second' DAY TO SECOND AS diff
# SELECT '1959-06-21 11:32:00'::TIMESTAMP +/- INTERVAL '44539 hour 42 minute' HOUR TO MINUTE AS diff
# SELECT '1959-06-21 11:32:00'::TIMESTAMP +/- INTERVAL '44539 hour 2520 second' HOUR TO SECOND AS diff
# SELECT '1959-06-21 11:32:00'::TIMESTAMP +/- INTERVAL '2672382 minute 00.000000 second' MINUTE TO SECOND AS diff


class arithtst_bneg_timestamp(TstView):
    def __init__(self):
        # checked manually
        self.data = [
            {
                "id": 0,
                "seconds_neg": -160342920,
                "minutes_neg": -2672382,
                "hours_neg": -44539,
                "days_neg": -1855,
                "months_neg": -60,
                "years_neg": -5,
            },
            {
                "id": 1,
                "seconds_neg": 84686400,
                "minutes_neg": 1411440,
                "hours_neg": 23524,
                "days_neg": 980,
                "months_neg": 32,
                "years_neg": 2,
            },
            {
                "id": 2,
                "seconds_neg": -332907420,
                "minutes_neg": -5548457,
                "hours_neg": -92474,
                "days_neg": -3853,
                "months_neg": -126,
                "years_neg": -10,
            },
        ]
        self.sql = """CREATE MATERIALIZED VIEW bneg_timestamp AS SELECT
                      id,
                      CAST(-seconds AS BIGINT) AS seconds_neg,
                      CAST(-minutes AS BIGINT) AS minutes_neg,
                      CAST(-hours AS BIGINT) AS hours_neg,
                      CAST(-days AS BIGINT) AS days_neg,
                      CAST(-months AS BIGINT) AS months_neg,
                      CAST(-years AS BIGINT) AS years_neg
                      FROM atimestamp_minus_timestamp"""


class arithtst_bneg_tsinterval(TstView):
    def __init__(self):
        # checked manually
        self.data = [
            {
                "id": 0,
                "ytm_neg": "-5-0",
                "dth_neg": "-1855 19",
                "dtm_neg": "-1855 19:42",
                "dts_neg": "-1855 19:42:00.000000",
                "htm_neg": "-44539:42",
                "hts_neg": "-44539:42:00.000000",
                "mts_neg": "-2672382:00.000000",
            },
            {
                "id": 1,
                "ytm_neg": "+2-8",
                "dth_neg": "+980 04",
                "dtm_neg": "+980 04:00",
                "dts_neg": "+980 04:00:00.000000",
                "htm_neg": "+23524:00",
                "hts_neg": "+23524:00:00.000000",
                "mts_neg": "+1411440:00.000000",
            },
            {
                "id": 2,
                "ytm_neg": "-10-6",
                "dth_neg": "-3853 02",
                "dtm_neg": "-3853 02:17",
                "dts_neg": "-3853 02:17:00.000000",
                "htm_neg": "-92474:17",
                "hts_neg": "-92474:17:00.000000",
                "mts_neg": "-5548457:00.000000",
            },
        ]
        self.sql = """CREATE LOCAL VIEW bneg_tsinterval AS SELECT
                      id,
                      CAST(-ytm AS VARCHAR) AS ytm_neg,
                      CAST(-dth AS VARCHAR) AS dth_neg,
                      CAST(-dtm AS VARCHAR) AS dtm_neg,
                      CAST(-dts AS VARCHAR) AS dts_neg,
                      CAST(-htm AS VARCHAR) AS htm_neg,
                      CAST(-hts AS VARCHAR) AS hts_neg,
                      CAST(-mts AS VARCHAR) AS mts_neg
                      FROM ats_minus_ts"""
