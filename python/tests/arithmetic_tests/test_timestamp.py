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


class arithtst_timestamp_minus_timestamp_res(TstView):
    def __init__(self):
        # Validated on MySQL
        self.data = [
            {
                "id": 0,
                "sec_res": 160342920,
                "min_res": 2672382,
                "hrs_res": 44539,
                "days_res": 1855,
                "mths_res": 60,
                "yrs_res": 5,
            },
            {
                "id": 1,
                "sec_res": -84686400,
                "min_res": -1411440,
                "hrs_res": -23524,
                "days_res": -980,
                "mths_res": -32,
                "yrs_res": -2,
            },
            {
                "id": 2,
                "sec_res": 332907420,
                "min_res": 5548457,
                "hrs_res": 92474,
                "days_res": 3853,
                "mths_res": 126,
                "yrs_res": 10,
            },
        ]
        self.sql = """CREATE MATERIALIZED VIEW timestamp_minus_timestamp_res AS SELECT
                      id,
                      CAST((seconds) AS BIGINT) AS sec_res,
                      CAST((minutes) AS BIGINT) AS min_res,
                      CAST((hours) AS BIGINT) AS hrs_res,
                      CAST((days) AS BIGINT) AS days_res,
                      CAST((months) AS BIGINT) AS mths_res,
                      CAST((years) AS BIGINT) AS yrs_res
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


class arithtst_timestamp_minus_sinterval(TstView):
    def __init__(self):
        # Validated on MySQL
        self.data = [
            # for hours_c1(column 4) MySQL returns different time for row 1 and row 3
            # for days_c1(column 5) MySQL returns different time for all rows and different day for row 1
            {
                "id": 0,
                "seconds_c1": "2014-11-05T12:45:00",
                "minutes_c1": "2014-11-05T12:45:00",
                "hours_c1": "2014-11-05T12:45:00",
                "days_c1": "2014-11-05T12:45:00",
            },
            {
                "id": 1,
                "seconds_c1": "2023-02-26T18:00:00",
                "minutes_c1": "2023-02-26T18:00:00",
                "hours_c1": "2023-02-26T18:00:00",
                "days_c1": "2023-02-26T18:00:00",
            },
            {
                "id": 2,
                "seconds_c1": "1948-12-02T09:15:00",
                "minutes_c1": "1948-12-02T09:15:00",
                "hours_c1": "1948-12-02T09:15:00",
                "days_c1": "1948-12-02T09:15:00",
            },
        ]
        self.sql = """CREATE MATERIALIZED VIEW timestamp_minus_sinterval AS SELECT
                      v1.id,
                      c1 - (v1.seconds) AS seconds_c1,
                      c1 - (v1.minutes) AS minutes_c1,
                      c1 - (v1.hours) AS hours_c1,
                      c1 - (v1.days) AS days_c1
                      FROM atimestamp_minus_timestamp v1
                      JOIN timestamp_tbl v2 ON v1.id = v2.id;"""


class arithtst_timestamp_minus_linterval(TstView):
    def __init__(self):
        # Validated on MySQL
        self.data = [
            # for years_c1(column 2) MySQL subtracts the specified number of years to the timestamp while keeping the month and day intact for row 2 and 3
            {
                "id": 0,
                "months_c1": "2014-12-05T08:27:00",
                "years_c1": "2014-12-05T08:27:00",
            },
            {
                "id": 1,
                "months_c1": "2023-02-21T14:00:00",
                "years_c1": "2023-02-21T14:00:00",
            },
            {
                "id": 2,
                "months_c1": "1948-12-21T11:32:00",
                "years_c1": "1948-12-21T11:32:00",
            },
        ]
        self.sql = """CREATE MATERIALIZED VIEW timestamp_minus_linterval AS SELECT
                      v1.id,
                      c1 - (v1.months) AS months_c1,
                      c1 - (v1.years) AS years_c1
                      FROM atimestamp_minus_timestamp v1
                      JOIN timestamp_tbl v2 ON v1.id = v2.id;"""


class arithtst_timestamp_plus_sinterval(TstView):
    def __init__(self):
        # Validated on MySQL
        self.data = [
            # for hours_c1(column 4) MySQL returns different time for row 1 and row 3
            # for days_c1(column 5) MySQL returns different time for all rows and different day for row 1
            {
                "id": 0,
                "seconds_c1": "2025-01-03T04:09:00",
                "minutes_c1": "2025-01-03T04:09:00",
                "hours_c1": "2025-01-03T04:09:00",
                "days_c1": "2025-01-03T04:09:00",
            },
            {
                "id": 1,
                "seconds_c1": "2017-10-15T10:00:00",
                "minutes_c1": "2017-10-15T10:00:00",
                "hours_c1": "2017-10-15T10:00:00",
                "days_c1": "2017-10-15T10:00:00",
            },
            {
                "id": 2,
                "seconds_c1": "1970-01-07T13:49:00",
                "minutes_c1": "1970-01-07T13:49:00",
                "hours_c1": "1970-01-07T13:49:00",
                "days_c1": "1970-01-07T13:49:00",
            },
        ]
        self.sql = """CREATE MATERIALIZED VIEW timestamp_plus_interval AS SELECT
                      v1.id,
                      c1 + (v1.seconds) AS seconds_c1,
                      c1 + (v1.minutes) AS minutes_c1,
                      c1 + (v1.hours) AS hours_c1,
                      c1 + (v1.days) AS days_c1
                      FROM atimestamp_minus_timestamp v1
                      JOIN timestamp_tbl v2 ON v1.id = v2.id;"""


class arithtst_timestamp_plus_linterval(TstView):
    def __init__(self):
        # Validated on MySQL
        self.data = [
            # for years_c1(column 2) MySQL adds the specified number of years to the timestamp while keeping the month and day intact for row 2 and 3
            {
                "id": 0,
                "months_c1": "2024-12-05T08:27:00",
                "years_c1": "2024-12-05T08:27:00",
            },
            {
                "id": 1,
                "months_c1": "2017-10-21T14:00:00",
                "years_c1": "2017-10-21T14:00:00",
            },
            {
                "id": 2,
                "months_c1": "1969-12-21T11:32:00",
                "years_c1": "1969-12-21T11:32:00",
            },
        ]
        self.sql = """CREATE MATERIALIZED VIEW timestamp_plus_linterval AS SELECT
                      v1.id,
                      c1 + (v1.months) AS months_c1,
                      c1 + (v1.years) AS years_c1
                      FROM atimestamp_minus_timestamp v1
                      JOIN timestamp_tbl v2 ON v1.id = v2.id;"""


# Equivalent SQL for MySQL
# SELECT DATE_SUB('timestamp_value', INTERVAL value SECOND/MINUTE/HOUR/DAYS/MONTH/YEAR) AS sub_res;
# SELECT DATE_ADD('timestamp_value', INTERVAL value SECOND/MINUTE/HOUR/DAYS/MONTH/YEAR) AS add_res;


class arithtst_bneg_timestamp(TstView):
    def __init__(self):
        # Result validation is not required for local views
        self.data = []
        self.sql = """CREATE LOCAL VIEW bneg_timestamp AS SELECT
                      id,
                      (-seconds) AS seconds_neg,
                      (-minutes) AS minutes_neg,
                      (-hours) AS hours_neg,
                      (-days) AS days_neg,
                      (-months) AS months_neg,
                      (-years) AS years_neg
                      FROM atimestamp_minus_timestamp"""


class arithtst_neg_timestamp_res(TstView):
    def __init__(self):
        # checked manually
        self.data = [
            {
                "id": 0,
                "seconds_neg_res": -160342920,
                "minutes_neg_res": -2672382,
                "hours_neg_res": -44539,
                "days_neg_res": -1855,
                "months_neg_res": -60,
                "years_neg_res": -5,
            },
            {
                "id": 1,
                "seconds_neg_res": 84686400,
                "minutes_neg_res": 1411440,
                "hours_neg_res": 23524,
                "days_neg_res": 980,
                "months_neg_res": 32,
                "years_neg_res": 2,
            },
            {
                "id": 2,
                "seconds_neg_res": -332907420,
                "minutes_neg_res": -5548457,
                "hours_neg_res": -92474,
                "days_neg_res": -3853,
                "months_neg_res": -126,
                "years_neg_res": -10,
            },
        ]
        self.sql = """CREATE MATERIALIZED VIEW neg_timestamp_res AS SELECT
                      id,
                      CAST((seconds_neg) AS BIGINT) AS seconds_neg_res,
                      CAST((minutes_neg) AS BIGINT) AS minutes_neg_res,
                      CAST((hours_neg) AS BIGINT) AS hours_neg_res,
                      CAST((days_neg) AS BIGINT) AS days_neg_res,
                      CAST((months_neg) AS BIGINT) AS months_neg_res,
                      CAST((years_neg) AS BIGINT) AS years_neg_res
                      FROM bneg_timestamp"""
