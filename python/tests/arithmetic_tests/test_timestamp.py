from tests.aggregate_tests.aggtst_base import TstView


class arithtst_atimestamp_minus_timestamp(TstView):
    def __init__(self):
        # Result validation is not required for local views
        self.data = []
        self.sql = """CREATE LOCAL VIEW atimestamp_minus_timestamp AS SELECT
                      id,
                      (c1-c2)SECOND AS seconds,
                      (c1-c2)DAY AS days,
                      (c1-c2)MONTH AS months
                      FROM timestamp_tbl"""


class arithtst_timestamp_minus_timestamp_res(TstView):
    def __init__(self):
        # Validated on MySQL
        self.data = [
            {'id': 0, 'sec_res': 160342920, 'days_res': 1855, 'mths_res': 60},
            {'id': 1, 'sec_res': -84686400, 'days_res': -980, 'mths_res': -32},
            {'id': 2, 'sec_res': 332907420, 'days_res': 3853, 'mths_res': 126}
        ]
        self.sql = """CREATE MATERIALIZED VIEW timestamp_minus_timestamp_res AS SELECT
                      id,
                      CAST((seconds) AS BIGINT) AS sec_res,
                      CAST((days) AS BIGINT) AS days_res,
                      CAST((months) AS BIGINT) AS mths_res
                      FROM atimestamp_minus_timestamp"""


# Equivalent SQL for MySQL
# SELECT
# 	id,
#     TIMESTAMPDIFF(SECOND, c2, c1) AS f_c1,
#     TIMESTAMPDIFF(DAY, c2, c1) AS f_c2,
#     TIMESTAMPDIFF(MONTH, c2, c1) AS f_c3
# FROM timestamp_tbl;


class arithtst_timestamp_minus_interval(TstView):
    def __init__(self):
        # Validated on MySQL
        self.data = [
            # for days_c1 MySQL returns different results for each column
            {'id': 0, 'seconds_c1': '2014-11-05T12:45:00', 'days_c1': '2014-11-05T12:45:00', 'months_c1': '2014-12-05T08:27:00'},
            {'id': 1, 'seconds_c1': '2023-02-26T18:00:00', 'days_c1': '2023-02-26T18:00:00', 'months_c1': '2023-02-21T14:00:00'},
            {'id': 2, 'seconds_c1': '1948-12-02T09:15:00', 'days_c1': '1948-12-02T09:15:00', 'months_c1': '1948-12-21T11:32:00'}
        ]
        self.sql = """CREATE MATERIALIZED VIEW timestamp_minus_interval AS SELECT
                      v1.id,
                      c1 - (v1.seconds) AS seconds_c1,
                      c1 - (v1.days) AS days_c1,
                      c1 - (v1.months) AS months_c1
                      FROM atimestamp_minus_timestamp v1
                      JOIN timestamp_tbl v2 ON v1.id = v2.id;"""


class arithtst_timestamp_plus_interval(TstView):
    def __init__(self):
        # Validated on MySQL
        self.data = [
            # for days_c1 MySQL returns different results for each column
            {'id': 0, 'seconds_c1': '2025-01-03T04:09:00', 'days_c1': '2025-01-03T04:09:00',
             'months_c1': '2024-12-05T08:27:00'},
            {'id': 1, 'seconds_c1': '2017-10-15T10:00:00', 'days_c1': '2017-10-15T10:00:00',
             'months_c1': '2017-10-21T14:00:00'},
            {'id': 2, 'seconds_c1': '1970-01-07T13:49:00', 'days_c1': '1970-01-07T13:49:00',
             'months_c1': '1969-12-21T11:32:00'}
        ]
        self.sql = """CREATE MATERIALIZED VIEW timestamp_plus_interval AS SELECT
                      v1.id,
                      c1 + (v1.seconds) AS seconds_c1,
                      c1 + (v1.days) AS days_c1,
                      c1 + (v1.months) AS months_c1
                      FROM atimestamp_minus_timestamp v1
                      JOIN timestamp_tbl v2 ON v1.id = v2.id;"""


# Equivalent SQL for MySQL
# SELECT DATE_SUB('date_value', INTERVAL value SECOND/DAYS/MONTH) AS sub_res;
# SELECT DATE_ADD('date_value', INTERVAL value SECOND/DAYS/MONTH) AS add_res;


class arithtst_bneg_timestamp(TstView):
    def __init__(self):
        # Result validation is not required for local views
        self.data = []
        self.sql = """CREATE LOCAL VIEW bneg_timestamp AS SELECT
                      id,
                      (-days) AS days_neg
                      FROM atimestamp_minus_timestamp"""


class arithtst_neg_timestamp_res(TstView):
    def __init__(self):
        # checked manually
        self.data = [
            {'id': 0, 'days_neg_res': -1855},
            {'id': 1, 'days_neg_res': 980},
            {'id': 2, 'days_neg_res': -3853}
        ]
        self.sql = """CREATE MATERIALIZED VIEW neg_timestamp_res AS SELECT
                      id,
                      CAST((days_neg) AS BIGINT) AS days_neg_res
                      FROM bneg_timestamp"""
