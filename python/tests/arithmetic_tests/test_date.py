from tests.aggregate_tests.aggtst_base import TstView


class arithtst_date_minus_date(TstView):
    def __init__(self):
        # Result validation is not required for local views
        self.data = []
        self.sql = """CREATE LOCAL VIEW date_minus_date AS SELECT
                      id,
                      (c1-c2)SECOND AS seconds,
                      (c1-c2)DAY AS days,
                      (c1-c2)MONTH AS months
                      FROM date_tbl"""


class arithtst_date_minus_date_res(TstView):
    def __init__(self):
        # Validated on MySQL
        self.data = [
            {"id": 0, "seconds_res": 318211200, "days": 3683, "months": 121},
            {"id": 1, "seconds_res": -84672000, "days": -980, "months": -32},
            {"id": 2, "seconds_res": 648518400, "days": 7506, "months": 246},
        ]
        self.sql = """CREATE MATERIALIZED VIEW date_minus_date_res AS SELECT
                      id,
                      CAST((seconds) AS BIGINT) AS seconds_res,
                      CAST((days) AS BIGINT) AS days,
                      CAST((months) AS BIGINT) AS months
                      FROM date_minus_date"""


# Equivalent SQL for MySQL
# SELECT
# 	id,
#     TIMESTAMPDIFF(SECOND, c2, c1) AS f_c1,
#     TIMESTAMPDIFF(DAY, c2, c1) AS f_c2,
#     TIMESTAMPDIFF(MONTH, c2, c1) AS f_c3
# FROM date_tbl;


class arithtst_date_minus_interval(TstView):
    def __init__(self):
        # Validated on MySQL
        self.data = [
            {
                "id": 0,
                "seconds_c1": "2014-11-05",
                "days_c1": "2014-11-05",
                "months_c1": "2014-11-05",
            },
            {
                "id": 1,
                "seconds_c1": "2023-02-26",
                "days_c1": "2023-02-26",
                "months_c1": "2023-02-21",
            },
            {
                "id": 2,
                "seconds_c1": "1948-12-02",
                "days_c1": "1948-12-02",
                "months_c1": "1948-12-21",
            },
        ]
        self.sql = """CREATE MATERIALIZED VIEW date_minus_interval AS SELECT
                      v1.id,
                      c1 - (v1.seconds) AS seconds_c1,
                      c1 - (v1.days) AS days_c1,
                      c1 - (v1.months) AS months_c1
                      FROM date_minus_date v1
                      JOIN date_tbl v2 ON v1.id = v2.id;"""


class arithtst_date_plus_interval(TstView):
    def __init__(self):
        # Validated on MySQL
        self.data = [
            {
                "id": 0,
                "seconds_c1": "2024-12-05",
                "days_c1": "2024-12-05",
                "months_c2": "2024-12-05",
            },
            {
                "id": 1,
                "seconds_c1": "2020-06-21",
                "days_c1": "2020-06-21",
                "months_c2": "2020-06-26",
            },
            {
                "id": 2,
                "seconds_c1": "1969-06-21",
                "days_c1": "1969-06-21",
                "months_c2": "1969-06-02",
            },
        ]
        self.sql = """CREATE MATERIALIZED VIEW date_plus_interval AS SELECT
                      v1.id,
                      c2 + (v1.seconds) AS seconds_c1,
                      c2 + (v1.days) AS days_c1,
                      c2 + (v1.months) AS months_c2
                      FROM date_minus_date v1
                      JOIN date_tbl v2 ON v1.id = v2.id;"""


# Equivalent SQL for MySQL
# SELECT DATE_SUB('date_value', INTERVAL value SECOND/DAYS/MONTH) AS sub_res;
# SELECT DATE_ADD('date_value', INTERVAL value SECOND/DAYS/MONTH) AS add_res;


class arithtst_neg_date(TstView):
    def __init__(self):
        # Result validation is not required for local views
        self.data = []
        self.sql = """CREATE LOCAL VIEW neg_date AS SELECT
                      id,
                      (-seconds) AS seconds_neg,
                      (-days) AS days_neg,
                      (-months) AS months_neg
                      FROM date_minus_date;"""


class arithtst_neg_date_res(TstView):
    def __init__(self):
        # checked manually
        self.data = [
            {"id": 0, "f_c1": -318211200, "f_c2": -3683, "f_c3": -121},
            {"id": 1, "f_c1": 84672000, "f_c2": 980, "f_c3": 32},
            {"id": 2, "f_c1": -648518400, "f_c2": -7506, "f_c3": -246},
        ]
        self.sql = """CREATE MATERIALIZED VIEW neg_date_res AS SELECT
                      id,
                      CAST((seconds_neg) AS BIGINT) AS f_c1,
                      CAST((days_neg) AS BIGINT) AS f_c2,
                      CAST((months_neg) AS BIGINT) AS f_c3
                      FROM neg_date"""
