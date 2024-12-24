from tests.aggregate_tests.aggtst_base import TstView


class arithtst_atime_minus_time(TstView):
    def __init__(self):
        # Result validation is not required for local views
        self.data = []
        self.sql = """CREATE LOCAL VIEW atime_minus_time AS SELECT
                      id,
                      (c1-c2)SECOND AS sec
                      FROM time_tbl"""


class arithtst_time_minus_time_seconds(TstView):
    def __init__(self):
        # Validated on MySQL
        self.data = [
            {"id": 0, "sec_res": 20700},
            {"id": 1, "sec_res": -21600},
        ]
        self.sql = """CREATE MATERIALIZED VIEW time_minus_time_seconds AS SELECT
                      id,
                      CAST((sec) AS BIGINT) AS sec_res
                      FROM atime_minus_time"""


# Equivalent SQL for MySQL
# SELECT
# TIMESTAMPDIFF(SECOND, c2, c1)
# FROM time_tbl;


class arithtst_time_minus_interval(TstView):
    def __init__(self):
        # Validated on Postgres
        self.data = [
            {'id': 0, 'min': '18:20:00', 'hour': '08:45:00', 'sec': '12:45:00'},
            {'id': 1, 'min': '07:50:00', 'hour': '10:00:00', 'sec': '14:00:00'}
        ]
        self.sql = """CREATE MATERIALIZED VIEW time_minus_interval AS SELECT
                      v2.id,
                      c1 - INTERVAL '10' MINUTES AS min,
                      c2 - INTERVAL '4' HOUR AS hour,
                      c1 - (v2.sec) AS sec
                      FROM time_tbl v1
                      JOIN atime_minus_time v2 ON v2.id = v1.id;"""


class arithtst_time_plus_interval(TstView):
    def __init__(self):
        # Validated on Postgres
        self.data = [
            {'id': 0, 'min': '18:40:00', 'hour': '16:45:00', 'sec': '00:15:00'},
            {'id': 1, 'min': '08:10:00', 'hour': '18:00:00', 'sec': '02:00:00'}
        ]
        self.sql = """CREATE MATERIALIZED VIEW time_plus_interval AS SELECT
                      v2.id,
                      c1 + INTERVAL '10' MINUTES AS min,
                      c2 + INTERVAL '4' HOUR AS hour,
                      c1 + (v2.sec) AS sec
                      FROM time_tbl v1
                      JOIN atime_minus_time v2 ON v2.id = v1.id;"""


# Equivalent SQL for Postgres
# CREATE TABLE v AS
# SELECT
#     id,
#     c1 -/+ c2 AS sec
# FROM time_tbl;
#
# SELECT
#     (c1 -/+ INTERVAL '10 minute') AS min,
#     (c2 -/+ INTERVAL '4 hour') AS hour,
#     (c1 -/+ v2.sec) AS sec
# FROM time_tbl v1
# JOIN v v2 ON v2.id = v1.id;


class arithtst_bneg_time(TstView):
    def __init__(self):
        # Result validation is not required for local views
        self.data = []
        self.sql = """CREATE LOCAL VIEW bneg_time AS SELECT
                      id,
                      (-sec) AS sec_neg
                      FROM atime_minus_time;"""


class arithtst_neg_time_res(TstView):
    def __init__(self):
        # checked manually
        self.data = [
            {"id": 0, "sec_neg_res": -20700},
            {"id": 1, "sec_neg_res": 21600},
        ]
        self.sql = """CREATE MATERIALIZED VIEW neg_time_res AS SELECT
                      id,
                      CAST((sec_neg) AS BIGINT) AS sec_neg_res
                      FROM bneg_time"""