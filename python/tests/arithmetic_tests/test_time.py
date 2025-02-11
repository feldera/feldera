from tests.aggregate_tests.aggtst_base import TstView


class arithtst_atime_minus_time(TstView):
    def __init__(self):
        # Result validation is not required for local views
        self.data = []
        self.sql = """CREATE LOCAL VIEW atime_minus_time AS SELECT
                      id,
                      (c1-c2)SECOND AS seconds,
                      (c1-c2)MINUTE AS minutes,
                      (c1-c2)HOUR AS hours
                      FROM time_tbl"""


class arithtst_time_minus_time_res(TstView):
    def __init__(self):
        # Validated on MySQL
        self.data = [
            {'id': 0, 'sec_res': '+20733.000000', 'min_res': '+345', 'hrs_res': '+5'},
            {'id': 1, 'sec_res': '-21194.000000', 'min_res': '-353', 'hrs_res': '-5'}
        ]
        self.sql = """CREATE MATERIALIZED VIEW time_minus_time_res AS SELECT
                      id,
                      CAST((seconds) AS VARCHAR) AS sec_res,
                      CAST((minutes) AS VARCHAR) AS min_res,
                      CAST((hours) AS VARCHAR) AS hrs_res
                      FROM atime_minus_time"""


# Equivalent SQL for MySQL
# SELECT
# 	id,
#     TIMESTAMPDIFF(SECOND, c2, c1) AS seconds,
#     TIMESTAMPDIFF(MINUTE, c2, c1) AS minutes,
#     TIMESTAMPDIFF(HOUR, c2, c1) AS hours
# FROM time_tbl;


# Using explicit interval types for subtraction and addition
class arithtst_time_minus_sinterval(TstView):
    def __init__(self):
        # Validated in MySQL
        # The result of subtracting SECOND type interval matches with MySQL
        # whereas MINUTES and HOURS types produce the same result as subtracting SECOND type
        self.data = [
            {'id': 0, 'seconds_c1': '12:45:12', 'minutes_c1': '12:45:12', 'hours_c1': '12:45:12'},
            {'id': 1, 'seconds_c1': '14:17:09', 'minutes_c1': '14:17:09', 'hours_c1': '14:17:09'}
        ]
        self.sql = """CREATE MATERIALIZED VIEW time_minus_sinterval AS SELECT
                      v1.id,
                      c1 - (v1.seconds) AS seconds_c1,
                      c1 - (v1.minutes) AS minutes_c1,
                      c1 - (v1.hours) AS hours_c1
                      FROM atime_minus_time v1
                      JOIN time_tbl v2 ON v1.id = v2.id;"""


class arithtst_time_plus_sinterval(TstView):
    def __init__(self):
        # Validated in MySQL
        # The result of adding SECOND type interval matches with MySQL
        # whereas MINUTES and HOURS types produce the same result as adding SECOND type
        self.data = [
            {'id': 0, 'seconds_c1': '00:16:18', 'minutes_c1': '00:16:18', 'hours_c1': '00:16:18'},
            {'id': 1, 'seconds_c1': '02:30:41', 'minutes_c1': '02:30:41', 'hours_c1': '02:30:41'}
        ]
        self.sql = """CREATE MATERIALIZED VIEW time_plus_sinterval AS SELECT
                      v1.id,
                      c1 + (v1.seconds) AS seconds_c1,
                      c1 + (v1.minutes) AS minutes_c1,
                      c1 + (v1.hours) AS hours_c1
                      FROM atime_minus_time v1
                      JOIN time_tbl v2 ON v1.id = v2.id;"""


# Using interval type cast as VARCHAR for subtraction and addition
class arithtst_time_sub_tmeinterval(TstView):
    def __init__(self):
        # Validated on MySQL
        self.data = [
            {'id': 0, 'sec_c1': '12:45:12', 'min_c1': '12:45:45', 'hrs_c1': '13:30:45'},
            {'id': 1, 'sec_c1': '14:17:09', 'min_c1': '14:16:55', 'hrs_c1': '13:23:55'}
        ]
        self.sql = """CREATE MATERIALIZED VIEW time_sub_sinterval AS SELECT
                      v1.id,
                      c1 - INTERVAL v1.sec_res SECOND AS sec_c1,
                      c1 - INTERVAL v1.min_res MINUTE AS min_c1,
                      c1 - INTERVAL v1.hrs_res HOUR AS hrs_c1
                      FROM time_minus_time_res v1
                      JOIN time_tbl v2 ON v1.id = v2.id;"""


class arithtst_tme_add_sinterval(TstView):
    def __init__(self):
        # Validated on MySQL
        self.data = [
            {'id': 0, 'sec_c1': '00:16:18', 'min_c1': '00:15:45', 'hrs_c1': '23:30:45'},
            {'id': 1, 'sec_c1': '02:30:41', 'min_c1': '02:30:55', 'hrs_c1': '03:23:55'}
        ]
        self.sql = """CREATE MATERIALIZED VIEW tme_add_sinterval AS SELECT
                      v1.id,
                      c1 + INTERVAL v1.sec_res SECOND AS sec_c1,
                      c1 + INTERVAL v1.min_res MINUTE AS min_c1,
                      c1 + INTERVAL v1.hrs_res HOUR AS hrs_c1
                      FROM time_minus_time_res v1
                      JOIN time_tbl v2 ON v1.id = v2.id;"""


# Equivalent SQL for MySQL
# CREATE TABLE interval_values AS
# SELECT
#     id,
#     TIMESTAMPDIFF(SECOND, c2, c1) AS seconds,
#     TIMESTAMPDIFF(MINUTE, c2, c1) AS minutes,
#     TIMESTAMPDIFF(HOUR, c2, c1) AS hours
# FROM time_tbl;
#
# SELECT
#     v1.id,
#     DATE_SUB(c1, INTERVAL v1.seconds SECOND) AS sec_c1,
#     DATE_SUB(c1, INTERVAL v1.minutes MINUTE) AS min_c1,
#     DATE_SUB(c1, INTERVAL v1.hours HOUR) AS hrs_c1
# FROM interval_values v1
# JOIN time_tbl v2 ON v1.id = v2.id;


class arithtst_atme_minus_tme(TstView):
    def __init__(self):
        # Result validation not required for local views
        self.data = [
        ]
        self.sql = """CREATE LOCAL VIEW atme_minus_tme AS SELECT
                      id,
                      (c1-c2)HOUR TO MINUTE AS htm,
                      (c1-c2)HOUR TO SECOND AS hts,
                      (c1-c2)MINUTE TO SECOND AS mts
                      FROM time_tbl"""


class arithtst_tme_minus_tme_res(TstView):
    def __init__(self):
        # Validated on Postgres
        self.data = [
            {'id': 0, 'htm_res': '+5:45', 'hts_res': '+5:45:33.000000', 'mts_res': '+345:33.000000'},
            {'id': 1, 'htm_res': '-5:53', 'hts_res': '-5:53:14.000000', 'mts_res': '-353:14.000000'}
        ]
        self.sql = """CREATE MATERIALIZED VIEW tme_minus_tme_res AS SELECT
                      id,
                      CAST((htm) AS VARCHAR) AS htm_res,
                      CAST((hts) AS VARCHAR) AS hts_res,
                      CAST((mts) AS VARCHAR) AS mts_res
                      FROM atme_minus_tme"""


# Equivalent SQL for Postgres
# SELECT
#     id,
#     c1 - c2 AS time_diff,
#     EXTRACT(HOUR FROM (c1 -c2))|| ':' || EXTRACT(MINUTE FROM (c1 - c2)) AS htm,
#     EXTRACT(HOUR FROM (c1 -c2))|| ':' || EXTRACT(MINUTE FROM (c1 - c2)) || ':' || EXTRACT(SECOND FROM (c1 - c2)) AS hts,
#     EXTRACT(HOUR FROM (c1 -c2))*60 + EXTRACT(MINUTE FROM (c1 -c2))|| ':' || EXTRACT(SECOND FROM (c1 - c2)) AS mts
# FROM time_tbl;


# Using explicit interval types for subtraction and addition
class arithtst_tme_minus_interval(TstView):
    def __init__(self):
        # Validated on Postgres
        # The result of subtracting "HOUR TO SECOND" and "MINUTE TO SECOND" type intervals matches with Postgres
        # Subtracting HOUR to MINUTE produces the same result as subtracting 'HOUR TO SECOND' and "MINUTE TO SECOND" type intervals
        self.data = [
            {'id': 0, 'htm': '12:45:12', 'hts': '12:45:12', 'mts': '12:45:12'},
            {'id': 1, 'htm': '14:17:09', 'hts': '14:17:09', 'mts': '14:17:09'}
        ]
        self.sql = """CREATE MATERIALIZED VIEW tme_minus_interval AS SELECT
                              v1.id,
                              c1 - (v1.htm)  AS htm,
                              c1 - (v1.hts)  AS hts,
                              c1 - (v1.mts)  AS mts
                              FROM atme_minus_tme v1
                              JOIN time_tbl v2 ON v1.id = v2.id"""


class arithtst_time_plus_interval(TstView):
    def __init__(self):
        # Validated on Postgres
        # The result of adding "HOUR TO SECOND" and "MINUTE TO SECOND" type intervals matches with Postgres
        # Adding HOUR to MINUTE produces the same result as adding 'HOUR TO SECOND' and "MINUTE TO SECOND" type intervals
        self.data = [
            {'id': 0, 'htm': '00:16:18', 'hts': '00:16:18', 'mts': '00:16:18'},
            {'id': 1, 'htm': '02:30:41', 'hts': '02:30:41', 'mts': '02:30:41'}
        ]
        self.sql = """CREATE MATERIALIZED VIEW time_plus_interval AS SELECT
                              v1.id,
                              c1 + (v1.htm)  AS htm,
                              c1 + (v1.hts)  AS hts,
                              c1 + (v1.mts)  AS mts
                              FROM atme_minus_tme v1
                              JOIN time_tbl v2 ON v1.id = v2.id"""


# Using interval type cast as VARCHAR for subtraction and addition
class arithtst_tme_minus_tmeinterval(TstView):
    def __init__(self):
        # Validated on Postgres
        self.data = [
            {'id': 0, 'htm': '12:45:45', 'hts': '12:45:12', 'mts': '12:45:12'},
            {'id': 1, 'htm': '14:16:55', 'hts': '14:17:09', 'mts': '14:17:09'}
        ]
        self.sql = """CREATE MATERIALIZED VIEW tme_minus_tmeinterval AS SELECT
                              v1.id,
                              c1 - CAST(v1.htm_res AS INTERVAL HOUR TO MINUTE)  AS htm,
                              c1 - CAST(v1.hts_res AS INTERVAL HOUR TO SECOND)  AS hts,
                              c1 - CAST(v1.mts_res AS INTERVAL MINUTE TO SECOND)  AS mts
                              FROM tme_minus_tme_res v1
                              JOIN time_tbl v2 ON v1.id = v2.id"""


class arithtst_tme_plus_tmeinterval(TstView):
    def __init__(self):
        # Validated on Postgres
        self.data = [
            {'htm': '00:15:45', 'hts': '00:16:18', 'mts': '00:16:18'},
            {'htm': '02:30:55', 'hts': '02:30:41', 'mts': '02:30:41'}
        ]
        self.sql = """CREATE MATERIALIZED VIEW tme_plus_tmeinterval AS SELECT
                              c1 + CAST(v1.htm_res AS INTERVAL HOUR TO MINUTE)  AS htm,
                              c1 + CAST(v1.hts_res AS INTERVAL HOUR TO SECOND)  AS hts,
                              c1 + CAST(v1.mts_res AS INTERVAL MINUTE TO SECOND)  AS mts
                              FROM tme_minus_tme_res v1
                              JOIN time_tbl v2 ON v1.id = v2.id"""


# Equivalent SQl for Postgres
# SELECT
#     '18:30:45'::TIME - INTERVAL '5 hour 45 minute' HOUR TO MINUTE AS htm,
#     '18:30:45'::TIME - INTERVAL '5 hour 2733 second' HOUR TO SECOND AS hts,
#     '18:30:45'::TIME - INTERVAL '345 minute 33.000000 second' MINUTE TO SECOND AS mts;


class arithtst_bneg_time(TstView):
    def __init__(self):
        # checked manually
        self.data = [
            {'id': 0, 'seconds_neg': '-20733.000000', 'minutes_neg': '-345', 'hours_neg': '-5'},
            {'id': 1, 'seconds_neg': '+21194.000000', 'minutes_neg': '+353', 'hours_neg': '+5'}
        ]
        self.sql = """CREATE MATERIALIZED VIEW bneg_time AS SELECT
                      id,
                      CAST(-seconds AS VARCHAR) AS seconds_neg,
                      CAST(-minutes AS VARCHAR) AS minutes_neg,
                      CAST(-hours AS VARCHAR) AS hours_neg
                      FROM atime_minus_time;"""


class arithtst_bneg_tmeinterval(TstView):
    def __init__(self):
        # checked manually
        self.data = [
            {'id': 0, 'htm_neg': '-5:45', 'hts_neg': '-5:45:33.000000', 'mts_neg': '-345:33.000000'},
            {'id': 1, 'htm_neg': '+5:53', 'hts_neg': '+5:53:14.000000', 'mts_neg': '+353:14.000000'}
        ]
        self.sql = """CREATE MATERIALIZED VIEW bneg_tmeinterval AS SELECT
                      id,
                      CAST(-htm AS VARCHAR) AS htm_neg,
                      CAST(-hts AS VARCHAR) AS hts_neg,
                      CAST(-mts AS VARCHAR) AS mts_neg
                      FROM atme_minus_tme"""