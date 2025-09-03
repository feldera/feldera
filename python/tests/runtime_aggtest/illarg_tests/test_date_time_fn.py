from tests.runtime_aggtest.aggtst_base import TstView


# Timestamp/Date/Time type functions
# DATE_TRUNC
class illarg_date_trunc_legal(TstView):
    def __init__(self):
        # checked manually
        self.data = [
            {
                "yr": "2020-01-01T00:00:00",
                "mth": "2020-06-01T00:00:00",
                "day": "2020-06-21T00:00:00",
                "yr1": "2020-01-01",
                "mth1": "2020-06-01",
                "day1": "2020-06-21",
            }
        ]
        self.sql = """CREATE MATERIALIZED VIEW date_trunc_legal AS SELECT
                      DATE_TRUNC(tmestmp, YEAR) AS yr,
                      DATE_TRUNC(tmestmp, MONTH) AS mth,
                      DATE_TRUNC(tmestmp, DAY) AS day,
                      DATE_TRUNC(datee, YEAR) AS yr1,
                      DATE_TRUNC(datee, MONTH) AS mth1,
                      DATE_TRUNC(datee, DAY) AS day1
                      FROM illegal_tbl
                      WHERE id = 0"""


# Negative Test
class illarg_date_trunc_illegal(TstView):
    def __init__(self):
        # checked manually
        self.sql = """CREATE MATERIALIZED VIEW date_trunc_illegal AS SELECT
                      DATE_TRUNC(ARR[4], DAY) AS booll
                      FROM illegal_tbl
                      WHERE id = 1"""
        self.expected_error = "Cannot apply 'DATE_TRUNC' to arguments of type"


# TIMESTAMP_TRUNC
class illarg_trunc_ts_legal(TstView):
    def __init__(self):
        # checked manually
        self.data = [
            {
                "yr": "2020-01-01T00:00:00",
                "mth": "2020-06-01T00:00:00",
                "day": "2020-06-21T00:00:00",
                "hr": "2020-06-21T14:00:00",
                "min": "2020-06-21T14:23:00",
                "sec": "2020-06-21T14:23:44",
            }
        ]
        self.sql = """CREATE MATERIALIZED VIEW ts_trunc_ts_legal AS SELECT
                      TIMESTAMP_TRUNC(tmestmp, YEAR) AS yr,
                      TIMESTAMP_TRUNC(tmestmp, MONTH) AS mth,
                      TIMESTAMP_TRUNC(tmestmp, DAY) AS day,
                      TIMESTAMP_TRUNC(tmestmp, HOUR) AS hr,
                      TIMESTAMP_TRUNC(tmestmp, MINUTE) AS min,
                      TIMESTAMP_TRUNC(tmestmp, SECOND) AS sec
                      FROM illegal_tbl
                      WHERE id = 0"""


class illarg_ts_trunc_datee_legal(TstView):
    def __init__(self):
        # checked manually
        self.data = [
            {
                "yr": "2020-01-01T00:00:00",
                "mth": "2020-06-01T00:00:00",
                "datee": "2020-06-21T00:00:00",
            }
        ]
        self.sql = """CREATE MATERIALIZED VIEW ts_trunc_datee_legal AS SELECT
                      TIMESTAMP_TRUNC(datee, YEAR) AS yr,
                      TIMESTAMP_TRUNC(datee, MONTH) AS mth,
                      TIMESTAMP_TRUNC(datee, DAY) AS datee
                      FROM illegal_tbl
                      WHERE id = 0"""


# Negative Test
class illarg_ts_trunc_illegal(TstView):
    def __init__(self):
        # checked manually
        self.sql = """CREATE MATERIALIZED VIEW ts_trunc_illegal AS SELECT
                      TIMESTAMP_TRUNC(tme, HOUR) AS booll
                      FROM illegal_tbl
                      WHERE id = 1"""
        self.expected_error = "Cannot apply 'TIMESTAMP_TRUNC' to arguments of type"


# TIME_TRUNC
class illarg_tme_trunc_legal(TstView):
    def __init__(self):
        # checked manually
        self.data = [{"yr": "14:00:00", "min": "14:23:00", "sec": "14:23:44"}]
        self.sql = """CREATE MATERIALIZED VIEW tme_trunc_legal AS SELECT
                      TIME_TRUNC(tme, HOUR) AS yr,
                      TIME_TRUNC(tme, MINUTE) AS min,
                      TIME_TRUNC(tme, SECOND) AS sec
                      FROM illegal_tbl
                      WHERE id = 0"""


# Negative Test
class illarg_tme_trunc_illegal(TstView):
    def __init__(self):
        # checked manually
        self.sql = """CREATE MATERIALIZED VIEW tme_trunc_illegal AS SELECT
                      TIME_TRUNC(tmestmp, HOUR) AS ts
                      FROM illegal_tbl
                      WHERE id = 1"""
        self.expected_error = "Cannot apply 'TIME_TRUNC' to arguments of type"


# EXTRACT
class illarg_extract_legal(TstView):
    def __init__(self):
        # checked manually
        self.data = [
            {
                "yr": 2020,
                "yr1": 2020,
                "mth": 6,
                "mth1": 6,
                "day": 21,
                "day1": 21,
                "dow": 1,
                "dow1": 1,
                "hr": 14,
                "hr1": 0,
                "min": 23,
                "min1": 0,
                "sec": 44,
                "sec1": 0,
            }
        ]
        self.sql = """CREATE MATERIALIZED VIEW extract_legal AS SELECT
                      EXTRACT(YEAR FROM tmestmp) AS yr,
                      EXTRACT(YEAR FROM datee) AS yr1,
                      EXTRACT(MONTH FROM tmestmp) AS mth,
                      EXTRACT(MONTH FROM datee) AS mth1,
                      EXTRACT(DAY FROM tmestmp) AS day,
                      EXTRACT(DAY FROM datee) AS day1,
                      EXTRACT(DOW FROM tmestmp) AS dow,
                      EXTRACT(DOW FROM datee) AS dow1,
                      EXTRACT(HOUR FROM tmestmp) AS hr,
                      EXTRACT(HOUR FROM datee) AS hr1,
                      EXTRACT(MINUTE FROM tmestmp) AS min,
                      EXTRACT(MINUTE FROM datee) AS min1,
                      EXTRACT(SECOND FROM tmestmp) AS sec,
                      EXTRACT(SECOND FROM datee) AS sec1
                      FROM illegal_tbl
                      WHERE id = 0"""


# Negative Test
class illarg_extract_illegal(TstView):
    def __init__(self):
        # checked manually
        self.sql = """CREATE MATERIALIZED VIEW extract_illegal AS SELECT
                      EXTRACT(DAY FROM ARR[4]) AS arr
                      FROM illegal_tbl
                      WHERE id = 1"""
        self.expected_error = "Cannot apply 'EXTRACT' to arguments of type"


# Abbreviations for EXTRACT
class illarg_extract_abrv_legal(TstView):
    def __init__(self):
        # checked manually
        self.data = [
            {
                "yr": 2020,
                "yr1": 2020,
                "mth": 6,
                "mth1": 6,
                "day": 21,
                "day1": 21,
                "dow": 1,
                "dow1": 1,
                "hr": 14,
                "hr1": 0,
                "min": 23,
                "min1": 0,
                "sec": 44,
                "sec1": 0,
            }
        ]
        self.sql = """CREATE MATERIALIZED VIEW extract_abrv_legal AS SELECT
                      YEAR(tmestmp) AS yr,
                      YEAR(datee) AS yr1,
                      MONTH(tmestmp) AS mth,
                      MONTH(datee) AS mth1,
                      DAYOFMONTH(tmestmp) AS day,
                      DAYOFMONTH(datee) AS day1,
                      DAYOFWEEK(tmestmp) AS dow,
                      DAYOFWEEK(datee) AS dow1,
                      HOUR(tmestmp) AS hr,
                      HOUR(datee) AS hr1,
                      MINUTE(tmestmp) AS min,
                      MINUTE(datee) AS min1,
                      SECOND(tmestmp) AS sec,
                      SECOND(datee) AS sec1
                      FROM illegal_tbl
                      WHERE id = 0"""


class illarg_extract_abrv1_legal(TstView):
    def __init__(self):
        # checked manually
        self.data = [
            {
                "yr": 2020,
                "yr1": 2020,
                "mth": 6,
                "mth1": 6,
                "day": 21,
                "day1": 21,
                "dow": 1,
                "dow1": 1,
                "hr": 14,
                "hr1": 0,
                "min": 23,
                "min1": 0,
                "sec": 44,
                "sec1": 0,
            }
        ]
        self.sql = """CREATE MATERIALIZED VIEW extract_abrv1_legal AS SELECT
                      DATE_PART(YEAR, tmestmp) AS yr,
                      DATE_PART(YEAR, datee) AS yr1,
                      DATE_PART(MONTH, tmestmp) AS mth,
                      DATE_PART(MONTH, datee) AS mth1,
                      DATE_PART(DAY, tmestmp) AS day,
                      DATE_PART(DAY, datee) AS day1,
                      DATE_PART(DOW, tmestmp) AS dow,
                      DATE_PART(DOW, datee) AS dow1,
                      DATE_PART(HOUR, tmestmp) AS hr,
                      DATE_PART(HOUR, datee) AS hr1,
                      DATE_PART(MINUTE, tmestmp) AS min,
                      DATE_PART(MINUTE, datee) AS min1,
                      DATE_PART(SECOND, tmestmp) AS sec,
                      DATE_PART(SECOND, datee) AS sec1
                      FROM illegal_tbl
                      WHERE id = 0"""


# FLOOR
class illarg_floor_ts_legal(TstView):
    def __init__(self):
        # checked manually
        self.data = [
            {
                "yr": "2020-01-01T00:00:00",
                "mth": "2020-06-01T00:00:00",
                "day": "2020-06-21T00:00:00",
                "hr": "2020-06-21T14:00:00",
                "min": "2020-06-21T14:23:00",
                "sec": "2020-06-21T14:23:44",
                "millsec": "2020-06-21T14:23:44.123",
                "microsec": "2020-06-21T14:23:44.123",
            }
        ]
        self.sql = """CREATE MATERIALIZED VIEW floor_ts_legal AS SELECT
                      FLOOR(tmestmp TO YEAR) AS yr,
                      FLOOR(tmestmp TO MONTH) AS mth,
                      FLOOR(tmestmp TO DAY) AS day,
                      FLOOR(tmestmp TO HOUR) AS hr,
                      FLOOR(tmestmp TO MINUTE) AS min,
                      FLOOR(tmestmp TO SECOND) AS sec,
                      FLOOR(tmestmp TO MILLISECOND) AS millsec,
                      FLOOR(tmestmp TO MICROSECOND) AS microsec
                      FROM illegal_tbl
                      WHERE id = 0"""


class illarg_floor_date_legal(TstView):
    def __init__(self):
        # checked manually
        self.data = [{"yr": "2020-01-01", "mth": "2020-06-01", "day": "2020-06-21"}]
        self.sql = """CREATE MATERIALIZED VIEW floor_date_legal AS SELECT
                      FLOOR(datee TO YEAR) AS yr,
                      FLOOR(datee TO MONTH) AS mth,
                      FLOOR(datee TO DAY) AS day
                      FROM illegal_tbl
                      WHERE id = 0"""


class illarg_floor_time_legal(TstView):
    def __init__(self):
        # checked manually
        self.data = [
            {
                "hr": "14:00:00",
                "min": "14:23:00",
                "sec": "14:23:44",
                "millsec": "14:23:44.456",
                "microsec": "14:23:44.456",
            }
        ]
        self.sql = """CREATE MATERIALIZED VIEW floor_time_legal AS SELECT
                      FLOOR(tme TO HOUR) AS hr,
                      FLOOR(tme TO MINUTE) AS min,
                      FLOOR(tme TO SECOND) AS sec,
                      FLOOR(tme TO MILLISECOND) AS millsec,
                      FLOOR(tme TO MICROSECOND) AS microsec
                      FROM illegal_tbl
                      WHERE id = 0"""


class illarg_floor_legal_ts_date(TstView):
    def __init__(self):
        # checked manually
        self.data = [
            {
                "millennium": "2000-01-01T00:00:00",
                "century": "2000-01-01T00:00:00",
                "decade": "2020-01-01T00:00:00",
                "quarter": "2020-04-01T00:00:00",
                "week": "2020-06-21T00:00:00",
                "millennium1": "2000-01-01",
                "century1": "2000-01-01",
                "decade1": "2020-01-01",
                "quarter1": "2020-04-01",
                "week1": "2020-06-21",
            }
        ]
        self.sql = """CREATE MATERIALIZED VIEW floor_legal_ts_date AS SELECT
                      FLOOR(tmestmp TO MILLENNIUM) millennium,
                      FLOOR(tmestmp TO CENTURY) century,
                      FLOOR(tmestmp TO DECADE) decade,
                      FLOOR(tmestmp TO QUARTER) quarter,
                      FLOOR(tmestmp TO WEEK) week,
                      FLOOR(datee TO MILLENNIUM) millennium1,
                      FLOOR(datee TO CENTURY) century1,
                      FLOOR(datee TO DECADE) decade1,
                      FLOOR(datee TO QUARTER) quarter1,
                      FLOOR(datee TO WEEK) week1
                      FROM illegal_tbl
                      WHERE id = 0"""


# Negative Test
class illarg_floor_illegal(TstView):
    def __init__(self):
        # checked manually
        self.sql = """CREATE MATERIALIZED VIEW floor_illegal AS SELECT
                      FLOOR(ARR[4] TO SECOND) AS arr
                      FROM illegal_tbl
                      WHERE id = 1"""
        self.expected_error = "Cannot apply 'FLOOR' to arguments of type"


# CEIL
class illarg_ceil_ts_legal(TstView):
    def __init__(self):
        # checked manually
        self.data = [
            {
                "yr": "2021-01-01T00:00:00",
                "mth": "2020-07-01T00:00:00",
                "day": "2020-06-22T00:00:00",
                "hr": "2020-06-21T15:00:00",
                "min": "2020-06-21T14:24:00",
                "sec": "2020-06-21T14:23:45",
                "millsec": "2020-06-21T14:23:44.123",
                "microsec": "2020-06-21T14:23:44.123",
            }
        ]
        self.sql = """CREATE MATERIALIZED VIEW ceil_ts_legal AS SELECT
                      CEIL(tmestmp TO YEAR) AS yr,
                      CEIL(tmestmp TO MONTH) AS mth,
                      CEIL(tmestmp TO DAY) AS day,
                      CEIL(tmestmp TO HOUR) AS hr,
                      CEIL(tmestmp TO MINUTE) AS min,
                      CEIL(tmestmp TO SECOND) AS sec,
                      CEIL(tmestmp TO MILLISECOND) AS millsec,
                      CEIL(tmestmp TO MICROSECOND) AS microsec
                      FROM illegal_tbl
                      WHERE id = 0"""


class illarg_ceil_date_legal(TstView):
    def __init__(self):
        # checked manually
        self.data = [{"yr": "2021-01-01", "mth": "2020-07-01", "day": "2020-06-21"}]
        self.sql = """CREATE MATERIALIZED VIEW ceil_date_legal AS SELECT
                      CEIL(datee TO YEAR) AS yr,
                      CEIL(datee TO MONTH) AS mth,
                      CEIL(datee TO DAY) AS day
                      FROM illegal_tbl
                      WHERE id = 0"""


class illarg_ceil_time_legal(TstView):
    def __init__(self):
        # checked manually
        self.data = [
            {
                "hr": "15:00:00",
                "min": "14:24:00",
                "sec": "14:23:45",
                "millsec": "14:23:44.456",
                "microsec": "14:23:44.456",
            }
        ]
        self.sql = """CREATE MATERIALIZED VIEW ceil_time_legal AS SELECT
                      CEIL(tme TO HOUR) AS hr,
                      CEIL(tme TO MINUTE) AS min,
                      CEIL(tme TO SECOND) AS sec,
                      CEIL(tme TO MILLISECOND) AS millsec,
                      CEIL(tme TO MICROSECOND) AS microsec
                      FROM illegal_tbl
                      WHERE id = 0"""


# Negative Test
class illarg_ceil_illegal(TstView):
    def __init__(self):
        # checked manually
        self.sql = """CREATE MATERIALIZED VIEW ceil_illegal AS SELECT
                      CEIL(ARR[4] TO SECOND) AS arr
                      FROM illegal_tbl
                      WHERE id = 1"""
        self.expected_error = "Cannot apply 'CEIL' to arguments of type"


class illarg_ceil_legal_legal_ts_date(TstView):
    def __init__(self):
        # checked manually
        self.data = [
            {
                "millennium": "3000-01-01T00:00:00",
                "century": "2100-01-01T00:00:00",
                "decade": "2030-01-01T00:00:00",
                "quarter": "2020-07-01T00:00:00",
                "week": "2020-06-28T00:00:00",
                "millennium1": "3000-01-01",
                "century1": "2100-01-01",
                "decade1": "2030-01-01",
                "quarter1": "2020-07-01",
                "week1": "2020-06-21",
            }
        ]
        self.sql = """CREATE MATERIALIZED VIEW ceil_legal_legal_ts_date AS SELECT
                      CEIL(tmestmp TO MILLENNIUM) millennium,
                      CEIL(tmestmp TO CENTURY) century,
                      CEIL(tmestmp TO DECADE) decade,
                      CEIL(tmestmp TO QUARTER) quarter,
                      CEIL(tmestmp TO WEEK) week,
                      CEIL(datee TO MILLENNIUM) millennium1,
                      CEIL(datee TO CENTURY) century1,
                      CEIL(datee TO DECADE) decade1,
                      CEIL(datee TO QUARTER) quarter1,
                      CEIL(datee TO WEEK) week1
                      FROM illegal_tbl
                      WHERE id = 0"""


# TIMESTAMPDIFF : contains bug: https://github.com/feldera/feldera/issues/4650
class illarg_tsdiff_ts_legal(TstView):
    def __init__(self):
        # checked manually
        self.data = [
            {
                "yr": 1,
                "mth": 19,
                "day": 580,
                "hr": 13926,
                "min": 835561,
                "sec": 50133660,
            }
        ]
        self.sql = """CREATE MATERIALIZED VIEW tsdiff_ts_legal AS SELECT
                      TIMESTAMPDIFF(YEAR, tmestmp, '2022-01-22 20:24:44.332'::TIMESTAMP) AS yr,
                      TIMESTAMPDIFF(MONTH, tmestmp, '2022-01-22 20:24:44.332'::TIMESTAMP) AS mth,
                      TIMESTAMPDIFF(DAY, tmestmp, '2022-01-22 20:24:44.332'::TIMESTAMP) AS day,
                      TIMESTAMPDIFF(HOUR, tmestmp, '2022-01-22 20:24:44.332'::TIMESTAMP) AS hr,
                      TIMESTAMPDIFF(MINUTE, tmestmp, '2022-01-22 20:24:44.332'::TIMESTAMP) AS min,
                      TIMESTAMPDIFF(SECOND, tmestmp, '2022-01-22 20:24:44.332'::TIMESTAMP) AS sec
                      FROM illegal_tbl
                      WHERE id = 0"""


# class illarg_tsdiff_ts1_legal(TstView):
#     def __init__(self):
#         # checked manually
#         self.data = [
#             {
#                 "yr": 1,
#                 "mth": 19,
#                 "day": 580,
#                 "doy": 580,
#                 "dow": 580,
#                 "hr": 13926,
#                 "min": 835560,
#                 "sec": 50133659,
#             }
#         ]
#         self.sql = """CREATE MATERIALIZED VIEW tsdiff_ts1_legal AS SELECT
#                       TIMESTAMPDIFF(YEAR, '2020-06-21 14:23:44.123'::TIMESTAMP, '2022-01-22 20:24:44.332'::TIMESTAMP) AS yr,
#                       TIMESTAMPDIFF(MONTH, '2020-06-21 14:23:44.123'::TIMESTAMP, '2022-01-22 20:24:44.332'::TIMESTAMP) AS mth,
#                       TIMESTAMPDIFF(DAY, '2020-06-21 14:23:44.123'::TIMESTAMP, '2022-01-22 20:24:44.332'::TIMESTAMP) AS day,
#                       TIMESTAMPDIFF(HOUR, '2020-06-21 14:23:44.123'::TIMESTAMP, '2022-01-22 20:24:44.332'::TIMESTAMP) AS hr,
#                       TIMESTAMPDIFF(MINUTE, '2020-06-21 14:23:44.123'::TIMESTAMP, '2022-01-22 20:24:44.332'::TIMESTAMP) AS min,
#                       TIMESTAMPDIFF(SECOND, '2020-06-21 14:23:44.123'::TIMESTAMP, '2022-01-22 20:24:44.332'::TIMESTAMP) AS sec
#                       FROM illegal_tbl
#                       WHERE id = 0"""


class illarg_tsdiff_datee_legal(TstView):
    def __init__(self):
        # checked manually
        self.data = [{"yr": 1, "mth": 19, "day": 580}]
        self.sql = """CREATE MATERIALIZED VIEW tsdiff_datee_legal AS SELECT
                      TIMESTAMPDIFF(YEAR, datee, '2022-01-22'::DATE) AS yr,
                      TIMESTAMPDIFF(MONTH, datee, '2022-01-22'::DATE) AS mth,
                      TIMESTAMPDIFF(DAY, datee, '2022-01-22'::DATE) AS day
                      FROM illegal_tbl
                      WHERE id = 0"""


class illarg_tsdiff_tme_legal(TstView):
    def __init__(self):
        # checked manually
        self.data = [{"hr": 6, "min": 360, "sec": 21659}]
        self.sql = """CREATE MATERIALIZED VIEW tsdiff_tme_legal AS SELECT
                      TIMESTAMPDIFF(HOUR, tme, '20:24:44.332'::TIME) AS hr,
                      TIMESTAMPDIFF(MINUTE, tme, '20:24:44.332'::TIME) AS min,
                      TIMESTAMPDIFF(SECOND, tme, '20:24:44.332'::TIME) AS sec
                      FROM illegal_tbl
                      WHERE id = 0"""


# Negative Test
class illarg_tsdiff_illegal(TstView):
    def __init__(self):
        # checked manually
        self.sql = """CREATE MATERIALIZED VIEW tsdiff_illegal AS SELECT
                      TIMESTAMPDIFF(HOUR, decimall, '20:24:44'::TIME) AS decimall
                      FROM illegal_tbl
                      WHERE id = 1"""
        self.expected_error = "Cannot apply 'TIMESTAMPDIFF' to arguments of type"


# TIMESTAMPADD
class illarg_tsadd_ts_legal(TstView):
    def __init__(self):
        # checked manually
        self.data = [
            {
                "yr": "2022-06-21T14:23:44.123",
                "mth": "2020-08-21T14:23:44.123",
                "day": "2020-06-23T14:23:44.123",
                "hr": "2020-06-21T16:23:44.123",
                "min": "2020-06-21T14:25:44.123",
                "sec": "2020-06-21T14:23:46.123",
                "millisec": "2020-06-21T14:23:44.125",
                "microsec": "2020-06-21T14:23:44.123",
            }
        ]
        self.sql = """CREATE MATERIALIZED VIEW tsadd_ts_legal AS SELECT
                      TIMESTAMPADD(YEAR, 2, tmestmp) AS yr,
                      TIMESTAMPADD(MONTH, 2, tmestmp) AS mth,
                      TIMESTAMPADD(DAY, 2, tmestmp) AS day,
                      TIMESTAMPADD(HOUR, 2, tmestmp) AS hr,
                      TIMESTAMPADD(MINUTE, 2, tmestmp) AS min,
                      TIMESTAMPADD(SECOND, 2, tmestmp) AS sec,
                      TIMESTAMPADD(MILLISECOND, 2, tmestmp) AS millisec,
                      TIMESTAMPADD(MICROSECOND, 2, tmestmp) AS microsec
                      FROM illegal_tbl
                      WHERE id = 0"""


# Ignore: https://github.com/feldera/feldera/issues/4652
class ignore_tsadd_ts1_legal(TstView):
    def __init__(self):
        # checked manually
        self.data = []
        self.sql = """CREATE MATERIALIZED VIEW tsadd_ts1_legal AS SELECT
                      TIMESTAMPADD(YEAR, 2, '2020-06-21 14:23:44.123'::TIMESTAMP) AS yr,
                      TIMESTAMPADD(MONTH, 2, '2020-06-21 14:23:44.123'::TIMESTAMP) AS mth,
                      TIMESTAMPADD(DAY, 2, '2020-06-21 14:23:44.123'::TIMESTAMP) AS day,
                      TIMESTAMPADD(HOUR, 2, '2020-06-21 14:23:44.123'::TIMESTAMP) AS hr,
                      TIMESTAMPADD(MINUTE, 2, '2020-06-21 14:23:44.123'::TIMESTAMP) AS min,
                      TIMESTAMPADD(SECOND, 2, '2020-06-21 14:23:44.123'::TIMESTAMP) AS sec,
                      TIMESTAMPADD(MILLISECOND, 2, '2020-06-21 14:23:44.123'::TIMESTAMP) AS millisec,
                      TIMESTAMPADD(MICROSECOND, 2, '2020-06-21 14:23:44.123'::TIMESTAMP) AS microsec
                      FROM illegal_tbl
                      WHERE id = 0"""


# Ignore: https://github.com/feldera/feldera/issues/4651
class ignore_v(TstView):
    def __init__(self):
        # checked manually
        self.data = []
        self.sql = """CREATE MATERIALIZED VIEW v AS SELECT
                      TIMESTAMPADD(DOY, 2, '2020-06-21 14:23:44.123'::TIMESTAMP) AS doy,
                      TIMESTAMPADD(DOW, 2, '2020-06-21 14:23:44.123'::TIMESTAMP) AS dow
                      FROM illegal_tbl
                      WHERE id = 0"""


class illarg_tsadd_datee_legal(TstView):
    def __init__(self):
        # checked manually
        self.data = [
            {
                "yr": "2022-06-21",
                "mth": "2020-08-21",
                "day": "2020-06-23",
            }
        ]
        self.sql = """CREATE MATERIALIZED VIEW tsadd_datee_legal AS SELECT
                      TIMESTAMPADD(YEAR, 2, datee) AS yr,
                      TIMESTAMPADD(MONTH, 2, datee) AS mth,
                      TIMESTAMPADD(DAY, 2, datee) AS day
                      FROM illegal_tbl
                      WHERE id = 0"""


class illarg_tsadd_tme_legal(TstView):
    def __init__(self):
        # checked manually
        self.data = [
            {
                "hr": "16:23:44.456",
                "min": "14:25:44.456",
                "sec": "14:23:46.456",
                "millisec": "14:23:44.458",
                "microsec": "14:23:44.456",
            }
        ]
        self.sql = """CREATE MATERIALIZED VIEW tsadd_legal AS SELECT
                      TIMESTAMPADD(HOUR, 2, tme) AS hr,
                      TIMESTAMPADD(MINUTE, 2, tme) AS min,
                      TIMESTAMPADD(SECOND, 2, tme) AS sec,
                      TIMESTAMPADD(MILLISECOND, 2, tme) AS millisec,
                      TIMESTAMPADD(MICROSECOND, 2, tme) AS microsec
                      FROM illegal_tbl
                      WHERE id = 0"""


# Negative Test
class illarg_tsadd_illegal(TstView):
    def __init__(self):
        # checked manually
        self.sql = """CREATE MATERIALIZED VIEW tsadd_illegal AS SELECT
                      TIMESTAMPADD(YEAR, 2, ARR[4]) AS arr
                      FROM illegal_tbl
                      WHERE id = 1"""
        self.expected_error = "Cannot apply 'TIMESTAMPADD' to arguments of type"


# FORMAT_DATE
class illarg_format_date_legal(TstView):
    def __init__(self):
        # checked manually
        self.data = [{"datee": "2020-06"}]
        self.sql = """CREATE MATERIALIZED VIEW format_date_legal AS SELECT
                      FORMAT_DATE('%Y-%m', datee) AS datee
                      FROM illegal_tbl
                      WHERE id = 0"""


class illarg_format_date_cast_legal(TstView):
    def __init__(self):
        # checked manually
        self.data = [{"tmestmp": "2020-06", "arr": "2021-01"}]
        self.sql = """CREATE MATERIALIZED VIEW format_date_cast_legal AS SELECT
                      FORMAT_DATE('%Y-%m', tmestmp) AS tmestmp,
                      FORMAT_DATE('%Y-%m', ARR[4]) AS arr
                      FROM illegal_tbl
                      WHERE id = 1"""


# Negative Test
class illarg_format_date_illegal(TstView):
    def __init__(self):
        # checked manually
        self.sql = """CREATE MATERIALIZED VIEW format_date_illegal AS SELECT
                      FORMAT_DATE('%Y-%m', tme) AS tme
                      FROM illegal_tbl
                      WHERE id = 1"""
        self.expected_error = "Cannot apply 'FORMAT_DATE' to arguments of type"


# PARSE_DATE
class illarg_parse_date_legal(TstView):
    def __init__(self):
        # checked manually
        self.data = [{"datee": "2021-01-20"}]
        self.sql = """CREATE MATERIALIZED VIEW parse_date_legal AS SELECT
                      PARSE_DATE('%Y-%m-%d', ARR[4]) AS datee
                      FROM illegal_tbl
                      WHERE id = 1"""


# Negative Test
class illarg_parse_date_illegal(TstView):
    def __init__(self):
        # checked manually
        self.data = [{"booll": None}]
        self.sql = """CREATE MATERIALIZED VIEW parse_date_illegal AS SELECT
                      PARSE_DATE('%Y-%m', booll) AS booll
                      FROM illegal_tbl
                      WHERE id = 1"""


# PARSE_TIMESTAMP
class illarg_parse_ts_legal(TstView):
    def __init__(self):
        # checked manually
        self.data = [{"ts": "2020-10-01T00:00:00"}]
        self.sql = """CREATE MATERIALIZED VIEW parse_ts_legal AS SELECT
                      PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%S', ARR[6]) AS ts
                      FROM illegal_tbl
                      WHERE id = 1"""


# Negative Test
class illarg_parse_ts_illegal(TstView):
    def __init__(self):
        # checked manually
        self.data = [{"booll": None}]
        self.sql = """CREATE MATERIALIZED VIEW parse_ts_illegal AS SELECT
                      PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%S', booll) AS booll
                      FROM illegal_tbl
                      WHERE id = 1"""


# PARSE_TIME
class illarg_parse_tme_legal(TstView):
    def __init__(self):
        # checked manually
        self.data = [{"tme": "10:10:00"}]
        self.sql = """CREATE MATERIALIZED VIEW parse_tme_legal AS SELECT
                      PARSE_TIME('%H:%M', ARR[5]) AS tme
                      FROM illegal_tbl
                      WHERE id = 1"""


# Negative Test
class illarg_parse_tme_illegal(TstView):
    def __init__(self):
        # checked manually
        self.data = [{"booll": None}]
        self.sql = """CREATE MATERIALIZED VIEW parse_tme_illegal AS SELECT
                      PARSE_TIME('%H:%M', booll) AS booll
                      FROM illegal_tbl
                      WHERE id = 1"""
