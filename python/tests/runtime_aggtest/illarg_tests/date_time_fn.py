from tests.runtime_aggtest.aggtst_base import TstView


# Timestamp/Date/Time type functions
# DATE_TRUNC
class illarg_date_trunc_legal(TstView):
    def __init__(self):
        # checked manually
        self.data = [{"tmestmp": "2020-06-01T00:00:00", "datee": "2020-06-01"}]
        self.sql = """CREATE MATERIALIZED VIEW date_trunc_legal AS SELECT
                      DATE_TRUNC(tmestmp, MONTH) AS tmestmp,
                      DATE_TRUNC(datee, MONTH) AS datee
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
class illarg_floor_legal(TstView):
    def __init__(self):
        # checked manually
        self.data = [
            {"yr": "2020-01-01T00:00:00", "mth": "2020-06-01", "hr": "14:00:00"}
        ]
        self.sql = """CREATE MATERIALIZED VIEW floor_legal AS SELECT
                      FLOOR(tmestmp TO YEAR) AS yr,
                      FLOOR(datee TO MONTH) AS mth,
                      FLOOR(tme TO HOUR) AS hr
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
class illarg_ceil_legal(TstView):
    def __init__(self):
        # checked manually
        self.data = [
            {"yr": "2021-01-01T00:00:00", "mth": "2020-07-01", "hr": "15:00:00"}
        ]
        self.sql = """CREATE MATERIALIZED VIEW ceil_legal AS SELECT
                      CEIL(tmestmp TO YEAR) AS yr,
                      CEIL(datee TO MONTH) AS mth,
                      CEIL(tme TO HOUR) AS hr
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


# TIMESTAMPDIFF
class illarg_tsdiff_legal(TstView):
    def __init__(self):
        # checked manually
        self.data = [{"yr": 1, "mth": 19, "hr": 6}]
        self.sql = """CREATE MATERIALIZED VIEW tsdiff_legal AS SELECT
                      TIMESTAMPDIFF(YEAR, tmestmp, '2022-01-22 20:24:44'::TIMESTAMP) AS yr,
                      TIMESTAMPDIFF(MONTH, datee, '2022-01-22'::DATE) AS mth,
                      TIMESTAMPDIFF(HOUR, tme, '20:24:44'::TIME) AS hr
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
class illarg_tsadd_legal(TstView):
    def __init__(self):
        # checked manually
        self.data = [
            {"yr": "2022-06-21T14:23:44", "mth": "2020-08-21", "hr": "16:23:44"}
        ]
        self.sql = """CREATE MATERIALIZED VIEW tsadd_legal AS SELECT
                      TIMESTAMPADD(YEAR, 2, tmestmp) AS yr,
                      TIMESTAMPADD(MONTH, 2, datee) AS mth,
                      TIMESTAMPADD(HOUR, 2, tme) AS hr
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


# Ignore: https://github.com/feldera/feldera/issues/4597
# PARSE_TIMESTAMP
# PARSE_TIME


# TIMESTAMP_TRUNC
class illarg_trunc_ts_legal(TstView):
    def __init__(self):
        # checked manually
        self.data = [{"tmestmp": "2020-06-01T00:00:00"}]
        self.sql = """CREATE MATERIALIZED VIEW ts_trunc_legal AS SELECT
                      TIMESTAMP_TRUNC(tmestmp, MONTH) AS tmestmp
                      FROM illegal_tbl
                      WHERE id = 1"""


class illarg_ts_trunc_cast_legal(TstView):
    def __init__(self):
        # checked manually
        self.data = [{"datee": "2020-06-01T00:00:00"}]
        self.sql = """CREATE MATERIALIZED VIEW ts_trunc_cast_legal AS SELECT
                      TIMESTAMP_TRUNC(datee, MONTH) AS datee
                      FROM illegal_tbl
                      WHERE id = 1"""


# Negative Test
class illarg_ts_trunc_illegal(TstView):
    def __init__(self):
        # checked manually
        self.sql = """CREATE MATERIALIZED VIEW ts_trunc_illegal AS SELECT
                      TIMESTAMP_TRUNC(tme, HOUR) AS booll
                      FROM illegal_tbl
                      WHERE id = 1"""
        self.expected_error = "Cannot apply 'TIMESTAMP_TRUNC' to arguments of type"
