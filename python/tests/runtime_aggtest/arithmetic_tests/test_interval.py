from tests.runtime_aggtest.aggtst_base import TstView


# Long Interval Type
# Using Calculated Intervals and Declared Intervals for Subtraction and Addition
class arithtst_linterval_minus_linterval(TstView):
    def __init__(self):
        # Validated on Postgres
        # ytm and yr_ytm, behave the same as mth_ytm
        self.data = [
            {
                "id": 0,
                "yr": "-5",
                "mth": "-68",
                "ytm": "-5-8",
                "mty": "-5-0",
                "yr_ytm": "-5-8",
                "mth_ytm": "-5-8",
            },
            {
                "id": 1,
                "yr": "-12",
                "mth": "-160",
                "ytm": "-13-4",
                "mty": "-12-8",
                "yr_ytm": "-13-4",
                "mth_ytm": "-13-4",
            },
            {
                "id": 2,
                "yr": "+0",
                "mth": "-2",
                "ytm": "-0-2",
                "mty": "+0-6",
                "yr_ytm": "-0-2",
                "mth_ytm": "-0-2",
            },
        ]
        self.sql = """CREATE MATERIALIZED VIEW linterval_minus_linterval AS SELECT
                      id,
                      CAST((years - INTERVAL '10' YEAR) AS VARCHAR)  AS yr,
                      CAST((months - INTERVAL '128' MONTH) AS VARCHAR ) AS mth,
                      CAST((years - INTERVAL '128' MONTH) AS VARCHAR)  AS ytm,
                      CAST((months - INTERVAL '10' YEAR) AS VARCHAR ) AS mty,
                      CAST((years -  INTERVAL '10-08' YEAR TO MONTH) AS VARCHAR ) AS yr_ytm,
                      CAST((months -  INTERVAL '10-08' YEAR TO MONTH) AS VARCHAR ) AS mth_ytm
                      FROM atimestamp_minus_timestamp"""


class arithtst_linterval_plus_linterval(TstView):
    def __init__(self):
        # checked manually
        self.data = [
            {
                "id": 0,
                "yr": "+15",
                "mth": "+188",
                "ytm": "+15-8",
                "mty": "+15-0",
                "yr_ytm": "+15-8",
                "mth_ytm": "+15-8",
            },
            {
                "id": 1,
                "yr": "+7",
                "mth": "+96",
                "ytm": "+8-0",
                "mty": "+7-4",
                "yr_ytm": "+8-0",
                "mth_ytm": "+8-0",
            },
            {
                "id": 2,
                "yr": "+20",
                "mth": "+254",
                "ytm": "+21-2",
                "mty": "+20-6",
                "yr_ytm": "+21-2",
                "mth_ytm": "+21-2",
            },
        ]
        self.sql = """CREATE MATERIALIZED VIEW linterval_plus_linterval AS SELECT
                      id,
                      CAST((years + INTERVAL '10' YEAR) AS VARCHAR)  AS yr,
                      CAST((months + INTERVAL '128' MONTH) AS VARCHAR ) AS mth,
                      CAST((years + INTERVAL '128' MONTH) AS VARCHAR)  AS ytm,
                      CAST((months + INTERVAL '10' YEAR) AS VARCHAR ) AS mty,
                      CAST((years + INTERVAL '10-08' YEAR TO MONTH) AS VARCHAR ) AS yr_ytm,
                      CAST((months + INTERVAL '10-08' YEAR TO MONTH) AS VARCHAR ) AS mth_ytm
                      FROM atimestamp_minus_timestamp"""


# Using Calculated Intervals cast as VARCHAR and Declared Intervals for Subtraction and Addition
class arithtst_linterval_minus_linterval_str(TstView):
    def __init__(self):
        # Validated in Postgres
        self.data = [
            {
                "id": 0,
                "yr_str": "-5",
                "mth_str": "-68",
                "ytm_str": "-5-8",
                "mty_str": "-5-0",
                "yr_ytm_str": "-5-8",
                "mth_ytm_str": "-5-8",
            },
            {
                "id": 1,
                "yr_str": "-12",
                "mth_str": "-160",
                "ytm_str": "-12-8",
                "mty_str": "-12-8",
                "yr_ytm_str": "-12-8",
                "mth_ytm_str": "-13-4",
            },
            {
                "id": 2,
                "yr_str": "+0",
                "mth_str": "-2",
                "ytm_str": "-0-8",
                "mty_str": "+0-6",
                "yr_ytm_str": "-0-8",
                "mth_ytm_str": "-0-2",
            },
        ]
        self.sql = """CREATE MATERIALIZED VIEW linterval_minus_linterval_str AS SELECT
                       id,
                       CAST((INTERVAL yrs_str YEAR - INTERVAL '10' YEAR) AS VARCHAR) AS yr_str,
                       CAST((INTERVAL mths_str MONTH - INTERVAL '128' MONTH) AS VARCHAR) AS mth_str,
                       CAST((INTERVAL yrs_str YEAR - INTERVAL '128' MONTH) AS VARCHAR) AS ytm_str,
                       CAST((INTERVAL mths_str MONTH - INTERVAL '10' YEAR) AS VARCHAR) AS mty_str,
                       CAST((INTERVAL yrs_str YEAR - INTERVAL '10-08' YEAR TO MONTH) AS VARCHAR) AS yr_ytm_str,
                       CAST((INTERVAL mths_str MONTHS - INTERVAL '10-08' YEAR TO MONTH) AS VARCHAR ) AS mth_ytm_str
                       FROM timestamp_minus_timestamp_str"""


class arithtst_linterval_plus_linterval_str(TstView):
    def __init__(self):
        # Validated in Postgres
        self.data = [
            {
                "id": 0,
                "yr_str": "+15",
                "mth_str": "+188",
                "ytm_str": "+15-8",
                "mty_str": "+15-0",
                "yr_ytm_str": "+15-8",
                "mth_ytm_str": "+15-8",
            },
            {
                "id": 1,
                "yr_str": "+8",
                "mth_str": "+96",
                "ytm_str": "+8-8",
                "mty_str": "+7-4",
                "yr_ytm_str": "+8-8",
                "mth_ytm_str": "+8-0",
            },
            {
                "id": 2,
                "yr_str": "+20",
                "mth_str": "+254",
                "ytm_str": "+20-8",
                "mty_str": "+20-6",
                "yr_ytm_str": "+20-8",
                "mth_ytm_str": "+21-2",
            },
        ]
        self.sql = """CREATE MATERIALIZED VIEW linterval_plus_linterval_str AS SELECT
                       id,
                       CAST((INTERVAL yrs_str YEAR + INTERVAL '10' YEAR) AS VARCHAR) AS yr_str,
                       CAST((INTERVAL mths_str MONTH + INTERVAL '128' MONTH) AS VARCHAR) AS mth_str,
                       CAST((INTERVAL yrs_str YEAR + INTERVAL '128' MONTH) AS VARCHAR) AS ytm_str,
                       CAST((INTERVAL mths_str MONTH + INTERVAL '10' YEAR) AS VARCHAR) AS mty_str,
                       CAST((INTERVAL yrs_str YEAR + INTERVAL '10-08' YEAR TO MONTH) AS VARCHAR) AS yr_ytm_str,
                       CAST((INTERVAL mths_str MONTHS + INTERVAL '10-08' YEAR TO MONTH) AS VARCHAR ) AS mth_ytm_str
                       FROM timestamp_minus_timestamp_str"""


# Equivalent SQL for Postgres
# SELECT
#    INTERVAL '10 year' YEAR +/- INTERVAL '10 year' YEAR AS yr,
#    EXTRACT(MONTH FROM (INTERVAL '126 months' +/- INTERVAL '128 months')) + EXTRACT(YEAR FROM (INTERVAL '126 months' +/- INTERVAL '128 months')) * 12 || ' months' AS mth,
#    INTERVAL '10 years' +/- INTERVAL '128 months' AS ytm,
#    INTERVAL '126 months' +/- INTERVAL '10 years' AS mty,
#    INTERVAL '10 years' +/- INTERVAL '10 year 8 months ' AS yr_ytm,
#    INTERVAL '126 months' +/- INTERVAL '10 year 8 months' AS mth_ytm
# ;


# Short Interval Type
# Using Calculated Intervals and Declared Intervals for Subtraction and Addition
class arithtst_sinterval_minus_sinterval(TstView):
    def __init__(self):
        # Validated on Postgres
        self.data = [
            {
                "id": 0,
                "days": "+1705",
                "hours": "+40939",
                "minute": "+2456382",
                "seconds": "+40939:42:00.000000",
            },
            {
                "id": 1,
                "days": "-1130",
                "hours": "-27124",
                "minute": "-1627440",
                "seconds": "-27124:00:00.000000",
            },
            {
                "id": 2,
                "days": "+3703",
                "hours": "+88874",
                "minute": "+5332457",
                "seconds": "+88874:17:00.000000",
            },
        ]
        self.sql = """CREATE MATERIALIZED VIEW sinterval_minus_sinterval AS SELECT
                      id,
                      CAST((days - INTERVAL '150'  DAYS) AS VARCHAR)  AS days,
                      CAST((hours- INTERVAL '3600'  HOURS) AS VARCHAR)  AS hours,
                      CAST((minutes - INTERVAL '216000'  MINUTE) AS VARCHAR)  AS minute,
                      CAST((hours - INTERVAL '12960000'  SECOND) AS VARCHAR)  AS seconds
                      FROM atimestamp_minus_timestamp"""


class arithtst_sinterval_plus_sinterval(TstView):
    def __init__(self):
        # Validated on Postgres
        self.data = [
            {
                "id": 0,
                "days": "+2005",
                "hours": "+48139",
                "minute": "+2888382",
                "seconds": "+48139:42:00.000000",
            },
            {
                "id": 1,
                "days": "-830",
                "hours": "-19924",
                "minute": "-1195440",
                "seconds": "-19924:00:00.000000",
            },
            {
                "id": 2,
                "days": "+4003",
                "hours": "+96074",
                "minute": "+5764457",
                "seconds": "+96074:17:00.000000",
            },
        ]
        self.sql = """CREATE MATERIALIZED VIEW sinterval_plus_sinterval AS SELECT
                      id,
                      CAST((days + INTERVAL '150'  DAYS) AS VARCHAR)  AS days,
                      CAST((hours + INTERVAL '3600'  HOURS) AS VARCHAR)  AS hours,
                      CAST((minutes + INTERVAL '216000'  MINUTE) AS VARCHAR)  AS minute,
                      CAST((hours + INTERVAL '12960000'  SECOND) AS VARCHAR)  AS seconds
                      FROM atimestamp_minus_timestamp"""


# Equivalent SQL for Postgres
# SELECT
#    (INTERVAL '-84686400 seconds' -/+ INTERVAL '12960000 seconds') AS diff
# ;


# Using Calculated Intervals cast as VARCHAR and Declared Intervals for Subtraction and Addition
class arithtst_sinterval_minus_sinterval_str(TstView):
    def __init__(self):
        # Validated in Postgres
        self.data = [
            {
                "id": 0,
                "days_str": "+1705",
                "hours_str": "+40939",
                "minute_str": "+2456382",
                "seconds_str": "+147382920.000000",
            },
            {
                "id": 1,
                "days_str": "-1130",
                "hours_str": "-27124",
                "minute_str": "-1627440",
                "seconds_str": "-97646400.000000",
            },
            {
                "id": 2,
                "days_str": "+3703",
                "hours_str": "+88874",
                "minute_str": "+5332457",
                "seconds_str": "+319947420.000000",
            },
        ]
        self.sql = """CREATE MATERIALIZED VIEW sinterval_minus_sinterval_str AS SELECT
                      id,
                      CAST((INTERVAL days_str DAYS - INTERVAL '150'  DAYS) AS VARCHAR)  AS days_str,
                      CAST((INTERVAL hrs_str HOURS - INTERVAL '3600'  HOURS) AS VARCHAR)  AS hours_str,
                      CAST((INTERVAL min_str MINUTE - INTERVAL '216000'  MINUTE) AS VARCHAR)  AS minute_str,
                      CAST((INTERVAL sec_str SECOND - INTERVAL '12960000'  SECOND) AS VARCHAR)  AS seconds_str
                      FROM timestamp_minus_timestamp_str"""


class arithtst_sinterval_plus_sinterval_str(TstView):
    def __init__(self):
        # Validated in Postgres
        self.data = [
            {
                "id": 0,
                "days_str": "+2005",
                "hours_str": "+48139",
                "minute_str": "+2888382",
                "seconds_str": "+173302920.000000",
            },
            {
                "id": 1,
                "days_str": "-830",
                "hours_str": "-19924",
                "minute_str": "-1195440",
                "seconds_str": "-71726400.000000",
            },
            {
                "id": 2,
                "days_str": "+4003",
                "hours_str": "+96074",
                "minute_str": "+5764457",
                "seconds_str": "+345867420.000000",
            },
        ]
        self.sql = """CREATE MATERIALIZED VIEW sinterval_plus_sinterval_str AS SELECT
                      id,
                      CAST((INTERVAL days_str DAYS + INTERVAL '150'  DAYS) AS VARCHAR)  AS days_str,
                      CAST((INTERVAL hrs_str HOURS + INTERVAL '3600'  HOURS) AS VARCHAR)  AS hours_str,
                      CAST((INTERVAL min_str MINUTE + INTERVAL '216000'  MINUTE) AS VARCHAR)  AS minute_str,
                      CAST((INTERVAL sec_str SECOND + INTERVAL '12960000'  SECOND) AS VARCHAR)  AS seconds_str
                      FROM timestamp_minus_timestamp_str"""


# Other Intervals(YEAR to MONTH, DAY TO HOUR, DAY TO MINUTE, DAY TO SECOND, HOUR TO MINUTE, HOUR TO SECOND, MINUTE TO SECOND)
# Using Calculated Intervals and Declared Intervals for Subtraction and Addition
class arithtst_iinterval_minus_iinterval(TstView):
    def __init__(self):
        # Validated in Postgres
        self.data = [
            {
                "id": 0,
                "ytm_yr": "-5-0",
                "ytm_ytm": "-5-8",
                "dth_days": "+1705 19",
                "dth_dth": "+1704 21",
                "dtm_dtm": "+1704 20:47",
                "dts_dts": "+1704 20:46:46.000000",
                "htm_hr": "+40939:42",
                "htm_htm": "+40939:29",
                "hts_hts": "+40939:28:20.000000",
                "mts_min": "+2456382:00.000000",
                "mts_mts": "+2456381:27.000000",
            },
            {
                "id": 1,
                "ytm_yr": "-12-8",
                "ytm_ytm": "-13-4",
                "dth_days": "-1130 04",
                "dth_dth": "-1131 02",
                "dtm_dtm": "-1131 02:55",
                "dts_dts": "-1131 02:55:14.000000",
                "htm_hr": "-27124:00",
                "htm_htm": "-27124:13",
                "hts_hts": "-27124:13:40.000000",
                "mts_min": "-1627440:00.000000",
                "mts_mts": "-1627440:33.000000",
            },
            {
                "id": 2,
                "ytm_yr": "+0-6",
                "ytm_ytm": "-0-2",
                "dth_days": "+3703 02",
                "dth_dth": "+3702 04",
                "dtm_dtm": "+3702 03:22",
                "dts_dts": "+3702 03:21:46.000000",
                "htm_hr": "+88874:17",
                "htm_htm": "+88874:04",
                "hts_hts": "+88874:03:20.000000",
                "mts_min": "+5332457:00.000000",
                "mts_mts": "+5332456:27.000000",
            },
        ]
        self.sql = """CREATE MATERIALIZED VIEW iinterval_minus_iinterval AS SELECT
                      id,
                      CAST((ytm - INTERVAL '10' YEAR) AS VARCHAR) AS ytm_yr,
                      CAST((ytm - INTERVAL '10-08' YEAR TO MONTH) AS VARCHAR) AS ytm_ytm,
                      CAST((dth - INTERVAL '150'  DAYS) AS VARCHAR)  AS dth_days,
                      CAST((dth - INTERVAL '150 22'  DAY TO HOUR) AS VARCHAR)  AS dth_dth,
                      CAST((dtm - INTERVAL '150 22:55'  DAY TO MINUTE) AS VARCHAR)  AS dtm_dtm,
                      CAST((dts - INTERVAL '150 22:55:14.000000'  DAY TO SECOND) AS VARCHAR)  AS dts_dts,
                      CAST((htm - INTERVAL '3600'  HOURS) AS VARCHAR)  AS htm_hr,
                      CAST((htm - INTERVAL '3600:13' HOUR TO MINUTE) AS VARCHAR)  AS htm_htm,
                      CAST((htm - INTERVAL '3600:13:40.000000'  HOUR TO SECOND) AS VARCHAR)  AS hts_hts,
                      CAST((mts - INTERVAL '216000'  MINUTE) AS VARCHAR)  AS mts_min,
                      CAST((mts - INTERVAL '216000:33.000000'  MINUTE TO SECOND) AS VARCHAR)  AS mts_mts
                      FROM ats_minus_ts"""


class arithtst_iinterval_plus_iinterval(TstView):
    def __init__(self):
        # Validated in Postgres
        self.data = [
            {
                "id": 0,
                "ytm_yr": "+15-0",
                "ytm_ytm": "+15-8",
                "dth_days": "+2005 19",
                "dth_dth": "+2006 17",
                "dtm_dtm": "+2006 18:37",
                "dts_dts": "+2006 18:37:14.000000",
                "htm_hr": "+48139:42",
                "htm_htm": "+48139:55",
                "hts_hts": "+48139:55:40.000000",
                "mts_min": "+2888382:00.000000",
                "mts_mts": "+2888382:33.000000",
            },
            {
                "id": 1,
                "ytm_yr": "+7-4",
                "ytm_ytm": "+8-0",
                "dth_days": "-830 04",
                "dth_dth": "-829 06",
                "dtm_dtm": "-829 05:05",
                "dts_dts": "-829 05:04:46.000000",
                "htm_hr": "-19924:00",
                "htm_htm": "-19923:47",
                "hts_hts": "-19923:46:20.000000",
                "mts_min": "-1195440:00.000000",
                "mts_mts": "-1195439:27.000000",
            },
            {
                "id": 2,
                "ytm_yr": "+20-6",
                "ytm_ytm": "+21-2",
                "dth_days": "+4003 02",
                "dth_dth": "+4004 00",
                "dtm_dtm": "+4004 01:12",
                "dts_dts": "+4004 01:12:14.000000",
                "htm_hr": "+96074:17",
                "htm_htm": "+96074:30",
                "hts_hts": "+96074:30:40.000000",
                "mts_min": "+5764457:00.000000",
                "mts_mts": "+5764457:33.000000",
            },
        ]
        self.sql = """CREATE MATERIALIZED VIEW iinterval_plus_iinterval AS SELECT
                      id,
                      CAST((ytm + INTERVAL '10' YEAR) AS VARCHAR) AS ytm_yr,
                      CAST((ytm + INTERVAL '10-08' YEAR TO MONTH) AS VARCHAR) AS ytm_ytm,
                      CAST((dth + INTERVAL '150'  DAYS) AS VARCHAR)  AS dth_days,
                      CAST((dth + INTERVAL '150 22'  DAY TO HOUR) AS VARCHAR)  AS dth_dth,
                      CAST((dtm + INTERVAL '150 22:55'  DAY TO MINUTE) AS VARCHAR)  AS dtm_dtm,
                      CAST((dts + INTERVAL '150 22:55:14.000000'  DAY TO SECOND) AS VARCHAR)  AS dts_dts,
                      CAST((htm + INTERVAL '3600'  HOURS) AS VARCHAR)  AS htm_hr,
                      CAST((htm + INTERVAL '3600:13' HOUR TO MINUTE) AS VARCHAR)  AS htm_htm,
                      CAST((htm + INTERVAL '3600:13:40.000000'  HOUR TO SECOND) AS VARCHAR)  AS hts_hts,
                      CAST((mts + INTERVAL '216000'  MINUTE) AS VARCHAR)  AS mts_min,
                      CAST((mts + INTERVAL '216000:33.000000'  MINUTE TO SECOND) AS VARCHAR)  AS mts_mts
                      FROM ats_minus_ts"""


# Using Constant Intervals for Subtraction and Addition
class arithtst_interval_minus_interval_constant(TstView):
    def __init__(self):
        # Validated in Postgres
        self.data = [
            {
                "const_ytm_yr": "-5-00",
                "const_ytm_ytm": "-5-08",
                "const_dth_days": "+1705 19",
                "const_dth_dth": "+1704 21",
                "const_dtm_dtm": "+1704 20:47",
                "const_dts_dts": "+1704 20:46:46.000000",
                "const_htm_hr": "+40939:42",
                "const_htm_htm": "+40939:29",
                "const_hts_hts": "+40939:28:20.000000",
                "const_mts_min": "+2456382:00.000000",
                "const_mts_mts": "+2456381:27.000000",
            }
        ]
        self.sql = """CREATE MATERIALIZED VIEW interval_minus_interval_constant AS SELECT
                      CAST((INTERVAL '+5-0' YEAR TO MONTH - INTERVAL '10' YEAR) AS VARCHAR) AS const_ytm_yr,
                      CAST((INTERVAL '+5-0' YEAR TO MONTH - INTERVAL '10-08' YEAR TO MONTH) AS VARCHAR) AS const_ytm_ytm,
                      CAST((INTERVAL '+1855 19' DAY TO HOUR - INTERVAL '150'  DAYS) AS VARCHAR)  AS const_dth_days,
                      CAST((INTERVAL '+1855 19' DAY TO HOUR - INTERVAL '150 22'  DAY TO HOUR) AS VARCHAR)  AS const_dth_dth,
                      CAST((INTERVAL '+1855 19:42' DAY TO MINUTE - INTERVAL '150 22:55'  DAY TO MINUTE) AS VARCHAR)  AS const_dtm_dtm,
                      CAST((INTERVAL '+1855 19:42:00.000000' DAY TO SECOND - INTERVAL '150 22:55:14.000000'  DAY TO SECOND) AS VARCHAR)  AS const_dts_dts,
                      CAST((INTERVAL '+44539:42' HOUR TO MINUTE - INTERVAL '3600'  HOURS) AS VARCHAR)  AS const_htm_hr,
                      CAST((INTERVAL '+44539:42' HOUR TO MINUTE - INTERVAL '3600:13' HOUR TO MINUTE) AS VARCHAR)  AS const_htm_htm,
                      CAST((INTERVAL '+44539:42:00.000000' HOUR TO SECOND - INTERVAL '3600:13:40.000000'  HOUR TO SECOND) AS VARCHAR)  AS const_hts_hts,
                      CAST((INTERVAL '+2672382:00.000000' MINUTE TO SECOND - INTERVAL '216000'  MINUTE) AS VARCHAR)  AS const_mts_min,
                      CAST((INTERVAL '+2672382:00.000000' MINUTE TO SECOND - INTERVAL '216000:33.000000'  MINUTE TO SECOND) AS VARCHAR)  AS const_mts_mts"""


class arithtst_interval_plus_interval_constant(TstView):
    def __init__(self):
        # Validated in Postgres
        self.data = [
            {
                "const_ytm_yr": "+15-00",
                "const_ytm_ytm": "+15-08",
                "const_dth_days": "+2005 19",
                "const_dth_dth": "+2006 17",
                "const_dtm_dtm": "+2006 18:37",
                "const_dts_dts": "+2006 18:37:14.000000",
                "const_htm_hr": "+48139:42",
                "const_htm_htm": "+48139:55",
                "const_hts_hts": "+48139:55:40.000000",
                "const_mts_min": "+2888382:00.000000",
                "const_mts_mts": "+2888382:33.000000",
            }
        ]
        self.sql = """CREATE MATERIALIZED VIEW interval_plus_interval_constant AS SELECT
                      CAST((INTERVAL '+5-0' YEAR TO MONTH + INTERVAL '10' YEAR) AS VARCHAR) AS const_ytm_yr,
                      CAST((INTERVAL '+5-0' YEAR TO MONTH + INTERVAL '10-08' YEAR TO MONTH) AS VARCHAR) AS const_ytm_ytm,
                      CAST((INTERVAL '+1855 19' DAY TO HOUR + INTERVAL '150'  DAYS) AS VARCHAR)  AS const_dth_days,
                      CAST((INTERVAL '+1855 19' DAY TO HOUR + INTERVAL '150 22'  DAY TO HOUR) AS VARCHAR)  AS const_dth_dth,
                      CAST((INTERVAL '+1855 19:42' DAY TO MINUTE + INTERVAL '150 22:55'  DAY TO MINUTE) AS VARCHAR)  AS const_dtm_dtm,
                      CAST((INTERVAL '+1855 19:42:00.000000' DAY TO SECOND + INTERVAL '150 22:55:14.000000'  DAY TO SECOND) AS VARCHAR)  AS const_dts_dts,
                      CAST((INTERVAL '+44539:42' HOUR TO MINUTE + INTERVAL '3600'  HOURS) AS VARCHAR)  AS const_htm_hr,
                      CAST((INTERVAL '+44539:42' HOUR TO MINUTE + INTERVAL '3600:13' HOUR TO MINUTE) AS VARCHAR)  AS const_htm_htm,
                      CAST((INTERVAL '+44539:42:00.000000' HOUR TO SECOND + INTERVAL '3600:13:40.000000'  HOUR TO SECOND) AS VARCHAR)  AS const_hts_hts,
                      CAST((INTERVAL '+2672382:00.000000' MINUTE TO SECOND + INTERVAL '216000'  MINUTE) AS VARCHAR)  AS const_mts_min,
                      CAST((INTERVAL '+2672382:00.000000' MINUTE TO SECOND + INTERVAL '216000:33.000000'  MINUTE TO SECOND) AS VARCHAR)  AS const_mts_mts"""


# Equivalent SQL for Postgres
# SELECT
#    INTERVAL '5 year 0 months' YEAR TO MONTH -/+ INTERVAL '10 year' YEAR AS ytm_yr,
#    INTERVAL '5 year 0 months' YEAR TO MONTH -/+ INTERVAL '10 year 8 months' YEAR TO MONTH AS ytm_ytm,
#    INTERVAL '1855 days 19 hours' -/+ INTERVAL '150 days' AS dth_days,
#    INTERVAL '1855 days 19 hours' -/+ INTERVAL '150 days 22 hours' AS dth_dth,
#    INTERVAL '1855 days 19 hours 42 minutes' -/+ INTERVAL '150 days 22 hours 55 minutes' AS dtm_dtm,
#    INTERVAL '1855 days 19 hours 42 minutes 0 seconds' -/+ INTERVAL '150 days 22 hours 55 minutes 14 seconds' AS dts_dts,
#    INTERVAL '44539 hours 42 minutes' -/+ INTERVAL '3600 hours' AS htm_hr,
#    INTERVAL '44539 hours 42 minutes' -/+ INTERVAL '3600 hours 13 minutes' AS htm_htm,
#    INTERVAL '44539 hours 42 minutes 0 seconds' -/+ INTERVAL '3600 hours 13 minutes 40 seconds' AS hts_hts,
#    INTERVAL '2672382 minutes 0 seconds' -/+ INTERVAL '216000 minutes' AS mts_min,
#    INTERVAL '2672382 minutes 0 seconds' -/+ INTERVAL '216000 minutes 33 seconds' AS mts_mts
# ;


# Long and Short Intervals
# Using Calculated Intervals for Negation, Scalar Multiplication and Division
class arithtst_interval_negation(TstView):
    def __init__(self):
        # checked manually
        self.data = [
            {
                "id": 0,
                "seconds": -160342920,
                "minutes": -2672382,
                "hours": -44539,
                "days": -1855,
                "months": -60,
                "year": -5,
            },
            {
                "id": 1,
                "seconds": 84686400,
                "minutes": 1411440,
                "hours": 23524,
                "days": 980,
                "months": 32,
                "year": 2,
            },
            {
                "id": 2,
                "seconds": -332907420,
                "minutes": -5548457,
                "hours": -92474,
                "days": -3853,
                "months": -126,
                "year": -10,
            },
        ]
        self.sql = """CREATE MATERIALIZED VIEW interval_negation AS SELECT
                      id,
                      CAST(-seconds AS BIGINT) AS seconds,
                      CAST(-minutes AS BIGINT) AS minutes,
                      CAST(-hours AS BIGINT) AS hours,
                      CAST(-days AS BIGINT) AS days,
                      CAST(-months AS BIGINT) AS months,
                      CAST(-years AS BIGINT) AS year
                      FROM atimestamp_minus_timestamp """


class arithtst_interval_mul_double(TstView):
    def __init__(self):
        # checked manually
        self.data = [
            {
                "id": 0,
                "seconds": "+414165762.000000",
                "minutes": "+6902762",
                "hours": "+115046",
                "days": "+4793",
                "months": "+154",
                "year": "+12",
            },
            {
                "id": 1,
                "seconds": "-218744971.000000",
                "minutes": "-3645749",
                "hours": "-60762",
                "days": "-2531",
                "months": "-82",
                "year": "-6",
            },
            {
                "id": 2,
                "seconds": "+859899865.000000",
                "minutes": "+14331664",
                "hours": "+238861",
                "days": "+9952",
                "months": "+325",
                "year": "+27",
            },
        ]
        self.sql = """CREATE MATERIALIZED VIEW interval_mul_double AS SELECT
                      id,
                      CAST(seconds * 2.583 AS VARCHAR) AS seconds,
                      CAST(minutes * 2.583 AS VARCHAR) AS minutes,
                      CAST(hours * 2.583 AS VARCHAR) AS hours,
                      CAST(days * 2.583 AS VARCHAR) AS days,
                      CAST(months * 2.583 AS VARCHAR) AS months,
                      CAST(years * 2.583 AS VARCHAR) AS year
                      FROM atimestamp_minus_timestamp """


class arithtst_interval_div_double(TstView):
    def __init__(self):
        # checked manually
        self.data = [
            {
                "id": 0,
                "seconds": "+62076236.000000",
                "minutes": "+1034603",
                "hours": "+17243",
                "days": "+718",
                "months": "+23",
                "year": "+1",
            },
            {
                "id": 1,
                "seconds": "-32786062.000000",
                "minutes": "-546434",
                "hours": "-9107",
                "days": "-379",
                "months": "-12",
                "year": "-1",
            },
            {
                "id": 2,
                "seconds": "+128884018.000000",
                "minutes": "+2148066",
                "hours": "+35801",
                "days": "+1491",
                "months": "+48",
                "year": "+4",
            },
        ]
        self.sql = """CREATE MATERIALIZED VIEW interval_div_double AS SELECT
                      id,
                      CAST(seconds / 2.583 AS VARCHAR) AS seconds,
                      CAST(minutes / 2.583 AS VARCHAR) AS minutes,
                      CAST(hours / 2.583 AS VARCHAR) AS hours,
                      CAST(days / 2.583 AS VARCHAR) AS days,
                      CAST(months / 2.583 AS VARCHAR) AS months,
                      CAST(years / 2.583 AS VARCHAR) AS year
                      FROM atimestamp_minus_timestamp """


# Using Calculated Intervals cast as VARCHAR for Negation, Scalar Multiplication and Division
class arithtst_interval_negation_str(TstView):
    def __init__(self):
        # checked manually
        self.data = [
            {
                "id": 0,
                "sec_str": "-160342920",
                "min_str": "-2672382",
                "hrs_str": "-44539",
                "days_str": "-1855",
                "mths_str": "-60",
                "yrs_str": "-5",
            },
            {
                "id": 1,
                "sec_str": "84686400",
                "min_str": "1411440",
                "hrs_str": "23524",
                "days_str": "980",
                "mths_str": "32",
                "yrs_str": "2",
            },
            {
                "id": 2,
                "sec_str": "-332907420",
                "min_str": "-5548457",
                "hrs_str": "-92474",
                "days_str": "-3853",
                "mths_str": "-126",
                "yrs_str": "-10",
            },
        ]
        self.sql = """CREATE MATERIALIZED VIEW interval_negation_str AS SELECT
                      id,
                      CAST(-sec_str  AS VARCHAR) AS sec_str,
                      CAST(-min_str  AS VARCHAR) AS min_str,
                      CAST(-hrs_str AS VARCHAR) AS hrs_str,
                      CAST(-days_str AS VARCHAR) AS days_str,
                      CAST(-mths_str AS VARCHAR) AS mths_str,
                      CAST(-yrs_str  AS VARCHAR) AS yrs_str
                      FROM timestamp_minus_timestamp_str"""


class arithtst_interval_mul_double_str(TstView):
    def __init__(self):
        # Validated on Postgres
        self.data = [
            {
                "id": 0,
                "sec_str": "+414165762.000000",
                "min_str": "+6902762",
                "hrs_str": "+115044",
                "days_str": "+4791",
                "mths_str": "+154",
                "yrs_str": "+12",
            },
            {
                "id": 1,
                "sec_str": "-218744971.000000",
                "min_str": "-3645749",
                "hrs_str": "-60762",
                "days_str": "-2531",
                "mths_str": "-82",
                "yrs_str": "-5",
            },
            {
                "id": 2,
                "sec_str": "+859899865.000000",
                "min_str": "+14331664",
                "hrs_str": "+238860",
                "days_str": "+9952",
                "mths_str": "+325",
                "yrs_str": "+25",
            },
        ]
        self.sql = """CREATE MATERIALIZED VIEW linterval_mul_double_str AS SELECT
                      id,
                      CAST((INTERVAL sec_str SECOND) * 2.583 AS VARCHAR) AS sec_str,
                      CAST((INTERVAL min_str MINUTE) * 2.583 AS VARCHAR) AS min_str,
                      CAST((INTERVAL hrs_str HOUR) * 2.583 AS VARCHAR) AS hrs_str,
                      CAST((INTERVAL days_str DAY) * 2.583 AS VARCHAR) AS days_str,
                      CAST((INTERVAL mths_str MONTH) * 2.583 AS VARCHAR) AS mths_str,
                      CAST((INTERVAL yrs_str YEAR) * 2.583 AS VARCHAR) AS yrs_str
                      FROM timestamp_minus_timestamp_str"""


class arithtst_interval_div_double_str(TstView):
    def __init__(self):
        # Validated on Postgres
        self.data = [
            {
                "id": 0,
                "sec_str": "+62076236.000000",
                "min_str": "+1034603",
                "hrs_str": "+17243",
                "days_str": "+718",
                "mths_str": "+23",
                "yrs_str": "+1",
            },
            {
                "id": 1,
                "sec_str": "-32786062.000000",
                "min_str": "-546434",
                "hrs_str": "-9107",
                "days_str": "-379",
                "mths_str": "-12",
                "yrs_str": "+0",
            },
            {
                "id": 2,
                "sec_str": "+128884018.000000",
                "min_str": "+2148066",
                "hrs_str": "+35801",
                "days_str": "+1491",
                "mths_str": "+48",
                "yrs_str": "+3",
            },
        ]
        self.sql = """CREATE MATERIALIZED VIEW interval_div_double_str AS SELECT
                      id,
                      CAST((INTERVAL sec_str SECOND) / 2.583 AS VARCHAR) AS sec_str,
                      CAST((INTERVAL min_str MINUTE) / 2.583 AS VARCHAR) AS min_str,
                      CAST((INTERVAL hrs_str HOUR) / 2.583 AS VARCHAR) AS hrs_str,
                      CAST((INTERVAL days_str DAY) / 2.583 AS VARCHAR) AS days_str,
                      CAST((INTERVAL mths_str MONTH) / 2.583 AS VARCHAR) AS mths_str,
                      CAST((INTERVAL yrs_str YEAR) / 2.583 AS VARCHAR) AS yrs_str
                      FROM timestamp_minus_timestamp_str"""


# Equivalent SQL for Postgres
# SELECT
#    EXTRACT(YEAR FROM (INTERVAL '5 years' * 2.583)) AS diff,
#    EXTRACT(YEAR FROM (INTERVAL '60 months' * 2.583))*12 + EXTRACT(MONTH FROM (INTERVAL '60 months' * 2.583)) AS diff,
#    EXTRACT(DAYS FROM (INTERVAL '1855 days' * 2.583)) AS diff,
#    INTERVAL '44539 hours' * 2.583 AS diff,
#    EXTRACT(HOURS FROM (INTERVAL '2672382 minutes' * 2.583)) * 60 + EXTRACT(MINUTES FROM (INTERVAL '2672382 minutes' * 2.583)) AS diff,
#    EXTRACT(HOURS FROM (INTERVAL '160342920 seconds' * 2.583)) * 3600 + EXTRACT(MINUTES FROM (INTERVAL '160342920 seconds' * 2.583)) * 60 + EXTRACT(SECONDS FROM (INTERVAL '160342920 seconds' * 2.583)) AS diff
# ;


# Other Intervals(YEAR to MONTH, DAY TO HOUR, DAY TO MINUTE, DAY TO SECOND, HOUR TO MINUTE, HOUR TO SECOND, MINUTE TO SECOND)
# Using Calculated Intervals for Negation, Scalar Multiplication and Division
class arithtst_iinterval_negation(TstView):
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
        self.sql = """CREATE MATERIALIZED VIEW iinterval_negation AS SELECT
                      id,
                      CAST(-ytm AS VARCHAR) AS ytm_neg,
                      CAST(-dth AS VARCHAR) AS dth_neg,
                      CAST(-dtm AS VARCHAR) AS dtm_neg,
                      CAST(-dts AS VARCHAR) AS dts_neg,
                      CAST(-htm AS VARCHAR) AS htm_neg,
                      CAST(-hts AS VARCHAR) AS hts_neg,
                      CAST(-mts AS VARCHAR) AS mts_neg
                      FROM ats_minus_ts """


class arithtst_iinterval_mul_double(TstView):
    def __init__(self):
        # Validated on Postgres
        self.data = [
            {
                "id": 0,
                "ytm": "+12-10",
                "dth": "+4793 14",
                "dtm": "+4793 14:02",
                "dts": "+4793 14:02:42.000000",
                "htm": "+115046:02",
                "hts": "+115046:02:42.000000",
                "mts": "+6902762:42.000000",
            },
            {
                "id": 1,
                "ytm": "-6-10",
                "dth": "-2531 18",
                "dtm": "-2531 18:29",
                "dts": "-2531 18:29:31.000000",
                "htm": "-60762:29",
                "hts": "-60762:29:31.000000",
                "mts": "-3645749:31.000000",
            },
            {
                "id": 2,
                "ytm": "+27-1",
                "dth": "+9952 13",
                "dtm": "+9952 13:04",
                "dts": "+9952 13:04:25.000000",
                "htm": "+238861:04",
                "hts": "+238861:04:25.000000",
                "mts": "+14331664:25.000000",
            },
        ]
        self.sql = """CREATE MATERIALIZED VIEW iinterval_mul_double AS SELECT
                      id,
                      CAST(ytm * 2.583 AS VARCHAR) AS ytm,
                      CAST(dth * 2.583 AS VARCHAR) AS dth,
                      CAST(dtm * 2.583 AS VARCHAR) AS dtm,
                      CAST(dts * 2.583 AS VARCHAR) AS dts,
                      CAST(htm * 2.583 AS VARCHAR) AS htm,
                      CAST(hts * 2.583 AS VARCHAR) AS hts,
                      CAST(mts * 2.583 AS VARCHAR) AS mts
                      FROM ats_minus_ts """


class arithtst_iinterval_div_double(TstView):
    def __init__(self):
        # Validated on Postgres
        self.data = [
            {
                "id": 0,
                "ytm": "+1-11",
                "dth": "+718 11",
                "dtm": "+718 11:23",
                "dts": "+718 11:23:56.000000",
                "htm": "+17243:23",
                "hts": "+17243:23:56.000000",
                "mts": "+1034603:56.000000",
            },
            {
                "id": 1,
                "ytm": "-1-0",
                "dth": "-379 11",
                "dtm": "-379 11:14",
                "dts": "-379 11:14:22.000000",
                "htm": "-9107:14",
                "hts": "-9107:14:22.000000",
                "mts": "-546434:22.000000",
            },
            {
                "id": 2,
                "ytm": "+4-0",
                "dth": "+1491 17",
                "dtm": "+1491 17:06",
                "dts": "+1491 17:06:58.000000",
                "htm": "+35801:06",
                "hts": "+35801:06:58.000000",
                "mts": "+2148066:58.000000",
            },
        ]
        self.sql = """CREATE MATERIALIZED VIEW iinterval_div_double AS SELECT
                      id,
                      CAST(ytm / 2.583 AS VARCHAR) AS ytm,
                      CAST(dth / 2.583 AS VARCHAR) AS dth,
                      CAST(dtm / 2.583 AS VARCHAR) AS dtm,
                      CAST(dts / 2.583 AS VARCHAR) AS dts,
                      CAST(htm / 2.583 AS VARCHAR) AS htm,
                      CAST(hts / 2.583 AS VARCHAR) AS hts,
                      CAST(mts / 2.583 AS VARCHAR) AS mts
                      FROM ats_minus_ts """


# Using Calculated Intervals cast as VARCHAR for Negation
# Ignored because: https://github.com/feldera/feldera/issues/3768
class empty_iinterval_negation_str(TstView):
    def __init__(self):
        # checked manually
        self.data = []
        self.sql = """CREATE MATERIALIZED VIEW iinterval_negation_str AS SELECT
                      id,
                      CAST(-ytm_str AS VARCHAR) AS ytm_str,
                      CAST(-dth_str AS VARCHAR) AS dth_str,
                      CAST(-dtm_str AS VARCHAR) AS dtm_str,
                      CAST(-dts_str AS VARCHAR) AS dts_str,
                      CAST(-htm_str AS VARCHAR) AS htm_str,
                      CAST(-hts_str AS VARCHAR) AS hts_str,
                      CAST(-mts_str AS VARCHAR) AS mts_str
                      FROM ats_minus_ts_str"""


# Using Constant Intervals for Negation, Scalar Multiplication and Division
class arithtst_const_interval_negation(TstView):
    def __init__(self):
        # Validated on Postgres
        self.data = [
            {
                "const_ytm_neg": "+2-08",
                "const_dth_neg": "+980 04",
                "const_dtm_neg": "+980 04:00",
                "const_dts_neg": "+980 04:00:00.000000",
                "const_htm_neg": "+23524:00",
                "const_hts_neg": "+23524:00:00.000000",
                "const_mts_neg": "+1411440:00.000000",
            }
        ]
        self.sql = """CREATE MATERIALIZED VIEW const_interval_negation AS SELECT
                      CAST(-(INTERVAL '-2-8' YEAR TO MONTH) AS VARCHAR) AS const_ytm_neg,
                      CAST(-(INTERVAL '-980 04' DAY TO HOUR) AS VARCHAR) AS const_dth_neg,
                      CAST(-(INTERVAL '-980 04:00' DAY TO MINUTE) AS VARCHAR) AS const_dtm_neg,
                      CAST(-(INTERVAL '-980 04:00:00.000000' DAY TO SECOND) AS VARCHAR) AS const_dts_neg,
                      CAST(-(INTERVAL '-23524:00' HOUR TO MINUTE) AS VARCHAR) AS const_htm_neg,
                      CAST(-(INTERVAL '-23524:00:00.000000' HOUR TO SECOND) AS VARCHAR) AS const_hts_neg,
                      CAST(-(INTERVAL '-1411440:00.000000' MINUTE TO SECOND) AS VARCHAR) AS const_mts_neg"""


class arithtst_const_interval_mul_double(TstView):
    def __init__(self):
        # Validated on Postgres
        self.data = [
            {
                "const_ytm": "-6-10",
                "const_dth": "-2531 18",
                "const_dtm": "-2531 18:29",
                "const_dts": "-2531 18:29:31.000000",
                "const_htm": "-60762:29",
                "const_hts": "-60762:29:31.000000",
                "const_mts": "-3645749:31.000000",
            }
        ]
        self.sql = """CREATE MATERIALIZED VIEW const_interval_mul_double AS SELECT
                      CAST((INTERVAL '-2-8' YEAR TO MONTH * 2.583) AS VARCHAR) AS const_ytm,
                      CAST((INTERVAL '-980 04' DAY TO HOUR * 2.583) AS VARCHAR) AS const_dth,
                      CAST((INTERVAL '-980 04:00' DAY TO MINUTE * 2.583) AS VARCHAR) AS const_dtm,
                      CAST((INTERVAL '-980 04:00:00.000000' DAY TO SECOND * 2.583) AS VARCHAR) AS const_dts,
                      CAST((INTERVAL '-23524:00' HOUR TO MINUTE * 2.583) AS VARCHAR) AS const_htm,
                      CAST((INTERVAL '-23524:00:00.000000' HOUR TO SECOND * 2.583) AS VARCHAR) AS const_hts,
                      CAST((INTERVAL '-1411440:00.000000' MINUTE TO SECOND * 2.583) AS VARCHAR) AS const_mts"""


class arithtst_const_interval_div_double(TstView):
    def __init__(self):
        # Validated on Postgres
        self.data = [
            {
                "const_ytm": "-1-00",
                "const_dth": "-379 11",
                "const_dtm": "-379 11:14",
                "const_dts": "-379 11:14:22.000000",
                "const_htm": "-9107:14",
                "const_hts": "-9107:14:22.000000",
                "const_mts": "-546434:22.000000",
            }
        ]
        self.sql = """CREATE MATERIALIZED VIEW const_interval_div_double AS SELECT
                      CAST((INTERVAL '-2-8' YEAR TO MONTH / 2.583) AS VARCHAR) AS const_ytm,
                      CAST((INTERVAL '-980 04' DAY TO HOUR / 2.583) AS VARCHAR) AS const_dth,
                      CAST((INTERVAL '-980 04:00' DAY TO MINUTE / 2.583) AS VARCHAR) AS const_dtm,
                      CAST((INTERVAL '-980 04:00:00.000000' DAY TO SECOND / 2.583) AS VARCHAR) AS const_dts,
                      CAST((INTERVAL '-23524:00' HOUR TO MINUTE / 2.583) AS VARCHAR) AS const_htm,
                      CAST((INTERVAL '-23524:00:00.000000' HOUR TO SECOND / 2.583) AS VARCHAR) AS const_hts,
                      CAST((INTERVAL '-1411440:00.000000' MINUTE TO SECOND / 2.583) AS VARCHAR) AS const_mts"""


# Equivalent SQL for Postgres
# SELECT
#    INTERVAL '10 year 6 months' YEAR TO MONTH / 2.583 AS ytm,
#    INTERVAL '3853 days 2 hours' / 2.583 AS dth,
#    INTERVAL '3853 days 2 hours 17 minutes' / 2.583 AS dtm,
#    INTERVAL '3853 days 2 hours 17 minutes 00 seconds' / 2.583 AS dts,
#    INTERVAL '92474 hours 17 minutes' / 2.583 AS htm,
#    INTERVAL '92474 hours 17 minutes 00 seconds' / 2.583 AS hts,
#    INTERVAL '5548457 minutes 0 seconds' / 2.583 AS mts
# ;
