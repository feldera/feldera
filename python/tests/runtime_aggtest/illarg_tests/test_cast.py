from tests.runtime_aggtest.aggtst_base import TstView
from decimal import Decimal


# INT to other types
# Casting a numeric value to TIMESTAMP interprets the value as epoch seconds
class illarg_cast_int(TstView):
    def __init__(self):
        self.data = [
            {
                "to_decimal": Decimal("-12.00"),
                "to_real": Decimal("-12.0"),
                "to_double": Decimal("-12.0"),
                "to_bool": True,
                "to_varchar": "-12",
                "to_binary": "f4",
                "to_timestamp": "1970-01-01T00:00:00",
            }
        ]
        self.sql = """CREATE MATERIALIZED VIEW cast_int AS SELECT
                      CAST(intt AS DECIMAL(6,2)) AS to_decimal,
                      CAST(intt AS REAL) AS to_real,
                      CAST(intt AS DOUBLE) AS to_double,
                      CAST(intt AS BOOL) AS to_bool,
                      CAST(intt AS VARCHAR) AS to_varchar,
                      CAST(intt AS BINARY) AS to_binary,
                      CAST(intt AS TIMESTAMP) AS to_timestamp
                      FROM illegal_tbl
                      WHERE id = 0;"""


# Decimal to other types
class illarg_cast_decimal_legal(TstView):
    def __init__(self):
        self.data = [
            {
                "to_intt": -1111,
                "to_real": Decimal("-1111.52"),
                "to_double": Decimal("-1111.52"),
                "to_bool": True,
                "to_varchar": "-1111.52",
                "to_timestamp": "1969-12-31T23:59:59",
            }
        ]
        self.sql = """CREATE MATERIALIZED VIEW cast_decimal_legal AS SELECT
                      CAST(decimall AS INT) AS to_intt,
                      CAST(decimall AS REAL) AS to_real,
                      CAST(decimall AS DOUBLE) AS to_double,
                      CAST(decimall AS BOOL) AS to_bool,
                      CAST(decimall AS VARCHAR) AS to_varchar,
                      CAST(decimall AS TIMESTAMP) AS to_timestamp
                      FROM illegal_tbl
                      WHERE id = 0;"""


# REAL to other types
class illarg_cast_real_legal(TstView):
    def __init__(self):
        self.data = [
            {
                "to_int": -57681,
                "to_decimal": None,
                "to_double": Decimal("-57681.1796875"),
                "to_bool": True,
                "to_varchar": "-57681.18",
                "to_timestamp": "1969-12-31T23:59:03",
            }
        ]
        self.sql = """CREATE MATERIALIZED VIEW cast_real_legal AS SELECT
                      CAST(reall AS INT) AS to_int,
                      SAFE_CAST(reall AS DECIMAL(6,2)) AS to_decimal,
                      CAST(reall AS DOUBLE) AS to_double,
                      CAST(reall AS BOOL) AS to_bool,
                      CAST(reall AS VARCHAR) AS to_varchar,
                      CAST(reall AS TIMESTAMP) AS to_timestamp
                      FROM illegal_tbl
                      WHERE id = 0;"""


# DOUBLE to other types
class illarg_cast_double_legal(TstView):
    def __init__(self):
        self.data = [
            {
                "to_intt": -38,
                "to_decimal": Decimal("-38.27"),
                "to_real": Decimal("-38.271122"),
                "to_bool": True,
                "to_varchar": "-38.2711234601246",
                "to_timestamp": "1970-01-01T00:00:00",
            }
        ]
        self.sql = """CREATE MATERIALIZED VIEW cast_double_legal AS SELECT
                      CAST(dbl AS INT) AS to_intt,
                      CAST(dbl AS DECIMAL(6,2)) AS to_decimal,
                      CAST(dbl AS REAL) AS to_real,
                      CAST(dbl AS BOOL) AS to_bool,
                      CAST(dbl AS VARCHAR) AS to_varchar,
                      CAST(dbl AS TIMESTAMP) AS to_timestamp
                      FROM illegal_tbl
                      WHERE id = 0;"""


# BOOLEAN to other types
class illarg_cast_boolean_legal(TstView):
    def __init__(self):
        self.data = [{"to_bool": True, "to_varchar": "TRUE"}]
        self.sql = """CREATE MATERIALIZED VIEW cast_boolean_legal AS SELECT
                      CAST(booll AS BOOL) AS to_bool,
                      CAST(booll AS VARCHAR) AS to_varchar
                      FROM illegal_tbl
                      WHERE id = 0;"""


# STRING to other types
class illarg_cast_varchar_legal(TstView):
    def __init__(self):
        self.data = [
            {
                "to_int": None,
                "to_decimal": None,
                "to_real": Decimal("0.0"),
                "to_double": Decimal("0.0"),
                "to_bool": False,
                "to_binary": "68",
                "to_timestamp": None,
                "to_date": None,
                "to_time": None,
                "to_uuid": None,
            }
        ]
        self.sql = """CREATE MATERIALIZED VIEW cast_varchar_legal AS SELECT
                      SAFE_CAST(str AS INT) AS to_int,
                      SAFE_CAST(str AS DECIMAL(6,2)) AS to_decimal,
                      CAST(str AS REAL) AS to_real,
                      CAST(str AS DOUBLE) AS to_double,
                      CAST(str AS BOOL) AS to_bool,
                      CAST(str AS BINARY) AS to_binary,
                      SAFE_CAST(str AS TIMESTAMP) AS to_timestamp,
                      SAFE_CAST(str AS DATE) AS to_date,
                      SAFE_CAST(str AS TIME) AS to_time,
                      SAFE_CAST(str AS UUID) AS to_uuid
                      FROM illegal_tbl
                      WHERE id = 0;"""


# BINARY to other types
class illarg_cast_binary_legal(TstView):
    def __init__(self):
        self.data = [{"to_varchar": "0b1620", "to_uuid": None}]
        self.sql = """CREATE MATERIALIZED VIEW cast_binary_legal AS SELECT
                      CAST(bin AS VARCHAR) AS to_varchar,
                      SAFE_CAST(bin AS UUID) AS to_uuid
                      FROM illegal_tbl
                      WHERE id = 0;"""


class illarg_cast_binary_legal1(TstView):
    def __init__(self):
        self.data = [{"to_uuid": "1f8b0800-0000-0000-00ff-4b4bcd49492d"}]
        self.sql = """CREATE MATERIALIZED VIEW cast_binary_legal1 AS SELECT
                      CAST(bin AS UUID) AS to_uuid
                      FROM illegal_tbl
                      WHERE id = 1;"""


# TIMESTAMP to other types
class illarg_cast_timestamp_legal(TstView):
    def __init__(self):
        self.data = [
            {
                "to_int": None,
                "to_decimal": 1592749424123,
                "to_decimal1": None,
                "to_real": Decimal("1.5927495E+12"),
                "to_double": Decimal("1592749424123"),
                "to_varchar": "2020-06-21 14:23:44",
                "to_date": "2020-06-21",
                "to_time": "14:23:44.123654",
            }
        ]
        self.sql = """CREATE MATERIALIZED VIEW cast_timestamp_legal AS SELECT
                      SAFE_CAST(tmestmp AS INT) AS to_int,
                      CAST(tmestmp AS DECIMAL) AS to_decimal,
                      SAFE_CAST(tmestmp AS DECIMAL(6,2)) AS to_decimal1,
                      SAFE_CAST(tmestmp AS REAL) AS to_real,
                      CAST(tmestmp AS DOUBLE) AS to_double,
                      CAST(tmestmp AS VARCHAR) AS to_varchar,
                      CAST(tmestmp AS DATE) AS to_date,
                      CAST(tmestmp AS TIME) AS to_time
                      FROM illegal_tbl
                      WHERE id = 0;"""


# DATE to other types
class illarg_cast_date_legal(TstView):
    def __init__(self):
        self.data = [
            {"to_varchar": "2020-06-21", "to_timestamp": "2020-06-21T00:00:00"}
        ]
        self.sql = """CREATE MATERIALIZED VIEW cast_date_legal AS SELECT
                      CAST(datee AS VARCHAR) AS to_varchar,
                      CAST(datee AS TIMESTAMP) AS to_timestamp
                      FROM illegal_tbl
                      WHERE id = 0;"""


# TIME to other types
class illarg_cast_time_legal(TstView):
    def __init__(self):
        self.data = [
            {"to_varchar": "14:23:44", "to_timestamp": "1970-01-01T14:23:44.456"}
        ]
        self.sql = """CREATE MATERIALIZED VIEW cast_time_legal AS SELECT
                      CAST(tme AS VARCHAR) AS to_varchar,
                      CAST(tme AS TIMESTAMP) AS to_timestamp
                      FROM illegal_tbl
                      WHERE id = 0;"""


# UUID to other types
class illarg_cast_uuid(TstView):
    def __init__(self):
        self.data = [
            {"to_varchar": "42b8fec7-c7a3-4531-9611-4bde80f9cb4c", "to_binary": "42"}
        ]
        self.sql = """CREATE MATERIALIZED VIEW cast_uuid AS SELECT
                      CAST(uuidd AS VARCHAR) AS to_varchar,
                      CAST(uuidd AS BINARY) AS to_binary
                      FROM illegal_tbl
                      WHERE id = 0;"""


# ARRAY to other types
class illarg_cast_array_legal(TstView):
    def __init__(self):
        self.data = [
            {
                "to_array": [
                    "bye",
                    "14",
                    "See you!",
                    "-0.52",
                    None,
                    "14",
                    "hello ",
                    "TRUE",
                ]
            }
        ]
        self.sql = """CREATE MATERIALIZED VIEW cast_array_legal AS SELECT
                      SAFE_CAST(arr AS VARCHAR ARRAY) AS to_array
                      FROM illegal_tbl
                      WHERE id = 0;"""


class illarg_cast_array_legal1(TstView):
    def __init__(self):
        self.data = [{"to_ts": "2020-10-01T00:00:00"}]
        self.sql = """CREATE MATERIALIZED VIEW cast_array_legal1 AS SELECT
                      SAFE_CAST(arr[6] AS TIMESTAMP) AS to_ts
                      FROM illegal_tbl
                      WHERE id = 1;"""


# MAP to other types
class illarg_safe_cast_map_legal(TstView):
    def __init__(self):
        self.data = [{"to_map": {"a": "12", "b": "17"}}]
        self.sql = """CREATE MATERIALIZED VIEW safe_cast_map_legal AS SELECT
                      SAFE_CAST(mapp AS MAP<VARCHAR, VARCHAR>) AS to_map
                      FROM illegal_tbl
                      WHERE id = 0;"""


# ROW to other types
class illarg_cast_row(TstView):
    def __init__(self):
        self.data = [
            {"to_row": {"i1": 4, "v1": "cat"}, "to_udt": {"i1": 4, "v1": "cat"}}
        ]
        self.sql = """CREATE MATERIALIZED VIEW cast_row AS SELECT
                      CAST(roww AS ROW(i1 INT, v1 VARCHAR)) AS to_row,
                      CAST(roww AS user_def) AS to_udt
                      FROM illegal_tbl
                      WHERE id = 0;"""


# UDT to other types
class illarg_safe_cast_udt(TstView):
    def __init__(self):
        self.data = [
            {"to_row": {"i1": 4, "v1": "cat"}, "to_udt": {"i1": 4, "v1": "cat"}}
        ]
        self.sql = """CREATE MATERIALIZED VIEW safe_cast_udt AS SELECT
                      CAST(udt AS ROW(i1 INT, v1 VARCHAR)) AS to_row,
                      CAST(udt AS user_def) AS to_udt
                      FROM illegal_tbl
                      WHERE id = 0;"""


# Complex type field to their respective types
class illarg_cast_cmpx_legal(TstView):
    def __init__(self):
        self.data = [
            {
                "map_int": 12,
                "row_int": 4,
                "row_var": "cat",
                "udt_int": 4,
                "udt_var": "cat",
            }
        ]
        self.sql = """CREATE MATERIALIZED VIEW cast_cmpx_legal AS SELECT
                      CAST(mapp['a'] AS INT) AS map_int,
                      CAST(roww[1] AS INT) AS row_int,
                      CAST(roww[2] AS VARCHAR) AS row_var,
                      CAST(udt[1] AS INT) AS udt_int,
                      CAST(udt[2] AS VARCHAR) AS udt_var
                      FROM illegal_tbl
                      WHERE id = 0;"""


# NULL to all types
class illarg_cast_all(TstView):
    def __init__(self):
        self.data = []
        self.sql = """CREATE MATERIALIZED VIEW cast_all AS SELECT
                      CAST(intt AS DECIMAL(6,2)) AS int_to_decimal,
                      CAST(decimall AS TIMESTAMP) AS decimal_to_timestamp,
                      CAST(reall AS INT) AS real_to_int,
                      CAST(dbl AS BOOL) AS double_to_bool,
                      CAST(booll AS VARCHAR) AS bool_to_varchar,
                      CAST(str AS BINARY) AS str_to_binary,
                      SAFE_CAST(bin AS UUID) AS binary_to_uuid,
                      CAST(tmestmp AS TIME) AS ts_to_time,
                      CAST(datee AS VARCHAR) AS date_to_varchar,
                      CAST(tme AS TIMESTAMP) AS time_to_timestamp,
                      CAST(uuidd AS BINARY) AS uuid_to_binary,
                      CAST(arr AS VARCHAR ARRAY) AS array_to_array,
                      CAST(mapp AS MAP<VARCHAR, VARCHAR>) AS map_to_map,
                      CAST(roww AS ROW(i1 INT, v1 VARCHAR)) AS row_to_row,
                      CAST(udt AS user_def) AS udt_to_udt
                      FROM illegal_tbl
                      WHERE id = 3;"""


# Negative Tests
class illarg_cast_real_illegal(TstView):
    def __init__(self):
        self.sql = """CREATE MATERIALIZED VIEW cast_real_illegal AS SELECT
                      CAST(reall AS DECIMAL(6,2)) AS to_decimal
                      FROM illegal_tbl
                      WHERE id = 0;"""
        self.expected_error = (
            "Error converting -57681.18 to DECIMAL(6, 2): Value out of range"
        )


class illarg_cast_varchar_illegal(TstView):
    def __init__(self):
        self.sql = """CREATE MATERIALIZED VIEW cast_varchar_illegal AS SELECT
                      CAST(str AS VARCHAR ARRAY) AS to_array
                      FROM illegal_tbl
                      WHERE id = 0;"""
        self.expected_error = "Cast cannot be used to convert VARCHAR to VARCHAR ARRAY"


class illarg_cast_map_illegal(TstView):
    def __init__(self):
        self.sql = """CREATE MATERIALIZED VIEW cast_map_illegal AS SELECT
                      CAST(mapp AS user_def) AS to_udt
                      FROM illegal_tbl
                      WHERE id = 0;"""
        self.expected_error = "Cast function cannot convert value of type"


class illarg_safe_cast_int_illegal(TstView):
    def __init__(self):
        self.sql = """CREATE MATERIALIZED VIEW safe_cast_int AS SELECT
                      SAFE_CAST(intt AS ROW(i1 INT, v1 VARCHAR)) AS to_row
                      FROM illegal_tbl
                      WHERE id = 0 ;"""
        self.expected_error = "Cast function cannot convert value of type"


class illarg_safe_cast_map_illegal(TstView):
    def __init__(self):
        self.sql = """CREATE MATERIALIZED VIEW safe_cast_map_illegal AS SELECT
                      SAFE_CAST(mapp AS MAP<VARCHAR, INT ARRAY>) AS to_map
                      FROM illegal_tbl
                      WHERE id = 0;"""
        self.expected_error = "Cast function cannot convert value of type"


class illarg_safe_cast_row_illegal(TstView):
    def __init__(self):
        self.sql = """CREATE MATERIALIZED VIEW safe_cast_row_illegal AS SELECT
                      SAFE_CAST(roww AS ROW(i1 INT, v1 VARCHAR)) AS to_row
                      FROM illegal_tbl
                      WHERE id = 0;"""
        self.expected_error = (
            "SAFE_CAST cannot be used to convert ROW(INT, VARCHAR) to ROW(INT, VARCHAR)"
        )


class illarg_cast_array_illegal(TstView):
    def __init__(self):
        self.sql = """CREATE MATERIALIZED VIEW cast_array_illegal AS SELECT
                      CAST(arr[6] AS TIME) AS to_tme
                      FROM illegal_tbl
                      WHERE id = 1;"""
        self.expected_error = "Error converting 2020-10-01 00:00:00 to TIME: input contains invalid characters"
