from tests.aggregate_tests.aggtst_base import TstView, TstTable
from decimal import Decimal


# INTEGER
class varnttst_int_tbl(TstTable):
    """Define the table used by the integer tests"""

    def __init__(self):
        self.sql = """CREATE TABLE varnt_int_tbl(
                      id INT,
                      tiny_int TINYINT,
                      small_int SMALLINT,
                      int INT,
                      bigint BIGINT)"""
        self.data = [
            {"id": 0, "tiny_int": 20, "small_int": 24, "int": 21, "bigint": 23},
            {"id": 1, "tiny_int": None, "small_int": None, "int": None, "bigint": None},
        ]


class varnttst_int_to_variant(TstView):
    def __init__(self):
        # checked manually
        self.data = [
            {
                "id": 0,
                "tiny_int_varnt": "20",
                "small_int_varnt": "24",
                "int_varnt": "21",
                "bigint_varnt": "23",
            },
            {
                "id": 1,
                "tiny_int_varnt": "null",
                "small_int_varnt": "null",
                "int_varnt": "null",
                "bigint_varnt": "null",
            },
        ]
        self.sql = """CREATE MATERIALIZED VIEW int_to_variant AS SELECT
                      id,
                      CAST(tiny_int AS VARIANT) AS tiny_int_varnt,
                      CAST(small_int AS VARIANT) AS small_int_varnt,
                      CAST(int AS VARIANT) AS int_varnt,
                      CAST(bigint AS VARIANT) AS bigint_varnt
                      FROM varnt_int_tbl"""


class varnttst_variant_to_int(TstView):
    def __init__(self):
        # checked manually
        self.data = [
            {"id": 0, "tiny_int": 20, "small_int": 24, "int": 21, "bigint": 23},
            {"id": 1, "tiny_int": None, "small_int": None, "int": None, "bigint": None},
        ]
        self.sql = """CREATE MATERIALIZED VIEW variant_to_int AS SELECT
                      id,
                      CAST(tiny_int_varnt AS TINYINT) AS tiny_int,
                      CAST(small_int_varnt AS SMALLINT) AS small_int,
                      CAST(int_varnt AS INT) AS int,
                      CAST(bigint_varnt AS BIGINT) AS bigint
                      FROM int_to_variant"""


class varnttst_variant_to_otherint(TstView):
    def __init__(self):
        # checked manually
        self.data = [
            {
                "id": 0,
                "tint_to_sint": 20,
                "tint_to_int": 20,
                "tint_to_bint": 20,
                "sint_to_tint": 24,
                "sint_to_int": 24,
                "sint_to_bint": 24,
                "int_to_tint": 21,
                "int_to_sint": 21,
                "int_to_bint": 21,
                "bint_to_tint": 23,
                "bint_to_sint": 23,
                "bint_to_int": 23,
            },
            {
                "id": 1,
                "tint_to_sint": None,
                "tint_to_int": None,
                "tint_to_bint": None,
                "sint_to_tint": None,
                "sint_to_int": None,
                "sint_to_bint": None,
                "int_to_tint": None,
                "int_to_sint": None,
                "int_to_bint": None,
                "bint_to_tint": None,
                "bint_to_sint": None,
                "bint_to_int": None,
            },
        ]
        self.sql = """CREATE MATERIALIZED VIEW variant_to_otherint AS SELECT
                      id,
                      CAST(tiny_int_varnt AS SMALLINT) AS tint_to_sint,
                      CAST(tiny_int_varnt AS INT) AS tint_to_int,
                      CAST(tiny_int_varnt AS BIGINT) AS tint_to_bint,
                      CAST(small_int_varnt AS TINYINT) AS sint_to_tint,
                      CAST(small_int_varnt AS INT) AS sint_to_int,
                      CAST(small_int_varnt AS BIGINT) AS sint_to_bint,
                      CAST(int_varnt AS TINYINT) AS int_to_tint,
                      CAST(int_varnt AS SMALLINT) AS int_to_sint,
                      CAST(int_varnt AS BIGINT) AS int_to_bint,
                      CAST(bigint_varnt AS TINYINT) AS bint_to_tint,
                      CAST(bigint_varnt AS SMALLINT) AS bint_to_sint,
                      CAST(bigint_varnt AS INT) AS bint_to_int
                      FROM int_to_variant"""


# BOOLEAN
class varnttst_bool_tbl(TstTable):
    """Define the table used by the bool tests"""

    def __init__(self):
        self.sql = """CREATE TABLE varnt_bool_tbl(
                      id INT,
                      bool1 BOOL
                      )"""
        self.data = [
            {"id": 0, "bool1": True},
            {"id": 1, "bool1": False},
            {"id": 2, "bool1": None},
        ]


class varnttst_bool_to_variant(TstView):
    def __init__(self):
        # checked manually
        self.data = [
            {"id": 0, "bool1_varnt": "true"},
            {"id": 1, "bool1_varnt": "false"},
            {"id": 2, "bool1_varnt": "null"},
        ]
        self.sql = """CREATE MATERIALIZED VIEW bool_to_variant AS SELECT
                      id,
                      CAST(bool1 AS VARIANT) AS bool1_varnt
                      FROM varnt_bool_tbl"""


class varnttst_variant_to_bool(TstView):
    def __init__(self):
        # checked manually
        self.data = [
            {"id": 0, "bool1": True},
            {"id": 1, "bool1": False},
            {"id": 2, "bool1": None},
        ]
        self.sql = """CREATE MATERIALIZED VIEW variant_to_bool AS SELECT
                      id,
                      CAST(bool1_varnt AS BOOL) AS bool1
                      FROM bool_to_variant"""


# DECIMAL
class varnttst_decimal_tbl(TstTable):
    """Define the table used by the decimal tests"""

    def __init__(self):
        self.sql = """CREATE TABLE varnt_decimal_tbl(
                      id INT,
                      decimall DECIMAL(6, 2)
                      )"""
        self.data = [{"id": 0, "decimall": 1111.52}, {"id": 1, "decimall": None}]


class varnttst_decimal_to_variant(TstView):
    def __init__(self):
        # checked manually
        self.data = [
            {"id": 0, "decimall_varnt": "1111.52"},
            {"id": 1, "decimall_varnt": "null"},
        ]
        self.sql = """CREATE MATERIALIZED VIEW decimal_to_variant AS SELECT
                      id,
                      CAST(decimall AS VARIANT) AS decimall_varnt
                      FROM varnt_decimal_tbl"""


class varnttst_variant_to_decimal(TstView):
    def __init__(self):
        # checked manually
        self.data = [
            {"id": 0, "decimall": Decimal("1111.52")},
            {"id": 1, "decimall": None},
        ]
        self.sql = """CREATE MATERIALIZED VIEW variant_to_decimal AS SELECT
                      id,
                      CAST(decimall_varnt AS DECIMAL(6,2)) AS decimall
                      FROM decimal_to_variant"""


class varnttst_variant_to_otherdecimal(TstView):
    def __init__(self):
        # checked manually
        self.data = [{"id": 0, "dec2": Decimal("1111.5")}, {"id": 1, "dec2": None}]
        self.sql = """CREATE MATERIALIZED VIEW variant_to_otherdecimal AS SELECT
                      id,
                      CAST(decimall_varnt AS DECIMAL(6, 1)) AS dec2
                      FROM decimal_to_variant"""


# FLOAT(REAL, DOUBLE)
class varnttst_float_tbl(TstTable):
    """Define the table used by the Float tests"""

    def __init__(self):
        self.sql = """CREATE TABLE varnt_float_tbl(
                      id INT,
                      real1 REAL,
                      double1 DOUBLE
                      )"""
        self.data = [
            {
                "id": 0,
                "real1": 57681.18,
                "double1": -38.2711234601246,
            },
            {
                "id": 1,
                "real1": None,
                "double1": None,
            },
        ]


class varnttst_float_to_variant(TstView):
    def __init__(self):
        # checked manually
        self.data = [
            {"id": 0, "real1_varnt": "57681.18", "double1_varnt": "-38.2711234601246"},
            {"id": 1, "real1_varnt": "null", "double1_varnt": "null"},
        ]
        self.sql = """CREATE MATERIALIZED VIEW float_to_variant AS SELECT
                      id,
                      CAST(real1 AS VARIANT) AS real1_varnt,
                      CAST(double1 AS VARIANT) AS double1_varnt
                      FROM varnt_float_tbl"""


# Returns Decimal because: https://github.com/feldera/feldera/issues/3396
class varnttst_variant_to_float(TstView):
    def __init__(self):
        # checked manually
        self.data = [
            {
                "id": 0,
                "real1": Decimal("57681.18"),
                "double1": Decimal("-38.2711234601246"),
            },
            {"id": 1, "real1": None, "double1": None},
        ]
        self.sql = """CREATE MATERIALIZED VIEW variant_to_float AS SELECT
                      id,
                      CAST(real1_varnt AS REAL) AS real1,
                      CAST(double1_varnt AS DOUBLE) AS double1
                      FROM float_to_variant"""


# Returns Decimal because: https://github.com/feldera/feldera/issues/3396
class varnttst_variant_to_otherfloat(TstView):
    def __init__(self):
        # checked manually
        self.data = [
            {
                "id": 0,
                "real_to_dbl": Decimal("57681.1796875"),
                "dbl_to_real": Decimal("-38.271122"),
            },
            {"id": 1, "real_to_dbl": None, "dbl_to_real": None},
        ]
        self.sql = """CREATE MATERIALIZED VIEW variant_to_otherfloat AS SELECT
                      id,
                      CAST(real1_varnt AS DOUBLE) AS real_to_dbl,
                      CAST(double1_varnt AS REAL) AS dbl_to_real
                      FROM float_to_variant"""


# STRING(CHAR, CHAR(n), VARCHAR, VARCHAR(n)
class varnttst_string_tbl(TstTable):
    """Define the table used by the string tests"""

    def __init__(self):
        self.sql = """CREATE TABLE varnt_string_tbl(
                      id INT,
                      charr CHAR,
                      charn CHAR(5),
                      vchar VARCHAR,
                      vcharn VARCHAR(5)
                      )"""
        self.data = [
            {
                "id": 0,
                "charr": "hi friend!",
                "charn": "hello",
                "vchar": "bye, mate!",
                "vcharn": "ciao!",
            },
            {
                "id": 1,
                "charr": None,
                "charn": None,
                "vchar": None,
                "vcharn": None,
            },
        ]


class varnttst_string_to_variant(TstView):
    def __init__(self):
        # checked manually
        self.data = [
            {
                "id": 0,
                "char_varnt": '"hi friend!"',
                "charn_varnt": '"hello"',
                "vchar_varnt": '"bye, mate!"',
                "vcharn_varnt": '"ciao!"',
            },
            {
                "id": 1,
                "char_varnt": "null",
                "charn_varnt": "null",
                "vchar_varnt": "null",
                "vcharn_varnt": "null",
            },
        ]
        self.sql = """CREATE MATERIALIZED VIEW string_to_variant AS SELECT
                      id,
                      CAST(charr AS VARIANT) AS char_varnt,
                      CAST(charn AS VARIANT) AS charn_varnt,
                      CAST(vchar AS VARIANT) AS vchar_varnt,
                      CAST(vcharn AS VARIANT) AS vcharn_varnt
                      FROM varnt_string_tbl"""


class varnttst_variant_to_string(TstView):
    def __init__(self):
        # checked manually
        self.data = [
            {
                "id": 0,
                "charr": "h",
                "charn": "hello",
                "vchar": "bye, mate!",
                "vcharn": "ciao!",
            },
            {"id": 1, "charr": None, "charn": None, "vchar": None, "vcharn": None},
        ]
        self.sql = """CREATE MATERIALIZED VIEW variant_to_string AS SELECT
                      id,
                      CAST(char_varnt AS CHAR) AS charr,
                      CAST(charn_varnt AS CHAR(5)) AS charn,
                      CAST(vchar_varnt AS VARCHAR) AS vchar,
                      CAST(vcharn_varnt AS VARCHAR(5)) AS vcharn
                      FROM string_to_variant"""


class varnttst_variant_to_otherstring(TstView):
    def __init__(self):
        # checked manually
        self.data = [
            {
                "id": 0,
                "charr": "hi",
                "vchar": "bye",
                "ch_to_vch": "hi",
                "vch_to_ch": "bye",
            },
            {
                "id": 1,
                "charr": None,
                "vchar": None,
                "ch_to_vch": None,
                "vch_to_ch": None,
            },
        ]
        self.sql = """CREATE MATERIALIZED VIEW variant_to_otherstring AS SELECT
                      id,
                      CAST(char_varnt AS CHAR(2)) AS charr,
                      CAST(vchar_varnt AS VARCHAR(3)) AS vchar,
                      CAST(char_varnt AS VARCHAR(2)) AS ch_to_vch,
                      CAST(vchar_varnt AS CHAR(3)) AS vch_to_ch
                      FROM string_to_variant"""


# BINARY, VARBINARY
class varnttst_binary_tbl(TstTable):
    """Define the table used by the binary and varbinary tests"""

    def __init__(self):
        self.sql = """CREATE TABLE varnt_binary_tbl(
                      id INT,
                      binary BINARY(4),
                      vbinary VARBINARY
                      )"""
        self.data = [
            {
                "id": 0,
                "binary": [12, 22, 32],
                "vbinary": [23, 56, 33, 21],
            },
            {
                "id": 1,
                "binary": None,
                "vbinary": None,
            },
        ]


class varnttst_binary_to_variant(TstView):
    def __init__(self):
        # checked manually
        self.data = [
            {"id": 0, "binary_varnt": "[12,22,32]", "vbinary_varnt": "[23,56,33,21]"},
            {"id": 1, "binary_varnt": "null", "vbinary_varnt": "null"},
        ]
        self.sql = """CREATE MATERIALIZED VIEW binary_to_variant AS SELECT
                      id,
                      CAST(binary AS VARIANT) AS binary_varnt,
                      CAST(vbinary AS VARIANT) AS vbinary_varnt
                      FROM varnt_binary_tbl"""


class varnttst_variant_to_binary(TstView):
    def __init__(self):
        # checked manually
        self.data = [
            {"id": 0, "binary": "0c162000", "vbinary": "17382115"},
            {"id": 1, "binary": None, "vbinary": None},
        ]
        self.sql = """CREATE MATERIALIZED VIEW variant_to_binary AS SELECT
                      id,
                      CAST(binary_varnt AS BINARY(4)) AS binary,
                      CAST(vbinary_varnt AS VARBINARY) AS vbinary
                      FROM binary_to_variant"""


class varnttst_variant_to_otherbinary(TstView):
    def __init__(self):
        # checked manually
        self.data = [
            {"id": 0, "bin_to_vbin": "0c1620", "vbin_to_bin": "17382115"},
            {"id": 1, "bin_to_vbin": None, "vbin_to_bin": None},
        ]
        self.sql = """CREATE MATERIALIZED VIEW variant_to_otherbinary AS SELECT
                      id,
                      CAST(binary_varnt AS VARBINARY) AS bin_to_vbin,
                      CAST(vbinary_varnt AS BINARY(4)) AS vbin_to_bin
                      FROM binary_to_variant"""


# DATE, TIME, TIMESTAMP
class varnttst_time_tbl(TstTable):
    """Define the table used by the time tests"""

    def __init__(self):
        self.sql = """CREATE TABLE varnt_time_tbl(
                      id INT,
                      datee DATE,
                      tmestmp TIMESTAMP,
                      timee TIME
                      )"""
        self.data = [
            {
                "id": 0,
                "datee": "2024-12-05",
                "tmestmp": "2020-06-21 14:23:44",
                "timee": "18:30:45",
            },
            {
                "id": 1,
                "datee": None,
                "tmestmp": None,
                "timee": None,
            },
        ]


class varnttst_time_to_variant(TstView):
    def __init__(self):
        # checked manually
        self.data = [
            {
                "id": 0,
                "datee_varnt": '"2024-12-05"',
                "tmestmp_varnt": '"2020-06-21 14:23:44"',
                "timee_varnt": '"18:30:45"',
            },
            {
                "id": 1,
                "datee_varnt": "null",
                "tmestmp_varnt": "null",
                "timee_varnt": "null",
            },
        ]
        self.sql = """CREATE MATERIALIZED VIEW time_to_variant AS SELECT
                      id,
                      CAST(datee AS VARIANT) AS datee_varnt,
                      CAST(tmestmp AS VARIANT) AS tmestmp_varnt,
                      CAST(timee AS VARIANT) AS timee_varnt
                      FROM varnt_time_tbl"""


class varnttst_variant_to_time(TstView):
    def __init__(self):
        # checked manually
        self.data = [
            {
                "id": 0,
                "datee": "2024-12-05",
                "tmestmp": "2020-06-21T14:23:44",
                "timee": "18:30:45",
            },
            {"id": 1, "datee": None, "tmestmp": None, "timee": None},
        ]
        self.sql = """CREATE MATERIALIZED VIEW variant_to_time AS SELECT
                      id,
                      CAST(datee_varnt AS DATE) AS datee,
                      CAST(tmestmp_varnt AS TIMESTAMP) AS tmestmp,
                      CAST(timee_varnt AS TIME) AS timee
                      FROM time_to_variant"""


# UUID
class varnttst_auuid_tbl(TstTable):
    """Define the table used by the time tests"""

    def __init__(self):
        self.sql = """CREATE TABLE varnt_uuid_tbl(
                      id INT,
                      uuid1 UUID
                      )"""
        self.data = [
            {"id": 0, "uuid1": "724b11b7-5a71-4d18-b241-299f82d9b403"},
            {"id": 1, "uuid1": None},
        ]


class varnttst_auuid_to_variant(TstView):
    def __init__(self):
        # checked manually
        self.data = [
            {"id": 0, "uuid1_varnt": '"724b11b7-5a71-4d18-b241-299f82d9b403"'},
            {"id": 1, "uuid1_varnt": "null"},
        ]
        self.sql = """CREATE MATERIALIZED VIEW uuid_to_variant AS SELECT
                      id,
                      CAST(uuid1 AS VARIANT) AS uuid1_varnt
                      FROM varnt_uuid_tbl"""


class varnttst_variant_to_uuid(TstView):
    def __init__(self):
        # checked manually
        self.data = [
            {"id": 0, "uuid1": "724b11b7-5a71-4d18-b241-299f82d9b403"},
            {"id": 1, "uuid1": None},
        ]
        self.sql = """CREATE MATERIALIZED VIEW variant_to_uuid AS SELECT
                      id,
                      CAST(uuid1_varnt AS UUID) AS uuid1
                      FROM uuid_to_variant"""


# Complex Types(Array, Map, Row)
class varnttst_cmpx_tbl(TstTable):
    """Define the table used by the complex type tests"""

    def __init__(self):
        self.sql = """CREATE TABLE varnt_cmpx_tbl(
                      id INT,
                      arr INT ARRAY,
                      map MAP<VARCHAR, INT>,
                      roww ROW(int INT, var VARCHAR)
                      )"""
        self.data = [
            {
                "id": 0,
                "arr": [12, 22],
                "map": {"1": 22, "2": 44},
                "roww": {"int": 20, "var": "bye bye!"},
            },
            {
                "id": 1,
                "arr": None,
                "map": None,
                "roww": None,
            },
        ]


class varnttst_cmpx_to_variant(TstView):
    def __init__(self):
        # checked manually
        self.data = [
            {
                "id": 0,
                "arr_varnt": "[12,22]",
                "map_varnt": '{"1":22,"2":44}',
                "roww_varnt": '{"int":20,"var":"bye bye!"}',
            },
            {
                "id": 1,
                "arr_varnt": "null",
                "map_varnt": "null",
                "roww_varnt": '{"int":null,"var":null}',
            },
        ]
        self.sql = """CREATE MATERIALIZED VIEW cmpx_to_variant AS SELECT
                      id,
                      CAST(arr AS VARIANT) AS arr_varnt,
                      CAST(map AS VARIANT) AS map_varnt,
                      CAST(roww AS VARIANT) AS roww_varnt
                      FROM varnt_cmpx_tbl"""


class varnttst_variant_to_cmpx(TstView):
    def __init__(self):
        # checked manually
        self.data = [
            {
                "id": 0,
                "arr": [12, 22],
                "map": {"1": 22, "2": 44},
                "roww": {"int": 20, "var": "bye bye!"},
            },
            {"id": 1, "arr": None, "map": None, "roww": {"int": None, "var": None}},
        ]
        self.sql = """CREATE MATERIALIZED VIEW variant_to_cmpx AS SELECT
                      id,

                      CAST(arr_varnt AS INT ARRAY) AS arr,
                      CAST(map_varnt AS MAP<VARCHAR, INT>) AS map,
                      CAST(roww_varnt AS ROW(int INT, var VARCHAR)) AS roww
                      FROM cmpx_to_variant"""
