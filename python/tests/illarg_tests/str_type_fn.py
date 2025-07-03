from tests.aggregate_tests.aggtst_base import TstView


# ASCII function(successful for all arguments)
class illarg_ascii_legal(TstView):
    def __init__(self):
        # checked manually
        self.data = [{"str": 104}]
        self.sql = """CREATE MATERIALIZED VIEW ascii_legal AS SELECT
                      ASCII(str) AS str
                      FROM illegal_tbl"""


class illarg_ascii_cast_legal(TstView):
    def __init__(self):
        # checked manually
        self.data = [{"intt": 45, "arr": 98}]
        self.sql = """CREATE MATERIALIZED VIEW ascii_cast_legal AS SELECT
                      ASCII(intt) AS intt,
                      ASCII(arr[1]) AS arr
                      FROM illegal_tbl"""


# CONCAT function
class illarg_concat_legal(TstView):
    def __init__(self):
        # checked manually
        self.data = [{"str": "hello hello "}]
        self.sql = """CREATE MATERIALIZED VIEW concat_legal AS SELECT
                      CONCAT(str, str) AS str
                      FROM illegal_tbl"""


class illarg_concat_cast_legal(TstView):
    def __init__(self):
        # checked manually
        self.data = [
            {
                "intt": "-12-12",
                "decimall": "-1111.52-1111.52",
                "reall": "-57681.18-57681.18",
                "dbl": "-38.2711234601246-38.2711234601246",
                "booll": "TRUETRUE",
                "tmestmp": "2020-06-21 14:23:442020-06-21 14:23:44",
                "uuidd": "42b8fec7-c7a3-4531-9611-4bde80f9cb4c42b8fec7-c7a3-4531-9611-4bde80f9cb4c",
            }
        ]
        self.sql = """CREATE MATERIALIZED VIEW concat_cast_legal AS SELECT
                      CONCAT(intt, intt) AS intt,
                      CONCAT(decimall, decimall) AS decimall,
                      CONCAT(reall, reall) AS reall,
                      CONCAT(dbl, dbl) AS dbl,
                      CONCAT(booll, booll) AS booll,
                      CONCAT(tmestmp, tmestmp) AS tmestmp,
                      CONCAT(uuidd, uuidd) AS uuidd
                      FROM illegal_tbl"""


# Negative Test
class illarg_concat_illegal(TstView):
    def __init__(self):
        # checked manually
        self.sql = """CREATE MATERIALIZED VIEW concat_illegal AS SELECT
                      CONCAT(bin, bin) AS bin
                      FROM illegal_tbl"""
        self.expected_error = "Not yet implemented"


# CONCAT_WS function(successful for all arguments)
class illarg_concatws_legal(TstView):
    def __init__(self):
        # checked manually
        self.data = [{"str": "hello @55"}]
        self.sql = """CREATE MATERIALIZED VIEW concatws_legal AS SELECT
                      CONCAT_WS('@', str, 55) AS str
                      FROM illegal_tbl"""


class illarg_concatws_cast_legal(TstView):
    def __init__(self):
        # checked manually
        self.data = [{"bin": "0b1620@55", "arr": "bye@55"}]
        self.sql = """CREATE MATERIALIZED VIEW concatws_cast_legal AS SELECT
                      CONCAT_WS('@', bin, 55) AS bin,
                      CONCAT_WS('@', arr[1], 55) AS arr
                      FROM illegal_tbl"""


# LEFT function(successful for all arguments)
class illarg_left_legal(TstView):
    def __init__(self):
        # checked manually
        self.data = [{"str": "he", "bin": "0b16"}]
        self.sql = """CREATE MATERIALIZED VIEW left_legal AS SELECT
                      LEFT(str, 2) AS str,
                      LEFT(bin, 2) AS bin
                      FROM illegal_tbl"""


class illarg_left_cast_legal(TstView):
    def __init__(self):
        # checked manually
        self.data = [{"intt": "-1", "booll": "TR"}]
        self.sql = """CREATE MATERIALIZED VIEW left_cast_legal AS SELECT
                      LEFT(intt, 2) AS intt,
                      LEFT(booll, 2) AS booll
                      FROM illegal_tbl"""


# RIGHT function(successful for all arguments)
class illarg_right_legal(TstView):
    def __init__(self):
        # checked manually
        self.data = [{"str": "o ", "bin": "1620"}]
        self.sql = """CREATE MATERIALIZED VIEW right_legal AS SELECT
                      RIGHT(str, 2) AS str,
                      RIGHT(bin, 2) AS bin
                      FROM illegal_tbl"""


class illarg_right_cast_legal(TstView):
    def __init__(self):
        # checked manually
        self.data = [{"tmestmp": "44", "uuidd": "4c"}]
        self.sql = """CREATE MATERIALIZED VIEW right_cast_legal AS SELECT
                      RIGHT(tmestmp, 2) AS tmestmp,
                      RIGHT(uuidd, 2) AS uuidd
                      FROM illegal_tbl"""


# INITCAP function(successful for all arguments)
class illarg_initcap_legal(TstView):
    def __init__(self):
        # checked manually
        self.data = [{"str": "Hello "}]
        self.sql = """CREATE MATERIALIZED VIEW initcap_legal AS SELECT
                      INITCAP(str) AS str
                      FROM illegal_tbl"""


class illarg_initcap_cast_legal(TstView):
    def __init__(self):
        # checked manually
        self.data = [{"booll": "True", "arr": "Bye"}]
        self.sql = """CREATE MATERIALIZED VIEW initcap_cast_legal AS SELECT
                      INITCAP(booll) AS booll,
                      INITCAP(arr[1]) AS arr
                      FROM illegal_tbl"""


# CHAR_LENGTH(string) or CHARACTER_LENGTH(string) or LENGTH(string) or LEN(string) function(successful for all arguments)
class illarg_len_legal(TstView):
    def __init__(self):
        # checked manually
        self.data = [{"str": 6}]
        self.sql = """CREATE MATERIALIZED VIEW len_legal AS SELECT
                      LEN(str) AS str
                      FROM illegal_tbl"""


class illarg_len_cast_legal(TstView):
    def __init__(self):
        # checked manually
        self.data = [{"intt": 3, "uuidd": 36}]
        self.sql = """CREATE MATERIALIZED VIEW len_cast_legal AS SELECT
                      LEN(intt) AS intt,
                      LEN(uuidd) AS uuidd
                      FROM illegal_tbl"""


# LOWER function(successful for all arguments)
class illarg_lower_legal(TstView):
    def __init__(self):
        # checked manually
        self.data = [{"str": "hello "}]
        self.sql = """CREATE MATERIALIZED VIEW lower_legal AS SELECT
                      LOWER(str) AS str
                      FROM illegal_tbl"""


class illarg_lower_cast_legal(TstView):
    def __init__(self):
        # checked manually
        self.data = [{"booll": "true", "arr": "see you!"}]
        self.sql = """CREATE MATERIALIZED VIEW lower_cast_legal AS SELECT
                      LOWER(booll) AS booll,
                      LOWER(arr[3]) AS arr
                      FROM illegal_tbl"""


# UPPER function(successful for all arguments)
class illarg_upper_legal(TstView):
    def __init__(self):
        # checked manually
        self.data = [{"str": "HELLO "}]
        self.sql = """CREATE MATERIALIZED VIEW upper_legal AS SELECT
                      UPPER(str) AS str
                      FROM illegal_tbl"""


class illarg_upper_cast_legal(TstView):
    def __init__(self):
        # checked manually
        self.data = [{"uuidd": "42B8FEC7-C7A3-4531-9611-4BDE80F9CB4C", "arr": "BYE"}]
        self.sql = """CREATE MATERIALIZED VIEW upper_cast_legal AS SELECT
                      UPPER(uuidd) AS uuidd,
                      UPPER(arr[1]) AS arr
                      FROM illegal_tbl"""


#  SUBSTR function(successful for all arguments)
class illarg_substr_legal(TstView):
    def __init__(self):
        # checked manually
        self.data = [{"str": "ell"}]
        self.sql = """CREATE MATERIALIZED VIEW substr_legal AS SELECT
                      SUBSTR(str, 2, 3) AS str
                      FROM illegal_tbl"""


class illarg_substr_cast_legal(TstView):
    def __init__(self):
        # checked manually
        self.data = [{"bin": "1620", "uuidd": "2b8"}]
        self.sql = """CREATE MATERIALIZED VIEW substr_cast_legal AS SELECT
                      SUBSTR(bin, 2, 3) AS bin,
                      SUBSTR(uuidd, 2, 3) AS uuidd
                      FROM illegal_tbl"""


# SUBSTRING function(successful for all arguments)
class illarg_substring_legal(TstView):
    def __init__(self):
        # checked manually
        self.data = [{"str": "ell"}]
        self.sql = """CREATE MATERIALIZED VIEW substring_legal AS SELECT
                      SUBSTRING(str from 2 for 3) AS str
                      FROM illegal_tbl"""


class illarg_substring_cast_legal(TstView):
    def __init__(self):
        # checked manually
        self.data = [{"tmestmp": "020", "bin": "1620"}]
        self.sql = """CREATE MATERIALIZED VIEW substring_cast_legal AS SELECT
                      SUBSTRING(tmestmp from 2 for 3) AS tmestmp,
                      SUBSTRING(bin from 2 for 3) AS bin
                      FROM illegal_tbl"""


# TRIM function
class illarg_trim_legal(TstView):
    def __init__(self):
        # checked manually
        self.data = [{"str": "hello"}]
        self.sql = """CREATE MATERIALIZED VIEW trim_legal AS SELECT
                      TRIM(trailing ' ' from str) AS str
                      FROM illegal_tbl"""


class illarg_trim_cast_legal(TstView):
    def __init__(self):
        # checked manually
        self.data = [{"intt": "-1", "booll": "RUE"}]
        self.sql = """CREATE MATERIALIZED VIEW trim_cast_legal AS SELECT
                      TRIM(trailing '2' from intt) AS intt,
                      TRIM(leading 'T' from booll) AS booll
                      FROM illegal_tbl"""


# Negative Test
class illarg_trim_illegal(TstView):
    def __init__(self):
        # checked manually
        self.sql = """CREATE MATERIALIZED VIEW trim_illegal AS SELECT
                      TRIM(leading '0b' from bin) AS bin
                      FROM illegal_tbl"""
        self.expected_error = "Cannot apply 'TRIM' to arguments of type"


# POSITION function(successful for all arguments)
class illarg_position_legal(TstView):
    def __init__(self):
        # checked manually
        self.data = [{"str": 2}]
        self.sql = """CREATE MATERIALIZED VIEW position_legal AS SELECT
                      POSITION('ell' in str) AS str
                      FROM illegal_tbl"""


class illarg_position_cast_legal(TstView):
    def __init__(self):
        # checked manually
        self.data = [{"reall": 2, "bin": 2}]
        self.sql = """CREATE MATERIALIZED VIEW position_cast_legal AS SELECT
                      POSITION('576' in reall) AS reall,
                      POSITION(x'16' in bin) AS bin
                      FROM illegal_tbl"""


# REGEXP_REPLACE function(successful for all arguments)
class illarg_regexp_replace_legal(TstView):
    def __init__(self):
        # checked manually
        self.data = [{"str": "hiiii "}]
        self.sql = """CREATE MATERIALIZED VIEW regexp_replace_legal AS SELECT
                      REGEXP_REPLACE(str, '[a-gi-z]', 'i') AS str
                      FROM illegal_tbl"""


class illarg_regexp_replace_cast_legal(TstView):
    def __init__(self):
        # checked manually
        self.data = [{"decimall": "-i.i", "uuidd": "ibifeci-ciai-i-i-ibdeificbic"}]
        self.sql = """CREATE MATERIALIZED VIEW regexp_replace_cast_legal AS SELECT
                      REGEXP_REPLACE(decimall, '[0-9]+', 'i') AS decimall,
                      REGEXP_REPLACE(uuidd, '[0-9]+', 'i') AS uuidd
                      FROM illegal_tbl"""


# RLIKE function(successful for all arguments)
class illarg_rlike_legal(TstView):
    def __init__(self):
        # checked manually
        self.data = [{"str": True}]
        self.sql = """CREATE MATERIALIZED VIEW rlike_legal AS SELECT
                      RLIKE(str, 'h..ll*') AS str
                      FROM illegal_tbl"""


class illarg_rlike_cast_legal(TstView):
    def __init__(self):
        # checked manually
        self.data = [{"booll": False, "tmestmp": True}]
        self.sql = """CREATE MATERIALIZED VIEW rlike_cast_legal AS SELECT
                      RLIKE(booll, '[0-9]+') AS booll,
                      RLIKE(tmestmp, '[0-9]+') AS tmestmp
                      FROM illegal_tbl"""


# SPLIT function(successful for all arguments)
class illarg_split_legal(TstView):
    def __init__(self):
        # checked manually
        self.data = [{"str": ["h", "llo "]}]
        self.sql = """CREATE MATERIALIZED VIEW split_legal AS SELECT
                      SPLIT(str, 'e') AS str
                      FROM illegal_tbl"""


class illarg_split_cast_legal(TstView):
    def __init__(self):
        # checked manually
        self.data = [
            {
                "decimall": ["-1111", "52"],
                "uuidd": ["42b8fec7", "c7a3", "4531", "9611", "4bde80f9cb4c"],
            }
        ]
        self.sql = """CREATE MATERIALIZED VIEW split_cast_legal AS SELECT
                      SPLIT(decimall, '.') AS decimall,
                      SPLIT(uuidd, '-') AS uuidd
                      FROM illegal_tbl"""


# SPLIT_PART function(successful for all arguments)
class illarg_split_part_legal(TstView):
    def __init__(self):
        # checked manually
        self.data = [{"str": "llo "}]
        self.sql = """CREATE MATERIALIZED VIEW split_part_legal AS SELECT
                      SPLIT_PART(str, 'e', 2) AS str
                      FROM illegal_tbl"""


class illarg_split_part_cast_legal(TstView):
    def __init__(self):
        # checked manually
        self.data = [{"intt": "-12", "uuidd": "c7a3"}]
        self.sql = """CREATE MATERIALIZED VIEW split_part_cast_legal AS SELECT
                      SPLIT_PART(intt, '', 1) AS intt,
                      SPLIT_PART(uuidd, '-', 2) AS uuidd
                      FROM illegal_tbl"""


# MD5 function(successful for all arguments)
class illarg_md5_legal(TstView):
    def __init__(self):
        # Validated on Postgres
        self.data = [{"str": "f814893777bcc2295fff05f00e508da6"}]
        self.sql = """CREATE MATERIALIZED VIEW md5_legal AS SELECT
                      MD5(str) AS str
                      FROM illegal_tbl"""


class illarg_md5_cast_legal(TstView):
    def __init__(self):
        # Validated on Postgres
        self.data = [
            {
                "uuidd": "be9a1f61ed7a08b3cc0fb79ba933d59c",
                "arr": "390dab837f65c126b113209e0a70a0c9",
            }
        ]
        self.sql = """CREATE MATERIALIZED VIEW md5_cast_legal AS SELECT
                      MD5(uuidd) AS uuidd,
                      MD5(arr[3]) AS arr
                      FROM illegal_tbl"""
