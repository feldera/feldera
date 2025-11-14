from tests.runtime_aggtest.aggtst_base import TstView


# ASCII function(successful for all arguments except Complex Types)
class illarg_ascii_legal(TstView):
    def __init__(self):
        # checked manually
        self.data = [{"str": 104}]
        self.sql = """CREATE MATERIALIZED VIEW ascii_legal AS SELECT
                      ASCII(str) AS str
                      FROM illegal_tbl
                      WHERE id = 0"""


class illarg_ascii_cast_legal(TstView):
    def __init__(self):
        # checked manually
        self.data = [{"intt": 45, "arr": 98}]
        self.sql = """CREATE MATERIALIZED VIEW ascii_cast_legal AS SELECT
                      ASCII(intt) AS intt,
                      ASCII(arr[1]) AS arr
                      FROM illegal_tbl
                      WHERE id = 0"""


# Negative Test
class illarg_ascii_illegal(TstView):
    def __init__(self):
        # checked manually
        self.sql = """CREATE MATERIALIZED VIEW ascii_illegal AS SELECT
                      ASCII(mapp) AS roww
                      FROM illegal_tbl"""
        self.expected_error = "Cannot apply 'ASCII' to arguments of type"


# CONCAT function
class illarg_concat_legal(TstView):
    def __init__(self):
        # checked manually
        self.data = [{"str": "hello hello "}]
        self.sql = """CREATE MATERIALIZED VIEW concat_legal AS SELECT
                      CONCAT(str, str) AS str
                      FROM illegal_tbl
                      WHERE id = 0"""


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
                      FROM illegal_tbl
                      WHERE id = 0"""


# Negative Test
class illarg_concat_illegal(TstView):
    def __init__(self):
        # checked manually
        self.sql = """CREATE MATERIALIZED VIEW concat_illegal AS SELECT
                      CONCAT(bin, bin) AS bin
                      FROM illegal_tbl"""
        self.expected_error = "Not yet implemented"


# CONCAT_WS function(successful for all arguments except Complex Types)
class illarg_concatws_legal(TstView):
    def __init__(self):
        # checked manually
        self.data = [{"str": "hello @55"}]
        self.sql = """CREATE MATERIALIZED VIEW concatws_legal AS SELECT
                      CONCAT_WS('@', str, 55) AS str
                      FROM illegal_tbl
                      WHERE id = 0"""


class illarg_concatws_cast_legal(TstView):
    def __init__(self):
        # checked manually
        self.data = [{"bin": "0b1620@55", "arr": "bye@55"}]
        self.sql = """CREATE MATERIALIZED VIEW concatws_cast_legal AS SELECT
                      CONCAT_WS('@', bin, 55) AS bin,
                      CONCAT_WS('@', arr[1], 55) AS arr
                      FROM illegal_tbl
                      WHERE id = 0"""


# Negative Test
class illarg_concatws_illegal(TstView):
    def __init__(self):
        # checked manually
        self.sql = """CREATE MATERIALIZED VIEW concatws_illegal AS SELECT
                      CONCAT_WS('@', roww, 55) AS roww
                      FROM illegal_tbl"""
        self.expected_error = "Cannot apply 'CONCAT_WS' to arguments of type"


# LEFT function(successful for all arguments except Complex Types)
class illarg_left_legal(TstView):
    def __init__(self):
        # checked manually
        self.data = [{"str": "he", "bin": "0b16"}]
        self.sql = """CREATE MATERIALIZED VIEW left_legal AS SELECT
                      LEFT(str, 2) AS str,
                      LEFT(bin, 2) AS bin
                      FROM illegal_tbl
                      WHERE id = 0"""


class illarg_left_cast_legal(TstView):
    def __init__(self):
        # checked manually
        self.data = [{"intt": "-1", "booll": "TR"}]
        self.sql = """CREATE MATERIALIZED VIEW left_cast_legal AS SELECT
                      LEFT(intt, 2) AS intt,
                      LEFT(booll, 2) AS booll
                      FROM illegal_tbl
                      WHERE id = 0"""


# Negative Test
class illarg_left_illegal(TstView):
    def __init__(self):
        # checked manually
        self.sql = """CREATE MATERIALIZED VIEW left_illegal AS SELECT
                      LEFT(roww, 2) AS roww
                      FROM illegal_tbl"""
        self.expected_error = "Cannot apply 'LEFT' to arguments of type"


# RIGHT function(successful for all arguments except Complex Types)
class illarg_right_legal(TstView):
    def __init__(self):
        # checked manually
        self.data = [{"str": "o ", "bin": "1620"}]
        self.sql = """CREATE MATERIALIZED VIEW right_legal AS SELECT
                      RIGHT(str, 2) AS str,
                      RIGHT(bin, 2) AS bin
                      FROM illegal_tbl
                      WHERE id = 0"""


class illarg_right_cast_legal(TstView):
    def __init__(self):
        # checked manually
        self.data = [{"tmestmp": "44", "uuidd": "4c"}]
        self.sql = """CREATE MATERIALIZED VIEW right_cast_legal AS SELECT
                      RIGHT(tmestmp, 2) AS tmestmp,
                      RIGHT(uuidd, 2) AS uuidd
                      FROM illegal_tbl
                      WHERE id = 0"""


# Negative Test
class illarg_right_illegal(TstView):
    def __init__(self):
        # checked manually
        self.sql = """CREATE MATERIALIZED VIEW right_illegal AS SELECT
                      RIGHT(roww, 2) AS roww
                      FROM illegal_tbl"""
        self.expected_error = "Cannot apply 'RIGHT' to arguments of type"


# INITCAP function(successful for all arguments except Complex Types)
class illarg_initcap_legal(TstView):
    def __init__(self):
        # checked manually
        self.data = [{"str": "Hello "}]
        self.sql = """CREATE MATERIALIZED VIEW initcap_legal AS SELECT
                      INITCAP(str) AS str
                      FROM illegal_tbl
                      WHERE id = 0"""


class illarg_initcap_cast_legal(TstView):
    def __init__(self):
        # checked manually
        self.data = [{"booll": "True", "arr": "Bye"}]
        self.sql = """CREATE MATERIALIZED VIEW initcap_cast_legal AS SELECT
                      INITCAP(booll) AS booll,
                      INITCAP(arr[1]) AS arr
                      FROM illegal_tbl
                      WHERE id = 0"""


# Negative Test
class illarg_initcap_illegal(TstView):
    def __init__(self):
        # checked manually
        self.sql = """CREATE MATERIALIZED VIEW initcap_illegal AS SELECT
                      INITCAP(roww) AS roww
                      FROM illegal_tbl"""
        self.expected_error = "Cannot apply 'INITCAP' to arguments of type"


# CHAR_LENGTH(string) or CHARACTER_LENGTH(string) or LENGTH(string) or LEN(string) function(successful for all arguments except Complex Types)
class illarg_len_legal(TstView):
    def __init__(self):
        # checked manually
        self.data = [{"str": 6}]
        self.sql = """CREATE MATERIALIZED VIEW len_legal AS SELECT
                      LEN(str) AS str
                      FROM illegal_tbl
                      WHERE id = 0"""


class illarg_len_cast_legal(TstView):
    def __init__(self):
        # checked manually
        self.data = [{"intt": 3, "uuidd": 36}]
        self.sql = """CREATE MATERIALIZED VIEW len_cast_legal AS SELECT
                      LEN(intt) AS intt,
                      LEN(uuidd) AS uuidd
                      FROM illegal_tbl
                      WHERE id = 0"""


# Negative Test
class illarg_len_illegal(TstView):
    def __init__(self):
        # checked manually
        self.sql = """CREATE MATERIALIZED VIEW len_illegal AS SELECT
                      LEN(roww) AS roww
                      FROM illegal_tbl"""
        self.expected_error = "Cannot apply 'LEN' to arguments of type"


# LOWER function(successful for all arguments)
class illarg_lower_legal(TstView):
    def __init__(self):
        # checked manually
        self.data = [{"str": "hello "}]
        self.sql = """CREATE MATERIALIZED VIEW lower_legal AS SELECT
                      LOWER(str) AS str
                      FROM illegal_tbl
                      WHERE id = 0"""


class illarg_lower_cast_legal(TstView):
    def __init__(self):
        # checked manually
        self.data = [{"booll": "true", "arr": "see you!"}]
        self.sql = """CREATE MATERIALIZED VIEW lower_cast_legal AS SELECT
                      LOWER(booll) AS booll,
                      LOWER(arr[3]) AS arr
                      FROM illegal_tbl
                      WHERE id = 0"""


# Negative Test
class illarg_lower_illegal(TstView):
    def __init__(self):
        # checked manually
        self.sql = """CREATE MATERIALIZED VIEW lower_illegal AS SELECT
                      LOWER(roww) AS roww
                      FROM illegal_tbl
                      WHERE id = 0"""
        self.expected_error = "Cannot apply 'LOWER' to arguments of type"


# UPPER function(successful for all arguments)
class illarg_upper_legal(TstView):
    def __init__(self):
        # checked manually
        self.data = [{"str": "HELLO "}]
        self.sql = """CREATE MATERIALIZED VIEW upper_legal AS SELECT
                      UPPER(str) AS str
                      FROM illegal_tbl
                      WHERE id = 0"""


class illarg_upper_cast_legal(TstView):
    def __init__(self):
        # checked manually
        self.data = [{"uuidd": "42B8FEC7-C7A3-4531-9611-4BDE80F9CB4C", "arr": "BYE"}]
        self.sql = """CREATE MATERIALIZED VIEW upper_cast_legal AS SELECT
                      UPPER(uuidd) AS uuidd,
                      UPPER(arr[1]) AS arr
                      FROM illegal_tbl
                      WHERE id = 0"""


# Negative Test
class illarg_upper_illegal(TstView):
    def __init__(self):
        # checked manually
        self.sql = """CREATE MATERIALIZED VIEW upper_illegal AS SELECT
                      UPPER(roww) AS roww
                      FROM illegal_tbl
                      WHERE id = 0"""
        self.expected_error = "Cannot apply 'UPPER' to arguments of type"


#  SUBSTR function(successful for all arguments)
class illarg_substr_legal(TstView):
    def __init__(self):
        # checked manually
        self.data = [{"str": "ell"}]
        self.sql = """CREATE MATERIALIZED VIEW substr_legal AS SELECT
                      SUBSTR(str, 2, 3) AS str
                      FROM illegal_tbl
                      WHERE id = 0"""


class illarg_substr_cast_legal(TstView):
    def __init__(self):
        # checked manually
        self.data = [{"bin": "1620", "uuidd": "2b8"}]
        self.sql = """CREATE MATERIALIZED VIEW substr_cast_legal AS SELECT
                      SUBSTR(bin, 2, 3) AS bin,
                      SUBSTR(uuidd, 2, 3) AS uuidd
                      FROM illegal_tbl
                      WHERE id = 0"""


# Negative Test
class illarg_substr_illegal(TstView):
    def __init__(self):
        # checked manually
        self.sql = """CREATE MATERIALIZED VIEW substr_illegal AS SELECT
                      SUBSTR(roww, 2, 3) AS roww
                      FROM illegal_tbl"""
        self.expected_error = "Cannot apply 'SUBSTR' to arguments of type"


# SUBSTRING function(successful for all arguments)
class illarg_substring_legal(TstView):
    def __init__(self):
        # checked manually
        self.data = [{"str": "ell"}]
        self.sql = """CREATE MATERIALIZED VIEW substring_legal AS SELECT
                      SUBSTRING(str from 2 for 3) AS str
                      FROM illegal_tbl
                      WHERE id = 0"""


class illarg_substring_cast_legal(TstView):
    def __init__(self):
        # checked manually
        self.data = [{"tmestmp": "020", "bin": "1620"}]
        self.sql = """CREATE MATERIALIZED VIEW substring_cast_legal AS SELECT
                      SUBSTRING(tmestmp from 2 for 3) AS tmestmp,
                      SUBSTRING(bin from 2 for 3) AS bin
                      FROM illegal_tbl
                      WHERE id = 0"""


# Negative Test
class illarg_substring_illegal(TstView):
    def __init__(self):
        # checked manually
        self.sql = """CREATE MATERIALIZED VIEW substring_illegal AS SELECT
                      SUBSTRING(roww, 2, 3) AS roww
                      FROM illegal_tbl"""
        self.expected_error = "Cannot apply 'SUBSTRING' to arguments of type"


# TRIM function
class illarg_trim_legal(TstView):
    def __init__(self):
        # checked manually
        self.data = [{"str": "hello"}]
        self.sql = """CREATE MATERIALIZED VIEW trim_legal AS SELECT
                      TRIM(trailing ' ' from str) AS str
                      FROM illegal_tbl
                      WHERE id = 0"""


class illarg_trim_cast_legal(TstView):
    def __init__(self):
        # checked manually
        self.data = [{"intt": "-1", "booll": "RUE"}]
        self.sql = """CREATE MATERIALIZED VIEW trim_cast_legal AS SELECT
                      TRIM(trailing '2' from intt) AS intt,
                      TRIM(leading 'T' from booll) AS booll
                      FROM illegal_tbl
                      WHERE id = 0"""


# Negative Test
class illarg_trim_illegal(TstView):
    def __init__(self):
        # checked manually
        self.sql = """CREATE MATERIALIZED VIEW trim_illegal AS SELECT
                      TRIM(leading '0b' from bin) AS bin
                      FROM illegal_tbl
                      WHERE id = 0"""
        self.expected_error = "Cannot apply 'TRIM' to arguments of type"


# POSITION function(successful for all arguments except Complex Types)
class illarg_position_legal(TstView):
    def __init__(self):
        # checked manually
        self.data = [{"str": 2}]
        self.sql = """CREATE MATERIALIZED VIEW position_legal AS SELECT
                      POSITION('ell' in str) AS str
                      FROM illegal_tbl
                      WHERE id = 0"""


class illarg_position_cast_legal(TstView):
    def __init__(self):
        # checked manually
        self.data = [{"reall": 2, "bin": 2}]
        self.sql = """CREATE MATERIALIZED VIEW position_cast_legal AS SELECT
                      POSITION('576' in reall) AS reall,
                      POSITION(x'16' in bin) AS bin
                      FROM illegal_tbl
                      WHERE id = 0"""


# Negative Test
class illarg_position_illegal(TstView):
    def __init__(self):
        # checked manually
        self.sql = """CREATE MATERIALIZED VIEW position_illegal AS SELECT
                      POSITION('cat' in roww) AS roww
                      FROM illegal_tbl"""
        self.expected_error = "Parameters must be of the same type"


# REGEXP_REPLACE function(successful for all arguments except Complex Types)
class illarg_regexp_replace_legal(TstView):
    def __init__(self):
        # checked manually
        self.data = [{"str": "hiiii "}]
        self.sql = """CREATE MATERIALIZED VIEW regexp_replace_legal AS SELECT
                      REGEXP_REPLACE(str, '[a-gi-z]', 'i') AS str
                      FROM illegal_tbl
                      WHERE id = 0"""


class illarg_regexp_replace_cast_legal(TstView):
    def __init__(self):
        # checked manually
        self.data = [{"decimall": "-i.i", "uuidd": "ibifeci-ciai-i-i-ibdeificbic"}]
        self.sql = """CREATE MATERIALIZED VIEW regexp_replace_cast_legal AS SELECT
                      REGEXP_REPLACE(decimall, '[0-9]+', 'i') AS decimall,
                      REGEXP_REPLACE(uuidd, '[0-9]+', 'i') AS uuidd
                      FROM illegal_tbl
                      WHERE id = 0"""


# Negative Test
class illarg_regexp_replace_illegal(TstView):
    def __init__(self):
        # checked manually
        self.sql = """CREATE MATERIALIZED VIEW regexp_replace_illegal AS SELECT
                      REGEXP_REPLACE(roww, '[a-gi-z]', 'i') AS roww
                      FROM illegal_tbl"""
        self.expected_error = "Cannot apply 'REGEXP_REPLACE' to arguments of type"


# RLIKE function(successful for all arguments except Complex Types)
class illarg_rlike_legal(TstView):
    def __init__(self):
        # checked manually
        self.data = [{"str": True}]
        self.sql = """CREATE MATERIALIZED VIEW rlike_legal AS SELECT
                      RLIKE(str, 'h..ll*') AS str
                      FROM illegal_tbl
                      WHERE id = 0"""


class illarg_rlike_cast_legal(TstView):
    def __init__(self):
        # checked manually
        self.data = [{"booll": False, "tmestmp": True}]
        self.sql = """CREATE MATERIALIZED VIEW rlike_cast_legal AS SELECT
                      RLIKE(booll, '[0-9]+') AS booll,
                      RLIKE(tmestmp, '[0-9]+') AS tmestmp
                      FROM illegal_tbl
                      WHERE id = 0"""


# Negative Test
class illarg_rlike_illegal(TstView):
    def __init__(self):
        # checked manually
        self.sql = """CREATE MATERIALIZED VIEW rlike_illegal AS SELECT
                      RLIKE(roww, 'c.t') AS roww
                      FROM illegal_tbl"""
        self.expected_error = "Cannot apply 'RLIKE' to arguments of type"


# SPLIT function(successful for all arguments except Complex Types)
class illarg_split_legal(TstView):
    def __init__(self):
        # checked manually
        self.data = [{"str": ["h", "llo "]}]
        self.sql = """CREATE MATERIALIZED VIEW split_legal AS SELECT
                      SPLIT(str, 'e') AS str
                      FROM illegal_tbl
                      WHERE id = 0"""


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
                      FROM illegal_tbl
                      WHERE id = 0"""


# Negative Test
class illarg_split_illegal(TstView):
    def __init__(self):
        # checked manually
        self.data = [{"roww": ["h", "llo "]}]
        self.sql = """CREATE MATERIALIZED VIEW split_illegal AS SELECT
                      SPLIT(roww, 'a') AS roww
                      FROM illegal_tbl"""
        self.expected_error = "Cannot apply 'SPLIT' to arguments of type"


# SPLIT_PART function(successful for all arguments except Complex Types)
class illarg_split_part_legal(TstView):
    def __init__(self):
        # checked manually
        self.data = [{"str": "llo "}]
        self.sql = """CREATE MATERIALIZED VIEW split_part_legal AS SELECT
                      SPLIT_PART(str, 'e', 2) AS str
                      FROM illegal_tbl
                      WHERE id = 0"""


class illarg_split_part_cast_legal(TstView):
    def __init__(self):
        # checked manually
        self.data = [{"intt": "-12", "uuidd": "c7a3"}]
        self.sql = """CREATE MATERIALIZED VIEW split_part_cast_legal AS SELECT
                      SPLIT_PART(intt, '', 1) AS intt,
                      SPLIT_PART(uuidd, '-', 2) AS uuidd
                      FROM illegal_tbl
                      WHERE id = 0"""


# Negative Test
class illarg_split_part_illegal(TstView):
    def __init__(self):
        # checked manually
        self.sql = """CREATE MATERIALIZED VIEW split_part_illegal AS SELECT
                      SPLIT_PART(roww, 'a', 2) AS roww
                      FROM illegal_tbl
                      WHERE id = 0"""
        self.expected_error = "Cannot apply 'SPLIT_PART' to arguments of type"


# MD5 function(successful for all arguments except Complex Types)
class illarg_md5_legal(TstView):
    def __init__(self):
        # Validated on Postgres
        self.data = [
            {
                "str": "f814893777bcc2295fff05f00e508da6",
                "bin": "e0309b5efe8eb13b4cb57f371b254591",
            }
        ]
        self.sql = """CREATE MATERIALIZED VIEW md5_legal AS SELECT
                      MD5(str) AS str,
                      MD5(bin) AS bin
                      FROM illegal_tbl
                      WHERE id = 0"""


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
                      FROM illegal_tbl
                      WHERE id = 0"""


# Negative Test
class illarg_md5_illegal(TstView):
    def __init__(self):
        # checked manually
        self.sql = """CREATE MATERIALIZED VIEW md5_illegal AS SELECT
                      MD5(roww) AS roww
                      FROM illegal_tbl
                      WHERE id = 0"""
        self.expected_error = "Cannot apply 'MD5' to arguments of type"


# BINARY specific functions
# ||(concatenation operator) => (successful for all arguments except Complex Types)
class illarg_bin_concat_legal(TstView):
    def __init__(self):
        # checked manually
        self.data = [{"bin": "0b16200b1620", "str": "hello hello "}]
        self.sql = """CREATE MATERIALIZED VIEW bin_concat_legal AS SELECT
                      bin || bin AS bin,
                      str || str AS str
                      FROM illegal_tbl
                      WHERE id = 0"""


class illarg_bin_concat_cast_legal(TstView):
    def __init__(self):
        # checked manually
        self.data = [{"intt": "-12-12"}]
        self.sql = """CREATE MATERIALIZED VIEW bin_concat_cast_legal AS SELECT
                      intt || intt AS intt
                      FROM illegal_tbl
                      WHERE id = 0"""


# Negative Test
class illarg_bin_concat_illegal(TstView):
    def __init__(self):
        # checked manually
        self.sql = """CREATE MATERIALIZED VIEW bin_concat_illegal AS SELECT
                      roww || roww AS roww
                      FROM illegal_tbl
                      WHERE id = 0"""
        self.expected_error = "Cannot apply '||' to arguments of type"


# GUNZIP(successful only for BINARY type in GZIP format)
class illarg_bin_gunzip_legal(TstView):
    def __init__(self):
        # checked manually
        self.data = [{"bin": "feldera"}]
        self.sql = """CREATE MATERIALIZED VIEW bin_gunzip_legal AS SELECT
                      GUNZIP(bin) AS bin
                      FROM illegal_tbl
                      WHERE id = 1"""


# OCTET_LENGTH
class illarg_bin_octet_length_legal(TstView):
    def __init__(self):
        # checked manually
        self.data = [{"bin": 3}]
        self.sql = """CREATE MATERIALIZED VIEW bin_octet_length_legal AS SELECT
                      OCTET_LENGTH(bin) AS bin
                      FROM illegal_tbl
                      WHERE id = 0"""


class illarg_bin_octet_length_cast_legal(TstView):
    def __init__(self):
        # checked manually
        self.data = [{"str": 6, "arr": 3}]
        self.sql = """CREATE MATERIALIZED VIEW bin_octet_length_cast_legal AS SELECT
                      OCTET_LENGTH(str) AS str,
                      OCTET_LENGTH(arr[1]) AS arr
                      FROM illegal_tbl
                      WHERE id = 0"""


# Negative Test
class illarg_octet_length_illegal(TstView):
    def __init__(self):
        # checked manually
        self.sql = """CREATE MATERIALIZED VIEW octet_length_illegal AS SELECT
                      OCTET_LENGTH(reall) AS reall
                      FROM illegal_tbl
                      WHERE id = 0"""
        self.expected_error = "Cannot apply 'OCTET_LENGTH' to arguments of type"


# OVERLAY(successful for all arguments except Complex Types)
class illarg_bin_overlay_legal(TstView):
    def __init__(self):
        # checked manually
        self.data = [{"bin": "0b0203"}]
        self.sql = """CREATE MATERIALIZED VIEW bin_overlay_legal AS SELECT
                      OVERLAY(bin placing x'0203' from 2 for 3) AS bin
                      FROM illegal_tbl
                      WHERE id = 0"""


class illarg_bin_overlay_cast_legal(TstView):
    def __init__(self):
        # checked manually
        self.data = [{"str": "hbyeo ", "arr": "bhello"}]
        self.sql = """CREATE MATERIALIZED VIEW bin_overlay_cast_legal AS SELECT
                      OVERLAY(str placing 'bye' from 2 for 3) AS str,
                      OVERLAY(arr[1] placing 'hello' from 2 for 3) AS arr
                      FROM illegal_tbl
                      WHERE id = 0"""


# Negative Test
class illarg_bin_overlay_illegal(TstView):
    def __init__(self):
        # checked manually
        self.sql = """CREATE MATERIALIZED VIEW bin_overlay_illegal AS SELECT
                      OVERLAY(roww placing 'a' from 2 for 3) AS roww
                      FROM illegal_tbl
                      WHERE id = 0"""
        self.expected_error = "Cannot apply 'OVERLAY' to arguments of type"


# POSITION(succesful for all arguments except Complex Types)
class illarg_bin_position_legal(TstView):
    def __init__(self):
        # checked manually
        self.data = [{"bin": 1}]
        self.sql = """CREATE MATERIALIZED VIEW bin_position_legal AS SELECT
                      POSITION(x'0b' in bin) AS bin
                      FROM illegal_tbl
                      WHERE id = 0"""


class illarg_bin_position_cast_legal(TstView):
    def __init__(self):
        # checked manually
        self.data = [{"str": 2, "arr": 1}]
        self.sql = """CREATE MATERIALIZED VIEW bin_position_cast_legal AS SELECT
                      POSITION('e' in str) AS str,
                      POSITION('b' in arr[1]) AS arr
                      FROM illegal_tbl
                      WHERE id = 0"""


# SUBSTRING(binary FROM integer)(successful for all arguments except Complex Types)
class illarg_bin_substring_legal(TstView):
    def __init__(self):
        # checked manually
        self.data = [{"bin": "20"}]
        self.sql = """CREATE MATERIALIZED VIEW bin_substring_legal AS SELECT
                      SUBSTRING(bin, 3) AS bin
                      FROM illegal_tbl
                      WHERE id = 0"""


class illarg_bin_substring_cast_legal(TstView):
    def __init__(self):
        # checked manually
        self.data = [{"str": "llo ", "arr": "e"}]
        self.sql = """CREATE MATERIALIZED VIEW bin_substring_cast_legal AS SELECT
                      SUBSTRING(str, 3) AS str,
                      SUBSTRING(arr[1], 3) AS arr
                      FROM illegal_tbl
                      WHERE id = 0"""


# SUBSTRING(binary FROM integer1 FOR integer2)(successful for all arguments except Complex Types)
class illarg_bin_substring1_legal(TstView):
    def __init__(self):
        # checked manually
        self.data = [{"bin": "0b16"}]
        self.sql = """CREATE MATERIALIZED VIEW bin_substring1_legal AS SELECT
                      SUBSTRING(bin FROM 1 FOR 2) AS bin
                      FROM illegal_tbl
                      WHERE id = 0"""


class illarg_bin_substring1_cast_legal(TstView):
    def __init__(self):
        # checked manually
        self.data = [{"str": "ll", "arr": "e"}]
        self.sql = """CREATE MATERIALIZED VIEW bin_substring1_cast_legal AS SELECT
                      SUBSTRING(str FROM 3 FOR 2) AS str,
                      SUBSTRING(arr[1] FROM 3 FOR 2) AS arr
                      FROM illegal_tbl
                      WHERE id = 0"""


# TO_HEX
class illarg_bin_to_hex_legal(TstView):
    def __init__(self):
        # Validated on Postgres
        self.data = [{"bin": "0b1620"}]
        self.sql = """CREATE MATERIALIZED VIEW bin_to_hex_legal AS SELECT
                      TO_HEX(bin) AS bin
                      FROM illegal_tbl
                      WHERE id = 0"""


class illarg_bin_to_hex_cast_legal(TstView):
    def __init__(self):
        # checked manually
        self.data = [{"str": "68656c6c6f20", "arr": "627965"}]
        self.sql = """CREATE MATERIALIZED VIEW bin_to_hex_cast_legal AS SELECT
                      TO_HEX(str) AS str,
                      TO_HEX(arr[1]) AS arr
                      FROM illegal_tbl
                      WHERE id = 0"""


# SELECT
# ENCODE('\x0b1620'::BYTEA, 'hex'),
# ENCODE('bye'::BYTEA, 'hex'),
# ENCODE('hello '::BYTEA, 'hex');


# Negative Test(ignore)
class illarg_bin_to_hex_illegal(TstView):
    def __init__(self):
        # checked manually
        self.sql = """CREATE MATERIALIZED VIEW bin_to_hex_illegal AS SELECT
                      TO_HEX(uuidd) AS uuidd
                      FROM illegal_tbl
                      WHERE id = 0"""
        self.expected_error = "Cannot apply 'TO_HEX' to arguments of type"


# TO_INT
class illarg_bin_to_int_legal(TstView):
    def __init__(self):
        # Validated on Postgres
        self.data = [{"bin": 726560}]
        self.sql = """CREATE MATERIALIZED VIEW bin_to_int_legal AS SELECT
                      TO_INT(bin) AS bin
                      FROM illegal_tbl
                      WHERE id = 0"""


class illarg_bin_to_int_cast_legal(TstView):
    def __init__(self):
        # Validated on Postgres
        self.data = [{"str": 1751477356, "arr": 6453605}]
        self.sql = """CREATE MATERIALIZED VIEW bin_to_int_cast_legal AS SELECT
                      TO_INT(str) AS str,
                      TO_INT(arr[1]) AS arr
                      FROM illegal_tbl
                      WHERE id = 0"""


# SELECT
# ('x' || '0b1620')::bit(24)::int,
# ('x' || '627965')::bit(24)::int,
# ('x' || '68656c6c6f20')::bit(32)::int;


# Negative Test
class illarg_bin_to_int_illegal(TstView):
    def __init__(self):
        # checked manually
        self.sql = """CREATE MATERIALIZED VIEW bin_to_int_illegal AS SELECT
                      TO_INT(roww) AS roww
                      FROM illegal_tbl
                      WHERE id = 0"""
        self.expected_error = "Cannot apply 'TO_INT' to arguments of type"
