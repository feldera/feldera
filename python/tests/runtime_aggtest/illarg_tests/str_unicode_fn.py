from tests.runtime_aggtest.aggtst_base import TstView, TstTable


class illarg_str_tbl(TstTable):
    """Define the table used by string function tests"""

    def __init__(self):
        self.sql = """CREATE TABLE str_tbl(
                      id INT,
                      str VARCHAR
                      )"""
        self.data = [
            {
                "id": 0,
                "str": "🐍🐍",
            },
            {
                "id": 1,
                "str": "かわいい",
            },
            {
                "id": 2,
                "str": "¯\\_(ツ)_/¯",
            },
            {
                "id": 3,
                "str": "h@pP√ ",
            },
            {
                "id": 4,
                "str": None,
            },
        ]


# ASCII function
class illarg_ascii_fn(TstView):
    def __init__(self):
        # Validated on Postgres
        self.data = [
            {"id": 0, "str": 128013},
            {"id": 1, "str": 12363},
            {"id": 2, "str": 175},
            {"id": 3, "str": 104},
            {"id": 4, "str": None},
        ]
        self.sql = """CREATE MATERIALIZED VIEW ascii_fn AS SELECT
                      id,
                      ASCII(str) AS str
                      FROM str_tbl"""


# CONCAT function
class illarg_concat_fn(TstView):
    def __init__(self):
        # Validated on Postgres
        self.data = [
            {"id": 0, "str": "🐍🐍🐍🐍"},
            {"id": 1, "str": "かわいいかわいい"},
            {"id": 2, "str": "¯\\_(ツ)_/¯¯\\_(ツ)_/¯"},
            {"id": 3, "str": "h@pP√ h@pP√ "},
            {"id": 4, "str": None},
        ]
        self.sql = """CREATE MATERIALIZED VIEW concat_fn AS SELECT
                      id,
                      CONCAT(str, str) AS str
                      FROM str_tbl"""


# CONCAT_WS function
class illarg_concatws_fn(TstView):
    def __init__(self):
        # Validated on Postgres
        self.data = [
            {"id": 0, "str": "🐍🐍@55"},
            {"id": 1, "str": "かわいい@55"},
            {"id": 2, "str": "¯\\_(ツ)_/¯@55"},
            {"id": 3, "str": "h@pP√ @55"},
            {"id": 4, "str": "@55"},
        ]
        self.sql = """CREATE MATERIALIZED VIEW concatws_fn AS SELECT
                      id,
                      CONCAT_WS('@', str, 55) AS str
                      FROM str_tbl"""


# LEFT function
class illarg_left_fn(TstView):
    def __init__(self):
        # Validated on Postgres
        self.data = [
            {"id": 0, "str": "🐍🐍"},
            {"id": 1, "str": "かわ"},
            {"id": 2, "str": "¯\\"},
            {"id": 3, "str": "h@"},
            {"id": 4, "str": None},
        ]
        self.sql = """CREATE MATERIALIZED VIEW left_fn AS SELECT
                      id,
                      LEFT(str, 2) AS str
                      FROM str_tbl"""


# RIGHT function
class illarg_right_fn(TstView):
    def __init__(self):
        # Validated on Postgres
        self.data = [
            {"id": 0, "str": "🐍🐍"},
            {"id": 1, "str": "いい"},
            {"id": 2, "str": "/¯"},
            {"id": 3, "str": "√ "},
            {"id": 4, "str": None},
        ]
        self.sql = """CREATE MATERIALIZED VIEW right_fn AS SELECT
                      id,
                      RIGHT(str, 2) AS str
                      FROM str_tbl"""


# INITCAP function
class illarg_initcap_fn(TstView):
    def __init__(self):
        # Validated on Postgres
        self.data = [
            {"id": 0, "str": "🐍🐍"},
            {"id": 1, "str": "かわいい"},
            {"id": 2, "str": "¯\\_(ツ)_/¯"},
            {"id": 3, "str": "H@Pp√ "},
            {"id": 4, "str": None},
        ]
        self.sql = """CREATE MATERIALIZED VIEW initcap_fn AS SELECT
                      id,
                      INITCAP(str) AS str
                      FROM str_tbl"""


# CHAR_LENGTH(string) or CHARACTER_LENGTH(string) or LENGTH(string) or LEN(string) function
class illarg_len_fn(TstView):
    def __init__(self):
        # Validated on Postgres
        self.data = [
            {"id": 0, "str": 2},
            {"id": 1, "str": 4},
            {"id": 2, "str": 9},
            {"id": 3, "str": 6},
            {"id": 4, "str": None},
        ]
        self.sql = """CREATE MATERIALIZED VIEW len_fn AS SELECT
                      id,
                      LEN(str) AS str
                      FROM str_tbl"""


# LOWER function
class illarg_lower_fn(TstView):
    def __init__(self):
        # Validated on Postgres
        self.data = [
            {"id": 0, "str": "🐍🐍"},
            {"id": 1, "str": "かわいい"},
            {"id": 2, "str": "¯\\_(ツ)_/¯"},
            {"id": 3, "str": "h@pp√ "},
            {"id": 4, "str": None},
        ]
        self.sql = """CREATE MATERIALIZED VIEW lower_fn AS SELECT
                      id,
                      LOWER(str) AS str
                      FROM str_tbl"""


# UPPER function
class illarg_upper_fn(TstView):
    def __init__(self):
        # Validated on Postgres
        self.data = [
            {"id": 0, "str": "🐍🐍"},
            {"id": 1, "str": "かわいい"},
            {"id": 2, "str": "¯\\_(ツ)_/¯"},
            {"id": 3, "str": "H@PP√ "},
            {"id": 4, "str": None},
        ]
        self.sql = """CREATE MATERIALIZED VIEW upper_fn AS SELECT
                      id,
                      UPPER(str) AS str
                      FROM str_tbl"""


#  SUBSTR function
class illarg_substr_fn(TstView):
    def __init__(self):
        # Validated on Postgres
        self.data = [
            {"id": 0, "str": "🐍"},
            {"id": 1, "str": "わいい"},
            {"id": 2, "str": "\\_("},
            {"id": 3, "str": "@pP"},
            {"id": 4, "str": None},
        ]
        self.sql = """CREATE MATERIALIZED VIEW substr_fn AS SELECT
                      id,
                      SUBSTR(str, 2, 3) AS str
                      FROM str_tbl"""


# SUBSTRING function
class illarg_substring_fn(TstView):
    def __init__(self):
        # Validated on Postgres
        self.data = [
            {"id": 0, "str": "🐍"},
            {"id": 1, "str": "わいい"},
            {"id": 2, "str": "\\_("},
            {"id": 3, "str": "@pP"},
            {"id": 4, "str": None},
        ]
        self.sql = """CREATE MATERIALIZED VIEW substring_fn AS SELECT
                      id,
                      SUBSTRING(str from 2 for 3) AS str
                      FROM str_tbl"""


# TRIM function
class illarg_trim_fn(TstView):
    def __init__(self):
        # Validated on Postgres
        self.data = [
            {"id": 0, "str": ""},
            {"id": 1, "str": "かわ"},
            {"id": 2, "str": "¯\\_(ツ)_"},
            {"id": 3, "str": "h@pP"},
            {"id": 4, "str": None},
        ]
        self.sql = """CREATE MATERIALIZED VIEW trim_fn AS SELECT
                      id,
                      CASE
                        WHEN id = 0 THEN TRIM(trailing '🐍' from str)
                        WHEN id = 1 THEN TRIM(trailing 'い' from str)
                        WHEN id = 2 THEN TRIM(trailing '/¯' from str)
                        WHEN id = 3 THEN TRIM(trailing '√ ' from str)
                      END AS str
                      FROM str_tbl"""


# POSITION function
class illarg_position_fn(TstView):
    def __init__(self):
        # Validated on Postgres
        self.data = [
            {"id": 0, "str": 1},
            {"id": 1, "str": 3},
            {"id": 2, "str": 8},
            {"id": 3, "str": 5},
            {"id": 4, "str": None},
        ]
        self.sql = """CREATE MATERIALIZED VIEW position_fn AS SELECT
                      id,
                      CASE
                        WHEN id = 0 THEN POSITION('🐍' in str)
                        WHEN id = 1 THEN POSITION('い' in str)
                        WHEN id = 2 THEN POSITION('/¯' in str)
                        WHEN id = 3 THEN POSITION('√ ' in str)
                      END AS str
                      FROM str_tbl"""


# REGEXP_REPLACE function
class illarg_regexp_replace_fn(TstView):
    def __init__(self):
        # Validated on Postgres
        self.data = [
            {"id": 0, "str": "i"},
            {"id": 1, "str": "かわi"},
            {"id": 2, "str": "¯\\_(ツ)_i"},
            {"id": 3, "str": "h@pPi"},
            {"id": 4, "str": None},
        ]
        self.sql = """CREATE MATERIALIZED VIEW regexp_replace_fn AS SELECT
                      id,
                      REGEXP_REPLACE(str, '([🐍い]|/¯|√\\s*)+$', 'i') AS str
                      FROM str_tbl"""


# RLIKE function
class illarg_rlike_fn(TstView):
    def __init__(self):
        # checked manually
        self.data = [
            {"id": 0, "str": True},
            {"id": 1, "str": True},
            {"id": 2, "str": True},
            {"id": 3, "str": True},
            {"id": 4, "str": None},
        ]
        self.sql = """CREATE MATERIALIZED VIEW rlike_fn AS SELECT
                      id,
                      CASE
                        WHEN id = 0 THEN RLIKE(str, '🐍.')
                        WHEN id = 1 THEN RLIKE(str, '..い.')
                        WHEN id = 2 THEN RLIKE(str, '...(ツ)...')
                        WHEN id = 3 THEN RLIKE(str, '....√ ')
                      END AS str
                      FROM str_tbl"""


# SPLIT function
class illarg_split_fn(TstView):
    def __init__(self):
        # Validated on Postgres(string_to_array function)
        self.data = [
            {"id": 0, "str": ["", "", ""]},
            {"id": 1, "str": ["か", "いい"]},
            {"id": 2, "str": ["¯\\_", "_/¯"]},
            {"id": 3, "str": ["h", "pP√ "]},
            {"id": 4, "str": None},
        ]
        self.sql = """CREATE MATERIALIZED VIEW split_fn AS SELECT
                      id,
                      CASE
                        WHEN id = 0 THEN SPLIT(str, '🐍')
                        WHEN id = 1 THEN SPLIT(str, 'わ')
                        WHEN id = 2 THEN SPLIT(str, '(ツ)')
                        WHEN id = 3 THEN SPLIT(str, '@')
                      END AS str
                      FROM str_tbl"""


# SPLIT_PART function
class illarg_split_part_fn(TstView):
    def __init__(self):
        # Validated on Postgres
        self.data = [
            {"id": 0, "str": ""},
            {"id": 1, "str": "いい"},
            {"id": 2, "str": "_/¯"},
            {"id": 3, "str": "pP√ "},
            {"id": 4, "str": None},
        ]
        self.sql = """CREATE MATERIALIZED VIEW split_part_fn AS SELECT
                      id,
                      CASE
                        WHEN id = 0 THEN SPLIT_PART(str, '🐍', 2)
                        WHEN id = 1 THEN SPLIT_PART(str, 'わ', 2)
                        WHEN id = 2 THEN SPLIT_PART(str, '(ツ)', 2)
                        WHEN id = 3 THEN SPLIT_PART(str, '@', 2)
                      END AS str
                      FROM str_tbl"""


# MD5 function
class illarg_md5_fn(TstView):
    def __init__(self):
        # Validated on Postgres
        self.data = [
            {"id": 0, "str": "45668611fd525b5d8c5914e12ebaf017"},
            {"id": 1, "str": "11dfd6f514117bf9cc0d0b41203dd557"},
            {"id": 2, "str": "ab78c3aaa3a3916354e740a9bd7e5df1"},
            {"id": 3, "str": "a7c561a0252cf2e8daba9229e78c1a8a"},
            {"id": 4, "str": None},
        ]
        self.sql = """CREATE MATERIALIZED VIEW md5_fn AS SELECT
                      id,
                      MD5(str) AS str
                      FROM str_tbl"""
