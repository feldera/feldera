from tests.runtime_aggtest.aggtst_base import TstView


# Equality
class un_int_equality(TstView):
    def __init__(self):
        self.data = [
            {
                "id": 0,
                "tiny_un_int": True,
                "small_un_int": True,
                "un_intt": True,
                "big_un_int": True,
            }
        ]
        self.sql = """CREATE MATERIALIZED VIEW equality_un_int AS SELECT
                      id,
                      tiny_int = 9 AS tiny_un_int,
                      small_int = 10192 AS small_un_int,
                      intt = 1146171806 AS un_intt,
                      big_int = 6996571239486628517 AS big_un_int
                      FROM un_int_datagen_tbl"""


# Inequality
class un_int_inequality(TstView):
    def __init__(self):
        self.data = [
            {
                "id": 0,
                "tiny_un_int": False,
                "small_un_int": False,
                "un_intt": False,
                "big_un_int": False,
            }
        ]
        self.sql = """CREATE MATERIALIZED VIEW inequality_un_int AS SELECT
                      id,
                      tiny_int != 9 AS tiny_un_int,
                      small_int != 10192 AS small_un_int,
                      intt != 1146171806 AS un_intt,
                      big_int != 6996571239486628517 AS big_un_int
                      FROM un_int_datagen_tbl"""


# Greater Than
class un_int_greater_than(TstView):
    def __init__(self):
        self.data = [
            {
                "id": 0,
                "tiny_un_int": False,
                "small_un_int": False,
                "un_intt": False,
                "big_un_int": False,
            }
        ]
        self.sql = """CREATE MATERIALIZED VIEW greater_than_un_int AS SELECT
                      id,
                      tiny_int > 9 AS tiny_un_int,
                      small_int > 10192 AS small_un_int,
                      intt > 1146171806 AS un_intt,
                      big_int > 6996571239486628517 AS big_un_int
                      FROM un_int_datagen_tbl"""


# Less Than
class un_int_less_than(TstView):
    def __init__(self):
        self.data = [
            {
                "id": 0,
                "tiny_un_int": False,
                "small_un_int": False,
                "un_intt": False,
                "big_un_int": False,
            }
        ]
        self.sql = """CREATE MATERIALIZED VIEW less_than_un_int AS SELECT
                      id,
                      tiny_int < 9 AS tiny_un_int,
                      small_int < 10192 AS small_un_int,
                      intt < 1146171806 AS un_intt,
                      big_int < 6996571239486628517 AS big_un_int
                      FROM un_int_datagen_tbl"""


# Less or Equal
class un_int_less_or_equal(TstView):
    def __init__(self):
        self.data = [
            {
                "id": 0,
                "tiny_un_int": True,
                "small_un_int": True,
                "un_intt": True,
                "big_un_int": True,
            }
        ]
        self.sql = """CREATE MATERIALIZED VIEW less_or_equal_un_int AS SELECT
                      id,
                      tiny_int <= 9 AS tiny_un_int,
                      small_int <= 10192 AS small_un_int,
                      intt <= 1146171806 AS un_intt,
                      big_int <= 6996571239486628517 AS big_un_int
                      FROM un_int_datagen_tbl"""


# Greater or Equal
class un_int_greater_or_equal(TstView):
    def __init__(self):
        self.data = [
            {
                "id": 0,
                "tiny_un_int": True,
                "small_un_int": True,
                "un_intt": True,
                "big_un_int": True,
            }
        ]
        self.sql = """CREATE MATERIALIZED VIEW greater_or_equal_un_int AS SELECT
                      id,
                      tiny_int >= 9 AS tiny_un_int,
                      small_int >= 10192 AS small_un_int,
                      intt >= 1146171806 AS un_intt,
                      big_int >= 6996571239486628517 AS big_un_int
                      FROM un_int_datagen_tbl"""


# IS NULL
class un_int_isnull(TstView):
    def __init__(self):
        self.data = [
            {
                "id": 0,
                "tiny_un_int": False,
                "small_un_int": False,
                "un_intt": False,
                "big_un_int": False,
            }
        ]
        self.sql = """CREATE MATERIALIZED VIEW isnull_un_int AS SELECT
                      id,
                      tiny_int IS NULL AS tiny_un_int,
                      small_int IS NULL AS small_un_int,
                      intt IS NULL AS un_intt,
                      big_int IS NULL AS big_un_int
                      FROM un_int_datagen_tbl"""


# IS NOT NULL
class un_int_isnotnull(TstView):
    def __init__(self):
        self.data = [
            {
                "id": 0,
                "tiny_un_int": True,
                "small_un_int": True,
                "un_intt": True,
                "big_un_int": True,
            }
        ]
        self.sql = """CREATE MATERIALIZED VIEW isnotnull_un_int AS SELECT
                      id,
                      tiny_int IS NOT NULL AS tiny_un_int,
                      small_int IS NOT NULL AS small_un_int,
                      intt IS NOT NULL AS un_intt,
                      big_int IS NOT NULL AS big_un_int
                      FROM un_int_datagen_tbl"""


# Equality check with NULL
class un_int_equality_null(TstView):
    def __init__(self):
        self.data = [
            {
                "id": 0,
                "tiny_un_int": False,
                "small_un_int": False,
                "un_intt": False,
                "big_un_int": False,
            }
        ]
        self.sql = """CREATE MATERIALIZED VIEW equality_null_un_int AS SELECT
                      id,
                      tiny_int <=> NULL AS tiny_un_int,
                      small_int <=> NULL AS small_un_int,
                      intt <=> NULL AS un_intt,
                      big_int <=> NULL AS big_un_int
                      FROM un_int_datagen_tbl"""


# IS DISTINCT FROM
class un_int_isdistinct(TstView):
    def __init__(self):
        self.data = [
            {
                "id": 0,
                "tiny_un_int": True,
                "small_un_int": True,
                "un_intt": True,
                "big_un_int": True,
            }
        ]
        self.sql = """CREATE MATERIALIZED VIEW isdistinct_un_int AS SELECT
                      id,
                      tiny_int IS DISTINCT FROM 12 AS tiny_un_int,
                      small_int IS DISTINCT FROM 1234 AS small_un_int,
                      intt IS DISTINCT FROM 123456 AS un_intt,
                      big_int IS DISTINCT FROM 1234567890 AS big_un_int
                      FROM un_int_datagen_tbl"""


# IS NOT DISTINCT FROM
class un_int_isnot_distinct(TstView):
    def __init__(self):
        self.data = [
            {
                "id": 0,
                "tiny_un_int": False,
                "small_un_int": False,
                "un_intt": False,
                "big_un_int": False,
            }
        ]
        self.sql = """CREATE MATERIALIZED VIEW isnot_distinct_un_int AS SELECT
                      id,
                      tiny_int IS NOT DISTINCT FROM 12 AS tiny_un_int,
                      small_int IS NOT DISTINCT FROM 1234 AS small_un_int,
                      intt IS NOT DISTINCT FROM 123456 AS un_intt,
                      big_int IS NOT DISTINCT FROM 1234567890 AS big_un_int
                      FROM un_int_datagen_tbl"""


# BETWEEN...AND...
class un_int_between_and(TstView):
    def __init__(self):
        self.data = [
            {
                "id": 0,
                "tiny_un_int": False,
                "small_un_int": False,
                "un_intt": False,
                "big_un_int": False,
            }
        ]
        self.sql = """CREATE MATERIALIZED VIEW between_and_un_int AS SELECT
                      id,
                      tiny_int BETWEEN 10 AND 20 AS tiny_un_int,
                      small_int BETWEEN 1000 AND 2000 AS small_un_int,
                      intt BETWEEN 100000 AND 200000 AS un_intt,
                      big_int BETWEEN 1000000000 AND 2000000000 AS big_un_int
                      FROM un_int_datagen_tbl"""


# NOT BETWEEN...AND...
class un_int_notbetween_and(TstView):
    def __init__(self):
        self.data = [
            {
                "id": 0,
                "tiny_un_int": True,
                "small_un_int": True,
                "un_intt": True,
                "big_un_int": True,
            }
        ]
        self.sql = """CREATE MATERIALIZED VIEW notbetween_and_un_int AS SELECT
                      id,
                      tiny_int NOT BETWEEN 10 AND 20 AS tiny_un_int,
                      small_int NOT BETWEEN 1000 AND 2000 AS small_un_int,
                      intt NOT BETWEEN 100000 AND 200000 AS un_intt,
                      big_int NOT BETWEEN 1000000000 AND 2000000000 AS big_un_int
                      FROM un_int_datagen_tbl"""


# [NOT] IN
class un_int_in(TstView):
    def __init__(self):
        self.data = [
            {
                "id": 0,
                "tiny_un_int": False,
                "small_un_int": False,
                "un_intt": False,
                "big_un_int": False,
            }
        ]
        self.sql = """CREATE MATERIALIZED VIEW in_un_int AS SELECT
                      id,
                      tiny_int IN (12, 15, 20) AS tiny_un_int,
                      small_int IN (1234, 2000, 3000) AS small_un_int,
                      intt IN (123456, 200000, 300000) AS un_intt,
                      big_int IN (1234567890, 2000000000, 3000000000) AS big_un_int
                      FROM un_int_datagen_tbl"""


class un_int_not_in(TstView):
    def __init__(self):
        self.data = [
            {
                "id": 0,
                "tiny_un_int": True,
                "small_un_int": True,
                "un_intt": True,
                "big_un_int": True,
            }
        ]
        self.sql = """CREATE MATERIALIZED VIEW not_in_un_int AS SELECT
                      id,
                      tiny_int NOT IN (12, 15, 20) AS tiny_un_int,
                      small_int NOT IN (1234, 2000, 3000) AS small_un_int,
                      intt NOT IN (123456, 200000, 300000) AS un_intt,
                      big_int NOT IN (1234567890, 2000000000, 3000000000) AS big_un_int
                      FROM un_int_datagen_tbl"""


# Exists
class un_int_exists(TstView):
    def __init__(self):
        self.data = [{"exists": True}]
        self.sql = """CREATE MATERIALIZED VIEW exists_un_int AS SELECT
                      EXISTS (SELECT 1
                      FROM un_int_datagen_tbl) AS exists"""


# CASE_value_WHEN
class un_int_case_val_when_tiny_int(TstView):
    def __init__(self):
        self.data = [{"id": 0, "tiny_un_int": 0}]
        self.sql = """CREATE MATERIALIZED VIEW case_val_when_tiny_int_un_int AS SELECT
                      id,
                      CASE tiny_int
                          WHEN 9 THEN 0
                          ELSE NULL
                      END AS tiny_un_int
                      FROM un_int_datagen_tbl"""


class un_int_case_val_when_small_int(TstView):
    def __init__(self):
        self.data = [{"id": 0, "small_un_int": None}]
        self.sql = """CREATE MATERIALIZED VIEW case_val_when_small_int_un_int AS SELECT
                      id,
                      CASE small_int
                          WHEN 9 THEN 0
                          ELSE NULL
                      END AS small_un_int
                      FROM un_int_datagen_tbl"""


class un_int_case_val_when_intt(TstView):
    def __init__(self):
        self.data = [{"id": 0, "un_intt": 0}]
        self.sql = """CREATE MATERIALIZED VIEW case_val_when_intt_un_int AS SELECT
                      id,
                      CASE intt
                          WHEN 1146171806 THEN 0
                          ELSE NULL
                      END AS un_intt
                      FROM un_int_datagen_tbl"""


class un_int_case_val_when_big_int(TstView):
    def __init__(self):
        self.data = [{"id": 0, "big_un_int": None}]
        self.sql = """CREATE MATERIALIZED VIEW case_val_when_big_int_un_int AS SELECT
                      id,
                      CASE big_int
                          WHEN 1146171806 THEN 0
                          ELSE NULL
                      END AS big_un_int
                      FROM un_int_datagen_tbl"""


# CASE WHEN
class un_int_case_when(TstView):
    def __init__(self):
        self.data = [
            {
                "id": 0,
                "tiny_un_int": True,
                "small_un_int": True,
                "un_intt": True,
                "big_un_int": True,
            }
        ]
        self.sql = """CREATE MATERIALIZED VIEW case_when_un_int AS SELECT
                      id,
                      CASE WHEN tiny_int = 9 THEN True ELSE NULL END AS tiny_un_int,
                      CASE WHEN small_int = 10192 THEN True ELSE NULL END AS small_un_int,
                      CASE WHEN intt = 1146171806 THEN True ELSE NULL END AS un_intt,
                      CASE WHEN big_int = 6996571239486628517 THEN True ELSE NULL END AS big_un_int
                      FROM un_int_datagen_tbl"""


# COALESCE
class un_int_coalesce(TstView):
    def __init__(self):
        self.data = [
            {
                "id": 0,
                "tiny_un_int": 9,
                "small_un_int": 10192,
                "un_intt": 1146171806,
                "big_un_int": 6996571239486628517,
            }
        ]
        self.sql = """CREATE MATERIALIZED VIEW coalesce_un_int AS SELECT
                      id,
                      COALESCE(NULL, tiny_int, 10) AS tiny_un_int,
                      COALESCE(NULL, small_int, 1000) AS small_un_int,
                      COALESCE(NULL, intt, 100000) AS un_intt,
                      COALESCE(NULL, big_int, 1000000000) AS big_un_int
                      FROM un_int_datagen_tbl"""


# IF
class un_int_if(TstView):
    def __init__(self):
        self.data = [
            {
                "id": 0,
                "tiny_un_int": "correct",
                "small_un_int": "incorrect",
                "un_intt": "incorrect",
                "big_un_int": "incorrect",
            }
        ]
        self.sql = """CREATE MATERIALIZED VIEW if_un_int AS SELECT
                      id,
                      IF(tiny_int <= 12, 'correct', 'incorrect') AS tiny_un_int,
                      IF(small_int <= 1234, 'correct', 'incorrect') AS small_un_int,
                      IF(intt <= 123456, 'correct', 'incorrect') AS un_intt,
                      IF(big_int <= 1234567890, 'correct', 'incorrect') AS big_un_int
                      FROM un_int_datagen_tbl"""


# NULLIF
class un_int_nullif(TstView):
    def __init__(self):
        self.data = [
            {
                "id": 0,
                "tiny_un_int": 9,
                "small_un_int": 10192,
                "un_intt": 1146171806,
                "big_un_int": 6996571239486628517,
            }
        ]
        self.sql = """CREATE MATERIALIZED VIEW nullif_un_int AS SELECT
                      id,
                      NULLIF(tiny_int, 12) AS tiny_un_int,
                      NULLIF(small_int, 1234) AS small_un_int,
                      NULLIF(intt, 123456) AS un_intt,
                      NULLIF(big_int, 1234567890) AS big_un_int
                      FROM un_int_datagen_tbl"""


# IS UNKNOWN
class un_int_is_unknown(TstView):
    def __init__(self):
        self.data = [
            {
                "id": 0,
                "tiny_un_int": False,
                "small_un_int": False,
                "un_intt": False,
                "big_un_int": False,
            }
        ]
        self.sql = """CREATE MATERIALIZED VIEW is_unknown_un_int AS SELECT
                      id,
                      (tiny_int > 100)::BOOLEAN IS UNKNOWN AS tiny_un_int,
                      (small_int > 10000)::BOOLEAN IS UNKNOWN AS small_un_int,
                      (intt > 1000000)::BOOLEAN IS UNKNOWN AS un_intt,
                      (big_int > 10000000000)::BOOLEAN IS UNKNOWN AS big_un_int
                      FROM un_int_datagen_tbl"""


# IS NOT UNKNOWN
class un_int_is_not_unknown(TstView):
    def __init__(self):
        self.data = [
            {
                "id": 0,
                "tiny_un_int": True,
                "small_un_int": True,
                "un_intt": True,
                "big_un_int": True,
            }
        ]
        self.sql = """CREATE MATERIALIZED VIEW is_not_unknown_un_int AS SELECT
                      id,
                      (tiny_int > 100)::BOOLEAN IS NOT UNKNOWN AS tiny_un_int,
                      (small_int > 10000)::BOOLEAN IS NOT UNKNOWN AS small_un_int,
                      (intt > 1000000)::BOOLEAN IS NOT UNKNOWN AS un_intt,
                      (big_int > 10000000000)::BOOLEAN IS NOT UNKNOWN AS big_un_int
                      FROM un_int_datagen_tbl"""


# ARRAY() function
class un_int_arr_construct(TstView):
    def __init__(self):
        self.data = [{"arr": [9, 10192, 1146171806, 6996571239486628517]}]
        self.sql = """CREATE MATERIALIZED VIEW un_int_arr_construct AS SELECT
                      ARRAY(tiny_int, small_int, intt, big_int) AS arr
                      FROM un_int_datagen_tbl"""


# ASCII
class un_int_ascii(TstView):
    def __init__(self):
        self.data = [{"tiny_int": 57, "small_int": 49, "intt": 49, "big_int": 54}]
        self.sql = """CREATE MATERIALIZED VIEW un_int_ascii AS SELECT
                      ASCII(tiny_int) AS tiny_int,
                      ASCII(small_int) AS small_int,
                      ASCII(intt) AS intt,
                      ASCII(big_int) AS big_int
                      FROM un_int_datagen_tbl"""


# CONCAT
class un_int_concat(TstView):
    def __init__(self):
        self.data = [
            {
                "tiny_int": "99",
                "small_int": "1019210192",
                "intt": "11461718061146171806",
                "big_int": "69965712394866285176996571239486628517",
            }
        ]
        self.sql = """CREATE MATERIALIZED VIEW un_int_concat AS SELECT
                      CONCAT(tiny_int, tiny_int) AS tiny_int,
                      CONCAT(small_int, small_int) AS small_int,
                      CONCAT(intt, intt) AS intt,
                      CONCAT(big_int, big_int) AS big_int
                      FROM un_int_datagen_tbl"""


# LEFT
class un_int_left(TstView):
    def __init__(self):
        self.data = [
            {"tiny_int": "9", "small_int": "10", "intt": "11", "big_int": "69"}
        ]
        self.sql = """CREATE MATERIALIZED VIEW un_int_left AS SELECT
                      LEFT(tiny_int, 2) AS tiny_int,
                      LEFT(small_int, 2) AS small_int,
                      LEFT(intt, 2) AS intt,
                      LEFT(big_int, 2) AS big_int
                      FROM un_int_datagen_tbl"""


# RIGHT
class un_int_right(TstView):
    def __init__(self):
        self.data = [
            {"tiny_int": "9", "small_int": "92", "intt": "06", "big_int": "17"}
        ]
        self.sql = """CREATE MATERIALIZED VIEW un_int_right AS SELECT
                      RIGHT(tiny_int, 2) AS tiny_int,
                      RIGHT(small_int, 2) AS small_int,
                      RIGHT(intt, 2) AS intt,
                      RIGHT(big_int, 2) AS big_int
                      FROM un_int_datagen_tbl"""


# LEN
class un_int_len(TstView):
    def __init__(self):
        self.data = [{"tiny_int": 1, "small_int": 5, "intt": 10, "big_int": 19}]
        self.sql = """CREATE MATERIALIZED VIEW un_int_len AS SELECT
                      LEN(tiny_int) AS tiny_int,
                      LEN(small_int) AS small_int,
                      LEN(intt) AS intt,
                      LEN(big_int) AS big_int
                      FROM un_int_datagen_tbl"""


# TRIM
class un_int_trim(TstView):
    def __init__(self):
        self.data = [
            {
                "tiny_int": "",
                "small_int": "101",
                "intt": "11461718",
                "big_int": "69965712394866285",
            }
        ]
        self.sql = """CREATE MATERIALIZED VIEW un_int_trim AS SELECT
                      TRIM(trailing '9' from tiny_int) AS tiny_int,
                      TRIM(trailing '92' from small_int) AS small_int,
                      TRIM(trailing '06' from intt) AS intt,
                      TRIM(trailing '17' from big_int) AS big_int
                      FROM un_int_datagen_tbl"""


# SPLIT_PART
class un_int_split_part(TstView):
    def __init__(self):
        self.data = [
            {
                "tiny_int": "9",
                "small_int": "10192",
                "intt": "1146171806",
                "big_int": "6996571239486628517",
            }
        ]
        self.sql = """CREATE MATERIALIZED VIEW un_int_split_part AS SELECT
                      SPLIT_PART(tiny_int, '', 1) AS tiny_int,
                      SPLIT_PART(small_int, '', 1) AS small_int,
                      SPLIT_PART(intt, '', 1) AS intt,
                      SPLIT_PART(big_int, '', 1) AS big_int
                      FROM un_int_datagen_tbl"""


# BINARY CONCAT
class un_int_bin_concat(TstView):
    def __init__(self):
        self.data = [
            {
                "tiny_int": "99",
                "small_int": "1019210192",
                "intt": "11461718061146171806",
                "big_int": "69965712394866285176996571239486628517",
            }
        ]
        self.sql = """CREATE MATERIALIZED VIEW un_int_bin_concat AS SELECT
                      tiny_int || tiny_int AS tiny_int,
                      small_int || small_int AS small_int,
                      intt || intt AS intt,
                      big_int || big_int AS big_int
                      FROM un_int_datagen_tbl"""
