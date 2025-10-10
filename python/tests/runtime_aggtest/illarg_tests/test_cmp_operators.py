from tests.runtime_aggtest.aggtst_base import TstView
from decimal import Decimal


# Comparison Operators


# Equality
class illarg_equality_legal(TstView):
    def __init__(self):
        # checked manually
        self.data = [
            {
                "intt": True,
                "decimall": True,
                "reall": True,
                "dbl": True,
                "booll": True,
                "str": True,
                "bin": True,
                "tmestmp": True,
                "datee": True,
                "tme": True,
                "uuidd": True,
                "arr": True,
                "mapp": True,
            }
        ]
        self.sql = """CREATE MATERIALIZED VIEW equality_legal AS SELECT
                      intt = -12 AS intt,
                      decimall = -1111.52 AS decimall,
                      reall = -57681.18 AS reall,
                      dbl = -38.2711234601246 AS dbl,
                      booll = True AS booll,
                      str = 'hello ' AS str,
                      bin = X'0B1620' AS bin,
                      tmestmp = TIMESTAMP '2020-06-21 14:23:44.123' AS tmestmp,
                      datee = DATE '2020-06-21' AS datee,
                      tme = TIME '14:23:44.456' AS tme,
                      uuidd = UUID '42b8fec7-c7a3-4531-9611-4bde80f9cb4c' AS uuidd,
                      arr = ARRAY['bye', '14', 'See you!', '-0.52', NULL, '14', 'hello '] AS arr,
                      mapp = MAP['a', 12, 'b', 17] AS mapp
                      FROM illegal_tbl
                      WHERE id = 0"""


# # Negative Test
class illarg_equality_illegal(TstView):
    def __init__(self):
        # checked manually
        self.sql = """CREATE MATERIALIZED VIEW equality_illegal AS SELECT
                      mapp = ARRAY['bye'] AS mapp
                      FROM illegal_tbl
                      WHERE id = 1"""
        self.expected_error = "Cannot apply '=' to arguments of type"


# Inequality
class illarg_inequality_legal(TstView):
    def __init__(self):
        # checked manually
        self.data = [
            {
                "intt": False,
                "decimall": False,
                "reall": False,
                "dbl": False,
                "booll": False,
                "str": False,
                "bin": False,
                "tmestmp": False,
                "datee": False,
                "tme": False,
                "uuidd": False,
                "arr": False,
                "mapp": False,
            }
        ]
        self.sql = """CREATE MATERIALIZED VIEW inequality_legal AS SELECT
                      intt != -12 AS intt,
                      decimall != -1111.52 AS decimall,
                      reall != -57681.18 AS reall,
                      dbl != -38.2711234601246 AS dbl,
                      booll != True AS booll,
                      str != 'hello ' AS str,
                      bin != X'0B1620' AS bin,
                      tmestmp != TIMESTAMP '2020-06-21 14:23:44.123' AS tmestmp,
                      datee != DATE '2020-06-21' AS datee,
                      tme != TIME '14:23:44.456' AS tme,
                      uuidd != UUID '42b8fec7-c7a3-4531-9611-4bde80f9cb4c' AS uuidd,
                      arr != ARRAY['bye', '14', 'See you!', '-0.52', NULL, '14', 'hello '] AS arr,
                      mapp != MAP['a', 12, 'b', 17] AS mapp
                      FROM illegal_tbl
                      WHERE id = 0"""


# Negative Test
class illarg_inequality_illegal(TstView):
    def __init__(self):
        # checked manually
        self.sql = """CREATE MATERIALIZED VIEW inequality_illegal AS SELECT
                      mapp != ARRAY['bye'] AS mapp
                      FROM illegal_tbl
                      WHERE id = 1"""
        self.expected_error = "Cannot apply '<>' to arguments of type"


# Greater Than
class illarg_greater_than_legal(TstView):
    def __init__(self):
        # checked manually
        self.data = [
            {
                "intt": False,
                "decimall": False,
                "reall": False,
                "dbl": False,
                "booll": False,
                "str": False,
                "bin": False,
                "tmestmp": False,
                "datee": False,
                "tme": False,
                "uuidd": False,
                "arr": False,
                "mapp": False,
            }
        ]
        self.sql = """CREATE MATERIALIZED VIEW greater_than_legal AS SELECT
                      intt > -12 AS intt,
                      decimall > -1111.52 AS decimall,
                      reall > -57681.18 AS reall,
                      dbl > -38.2711234601246 AS dbl,
                      booll > True AS booll,
                      str > 'hello ' AS str,
                      bin > X'0B1620' AS bin,
                      tmestmp > TIMESTAMP '2020-06-21 14:23:44.123' AS tmestmp,
                      datee > DATE '2020-06-21' AS datee,
                      tme > TIME '14:23:44.456' AS tme,
                      uuidd > UUID '42b8fec7-c7a3-4531-9611-4bde80f9cb4c' AS uuidd,
                      arr > ARRAY['bye', '14', 'See you!', '-0.52', NULL, '14', 'hello '] AS arr,
                      mapp > MAP['a', 12, 'b', 17] AS mapp
                      FROM illegal_tbl
                      WHERE id = 0"""


# Negative Test
class illarg_greater_than_illegal(TstView):
    def __init__(self):
        # checked manually
        self.sql = """CREATE MATERIALIZED VIEW greater_than_illegal AS SELECT
                      arr > MAP['a', 12, 'b', 17] AS arr
                      FROM illegal_tbl
                      WHERE id = 1"""
        self.expected_error = "Cannot apply '>' to arguments of type"


# Less Than
class illarg_less_than_legal(TstView):
    def __init__(self):
        # checked manually
        self.data = [
            {
                "intt": False,
                "decimall": False,
                "reall": False,
                "dbl": False,
                "booll": False,
                "str": False,
                "bin": False,
                "tmestmp": False,
                "datee": False,
                "tme": False,
                "uuidd": False,
                "arr": False,
                "mapp": False,
            }
        ]
        self.sql = """CREATE MATERIALIZED VIEW less_than_legal AS SELECT
                      intt < -12 AS intt,
                      decimall < -1111.52 AS decimall,
                      reall < -57681.18 AS reall,
                      dbl < -38.2711234601246 AS dbl,
                      booll < True AS booll,
                      str < 'hello ' AS str,
                      bin < X'0B1620' AS bin,
                      tmestmp < TIMESTAMP '2020-06-21 14:23:44.123' AS tmestmp,
                      datee < DATE '2020-06-21' AS datee,
                      tme < TIME '14:23:44.456' AS tme,
                      uuidd < UUID '42b8fec7-c7a3-4531-9611-4bde80f9cb4c' AS uuidd,
                      arr < ARRAY['bye', '14', 'See you!', '-0.52', NULL, '14', 'hello '] AS arr,
                      mapp < MAP['a', 12, 'b', 17] AS mapp
                      FROM illegal_tbl
                      WHERE id = 0"""


# Negative Test
class illarg_less_than_illegal(TstView):
    def __init__(self):
        # checked manually
        self.sql = """CREATE MATERIALIZED VIEW less_than_illegal AS SELECT
                      arr < MAP['a', 12, 'b', 17] AS arr
                      FROM illegal_tbl
                      WHERE id = 1"""
        self.expected_error = "Cannot apply '<' to arguments of type"


# Greater or Equal
class illarg_less_or_equal_legal(TstView):
    def __init__(self):
        # checked manually
        self.data = [
            {
                "intt": True,
                "decimall": True,
                "reall": True,
                "dbl": True,
                "booll": True,
                "str": True,
                "bin": True,
                "tmestmp": True,
                "datee": True,
                "tme": True,
                "uuidd": True,
                "arr": True,
                "mapp": True,
            }
        ]
        self.sql = """CREATE MATERIALIZED VIEW less_or_equal_legal AS SELECT
                      intt <= -12 AS intt,
                      decimall <= -1111.52 AS decimall,
                      reall <= -57681.18 AS reall,
                      dbl <= -38.2711234601246 AS dbl,
                      booll <= True AS booll,
                      str <= 'hello ' AS str,
                      bin <= X'0B1620' AS bin,
                      tmestmp <= TIMESTAMP '2020-06-21 14:23:44.123' AS tmestmp,
                      datee <= DATE '2020-06-21' AS datee,
                      tme <= TIME '14:23:44.456' AS tme,
                      uuidd <= UUID '42b8fec7-c7a3-4531-9611-4bde80f9cb4c' AS uuidd,
                      arr <= ARRAY['bye', '14', 'See you!', '-0.52', NULL, '14', 'hello '] AS arr,
                      mapp <= MAP['a', 12, 'b', 17] AS mapp
                      FROM illegal_tbl
                      WHERE id = 0"""


# Negative Test
class illarg_less_or_equal_illegal(TstView):
    def __init__(self):
        # checked manually
        self.sql = """CREATE MATERIALIZED VIEW less_or_equal_illegal AS SELECT
                      arr <= MAP['a', 12, 'b', 17] AS arr
                      FROM illegal_tbl
                      WHERE id = 1"""
        self.expected_error = "Cannot apply '<=' to arguments of type"


# IS NULL (passes for all types)
class illarg_isnull_legal(TstView):
    def __init__(self):
        # checked manually
        self.data = [
            {
                "intt": True,
                "decimall": True,
                "reall": True,
                "dbl": True,
                "booll": True,
                "str": True,
                "bin": True,
                "tmestmp": True,
                "datee": True,
                "tme": True,
                "uuidd": True,
                "arr": True,
                "mapp": True,
            }
        ]
        self.sql = """CREATE MATERIALIZED VIEW isnull_legal AS SELECT
                      intt IS NULL AS intt,
                      decimall IS NULL AS decimall,
                      reall IS NULL AS reall,
                      dbl IS NULL AS dbl,
                      booll IS NULL AS booll,
                      str IS NULL AS str,
                      bin IS NULL AS bin,
                      tmestmp IS NULL AS tmestmp,
                      datee IS NULL AS datee,
                      tme IS NULL AS tme,
                      uuidd IS NULL AS uuidd,
                      arr IS NULL  AS arr,
                      mapp IS NULL AS mapp
                      FROM illegal_tbl
                      WHERE id = 2"""


# IS NOT NULL (passes for all types)
class illarg_isnotnull_legal(TstView):
    def __init__(self):
        # checked manually
        self.data = [
            {
                "intt": True,
                "decimall": True,
                "reall": True,
                "dbl": True,
                "booll": True,
                "str": True,
                "bin": True,
                "tmestmp": True,
                "datee": True,
                "tme": True,
                "uuidd": True,
                "arr": True,
                "mapp": True,
            }
        ]
        self.sql = """CREATE MATERIALIZED VIEW isnotnull_legal AS SELECT
                      intt IS NOT NULL AS intt,
                      decimall IS NOT NULL AS decimall,
                      reall IS NOT NULL AS reall,
                      dbl IS NOT NULL AS dbl,
                      booll IS NOT NULL AS booll,
                      str IS NOT NULL AS str,
                      bin IS NOT NULL AS bin,
                      tmestmp IS NOT NULL AS tmestmp,
                      datee IS NOT NULL AS datee,
                      tme IS NOT NULL AS tme,
                      uuidd IS NOT NULL AS uuidd,
                      arr IS NOT NULL  AS arr,
                      mapp IS NOT NULL AS mapp
                      FROM illegal_tbl
                      WHERE id = 1"""


# Equality check
class illarg_equality_null_legal(TstView):
    def __init__(self):
        # checked manually
        self.data = [
            {
                "intt": True,
                "decimall": True,
                "reall": True,
                "dbl": True,
                "booll": True,
                "str": True,
                "bin": True,
                "tmestmp": True,
                "datee": True,
                "tme": True,
                "uuidd": True,
                "arr": True,
                "mapp": True,
            }
        ]
        self.sql = """CREATE MATERIALIZED VIEW equality_null_legal AS SELECT
                      intt <=> NULL AS intt,
                      decimall <=> NULL AS decimall,
                      reall <=> NULL AS reall,
                      dbl <=> NULL AS dbl,
                      booll <=> NULL AS booll,
                      str <=> NULL AS str,
                      bin <=> NULL AS bin,
                      tmestmp <=> NULL AS tmestmp,
                      datee <=> NULL AS datee,
                      tme <=> NULL AS tme,
                      uuidd <=> NULL AS uuidd,
                      arr <=> NULL  AS arr,
                      mapp <=> NULL AS mapp
                      FROM illegal_tbl
                      WHERE id = 2"""


# Negative Test
class illarg_equality_null_illegal(TstView):
    def __init__(self):
        # checked manually
        self.sql = """CREATE MATERIALIZED VIEW equality_null_illegal AS SELECT
                      mapp <=> ARRAY['bye', '14', 'See you!', '-0.52', NULL, '14', 'hello '] AS mapp
                      FROM illegal_tbl
                      WHERE id = 1"""
        self.expected_error = "Cannot apply '<=>' to arguments of type"


# IS DISTINCT FROM
class illarg_isdistinct_legal(TstView):
    def __init__(self):
        # checked manually
        self.data = [
            {
                "intt": True,
                "decimall": True,
                "reall": True,
                "dbl": True,
                "booll": True,
                "str": True,
                "bin": True,
                "tmestmp": True,
                "datee": True,
                "tme": True,
                "uuidd": True,
                "arr": True,
                "mapp": True,
            }
        ]
        self.sql = """CREATE MATERIALIZED VIEW isdistinct_legal AS SELECT
                      intt IS DISTINCT FROM -1 AS intt,
                      decimall IS DISTINCT FROM -1111.2 AS decimall,
                      reall IS DISTINCT FROM -57681.8 AS reall,
                      dbl IS DISTINCT FROM -38.271123460124 AS dbl,
                      booll IS DISTINCT FROM False AS booll,
                      str IS DISTINCT FROM 'hello' AS str,
                      bin IS DISTINCT FROM X'0B1625' AS bin,
                      tmestmp IS DISTINCT FROM TIMESTAMP '2020-06-21 14:23:44' AS tmestmp,
                      datee IS DISTINCT FROM DATE '2020-06-20' AS datee,
                      tme IS DISTINCT FROM TIME '14:23:44.45' AS tme,
                      uuidd IS DISTINCT FROM UUID '42b8fec7-c7a3-4531-9611-4bde80f9cb4d' AS uuidd,
                      arr IS DISTINCT FROM ARRAY['14', 'See you!', '-0.52', NULL, '14', 'hello '] AS arr,
                      mapp IS DISTINCT FROM MAP['a', 12] AS mapp
                      FROM illegal_tbl
                      WHERE id = 0"""


# Negative Test
class illarg_isdistinct_illegal(TstView):
    def __init__(self):
        # checked manually
        self.sql = """CREATE MATERIALIZED VIEW isdistinct_illegal AS SELECT
                      dbl IS DISTINCT FROM X'0B1620' AS dbl
                      FROM illegal_tbl
                      WHERE id = 1"""
        self.expected_error = "Cannot apply 'IS DISTINCT FROM' to arguments of type"


# IS NOT DISTINCT FROM
class illarg_isnot_distinct_legal(TstView):
    def __init__(self):
        # checked manually
        self.data = [
            {
                "intt": True,
                "decimall": True,
                "reall": True,
                "dbl": True,
                "booll": True,
                "str": True,
                "bin": True,
                "tmestmp": True,
                "datee": True,
                "tme": True,
                "uuidd": True,
                "arr": True,
                "mapp": True,
            }
        ]
        self.sql = """CREATE MATERIALIZED VIEW isnot_distinct_legal AS SELECT
                      intt IS NOT DISTINCT FROM -12 AS intt,
                      decimall IS NOT DISTINCT FROM -1111.52 AS decimall,
                      reall IS NOT DISTINCT FROM -57681.18 AS reall,
                      dbl IS NOT DISTINCT FROM -38.2711234601246 AS dbl,
                      booll IS NOT DISTINCT FROM True AS booll,
                      str IS NOT DISTINCT FROM 'hello ' AS str,
                      bin IS NOT DISTINCT FROM X'0B1620' AS bin,
                      tmestmp IS NOT DISTINCT FROM TIMESTAMP '2020-06-21 14:23:44.123' AS tmestmp,
                      datee IS NOT DISTINCT FROM DATE '2020-06-21' AS datee,
                      tme IS NOT DISTINCT FROM TIME '14:23:44.456' AS tme,
                      uuidd IS NOT DISTINCT FROM UUID '42b8fec7-c7a3-4531-9611-4bde80f9cb4c' AS uuidd,
                      arr IS NOT DISTINCT FROM ARRAY['bye', '14', 'See you!', '-0.52', NULL, '14', 'hello '] AS arr,
                      mapp IS NOT DISTINCT FROM MAP['a', 12, 'b', 17] AS mapp
                      FROM illegal_tbl
                      WHERE id = 0"""


# Negative Test
class illarg_isnot_distinct_illegal(TstView):
    def __init__(self):
        # checked manually
        self.sql = """CREATE MATERIALIZED VIEW isnot_distinct_illegal AS SELECT
                      dbl IS NOT DISTINCT FROM X'0B1620' AS dbl
                      FROM illegal_tbl
                      WHERE id = 1"""
        self.expected_error = "Cannot apply 'IS NOT DISTINCT FROM' to arguments of type"


# BETWEEN...AND...
class illarg_between_and_legal(TstView):
    def __init__(self):
        # checked manually
        self.data = [
            {
                "intt": False,
                "decimall": False,
                "reall": False,
                "dbl": False,
                "str": True,
                "booll": True,
                "bin": True,
                "tmestmp": True,
                "datee": True,
                "tme": True,
                "uuidd": True,
                "arr": True,
                "mapp": True,
            }
        ]
        self.sql = """CREATE MATERIALIZED VIEW between_and_legal AS SELECT
                      intt BETWEEN -11 AND -13 AS intt,
                      decimall BETWEEN -1111.51 AND -1111.53 AS decimall,
                      reall BETWEEN -57681.17 AND -57681.19 AS reall,
                      dbl BETWEEN -38.2711234601245 AND -38.2711234601247 AS dbl,
                      str BETWEEN 'hello ' AND 'hello ' AS str,
                      booll BETWEEN True AND True AS booll,
                      bin BETWEEN X'0B1620' AND X'0B1620' AS bin,
                      tmestmp BETWEEN TIMESTAMP '2020-06-21 14:23:43.123' AND TIMESTAMP '2020-06-21 14:23:45.123' AS tmestmp,
                      datee BETWEEN DATE '2020-06-20' AND DATE '2020-06-22' AS datee,
                      tme BETWEEN TIME '14:23:43.456' AND TIME '14:23:45.456' AS tme,
                      uuidd BETWEEN UUID '42b8fec7-c7a3-4531-9611-4bde80f9cb4c' AND UUID '42b8fec7-c7a3-4531-9611-4bde80f9cb4c' AS uuidd,
                      ARRAY['bye', '14'] BETWEEN ARRAY['bye', '14'] AND ARRAY['bye', '14'] AS arr,
                      mapp BETWEEN MAP['a', 12, 'b', 17] AND MAP['a', 12, 'b', 17] AS mapp
                      FROM illegal_tbl
                      WHERE id = 0"""


# Negative Test
class illarg_between_and_illegal(TstView):
    def __init__(self):
        # checked manually
        self.sql = """CREATE MATERIALIZED VIEW between_and_illegal AS SELECT
                      mapp BETWEEN ARRAY['bye', '14'] AND ARRAY['bye', '14'] AS mapp
                      FROM illegal_tbl
                      WHERE id = 1"""
        self.expected_error = "Cannot apply 'BETWEEN ASYMMETRIC' to arguments of type"


# NOT BETWEEN...AND...
class illarg_notbetween_and_legal(TstView):
    def __init__(self):
        # checked manually
        self.data = [
            {
                "intt": True,
                "decimall": True,
                "reall": True,
                "dbl": True,
                "str": False,
                "bin": False,
                "booll": False,
                "tmestmp": False,
                "datee": False,
                "tme": False,
                "uuidd": False,
                "arr": False,
                "mapp": False,
            }
        ]
        self.sql = """CREATE MATERIALIZED VIEW notbetween_and_legal AS SELECT
                      intt NOT BETWEEN -11 AND -13 AS intt,
                      decimall NOT BETWEEN -1111.51 AND -1111.53 AS decimall,
                      reall NOT BETWEEN -57681.17 AND -57681.19 AS reall,
                      dbl NOT BETWEEN -38.2711234601245 AND -38.2711234601247 AS dbl,
                      str NOT BETWEEN 'hello ' AND 'hello ' AS str,
                      X'0B1620' NOT BETWEEN X'0B1620' AND X'0B1620' AS bin,
                      booll NOT BETWEEN True AND True AS booll,
                      tmestmp NOT BETWEEN TIMESTAMP '2020-06-21 14:23:43.123' AND TIMESTAMP '2020-06-21 14:23:45.123' AS tmestmp,
                      datee NOT BETWEEN DATE '2020-06-20' AND DATE '2020-06-22' AS datee,
                      tme NOT BETWEEN TIME '14:23:43.456' AND TIME '14:23:45.456' AS tme,
                      uuidd NOT BETWEEN UUID '42b8fec7-c7a3-4531-9611-4bde80f9cb4c' AND UUID '42b8fec7-c7a3-4531-9611-4bde80f9cb4c' AS uuidd,
                      ARRAY['bye', '14'] NOT BETWEEN ARRAY['bye', '14'] AND ARRAY['bye', '14'] AS arr,
                      mapp NOT BETWEEN MAP['a', 12, 'b', 17] AND MAP['a', 12, 'b', 17] AS mapp
                      FROM illegal_tbl
                      WHERE id = 0"""


# Negative Test
class illarg_notbetween_and_illegal(TstView):
    def __init__(self):
        # checked manually
        self.sql = """CREATE MATERIALIZED VIEW notbetween_and_illegal AS SELECT
                      mapp NOT BETWEEN ARRAY['bye', '14'] AND ARRAY['bye', '14'] AS mapp
                      FROM illegal_tbl
                      WHERE id = 1"""
        self.expected_error = (
            "Cannot apply 'NOT BETWEEN ASYMMETRIC' to arguments of type"
        )


# [NOT] IN
class illarg_in_legal(TstView):
    def __init__(self):
        # checked manually
        self.data = [
            {
                "intt": True,
                "decimall": True,
                "reall": True,
                "dbl": True,
                "str": True,
                "bin": True,
                "booll": True,
                "tmestmp": True,
                "datee": True,
                "tme": True,
                "uuidd": True,
                "arr": True,
                "mapp": True,
            }
        ]
        self.sql = """CREATE MATERIALIZED VIEW in_legal AS SELECT
                      intt IN (-12) AS intt,
                      decimall IN (-1111.52) AS decimall,
                      reall IN (-57681.18) AS reall,
                      dbl IN (-38.2711234601246) AS dbl,
                      str IN ('hello ') AS str,
                      X'0B1620' IN (X'0B1620') AS bin,
                      booll IN (True) AS booll,
                      tmestmp IN (TIMESTAMP '2020-06-21 14:23:44.123') AS tmestmp,
                      datee IN (DATE '2020-06-21') AS datee,
                      tme IN (TIME '14:23:44.456') AS tme,
                      uuidd IN (UUID '42b8fec7-c7a3-4531-9611-4bde80f9cb4c') AS uuidd,
                      arr IN (ARRAY['bye', '14', 'See you!', '-0.52', NULL, '14', 'hello ']) AS arr,
                      mapp IN (MAP['a', 12, 'b', 17]) AS mapp
                      FROM illegal_tbl
                      WHERE id = 0"""


class illarg_not_in_legal(TstView):
    def __init__(self):
        # checked manually
        self.data = [
            {
                "intt": False,
                "decimall": False,
                "reall": False,
                "dbl": False,
                "str": False,
                "bin": False,
                "booll": False,
                "tmestmp": False,
                "datee": False,
                "tme": False,
                "uuidd": False,
                "arr": False,
                "mapp": False,
            }
        ]
        self.sql = """CREATE MATERIALIZED VIEW not_in_legal AS SELECT
                      intt NOT IN (-12) AS intt,
                      decimall NOT IN (-1111.52) AS decimall,
                      reall NOT IN (-57681.18) AS reall,
                      dbl NOT IN (-38.2711234601246) AS dbl,
                      str NOT IN ('hello ') AS str,
                      X'0B1620' NOT IN (X'0B1620') AS bin,
                      booll NOT IN (True) AS booll,
                      tmestmp NOT IN (TIMESTAMP '2020-06-21 14:23:44.123') AS tmestmp,
                      datee NOT IN (DATE '2020-06-21') AS datee,
                      tme NOT IN (TIME '14:23:44.456') AS tme,
                      uuidd NOT IN (UUID '42b8fec7-c7a3-4531-9611-4bde80f9cb4c') AS uuidd,
                      arr NOT IN (ARRAY['bye', '14', 'See you!', '-0.52', NULL, '14', 'hello ']) AS arr,
                      mapp NOT IN (MAP['a', 12, 'b', 17]) AS mapp
                      FROM illegal_tbl
                      WHERE id = 0"""


# Negative Tests
class illarg_not_in_illegal(TstView):
    def __init__(self):
        # checked manually
        self.sql = """CREATE MATERIALIZED VIEW not_in_illegal AS SELECT
                      mapp NOT IN (ARRAY['bye', '14', 'See you!', '-0.52', NULL, '14', 'hello ']) AS mapp
                      FROM illegal_tbl
                      WHERE id = 1"""
        self.expected_error = (
            "Values passed to NOT IN operator must have compatible types"
        )


class illarg_in_illegal(TstView):
    def __init__(self):
        # checked manually
        self.sql = """CREATE MATERIALIZED VIEW in_illegal AS SELECT
                      arr IN (MAP['a', 12, 'b', 17]) AS arr
                      FROM illegal_tbl
                      WHERE id = 1"""
        self.expected_error = "Values passed to IN operator must have compatible types"


# Exists
class illarg_exists_legal(TstView):
    def __init__(self):
        # checked manually
        self.data = [{"exists": True}]
        self.sql = """CREATE MATERIALIZED VIEW exists_legal AS SELECT
                      EXISTS (SELECT 1
                      FROM illegal_tbl) AS exists"""


# CASE_value_WHEN
class illarg_case_val_when_intt_legal(TstView):
    def __init__(self):
        # checked manually
        self.data = [
            {"id": 0, "intt": 0},
            {"id": 1, "intt": 1},
            {"id": 2, "intt": None},
        ]
        self.sql = """CREATE MATERIALIZED VIEW case_val_when_int_legal AS SELECT
                      id,
                      CASE intt
                          WHEN -12 THEN 0
                          WHEN -1 THEN 1
                          ELSE NULL
                      END AS intt
                      FROM illegal_tbl"""


class illarg_case_val_when_dec_legal(TstView):
    def __init__(self):
        # checked manually
        self.data = [{"id": 0, "dec": 0}, {"id": 1, "dec": 1}, {"id": 2, "dec": None}]
        self.sql = """CREATE MATERIALIZED VIEW case_val_when_dec_legal AS SELECT
                      id,
                      CASE decimall
                          WHEN -1111.52 THEN 0
                          WHEN -0.52 THEN 1
                          ELSE NULL
                      END AS dec
                      FROM illegal_tbl"""


class illarg_case_val_when_reall_legal(TstView):
    def __init__(self):
        # checked manually
        self.data = [
            {"id": 0, "reall": 0},
            {"id": 1, "reall": 1},
            {"id": 2, "reall": None},
        ]
        self.sql = """CREATE MATERIALIZED VIEW case_val_when_reall_legal AS SELECT
                      id,
                      CASE reall
                          WHEN -57681.18 THEN 0
                          WHEN -0.1234567 THEN 1
                          ELSE NULL
                      END AS reall
                      FROM illegal_tbl"""


class illarg_case_val_when_dbl_legal(TstView):
    def __init__(self):
        # checked manually
        self.data = [{"id": 0, "dbl": 0}, {"id": 1, "dbl": 1}, {"id": 2, "dbl": None}]
        self.sql = """CREATE MATERIALIZED VIEW case_val_when_dbl_legal AS SELECT
                      id,
                      CASE dbl
                          WHEN -38.2711234601246 THEN 0
                          WHEN -0.82711234601246 THEN 1
                          ELSE NULL
                      END AS dbl
                      FROM illegal_tbl"""


class illarg_case_val_when_bool_legal(TstView):
    def __init__(self):
        # checked manually
        self.data = [
            {"id": 0, "booll": 0},
            {"id": 1, "booll": 1},
            {"id": 2, "booll": None},
        ]
        self.sql = """CREATE MATERIALIZED VIEW case_val_when_bool_legal AS SELECT
                      id,
                      CASE booll
                          WHEN True THEN 0
                          WHEN False THEN 1
                          ELSE NULL
                      END AS booll
                      FROM illegal_tbl"""


class illarg_case_val_when_str_legal(TstView):
    def __init__(self):
        # checked manually
        self.data = [{"id": 0, "str": 0}, {"id": 1, "str": 1}, {"id": 2, "str": None}]
        self.sql = """CREATE MATERIALIZED VIEW case_val_when_str_legal AS SELECT
                      id,
                      CASE str
                          WHEN 'hello ' THEN 0
                          WHEN '0.12'THEN 1
                          ELSE NULL
                      END AS str
                      FROM illegal_tbl"""


class illarg_case_val_when_bin_legal(TstView):
    def __init__(self):
        # checked manually
        self.data = [{"id": 0, "bin": 0}, {"id": 1, "bin": 1}, {"id": 2, "bin": None}]
        self.sql = """CREATE MATERIALIZED VIEW case_val_when_bin_legal AS SELECT
                      id,
                      CASE bin
                          WHEN '0b1620' THEN 0
                          WHEN '1f8b08000000000000ff4b4bcd49492d4a0400218115ac07000000' THEN 1
                          ELSE NULL
                      END AS bin
                      FROM illegal_tbl"""


class illarg_case_val_when_tme_legal(TstView):
    def __init__(self):
        # checked manually
        self.data = [{"id": 0, "tme": 0}, {"id": 1, "tme": 1}, {"id": 2, "tme": None}]
        self.sql = """CREATE MATERIALIZED VIEW case_val_when_tme_legal AS SELECT
                      id,
                      CASE tme
                          WHEN '14:23:44.456' THEN 0
                          WHEN '14:23:44' THEN 1
                          ELSE NULL
                      END AS tme
                      FROM illegal_tbl"""


class illarg_case_val_when_tmestmp_legal(TstView):
    def __init__(self):
        # checked manually
        self.data = [
            {"id": 0, "tmestmp": 0},
            {"id": 1, "tmestmp": 1},
            {"id": 2, "tmestmp": None},
        ]
        self.sql = """CREATE MATERIALIZED VIEW case_val_when_tmestmp_legal AS SELECT
                      id,
                      CASE tmestmp
                          WHEN '2020-06-21 14:23:44.123' THEN 0
                          WHEN '2020-06-21 14:23:44' THEN 1
                          ELSE NULL
                      END AS tmestmp
                      FROM illegal_tbl"""


class illarg_case_val_when_datee_legal(TstView):
    def __init__(self):
        # checked manually
        self.data = [
            {"id": 0, "datee": "01"},
            {"id": 1, "datee": "01"},
            {"id": 2, "datee": None},
        ]
        self.sql = """CREATE MATERIALIZED VIEW case_val_when_datee_legal AS SELECT
                      id,
                      CASE datee
                          WHEN '2020-06-21' THEN '01'
                          ELSE NULL
                      END AS datee
                      FROM illegal_tbl"""


class illarg_case_val_when_uuidd_legal(TstView):
    def __init__(self):
        # checked manually
        self.data = [
            {"id": 0, "uuidd": "01"},
            {"id": 1, "uuidd": "01"},
            {"id": 2, "uuidd": None},
        ]
        self.sql = """CREATE MATERIALIZED VIEW case_val_when_uuidd_legal AS SELECT
                      id,
                      CASE uuidd
                          WHEN '42b8fec7-c7a3-4531-9611-4bde80f9cb4c' THEN '01'
                          ELSE NULL
                      END AS uuidd
                      FROM illegal_tbl"""


class illarg_case_val_when_arr_legal(TstView):
    def __init__(self):
        # checked manually
        self.data = [{"id": 0, "arr": 0}, {"id": 1, "arr": 1}, {"id": 2, "arr": None}]
        self.sql = """CREATE MATERIALIZED VIEW case_val_when_arr_legal AS SELECT
                      id,
                      CASE arr
                        WHEN ARRAY['bye', '14', 'See you!', '-0.52', NULL, '14', 'hello '] THEN 0
                        WHEN ARRAY['-0.14', 'friends', 'See you!', '2021-01-20', '10:10', '2020-10-01 00:00:00'] THEN 1
                        ELSE NULL
                      END AS arr
                      FROM illegal_tbl"""


class illarg_case_val_when_mapp_legal(TstView):
    def __init__(self):
        # checked manually
        self.data = [
            {"id": 0, "mapp": 0},
            {"id": 1, "mapp": 1},
            {"id": 2, "mapp": None},
        ]
        self.sql = """CREATE MATERIALIZED VIEW case_val_when_mapp_legal AS SELECT
                      id,
                      CASE mapp
                          WHEN MAP['a', 12, 'b', 17] THEN 0
                          WHEN MAP['a', 15, 'b', NULL] THEN 1
                          ELSE NULL
                      END AS mapp
                      FROM illegal_tbl"""


# Negative Test
class illarg_case_val_when_mapp_illegal(TstView):
    def __init__(self):
        # checked manually
        self.data = [
            {"id": 0, "mapp": 0},
            {"id": 1, "mapp": 1},
            {"id": 2, "mapp": None},
        ]
        self.sql = """CREATE MATERIALIZED VIEW case_val_when_mapp_illegal AS SELECT
                      id,
                      CASE mapp
                          WHEN ARRAY['bye', '14', 'See you!', '-0.52', NULL, '14', 'hello '] THEN 0
                          WHEN ARRAY['-0.14', 'friends', 'See you!', '2021-01-20', '10:10', '2020-10-01 00:00:00'] THEN 1
                          ELSE NULL
                      END AS mapp
                      FROM illegal_tbl"""
        self.expected_error = "Cannot apply '=' to arguments of type"


# CASE WHEN
class illarg_case_when_legal(TstView):
    def __init__(self):
        # checked manually
        self.data = [
            {
                "intt": True,
                "decimall": True,
                "reall": True,
                "dbl": True,
                "booll": True,
                "str": True,
                "bin": True,
                "tmestmp": True,
                "datee": True,
                "tme": True,
                "uuidd": True,
                "arr": True,
                "mapp": True,
            }
        ]
        self.sql = """CREATE MATERIALIZED VIEW case_when_legal AS SELECT
                      CASE WHEN intt = -12 THEN True ELSE NULL END AS intt,
                      CASE WHEN decimall = -1111.52 THEN True ELSE NULL END AS decimall,
                      CASE WHEN reall = -57681.18 THEN True ELSE NULL END AS reall,
                      CASE WHEN dbl = -38.2711234601246 THEN True ELSE NULL END AS dbl,
                      CASE WHEN booll = True THEN True ELSE NULL END AS booll,
                      CASE WHEN str = 'hello ' THEN True ELSE NULL END AS str,
                      CASE WHEN bin = X'0B1620' THEN True ELSE NULL END AS bin,
                      CASE WHEN tmestmp = TIMESTAMP '2020-06-21 14:23:44.123' THEN True ELSE NULL END AS tmestmp,
                      CASE WHEN datee = DATE '2020-06-21' THEN True ELSE NULL END AS datee,
                      CASE WHEN tme = TIME '14:23:44.456' THEN True ELSE NULL END AS tme,
                      CASE WHEN uuidd = UUID '42b8fec7-c7a3-4531-9611-4bde80f9cb4c' THEN True ELSE NULL END AS uuidd,
                      CASE WHEN arr = ARRAY['bye', '14', 'See you!', '-0.52', NULL, '14', 'hello '] THEN True ELSE NULL END AS arr,
                      CASE WHEN mapp = MAP['a', 12, 'b', 17] THEN True ELSE NULL END AS mapp
                      FROM illegal_tbl
                      WHERE id = 0"""


# Negative Test
class illarg_case_when_illegal(TstView):
    def __init__(self):
        # checked manually
        self.sql = """CREATE MATERIALIZED VIEW case_when_illegal AS SELECT
                      CASE WHEN mapp = ARRAY['bye', '14', 'See you!', '-0.52', NULL, '14', 'hello '] THEN True ELSE NULL END AS mapp
                      FROM illegal_tbl
                      WHERE id = 0"""
        self.expected_error = "Cannot apply '=' to arguments of type"


# COALESCE
class illarg_coalesce_legal(TstView):
    def __init__(self):
        # checked manually
        self.data = [
            {
                "intt": -12,
                "decimall": Decimal("-1111.52"),
                "reall": Decimal("-57681.18"),
                "dbl": Decimal("-38.2711234601246"),
                "booll": True,
                "str": "hello ",
                "bin": "0b1620",
                "tmestmp": "2020-06-21T14:23:44.123",
                "datee": "2020-06-21",
                "tme": "14:23:44.456",
                "uuidd": "42b8fec7-c7a3-4531-9611-4bde80f9cb4c",
            }
        ]
        self.sql = """CREATE MATERIALIZED VIEW coalesce_legal AS SELECT
                      COALESCE(NULL, intt, -1) intt,
                      COALESCE(NULL, decimall, -1) AS decimall,
                      COALESCE(NULL, reall, -1) AS reall,
                      COALESCE(NULL, dbl, -1) AS dbl,
                      COALESCE(NULL, booll, False) AS booll,
                      COALESCE(NULL, str, -1) AS str,
                      COALESCE(NULL, bin, X'0B1620') AS bin,
                      COALESCE(NULL, tmestmp, TIMESTAMP '2020-06-21 14:23:44') AS tmestmp,
                      COALESCE(NULL, datee, DATE '2020-06-21') AS datee,
                      COALESCE(NULL, tme, TIME '14:23:44') AS tme,
                      COALESCE(NULL, uuidd, UUID '42b8fec7-c7a3-4531-9611-4bde80f9cb4c') AS uuidd
                      FROM illegal_tbl
                      WHERE id = 0"""


# Negative Test
class illarg_coalesce_illegal(TstView):
    def __init__(self):
        # checked manually
        self.sql = """CREATE MATERIALIZED VIEW coalesce_legal AS SELECT
                      COALESCE(NULL, uuidd, X'0B1620') AS uuidd
                      FROM illegal_tbl
                      WHERE id = 0"""
        self.expected_error = "Parameters must be of the same type"


# GREATEST
class illarg_greatest_legal(TstView):
    def __init__(self):
        # checked manually
        self.data = [
            {
                "intt": -1,
                "decimall": Decimal("-0.52"),
                "reall": Decimal("-0.1234567"),
                "dbl": Decimal("-0.82711234601246"),
                "booll": True,
                "str": "hello ",
                "bin": "1f8b080000000000ff4b4bcd49492d4a0400218115ac07000000",
                "tmestmp": "2020-06-21T14:23:44.123",
                "datee": "2020-06-21",
                "tme": "14:23:44.456",
                "uuidd": "42b8fec7-c7a3-4531-9611-4bde80f9cb4c",
                "arr": ["ciao"],
                "mapp": {"a": 13, "b": 17},
            }
        ]
        self.sql = """CREATE MATERIALIZED VIEW greatest_legal AS SELECT
                        GREATEST(intt, -1) AS intt,
                        GREATEST(decimall, -0.52) AS decimall,
                        GREATEST(reall, -0.1234567) AS reall,
                        GREATEST(dbl, -0.82711234601246) AS dbl,
                        GREATEST(booll, FALSE) AS booll,
                        GREATEST(str, '0.12') AS str,
                        GREATEST(bin, X'1F8B080000000000FF4B4BCD49492D4A0400218115AC07000000') AS bin,
                        GREATEST(tmestmp, TIMESTAMP '2020-06-21 14:23:44') AS tmestmp,
                        GREATEST(datee, DATE '2020-06-21') AS datee,
                        GREATEST(tme, TIME '14:23:44') AS tme,
                        GREATEST(uuidd, UUID '42b8fec7-c7a3-4531-9611-4bde80f9cb4c') AS uuidd,
                        GREATEST(arr, ARRAY['ciao']) AS arr,
                        GREATEST(mapp, MAP['a', 13, 'b', 17]) AS mapp
                      FROM illegal_tbl
                      WHERE id = 0"""


# Negative Tests
class illarg_greatest_illegal(TstView):
    def __init__(self):
        # checked manually
        self.sql = """CREATE MATERIALIZED VIEW greatest_illegal AS SELECT
                      GREATEST(arr, MAP['a', 13, 'b', 17]) AS arr
                      FROM illegal_tbl
                      WHERE id = 0"""
        self.expected_error = "Parameters must be of the same type"


class illarg_greatest_illegal1(TstView):
    def __init__(self):
        # checked manually
        self.sql = """CREATE MATERIALIZED VIEW greatest_illegal1 AS SELECT
                      GREATEST(str, False) AS str
                      FROM illegal_tbl
                      WHERE id = 0"""
        self.expected_error = (
            "cannot infer return type for greatest; operand types: [varchar, boolean]"
        )


# GREATEST_IGNORE_NULLS
class illarg_greatest_ignore_nulls_legal(TstView):
    def __init__(self):
        # checked manually
        self.data = [
            {
                "intt": -1,
                "decimall": Decimal("-0.52"),
                "reall": Decimal("-0.1234567"),
                "dbl": Decimal("-0.82711234601246"),
                "booll": True,
                "str": "hello ",
                "bin": "1f8b080000000000ff4b4bcd49492d4a0400218115ac07000000",
                "tmestmp": "2020-06-21T14:23:44.123",
                "datee": "2020-06-21",
                "tme": "14:23:44.456",
                "uuidd": "42b8fec7-c7a3-4531-9611-4bde80f9cb4c",
                "arr": ["ciao"],
                "mapp": {"a": 13, "b": 17},
            }
        ]
        self.sql = """CREATE MATERIALIZED VIEW greatest_ignore_nulls_legal AS SELECT
                        GREATEST_IGNORE_NULLS(NULL, intt, -1) AS intt,
                        GREATEST_IGNORE_NULLS(NULL, decimall, -0.52) AS decimall,
                        GREATEST_IGNORE_NULLS(NULL, reall, -0.1234567) AS reall,
                        GREATEST_IGNORE_NULLS(NULL, dbl, -0.82711234601246) AS dbl,
                        GREATEST_IGNORE_NULLS(NULL, booll, FALSE) AS booll,
                        GREATEST_IGNORE_NULLS(NULL, str, '0.12') AS str,
                        GREATEST_IGNORE_NULLS(NULL, bin, X'1F8B080000000000FF4B4BCD49492D4A0400218115AC07000000') AS bin,
                        GREATEST_IGNORE_NULLS(NULL, tmestmp, TIMESTAMP '2020-06-21 14:23:44') AS tmestmp,
                        GREATEST_IGNORE_NULLS(NULL, datee, DATE '2020-06-21') AS datee,
                        GREATEST_IGNORE_NULLS(NULL, tme, TIME '14:23:44') AS tme,
                        GREATEST_IGNORE_NULLS(NULL, uuidd, UUID '42b8fec7-c7a3-4531-9611-4bde80f9cb4c') AS uuidd,
                        GREATEST_IGNORE_NULLS(arr, ARRAY['ciao']) AS arr,
                        GREATEST_IGNORE_NULLS(mapp, MAP['a', 13, 'b', 17]) AS mapp
                      FROM illegal_tbl
                      WHERE id = 0"""


# Negative Test
class illarg_greatest_ignore_nulls_illegal(TstView):
    def __init__(self):
        # checked manually
        self.sql = """CREATE MATERIALIZED VIEW greatest_ignore_nulls_illegal AS SELECT
                      GREATEST_IGNORE_NULLS(mapp, ARRAY['apple']) AS mapp
                      FROM illegal_tbl
                      WHERE id = 0"""
        self.expected_error = "Parameters must be of the same type"


# LEAST
class illarg_least_legal(TstView):
    def __init__(self):
        # checked manually
        self.data = [
            {
                "intt": -12,
                "decimall": Decimal("-1111.52"),
                "reall": Decimal("-57681.1796875"),
                "dbl": Decimal("-38.2711234601246"),
                "booll": False,
                "str": "0.12",
                "bin": "0b1620",
                "tmestmp": "2020-06-21T14:23:44",
                "datee": "2020-06-21",
                "tme": "14:23:44",
                "uuidd": "42b8fec7-c7a3-4531-9611-4bde80f9cb4c",
                "arr": ["apple"],
                "mapp": {"a": 12, "b": 17},
            }
        ]
        self.sql = """CREATE MATERIALIZED VIEW least_legal AS SELECT
                        LEAST(intt, -1) AS intt,
                        LEAST(decimall, -0.52) AS decimall,
                        LEAST(reall, -0.1234567) AS reall,
                        LEAST(dbl, -0.82711234601246) AS dbl,
                        LEAST(booll, FALSE) AS booll,
                        LEAST(str, '0.12') AS str,
                        LEAST(bin, X'1F8B080000000000FF4B4BCD49492D4A0400218115AC07000000') AS bin,
                        LEAST(tmestmp, TIMESTAMP '2020-06-21 14:23:44') AS tmestmp,
                        LEAST(datee, DATE '2020-06-21') AS datee,
                        LEAST(tme, TIME '14:23:44') AS tme,
                        LEAST(uuidd, UUID '42b8fec7-c7a3-4531-9611-4bde80f9cb4c') AS uuidd,
                        LEAST(arr, ARRAY['apple']) AS arr,
                        LEAST(mapp, MAP['a', 13, 'b', 17]) AS mapp
                      FROM illegal_tbl
                      WHERE id = 0"""


# Negative Test
class illarg_least_illegal(TstView):
    def __init__(self):
        # checked manually
        self.sql = """CREATE MATERIALIZED VIEW least_illegal AS SELECT
                      LEAST(mapp, ARRAY['apple']) AS mapp
                      FROM illegal_tbl
                      WHERE id = 0"""
        self.expected_error = "Parameters must be of the same type"


# LEAST_IGNORE_NULLS
class illarg_least_ignore_nulls_legal(TstView):
    def __init__(self):
        # checked manually
        self.data = [
            {
                "intt": -12,
                "decimall": Decimal("-1111.52"),
                "reall": Decimal("-57681.1796875"),
                "dbl": Decimal("-38.2711234601246"),
                "booll": False,
                "str": "0.12",
                "bin": "0b1620",
                "tmestmp": "2020-06-21T14:23:44",
                "datee": "2020-06-21",
                "tme": "14:23:44",
                "uuidd": "42b8fec7-c7a3-4531-9611-4bde80f9cb4c",
                "arr": ["apple"],
                "mapp": {"a": 12, "b": 17},
            }
        ]
        self.sql = """CREATE MATERIALIZED VIEW least_ignore_nulls_legal AS SELECT
                        LEAST_IGNORE_NULLS(NULL, intt, -1) AS intt,
                        LEAST_IGNORE_NULLS(NULL, decimall, -0.52) AS decimall,
                        LEAST_IGNORE_NULLS(NULL, reall, -0.1234567) AS reall,
                        LEAST_IGNORE_NULLS(NULL, dbl, -0.82711234601246) AS dbl,
                        LEAST_IGNORE_NULLS(NULL, booll, FALSE) AS booll,
                        LEAST_IGNORE_NULLS(NULL, str, '0.12') AS str,
                        LEAST_IGNORE_NULLS(NULL, bin, X'1F8B080000000000FF4B4BCD49492D4A0400218115AC07000000') AS bin,
                        LEAST_IGNORE_NULLS(NULL, tmestmp, TIMESTAMP '2020-06-21 14:23:44') AS tmestmp,
                        LEAST_IGNORE_NULLS(NULL, datee, DATE '2020-06-21') AS datee,
                        LEAST_IGNORE_NULLS(NULL, tme, TIME '14:23:44') AS tme,
                        LEAST_IGNORE_NULLS(NULL, uuidd, UUID '42b8fec7-c7a3-4531-9611-4bde80f9cb4c') AS uuidd,
                        LEAST_IGNORE_NULLS(NULL, arr, ARRAY['apple']) AS arr,
                        LEAST_IGNORE_NULLS(NULL, mapp, MAP['a', 13, 'b', 17]) AS mapp
                      FROM illegal_tbl
                      WHERE id = 0"""


# Negative Test
class illarg_least_ignore_nulls_illegal(TstView):
    def __init__(self):
        # checked manually
        self.sql = """CREATE MATERIALIZED VIEW least_ignore_nulls_illegal AS SELECT
                      LEAST_IGNORE_NULLS(NULL, uuidd, X'0B1620') AS uuidd
                      FROM illegal_tbl
                      WHERE id = 0"""
        self.expected_error = "Parameters must be of the same type"


# IF
class illarg_if_legal(TstView):
    def __init__(self):
        # checked manually
        self.data = [
            {
                "intt": "correct",
                "decimall": "correct",
                "reall": "correct",
                "dbl": "correct",
                "booll": "incorrect",
                "str": "incorrect",
                "bin": "correct",
                "tmestmp": "incorrect",
                "datee": "correct",
                "tme": "incorrect",
                "uuidd": "correct",
                "arr": "incorrect",
                "mapp": "correct",
            }
        ]
        self.sql = """CREATE MATERIALIZED VIEW if_legal AS SELECT
                        IF(intt <= -12, 'correct', 'incorrect') AS intt,
                        IF(decimall <= -0.52, 'correct', 'incorrect') AS decimall,
                        IF(reall <= -0.1234567, 'correct', 'incorrect') AS reall,
                        IF(dbl <= -0.82711234601246, 'correct', 'incorrect') AS dbl,
                        IF(booll <= FALSE, 'correct', 'incorrect') AS booll,
                        IF(str <= '0.12', 'correct', 'incorrect') AS str,
                        IF(bin <= X'1F8B080000000000FF4B4BCD49492D4A0400218115AC07000000', 'correct', 'incorrect') AS bin,
                        IF(tmestmp <= TIMESTAMP '2020-06-21 14:23:44', 'correct', 'incorrect') AS tmestmp,
                        IF(datee <= DATE '2020-06-21', 'correct', 'incorrect') AS datee,
                        IF(tme <= TIME '14:23:44', 'correct', 'incorrect') AS tme,
                        IF(uuidd  <= UUID'42b8fec7-c7a3-4531-9611-4bde80f9cb4c', 'correct', 'incorrect') AS uuidd,
                        IF(arr <= ARRAY['apple'], 'correct', 'incorrect') AS arr,
                        IF(mapp <= MAP['a', 13, 'b', 17], 'correct', 'incorrect') AS mapp
                      FROM illegal_tbl
                      WHERE id = 0"""


# Negative Test
class illarg_if_illegal(TstView):
    def __init__(self):
        # checked manually
        self.sql = """CREATE MATERIALIZED VIEW if_illegal AS SELECT
                        IF(mapp <= ARRAY['apple'], 'correct', 'incorrect') AS mapp
                      FROM illegal_tbl
                      WHERE id = 0"""
        self.expected_error = "Cannot apply '<=' to arguments of type"


# NULLIF
class illarg_nullif_legal(TstView):
    def __init__(self):
        # checked manually
        self.data = [
            {
                "intt": None,
                "decimall": Decimal("-1111.52"),
                "reall": Decimal("-57681.18"),
                "dbl": Decimal("-38.2711234601246"),
                "booll": True,
                "str": "hello ",
                "bin": "0b1620",
                "tmestmp": "2020-06-21T14:23:44.123",
                "datee": None,
                "tme": "14:23:44.456",
                "uuidd": None,
                "arr": ["bye", "14", "See you!", "-0.52", None, "14", "hello "],
                "mapp": {"a": 12, "b": 17},
            }
        ]
        self.sql = """CREATE MATERIALIZED VIEW nullif_legal AS SELECT
                        NULLIF(intt, -12) AS intt,
                        NULLIF(decimall, -0.52) AS decimall,
                        NULLIF(reall, -0.1234567) AS reall,
                        NULLIF(dbl, -0.82711234601246) AS dbl,
                        NULLIF(booll, FALSE) AS booll,
                        NULLIF(str, '0.12') AS str,
                        NULLIF(bin, X'1F8B080000000000FF4B4BCD49492D4A0400218115AC07000000') AS bin,
                        NULLIF(tmestmp, TIMESTAMP '2020-06-21 14:23:44') AS tmestmp,
                        NULLIF(datee, DATE '2020-06-21') AS datee,
                        NULLIF(tme, TIME '14:23:44') AS tme,
                        NULLIF(uuidd, UUID'42b8fec7-c7a3-4531-9611-4bde80f9cb4c') AS uuidd,
                        NULLIF(arr, ARRAY['apple']) AS arr,
                        NULLIF(mapp, MAP['a', 13, 'b', 17]) AS mapp
                      FROM illegal_tbl
                      WHERE id = 0"""


# Negative Test
class illarg_nullif_illegal(TstView):
    def __init__(self):
        # checked manually
        self.sql = """CREATE MATERIALIZED VIEW nullif_illegal AS SELECT
                        NULLIF(mapp, ARRAY['apple']) AS mapp
                      FROM illegal_tbl
                      WHERE id = 0"""
        self.expected_error = "Cannot apply 'NULLIF' to arguments of type"
