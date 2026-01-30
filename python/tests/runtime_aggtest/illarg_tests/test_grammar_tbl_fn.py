from tests.runtime_aggtest.aggtst_base import TstView


# AS
class illarg_as_legal(TstView):
    def __init__(self):
        self.data = [{"intt": -12}]
        self.sql = """CREATE MATERIALIZED VIEW as_legal AS SELECT
                      intt AS intt
                      FROM illegal_tbl
                      WHERE id = 0"""


# Negative test
class illarg_as_illegal(TstView):
    def __init__(self):
        self.sql = """CREATE MATERIALIZED VIEW as_illegal AS SELECT
                      intt AS
                      FROM illegal_tbl
                      WHERE id = 0"""
        self.expected_error = "Incorrect syntax near the keyword 'AS'"


# CUBE
class illarg_cube_legal(TstView):
    def __init__(self):
        self.data = [
            {"str": "0.12", "cnt": 1},
            {"str": "hello ", "cnt": 1},
            {"str": None, "cnt": 1},
            {"str": None, "cnt": 3},
        ]
        self.sql = """CREATE MATERIALIZED VIEW cube_legal AS SELECT
                      str, COUNT(*) AS cnt
                      FROM illegal_tbl
                      GROUP BY CUBE (str)"""


# Negative test
class illarg_cube_illegal(TstView):
    def __init__(self):
        self.sql = """CREATE MATERIALIZED VIEW cube_illegal AS SELECT
                      str, COUNT(*) AS cnt
                      FROM illegal_tbl
                      GROUP BY CUBE (CUBE(str))"""
        self.expected_error = "No match found for function signature cube(<CHARACTER>)"


# ROLLUP
class illarg_rollup_legal(TstView):
    def __init__(self):
        self.data = [
            {"str": "0.12", "cnt": 1},
            {"str": "hello ", "cnt": 1},
            {"str": None, "cnt": 1},
            {"str": None, "cnt": 3},
        ]
        self.sql = """CREATE MATERIALIZED VIEW rollup_legal AS SELECT
                      str, COUNT(*) AS cnt
                      FROM illegal_tbl
                      GROUP BY ROLLUP (str)"""


# Negative test
class illarg_rollup_illegal(TstView):
    def __init__(self):
        self.sql = """CREATE MATERIALIZED VIEW rollup_illegal AS SELECT
                      str, COUNT(*) AS cnt
                      FROM illegal_tbl
                      GROUP BY ROLLUP (ROLLUP(str))"""
        self.expected_error = "Incorrect syntax near the keyword 'ROLLUP'"


# ASC/DESC
class illarg_asc_legal(TstView):
    def __init__(self):
        self.data = [{"intt": -12}, {"intt": -1}, {"intt": None}]
        self.sql = """CREATE MATERIALIZED VIEW asc_legal AS SELECT
                      intt
                      FROM illegal_tbl
                      ORDER BY intt ASC"""


class illarg_desc_legal(TstView):
    def __init__(self):
        self.data = [{"intt": -12}, {"intt": -1}, {"intt": None}]
        self.sql = """CREATE MATERIALIZED VIEW desc_legal AS SELECT
                      intt
                      FROM illegal_tbl
                      ORDER BY intt DESC"""


# Negative test
class illarg_asc_desc_illegal(TstView):
    def __init__(self):
        self.sql = """CREATE MATERIALIZED VIEW asc_desc_illegal AS SELECT
                      *
                      FROM illegal_tbl
                      ORDER BY intt ASC str DESC"""
        self.expected_error = "Error parsing SQL"


# NULLS FIRST/LAST
class illarg_nulls_first_legal(TstView):
    def __init__(self):
        self.data = [{"intt": -12}, {"intt": -1}, {"intt": None}]
        self.sql = """CREATE MATERIALIZED VIEW nulls_first_legal AS SELECT
                      intt
                      FROM illegal_tbl
                      ORDER BY str NULLS FIRST"""


class illarg_nulls_last_legal(TstView):
    def __init__(self):
        self.data = [{"intt": -12}, {"intt": -1}, {"intt": None}]
        self.sql = """CREATE MATERIALIZED VIEW nulls_last_legal AS SELECT
                      intt
                      FROM illegal_tbl
                      ORDER BY str NULLS LAST"""


# Negative Test
class illarg_nulls_first_last_illegal(TstView):
    def __init__(self):
        self.sql = """CREATE MATERIALIZED VIEW nulls_first_last_illegal AS SELECT
                      intt
                      FROM illegal_tbl
                      ORDER BY NULLS FIRST, NULLS LAST"""
        self.expected_error = "Error parsing SQL"


# TUMBLE
class illarg_tumble_legal(TstView):
    def __init__(self):
        self.data = [
            {"tmestmp": "2020-06-21T14:23:44"},
            {"tmestmp": "2020-06-21T14:23:44.123"},
        ]
        self.sql = """CREATE MATERIALIZED VIEW tumble_legal AS SELECT tmestmp FROM TABLE(
                      TUMBLE(TABLE illegal_tbl, DESCRIPTOR(tmestmp), INTERVAL '1' MINUTE))"""


# Negative Test
class illarg_tumble_illegal(TstView):
    def __init__(self):
        self.sql = """CREATE MATERIALIZED VIEW tumble_illegal AS SELECT * FROM TABLE(
                      TUMBLE(TABLE illegal_tbl, DESCRIPTOR(intt), INTERVAL '1' MINUTE))"""
        self.expected_error = "Cannot apply 'TUMBLE' to arguments of type"


# HOP
class illarg_hop_legal(TstView):
    def __init__(self):
        self.data = [
            {"tmestmp": "2020-06-21T14:23:44"},
            {"tmestmp": "2020-06-21T14:23:44"},
            {"tmestmp": "2020-06-21T14:23:44.123"},
            {"tmestmp": "2020-06-21T14:23:44.123"},
        ]
        self.sql = """CREATE MATERIALIZED VIEW hop_legal AS SELECT tmestmp FROM TABLE(
                      HOP(TABLE illegal_tbl, DESCRIPTOR(tmestmp), INTERVAL '1' MINUTE, INTERVAL '2' MINUTE))"""


# Negative Test
class illarg_hop_illegal(TstView):
    def __init__(self):
        self.sql = """CREATE MATERIALIZED VIEW hop_illegal AS SELECT * FROM TABLE(
                      HOP(TABLE illegal_tbl, DESCRIPTOR(intt), INTERVAL '1' MINUTE, INTERVAL '2' MINUTE))"""
        self.expected_error = "Cannot apply 'HOP' to arguments of type"


# GROUPING
class illarg_grouping_legal(TstView):
    def __init__(self):
        self.data = [
            {"g_id": 0, "g_intt": 0},
            {"g_id": 0, "g_intt": 0},
            {"g_id": 0, "g_intt": 0},
            {"g_id": 1, "g_intt": 1},
        ]
        self.sql = """CREATE MATERIALIZED VIEW grouping_legal AS SELECT
                      GROUPING(id) AS g_id,
                      GROUPING(intt) AS g_intt
                      FROM illegal_tbl
                      GROUP BY
                      GROUPING SETS ((id, intt),())"""


# Negative Test
class illarg_grouping_illegal(TstView):
    def __init__(self):
        self.sql = """CREATE MATERIALIZED VIEW grouping_illegal AS SELECT
                      arr,
                      COUNT(*) AS c,
                      GROUPING(arr[1]) AS g
                      FROM illegal_tbl
                      GROUP BY CUBE(arr)"""
        self.expected_error = (
            "Argument to GROUPING operator must be a grouped expression"
        )


# UNION
class illarg_union_legal(TstView):
    def __init__(self):
        self.data = [{"intt": -12}, {"intt": -1}, {"intt": None}]
        self.sql = """CREATE MATERIALIZED VIEW union_legal AS SELECT
                      intt FROM illegal_tbl
                      UNION
                      SELECT intt FROM illegal_tbl"""


# Negative Test
class illarg_union_illegal(TstView):
    def __init__(self):
        self.sql = """CREATE MATERIALIZED VIEW union_illegal AS SELECT
                      mapp FROM illegal_tbl
                      UNION
                      SELECT MAP(ARRAY['a'], ARRAY[intt]) FROM illegal_tbl"""
        self.expected_error = "Type mismatch in column 1 of UNION"


# UNION ALL
class illarg_union_all_legal(TstView):
    def __init__(self):
        self.data = [
            {"intt": -12},
            {"intt": -12},
            {"intt": -1},
            {"intt": -1},
            {"intt": None},
            {"intt": None},
        ]
        self.sql = """CREATE MATERIALIZED VIEW union_all_legal AS SELECT
                      intt FROM illegal_tbl
                      UNION ALL
                      SELECT intt FROM illegal_tbl"""


class illarg_union_all_illegal(TstView):
    def __init__(self):
        self.sql = """CREATE MATERIALIZED VIEW union_all_illegal AS SELECT
                      uuidd FROM illegal_tbl
                      UNION ALL
                      SELECT CAST('hello ' AS UUID)
                      FROM illegal_tbl"""
        self.expected_error = "invalid character: expected an optional prefix of `urn:uuid:` followed by [0-9a-fA-F-], found `h` at 1"


# INTERSECT
class illarg_intersect_legal(TstView):
    def __init__(self):
        self.data = [{"str": "0.12"}, {"str": "hello "}, {"str": None}]
        self.sql = """CREATE MATERIALIZED VIEW intersect_legal AS SELECT
                      str FROM illegal_tbl
                      INTERSECT
                      SELECT str
                      FROM illegal_tbl"""


# Negative Test
class illarg_intersect_illegal(TstView):
    def __init__(self):
        self.data = []
        self.sql = """CREATE MATERIALIZED VIEW intersect_illegal AS SELECT
                      mapp FROM illegal_tbl
                      INTERSECT
                      SELECT arr
                      FROM illegal_tbl"""
        self.expected_error = "Type mismatch in column 1 of INTERSECT"


# INTERSECT ALL
# Negative Test
class illarg_intersect_all_illegal(TstView):
    def __init__(self):
        self.sql = """CREATE MATERIALIZED VIEW intersect_all_illegal AS SELECT
                      str FROM illegal_tbl
                      INTERSECT ALL
                      SELECT booll
                      FROM illegal_tbl"""
        self.expected_error = "Not yet implemented"


# MINUS
# Negative Test
class illarg_minus_illegal(TstView):
    def __init__(self):
        self.data = []
        self.sql = """CREATE MATERIALIZED VIEW minus_illegal AS SELECT
                      roww FROM illegal_tbl
                      MINUS
                      SELECT arr FROM illegal_tbl"""
        self.expected_error = "Type mismatch in column 1 of EXCEPT"


# MINUS ALL
# Negative Test
class illarg_minus_all_illegal(TstView):
    def __init__(self):
        self.data = []
        self.sql = """CREATE MATERIALIZED VIEW minus_all_illegal AS SELECT
                      arr FROM illegal_tbl
                      MINUS ALL
                      SELECT arr FROM illegal_tbl"""
        self.expected_error = "Not yet implemented"


# VALUES
class illarg_values_legal_int(TstView):
    def __init__(self):
        self.data = [{"intt": 1}, {"intt": 2}, {"intt": None}]
        self.sql = """CREATE MATERIALIZED VIEW values_legal AS SELECT
                      *
                      FROM (VALUES (1), (2), (NULL)) AS v(intt)"""


class illarg_values_illegal(TstView):
    def __init__(self):
        self.sql = """CREATE MATERIALIZED VIEW values_illegal AS SELECT
                      *
                      FROM (VALUES (1), (1, 2)) AS v(a)"""
        self.expected_error = (
            "Values passed to values operator must have compatible types"
        )


# DOT
class illarg_dot_legal(TstView):
    def __init__(self):
        self.data = [{"intt": -12}, {"intt": -1}, {"intt": None}]
        self.sql = """CREATE MATERIALIZED VIEW dot_int_legal AS SELECT
                      illegal_tbl.intt
                      FROM illegal_tbl"""


#  Negative Test
class illarg_dot_illegal(TstView):
    def __init__(self):
        self.sql = """CREATE MATERIALIZED VIEW dot_illegal AS SELECT
                      illegal_tbl.foo
                      FROM illegal_tbl"""
        self.expected_error = "Column 'foo' not found in table 'illegal_tbl'"


# IGNORE/RESPECT_NULLS
class illarg_ignore_respect_nulls_legal(TstView):
    def __init__(self):
        self.data = [{"arr_int": [-12, -1], "arr_int_respect": [None, -12, -1]}]
        self.sql = """CREATE MATERIALIZED VIEW ignore_respect_nulls_legal AS SELECT
                      ARRAY_AGG(intt IGNORE NULLS) AS arr_int,
                      ARRAY_AGG(intt RESPECT NULLS) AS arr_int_respect
                      FROM illegal_tbl"""


# Negative Test
class illarg_ignore_nulls_illegal(TstView):
    def __init__(self):
        self.sql = """CREATE MATERIALIZED VIEW ignore_nulls_illegal AS SELECT
                      SUM(intt) IGNORE NULLS
                      FROM illegal_tbl"""
        self.expected_error = (
            "Cannot specify IGNORE NULLS or RESPECT NULLS following 'SUM'"
        )


# LATERAL
class illarg_lateral_legal(TstView):
    def __init__(self):
        self.data = [{"id": 0, "val": -11}, {"id": 1, "val": 0}, {"id": 2, "val": None}]
        self.sql = """CREATE MATERIALIZED VIEW lateral_legal AS SELECT
                      t1.id, x.val
                      FROM illegal_tbl t1,
                           LATERAL (SELECT t1.intt + 1 AS val) AS x"""


# Negative Test
class illarg_lateral_illegal(TstView):
    def __init__(self):
        self.sql = """CREATE MATERIALIZED VIEW lateral_illegal AS SELECT
                      *
                      FROM illegal_tbl t,
                            LATERAL(1 + 2)"""
        self.expected_error = "Error parsing SQL"


# GROUPING_SET
class illarg_grouping_set_legal(TstView):
    def __init__(self):
        self.data = [
            {"intt": -1, "max_intt": -1},
            {"intt": -12, "max_intt": -12},
            {"intt": None, "max_intt": None},
        ]
        self.sql = """CREATE MATERIALIZED VIEW grouping_set_legal AS SELECT
                      intt, MAX(intt) as max_intt
                      FROM illegal_tbl
                      GROUP BY GROUPING SETS (intt)
                      """


# Negative Test
class illarg_grouping_set_illegal(TstView):
    def __init__(self):
        self.sql = """CREATE MATERIALIZED VIEW grouping_set_illegal AS SELECT
                      str, COUNT(*)
                      FROM illegal_tbl
                      GROUP BY GROUPING SETS (str + 1)"""
        self.expected_error = "Expression 'str' is not being grouped"


# OVER
class illarg_over_legal(TstView):
    def __init__(self):
        self.data = [
            {"id": 0, "sum_int": -13},
            {"id": 1, "sum_int": -13},
            {"id": 2, "sum_int": -13},
        ]
        self.sql = """CREATE MATERIALIZED VIEW over_legal AS SELECT
                      id, SUM(intt) OVER () AS sum_int
                      FROM illegal_tbl"""


# Negative Test
class illarg_over_illegal(TstView):
    def __init__(self):
        self.sql = """CREATE MATERIALIZED VIEW over_illegal AS SELECT
                      COUNT(*) OVER (intt)
                      FROM illegal_tbl
                      GROUP BY GROUPING SETS (str + 1)"""
        self.expected_error = "Window 'intt' not found"
