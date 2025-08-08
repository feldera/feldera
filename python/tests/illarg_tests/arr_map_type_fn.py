from tests.aggregate_tests.aggtst_base import TstView


# INDEX function
class illarg_index_legal(TstView):
    def __init__(self):
        # checked manually
        self.data = [{"arr": "14", "arr2": "See you!"}]
        self.sql = """CREATE MATERIALIZED VIEW index_legal AS SELECT
                      arr[2] AS arr,
                      arr[SAFE_OFFSET(2)] AS arr2
                      FROM illegal_tbl
                      WHERE id = 0"""


# Negative Test
class illarg_index_illegal(TstView):
    def __init__(self):
        # checked manually
        self.sql = """CREATE MATERIALIZED VIEW index_illegal AS SELECT
                      bin[2] AS bin
                      FROM illegal_tbl"""
        self.expected_error = "Cannot apply 'ITEM' to arguments of type"


# ARRAY() function
class illarg_arr_construct_legal(TstView):
    def __init__(self):
        # checked manually
        self.data = [
            {"arr": ["-12", "-1111.52", "-57681.18", "-38.2711234601246", "hello "]}
        ]
        self.sql = """CREATE MATERIALIZED VIEW arr_construct_legal AS SELECT
                      ARRAY(intt, decimall, reall, dbl, str) AS arr
                      FROM illegal_tbl
                      WHERE id = 0"""


# Negative Test
class illarg_arr_construct_illegal(TstView):
    def __init__(self):
        # checked manually
        self.sql = """CREATE MATERIALIZED VIEW arr_construct_illegal AS SELECT
                      ARRAY(arr, uuidd)  AS bin
                      FROM illegal_tbl"""
        self.expected_error = "Parameters must be of the same type"


# ARRAY_CONCAT function
class illarg_arr_concat_legal(TstView):
    def __init__(self):
        # checked manually
        self.data = [
            {
                "arr": [
                    "bye",
                    "14",
                    "See you!",
                    "-0.52",
                    None,
                    "14",
                    "hello ",
                    "bye",
                    "14",
                    "See you!",
                    "-0.52",
                    None,
                    "14",
                    "hello ",
                ]
            }
        ]
        self.sql = """CREATE MATERIALIZED VIEW arr_concat_legal AS SELECT
                      ARRAY_CONCAT(arr, arr) AS arr
                      FROM illegal_tbl
                      WHERE id = 0"""


# Negative Test
class illarg_arr_concat_illegal(TstView):
    def __init__(self):
        # checked manually
        self.sql = """CREATE MATERIALIZED VIEW arr_concat_illegal AS SELECT
                      ARRAY_CONCAT(str, str)  AS str
                      FROM illegal_tbl"""
        self.expected_error = "Cannot apply 'ARRAY_CONCAT' to arguments of type"


# ARRAY_APPEND function
class illarg_arr_append_legal(TstView):
    def __init__(self):
        # checked manually
        self.data = [
            {"arr": ["bye", "14", "See you!", "-0.52", None, "14", "hello ", "hello "]}
        ]
        self.sql = """CREATE MATERIALIZED VIEW arr_append_legal AS SELECT
                      ARRAY_APPEND(arr, str) AS arr
                      FROM illegal_tbl
                      WHERE id = 0"""


# Negative Test
class illarg_arr_append_illegal(TstView):
    def __init__(self):
        # checked manually
        self.sql = """CREATE MATERIALIZED VIEW arr_append_illegal AS SELECT
                      ARRAY_APPEND(arr, intt)  AS arr
                      FROM illegal_tbl"""
        self.expected_error = "VARCHAR is not comparable to INTEGER"


# ARRAY_COMPACT function -> fails
class illarg_arr_compact_legal(TstView):
    def __init__(self):
        # checked manually
        self.data = [{"arr": ["bye", "14", "See you!", "-0.52", "14", "hello "]}]
        self.sql = """CREATE MATERIALIZED VIEW arr_compact_legal AS SELECT
                      ARRAY_COMPACT(arr) AS arr
                      FROM illegal_tbl
                      WHERE id = 0"""


# Negative Test
class illarg_arr_compact_illegal(TstView):
    def __init__(self):
        # checked manually
        self.sql = """CREATE MATERIALIZED VIEW arr_compact_illegal AS SELECT
                      ARRAY_COMPACT(str)  AS str
                      FROM illegal_tbl"""
        self.expected_error = "Cannot apply 'ARRAY_COMPACT' to arguments of type"


# ARRAY_CONTAINS function
class illarg_arr_contains_legal(TstView):
    def __init__(self):
        # checked manually
        self.data = [{"arr": True}]
        self.sql = """CREATE MATERIALIZED VIEW arr_contains_legal AS SELECT
                      ARRAY_CONTAINS(arr, 'hello ') AS arr
                      FROM illegal_tbl
                      WHERE id = 0"""


# Negative Test
class illarg_arr_contains_illegal(TstView):
    def __init__(self):
        # checked manually
        self.sql = """CREATE MATERIALIZED VIEW arr_contains_illegal AS SELECT
                      ARRAY_CONTAINS(arr, intt)  AS str
                      FROM illegal_tbl"""
        self.expected_error = "VARCHAR is not comparable to INTEGER"


# ARRAY_DISTINCT function
class illarg_arr_distinct_legal(TstView):
    def __init__(self):
        # checked manually
        self.data = [{"arr": ["bye", "14", "See you!", "-0.52", None, "hello "]}]
        self.sql = """CREATE MATERIALIZED VIEW arr_distinct_legal AS SELECT
                      ARRAY_DISTINCT(arr) AS arr
                      FROM illegal_tbl
                      WHERE id = 0"""


# Negative Test
class illarg_arr_distinct_illegal(TstView):
    def __init__(self):
        # checked manually
        self.sql = """CREATE MATERIALIZED VIEW arr_distinct_illegal AS SELECT
                      ARRAY_DISTINCT(str)  AS str
                      FROM illegal_tbl"""
        self.expected_error = "Cannot apply 'ARRAY_DISTINCT' to arguments of type"


# ARRAY_EXCEPT function
class illarg_arr_except_legal(TstView):
    def __init__(self):
        # checked manually
        self.data = [
            {
                "arr": [None, "-0.52", "14", "See you!", "bye"],
                "arr1": [None, "-0.52", "14", "See you!", "bye"],
            }
        ]
        self.sql = """CREATE MATERIALIZED VIEW arr_except_legal AS SELECT
                      ARRAY_EXCEPT(arr, ARRAY[str]) AS arr,
                      ARRAY_EXCEPT(arr, ARRAY['hello ']) AS arr1
                      FROM illegal_tbl
                      WHERE id = 0"""


# Negative Test
class illarg_arr_except_illegal(TstView):
    def __init__(self):
        # checked manually
        self.sql = """CREATE MATERIALIZED VIEW arr_except_illegal AS SELECT
                      ARRAY_EXCEPT(arr, str)  AS str
                      FROM illegal_tbl"""
        self.expected_error = "Cannot apply 'ARRAY_EXCEPT' to arguments of type"


# ARRAY_EXISTS function
class illarg_arr_exists_legal(TstView):
    def __init__(self):
        # checked manually
        self.data = [{"arr": True}]
        self.sql = """CREATE MATERIALIZED VIEW arr_exists_legal AS SELECT
                      ARRAY_EXISTS(arr, x -> x = 'hello ')  AS arr
                      FROM illegal_tbl
                      WHERE id = 0"""


# Negative Test
class illarg_arr_exists_illegal(TstView):
    def __init__(self):
        # checked manually
        self.sql = """CREATE MATERIALIZED VIEW arr_exists_illegal AS SELECT
                      ARRAY_EXISTS(str, x -> x = '14')  AS str
                      FROM illegal_tbl"""
        self.expected_error = "Cannot apply 'ARRAY_EXISTS' to arguments of type"


# ARRAY_INSERT function
class illarg_arr_insert_legal(TstView):
    def __init__(self):
        # checked manually
        self.data = [
            {"arr": ["bye", "14", "See you!", "-0.52", None, "14", "hello ", "fred"]}
        ]
        self.sql = """CREATE MATERIALIZED VIEW arr_insert_legal AS SELECT
                      ARRAY_INSERT(arr, 8, 'fred') AS arr
                      FROM illegal_tbl
                      WHERE id = 0"""


# Negative Test
class illarg_arr_insert_illegal(TstView):
    def __init__(self):
        # checked manually
        self.sql = """CREATE MATERIALIZED VIEW arr_insert_illegal AS SELECT
                      ARRAY_INSERT(arr, 8, 4)  AS str
                      FROM illegal_tbl"""
        self.expected_error = "VARCHAR is not comparable to INTEGER"


# ARRAY_INTERSECT function
class illarg_arr_intersect_legal(TstView):
    def __init__(self):
        # checked manually
        self.data = [{"arr": ["hello "]}]
        self.sql = """CREATE MATERIALIZED VIEW arr_intersect_legal AS SELECT
                      ARRAY_INTERSECT(arr, ARRAY['hello ']) AS arr
                      FROM illegal_tbl
                      WHERE id = 0"""


# Negative Test
class illarg_arr_intersect_illegal(TstView):
    def __init__(self):
        # checked manually
        self.sql = """CREATE MATERIALIZED VIEW arr_intersect_illegal AS SELECT
                      ARRAY_INTERSECT(str, ARRAY['hello '])  AS str
                      FROM illegal_tbl"""
        self.expected_error = "Cannot apply 'ARRAY_INTERSECT' to arguments of type"


# ARRAY_JOIN function
class illarg_arr_join_legal(TstView):
    def __init__(self):
        # checked manually
        self.data = [{"arr": "bye,14,See you!,-0.52,*,14,hello "}]
        self.sql = """CREATE MATERIALIZED VIEW arr_join_legal AS SELECT
                      ARRAY_JOIN(arr,',', '*') AS arr
                      FROM illegal_tbl
                      WHERE id = 0"""


# Negative Test
class illarg_arr_join_illegal(TstView):
    def __init__(self):
        # checked manually
        self.sql = """CREATE MATERIALIZED VIEW arr_join_illegal AS SELECT
                      ARRAY_JOIN(str, ',', '*')  AS str
                      FROM illegal_tbl"""
        self.expected_error = "Cannot apply 'ARRAY_JOIN' to arguments of type"


# ARRAY_TO_STRING function
class illarg_arr_to_string_legal(TstView):
    def __init__(self):
        # checked manually
        self.data = [{"arr": "bye,14,See you!,-0.52,*,14,hello "}]
        self.sql = """CREATE MATERIALIZED VIEW arr_to_string_legal AS SELECT
                      ARRAY_JOIN(arr,',', '*') AS arr
                      FROM illegal_tbl
                      WHERE id = 0"""


# Negative Test
class illarg_arr_to_string_illegal(TstView):
    def __init__(self):
        # checked manually
        self.sql = """CREATE MATERIALIZED VIEW arr_to_string_illegal AS SELECT
                      ARRAY_TO_STRING(str, ',', '*')  AS str
                      FROM illegal_tbl"""
        self.expected_error = "Cannot apply 'ARRAY_TO_STRING' to arguments of type"


# ARRAY_LENGTH function
class illarg_arr_length_legal(TstView):
    def __init__(self):
        # checked manually
        self.data = [{"arr": 7}]
        self.sql = """CREATE MATERIALIZED VIEW arr_length_legal AS SELECT
                      ARRAY_LENGTH(arr) AS arr
                      FROM illegal_tbl
                      WHERE id = 0"""


# Negative Test
class illarg_arr_length_illegal(TstView):
    def __init__(self):
        # checked manually
        self.sql = """CREATE MATERIALIZED VIEW arr_length_illegal AS SELECT
                      ARRAY_LENGTH(str)  AS str
                      FROM illegal_tbl"""
        self.expected_error = "Cannot apply 'ARRAY_LENGTH' to arguments of type"


# CARDINALITY function
class illarg_arr_cardinality_legal(TstView):
    def __init__(self):
        # checked manually
        self.data = [{"arr": 7}]
        self.sql = """CREATE MATERIALIZED VIEW arr_cardinality_legal AS SELECT
                      CARDINALITY(arr) AS arr
                      FROM illegal_tbl
                      WHERE id = 0"""


# Negative Test
class illarg_arr_cardinality_illegal(TstView):
    def __init__(self):
        # checked manually
        self.sql = """CREATE MATERIALIZED VIEW arr_cardinality_illegal AS SELECT
                      CARDINALITY(str)  AS str
                      FROM illegal_tbl"""
        self.expected_error = "Cannot apply 'CARDINALITY' to arguments of type"


# ARRAY_MAX function
class illarg_arr_max_legal(TstView):
    def __init__(self):
        # checked manually
        self.data = [{"arr": "hello "}]
        self.sql = """CREATE MATERIALIZED VIEW arr_max_legal AS SELECT
                      ARRAY_MAX(arr) AS arr
                      FROM illegal_tbl
                      WHERE id = 0"""


# Negative Test
class illarg_arr_max_illegal(TstView):
    def __init__(self):
        # checked manually
        self.sql = """CREATE MATERIALIZED VIEW arr_max_illegal AS SELECT
                      ARRAY_MAX(str)  AS str
                      FROM illegal_tbl"""
        self.expected_error = "Cannot apply 'ARRAY_MAX' to arguments of type"


# ARRAY_MIN function
class illarg_arr_min_legal(TstView):
    def __init__(self):
        # checked manually
        self.data = [{"arr": "-0.52"}]
        self.sql = """CREATE MATERIALIZED VIEW arr_min_legal AS SELECT
                      ARRAY_MIN(arr) AS arr
                      FROM illegal_tbl
                      WHERE id = 0"""


# Negative Test
class illarg_arr_min_illegal(TstView):
    def __init__(self):
        # checked manually
        self.sql = """CREATE MATERIALIZED VIEW arr_min_illegal AS SELECT
                      ARRAY_MIN(str)  AS str
                      FROM illegal_tbl"""
        self.expected_error = "Cannot apply 'ARRAY_MIN' to arguments of type"


# ARRAYS_OVERLAP function
class illarg_arr_overlap_legal(TstView):
    def __init__(self):
        # checked manually
        self.data = [{"arr": True}]
        self.sql = """CREATE MATERIALIZED VIEW arr_overlap_legal AS SELECT
                      ARRAYS_OVERLAP(arr, ARRAY[str]) AS arr
                      FROM illegal_tbl
                      WHERE id = 0"""


# Negative Test
class illarg_arr_overlap_illegal(TstView):
    def __init__(self):
        # checked manually
        self.sql = """CREATE MATERIALIZED VIEW arr_overlap_illegal AS SELECT
                      ARRAYS_OVERLAP(str, ARRAY['hello '])  AS str
                      FROM illegal_tbl"""
        self.expected_error = "Cannot apply 'ARRAYS_OVERLAP' to arguments of type"


# ARRAY_POSITION function
class illarg_arr_pos_legal(TstView):
    def __init__(self):
        # checked manually
        self.data = [{"arr": 3}]
        self.sql = """CREATE MATERIALIZED VIEW arr_pos_legal AS SELECT
                      ARRAY_POSITION(arr, 'See you!') AS arr
                      FROM illegal_tbl
                      WHERE id = 0"""


# Negative Test
class illarg_arr_pos_illegal(TstView):
    def __init__(self):
        # checked manually
        self.sql = """CREATE MATERIALIZED VIEW arr_pos_illegal AS SELECT
                      ARRAY_POSITION(str, 'See you!')  AS str
                      FROM illegal_tbl"""
        self.expected_error = "Cannot apply 'ARRAY_POSITION' to arguments of type"


# ARRAY_PREPEND function
class illarg_arr_prepend_legal(TstView):
    def __init__(self):
        # checked manually
        self.data = [
            {"arr": ["friend", "bye", "14", "See you!", "-0.52", None, "14", "hello "]}
        ]
        self.sql = """CREATE MATERIALIZED VIEW arr_prepend_legal AS SELECT
                      ARRAY_PREPEND(arr, 'friend') AS arr
                      FROM illegal_tbl
                      WHERE id = 0"""


# Negative Test
class illarg_arr_prepend_illegal(TstView):
    def __init__(self):
        # checked manually
        self.sql = """CREATE MATERIALIZED VIEW arr_prepend_illegal AS SELECT
                      ARRAY_PREPEND(str, 'friend')  AS str
                      FROM illegal_tbl"""
        self.expected_error = "Cannot apply 'ARRAY_PREPEND' to arguments of type"


# ARRAY_REMOVE function
class illarg_arr_remove_legal(TstView):
    def __init__(self):
        # checked manually
        self.data = [{"arr": ["bye", "14", "-0.52", None, "14", "hello "]}]
        self.sql = """CREATE MATERIALIZED VIEW arr_remove_legal AS SELECT
                      ARRAY_REMOVE(arr, 'See you!') AS arr
                      FROM illegal_tbl
                      WHERE id = 0"""


# Negative Test
class illarg_arr_remove_illegal(TstView):
    def __init__(self):
        # checked manually
        self.sql = """CREATE MATERIALIZED VIEW arr_remove_illegal AS SELECT
                      ARRAY_REMOVE(str, 'See you!')  AS str
                      FROM illegal_tbl"""
        self.expected_error = "Cannot apply 'ARRAY_REMOVE' to arguments of type"


# ARRAY_REVERSE function
class illarg_arr_reverse_legal(TstView):
    def __init__(self):
        # checked manually
        self.data = [{"arr": ["hello ", "14", None, "-0.52", "See you!", "14", "bye"]}]
        self.sql = """CREATE MATERIALIZED VIEW arr_reverse_legal AS SELECT
                      ARRAY_REVERSE(arr) AS arr
                      FROM illegal_tbl
                      WHERE id = 0"""


# Negative Test
class illarg_arr_reverse_illegal(TstView):
    def __init__(self):
        # checked manually
        self.sql = """CREATE MATERIALIZED VIEW arr_reverse_illegal AS SELECT
                      ARRAY_REVERSE(str)  AS str
                      FROM illegal_tbl"""
        self.expected_error = "Cannot apply 'ARRAY_REVERSE' to arguments of type"


# ARRAY_REPEAT function (passes for all ARRAY_REPEAT(dtype, integer)
class illarg_arr_repeat_legal(TstView):
    def __init__(self):
        # checked manually
        self.data = [
            {
                "arr": [
                    ["bye", "14", "See you!", "-0.52", None, "14", "hello "],
                    ["bye", "14", "See you!", "-0.52", None, "14", "hello "],
                ]
            }
        ]
        self.sql = """CREATE MATERIALIZED VIEW arr_repeat_legal AS SELECT
                      ARRAY_REPEAT(arr, 2) AS arr
                      FROM illegal_tbl
                      WHERE id = 0"""


class illarg_arr_repeat_dtype_legal(TstView):
    def __init__(self):
        # checked manually
        self.data = [{"str": ["0.12", "0.12"]}, {"str": ["hello ", "hello "]}]
        self.sql = """CREATE MATERIALIZED VIEW arr_repeat_dtype_legal AS SELECT
                      ARRAY_REPEAT(str, 2)  AS str
                      FROM illegal_tbl"""


class illarg_arr_repeat_dtype_illegal(TstView):
    def __init__(self):
        # checked manually
        self.sql = """CREATE MATERIALIZED VIEW arr_repeat_dtype_illegal AS SELECT
                      ARRAY_REPEAT(str, False)  AS str
                      FROM illegal_tbl"""
        self.expected_error = "Cannot apply 'ARRAY_REPEAT' to arguments of type 'ARRAY_REPEAT(<VARCHAR>, <BOOLEAN>)'"


# ARRAY_UNION function
class illarg_arr_union_legal(TstView):
    def __init__(self):
        # checked manually
        self.data = [
            {"arr": [None, "-0.52", "14", "See you!", "bye", "friend", "hello "]}
        ]
        self.sql = """CREATE MATERIALIZED VIEW arr_union_legal AS SELECT
                      ARRAY_UNION(arr, ARRAY['friend']) AS arr
                      FROM illegal_tbl
                      WHERE id = 0"""


# Negative Test
class illarg_arr_union_illegal(TstView):
    def __init__(self):
        # checked manually
        self.sql = """CREATE MATERIALIZED VIEW arr_union_illegal AS SELECT
                      ARRAY_UNION(str, ARRAY['friend'])  AS str
                      FROM illegal_tbl"""
        self.expected_error = "Cannot apply 'ARRAY_UNION' to arguments of type"


# ELEMENT function
class illarg_arr_element_legal(TstView):
    def __init__(self):
        # checked manually
        self.data = [{"arr": 2}]
        self.sql = """CREATE MATERIALIZED VIEW arr_element_legal AS SELECT
                      ELEMENT(ARRAY[2]) AS arr
                      FROM illegal_tbl
                      WHERE id = 0"""


# Negative Test
class illarg_arr_element_illegal(TstView):
    def __init__(self):
        # checked manually
        self.sql = """CREATE MATERIALIZED VIEW arr_element_illegal AS SELECT
                      ELEMENT(arr[2])  AS str
                      FROM illegal_tbl"""
        self.expected_error = "Cannot apply 'ELEMENT' to arguments of type"


# SORT_ARRAY function
class illarg_sort_arr_legal(TstView):
    def __init__(self):
        # checked manually
        self.data = [{"arr": ["hello ", "bye", "See you!", "14", "14", "-0.52", None]}]
        self.sql = """CREATE MATERIALIZED VIEW sort_arr_legal AS SELECT
                      SORT_ARRAY(arr, False) AS arr
                      FROM illegal_tbl
                      WHERE id = 0"""


# Negative Test
class illarg_sort_arr_illegal(TstView):
    def __init__(self):
        # checked manually
        self.sql = """CREATE MATERIALIZED VIEW sort_arr_illegal AS SELECT
                      SORT_ARRAY(str)  AS str
                      FROM illegal_tbl"""
        self.expected_error = "Cannot apply 'SORT_ARRAY' to arguments of type"


# TRANSFORM function
class illarg_transform_arr_legal(TstView):
    def __init__(self):
        # checked manually
        self.data = [{"arr": ["BYE", "14", "SEE YOU!", "-0.52", None, "14", "HELLO "]}]
        self.sql = """CREATE MATERIALIZED VIEW transform_arr_legal AS SELECT
                      TRANSFORM(arr, x -> UPPER(X)) AS arr
                      FROM illegal_tbl
                      WHERE id = 0"""


# Negative Test
class illarg_transform_arr_illegal(TstView):
    def __init__(self):
        # checked manually
        self.sql = """CREATE MATERIALIZED VIEW transform_arr_illegal AS SELECT
                      TRANSFORM(str, x -> UPPER(x))  AS str
                      FROM illegal_tbl"""
        self.expected_error = "Cannot apply 'TRANSFORM' to arguments of type"


# MAP Tests
# MAP function
class illarg_map_item_legal(TstView):
    def __init__(self):
        # checked manually
        self.data = [{"mapp": 12}]
        self.sql = """CREATE MATERIALIZED VIEW map_item_legal AS SELECT
                      mapp['a'] AS mapp
                      FROM illegal_tbl
                      WHERE id = 0"""


# Negative Test
class illarg_map_item_illegal(TstView):
    def __init__(self):
        # checked manually
        self.sql = """CREATE MATERIALIZED VIEW map_item_illegal AS SELECT
                      bin['a']  AS str
                      FROM illegal_tbl"""
        self.expected_error = "Cannot apply 'ITEM' to arguments of type"


# CARDINALITY function
class illarg_cardinality_map_legal(TstView):
    def __init__(self):
        # checked manually
        self.data = [{"mapp": 2}]
        self.sql = """CREATE MATERIALIZED VIEW cardinality_map_legal AS SELECT
                      CARDINALITY(mapp) AS mapp
                      FROM illegal_tbl
                      WHERE id = 0"""


# Negative Test
class illarg_cardinality_map_illegal(TstView):
    def __init__(self):
        # checked manually
        self.sql = """CREATE MATERIALIZED VIEW cardinality_map_illegal AS SELECT
                      CARDINALITY(bin)  AS str
                      FROM illegal_tbl"""
        self.expected_error = "Cannot apply 'CARDINALITY' to arguments of type"


# MAP_CONTAINS_KEY function
class illarg_map_contains_key_legal(TstView):
    def __init__(self):
        # checked manually
        self.data = [{"mapp": True}]
        self.sql = """CREATE MATERIALIZED VIEW map_contains_key_legal AS SELECT
                      MAP_CONTAINS_KEY(mapp, 'a') AS mapp
                      FROM illegal_tbl
                      WHERE id = 0"""


# Negative Test
class illarg_map_contains_key_illegal(TstView):
    def __init__(self):
        # checked manually
        self.sql = """CREATE MATERIALIZED VIEW map_contains_key_illegal AS SELECT
                      MAP_CONTAINS_KEY(bin, 'a')  AS str
                      FROM illegal_tbl"""
        self.expected_error = "Cannot apply 'MAP_CONTAINS_KEY' to arguments of type"
