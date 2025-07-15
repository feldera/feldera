package org.dbsp.sqlCompiler.compiler.sql.simple;

import org.dbsp.sqlCompiler.compiler.sql.tools.SqlIoTest;
import org.junit.Test;

public class ArrayFunctionsTests extends SqlIoTest {
    @Test
    public void testArrayAppend() {
        this.qs("""
                SELECT array_append(ARRAY [1, 2], null::int);
                 array_append
                --------------
                 {1, 2, NULL}
                (1 row)

                SELECT array_append(ARRAY [1, 2], 3);
                 array_append
                --------------
                 {1, 2, 3}
                (1 row)

                SELECT array_append(ARRAY [1, 2], 3.5);
                 array_append
                --------------
                 {1, 2, 3.5}
                (1 row)

                SELECT array_append(ARRAY [NULL], 1);
                 array_append
                --------------
                 {NULL, 1}
                (1 row)
                """
        );
    }

    @Test
    public void testArrayConcat() {
        this.statementsFailingInCompilation("CREATE VIEW V AS SELECT array_concat()",
                "Invalid number of arguments to function");
        this.qs("""
                SELECT array_concat(array[1, 2], array[2, 3]);
                 r
                ---
                 {1, 2, 2, 3}
                (1 row)
                
                SELECT array_concat(array[1, 2], array[2, null]);
                 r
                ---
                 {1, 2, 2, NULL}
                (1 row)
               
                SELECT array_concat(array['hello', 'world'], array['!'], array[cast(null as char)]);
                 r
                ---
                 { hello, world, !,NULL}
                (1 row)
               
                SELECT array_concat(cast(null as integer array), array[1]);
                 r
                ---
                NULL
                (1 row)""");
    }

    @Test
    public void testArrayExcept() {
        this.qs("""
                SELECT array_except(array[2, 3, 3], array[2]);
                 r
                ---
                 { 3 }
                (1 row)
                
                SELECT array_except(array[2], array[2, 3]);
                 r
                ---
                 {}
                (1 row)
                
                SELECT array_except(array[2, null, 3, 3], array[1, 2, null]);
                 r
                ---
                 {3}
                (1 row)
                
                SELECT array_except(cast(null as integer array), array[1]);
                 r
                ---
                NULL
                (1 row)
                
                SELECT array_except(array[1], cast(null as integer array));
                 r
                ---
                NULL
                (1 row)
                
                SELECT array_except(cast(null as integer array), cast(null as integer array));
                 r
                ---
                NULL
                (1 row)""");
    }
    
    @Test
    public void testArrayUnion() {
        this.qs("""
                select array_union(array[2, 3, 3], array[3]);
                 r
                ---
                 {2,3}
                (1 row)
                
                select array_union(array[2, null, 2], array[1, 2, null]);
                 r
                ---
                {NULL,1,2}
                (1 row)
                
                select array_union(cast(null as integer array), array[1]);
                 r
                ---
                NULL
                (1 row)
                
                select array_union(array[1], cast(null as integer array));
                 r
                ---
                NULL
                (1 row)
               
                select array_union(cast(null as integer array), cast(null as integer array));
                 r
                ---
                NULL
                (1 row)""");
    }

    @Test
    public void testArrayRepeat() {
        this.qs("""
                SELECT array_repeat(3, 3);
                 array_repeat
                --------------
                 {3,3,3}
                (1 row)

                SELECT array_repeat(2.1, 3);
                 array_repeat
                --------------
                 {2.1, 2.1, 2.1}
                (1 row)

                SELECT array_repeat(3, -1);
                 array_repeat
                --------------
                 {}
                (1 row)

                SELECT array_repeat(null, -1);
                 array_repeat
                --------------
                 {}
                (1 row)

                SELECT array_repeat(null, 3);
                 array_repeat
                --------------
                 {null,null,null}
                (1 row)
                """
        );
    }

    @Test
    public void testArrayRepeat2() {
        this.qs("""
                SELECT array_repeat(123, null);
                 array_repeat
                --------------
                 NULL
                (1 row)
                """
        );
    }

    @Test
    public void testArrayRepeat3() {
        this.qs("""
                SELECT array_repeat('a', 6);
                 array_repeat
                --------------
                 { a, a, a, a, a, a}
                (1 row)
                """
        );
    }

    @Test
    public void testArrayPosition() {
        this.qs("""
                SELECT array_position(ARRAY [2, 4, 6, 8, null], null);
                 array_position
                ----------------
                 NULL
                (1 row)

                SELECT array_position(ARRAY [2, 4, 6, 8, null], 4);
                 array_position
                ----------------
                 2
                (1 row)

                SELECT array_position(ARRAY [2, 4, 6, 8], 4);
                 array_position
                ----------------
                 2
                (1 row)

                SELECT array_position(ARRAY [2, 4, 6, 8, null], 3);
                 array_position
                ----------------
                 0
                (1 row)

                SELECT array_position(ARRAY [2, 4, 6, 8], 3);
                 array_position
                ----------------
                 0
                (1 row)

                SELECT array_position(ARRAY [2, 4, 6, 8], null);
                 array_position
                ----------------
                 NULL
                (1 row)

                SELECT array_position(ARRAY [null], 1);
                 array_position
                ----------------
                 0
                (1 row)
                """
        );
    }


    @Test
    public void testArrayContains() {
        this.qs("""
            SELECT array_contains(ARRAY [2, 4, 6, 8, null], 4);
             array_contains
            ---------------
             true
            (1 row)

            SELECT array_contains(ARRAY [2, 4, 6, 8], 4);
             array_contains
            ---------------
             true
            (1 row)

            SELECT array_contains(ARRAY [2, 4, 6, 8, null], 3);
             array_contains
            ---------------
             false
            (1 row)

            SELECT array_contains(ARRAY [2, 4, 6, 8], 3);
             array_contains
            ---------------
             false
            (1 row)

            SELECT array_contains(ARRAY [2, 4, 6, 8], null);
             array_contains
            ---------------
             NULL
            (1 row)

            SELECT array_contains(ARRAY [null, 2, 2, 6, 6, 8, 2], null);
             array_contains
            ----------------
             NULL
            (1 row)
            """
        );
    }

    @Test
    public void testArrayPositionDiffTypes() {
        this.qs("""
                SELECT array_position(ARRAY [1, 2, 3, 4], 1e0);
                 result
                --------
                 1
                (1 row)
                
                SELECT array_position(ARRAY [1.0, 2.0, 3.0, 4.0], 1e0);
                 result
                --------
                 1
                (1 row)
                
                SELECT array_position(ARRAY [1.0, 2.0, 3.0, 4.0], 0e0);
                 result
                --------
                 0
                (1 row)""");
    }

    @Test
    public void testArrayContainsDiffTypes() {
        this.qs("""
                SELECT array_contains(ARRAY [1, 2, 3, 4], 1e0);
                 result
                --------
                 true
                (1 row)
                
                SELECT array_contains(ARRAY [1.0, 2.0, 3.0, 4.0], 1e0);
                 result
                --------
                 true
                (1 row)
                
                SELECT array_contains(ARRAY [1.0, 2.0, 3.0, 4.0], 0e0);
                 result
                --------
                 false
                (1 row)""");
    }

    @Test
    public void testArrayRemoveDiffTypes() {
        this.qs("""
                SELECT array_remove(ARRAY [1, 2, 3, 4], 1e0);
                 result
                --------
                 {2,3,4}
                (1 row)
                
                SELECT array_remove(ARRAY [1.0, 2.0, 3.0, 4.0], 1e0);
                 result
                --------
                 {2,3,4}
                (1 row)
                
                SELECT array_remove(ARRAY [1.0, 2.0, 3.0, 4.0], 0e0);
                 result
                --------
                 {1,2,3,4}
                (1 row)""");
    }

    @Test
    public void testArrayRemove() {
        this.qs("""
                SELECT array_remove(ARRAY [2, 2, 6, 6, 8, 2], 2);
                 array_remove
                --------------
                 {6, 6, 8}
                (1 row)

                SELECT array_remove(ARRAY [1, 2, 3], null);
                 array_remove
                --------------
                 NULL
                (1 row)

                SELECT array_remove(ARRAY [null, 2, 2, 6, 6, 8, 2], 2);
                 array_remove
                --------------
                 {null, 6, 6, 8}
                (1 row)

                SELECT array_remove(ARRAY [null, 2, 2, 6, 6, 8, 2], null);
                 array_remove
                --------------
                 NULL
                (1 row)

                SELECT array_remove(ARRAY [2, 2, 6, 6, 8, 2], elem) FROM (SELECT elem FROM UNNEST(ARRAY [2, 6, 8]) as elem);
                 array_remove
                --------------
                 {6, 6, 8}
                 {2, 2, 8, 2}
                 {2, 2, 6, 6, 2}
                (3 rows)

                SELECT array_remove(ARRAY [2, 2, 6, 6, 8, 2], elem) FROM (SELECT elem FROM UNNEST(ARRAY [2, 6, 8, null]) as elem);
                 array_remove
                --------------
                 {6, 6, 8}
                 {2, 2, 8, 2}
                 {2, 2, 6, 6, 2}
                 NULL
                (4 rows)

                SELECT array_remove(CAST(NULL AS INTEGER ARRAY), 1);
                 array_remove
                --------------
                 NULL
                (1 row)

                SELECT array_remove(CAST(NULL AS INTEGER ARRAY), NULL);
                 array_remove
                --------------
                 NULL
                (1 row)
                """
        );
    }

    @Test
    public void testNullArray() {
        this.queryFailingInCompilation("SELECT array_position(null, 3)", "Illegal use of 'NULL'");
        this.queryFailingInCompilation("SELECT array_max(NULL)", "Cannot apply 'ARRAY_MAX' to arguments of type");
        this.queryFailingInCompilation("SELECT array_min(NULL)", "Cannot apply 'ARRAY_MIN' to arguments of type");
        this.queryFailingInCompilation("SELECT array_prepend(null, 1)", "Illegal use of 'NULL'");
        this.queryFailingInCompilation("SELECT array_remove(NULL, 1)", "Illegal use of 'NULL'");
    }

    @Test
    public void testArrayPrepend() {
        this.qs("""
                SELECT array_prepend(ARRAY [2, 3], 1);
                 array_prepend
                ---------------
                 {1, 2, 3}
                (1 row)

                SELECT array_prepend(ARRAY [2, 3], null);
                 array_prepend
                ---------------
                 {NULL, 2, 3}
                (1 row)

                SELECT array_prepend(ARRAY [2, 3], null::double);
                 array_prepend
                ---------------
                 {NULL, 2, 3}
                (1 row)

                SELECT array_prepend(ARRAY [2.5, 3.5], null::int);
                 array_prepend
                ---------------
                 {NULL, 2.5, 3.5}
                (1 row)

                SELECT array_prepend(ARRAY [2, 3, null], null);
                 array_prepend
                ---------------
                 {NULL, 2, 3, null}
                (1 row)

                SELECT array_prepend(ARRAY [null], null);
                 array_prepend
                ---------------
                 {NULL, null}
                (1 row)

                SELECT array_prepend(ARRAY [null], 1);
                 array_prepend
                ---------------
                 {1, NULL}
                (1 row)
                """
        );
    }

    @Test
    public void testCardinality() {
        this.qs("""
                SELECT cardinality(ARRAY [1]);
                 cardinality
                -------------
                 1
                (1 row)

                SELECT cardinality(ARRAY [1, null]);
                 cardinality
                -------------
                 2
                (1 row)

                SELECT cardinality(null);
                 cardinality
                -------------
                 NULL
                (1 row)
                """
        );
    }

    @Test
    public void testSortArray() {
        this.qs("""
                SELECT sort_array(ARRAY [7, 1, 4, 3]);
                 sort_array
                ------------
                 {1, 3, 4, 7}
                (1 row)

                SELECT sort_array(ARRAY [7, 1, 4, 3], true);
                 sort_array
                ------------
                 {1, 3, 4, 7}
                (1 row)

                SELECT sort_array(ARRAY [7, 1, 4, 3], false);
                 sort_array
                ------------
                 {7, 4, 3, 1}
                (1 row)

                SELECT sort_array(ARRAY [7, 1, null, 4, null, 3], false);
                 sort_array
                ------------
                 {7, 4, 3, 1, null, null}
                (1 row)

                SELECT sort_array(ARRAY [7, 1, null, 4, null, 3], true);
                 sort_array
                ------------
                 {null, null, 1, 3, 4, 7}
                (1 row)

                SELECT sort_array(ARRAY [7e0, 1e0, null, 4e0, null, 3e0], true);
                 sort_array
                ------------
                 {null, null, 1, 3, 4, 7}
                (1 row)

                SELECT sort_array(null);
                 sort_array
                ------------
                 NULL
                (1 row)

                SELECT sort_array(ARRAY [true, false, null]);
                 sort_array
                ------------
                 {null, false, true}
                (1 row)

                SELECT sort_array(ARRAY ['z', 'a', 'c']);
                 sort_array
                ------------
                 { a, c, z}
                (1 row)

                SELECT sort_array(ARRAY ['z', 'a', 'c'], false);
                 sort_array
                ------------
                 { z, c, a}
                (1 row)
                """
        );
    }

    @Test
    public void testArraySize() {
        this.qs("""
                SELECT cardinality(ARRAY [1, 2, 3]);
                 array_size
                ------------
                 3
                (1 row)

                SELECT array_size(ARRAY [1, 2, 3]);
                 array_size
                ------------
                 3
                (1 row)

                SELECT array_size(ARRAY [1]);
                 array_size
                ------------
                 1
                (1 row)

                SELECT array_size(null);
                 array_size
                ------------
                 NULL
                (1 row)

                SELECT array_length(ARRAY [1, 2, 3]);
                 array_length
                ------------
                 3
                (1 row)

                SELECT array_length(ARRAY [1]);
                 array_length
                ------------
                 1
                (1 row)

                SELECT array_length(null);
                 array_length
                ------------
                 NULL
                (1 row)
                """);
    }

    @Test
    public void testArrayReverse() {
        this.qs("""
                SELECT ARRAY_REVERSE(ARRAY [2, 3, 3]);
                 array_reverse
                ---------------
                 {3, 3, 2}
                (1 row)

                SELECT array_reverse(null);
                 array_reverse
                ---------------
                 NULL
                (1 row)

                SELECT array_reverse(ARRAY [1, 2, 3, null]);
                 array_reverse
                ---------------
                 {NULL, 3, 2, 1}
                (1 row)

                SELECT array_reverse(ARRAY [1]);
                 array_reverse
                ---------------
                 {1}
                (1 row)

                SELECT array_reverse(ARRAY [NULL]);
                 array_reverse
                ---------------
                 {NULL}
                (1 row)
                """
        );
    }

    @Test
    public void testArrayMinMax() {
        this.qs("""
                SELECT array_max(ARRAY [9, 1, 2, 4, 8]);
                 array_max
                -----------
                 9
                (1 row)

                SELECT array_max(ARRAY [9, 1, 2, 4, 8, null]);
                 array_max
                -----------
                 9
                (1 row)

                SELECT array_min(ARRAY [9, 1, 2, 4, 8]);
                 array_min
                -----------
                 1
                (1 row)

                SELECT array_min(ARRAY [9, 1, 2, 4, 8, null]);
                 array_min
                -----------
                 1
                (1 row)
                """
        );
    }

    // Test for https://github.com/feldera/feldera/issues/1472
    @Test
    public void testArrayCompact2() {
        this.qs("""
                SELECT array_compact(ARRAY [NULL]);
                 array_compact
                ---------------
                 {}
                (1 row)
                """);
    }

    @Test
    public void testArrayCompact() {
        this.qs("""
                SELECT array_compact(ARRAY [1, null, 2, null]);
                 array_compact
                ---------------
                 {1, 2}
                (1 row)

                SELECT array_compact(ARRAY [1, 2]);
                 array_compact
                ---------------
                 {1, 2}
                (1 row)

                SELECT array_compact(NULL);
                 array_compact
                ---------------
                 NULL
                (1 row)

                SELECT array_compact(ARRAY [1, 2, 3.1, null]);
                 array_compact
                ---------------
                 {1.0, 2.0, 3.1}
                (1 row)

                SELECT array_compact(ARRAY ['a', 'b', null]);
                 array_compact
                ---------------
                 { a, b}
                (1 row)

                SELECT array_compact(ARRAY ['1', NULL, '2', NULL]);
                 array_compact
                ---------------
                 { 1, 2}
                (1 row)

                SELECT array_compact(ARRAY ['a', 'b']);
                 array_compact
                ---------------
                 { a, b}
                (1 row)

                SELECT array_compact(ARRAY [NULL::int]);
                 array_compact
                ---------------
                 {}
                (1 row)

                SELECT array_compact(NULL);
                 array_compact
                ---------------
                 NULL
                (1 row)
                """
        );
    }

    @Test
    public void testArrayDistinct() {
        this.qs("""
                SELECT array_distinct(ARRAY [1, 1, 2, 2, 3, 3]);
                 array_distinct
                ----------------
                 {1, 2, 3}
                (1 row)

                SELECT array_distinct(ARRAY [2, 4, 6, 8, 4, 6]);
                 array_distinct
                -----------------
                 {2,4,6,8}
                (1 row)

                SELECT array_distinct(ARRAY [2, 2, 2, 2]);
                 array_distinct
                ---------------
                 {2}
                (1 row)

                SELECT array_distinct(ARRAY ['a', 'b', 'c', 'b', 'a']);
                 array_distinct
                -----------------
                 { a, b, c}
                (1 row)

                SELECT array_distinct(ARRAY [null, null, null]);
                 array_distinct
                ---------------
                 {NULL}
                (1 row)

                SELECT array_distinct(ARRAY [1, 2, null, 2, null, 3]);
                 array_distinct
                -----------------
                 {1,2,null,3}
                (1 row)

                SELECT array_distinct(ARRAY [null, 1, null, 2, 3]);
                 array_distinct
                -----------------
                 {NULL,1,2,3}
                (1 row)

                SELECT array_distinct(ARRAY [1, 2, 3, 4, 5]);
                 array_distinct
                -----------------
                 {1,2,3,4,5}
                (1 row)

                SELECT array_distinct(NULL);
                 array_distinct
                ----------------
                 NULL
                (1 row)

                SELECT array_distinct(CAST(NULL AS INTEGER ARRAY));
                 array_distinct
                ----------------
                 NULL
                (1 row)
                """
        );
    }

    @Test
    public void issue3272() {
        this.q("""
                SELECT ARRAYS_OVERLAP(array['UC', 'uc', 'f'], array['f']);
                 result
                --------
                 true""");
    }

    @Test
    public void testArraysOverlap() {
        this.qs("""
                SELECT ARRAYS_OVERLAP(null, null);
                 arrays_overlap
                ----------------
                 null
                (1 row)
                
                SELECT ARRAYS_OVERLAP(ARRAY [1, 2, 3], ARRAY [2, 4]);
                 arrays_overlap
                ----------------
                    true
                (1 row)

                SELECT ARRAYS_OVERLAP(ARRAY [1, 2, 3], cast(null as integer array));
                 arrays_overlap
                ----------------
                    NULL
                (1 row)

                SELECT ARRAYS_OVERLAP(ARRAY [1, 2, 3, null], cast(null as integer array));
                 arrays_overlap
                ----------------
                    NULL
                (1 row)

                SELECT ARRAYS_OVERLAP(cast(null as integer array), ARRAY [1, 2, 3]);
                 arrays_overlap
                ----------------
                    NULL
                (1 row)

                SELECT ARRAYS_OVERLAP(cast(null as integer array), ARRAY [1, 2, 3, null]);
                 arrays_overlap
                ----------------
                    NULL
                (1 row)

                SELECT ARRAYS_OVERLAP(ARRAY [null, 1], ARRAY [2, 1]);
                 arrays_overlap
                ----------------
                 true
                (1 row)

                SELECT ARRAYS_OVERLAP(ARRAY [1, 2], ARRAY [1, null]);
                 arrays_overlap
                ----------------
                 true
                (1 row)

                SELECT ARRAYS_OVERLAP(ARRAY [null, 1], ARRAY [2, 1, null]);
                 arrays_overlap
                ----------------
                 true
                (1 row)

                SELECT ARRAYS_OVERLAP(ARRAY [null, 1], ARRAY [2, 3, null]);
                 arrays_overlap
                ----------------
                 true
                (1 row)

                SELECT ARRAYS_OVERLAP(cast(null as integer array), cast(null as integer array));
                 arrays_overlap
                ----------------
                 NULL
                (1 row)

                SELECT ARRAYS_OVERLAP(array[1, 2], array[3]);
                 arrays_overlap
                ----------------
                 false
                (1 row)

                SELECT ARRAYS_OVERLAP(array[3], array[2]);
                 arrays_overlap
                ----------------
                 false
                (1 row)

                SELECT ARRAYS_OVERLAP(array [3], array [1, null]);
                 arrays_overlap
                ----------------
                 false
                (1 row)
                """
        );
    }

    @Test
    public void testArraysOverlapDiffTypes() {
        // fails for the Calcite optimized version as Calcite returns false
        this.queryFailingInCompilation("SELECT ARRAYS_OVERLAP(ARRAY [1, 2, 3], ARRAY [2e0, 4e0])",
                "Cannot apply 'arrays_overlap' to arguments of type");
        this.queryFailingInCompilation("SELECT ARRAYS_OVERLAP(ARRAY [1, 2, 3], ARRAY [2.0, 4.0])",
                "Cannot apply 'arrays_overlap' to arguments of type");
    }

    @Test
    public void testArrayAgg() {
        // Tests from https://cloud.google.com/bigquery/docs/reference/standard-sql/aggregate_functions#array_agg
        this.qs("""
                SELECT ARRAY_AGG(x) AS array_agg FROM UNNEST(ARRAY [2, 1,-2, 3, -2, 1, 2]) AS x;
                +-------------------------+
                | array_agg               |
                +-------------------------+
                | {-2, -2, 1, 1, 2, 2, 3} |
                +-------------------------+
                (1 row)

                SELECT ARRAY_AGG(DISTINCT x) AS array_agg
                FROM UNNEST(ARRAY [2, 1, -2, 3, -2, 1, 2]) AS x;
                +---------------+
                | array_agg     |
                +---------------+
                | {-2, 1, 2, 3} |
                +---------------+
                (1 row)

                SELECT ARRAY_AGG(x IGNORE NULLS) AS array_agg
                FROM UNNEST(ARRAY [NULL, 1, -2, 3, -2, 1, NULL]) AS x;
                +-------------------+
                | array_agg         |
                +-------------------+
                | {-2, -2, 1, 1, 3} |
                +-------------------+
                (1 row)

                WITH vals AS
                  (
                    SELECT 1 x, 'a' y UNION ALL
                    SELECT 1 x, 'b' y UNION ALL
                    SELECT 2 x, 'a' y UNION ALL
                    SELECT 2 x, 'c' y
                  )
                SELECT x, ARRAY_AGG(y) as array_agg
                FROM vals
                GROUP BY x;
                +---------------+
                | x | array_agg |
                +---------------+
                | 1 | { a, b}   |
                | 2 | { a, c}   |
                +---------------+
                (2 rows)""");
    }

    @Test
    public void testOverArrayAgg() {
        // The order of the elements in the array is non-deterministic
        this.qs("""
                SELECT
                  x,
                  ARRAY_AGG(x) OVER (ORDER BY ABS(x)) AS array_agg
                FROM UNNEST(ARRAY [2, 1, -2, 3, -2, 1, 2]) AS x;
                +----+-------------------------+
                | x  | array_agg               |
                +----+-------------------------+
                | 1  | {1, 1}                  |
                | 1  | {1, 1}                  |
                | 2  | {-2, -2, 1, 1, 2, 2}    |
                | -2 | {-2, -2, 1, 1, 2, 2}    |
                | -2 | {-2, -2, 1, 1, 2, 2}    |
                | 2  | {-2, -2, 1, 1, 2, 2}    |
                | 3  | {-2, -2, 1, 1, 2, 2, 3} |
                +----+-------------------------+
                (7 rows)""");
    }
    
    @Test
    public void testArrayIntersect() {
        this.qs("""
                select array_intersect(array[2, 3, 3], array[3]);
                 r
                ---
                {3}
                (1 row)

                select array_intersect(array[1], array[2, 3]);
                 r
                ---
                 {}
                (1 row)

                select array_intersect(array[2, null, 2], array[1, 2, null]);
                 r
                ---
                {NULL,2}
                (1 row)
                
                select array_intersect(cast(null as integer array), array[1]);
                 r
                ---
                NULL
                (1 row)
                
                select array_intersect(array[1], cast(null as integer array));
                 r
                ---
                NULL
                (1 row)
                
                select array_intersect(cast(null as integer array), cast(null as integer array));
                 r
                ---
                NULL
                (1 row)""");
    }
    
    @Test
    public void testArrayInsert() {
        this.qs("""
                select array_insert(cast(null as integer array), 3, 4);
                 r
                ---
                NULL
                (1 row)
                
                select array_insert(array[1], cast(null as integer), 4);
                 r
                ---
                NULL
                (1 row)
                
                select array_insert(array[1, 2, 3], 3, 4);
                 r
                ---
                {1, 2, 4, 3}
                (1 row)
                
                select array_insert(array[1, 2, 3], 3, cast(null as integer));
                 r
                ---
                {1, 2, null, 3}
                (1 row)
                
                select array_insert(array[2, 3, 4], 1, 1);
                 r
                ---
                {1, 2, 3, 4}
                (1 row)
                
                select array_insert(array[1, 3, 4], -1, 2);
                 r
                ---
                {1, 3, 4, 2}
                (1 row)
                
                select array_insert(array[1, 3, 4], -3, 2);
                 r
                ---
                {1, 2, 3, 4}
                (1 row)
                
                select array_insert(array[2, 3, null, 4], -6, 1);
                 r
                ---
                {1, null, 2, 3, null, 4}
                (1 row)
                
                select array_insert(array(1, 2, 3), 3, cast(4 as tinyint));
                 r
                ---
                {1, 2, 4, 3}
                (1 row)
                
                select array_insert(array(1, 2, 3), 3, 4.0e0);
                 r
                ---
                {1, 2, 4, 3}
                (1 row)
                
                select array_insert(array(1, 2, 3), 3, cast(4 as bigint));
                 r
                ---
                {1, 2, 4, 3}
                (1 row)
                
                select array_insert(array(1, 2, 3), 3, cast(null as bigint));
                 r
                ---
                {1, 2, null, 3}
                (1 row)
                
                select array_insert(array(1, 2, 3), 3, cast(null as real));
                 r
                ---
                {1, 2, null, 3}
                (1 row)
                
                select array_insert(array(1, 2, 3), 3, cast(null as tinyint));
                 r
                ---
                {1, 2, null, 3}
                (1 row)""");

        // select array_insert(array[array[1,2]], 1, array[1])",
        //  r
        // ---
        // [[1], [1, 2]]", "INTEGER NOT NULL ARRAY NOT NULL ARRAY NOT NULL");
        // select array_insert(array[array[1,2]], -1, array[1])",
        //  r
        // ---
        //     "[[1, 2], [1]]", "INTEGER NOT NULL ARRAY NOT NULL ARRAY NOT NULL");
        // select array_insert(array[map[1, 'a']], 1, map[2, 'b'])", "[{2=b}, {1=a}]",
        //  r
        // ---
        //     "(INTEGER NOT NULL, CHAR(1) NOT NULL) MAP NOT NULL ARRAY NOT NULL");
        // select array_insert(array[map[1, 'a']], -1, map[2, 'b'])", "[{1=a}, {2=b}]",
        //  r
        // ---
        //     "(INTEGER NOT NULL, CHAR(1) NOT NULL) MAP NOT NULL ARRAY NOT NULL");
    }

    @Test
    public void testExists() {
        this.qs("""
                SELECT array_EXISTS(array[1, 2, 3], x -> x > 2);
                result
                ------
                 true
                (1 row)

                SELECT array_exists(array[1, 2, 3], x -> x > 3);
                result
                ------
                 false
                (1 row)

                SELECT array_exists(array[1, 2, 3], x -> false);
                result
                ------
                 false
                (1 row)

                SELECT array_exists(array[1, 2, 3], x -> true);
                result
                ------
                 true
                (1 row)

                SELECT array_exists(CAST(array() AS INT ARRAY), x -> true);
                result
                ------
                 false
                (1 row)

                SELECT array_exists(CAST(array() AS INT ARRAY), x -> false);
                result
                ------
                 false
                (1 row)

                SELECT array_exists(CAST(array() AS INT ARRAY), x -> cast(x as int) = 1);
                result
                ------
                 false
                (1 row)

                SELECT array_exists(array[-1, 2, 3], y -> abs(y) = 1);
                result
                ------
                 true
                (1 row)

                SELECT array_exists(array[-1, 2, 3], y -> abs(y) = 4);
                result
                ------
                 false
                (1 row)

                SELECT array_exists(array[1, 2, 3], x -> x > 2 and x < 4);
                result
                ------
                 true
                (1 row)

                SELECT array_exists(array[array[1, 2], array[3, 4]], x -> x[1] = 1);
                result
                ------
                 true
                (1 row)

                SELECT array_exists(array[array[1, 2], array[3, 4]], x -> x[1] = 5);
                result
                ------
                 false
                (1 row)

                SELECT array_exists(array[null, 3], x -> x > 2 or x < 4);
                result
                ------
                 true
                (1 row)

                SELECT array_exists(array[null, 3], x -> x is null);
                result
                ------
                 true
                (1 row)

                SELECT array_exists(array[null, 3], x -> cast(null as boolean));
                result
                ------
                NULL
                (1 row)

                SELECT array_exists(array[null, 3], x -> x = null);
                result
                ------
                NULL
                (1 row)
                
                SELECT array_exists(cast(null as integer array), x -> x > 2);
                result
                ------
                NULL
                (1 row)""");
    }

    @Test
    public void testArrayExistsTable() {
        var ccs = this.getCCS("""
                CREATE TABLE T(x INT ARRAY);
                CREATE VIEW V AS SELECT ARRAY_EXISTS(x, e -> e % 2 = 0) FROM T;""");
        ccs.step("INSERT INTO T VALUES(ARRAY[1, 2, 3])", """
                  r     | weight
                 ----------------
                  true  | 1""");
        ccs.step("INSERT INTO T VALUES(ARRAY())", """
                  r     | weight
                 ----------------
                  false | 1""");
    }

    @Test
    public void testTransform() {
        this.qs("""
                SELECT transform(array[1, 2, 3], x -> x + 3);
                result
                ------
                 { 4, 5, 6 }
                (1 row)

                SELECT transform(array[1, 2, 3], x -> x > 2);
                result
                ------
                 { false, false, true }
                (1 row)

                SELECT transform(array[1, 2, 3], x -> false);
                result
                ------
                 { false, false, false }
                (1 row)

                SELECT transform(array[1, 2, 3], x -> CAST(x as VARCHAR));
                result
                ------
                 { 1, 2, 3 }
                (1 row)

                SELECT transform(CAST(array() AS INT ARRAY), x -> true);
                result
                ------
                 {}
                (1 row)

                SELECT transform(CAST(array() AS INT ARRAY), x -> cast(x as varchar));
                result
                ------
                 {}
                (1 row)

                SELECT transform(array[-1, 2, 3], y -> abs(y));
                result
                ------
                 { 1, 2, 3 }
                (1 row)

                SELECT transform(array[1, 2, 3], x -> x > 2 and x < 4);
                result
                ------
                 { false, false, true }
                (1 row)

                SELECT transform(array[array[1, 2], array[3, 4]], x -> x[1]);
                result
                ------
                 { 1, 3 }
                (1 row)

                SELECT transform(array[array[1, 2], array[3, 4]], x -> x[1] + 5);
                result
                ------
                 { 6, 8 }
                (1 row)

                SELECT transform(array[null, 3], x -> x + 2);
                result
                ------
                 { NULL, 5 }
                (1 row)

                SELECT transform(array[null, 3], x -> x is null);
                result
                ------
                 { true, false }
                (1 row)

                SELECT transform(array[null, 3], x -> cast(null as boolean));
                result
                ------
                 { NULL, NULL }
                (1 row)

                SELECT transform(array[null, 3], x -> x = null);
                result
                ------
                 { NULL, NULL }
                (1 row)
                
                SELECT transform(cast(null as integer array), x -> x > 2);
                result
                ------
                NULL
                (1 row)""");
    }

    @Test
    public void testArrayTransformTable() {
        var ccs = this.getCCS("""
                CREATE TABLE T(x INT ARRAY);
                CREATE VIEW V AS SELECT TRANSFORM(x, e -> e % 2) FROM T;""");
        ccs.step("INSERT INTO T VALUES(ARRAY[1, 2, 3])", """
                  r           | weight
                 ----------------
                  { 1, 0, 1 } | 1""");
        ccs.step("INSERT INTO T VALUES(ARRAY())", """
                  r     | weight
                 ----------------
                  {}    | 1""");
    }

    @Test
    public void failingLambdaCases() {
        this.queryFailingInCompilation("SELECT array_exists(array(), x -> true)",
                "Could not infer a type for array elements");
        this.queryFailingInCompilation("SELECT array_exists(array[1], (x, y) -> x AND y)",
                "Cannot apply 'array_exists' to arguments of type");
        this.queryFailingInCompilation("SELECT array_exists(array[1], x -> 1)",
                "Cannot apply 'array_exists' to arguments of type");
        this.queryFailingInCompilation("SELECT transform(array(), x -> x)",
                "Could not infer a type for array elements");
        this.queryFailingInCompilation("SELECT transform(array[1], (x, y) -> x + y)",
                "Cannot apply 'transform' to arguments of type");
        this.queryFailingInCompilation("SELECT transform(array[1], x -> x AND true)",
                "Cannot apply 'AND' to arguments of type '<INTEGER> AND <BOOLEAN>'");
    }
}
