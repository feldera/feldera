package org.dbsp.sqlCompiler.compiler.sql.simple;

import org.dbsp.sqlCompiler.compiler.sql.SqlIoTest;
import org.junit.Ignore;
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

    @Test @Ignore("https://github.com/feldera/feldera/issues/1475")
    public void testArrayPositionDiffTypes() {
        this.qs("""
                SELECT array_position(ARRAY [1, 2, 3, 4], 1e0);
                 array_position
                ----------------
                 1
                (1 row)
                
                SELECT array_position(ARRAY [1.0, 2.0, 3.0, 4.0], 1e0);
                 array_position
                ----------------
                 1
                (1 row)
                
                SELECT array_position(ARRAY [1.0, 2.0, 3.0, 4.0], 0e0);
                 array_position
                ----------------
                 0
                (1 row)
                """
        );
    }

    @Test @Ignore("https://github.com/feldera/feldera/issues/1465")
    public void testArrayPosition2() {
        this.qs("""
                SELECT array_position(null, 3);
                 array_position
                ----------------
                 NULL
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
}
