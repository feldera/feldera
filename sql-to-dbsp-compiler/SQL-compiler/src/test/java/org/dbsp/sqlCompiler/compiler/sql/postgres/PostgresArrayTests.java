package org.dbsp.sqlCompiler.compiler.sql.postgres;

import org.junit.Test;

import org.dbsp.sqlCompiler.compiler.sql.tools.SqlIoTest;

// https://github.com/postgres/postgres/blob/master/src/test/regress/expected/arrays.out
public class PostgresArrayTests extends SqlIoTest {
    @Test
    public void testSplit() {
        // Renamed 'string_to_array' to 'split'
        this.qs("""
                select split('1|2|3', '|');
                 split
                -----------------
                 { 1, 2, 3}
                (1 row)

                select split('1|2|3|', '|');
                 split
                -----------------
                 { 1, 2, 3, }
                (1 row)

                select split('1||2|3||', '||');
                 split
                -----------------
                 { 1, 2|3, }
                (1 row)

                select split('1|2|3', '');
                 split
                -----------------
                 { 1|2|3}
                (1 row)

                select split('', '|');
                 split
                -----------------
                 {}
                (1 row)

                -- Semantics different from Postgres
                select split('1|2|3', NULL);
                 split
                -----------------
                 NULL
                (1 row)

                select split(NULL, '|') IS NULL;
                 ?column?
                ----------
                 t
                (1 row)

                select split('abc', '');
                 split
                -----------------
                 { abc}
                (1 row)

                select split('abc', ',');
                 split\s
                -----------------
                 { abc}
                (1 row)

                select split('1,2,3,4,,6', ',');
                 split
                -----------------
                 { 1, 2, 3, 4, , 6}
                (1 row)

                select split('', '');
                 split
                -----------------
                 { }
                (1 row)""");
        // Deleted some three-operand cases
    }

    @Test
    public void testArrayToString() {
        // In Calcite array_to_string requires all arguments to be strings
        this.qs("""
                select array_to_string(NULL::TEXT ARRAY, ',') IS NULL;
                 ?column?
                ----------
                 t
                (1 row)

                select array_to_string(ARRAY()::TEXT ARRAY, ',');
                 array_to_string
                -----------------
                \s
                (1 row)

                select array_to_string(array['1','2','3','4',NULL,'6'], ',');
                 array_to_string
                -----------------
                 1,2,3,4,6
                (1 row)

                select array_to_string(array['1','2','3','4',NULL,'6'], ',', '*');
                 array_to_string
                -----------------
                 1,2,3,4,*,6
                (1 row)

                select array_to_string(array['1','2','3','4',NULL,'6'], NULL);
                 array_to_string
                -----------------
                NULL
                (1 row)

                select array_to_string(split('1|2|3', '|'), '|');
                 array_to_string
                -----------------
                 1|2|3
                (1 row)
                """);
    }
}
