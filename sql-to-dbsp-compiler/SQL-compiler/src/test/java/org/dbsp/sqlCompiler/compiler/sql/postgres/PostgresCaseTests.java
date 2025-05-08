package org.dbsp.sqlCompiler.compiler.sql.postgres;

import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.sql.tools.SqlIoTest;
import org.junit.Test;

// https://github.com/postgres/postgres/blob/master/src/test/regress/expected/case.out
public class PostgresCaseTests extends SqlIoTest {
    @Override
    public void prepareInputs(DBSPCompiler compiler) {
        compiler.submitStatementsForCompilation("""
                CREATE TABLE CASE_TBL (
                  i integer,
                  f double precision
                );
                CREATE TABLE CASE2_TBL (
                  i integer,
                  j integer
                );
                INSERT INTO CASE_TBL VALUES (1, 10.1);
                INSERT INTO CASE_TBL VALUES (2, 20.2);
                INSERT INTO CASE_TBL VALUES (3, -30.3);
                INSERT INTO CASE_TBL VALUES (4, NULL);
                INSERT INTO CASE2_TBL VALUES (1, -1);
                INSERT INTO CASE2_TBL VALUES (2, -2);
                INSERT INTO CASE2_TBL VALUES (3, -3);
                INSERT INTO CASE2_TBL VALUES (2, -4);
                INSERT INTO CASE2_TBL VALUES (1, NULL);
                INSERT INTO CASE2_TBL VALUES (NULL, -6);
                """);
    }

    @Test
    public void testCase() {
        this.qs("""
                SELECT '3' AS "One",
                  CASE
                    WHEN 1 < 2 THEN 3
                  END AS "Simple WHEN";
                 One | Simple WHEN
                -----+-------------
                 3|           3
                (1 row)

                SELECT '<NULL>' AS "One",
                  CASE
                    WHEN 1 > 2 THEN 3
                  END AS "Simple default";
                  One   | Simple default
                --------+----------------
                 <NULL>|              \s
                (1 row)

                SELECT '3' AS "One",
                  CASE
                    WHEN 1 < 2 THEN 3
                    ELSE 4
                  END AS "Simple ELSE";
                 One | Simple ELSE
                -----+-------------
                 3|           3
                (1 row)

                SELECT '4' AS "One",
                  CASE
                    WHEN 1 > 2 THEN 3
                    ELSE 4
                  END AS "ELSE default";
                 One | ELSE default
                -----+--------------
                 4|            4
                (1 row)

                SELECT '6' AS "One",
                  CASE
                    WHEN 1 > 2 THEN 3
                    WHEN 4 < 5 THEN 6
                    ELSE 7
                  END AS "Two WHEN with default";
                 One | Two WHEN with default
                -----+-----------------------
                 6|                     6
                (1 row)

                SELECT '7' AS "None",
                   CASE WHEN 2 < 0 THEN 1
                   END AS "NULL on no matches";
                 None | NULL on no matches
                ------+--------------------
                 7|                  \s
                (1 row)

                -- Constant-expression folding shouldn't evaluate unreachable subexpressions
                SELECT CASE WHEN 1=0 THEN 1/0 WHEN 1=1 THEN 1 ELSE 2/0 END;
                 case
                ------
                    1
                (1 row)

                SELECT CASE 1 WHEN 0 THEN 1/0 WHEN 1 THEN 1 ELSE 2/0 END;
                 case
                ------
                    1
                (1 row)""");
    }

    @Test
    public void remove() {
        this.qs("""
                SELECT COALESCE(a.f, b.i, b.j)
                FROM CASE_TBL a, CASE2_TBL b;
                 coalesce
                ----------
                     10.1
                     20.2
                    -30.3
                        1
                     10.1
                     20.2
                    -30.3
                        2
                     10.1
                     20.2
                    -30.3
                        3
                     10.1
                     20.2
                    -30.3
                        2
                     10.1
                     20.2
                    -30.3
                        1
                     10.1
                     20.2
                    -30.3
                       -6
                (24 rows)""");
    }

    @Test
    public void testCases2() {
        // changed error to null output
        this.qs(
                """
                        SELECT CASE WHEN i > 100 THEN 1/0 ELSE 0 END FROM case_tbl;
                        case
                        ------
                        0
                        0
                        0
                        0
                        (4 rows)

                        -- Test for cases involving untyped literals in test expression
                        SELECT CASE 'a' WHEN 'a' THEN 1 ELSE 2 END;
                         case
                        ------
                            1
                        (1 row)

                        --
                        -- Examples of targets involving tables
                        --
                        SELECT
                          CASE
                            WHEN i >= 3 THEN i
                          END AS ">= 3 or Null"
                          FROM CASE_TBL;
                         >= 3 or Null
                        --------------
                                    \s
                                    \s
                                    3
                                    4
                        (4 rows)

                        SELECT
                          CASE WHEN i >= 3 THEN (i + i)
                               ELSE i
                          END AS "Simplest Math"
                          FROM CASE_TBL;
                         Simplest Math
                        ---------------
                                     1
                                     2
                                     6
                                     8
                        (4 rows)

                        SELECT i AS "Value",
                          CASE WHEN (i < 0) THEN 'small'
                               WHEN (i = 0) THEN 'zero'
                               WHEN (i = 1) THEN 'one'
                               WHEN (i = 2) THEN 'two'
                               ELSE 'big'
                          END AS "Category"
                          FROM CASE_TBL;
                         Value | Category
                        -------+----------
                             1 | one
                             2 | two
                             3 | big
                             4 | big
                        (4 rows)

                        SELECT
                          CASE WHEN ((i < 0) or (i < 0)) THEN 'small'
                               WHEN ((i = 0) or (i = 0)) THEN 'zero'
                               WHEN ((i = 1) or (i = 1)) THEN 'one'
                               WHEN ((i = 2) or (i = 2)) THEN 'two'
                               ELSE 'big'
                          END AS "Category"
                          FROM CASE_TBL;
                         Category
                        ----------
                         one
                         two
                         big
                         big
                        (4 rows)

                        --
                        -- Examples of qualifications involving tables
                        --
                        --
                        -- NULLIF() and COALESCE()
                        -- Shorthand forms for typical CASE constructs
                        --  defined in the SQL standard.
                        --
                        SELECT * FROM CASE_TBL WHERE COALESCE(f,i) = 4;
                         i | f
                        ---+---
                         4 | \s
                        (1 row)

                        SELECT * FROM CASE_TBL WHERE NULLIF(f,i) = 2;
                         i | f
                        ---+---
                        (0 rows)

                        SELECT COALESCE(a.f, b.i, b.j)
                          FROM CASE_TBL a, CASE2_TBL b;
                         coalesce
                        ----------
                             10.1
                             20.2
                            -30.3
                                1
                             10.1
                             20.2
                            -30.3
                                2
                             10.1
                             20.2
                            -30.3
                                3
                             10.1
                             20.2
                            -30.3
                                2
                             10.1
                             20.2
                            -30.3
                                1
                             10.1
                             20.2
                            -30.3
                               -6
                        (24 rows)

                        SELECT *
                          FROM CASE_TBL a, CASE2_TBL b
                          WHERE COALESCE(a.f, b.i, b.j) = 2;
                         i | f | i | j
                        ---+---+---+----
                         4 |   | 2 | -2
                         4 |   | 2 | -4
                        (2 rows)

                        SELECT NULLIF(a.i,b.i) AS "NULLIF(a.i,b.i)",
                          NULLIF(b.i, 4) AS "NULLIF(b.i,4)"
                          FROM CASE_TBL a, CASE2_TBL b;
                         NULLIF(a.i,b.i) | NULLIF(b.i,4)
                        -----------------+---------------
                                         |             1
                                       2 |             1
                                       3 |             1
                                       4 |             1
                                       1 |             2
                                         |             2
                                       3 |             2
                                       4 |             2
                                       1 |             3
                                       2 |             3
                                         |             3
                                       4 |             3
                                       1 |             2
                                         |             2
                                       3 |             2
                                       4 |             2
                                         |             1
                                       2 |             1
                                       3 |             1
                                       4 |             1
                                       1 |             \s
                                       2 |             \s
                                       3 |             \s
                                       4 |             \s
                        (24 rows)

                        SELECT *
                          FROM CASE_TBL a, CASE2_TBL b
                          WHERE COALESCE(f,b.i) = 2;
                         i | f | i | j
                        ---+---+---+----
                         4 |   | 2 | -2
                         4 |   | 2 | -4
                        (2 rows)""");
    }

    // Skipped a bunch of explain and create function and create domain tests.
}
