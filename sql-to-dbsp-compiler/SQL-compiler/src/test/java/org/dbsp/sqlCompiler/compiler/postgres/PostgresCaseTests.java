package org.dbsp.sqlCompiler.compiler.postgres;

import org.dbsp.sqlCompiler.compiler.backend.DBSPCompiler;
import org.junit.Test;

// https://github.com/postgres/postgres/blob/master/src/test/regress/expected/case.out
public class PostgresCaseTests extends PostgresBaseTest {
    @Override
    public void prepareData(DBSPCompiler compiler) {
        compiler.compileStatements("CREATE TABLE CASE_TBL (\n" +
                "  i integer,\n" +
                "  f double precision\n" +
                ");\n" +
                "CREATE TABLE CASE2_TBL (\n" +
                "  i integer,\n" +
                "  j integer\n" +
                ");\n" +
                "INSERT INTO CASE_TBL VALUES (1, 10.1);\n" +
                "INSERT INTO CASE_TBL VALUES (2, 20.2);\n" +
                "INSERT INTO CASE_TBL VALUES (3, -30.3);\n" +
                "INSERT INTO CASE_TBL VALUES (4, NULL);\n" +
                "INSERT INTO CASE2_TBL VALUES (1, -1);\n" +
                "INSERT INTO CASE2_TBL VALUES (2, -2);\n" +
                "INSERT INTO CASE2_TBL VALUES (3, -3);\n" +
                "INSERT INTO CASE2_TBL VALUES (2, -4);\n" +
                "INSERT INTO CASE2_TBL VALUES (1, NULL);\n" +
                "INSERT INTO CASE2_TBL VALUES (NULL, -6);\n");
    }

    @Test
    public void testCase() {
        this.qs("SELECT '3' AS \"One\",\n" +
                "  CASE\n" +
                "    WHEN 1 < 2 THEN 3\n" +
                "  END AS \"Simple WHEN\";\n" +
                " One | Simple WHEN \n" +
                "-----+-------------\n" +
                "3|           3\n" +
                "(1 row)\n" +
                "\n" +
                "SELECT '<NULL>' AS \"One\",\n" +
                "  CASE\n" +
                "    WHEN 1 > 2 THEN 3\n" +
                "  END AS \"Simple default\";\n" +
                "  One   | Simple default \n" +
                "--------+----------------\n" +
                "<NULL>|               \n" +
                "(1 row)\n" +
                "\n" +
                "SELECT '3' AS \"One\",\n" +
                "  CASE\n" +
                "    WHEN 1 < 2 THEN 3\n" +
                "    ELSE 4\n" +
                "  END AS \"Simple ELSE\";\n" +
                " One | Simple ELSE \n" +
                "-----+-------------\n" +
                "3|           3\n" +
                "(1 row)\n" +
                "\n" +
                "SELECT '4' AS \"One\",\n" +
                "  CASE\n" +
                "    WHEN 1 > 2 THEN 3\n" +
                "    ELSE 4\n" +
                "  END AS \"ELSE default\";\n" +
                " One | ELSE default \n" +
                "-----+--------------\n" +
                "4|            4\n" +
                "(1 row)\n" +
                "\n" +
                "SELECT '6' AS \"One\",\n" +
                "  CASE\n" +
                "    WHEN 1 > 2 THEN 3\n" +
                "    WHEN 4 < 5 THEN 6\n" +
                "    ELSE 7\n" +
                "  END AS \"Two WHEN with default\";\n" +
                " One | Two WHEN with default \n" +
                "-----+-----------------------\n" +
                "6|                     6\n" +
                "(1 row)\n" +
                "\n" +
                "SELECT '7' AS \"None\",\n" +
                "   CASE WHEN 2 < 0 THEN 1\n" +
                "   END AS \"NULL on no matches\";\n" +
                " None | NULL on no matches \n" +
                "------+--------------------\n" +
                "7|                   \n" +
                "(1 row)\n" +
                "\n" +
                "-- Constant-expression folding shouldn't evaluate unreachable subexpressions\n" +
                "SELECT CASE WHEN 1=0 THEN 1/0 WHEN 1=1 THEN 1 ELSE 2/0 END;\n" +
                " case \n" +
                "------\n" +
                "    1\n" +
                "(1 row)\n" +
                "\n" +
                "SELECT CASE 1 WHEN 0 THEN 1/0 WHEN 1 THEN 1 ELSE 2/0 END;\n" +
                " case \n" +
                "------\n" +
                "    1\n" +
                "(1 row)");
    }

    @Test
    public void testCases2() {
        // changed error to null output
        this.qs(
                "SELECT CASE WHEN i > 100 THEN 1/0 ELSE 0 END FROM case_tbl;\n" +
                "case\n" +
                "------\n" +
                "0\n" +
                "0\n" +
                "0\n" +
                "0\n" +
                "(3 rows)\n" +
                "\n" +
                "-- Test for cases involving untyped literals in test expression\n" +
                "SELECT CASE 'a' WHEN 'a' THEN 1 ELSE 2 END;\n" +
                " case \n" +
                "------\n" +
                "    1\n" +
                "(1 row)\n" +
                "\n" +
                "--\n" +
                "-- Examples of targets involving tables\n" +
                "--\n" +
                "SELECT\n" +
                "  CASE\n" +
                "    WHEN i >= 3 THEN i\n" +
                "  END AS \">= 3 or Null\"\n" +
                "  FROM CASE_TBL;\n" +
                " >= 3 or Null \n" +
                "--------------\n" +
                "             \n" +
                "             \n" +
                "            3\n" +
                "            4\n" +
                "(4 rows)\n" +
                "\n" +
                "SELECT\n" +
                "  CASE WHEN i >= 3 THEN (i + i)\n" +
                "       ELSE i\n" +
                "  END AS \"Simplest Math\"\n" +
                "  FROM CASE_TBL;\n" +
                " Simplest Math \n" +
                "---------------\n" +
                "             1\n" +
                "             2\n" +
                "             6\n" +
                "             8\n" +
                "(4 rows)\n" +
                "\n" +
                "SELECT i AS \"Value\",\n" +
                "  CASE WHEN (i < 0) THEN 'small'\n" +
                "       WHEN (i = 0) THEN 'zero'\n" +
                "       WHEN (i = 1) THEN 'one'\n" +
                "       WHEN (i = 2) THEN 'two'\n" +
                "       ELSE 'big'\n" +
                "  END AS \"Category\"\n" +
                "  FROM CASE_TBL;\n" +
                        // This is padded with spaces in Calcite.
                " Value | Category \n" +
                "-------+----------\n" +
                "     1 |one\n" +
                "     2 |two\n" +
                "     3 |big\n" +
                "     4 |big\n" +
                "(4 rows)\n" +
                "\n" +
                "SELECT\n" +
                "  CASE WHEN ((i < 0) or (i < 0)) THEN 'small'\n" +
                "       WHEN ((i = 0) or (i = 0)) THEN 'zero'\n" +
                "       WHEN ((i = 1) or (i = 1)) THEN 'one'\n" +
                "       WHEN ((i = 2) or (i = 2)) THEN 'two'\n" +
                "       ELSE 'big'\n" +
                "  END AS \"Category\"\n" +
                "  FROM CASE_TBL;\n" +
                " Category \n" +
                "----------\n" +
                "one\n" +
                "two\n" +
                "big\n" +
                "big\n" +
                "(4 rows)\n" +
                "\n" +
                "--\n" +
                "-- Examples of qualifications involving tables\n" +
                "--\n" +
                "--\n" +
                "-- NULLIF() and COALESCE()\n" +
                "-- Shorthand forms for typical CASE constructs\n" +
                "--  defined in the SQL standard.\n" +
                "--\n" +
                "SELECT * FROM CASE_TBL WHERE COALESCE(f,i) = 4;\n" +
                " i | f \n" +
                "---+---\n" +
                " 4 |  \n" +
                "(1 row)\n" +
                "\n" +
                "SELECT * FROM CASE_TBL WHERE NULLIF(f,i) = 2;\n" +
                " i | f \n" +
                "---+---\n" +
                "(0 rows)\n" +
                "\n" +
                "SELECT COALESCE(a.f, b.i, b.j)\n" +
                "  FROM CASE_TBL a, CASE2_TBL b;\n" +
                " coalesce \n" +
                "----------\n" +
                "     10.1\n" +
                "     20.2\n" +
                "    -30.3\n" +
                "        1\n" +
                "     10.1\n" +
                "     20.2\n" +
                "    -30.3\n" +
                "        2\n" +
                "     10.1\n" +
                "     20.2\n" +
                "    -30.3\n" +
                "        3\n" +
                "     10.1\n" +
                "     20.2\n" +
                "    -30.3\n" +
                "        2\n" +
                "     10.1\n" +
                "     20.2\n" +
                "    -30.3\n" +
                "        1\n" +
                "     10.1\n" +
                "     20.2\n" +
                "    -30.3\n" +
                "       -6\n" +
                "(24 rows)\n" +
                "\n" +
                "SELECT *\n" +
                "  FROM CASE_TBL a, CASE2_TBL b\n" +
                "  WHERE COALESCE(a.f, b.i, b.j) = 2;\n" +
                " i | f | i | j  \n" +
                "---+---+---+----\n" +
                " 4 |   | 2 | -2\n" +
                " 4 |   | 2 | -4\n" +
                "(2 rows)\n" +
                "\n" +
                "SELECT NULLIF(a.i,b.i) AS \"NULLIF(a.i,b.i)\",\n" +
                "  NULLIF(b.i, 4) AS \"NULLIF(b.i,4)\"\n" +
                "  FROM CASE_TBL a, CASE2_TBL b;\n" +
                " NULLIF(a.i,b.i) | NULLIF(b.i,4) \n" +
                "-----------------+---------------\n" +
                "                 |             1\n" +
                "               2 |             1\n" +
                "               3 |             1\n" +
                "               4 |             1\n" +
                "               1 |             2\n" +
                "                 |             2\n" +
                "               3 |             2\n" +
                "               4 |             2\n" +
                "               1 |             3\n" +
                "               2 |             3\n" +
                "                 |             3\n" +
                "               4 |             3\n" +
                "               1 |             2\n" +
                "                 |             2\n" +
                "               3 |             2\n" +
                "               4 |             2\n" +
                "                 |             1\n" +
                "               2 |             1\n" +
                "               3 |             1\n" +
                "               4 |             1\n" +
                "               1 |              \n" +
                "               2 |              \n" +
                "               3 |              \n" +
                "               4 |              \n" +
                "(24 rows)\n" +
                "\n" +
                "SELECT *\n" +
                "  FROM CASE_TBL a, CASE2_TBL b\n" +
                "  WHERE COALESCE(f,b.i) = 2;\n" +
                " i | f | i | j  \n" +
                "---+---+---+----\n" +
                " 4 |   | 2 | -2\n" +
                " 4 |   | 2 | -4\n" +
                "(2 rows)");
    }

    // Skipped a bunch of explain and create function and create domain tests.
}
