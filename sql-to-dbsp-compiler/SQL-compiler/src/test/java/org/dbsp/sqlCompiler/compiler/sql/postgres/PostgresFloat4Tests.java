package org.dbsp.sqlCompiler.compiler.sql.postgres;

import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.sql.SqlIoTest;
import org.junit.Ignore;
import org.junit.Test;

// https://github.com/postgres/postgres/blob/master/src/test/regress/expected/float4.out
public class PostgresFloat4Tests extends SqlIoTest {
    @Override
    public void prepareData(DBSPCompiler compiler) {
        compiler.compileStatements("CREATE TABLE FLOAT4_TBL (f1  float4);\n" +
                "INSERT INTO FLOAT4_TBL(f1) VALUES ('    0.0');\n" +
                "INSERT INTO FLOAT4_TBL(f1) VALUES ('1004.30   ');\n" +
                "INSERT INTO FLOAT4_TBL(f1) VALUES ('     -34.84    ');\n" +
                "INSERT INTO FLOAT4_TBL(f1) VALUES ('1.2345678901234e+20');\n" +
                "INSERT INTO FLOAT4_TBL(f1) VALUES ('1.2345678901234e-20');");
    }

    @Test @Ignore("Waiting for https://issues.apache.org/jira/projects/CALCITE/issues/CALCITE-6094")
    public void testOverflow() {
        this.q("SELECT 10e70 :: FLOAT4;\n" +
                "result\n" +
                "------\n" +
                "Infinity");
        this.q("SELECT 10e-70 :: FLOAT4;\n" +
                "result\n" +
                "------\n" +
                "0");
        this.q("SELECT -10e70 :: FLOAT4;\n" +
                "result\n" +
                "------\n" +
                "-Infinity");
        this.q("SELECT -10e-70 :: FLOAT4;\n" +
                "result\n" +
                "------\n" +
                "0");
        // The next one fails in Postgres
        this.q("SELECT -10e-400 :: FLOAT4;\n" +
                "result\n" +
                "------\n" +
                "0");
    }

    @Test
    public void testOverflowException() {
        this.shouldFail("SELECT 10e400 :: FLOAT4",
                "out of range");
        this.shouldFail("SELECT-10e400 :: FLOAT4",
                "out of range");
    }

    @Test
    public void testParseError() {
        // This one fails in Postgres
        this.q("SELECT 'xyz' :: FLOAT4;\n" +
                "result\n" +
                "------\n" +
                "0");
        this.shouldFail("SELECT 5.0.0",
                "Error parsing SQL");
        this.shouldFail("SELECT 5.  0",
                "Error parsing SQL");
    }

    @Test
    public void testSpecialValues() {
        this.q("SELECT 'NaN'::float4;\n" +
                " float4 \n" +
                "--------\n" +
                "    NaN");
        this.q("SELECT 'nan'::float4;\n" +
                " float4 \n" +
                "--------\n" +
                "    NaN");
        this.q("SELECT 'NaN'::float4;\n" +
                " float4 \n" +
                "--------\n" +
                "    NaN");
        // Postgres is *not* case-sensitive, but we are.
        // Tests were modified to use proper spelling,
        this.q("SELECT '  Infinity'::float4;\n" +
                "  float4  \n" +
                "----------\n" +
                " Infinity");
        this.q("SELECT '-Infinity'::float4;\n" +
                "  float4   \n" +
                "-----------\n" +
                " -Infinity");
        this.q("SELECT '  inf  '::float4;\n" +
                "  float4  \n" +
                "----------\n" +
                " Infinity");
        this.q("SELECT '-inf'::float4;\n" +
                "  float4   \n" +
                "-----------\n" +
                " -Infinity");
    }

    @Test
    public void testSpecialArithmetic() {
        this.q("SELECT 'Infinity'::float4 / 'Infinity'::float4;\n" +
                " ?column? \n" +
                "----------\n" +
                "      NaN");
        this.q("SELECT '42'::float4 / 'Infinity'::float4;\n" +
                " ?column? \n" +
                "----------\n" +
                "        0");
        this.q("SELECT 'nan'::float4 / 'nan'::float4;\n" +
                " ?column? \n" +
                "----------\n" +
                "      NaN");
        this.q("SELECT 'nan'::float4 / '0'::float4;\n" +
                " ?column? \n" +
                "----------\n" +
                "      NaN");
    }

    @Test
    public void testFPTable() {
        this.q("SELECT * FROM FLOAT4_TBL;\n" +
                "      f1       \n" +
                "---------------\n" +
                "             0\n" +
                "        1004.3\n" +
                "        -34.84\n" +
                " 1.2345679e+20\n" +
                " 1.2345679e-20");
    }

    @Test
    public void testComp() {
        this.qs("SELECT f.* FROM FLOAT4_TBL f WHERE f.f1 <> '1004.3';\n" +
                "      f1       \n" +
                "---------------\n" +
                "             0\n" +
                "        -34.84\n" +
                " 1.2345679e+20\n" +
                " 1.2345679e-20\n" +
                "(4 rows)\n" +
                "\n" +
                "SELECT f.* FROM FLOAT4_TBL f WHERE f.f1 = '1004.3';\n" +
                "   f1   \n" +
                "--------\n" +
                " 1004.3\n" +
                "(1 row)\n" +
                "\n" +
                "SELECT f.* FROM FLOAT4_TBL f WHERE '1004.3' > f.f1;\n" +
                "      f1       \n" +
                "---------------\n" +
                "             0\n" +
                "        -34.84\n" +
                " 1.2345679e-20\n" +
                "(3 rows)\n" +
                "\n" +
                "SELECT f.* FROM FLOAT4_TBL f WHERE  f.f1 < '1004.3';\n" +
                "      f1       \n" +
                "---------------\n" +
                "             0\n" +
                "        -34.84\n" +
                " 1.2345679e-20\n" +
                "(3 rows)\n" +
                "\n" +
                "SELECT f.* FROM FLOAT4_TBL f WHERE '1004.3' >= f.f1;\n" +
                "      f1       \n" +
                "---------------\n" +
                "             0\n" +
                "        1004.3\n" +
                "        -34.84\n" +
                " 1.2345679e-20\n" +
                "(4 rows)\n" +
                "\n" +
                "SELECT f.* FROM FLOAT4_TBL f WHERE  f.f1 <= '1004.3';\n" +
                "      f1       \n" +
                "---------------\n" +
                "             0\n" +
                "        1004.3\n" +
                "        -34.84\n" +
                " 1.2345679e-20\n" +
                "(4 rows)");
    }

    @Test
    public void testFPArithmetic() {
        // Skipped the unary 'abs' Postgres operator written as '@'
        this.qs("SELECT f.f1, f.f1 * '-10' AS x FROM FLOAT4_TBL f\n" +
                "   WHERE f.f1 > '0.0';\n" +
                "      f1       |       x        \n" +
                "---------------+----------------\n" +
                "        1004.3 |         -10043\n" +
                " 1.2345679e+20 | -1.2345678e+21\n" +
                " 1.2345679e-20 | -1.2345678e-19\n" +
                "(3 rows)\n" +
                "\n" +
                "SELECT f.f1, f.f1 + '-10' AS x FROM FLOAT4_TBL f\n" +
                "   WHERE f.f1 > '0.0';\n" +
                "      f1       |       x       \n" +
                "---------------+---------------\n" +
                "        1004.3 |         994.3\n" +
                " 1.2345679e+20 | 1.2345679e+20\n" +
                " 1.2345679e-20 |           -10\n" +
                "(3 rows)\n" +
                "\n" +
                // TODO: precision doesn't match in output
                //"SELECT f.f1, f.f1 / '-10' AS x FROM FLOAT4_TBL f\n" +
                //"   WHERE f.f1 > '0.0';\n" +
                //"      f1       |       x        \n" +
                //"---------------+----------------\n" +
                //"        1004.3 |        -100.43\n" +
                //" 1.2345679e+20 | -1.2345679e+19\n" +
                //" 1.2345679e-20 | -1.2345679e-21\n" +
                //"(3 rows)\n" +
                //"\n" +
                "SELECT f.f1, f.f1 - '-10' AS x FROM FLOAT4_TBL f\n" +
                "   WHERE f.f1 > '0.0';\n" +
                "      f1       |       x       \n" +
                "---------------+---------------\n" +
                "        1004.3 |        1014.3\n" +
                " 1.2345679e+20 | 1.2345679e+20\n" +
                " 1.2345679e-20 |            10\n" +
                "(3 rows)\n" +
                "\n" +
                "SELECT * FROM FLOAT4_TBL;\n" +
                "      f1       \n" +
                "---------------\n" +
                "             0\n" +
                "        1004.3\n" +
                "        -34.84\n" +
                " 1.2345679e+20\n" +
                " 1.2345679e-20\n" +
                "(5 rows)");
    }

    @Test
    public void testCasts() {
        this.q("-- test edge-case coercions to integer\n" +
                "SELECT CAST(CAST('32767.4' AS REAL) AS SMALLINT);\n" +
                " int2  \n" +
                "-------\n" +
                " 32767");
        this.q("-- test edge-case coercions to integer\n" +
                "SELECT '32767.4'::float4::int2;\n" +
                " int2  \n" +
                "-------\n" +
                " 32767");
    }

    // Taken from Postgres `float8.out` and converted to `float4` using Postgres 15.2
    // Note that the Postgres docs say `cbrt()` is implemented only for double
    @Test
    public void testCbrt() {
        this.q("SELECT cbrt(null);\n" +
                " cbrt \n" +
                "------\n" +
                "NULL"
        );

        this.q("SELECT f.f1, cbrt(f.f1) AS cbrt_f1 FROM FLOAT4_TBL f;\n" +
                "          f1          |       cbrt_f1        \n" +
                "----------------------+----------------------\n" +
                "                    0 |                    0\n" +
                "               1004.3 |    10.01431279725316\n" +
                "               -34.84 |   -3.266074218210196\n" +
                "  1.2345678901234e+20 |    4979338.599670366\n" +
                "  1.2345678901234e-20 | 2.3112042296104156e-07"
        );
    }
}
