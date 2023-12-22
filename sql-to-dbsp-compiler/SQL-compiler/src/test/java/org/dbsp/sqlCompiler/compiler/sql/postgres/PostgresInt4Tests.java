package org.dbsp.sqlCompiler.compiler.sql.postgres;

import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.sql.SqlIoTest;
import org.junit.Ignore;
import org.junit.Test;

public class PostgresInt4Tests extends SqlIoTest {
    @Override
    public void prepareData(DBSPCompiler compiler) {
        String createTable = "CREATE TABLE INT4_TBL(f1 int4)";

        String insert =
                "INSERT INTO INT4_TBL(f1) VALUES\n" +
                "  ('0'),\n" +
                "  ('123456'),\n" +
                "  ('-123456'),\n" +
                "  ('2147483647'),\n" +
                "  ('-2147483647');";

        compiler.compileStatement(createTable);
        compiler.compileStatements(insert);
    }

    @Test
    public void testSelect() {
        this.qs(
                "SELECT i.* FROM INT4_TBL i WHERE i.f1 <> '0'::INT2;\n" +
                        "     f1      \n" +
                        "-------------\n" +
                        "      123456\n" +
                        "     -123456\n" +
                        "  2147483647\n" +
                        " -2147483647\n" +
                        "(4 rows)\n" +
                        "\n" +
                        "SELECT i.* FROM INT4_TBL i WHERE i.f1 <> '0'::INT4;\n" +
                        "     f1      \n" +
                        "-------------\n" +
                        "      123456\n" +
                        "     -123456\n" +
                        "  2147483647\n" +
                        " -2147483647\n" +
                        "(4 rows)\n" +
                        "\n" +
                        "SELECT i.* FROM INT4_TBL i WHERE i.f1 = '0'::INT2;\n" +
                        " f1 \n" +
                        "----\n" +
                        "  0\n" +
                        "(1 row)\n" +
                        "\n" +
                        "SELECT i.* FROM INT4_TBL i WHERE i.f1 = '0'::INT4;\n" +
                        " f1 \n" +
                        "----\n" +
                        "  0\n" +
                        "(1 row)\n" +
                        "\n" +
                        "SELECT i.* FROM INT4_TBL i WHERE i.f1 < '0'::INT2;\n" +
                        "     f1      \n" +
                        "-------------\n" +
                        "     -123456\n" +
                        " -2147483647\n" +
                        "(2 rows)\n" +
                        "\n" +
                        "SELECT i.* FROM INT4_TBL i WHERE i.f1 < '0'::INT4;\n" +
                        "     f1      \n" +
                        "-------------\n" +
                        "     -123456\n" +
                        " -2147483647\n" +
                        "(2 rows)\n" +
                        "\n" +
                        "SELECT i.* FROM INT4_TBL i WHERE i.f1 <= '0'::INT2;\n" +
                        "     f1      \n" +
                        "-------------\n" +
                        "           0\n" +
                        "     -123456\n" +
                        " -2147483647\n" +
                        "(3 rows)\n" +
                        "\n" +
                        "SELECT i.* FROM INT4_TBL i WHERE i.f1 <= '0'::INT4;\n" +
                        "     f1      \n" +
                        "-------------\n" +
                        "           0\n" +
                        "     -123456\n" +
                        " -2147483647\n" +
                        "(3 rows)\n" +
                        "\n" +
                        "SELECT i.* FROM INT4_TBL i WHERE i.f1 > '0'::INT2;\n" +
                        "     f1     \n" +
                        "------------\n" +
                        "     123456\n" +
                        " 2147483647\n" +
                        "(2 rows)\n" +
                        "\n" +
                        "SELECT i.* FROM INT4_TBL i WHERE i.f1 > '0'::INT4;\n" +
                        "     f1     \n" +
                        "------------\n" +
                        "     123456\n" +
                        " 2147483647\n" +
                        "(2 rows)\n" +
                        "\n" +
                        "SELECT i.* FROM INT4_TBL i WHERE i.f1 >= '0'::INT2;\n" +
                        "     f1     \n" +
                        "------------\n" +
                        "          0\n" +
                        "     123456\n" +
                        " 2147483647\n" +
                        "(3 rows)\n" +
                        "\n" +
                        "SELECT i.* FROM INT4_TBL i WHERE i.f1 >= '0'::INT4;\n" +
                        "     f1     \n" +
                        "------------\n" +
                        "          0\n" +
                        "     123456\n" +
                        " 2147483647\n" +
                        "(3 rows)\n" +
                        "\n" +
                        "-- positive odds\n" +
                        "SELECT i.* FROM INT4_TBL i WHERE (i.f1 % '2'::INT2) = '1'::INT2;\n" +
                        "     f1     \n" +
                        "------------\n" +
                        " 2147483647\n" +
                        "(1 row)\n" +
                        "\n" +
                        "-- any evens\n" +
                        "SELECT i.* FROM INT4_TBL i WHERE (i.f1 % '2'::INT4) = '0'::INT2;\n" +
                        "   f1    \n" +
                        "---------\n" +
                        "       0\n" +
                        "  123456\n" +
                        " -123456\n" +
                        "(3 rows)\n" +
                        "\n" +
                        "SELECT i.f1, i.f1 * '2'::INT2 AS x FROM INT4_TBL i\n" +
                        "WHERE abs(f1) < 1073741824;\n" +
                        "   f1    |    x    \n" +
                        "---------+---------\n" +
                        "       0 |       0\n" +
                        "  123456 |  246912\n" +
                        " -123456 | -246912\n" +
                        "(3 rows)\n" +
                        "\n" +
                        "SELECT i.f1, i.f1 * '2'::INT4 AS x FROM INT4_TBL i\n" +
                        "WHERE abs(f1) < 1073741824;\n" +
                        "   f1    |    x    \n" +
                        "---------+---------\n" +
                        "       0 |       0\n" +
                        "  123456 |  246912\n" +
                        " -123456 | -246912\n" +
                        "(3 rows)\n" +
                        "\n" +
                        "SELECT i.f1, i.f1 + '2'::INT2 AS x FROM INT4_TBL i\n" +
                        "WHERE f1 < 2147483646;\n" +
                        "     f1      |      x      \n" +
                        "-------------+-------------\n" +
                        "           0 |           2\n" +
                        "      123456 |      123458\n" +
                        "     -123456 |     -123454\n" +
                        " -2147483647 | -2147483645\n" +
                        "(4 rows)\n" +
                        "\n" +
                        "SELECT i.f1, i.f1 + '2'::INT4 AS x FROM INT4_TBL i\n" +
                        "WHERE f1 < 2147483646;\n" +
                        "     f1      |      x      \n" +
                        "-------------+-------------\n" +
                        "           0 |           2\n" +
                        "      123456 |      123458\n" +
                        "     -123456 |     -123454\n" +
                        " -2147483647 | -2147483645\n" +
                        "(4 rows)\n" +
                        "\n" +
                        "SELECT i.f1, i.f1 - '2'::INT2 AS x FROM INT4_TBL i\n" +
                        "WHERE f1 > -2147483647;\n" +
                        "     f1     |     x      \n" +
                        "------------+------------\n" +
                        "          0 |         -2\n" +
                        "     123456 |     123454\n" +
                        "    -123456 |    -123458\n" +
                        " 2147483647 | 2147483645\n" +
                        "(4 rows)\n" +
                        "\n" +
                        "SELECT i.f1, i.f1 - '2'::INT4 AS x FROM INT4_TBL i\n" +
                        "WHERE f1 > -2147483647;\n" +
                        "     f1     |     x      \n" +
                        "------------+------------\n" +
                        "          0 |         -2\n" +
                        "     123456 |     123454\n" +
                        "    -123456 |    -123458\n" +
                        " 2147483647 | 2147483645\n" +
                        "(4 rows)\n" +
                        "\n" +
                        "SELECT i.f1, i.f1 / '2'::INT2 AS x FROM INT4_TBL i;\n" +
                        "     f1      |      x      \n" +
                        "-------------+-------------\n" +
                        "           0 |           0\n" +
                        "      123456 |       61728\n" +
                        "     -123456 |      -61728\n" +
                        "  2147483647 |  1073741823\n" +
                        " -2147483647 | -1073741823\n" +
                        "(5 rows)\n" +
                        "\n" +
                        "SELECT i.f1, i.f1 / '2'::INT4 AS x FROM INT4_TBL i;\n" +
                        "     f1      |      x      \n" +
                        "-------------+-------------\n" +
                        "           0 |           0\n" +
                        "      123456 |       61728\n" +
                        "     -123456 |      -61728\n" +
                        "  2147483647 |  1073741823\n" +
                        " -2147483647 | -1073741823\n" +
                        "(5 rows)\n" +
                        "\n" +
                        "SELECT -2+3 AS one;\n" +
                        " one \n" +
                        "-----\n" +
                        "   1\n" +
                        "(1 row)\n" +
                        "\n" +
                        "SELECT 4-2 AS two;\n" +
                        " two \n" +
                        "-----\n" +
                        "   2\n" +
                        "(1 row)\n" +
                        "\n" +
                        "SELECT 2- -1 AS three;\n" +
                        " three \n" +
                        "-------\n" +
                        "     3\n" +
                        "(1 row)\n" +
                        "\n" +
                        "SELECT 2 - -2 AS four;\n" +
                        " four \n" +
                        "------\n" +
                        "    4\n" +
                        "(1 row)\n" +
                        "\n" +
                        "SELECT '2'::INT2 * '2'::INT2 = '16'::INT2 / '4'::INT2 AS true;\n" +
                        " true \n" +
                        "------\n" +
                        " t\n" +
                        "(1 row)\n" +
                        "\n" +
                        "SELECT '2'::INT4 * '2'::INT2 = '16'::INT2 / '4'::INT4 AS true;\n" +
                        " true \n" +
                        "------\n" +
                        " t\n" +
                        "(1 row)\n" +
                        "\n" +
                        "SELECT '2'::INT2 * '2'::INT4 = '16'::INT4 / '4'::INT2 AS true;\n" +
                        " true \n" +
                        "------\n" +
                        " t\n" +
                        "(1 row)\n" +
                        "\n" +
                        "SELECT '1000'::INT4 < '999'::INT4 AS false;\n" +
                        " false \n" +
                        "-------\n" +
                        " f\n" +
                        "(1 row)\n" +
                        "\n" +
                        "SELECT 1 + 1 + 1 + 1 + 1 + 1 + 1 + 1 + 1 + 1 AS ten;\n" +
                        " ten \n" +
                        "-----\n" +
                        "  10\n" +
                        "(1 row)\n" +
                        "\n" +
                        "SELECT 2 + 2 / 2 AS three;\n" +
                        " three \n" +
                        "-------\n" +
                        "     3\n" +
                        "(1 row)\n" +
                        "\n" +
                        "SELECT (2 + 2) / 2 AS two;\n" +
                        " two \n" +
                        "-----\n" +
                        "   2\n" +
                        "(1 row)"
        );
    }

    // Check PostgresInt2Tests::testSelectOverflow for details
    @Test @Ignore("Integer wrapping: https://github.com/feldera/feldera/issues/1186")
    public void testSelectOverflow() {
        String error = "INT4 out of range";

        // We get:
        // L: (Some(-2147483647), Some(2))x1 --> wraps around
        // L: (Some(-123456), Some(-246912))x1
        // L: (Some(0), Some(0))x1
        // L: (Some(123456), Some(246912))x1
        // L: (Some(2147483647), Some(-2))x1 --> wraps around
        this.shouldFail("SELECT i.f1, i.f1 * '2'::INT2 AS x FROM INT4_TBL i", error);

        // We get:
        // L: (Some(-2147483647), Some(2))x1 --> wraps around
        // L: (Some(-123456), Some(-246912))x1
        // L: (Some(0), Some(0))x1
        // L: (Some(123456), Some(246912))x1
        // L: (Some(2147483647), Some(-2))x1 --> wraps around
        this.shouldFail("SELECT i.f1, i.f1 * '2'::INT4 AS x FROM INT4_TBL i", error);

        // We get:
        // L: (Some(-2147483647), Some(-2147483645))x1
        // L: (Some(-123456), Some(-123454))x1
        // L: (Some(0), Some(2))x1
        // L: (Some(123456), Some(123458))x1
        // L: (Some(2147483647), Some(-2147483647))x1 --> wraps around
        this.shouldFail("SELECT i.f1, i.f1 + '2'::INT2 AS x FROM INT4_TBL i", error);

        // We get:
        // L: (Some(-2147483647), Some(-2147483645))x1
        // L: (Some(-123456), Some(-123454))x1
        // L: (Some(0), Some(2))x1
        // L: (Some(123456), Some(123458))x1
        // L: (Some(2147483647), Some(-2147483647))x1 --> wraps around
        this.shouldFail("SELECT i.f1, i.f1 + '2'::INT4 AS x FROM INT4_TBL i", error);

        // We get:
        // L: (Some(-2147483647), Some(2147483647))x1 --> wraps around
        // L: (Some(-123456), Some(-123458))x1
        // L: (Some(0), Some(-2))x1
        // L: (Some(123456), Some(123454))x1
        // L: (Some(2147483647), Some(2147483645))x1
        this.shouldFail("SELECT i.f1, i.f1 - '2'::INT2 AS x FROM INT4_TBL i", error);

        // We get:
        // L: (Some(-2147483647), Some(2147483647))x1 --> wraps around
        // L: (Some(-123456), Some(-123458))x1
        // L: (Some(0), Some(-2))x1
        // L: (Some(123456), Some(123454))x1
        // L: (Some(2147483647), Some(2147483645))x1
        this.shouldFail("SELECT i.f1, i.f1 - '2'::INT4 AS x FROM INT4_TBL i", error);
    }

    // This passes for the Calcite version but fails for the run time version
    // Check PostgresInt2Tests::testINT2MINOverflow for details
    @Test @Ignore("Modulo edge case integer overflow: https://github.com/feldera/feldera/issues/1187")
    public void testINT4MINOverflow() {
        this.q(
                "SELECT (-2147483648)::int4 % (-1)::int4 as x;\n" +
                        " x \n" +
                        "---\n" +
                        "  0"
        );
    }

    @Test @Ignore("Integer wrapping: https://github.com/feldera/feldera/issues/1186")
    public void testINT4MINOverflowError() {
        String error = "INT4 out of range";

        this.shouldFail("SELECT (-2147483648)::int4 * (-1)::int2", error);
        this.shouldFail("SELECT (-2147483648)::int4 / (-1)::int2", error);
    }

    @Test
    public void testFloatRound() {
        this.q(
                "SELECT x, x::int4 AS int4_value\n" +
                        "FROM (VALUES (-2.9::float8),\n" +
                        "             (-2.5::float8),\n" +
                        "             (-1.5::float8),\n" +
                        "             (-0.5::float8),\n" +
                        "             (0.0::float8),\n" +
                        "             (0.5::float8),\n" +
                        "             (1.5::float8),\n" +
                        "             (2.5::float8)) t(x);\n" +
                        "  x   | int4_value \n" +
                        "------+------------\n" +
                        " -2.9 |         -2\n" +
                        " -2.5 |         -2\n" +
                        " -1.5 |         -1\n" +
                        " -0.5 |          0\n" +
                        "    0 |          0\n" +
                        "  0.5 |          0\n" +
                        "  1.5 |          1\n" +
                        "  2.5 |          2"
        );
    }
    @Test
    public void testNumericRound() {
        this.q(
                "SELECT x, x::int4 AS int4_value\n" +
                        "FROM (VALUES (-2.9::numeric),\n" +
                        "             (-2.5::numeric),\n" +
                        "             (-1.5::numeric),\n" +
                        "             (-0.5::numeric),\n" +
                        "             (0.0::numeric),\n" +
                        "             (0.5::numeric),\n" +
                        "             (1.5::numeric),\n" +
                        "             (2.5::numeric)) t(x);\n" +
                        "  x   | int4_value \n" +
                        "------+------------\n" +
                        " -2.9 |         -2\n" +
                        " -2.5 |         -2\n" +
                        " -1.5 |         -1\n" +
                        " -0.5 |         -0\n" +
                        "  0.0 |          0\n" +
                        "  0.5 |          0\n" +
                        "  1.5 |          1\n" +
                        "  2.5 |          2"
        );
    }
}
