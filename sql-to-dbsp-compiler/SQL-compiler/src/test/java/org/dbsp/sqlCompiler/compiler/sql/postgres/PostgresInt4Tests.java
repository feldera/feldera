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
    @Test @Ignore
    public void testSelectOverflow() {
        String error = "INT4 out of range";

        this.shouldFail("SELECT i.f1, i.f1 * '2'::INT4 AS x FROM INT4_TBL i", error);
        this.shouldFail("SELECT i.f1, i.f1 * '2'::INT4 AS x FROM INT4_TBL i", error);
        this.shouldFail("SELECT i.f1, i.f1 + '2'::INT2 AS x FROM INT4_TBL i", error);
        this.shouldFail("SELECT i.f1, i.f1 + '2'::INT4 AS x FROM INT4_TBL i", error);
        this.shouldFail("SELECT i.f1, i.f1 - '2'::INT2 AS x FROM INT4_TBL i", error);
        this.shouldFail("SELECT i.f1, i.f1 - '2'::INT4 AS x FROM INT4_TBL i", error);
    }

    @Test @Ignore("shift operator is not supported")
    public void testLeftShift() {
        this.qs(
                "SELECT (-1::int4<<31)::text;\n" +
                        "    text     \n" +
                        "-------------\n" +
                        " -2147483648\n" +
                        "(1 row)\n" +
                        "\n" +
                        "SELECT ((-1::int4<<31)+1)::text;\n" +
                        "    text     \n" +
                        "-------------\n" +
                        " -2147483647\n" +
                        "(1 row)"
        );
    }

    // This passes for the Calcite version but fails for the run time version
    // Check PostgresInt2Tests::testINT2MINOverflow for details
    @Test @Ignore
    public void testINT4MINOverflow() {
        this.q(
                "SELECT (-2147483648)::int4 % (-1)::int4 as x;\n" +
                        " x \n" +
                        "---\n" +
                        "  0"
        );
    }

    @Test @Ignore
    public void testINT4MINOverflowError() {
        String error = "INT4 out of range";

        this.shouldFail("SELECT (-2147483648)::int4 * (-1)::int2", error);
        this.shouldFail("SELECT (-2147483648)::int4 / (-1)::int2", error);
    }

    @Test @Ignore("fails because we round differently than Postgres")
    public void testFloatRound() {
        this.q(
                "SELECT x, x::int4 AS int4_value\n" +
                        "FROM (VALUES (-2.5::float8),\n" +
                        "             (-1.5::float8),\n" +
                        "             (-0.5::float8),\n" +
                        "             (0.0::float8),\n" +
                        "             (0.5::float8),\n" +
                        "             (1.5::float8),\n" +
                        "             (2.5::float8)) t(x);\n" +
                        "  x   | int4_value \n" +
                        "------+------------\n" +
                        " -2.5 |         -2\n" +
                        " -1.5 |         -2\n" +
                        " -0.5 |          0\n" +
                        "    0 |          0\n" +
                        "  0.5 |          0\n" +
                        "  1.5 |          2\n" +
                        "  2.5 |          2"
        );
    }
    @Test @Ignore("fails because we round differently than Postgres")
    public void testNumericRound() {
        this.q(
                "SELECT x, x::int4 AS int4_value\n" +
                        "FROM (VALUES (-2.5::numeric),\n" +
                        "             (-1.5::numeric),\n" +
                        "             (-0.5::numeric),\n" +
                        "             (0.0::numeric),\n" +
                        "             (0.5::numeric),\n" +
                        "             (1.5::numeric),\n" +
                        "             (2.5::numeric)) t(x);\n" +
                        "  x   | int4_value \n" +
                        "------+------------\n" +
                        " -2.5 |         -3\n" +
                        " -1.5 |         -2\n" +
                        " -0.5 |         -1\n" +
                        "  0.0 |          0\n" +
                        "  0.5 |          1\n" +
                        "  1.5 |          2\n" +
                        "  2.5 |          3"
        );
    }

    @Test @Ignore("GCD not supported yet")
    public void testGCD() {
        this.q(
                "SELECT a, b, gcd(a, b), gcd(a, -b), gcd(b, a), gcd(-b, a)\n" +
                        "FROM (VALUES (0::int4, 0::int4),\n" +
                        "             (0::int4, 6410818::int4),\n" +
                        "             (61866666::int4, 6410818::int4),\n" +
                        "             (-61866666::int4, 6410818::int4),\n" +
                        "             ((-2147483648)::int4, 1::int4),\n" +
                        "             ((-2147483648)::int4, 2147483647::int4),\n" +
                        "             ((-2147483648)::int4, 1073741824::int4)) AS v(a, b);\n" +
                        "      a      |     b      |    gcd     |    gcd     |    gcd     |    gcd     \n" +
                        "-------------+------------+------------+------------+------------+------------\n" +
                        "           0 |          0 |          0 |          0 |          0 |          0\n" +
                        "           0 |    6410818 |    6410818 |    6410818 |    6410818 |    6410818\n" +
                        "    61866666 |    6410818 |       1466 |       1466 |       1466 |       1466\n" +
                        "   -61866666 |    6410818 |       1466 |       1466 |       1466 |       1466\n" +
                        " -2147483648 |          1 |          1 |          1 |          1 |          1\n" +
                        " -2147483648 | 2147483647 |          1 |          1 |          1 |          1\n" +
                        " -2147483648 | 1073741824 | 1073741824 | 1073741824 | 1073741824 | 1073741824"
        );
    }

    @Test @Ignore("LCM not supported yet")
    public void testLCM() {
        this.q(
                "SELECT a, b, lcm(a, b), lcm(a, -b), lcm(b, a), lcm(-b, a)\n" +
                        "FROM (VALUES (0::int4, 0::int4),\n" +
                        "             (0::int4, 42::int4),\n" +
                        "             (42::int4, 42::int4),\n" +
                        "             (330::int4, 462::int4),\n" +
                        "             (-330::int4, 462::int4),\n" +
                        "             ((-2147483648)::int4, 0::int4)) AS v(a, b);\n" +
                        "      a      |  b  | lcm  | lcm  | lcm  | lcm  \n" +
                        "-------------+-----+------+------+------+------\n" +
                        "           0 |   0 |    0 |    0 |    0 |    0\n" +
                        "           0 |  42 |    0 |    0 |    0 |    0\n" +
                        "          42 |  42 |   42 |   42 |   42 |   42\n" +
                        "         330 | 462 | 2310 | 2310 | 2310 | 2310\n" +
                        "        -330 | 462 | 2310 | 2310 | 2310 | 2310\n" +
                        " -2147483648 |   0 |    0 |    0 |    0 |    0"
        );
    }

}
