package org.dbsp.sqlCompiler.compiler.sql.postgres;

import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.sql.SqlIoTest;
import org.junit.Ignore;
import org.junit.Test;

public class PostgresInt2Tests extends SqlIoTest {
    @Override
    public void prepareData(DBSPCompiler compiler) {
        String createTable = "CREATE TABLE INT2_TBL(f1 int2)";

        String insert = "INSERT INTO INT2_TBL(f1) VALUES\n" +
                "  (0),\n" +
                "  (1234),\n" +
                "  (-1234),\n" +
                "  (32767),\n" +
                "  (-32767);";

        compiler.compileStatement(createTable);
        compiler.compileStatements(insert);
    }

    @Test
    public void testSelect() {
        this.qs(
                "SELECT * FROM INT2_TBL;\n" +
                        " f1 \n" +
                        "----\n" +
                        " 0  \n" +
                        " 1234 \n" +
                        " -1234 \n" +
                        " 32767 \n" +
                        " -32767 \n" +
                        "(5 rows)\n" +
                        "\n" +
                        "SELECT i.* FROM INT2_TBL i WHERE i.f1 <> 0::INT2;\n" +
                        " f1 \n" +
                        "----\n" +
                        " 1234 \n" +
                        " -1234 \n" +
                        " 32767 \n" +
                        " -32767 \n" +
                        "(4 rows)\n" +
                        "\n" +
                        "SELECT i.* FROM INT2_TBL i WHERE i.f1 <> 0::INT4;\n" +
                        " f1 \n" +
                        "----\n" +
                        " 1234 \n" +
                        " -1234 \n" +
                        " 32767 \n" +
                        " -32767 \n" +
                        "(4 rows)\n" +
                        "\n" +
                        "SELECT i.* FROM INT2_TBL i WHERE i.f1 = 0::INT2;\n" +
                        " f1 \n" +
                        "----\n" +
                        " 0 \n" +
                        "(1 row)\n" +
                        "\n" +
                        "SELECT i.* FROM INT2_TBL i WHERE i.f1 = 0::INT4;\n" +
                        " f1 \n" +
                        "----\n" +
                        " 0 \n" +
                        "(1 row)\n" +
                        "\n" +
                        "SELECT i.* FROM INT2_TBL i WHERE i.f1 < 0::INT2;\n" +
                        " f1 \n" +
                        "----\n" +
                        " -1234 \n" +
                        " -32767 \n" +
                        "(2 rows)\n" +
                        "\n" +
                        "SELECT i.* FROM INT2_TBL i WHERE i.f1 < 0::INT4;\n" +
                        " f1 \n" +
                        "----\n" +
                        " -1234 \n" +
                        " -32767 \n" +
                        "(2 rows)\n" +
                        "\n" +
                        "SELECT i.* FROM INT2_TBL i WHERE i.f1 <= '0'::INT2;\n" +
                        " f1 \n" +
                        "----\n" +
                        " 0 \n" +
                        " -1234 \n" +
                        " -32767 \n" +
                        "(3 rows)\n" +
                        "\n" +
                        "SELECT i.* FROM INT2_TBL i WHERE i.f1 <= '0'::INT4;\n" +
                        " f1 \n" +
                        "----\n" +
                        " 0 \n" +
                        " -1234 \n" +
                        " -32767 \n" +
                        "(3 rows)\n" +
                        "\n" +
                        "SELECT i.* FROM INT2_TBL i WHERE i.f1 > '0'::INT2;\n" +
                        " f1 \n" +
                        "----\n" +
                        " 1234 \n" +
                        " 32767 \n" +
                        "(2 rows)\n" +
                        "\n" +
                        "SELECT i.* FROM INT2_TBL i WHERE i.f1 > '0'::INT4;\n" +
                        " f1 \n" +
                        "----\n" +
                        " 1234 \n" +
                        " 32767 \n" +
                        "(2 rows)\n" +
                        "\n" +
                        "SELECT i.* FROM INT2_TBL i WHERE i.f1 >= '0'::INT2;\n" +
                        " f1 \n" +
                        "----\n" +
                        " 0 \n" +
                        " 1234 \n" +
                        " 32767 \n" +
                        "(3 rows)\n" +
                        "\n" +
                        "SELECT i.* FROM INT2_TBL i WHERE i.f1 >= '0'::INT4;\n" +
                        " f1 \n" +
                        "----\n" +
                        " 0 \n" +
                        " 1234 \n" +
                        " 32767 \n" +
                        "(3 rows)\n" +
                        "\n" +
                        "SELECT i.* FROM INT2_TBL i WHERE (i.f1 % '2'::INT2) = '1'::INT2;\n" +
                        " f1 \n" +
                        "----\n" +
                        " 32767\n" +
                        "(1 row)\n" +
                        "\n" +
                        "SELECT i.* FROM INT2_TBL i WHERE (i.f1 % '2'::INT4) = '0'::INT2;\n" +
                        " f1 \n" +
                        "----\n" +
                        " 0\n" +
                        " 1234\n" +
                        " -1234\n" +
                        "(3 rows)\n" +
                        "\n" +
                        "SELECT i.f1, i.f1 * '2'::INT2 AS x FROM INT2_TBL i WHERE abs(f1) < 16384;\n" +
                        "  f1   |   x   \n" +
                        "-------+-------\n" +
                        "     0 |     0\n" +
                        "  1234 |  2468\n" +
                        " -1234 | -2468\n" +
                        "(3 rows)\n" +
                        "\n" +
                        "SELECT i.f1, i.f1 * '2'::INT4 AS x FROM INT2_TBL i;\n" +
                        "   f1   |   x    \n" +
                        "--------+--------\n" +
                        "      0 |      0\n" +
                        "   1234 |   2468\n" +
                        "  -1234 |  -2468\n" +
                        "  32767 |  65534\n" +
                        " -32767 | -65534\n" +
                        "(5 rows)\n" +
                        "\n" +
                        "SELECT i.f1, i.f1 + '2'::INT2 AS x FROM INT2_TBL i WHERE f1 < 32766;\n" +
                        "   f1   |   x    \n" +
                        "--------+--------\n" +
                        "      0 |      2\n" +
                        "   1234 |   1236\n" +
                        "  -1234 |  -1232\n" +
                        " -32767 | -32765\n" +
                        "(4 rows)\n" +
                        "\n" +
                        "SELECT i.f1, i.f1 + '2'::INT4 AS x FROM INT2_TBL i;\n" +
                        "   f1   |   x    \n" +
                        "--------+--------\n" +
                        "      0 |      2\n" +
                        "   1234 |   1236\n" +
                        "  -1234 |  -1232\n" +
                        "  32767 |  32769\n" +
                        " -32767 | -32765\n" +
                        "(5 rows)\n" +
                        "\n" +
                        "SELECT i.f1, i.f1 - '2'::INT2 AS x FROM INT2_TBL i WHERE f1 > -32767;\n" +
                        "  f1   |   x   \n" +
                        "-------+-------\n" +
                        "     0 |    -2\n" +
                        "  1234 |  1232\n" +
                        " -1234 | -1236\n" +
                        " 32767 | 32765\n" +
                        "(4 rows)\n" +
                        "\n" +
                        "SELECT i.f1, i.f1 - '2'::INT4 AS x FROM INT2_TBL i;\n" +
                        "   f1   |   x    \n" +
                        "--------+--------\n" +
                        "      0 |     -2\n" +
                        "   1234 |   1232\n" +
                        "  -1234 |  -1236\n" +
                        "  32767 |  32765\n" +
                        " -32767 | -32769\n" +
                        "(5 rows)\n" +
                        "\n" +
                        "SELECT i.f1, i.f1 / '2'::INT4 AS x FROM INT2_TBL i;\n" +
                        "   f1   |   x    \n" +
                        "--------+--------\n" +
                        "      0 |      0\n" +
                        "   1234 |    617\n" +
                        "  -1234 |   -617\n" +
                        "  32767 |  16383\n" +
                        " -32767 | -16383\n" +
                        "(5 rows)\n" +
                        "\n" +
                        "SELECT i.f1, i.f1 / '2'::INT4 AS x FROM INT2_TBL i;\n" +
                        "   f1   |   x    \n" +
                        "--------+--------\n" +
                        "      0 |      0\n" +
                        "   1234 |    617\n" +
                        "  -1234 |   -617\n" +
                        "  32767 |  16383\n" +
                        " -32767 | -16383\n" +
                        "(5 rows)\n" +
                        "\n"
        );
    }

    @Test
    public void testBadSelect() {
        this.shouldFail("SELECT * FROM INT2_TBL AS s (a, b)", "List of column aliases must have same degree as table; table has 1 columns ('F1'), whereas alias list has 2 columns");

    }

    // We round differently than Postgres
    // R: ("-1.5", -2)x1  L: ("-1.5", -1)x1
    // R: ("-0.5", 0)x1   L: ("-0.5", 0)x1
    // R: ("1.5", 2)x1    L: ("1.5", 1)x1
    @Test @Ignore("fails because we round differently than Postgres")
    public void testFloatRound() {
        this.q(
                "SELECT x, x::int2 AS int2_value " +
                        "FROM (VALUES (-2.5::float8)," +
                        "             (-1.5::float8)," +
                        "             (-0.5::float8)," +
                        "             (0.0::float8)," +
                        "             (0.5::float8)," +
                        "             (1.5::float8)," +
                        "             (2.5::float8)) t(x);\n" +
                        "  x   | int2_value \n" +
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

    // Fails because we round differently than Postgres
    // R: (-2.5, -3)x1  L: (-2.5, -2)x1
    // R: (-1.5, -2)x1  L: (-1.5, -1)x1
    // R: (-0.5, -1)x1  L: (-0.5, 0)x1
    // R: (0.5, 1)x1    L: (0.5, 0)x1
    // R: (1.5, 2)x1    L: (1.5, 1)x1
    // R: (2.5, 3)x1    L: (2.5, 2)x1
    @Test @Ignore("fails because we round differently than Postgres")
    public void testNumericRound() {
        this.q(
                "SELECT x, x::int2 AS int2_value " +
                        "FROM (VALUES (-2.5::numeric)," +
                        "             (-1.5::numeric)," +
                        "             (-0.5::numeric)," +
                        "             (0.0::numeric)," +
                        "             (0.5::numeric)," +
                        "             (1.5::numeric)," +
                        "             (2.5::numeric)) t(x);\n" +
                        "  x   | int2_value \n" +
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

    @Test @Ignore("non decimals not suppported yet")
    public void testNonDecimals() {
        this.qs(
                "SELECT '0b100101'::INT2 as x;\n" +
                        " x \n" +
                        "---\n" +
                        " 37\n" +
                        "(1 row)\n" +
                        "\n" +
                        "SELECT '0o273'::INT2;\n" +
                        " x \n" +
                        "---\n" +
                        " 187\n" +
                        "(1 row)\n" +
                        "\n" +
                        "SELECT '0x42F'::INT2;\n" +
                        " x \n" +
                        "---\n" +
                        " 1071\n" +
                        "(1 row)\n" +
                        "\n"
        );
    }

    @Test @Ignore("unimplemented operator")
    public void testLeftShift() {
        this.qs(
        "SELECT (-1::INT2 << 15)::TEXT;\n" +
                "  text  \n" +
                "--------\n" +
                " -32768\n" +
                "(1 row)\n" +
                "\n" +
                "SELECT ((-1::int2<<15)+1::int2)::text;\n" +
                "  text  \n" +
                "--------\n" +
                " -32767\n" +
                "(1 row)"
        );
    }

    // Ignored because this fails in Postgres but here we get:
    @Test @Ignore
    public void testSelectOverflow() {
        String error_message = "INT2 out of range";
        // We get:
        // L: (Some(-32767), Some(2))x1 --> wraps around
        // L: (Some(-1234), Some(-2468))x1
        // L: (Some(0), Some(0))x1
        // L: (Some(1234), Some(2468))x1
        // L: (Some(32767), Some(-2))x1 --> wraps around
        // Taken from: https://github.com/postgres/postgres/blob/c161ab74f76af8e0f3c6b349438525ad9575683b/src/test/regress/expected/int2.out#L202C1-L203C30
        this.shouldFail("SELECT i.f1, i.f1 * 2::INT2 AS x FROM INT2_TBL i", error_message);

        // We get:
        // L: (Some(-32767), Some(-32765))x1
        // L: (Some(-1234), Some(-1232))x1
        // L: (Some(0), Some(2))x1
        // L: (Some(1234), Some(1236))x1
        // L: (Some(32767), Some(-32767))x1 --> wraps around
        // https://github.com/postgres/postgres/blob/c161ab74f76af8e0f3c6b349438525ad9575683b/src/test/regress/expected/int2.out#L223
        this.shouldFail( "SELECT i.f1, i.f1 + '2'::INT2 AS x FROM INT2_TBL i", error_message);

        // Similarly,
        this.shouldFail("SELECT i.f1, i.f1 - '2'::INT2 AS x FROM INT2_TBL i;", error_message);
    }

    // This passes for the Calcite version but fails for the run time version
    // error [E0080]:
    //  |
    //2 | println!("{}", -32768i16 % -1i16);
    //  |                ^^^^^^^^^^^^^^^^^ attempt to compute `i16::MIN % -1_i16`, which would overflow
    //  |
    //  = note: `#[deny(unconditional_panic)]` on by default
    //  |
    //2 | println!("{}", -32768i16 % -1i16);
    //  |                ^^^^^^^^^^^^^^^^^ overflow in signed remainder (dividing MIN by -1)
    @Test @Ignore
    public void testINT2MINOverflow() {
        this.q(
                "SELECT (-32768)::int2 % (-1)::int2 as x;\n" +
                        " x \n" +
                        "---\n" +
                        "  0"
        );
    }

    @Test @Ignore
    public void testINT2MINOverflowError() {
        String error = "INT2 out of range";

        // This fails in Postgres, but we get: `-32768`
        this.shouldFail("SELECT (-32768)::int2 * (-1)::int2", error);

        // This panics in run time
        this.shouldFail("SELECT (-32768)::int2 / (-1)::int2", error);
    }

    @Test @Ignore("underscores not supported yet")
    public void testUnderscores() {
        this.qs(
                "SELECT '1_000'::INT2;\n" +
                        " int2 \n" +
                        "------\n" +
                        " 1000\n" +
                        "(1 row)\n" +
                        "\n" +
                        "SELECT '1_2_3'::INT2;\n" +
                        " int2 \n" +
                        "------\n" +
                        "  123\n" +
                        "(1 row)"
        );
    }
}
