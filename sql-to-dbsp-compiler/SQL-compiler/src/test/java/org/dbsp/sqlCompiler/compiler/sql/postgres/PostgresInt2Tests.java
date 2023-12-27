package org.dbsp.sqlCompiler.compiler.sql.postgres;

import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.sql.SqlIoTest;
import org.junit.Ignore;
import org.junit.Test;

/**
 * Tests manually adopted from
 * https://github.com/postgres/postgres/blob/master/src/test/regress/expected/int2.out
 */
@SuppressWarnings("JavadocLinkAsPlainText")
public class PostgresInt2Tests extends SqlIoTest {
    @Override
    public void prepareData(DBSPCompiler compiler) {
        String createTable = "CREATE TABLE INT2_TBL(f1 int2)";

        String insert = """
                INSERT INTO INT2_TBL(f1) VALUES
                  (0),
                  (1234),
                  (-1234),
                  (32767),
                  (-32767);""";

        compiler.compileStatement(createTable);
        compiler.compileStatements(insert);
    }

    @Test
    public void testSelect() {
        this.qs(
                """
                        SELECT * FROM INT2_TBL;
                         f1
                        ----
                         0
                         1234
                         -1234
                         32767
                         -32767
                        (5 rows)

                        SELECT i.* FROM INT2_TBL i WHERE i.f1 <> 0::INT2;
                         f1
                        ----
                         1234
                         -1234
                         32767
                         -32767
                        (4 rows)

                        SELECT i.* FROM INT2_TBL i WHERE i.f1 <> 0::INT4;
                         f1
                        ----
                         1234
                         -1234
                         32767
                         -32767
                        (4 rows)

                        SELECT i.* FROM INT2_TBL i WHERE i.f1 = 0::INT2;
                         f1
                        ----
                         0
                        (1 row)

                        SELECT i.* FROM INT2_TBL i WHERE i.f1 = 0::INT4;
                         f1
                        ----
                         0
                        (1 row)

                        SELECT i.* FROM INT2_TBL i WHERE i.f1 < 0::INT2;
                         f1
                        ----
                         -1234
                         -32767
                        (2 rows)

                        SELECT i.* FROM INT2_TBL i WHERE i.f1 < 0::INT4;
                         f1
                        ----
                         -1234
                         -32767
                        (2 rows)

                        SELECT i.* FROM INT2_TBL i WHERE i.f1 <= '0'::INT2;
                         f1
                        ----
                         0
                         -1234
                         -32767
                        (3 rows)

                        SELECT i.* FROM INT2_TBL i WHERE i.f1 <= '0'::INT4;
                         f1
                        ----
                         0
                         -1234
                         -32767
                        (3 rows)

                        SELECT i.* FROM INT2_TBL i WHERE i.f1 > '0'::INT2;
                         f1
                        ----
                         1234
                         32767
                        (2 rows)

                        SELECT i.* FROM INT2_TBL i WHERE i.f1 > '0'::INT4;
                         f1
                        ----
                         1234
                         32767
                        (2 rows)

                        SELECT i.* FROM INT2_TBL i WHERE i.f1 >= '0'::INT2;
                         f1
                        ----
                         0
                         1234
                         32767
                        (3 rows)

                        SELECT i.* FROM INT2_TBL i WHERE i.f1 >= '0'::INT4;
                         f1
                        ----
                         0
                         1234
                         32767
                        (3 rows)

                        SELECT i.* FROM INT2_TBL i WHERE (i.f1 % '2'::INT2) = '1'::INT2;
                         f1
                        ----
                         32767
                        (1 row)

                        SELECT i.* FROM INT2_TBL i WHERE (i.f1 % '2'::INT4) = '0'::INT2;
                         f1
                        ----
                         0
                         1234
                         -1234
                        (3 rows)

                        SELECT i.f1, i.f1 * '2'::INT2 AS x FROM INT2_TBL i WHERE abs(f1) < 16384;
                          f1   |   x
                        -------+-------
                             0 |     0
                          1234 |  2468
                         -1234 | -2468
                        (3 rows)

                        SELECT i.f1, i.f1 * '2'::INT4 AS x FROM INT2_TBL i;
                           f1   |   x
                        --------+--------
                              0 |      0
                           1234 |   2468
                          -1234 |  -2468
                          32767 |  65534
                         -32767 | -65534
                        (5 rows)

                        SELECT i.f1, i.f1 + '2'::INT2 AS x FROM INT2_TBL i WHERE f1 < 32766;
                           f1   |   x
                        --------+--------
                              0 |      2
                           1234 |   1236
                          -1234 |  -1232
                         -32767 | -32765
                        (4 rows)

                        SELECT i.f1, i.f1 + '2'::INT4 AS x FROM INT2_TBL i;
                           f1   |   x
                        --------+--------
                              0 |      2
                           1234 |   1236
                          -1234 |  -1232
                          32767 |  32769
                         -32767 | -32765
                        (5 rows)

                        SELECT i.f1, i.f1 - '2'::INT2 AS x FROM INT2_TBL i WHERE f1 > -32767;
                          f1   |   x
                        -------+-------
                             0 |    -2
                          1234 |  1232
                         -1234 | -1236
                         32767 | 32765
                        (4 rows)

                        SELECT i.f1, i.f1 - '2'::INT4 AS x FROM INT2_TBL i;
                           f1   |   x
                        --------+--------
                              0 |     -2
                           1234 |   1232
                          -1234 |  -1236
                          32767 |  32765
                         -32767 | -32769
                        (5 rows)

                        SELECT i.f1, i.f1 / '2'::INT4 AS x FROM INT2_TBL i;
                           f1   |   x
                        --------+--------
                              0 |      0
                           1234 |    617
                          -1234 |   -617
                          32767 |  16383
                         -32767 | -16383
                        (5 rows)

                        SELECT i.f1, i.f1 / '2'::INT4 AS x FROM INT2_TBL i;
                           f1   |   x
                        --------+--------
                              0 |      0
                           1234 |    617
                          -1234 |   -617
                          32767 |  16383
                         -32767 | -16383
                        (5 rows)

                        """
        );
    }

    @Test
    public void testBadSelect() {
        this.shouldFail("SELECT * FROM INT2_TBL AS s (a, b)", "List of column aliases must have same degree as table; table has 1 columns ('F1'), whereas alias list has 2 columns");
    }

    // We round down to zero
    @Test
    public void testFloatRound() {
        this.q(
                """
                        SELECT x, x::int2 AS int2_value FROM (VALUES (-2.9::float8), (-2.5::float8), (-1.5::float8), (-0.5::float8), (0.0::float8), (0.5::float8), (1.5::float8), (2.5::float8)) t(x);
                          x   | int2_value
                        ------+------------
                         -2.9 |         -2
                         -2.5 |         -2
                         -1.5 |         -1
                         -0.5 |          0
                            0 |          0
                          0.5 |          0
                          1.5 |          1
                          2.5 |          2"""
        );
    }

    // We round down to zero
    @Test
    public void testNumericRound() {
        this.q(
                """
                        SELECT x, x::int2 AS int2_value FROM (VALUES (-2.9::numeric), (-2.5::numeric), (-1.5::numeric), (-0.5::numeric), (0.0::numeric), (0.5::numeric), (1.5::numeric), (2.5::numeric)) t(x);
                          x   | int2_value
                        ------+------------
                         -2.9 |         -2
                         -2.5 |         -2
                         -1.5 |         -1
                         -0.5 |         -0
                          0.0 |          0
                          0.5 |          0
                          1.5 |          1
                          2.5 |          2"""
        );
    }

    // Ignored because this fails in Postgres but here we get:
    @Test @Ignore("Integer wrapping: https://github.com/feldera/feldera/issues/1186")
    public void testSelectOverflow() {
        String error_message = "INT2 out of range";
        // We get:
        // L: (Some(-32767), Some(2))x1 --> wraps around
        // L: (Some(-1234), Some(-2468))x1
        // L: (Some(0), Some(0))x1
        // L: (Some(1234), Some(2468))x1
        // L: (Some(32767), Some(-2))x1 --> wraps around
        // Taken from: https://github.com/postgres/postgres/blob/c161ab74f76af8e0f3c6b349438525ad9575683b/src/test/regress/expected/int2.out#L202C1-L203C30
        this.runtimeFail("SELECT i.f1, i.f1 * 2::INT2 AS x FROM INT2_TBL i", error_message, this.getEmptyIOPair());

        // We get:
        // L: (Some(-32767), Some(-32765))x1
        // L: (Some(-1234), Some(-1232))x1
        // L: (Some(0), Some(2))x1
        // L: (Some(1234), Some(1236))x1
        // L: (Some(32767), Some(-32767))x1 --> wraps around
        // https://github.com/postgres/postgres/blob/c161ab74f76af8e0f3c6b349438525ad9575683b/src/test/regress/expected/int2.out#L223
        this.runtimeFail( "SELECT i.f1, i.f1 + '2'::INT2 AS x FROM INT2_TBL i", error_message, this.getEmptyIOPair());

        // Similarly,
        this.runtimeFail("SELECT i.f1, i.f1 - '2'::INT2 AS x FROM INT2_TBL i;", error_message, this.getEmptyIOPair());
    }

    // This passes for the Calcite version but fails for the run time version
    @Test @Ignore("Modulo edge case integer overflow: https://github.com/feldera/feldera/issues/1195")
    public void testINT2MINOverflow() {
        this.q(
                """
                        SELECT (-32768)::int2 % (-1)::int2 as x;
                         x
                        ---
                          0"""
        );
    }

    @Test @Ignore("Integer wrapping: https://github.com/feldera/feldera/issues/1186")
    public void testINT2MINOverflowError() {
        // This fails in Postgres, but we get: `-32768`
        this.runtimeFail("SELECT (-32768)::int2 * (-1)::int2", "attempt to multiply with overflow", this.getEmptyIOPair());

        this.runtimeFail("SELECT (-32768)::int2 / (-1)::int2", "attempt to divide with overflow", this.getEmptyIOPair());
    }
}
