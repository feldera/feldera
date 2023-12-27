package org.dbsp.sqlCompiler.compiler.sql.postgres;

import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.sql.SqlIoTest;
import org.junit.Ignore;
import org.junit.Test;

/**
 * Tests manually adopted from
 * https://github.com/postgres/postgres/blob/master/src/test/regress/expected/int4.out
 */
@SuppressWarnings("JavadocLinkAsPlainText")
public class PostgresInt4Tests extends SqlIoTest {
    @Override
    public void prepareData(DBSPCompiler compiler) {
        String createTable = "CREATE TABLE INT4_TBL(f1 int4)";

        String insert =
                """
                        INSERT INTO INT4_TBL(f1) VALUES
                          ('0'),
                          ('123456'),
                          ('-123456'),
                          ('2147483647'),
                          ('-2147483647');""";

        compiler.compileStatement(createTable);
        compiler.compileStatements(insert);
    }

    @Test
    public void testSelect() {
        this.qs(
                """
                        SELECT i.* FROM INT4_TBL i WHERE i.f1 <> '0'::INT2;
                             f1
                        -------------
                              123456
                             -123456
                          2147483647
                         -2147483647
                        (4 rows)

                        SELECT i.* FROM INT4_TBL i WHERE i.f1 <> '0'::INT4;
                             f1
                        -------------
                              123456
                             -123456
                          2147483647
                         -2147483647
                        (4 rows)

                        SELECT i.* FROM INT4_TBL i WHERE i.f1 = '0'::INT2;
                         f1
                        ----
                          0
                        (1 row)

                        SELECT i.* FROM INT4_TBL i WHERE i.f1 = '0'::INT4;
                         f1
                        ----
                          0
                        (1 row)

                        SELECT i.* FROM INT4_TBL i WHERE i.f1 < '0'::INT2;
                             f1
                        -------------
                             -123456
                         -2147483647
                        (2 rows)

                        SELECT i.* FROM INT4_TBL i WHERE i.f1 < '0'::INT4;
                             f1
                        -------------
                             -123456
                         -2147483647
                        (2 rows)

                        SELECT i.* FROM INT4_TBL i WHERE i.f1 <= '0'::INT2;
                             f1
                        -------------
                                   0
                             -123456
                         -2147483647
                        (3 rows)

                        SELECT i.* FROM INT4_TBL i WHERE i.f1 <= '0'::INT4;
                             f1
                        -------------
                                   0
                             -123456
                         -2147483647
                        (3 rows)

                        SELECT i.* FROM INT4_TBL i WHERE i.f1 > '0'::INT2;
                             f1
                        ------------
                             123456
                         2147483647
                        (2 rows)

                        SELECT i.* FROM INT4_TBL i WHERE i.f1 > '0'::INT4;
                             f1
                        ------------
                             123456
                         2147483647
                        (2 rows)

                        SELECT i.* FROM INT4_TBL i WHERE i.f1 >= '0'::INT2;
                             f1
                        ------------
                                  0
                             123456
                         2147483647
                        (3 rows)

                        SELECT i.* FROM INT4_TBL i WHERE i.f1 >= '0'::INT4;
                             f1
                        ------------
                                  0
                             123456
                         2147483647
                        (3 rows)

                        -- positive odds
                        SELECT i.* FROM INT4_TBL i WHERE (i.f1 % '2'::INT2) = '1'::INT2;
                             f1
                        ------------
                         2147483647
                        (1 row)

                        -- any evens
                        SELECT i.* FROM INT4_TBL i WHERE (i.f1 % '2'::INT4) = '0'::INT2;
                           f1
                        ---------
                               0
                          123456
                         -123456
                        (3 rows)

                        SELECT i.f1, i.f1 * '2'::INT2 AS x FROM INT4_TBL i
                        WHERE abs(f1) < 1073741824;
                           f1    |    x
                        ---------+---------
                               0 |       0
                          123456 |  246912
                         -123456 | -246912
                        (3 rows)

                        SELECT i.f1, i.f1 * '2'::INT4 AS x FROM INT4_TBL i
                        WHERE abs(f1) < 1073741824;
                           f1    |    x
                        ---------+---------
                               0 |       0
                          123456 |  246912
                         -123456 | -246912
                        (3 rows)

                        SELECT i.f1, i.f1 + '2'::INT2 AS x FROM INT4_TBL i
                        WHERE f1 < 2147483646;
                             f1      |      x
                        -------------+-------------
                                   0 |           2
                              123456 |      123458
                             -123456 |     -123454
                         -2147483647 | -2147483645
                        (4 rows)

                        SELECT i.f1, i.f1 + '2'::INT4 AS x FROM INT4_TBL i
                        WHERE f1 < 2147483646;
                             f1      |      x
                        -------------+-------------
                                   0 |           2
                              123456 |      123458
                             -123456 |     -123454
                         -2147483647 | -2147483645
                        (4 rows)

                        SELECT i.f1, i.f1 - '2'::INT2 AS x FROM INT4_TBL i
                        WHERE f1 > -2147483647;
                             f1     |     x
                        ------------+------------
                                  0 |         -2
                             123456 |     123454
                            -123456 |    -123458
                         2147483647 | 2147483645
                        (4 rows)

                        SELECT i.f1, i.f1 - '2'::INT4 AS x FROM INT4_TBL i
                        WHERE f1 > -2147483647;
                             f1     |     x
                        ------------+------------
                                  0 |         -2
                             123456 |     123454
                            -123456 |    -123458
                         2147483647 | 2147483645
                        (4 rows)

                        SELECT i.f1, i.f1 / '2'::INT2 AS x FROM INT4_TBL i;
                             f1      |      x
                        -------------+-------------
                                   0 |           0
                              123456 |       61728
                             -123456 |      -61728
                          2147483647 |  1073741823
                         -2147483647 | -1073741823
                        (5 rows)

                        SELECT i.f1, i.f1 / '2'::INT4 AS x FROM INT4_TBL i;
                             f1      |      x
                        -------------+-------------
                                   0 |           0
                              123456 |       61728
                             -123456 |      -61728
                          2147483647 |  1073741823
                         -2147483647 | -1073741823
                        (5 rows)

                        SELECT -2+3 AS one;
                         one
                        -----
                           1
                        (1 row)

                        SELECT 4-2 AS two;
                         two
                        -----
                           2
                        (1 row)

                        SELECT 2- -1 AS three;
                         three
                        -------
                             3
                        (1 row)

                        SELECT 2 - -2 AS four;
                         four
                        ------
                            4
                        (1 row)

                        SELECT '2'::INT2 * '2'::INT2 = '16'::INT2 / '4'::INT2 AS true;
                         true
                        ------
                         t
                        (1 row)

                        SELECT '2'::INT4 * '2'::INT2 = '16'::INT2 / '4'::INT4 AS true;
                         true
                        ------
                         t
                        (1 row)

                        SELECT '2'::INT2 * '2'::INT4 = '16'::INT4 / '4'::INT2 AS true;
                         true
                        ------
                         t
                        (1 row)

                        SELECT '1000'::INT4 < '999'::INT4 AS false;
                         false
                        -------
                         f
                        (1 row)

                        SELECT 1 + 1 + 1 + 1 + 1 + 1 + 1 + 1 + 1 + 1 AS ten;
                         ten
                        -----
                          10
                        (1 row)

                        SELECT 2 + 2 / 2 AS three;
                         three
                        -------
                             3
                        (1 row)

                        SELECT (2 + 2) / 2 AS two;
                         two
                        -----
                           2
                        (1 row)"""
        );
    }

    // Check PostgresInt2Tests::testSelectOverflow for details
    @Test @Ignore("Integer wrapping: https://github.com/feldera/feldera/issues/1186")
    public void testSelectOverflow() {
        String error = "overflow";

        // We get:
        // L: (Some(-2147483647), Some(2))x1 --> wraps around
        // L: (Some(-123456), Some(-246912))x1
        // L: (Some(0), Some(0))x1
        // L: (Some(123456), Some(246912))x1
        // L: (Some(2147483647), Some(-2))x1 --> wraps around
        this.runtimeFail("SELECT i.f1, i.f1 * '2'::INT2 AS x FROM INT4_TBL i", error, this.getEmptyIOPair());

        // We get:
        // L: (Some(-2147483647), Some(2))x1 --> wraps around
        // L: (Some(-123456), Some(-246912))x1
        // L: (Some(0), Some(0))x1
        // L: (Some(123456), Some(246912))x1
        // L: (Some(2147483647), Some(-2))x1 --> wraps around
        this.runtimeFail("SELECT i.f1, i.f1 * '2'::INT4 AS x FROM INT4_TBL i", error, this.getEmptyIOPair());

        // We get:
        // L: (Some(-2147483647), Some(-2147483645))x1
        // L: (Some(-123456), Some(-123454))x1
        // L: (Some(0), Some(2))x1
        // L: (Some(123456), Some(123458))x1
        // L: (Some(2147483647), Some(-2147483647))x1 --> wraps around
        this.runtimeFail("SELECT i.f1, i.f1 + '2'::INT2 AS x FROM INT4_TBL i", error, this.getEmptyIOPair());

        // We get:
        // L: (Some(-2147483647), Some(-2147483645))x1
        // L: (Some(-123456), Some(-123454))x1
        // L: (Some(0), Some(2))x1
        // L: (Some(123456), Some(123458))x1
        // L: (Some(2147483647), Some(-2147483647))x1 --> wraps around
        this.runtimeFail("SELECT i.f1, i.f1 + '2'::INT4 AS x FROM INT4_TBL i", error, this.getEmptyIOPair());

        // We get:
        // L: (Some(-2147483647), Some(2147483647))x1 --> wraps around
        // L: (Some(-123456), Some(-123458))x1
        // L: (Some(0), Some(-2))x1
        // L: (Some(123456), Some(123454))x1
        // L: (Some(2147483647), Some(2147483645))x1
        this.runtimeFail("SELECT i.f1, i.f1 - '2'::INT2 AS x FROM INT4_TBL i", error, this.getEmptyIOPair());

        // We get:
        // L: (Some(-2147483647), Some(2147483647))x1 --> wraps around
        // L: (Some(-123456), Some(-123458))x1
        // L: (Some(0), Some(-2))x1
        // L: (Some(123456), Some(123454))x1
        // L: (Some(2147483647), Some(2147483645))x1
        this.runtimeFail("SELECT i.f1, i.f1 - '2'::INT4 AS x FROM INT4_TBL i", error, this.getEmptyIOPair());
    }

    // This passes for the Calcite version but fails for the run time version
    // Check PostgresInt2Tests::testINT2MINOverflow for details
    @Test @Ignore("Modulo edge case integer overflow: https://github.com/feldera/feldera/issues/1195")
    public void testINT4MINOverflow() {
        this.q(
                """
                        SELECT (-2147483648)::int4 % (-1)::int4 as x;
                         x
                        ---
                          0"""
        );
    }

    @Test @Ignore("Integer wrapping: https://github.com/feldera/feldera/issues/1186")
    public void testINT4MINOverflowError() {
        this.runtimeFail("SELECT (-2147483648)::int4 * (-1)::int2", "attempt to multiply with overflow", this.getEmptyIOPair());
        this.runtimeFail("SELECT (-2147483648)::int4 / (-1)::int2", "attempt to divide with overflow", this.getEmptyIOPair());
    }

    @Test
    public void testFloatRound() {
        this.q(
                """
                        SELECT x, x::int4 AS int4_value
                        FROM (VALUES (-2.9::float8),
                                     (-2.5::float8),
                                     (-1.5::float8),
                                     (-0.5::float8),
                                     (0.0::float8),
                                     (0.5::float8),
                                     (1.5::float8),
                                     (2.5::float8)) t(x);
                          x   | int4_value
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
    @Test
    public void testNumericRound() {
        this.q(
                """
                        SELECT x, x::int4 AS int4_value
                        FROM (VALUES (-2.9::numeric),
                                     (-2.5::numeric),
                                     (-1.5::numeric),
                                     (-0.5::numeric),
                                     (0.0::numeric),
                                     (0.5::numeric),
                                     (1.5::numeric),
                                     (2.5::numeric)) t(x);
                          x   | int4_value
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
}
