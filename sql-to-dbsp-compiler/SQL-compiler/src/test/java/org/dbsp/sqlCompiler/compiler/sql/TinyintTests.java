package org.dbsp.sqlCompiler.compiler.sql;

import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.junit.Ignore;
import org.junit.Test;

public class TinyintTests extends SqlIoTest {
    @Override
    public void prepareData(DBSPCompiler compiler) {
        String createTable = "CREATE TABLE INT_TBL(f1 tinyint)";

        String insert = """
                INSERT INTO INT_TBL(f1) VALUES
                  (0),
                  (12),
                  (-12),
                  (127),
                  (-127);""";

        compiler.compileStatement(createTable);
        compiler.compileStatements(insert);
    }

    @Test
    public void testSelect() {
        this.qs(
                """
                        SELECT * FROM INT_TBL;
                         f1
                        ----
                         0
                         12
                         -12
                         127
                         -127
                        (5 rows)
                        
                        SELECT i.* FROM INT_TBL i WHERE i.f1 <> 0::TINYINT;
                         f1
                        ----
                         12
                         -12
                         127
                         -127
                        (4 rows)

                        SELECT i.* FROM INT_TBL i WHERE i.f1 <> 0::INT2;
                         f1
                        ----
                         12
                         -12
                         127
                         -127
                        (4 rows)

                        SELECT i.* FROM INT_TBL i WHERE i.f1 <> 0::INT4;
                         f1
                        ----
                         12
                         -12
                         127
                         -127
                        (4 rows)

                        SELECT i.* FROM INT_TBL i WHERE i.f1 = 0::TINYINT;
                         f1
                        ----
                         0
                        (1 row)

                        SELECT i.* FROM INT_TBL i WHERE i.f1 = 0::INT2;
                         f1
                        ----
                         0
                        (1 row)

                        SELECT i.* FROM INT_TBL i WHERE i.f1 = 0::INT4;
                         f1
                        ----
                         0
                        (1 row)

                        SELECT i.* FROM INT_TBL i WHERE i.f1 < 0::TINYINT;
                         f1
                        ----
                         -12
                         -127
                        (2 rows)

                        SELECT i.* FROM INT_TBL i WHERE i.f1 < 0::INT2;
                         f1
                        ----
                         -12
                         -127
                        (2 rows)

                        SELECT i.* FROM INT_TBL i WHERE i.f1 < 0::INT4;
                         f1
                        ----
                         -12
                         -127
                        (2 rows)
                        
                        SELECT i.* FROM INT_TBL i WHERE i.f1 <= '0'::TINYINT;
                         f1
                        ----
                         0
                         -12
                         -127
                        (3 rows)

                        SELECT i.* FROM INT_TBL i WHERE i.f1 <= '0'::INT2;
                         f1
                        ----
                         0
                         -12
                         -127
                        (3 rows)

                        SELECT i.* FROM INT_TBL i WHERE i.f1 <= '0'::INT4;
                         f1
                        ----
                         0
                         -12
                         -127
                        (3 rows)

                        SELECT i.* FROM INT_TBL i WHERE i.f1 > '0'::TINYINT;
                         f1
                        ----
                         12
                         127
                        (2 rows)

                        SELECT i.* FROM INT_TBL i WHERE i.f1 > '0'::INT2;
                         f1
                        ----
                         12
                         127
                        (2 rows)

                        SELECT i.* FROM INT_TBL i WHERE i.f1 > '0'::INT4;
                         f1
                        ----
                         12
                         127
                        (2 rows)

                        SELECT i.* FROM INT_TBL i WHERE i.f1 >= '0'::TINYINT;
                         f1
                        ----
                         0
                         12
                         127
                        (3 rows)

                        SELECT i.* FROM INT_TBL i WHERE i.f1 >= '0'::INT2;
                         f1
                        ----
                         0
                         12
                         127
                        (3 rows)

                        SELECT i.* FROM INT_TBL i WHERE i.f1 >= '0'::INT4;
                         f1
                        ----
                         0
                         12
                         127
                        (3 rows)

                        SELECT i.* FROM INT_TBL i WHERE (i.f1 % '2'::TINYINT) = '1'::TINYINT;
                         f1
                        ----
                         127
                        (1 row)

                        SELECT i.* FROM INT_TBL i WHERE (i.f1 % '2'::INT2) = '1'::INT2;
                         f1
                        ----
                         127
                        (1 row)

                        SELECT i.* FROM INT_TBL i WHERE (i.f1 % '2'::INT4) = '0'::INT2;
                         f1
                        ----
                         0
                         12
                         -12
                        (3 rows)
                        
                        SELECT i.f1, i.f1 * '2'::TINYINT AS x FROM INT_TBL i WHERE abs(f1) < 64;
                          f1 |   x
                        -----+-------
                           0 |  0
                          12 |  24
                         -12 | -24
                        (3 rows)

                        SELECT i.f1, i.f1 * '2'::INT2 AS x FROM INT_TBL i WHERE abs(f1) < 64;
                          f1 |   x
                        -----+-------
                           0 |  0
                          12 |  24
                         -12 | -24
                        (3 rows)

                        SELECT i.f1, i.f1 * '2'::INT4 AS x FROM INT_TBL i;
                           f1 |   x
                        ------+--------
                            0 |    0
                           12 |   24
                          -12 |  -24
                          127 |  254
                         -127 | -254
                        (5 rows)

                        SELECT i.f1, i.f1 + '2'::TINYINT AS x FROM INT_TBL i WHERE f1 < 125;
                           f1 |   x
                        ------+--------
                            0 |    2
                           12 |   14
                          -12 |  -10
                         -127 | -125
                        (4 rows)

                        SELECT i.f1, i.f1 + '2'::INT2 AS x FROM INT_TBL i;
                           f1 |   x
                        ------+--------
                            0 |    2
                           12 |   14
                          -12 |  -10
                          127 |  129
                         -127 | -125
                        (5 rows)

                        SELECT i.f1, i.f1 - '2'::TINYINT AS x FROM INT_TBL i WHERE f1 > -127;
                          f1 |   x
                        -----+-------
                           0 |  -2
                          12 |  10
                         -12 | -14
                         127 | 125
                        (4 rows)

                        SELECT i.f1, i.f1 - '2'::INT2 AS x FROM INT_TBL i;
                           f1 |   x
                        ------+--------
                            0 |   -2
                           12 |   10
                          -12 |  -14
                          127 |  125
                         -127 | -129
                        (5 rows)

                        SELECT i.f1, i.f1 / '2'::INT4 AS x FROM INT_TBL i;
                           f1 |   x
                        ------+-------
                            0 |   0
                           12 |   6
                          -12 |  -6
                          127 |  63
                         -127 | -63
                        (5 rows)

                        SELECT i.f1, i.f1 / '2'::INT4 AS x FROM INT_TBL i;
                           f1 |   x
                        ------+--------
                            0 |    0
                           12 |    6
                          -12 |   -6
                          127 |   63
                         -127 |  -63
                        (5 rows)
                        
                        """
        );
    }

    // We round down to zero
    @Test
    public void testFloatRound() {
        this.q(
                """
                        SELECT x, x::tinyint AS tinyint_value FROM (VALUES (-2.9::float8), (-2.5::float8), (-1.5::float8), (-0.5::float8), (0.0::float8), (0.5::float8), (1.5::float8), (2.5::float8)) t(x);
                          x   | tinyint_value
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
                        SELECT x, x::tinyint AS tinyint_value FROM (VALUES (-2.9::numeric), (-2.5::numeric), (-1.5::numeric), (-0.5::numeric), (0.0::numeric), (0.5::numeric), (1.5::numeric), (2.5::numeric)) t(x);
                          x   | tinyint_value
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
        String error_message = "TINYINT out of range";
        this.qf("SELECT i.f1, i.f1 * 2::TINYINT AS x FROM INT_TBL i", error_message);
        this.qf( "SELECT i.f1, i.f1 + '2'::TINYINT AS x FROM INT_TBL i", error_message);
        this.qf("SELECT i.f1, i.f1 - '2'::TINYINT AS x FROM INT_TBL i", error_message);
    }

    @Test
    public void testTINYINTMINOverflow() {
        this.q(
                """
                        SELECT (-128)::tinyint % (-1)::tinyint as x;
                         x
                        ---
                          0"""
        );
    }

    @Test @Ignore("Integer wrapping: https://github.com/feldera/feldera/issues/1186")
    public void testTINYINTMINOverflowError() {
        this.qf("SELECT (-128)::tinyint * (-1)::tinyint", "attempt to multiply with overflow");
        this.qf("SELECT (-128)::tinyint / (-1)::tinyint", "attempt to divide with overflow");
    }
}
