package org.dbsp.sqlCompiler.compiler.sql.postgres;

import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.sql.tools.SqlIoTest;
import org.junit.Test;

/* `BIGINT` and `INT64` tests manually adopted from
 * https://github.com/postgres/postgres/blob/master/src/test/regress/expected/int8.out */
public class PostgresInt8Tests extends SqlIoTest {
    @Override
    public void prepareInputs(DBSPCompiler compiler) {
        String createTable = "CREATE TABLE INT8_TBL(q1 bigint, q2 bigint)";
        String insert = """
                INSERT INTO INT8_TBL VALUES
                  ('123','456'),
                  ('123','4567890123456789'),
                  ('4567890123456789','123'),
                  ('+4567890123456789','4567890123456789'),
                  ('+4567890123456789','-4567890123456789');""";

        compiler.submitStatementForCompilation(createTable);
        compiler.submitStatementsForCompilation(insert);
    }

    @Test
    public void precisionLossTest() {
        this.qs("""
                SELECT CAST('36854775807.0'::float4 AS int64);
                    int8
                -------------
                 36854775808
                (1 row)""");
    }

    @Test
    public void testSelect() {
        this.qs(
                """
                        SELECT * FROM INT8_TBL;
                                q1        |        q2
                        ------------------+-------------------
                                      123 |               456
                                      123 |  4567890123456789
                         4567890123456789 |               123
                         4567890123456789 |  4567890123456789
                         4567890123456789 | -4567890123456789
                        (5 rows)

                        SELECT * FROM INT8_TBL WHERE q2 = 4567890123456789;
                                q1        |        q2
                        ------------------+------------------
                                      123 | 4567890123456789
                         4567890123456789 | 4567890123456789
                        (2 rows)

                        SELECT * FROM INT8_TBL WHERE q2 <> 4567890123456789;
                                q1        |        q2
                        ------------------+-------------------
                                      123 |               456
                         4567890123456789 |               123
                         4567890123456789 | -4567890123456789
                        (3 rows)

                        SELECT * FROM INT8_TBL WHERE q2 < 4567890123456789;
                                q1        |        q2
                        ------------------+-------------------
                                      123 |               456
                         4567890123456789 |               123
                         4567890123456789 | -4567890123456789
                        (3 rows)

                        SELECT * FROM INT8_TBL WHERE q2 > 4567890123456789;
                         q1 | q2
                        ----+----
                        (0 rows)

                        SELECT * FROM INT8_TBL WHERE q2 <= 4567890123456789;
                                q1        |        q2
                        ------------------+-------------------
                                      123 |               456
                                      123 |  4567890123456789
                         4567890123456789 |               123
                         4567890123456789 |  4567890123456789
                         4567890123456789 | -4567890123456789
                        (5 rows)

                        SELECT * FROM INT8_TBL WHERE q2 >= 4567890123456789;
                                q1        |        q2
                        ------------------+------------------
                                      123 | 4567890123456789
                         4567890123456789 | 4567890123456789
                        (2 rows)

                        SELECT * FROM INT8_TBL WHERE q2 = 456;
                         q1  | q2
                        -----+-----
                         123 | 456
                        (1 row)

                        SELECT * FROM INT8_TBL WHERE q2 <> 456;
                                q1        |        q2
                        ------------------+-------------------
                                      123 |  4567890123456789
                         4567890123456789 |               123
                         4567890123456789 |  4567890123456789
                         4567890123456789 | -4567890123456789
                        (4 rows)

                        SELECT * FROM INT8_TBL WHERE q2 < 456;
                                q1        |        q2
                        ------------------+-------------------
                         4567890123456789 |               123
                         4567890123456789 | -4567890123456789
                        (2 rows)

                        SELECT * FROM INT8_TBL WHERE q2 > 456;
                                q1        |        q2
                        ------------------+------------------
                                      123 | 4567890123456789
                         4567890123456789 | 4567890123456789
                        (2 rows)

                        SELECT * FROM INT8_TBL WHERE q2 <= 456;
                                q1        |        q2
                        ------------------+-------------------
                                      123 |               456
                         4567890123456789 |               123
                         4567890123456789 | -4567890123456789
                        (3 rows)

                        SELECT * FROM INT8_TBL WHERE q2 >= 456;
                                q1        |        q2
                        ------------------+------------------
                                      123 |              456
                                      123 | 4567890123456789
                         4567890123456789 | 4567890123456789
                        (3 rows)

                        SELECT * FROM INT8_TBL WHERE 123 = q1;
                         q1  |        q2
                        -----+------------------
                         123 |              456
                         123 | 4567890123456789
                        (2 rows)

                        SELECT * FROM INT8_TBL WHERE 123 <> q1;
                                q1        |        q2
                        ------------------+-------------------
                         4567890123456789 |               123
                         4567890123456789 |  4567890123456789
                         4567890123456789 | -4567890123456789
                        (3 rows)

                        SELECT * FROM INT8_TBL WHERE 123 < q1;
                                q1        |        q2
                        ------------------+-------------------
                         4567890123456789 |               123
                         4567890123456789 |  4567890123456789
                         4567890123456789 | -4567890123456789
                        (3 rows)

                        SELECT * FROM INT8_TBL WHERE 123 > q1;
                         q1 | q2
                        ----+----
                        (0 rows)

                        SELECT * FROM INT8_TBL WHERE 123 <= q1;
                                q1        |        q2
                        ------------------+-------------------
                                      123 |               456
                                      123 |  4567890123456789
                         4567890123456789 |               123
                         4567890123456789 |  4567890123456789
                         4567890123456789 | -4567890123456789
                        (5 rows)

                        SELECT * FROM INT8_TBL WHERE 123 >= q1;
                         q1  |        q2
                        -----+------------------
                         123 |              456
                         123 | 4567890123456789
                        (2 rows)

                        SELECT * FROM INT8_TBL WHERE q2 = '456'::int2;
                         q1  | q2
                        -----+-----
                         123 | 456
                        (1 row)

                        SELECT * FROM INT8_TBL WHERE q2 <> '456'::int2;
                                q1        |        q2
                        ------------------+-------------------
                                      123 |  4567890123456789
                         4567890123456789 |               123
                         4567890123456789 |  4567890123456789
                         4567890123456789 | -4567890123456789
                        (4 rows)

                        SELECT * FROM INT8_TBL WHERE q2 < '456'::int2;
                                q1        |        q2
                        ------------------+-------------------
                         4567890123456789 |               123
                         4567890123456789 | -4567890123456789
                        (2 rows)

                        SELECT * FROM INT8_TBL WHERE q2 > '456'::int2;
                                q1        |        q2
                        ------------------+------------------
                                      123 | 4567890123456789
                         4567890123456789 | 4567890123456789
                        (2 rows)

                        SELECT * FROM INT8_TBL WHERE q2 <= '456'::int2;
                                q1        |        q2
                        ------------------+-------------------
                                      123 |               456
                         4567890123456789 |               123
                         4567890123456789 | -4567890123456789
                        (3 rows)

                        SELECT * FROM INT8_TBL WHERE q2 >= '456'::int2;
                                q1        |        q2
                        ------------------+------------------
                                      123 |              456
                                      123 | 4567890123456789
                         4567890123456789 | 4567890123456789
                        (3 rows)

                        SELECT * FROM INT8_TBL WHERE '123'::int2 = q1;
                         q1  |        q2
                        -----+------------------
                         123 |              456
                         123 | 4567890123456789
                        (2 rows)

                        SELECT * FROM INT8_TBL WHERE '123'::int2 <> q1;
                                q1        |        q2
                        ------------------+-------------------
                         4567890123456789 |               123
                         4567890123456789 |  4567890123456789
                         4567890123456789 | -4567890123456789
                        (3 rows)

                        SELECT * FROM INT8_TBL WHERE '123'::int2 < q1;
                                q1        |        q2
                        ------------------+-------------------
                         4567890123456789 |               123
                         4567890123456789 |  4567890123456789
                         4567890123456789 | -4567890123456789
                        (3 rows)

                        SELECT * FROM INT8_TBL WHERE '123'::int2 > q1;
                         q1 | q2
                        ----+----
                        (0 rows)

                        SELECT * FROM INT8_TBL WHERE '123'::int2 <= q1;
                                q1        |        q2
                        ------------------+-------------------
                                      123 |               456
                                      123 |  4567890123456789
                         4567890123456789 |               123
                         4567890123456789 |  4567890123456789
                         4567890123456789 | -4567890123456789
                        (5 rows)

                        SELECT * FROM INT8_TBL WHERE '123'::int2 >= q1;
                         q1  |        q2
                        -----+------------------
                         123 |              456
                         123 | 4567890123456789
                        (2 rows)

                        SELECT q1, q2, q1 * q2 AS multiply FROM INT8_TBL WHERE q1 < 1000 or (q2 > 0 and q2 < 1000);
                                q1        |        q2        |      multiply
                        ------------------+------------------+--------------------
                                      123 |              456 |              56088
                                      123 | 4567890123456789 | 561850485185185047
                         4567890123456789 |              123 | 561850485185185047
                        (3 rows)


                        SELECT q1, q2, q1 / q2 AS divide, q1 % q2 AS mod FROM INT8_TBL;
                                q1        |        q2         |     divide     | mod
                        ------------------+-------------------+----------------+-----
                                      123 |               456 |              0 | 123
                                      123 |  4567890123456789 |              0 | 123
                         4567890123456789 |               123 | 37137318076884 |  57
                         4567890123456789 |  4567890123456789 |              1 |   0
                         4567890123456789 | -4567890123456789 |             -1 |   0
                        (5 rows)

                        SELECT q1, q1::float8 FROM INT8_TBL;
                                q1        |        float8
                        ------------------+-----------------------
                                      123 |                   123
                                      123 |                   123
                         4567890123456789 | 4.567890123456789e+15
                         4567890123456789 | 4.567890123456789e+15
                         4567890123456789 | 4.567890123456789e+15
                        (5 rows)

                        SELECT q2, q2::float8 FROM INT8_TBL;
                                q2         |         float8
                        -------------------+------------------------
                                       456 |                    456
                          4567890123456789 |  4.567890123456789e+15
                                       123 |                    123
                          4567890123456789 |  4.567890123456789e+15
                         -4567890123456789 | -4.567890123456789e+15
                        (5 rows)


                        SELECT 37 + q1 AS plus4 FROM INT8_TBL;
                              plus4
                        ------------------
                                      160
                                      160
                         4567890123456826
                         4567890123456826
                         4567890123456826
                        (5 rows)

                        SELECT 37 - q1 AS minus4 FROM INT8_TBL;
                              minus4
                        -------------------
                                       -86
                                       -86
                         -4567890123456752
                         -4567890123456752
                         -4567890123456752
                        (5 rows)

                        SELECT 2 * q1 AS "twice int4" FROM INT8_TBL;
                            twice int4
                        ------------------
                                      246
                                      246
                         9135780246913578
                         9135780246913578
                         9135780246913578
                        (5 rows)


                        SELECT q1 * 2 AS "twice int4" FROM INT8_TBL;
                            twice int4
                        ------------------
                                      246
                                      246
                         9135780246913578
                         9135780246913578
                         9135780246913578
                        (5 rows)

                        -- int8 op int4
                        SELECT q1 + 42::int4 AS "8plus4", q1 - 42::int4 AS "8minus4", q1 * 42::int4 AS "8mul4", q1 / 42::int4 AS "8div4" FROM INT8_TBL;
                              8plus4      |     8minus4      |       8mul4        |      8div4
                        ------------------+------------------+--------------------+-----------------
                                      165 |               81 |               5166 |               2
                                      165 |               81 |               5166 |               2
                         4567890123456831 | 4567890123456747 | 191851385185185138 | 108759288653733
                         4567890123456831 | 4567890123456747 | 191851385185185138 | 108759288653733
                         4567890123456831 | 4567890123456747 | 191851385185185138 | 108759288653733
                        (5 rows)

                        -- int4 op int8
                        SELECT 246::int4 + q1 AS "4plus8", 246::int4 - q1 AS "4minus8", 246::int4 * q1 AS "4mul8", 246::int4 / q1 AS "4div8" FROM INT8_TBL;
                              4plus8      |      4minus8      |        4mul8        | 4div8
                        ------------------+-------------------+---------------------+-------
                                      369 |               123 |               30258 |     2
                                      369 |               123 |               30258 |     2
                         4567890123457035 | -4567890123456543 | 1123700970370370094 |     0
                         4567890123457035 | -4567890123456543 | 1123700970370370094 |     0
                         4567890123457035 | -4567890123456543 | 1123700970370370094 |     0
                        (5 rows)

                        -- int8 op int2
                        SELECT q1 + 42::int2 AS "8plus2", q1 - 42::int2 AS "8minus2", q1 * 42::int2 AS "8mul2", q1 / 42::int2 AS "8div2" FROM INT8_TBL;
                              8plus2      |     8minus2      |       8mul2        |      8div2
                        ------------------+------------------+--------------------+-----------------
                                      165 |               81 |               5166 |               2
                                      165 |               81 |               5166 |               2
                         4567890123456831 | 4567890123456747 | 191851385185185138 | 108759288653733
                         4567890123456831 | 4567890123456747 | 191851385185185138 | 108759288653733
                         4567890123456831 | 4567890123456747 | 191851385185185138 | 108759288653733
                        (5 rows)

                        -- int2 op int8
                        SELECT 246::int2 + q1 AS "2plus8", 246::int2 - q1 AS "2minus8", 246::int2 * q1 AS "2mul8", 246::int2 / q1 AS "2div8" FROM INT8_TBL;
                              2plus8      |      2minus8      |        2mul8        | 2div8
                        ------------------+-------------------+---------------------+-------
                                      369 |               123 |               30258 |     2
                                      369 |               123 |               30258 |     2
                         4567890123457035 | -4567890123456543 | 1123700970370370094 |     0
                         4567890123457035 | -4567890123456543 | 1123700970370370094 |     0
                         4567890123457035 | -4567890123456543 | 1123700970370370094 |     0
                        (5 rows)

                        SELECT q2, abs(q2) FROM INT8_TBL;
                                q2         |       abs
                        -------------------+------------------
                                       456 |              456
                          4567890123456789 | 4567890123456789
                                       123 |              123
                          4567890123456789 | 4567890123456789
                         -4567890123456789 | 4567890123456789
                        (5 rows)

                        SELECT min(q1), min(q2) FROM INT8_TBL;
                         min |        min
                        -----+-------------------
                         123 | -4567890123456789
                        (1 row)

                        SELECT max(q1), max(q2) FROM INT8_TBL;
                               max        |       max
                        ------------------+------------------
                         4567890123456789 | 4567890123456789
                        (1 row)

                        -- check min/max values and overflow behavior
                        select '-9223372036854775808'::int8;
                                 int8
                        ----------------------
                         -9223372036854775808
                        (1 row)

                        select '9223372036854775807'::int8;
                                int8
                        ---------------------
                         9223372036854775807
                        (1 row)

                        select -('-9223372036854775807'::int8);
                              ?column?
                        ---------------------
                         9223372036854775807
                        (1 row)

                        SELECT CAST(q1 AS int4) FROM int8_tbl WHERE q2 = 456;
                         q1
                        -----
                         123
                        (1 row)

                        SELECT CAST('42'::int2 AS int8), CAST('-37'::int2 AS int8);
                         int8 | int8
                        ------+------
                           42 |  -37
                        (1 row)

                        SELECT CAST(q1 AS float4), CAST(q2 AS float8) FROM INT8_TBL;
                             q1      |           q2
                        -------------+------------------------
                                 123 |                    456
                                 123 |  4.567890123456789e+15
                         4.56789e+15 |                    123
                         4.56789e+15 |  4.567890123456789e+15
                         4.56789e+15 | -4.567890123456789e+15
                        (5 rows)

                        -- check rounding when casting from float
                        SELECT x, x::int8 AS int8_value
                        FROM (VALUES (-2.5::float8),
                                     (-1.5::float8),
                                     (-0.5::float8),
                                     (0.0::float8),
                                     (0.5::float8),
                                     (1.5::float8),
                                     (2.5::float8)) t(x);
                          x   | int8_value
                        ------+------------
                         -2.5 |         -2
                         -1.5 |         -1
                         -0.5 |          0
                            0 |          0
                          0.5 |          0
                          1.5 |          1
                          2.5 |          2
                        (7 rows)

                        -- check rounding when casting from numeric
                        SELECT x, x::int8 AS int8_value
                        FROM (VALUES (-2.5::numeric(2, 1)),
                                     (-1.5::numeric(2, 1)),
                                     (-0.5::numeric(2, 1)),
                                     (0.0::numeric(2, 1)),
                                     (0.5::numeric(2, 1)),
                                     (1.5::numeric(2, 1)),
                                     (2.5::numeric(2, 1))) t(x);
                          x   | int8_value
                        ------+------------
                         -2.5 |         -2
                         -1.5 |         -1
                         -0.5 |         -0
                          0.0 |          0
                          0.5 |          0
                          1.5 |          1
                          2.5 |          2
                        (7 rows)

                        SELECT q1 AS "plus", -q1 AS "minus" FROM INT8_TBL;
                               plus       |       minus
                        ------------------+-------------------
                                      123 |              -123
                                      123 |              -123
                         4567890123456789 | -4567890123456789
                         4567890123456789 | -4567890123456789
                         4567890123456789 | -4567890123456789
                        (5 rows)

                        SELECT q1, q2, q1 + q2 AS "plus" FROM INT8_TBL;
                                q1        |        q2         |       plus
                        ------------------+-------------------+------------------
                                      123 |               456 |              579
                                      123 |  4567890123456789 | 4567890123456912
                         4567890123456789 |               123 | 4567890123456912
                         4567890123456789 |  4567890123456789 | 9135780246913578
                         4567890123456789 | -4567890123456789 |                0
                        (5 rows)

                        SELECT q1, q2, q1 - q2 AS "minus" FROM INT8_TBL;
                                q1        |        q2         |       minus
                        ------------------+-------------------+-------------------
                                      123 |               456 |              -333
                                      123 |  4567890123456789 | -4567890123456666
                         4567890123456789 |               123 |  4567890123456666
                         4567890123456789 |  4567890123456789 |                 0
                         4567890123456789 | -4567890123456789 |  9135780246913578
                        (5 rows)
                        """
        );
    }

    @Test
    public void testSelectOverflow() {
        this.qf("SELECT q1, q2, q1 * q2 AS multiply FROM INT8_TBL", "overflow");
    }

    @Test
    public void testOutOfRangeCast() {
        this.runtimeConstantFail("select '-9223372036854775809'::int64", "number too small to fit");
        this.runtimeConstantFail("select '9223372036854775808'::int64", "number too large to fit");
        this.runtimeConstantFail("SELECT CAST('4567890123456789' AS int4)", "number too large to fit");
        this.runtimeConstantFail("SELECT CAST('4567890123456789' AS int2)", "number too large to fit");
        this.runtimeConstantFail("SELECT CAST('+4567890123456789' AS int4)", "number too large to fit");
        this.runtimeConstantFail("SELECT CAST('+4567890123456789' AS int2)", "number too large to fit");
        this.runtimeConstantFail("SELECT CAST('-4567890123456789' AS int4)", "number too small to fit");
        this.runtimeConstantFail("SELECT CAST('-4567890123456789' AS int2)", "number too small to fit");
    }

    @Test
    public void issue1199() {
        this.runtimeConstantFail("SELECT CAST('922337203685477580700.0'::float8 AS int64)",
                "Cannot convert 922337203685477600000 to BIGINT");
    }

    @Test
    public void testINT64MINOverflow() {
        this.q("""
               SELECT (-9223372036854775808)::int64 % (-1)::int as x;
                x
               ----
                0"""
        );
    }

    @Test
    public void testINT64MINOverflowError() {
        this.qf("SELECT (-9223372036854775808)::int64 * (-1)::int64", "causes overflow for type BIGINT");
        this.qf("SELECT (-9223372036854775808)::int64 / (-1)::int64", "causes overflow for type BIGINT");
        this.qf("SELECT (-9223372036854775808)::int64 * (-1)::int4",  "causes overflow");
        this.qf("SELECT (-9223372036854775808)::int64 / (-1)::int4",  "causes overflow");
        this.qf("SELECT (-9223372036854775808)::int64 * (-1)::int2",  "causes overflow");
        this.qf("SELECT (-9223372036854775808)::int64 / (-1)::int2",  "causes overflow");
    }
}
