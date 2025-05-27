package org.dbsp.sqlCompiler.compiler.sql.postgres;

import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.sql.tools.SqlIoTest;
import org.junit.Test;

/*
 * Tests manually adapted from
 * https://github.com/postgres/postgres/blob/master/src/test/regress/expected/float8.out
 *
 * Tests have been almost completely ported except for unsupported features:
 * - create type, create function, create cast
 * - degree based trigonometric functions
 * - erf, erfc methods
 * - subnormal tests and xfloat8 related tests
 *
 * Rest of the tests are in PostgresFloat8Part2Tests  */
public class PostgresFloat8Tests extends SqlIoTest {
    @Override
    public void prepareInputs(DBSPCompiler compiler) {
        String prepareQuery = """
                CREATE TABLE FLOAT8_TBL(f1 float8);
                INSERT INTO FLOAT8_TBL(f1) VALUES ('    0.0   ');
                INSERT INTO FLOAT8_TBL(f1) VALUES ('1004.30  ');
                INSERT INTO FLOAT8_TBL(f1) VALUES ('   -34.84');
                INSERT INTO FLOAT8_TBL(f1) VALUES ('1.2345678901234e+200');
                INSERT INTO FLOAT8_TBL(f1) VALUES ('1.2345678901234e-200');
                """;
        compiler.submitStatementsForCompilation(prepareQuery);
    }

    @Test
    public void testSelect() {
        this.qs("""
                -- special inputs
                SELECT 'NaN'::float8;
                 float8
                --------
                    NaN
                (1 row)

                SELECT 'nan'::float8;
                 float8
                --------
                    NaN
                (1 row)

                SELECT '   NAN  '::float8;
                 float8
                --------
                    NaN
                (1 row)

                SELECT 'infinity'::float8;
                  float8
                ----------
                 Infinity
                (1 row)

                SELECT '          -INFINiTY   '::float8;
                  float8
                -----------
                 -Infinity
                (1 row)

                SELECT 'Infinity'::float8 + 100.0;
                 ?column?
                ----------
                 Infinity
                (1 row)

                SELECT 'Infinity'::float8 / 'Infinity'::float8;
                 ?column?
                ----------
                      NaN
                (1 row)

                SELECT '42'::float8 / 'Infinity'::float8;
                 ?column?
                ----------
                        0
                (1 row)

                SELECT 'nan'::float8 / 'nan'::float8;
                 ?column?
                ----------
                      NaN
                (1 row)

                SELECT 'nan'::float8 / '0'::float8;
                 ?column?
                ----------
                      NaN
                (1 row)

                SELECT 'nan'::float8; -- the original postgres version was: 'nan'::numeric::float8
                 float8
                --------
                    NaN
                (1 row)

                SELECT * FROM FLOAT8_TBL;
                          f1
                ----------------------
                                    0
                               1004.3
                               -34.84
                 1.2345678901234e+200
                 1.2345678901234e-200
                (5 rows)

                SELECT f.* FROM FLOAT8_TBL f WHERE f.f1 <> '1004.3';
                          f1
                ----------------------
                                    0
                               -34.84
                 1.2345678901234e+200
                 1.2345678901234e-200
                (4 rows)

                SELECT f.* FROM FLOAT8_TBL f WHERE f.f1 = '1004.3';
                   f1
                --------
                 1004.3
                (1 row)

                SELECT f.* FROM FLOAT8_TBL f WHERE '1004.3' > f.f1;
                          f1
                ----------------------
                                    0
                               -34.84
                 1.2345678901234e-200
                (3 rows)

                SELECT f.* FROM FLOAT8_TBL f WHERE  f.f1 < '1004.3';
                          f1
                ----------------------
                                    0
                               -34.84
                 1.2345678901234e-200
                (3 rows)

                SELECT f.* FROM FLOAT8_TBL f WHERE '1004.3' >= f.f1;
                          f1
                ----------------------
                                    0
                               1004.3
                               -34.84
                 1.2345678901234e-200
                (4 rows)

                SELECT f.* FROM FLOAT8_TBL f WHERE  f.f1 <= '1004.3';
                          f1
                ----------------------
                                    0
                               1004.3
                               -34.84
                 1.2345678901234e-200
                (4 rows)

                SELECT f.f1, f.f1 * '-10' AS x
                   FROM FLOAT8_TBL f
                   WHERE f.f1 > '0.0';
                          f1          |           x
                ----------------------+-----------------------
                               1004.3 |                -10043
                 1.2345678901234e+200 | -1.2345678901234e+201
                 1.2345678901234e-200 | -1.2345678901234e-199
                (3 rows)

                SELECT f.f1, f.f1 + '-10' AS x
                   FROM FLOAT8_TBL f
                   WHERE f.f1 > '0.0';
                          f1          |          x
                ----------------------+----------------------
                               1004.3 |                994.3
                 1.2345678901234e+200 | 1.2345678901234e+200
                 1.2345678901234e-200 |                  -10
                (3 rows)

                SELECT f.f1, f.f1 / '-10' AS x
                   FROM FLOAT8_TBL f
                   WHERE f.f1 > '0.0';
                          f1          |           x
                ----------------------+-----------------------
                               1004.3 |   -100.42999999999999
                 1.2345678901234e+200 | -1.2345678901234e+199
                 1.2345678901234e-200 | -1.2345678901234e-201
                (3 rows)

                SELECT f.f1, f.f1 - '-10' AS x
                   FROM FLOAT8_TBL f
                   WHERE f.f1 > '0.0';
                          f1          |          x
                ----------------------+----------------------
                               1004.3 |               1014.3
                 1.2345678901234e+200 | 1.2345678901234e+200
                 1.2345678901234e-200 |                   10
                (3 rows)

                -- absolute value
                SELECT f.f1, abs(f.f1) AS abs_f1
                   FROM FLOAT8_TBL f;
                          f1          |        abs_f1
                ----------------------+----------------------
                                    0 |                    0
                               1004.3 |               1004.3
                               -34.84 |                34.84
                 1.2345678901234e+200 | 1.2345678901234e+200
                 1.2345678901234e-200 | 1.2345678901234e-200
                (5 rows)

                -- truncate
                SELECT f.f1, truncate(f.f1) AS trunc_f1
                   FROM FLOAT8_TBL f;
                          f1          |       trunc_f1
                ----------------------+----------------------
                                    0 |                    0
                               1004.3 |                 1004
                               -34.84 |                  -34
                 1.2345678901234e+200 | 1.2345678901234e+200
                 1.2345678901234e-200 |                    0
                (5 rows)

                -- round
                SELECT f.f1, round(f.f1) AS round_f1
                   FROM FLOAT8_TBL f;
                          f1          |       round_f1
                ----------------------+----------------------
                                    0 |                    0
                               1004.3 |                 1004
                               -34.84 |                  -35
                 1.2345678901234e+200 | 1.2345678901234e+200
                 1.2345678901234e-200 |                    0
                (5 rows)

                -- ceil / ceiling
                select ceil(f1) as ceil_f1 from float8_tbl f;
                       ceil_f1
                ----------------------
                                    0
                                 1005
                                  -34
                 1.2345678901234e+200
                                    1
                (5 rows)

                select ceiling(f1) as ceiling_f1 from float8_tbl f;
                      ceiling_f1
                ----------------------
                                    0
                                 1005
                                  -34
                 1.2345678901234e+200
                                    1
                (5 rows)

                -- floor
                select floor(f1) as floor_f1 from float8_tbl f;
                       floor_f1
                ----------------------
                                    0
                                 1004
                                  -35
                 1.2345678901234e+200
                                    0
                (5 rows)

                -- sign
                select sign(f1) as sign_f1 from float8_tbl f;
                 sign_f1
                ---------
                       0
                       1
                      -1
                       1
                       1
                (5 rows)

                -- power
                SELECT power('144'::FLOAT8, '0.5'::FLOAT8);
                 power
                -------
                    12
                (1 row)

                SELECT power('NaN'::DOUBLE, '0.5'::DOUBLE);
                 power
                -------
                   NaN
                (1 row)

                SELECT power('144'::DOUBLE, 'NaN'::DOUBLE);
                 power
                -------
                   NaN
                (1 row)

                SELECT power('NaN'::DOUBLE, 'NaN'::DOUBLE);
                 power
                -------
                   NaN
                (1 row)

                SELECT power('-1'::DOUBLE, 'NaN'::DOUBLE);
                 power
                -------
                   NaN
                (1 row)

                SELECT power('1'::DOUBLE, 'NaN'::DOUBLE);
                 power
                -------
                   NaN
                (1 row)

                SELECT power('NaN'::DOUBLE, '0'::DOUBLE);
                 power
                -------
                     1
                (1 row)

                SELECT power('inf'::DOUBLE, '0'::DOUBLE);
                 power
                -------
                     1
                (1 row)

                SELECT power('-inf'::DOUBLE, '0'::DOUBLE);
                 power
                -------
                     1
                (1 row)

                SELECT power('0'::DOUBLE, 'inf'::DOUBLE);
                 power
                -------
                     0
                (1 row)

                SELECT power('1'::double, 'inf'::double);
                 power
                -------
                     1
                (1 row)

                SELECT power('1'::double, '-inf'::double);
                 power
                -------
                     1
                (1 row)

                SELECT power('-1'::double, 'inf'::double);
                 power
                -------
                     1
                (1 row)

                SELECT power('-1'::double, '-inf'::double);
                 power
                -------
                     1
                (1 row)

                SELECT power('0.1'::double, 'inf'::double);
                 power
                -------
                     0
                (1 row)

                SELECT power('-0.1'::double, 'inf'::double);
                 power
                -------
                     0
                (1 row)

                SELECT power('1.1'::double, 'inf'::double);
                  power
                ----------
                 Infinity
                (1 row)

                SELECT power('-1.1'::double, 'inf'::double);
                  power
                ----------
                 Infinity
                (1 row)

                SELECT power('0.1'::double, '-inf'::double);
                  power
                ----------
                 Infinity
                (1 row)

                SELECT power('-0.1'::double, '-inf'::double);
                  power
                ----------
                 Infinity
                (1 row)

                SELECT power('1.1'::double, '-inf'::double);
                 power
                -------
                     0
                (1 row)

                SELECT power('-1.1'::double, '-inf'::double);
                 power
                -------
                     0
                (1 row)

                SELECT power('inf'::double, '-2'::double);
                 power
                -------
                     0
                (1 row)

                SELECT power('inf'::double, '2'::double);
                  power
                ----------
                 Infinity
                (1 row)

                SELECT power('inf'::double, 'inf'::double);
                  power
                ----------
                 Infinity
                (1 row)

                SELECT power('inf'::double, '-inf'::double);
                 power
                -------
                     0
                (1 row)

                SELECT power('-inf'::double, '-2'::double) = '0';
                 ?column?
                ----------
                 t
                (1 row)


                SELECT power('-inf'::double, '-3'::double);
                 power
                -------
                    -0
                (1 row)

                SELECT power('-inf'::double, '2'::double);
                  power
                ----------
                 Infinity
                (1 row)

                SELECT power('-inf'::double, '3'::double);
                   power
                -----------
                 -Infinity
                (1 row)

                SELECT power('-inf'::double, 'inf'::double);
                  power
                ----------
                 Infinity
                (1 row)

                SELECT power('-inf'::double, '-inf'::double);
                 power
                -------
                     0
                (1 row)

                SELECT sqrt('64'::float8) AS eight;
                 eight
                -------
                     8
                (1 row)

                -- cube root
                SELECT ROUND(cbrt('27'::double), 12) AS three;
                 three
                -------
                     3
                (1 row)

                SELECT f.f1, cbrt(f.f1) AS cbrt_f1 FROM FLOAT8_TBL f;
                          f1          |       cbrt_f1
                ----------------------+--------------------------
                                    0 |                      0.0
                               1004.3 |       10.014312837827033
                               -34.84 |      -3.2660742134420806
                 1.2345678901234e+200 |    4.979338592347648e+66
                 1.2345678901234e-200 |   2.3112042409018007e-67
                (5 rows)

                SELECT * FROM FLOAT8_TBL;
                          f1
                ----------------------
                                    0
                               1004.3
                               -34.84
                 1.2345678901234e+200
                 1.2345678901234e-200
                (5 rows)

                SELECT ROUND(sinh('1'::double), 10);
                      sinh
                -----------------
                 1.1752011936
                (1 row)

                SELECT ROUND(cosh('1'::double), 10);
                       cosh
                ------------------
                 1.5430806348
                (1 row)

                SELECT ROUND(tanh('1'::double), 10);
                       tanh
                -------------------
                 0.761594156
                (1 row)

                SELECT ROUND(asinh('1'::double), 10);
                       asinh
                -------------------
                 0.8813735870
                (1 row)

                SELECT ROUND(acosh('2'::double), 10);
                      acosh
                ------------------
                 1.3169578969
                (1 row)

                SELECT ROUND(atanh('0.5'::double), 10);
                       atanh
                -------------------
                 0.5493061443
                (1 row)

                -- test Inf/NaN cases for hyperbolic functions
                SELECT sinh('infinity'::double);
                   sinh
                ----------
                 Infinity
                (1 row)

                SELECT sinh('-infinity'::double);
                   sinh
                -----------
                 -Infinity
                (1 row)

                SELECT sinh('nan'::double);
                 sinh
                ------
                  NaN
                (1 row)

                SELECT cosh('infinity'::double);
                   cosh
                ----------
                 Infinity
                (1 row)

                SELECT cosh('-infinity'::double);
                   cosh
                ----------
                 Infinity
                (1 row)

                SELECT cosh('nan'::double);
                 cosh
                ------
                  NaN
                (1 row)

                SELECT tanh('infinity'::double);
                 tanh
                ------
                    1
                (1 row)

                SELECT tanh('-infinity'::double);
                 tanh
                ------
                   -1
                (1 row)

                SELECT tanh('nan'::double);
                 tanh
                ------
                  NaN
                (1 row)

                SELECT asinh('infinity'::double);
                  asinh
                ----------
                 Infinity
                (1 row)

                SELECT asinh('-infinity'::double);
                   asinh
                -----------
                 -Infinity
                (1 row)

                SELECT asinh('nan'::double);
                 asinh
                -------
                   NaN
                (1 row)

                SELECT '32767.4'::double::int2;
                 int2
                -------
                 32767
                (1 row)

                SELECT '-32768.4'::double::int2;
                  int2
                --------
                 -32768
                (1 row)

                SELECT '-32767.6'::double::int2;
                  int2
                --------
                  -32767
                (1 row)

                SELECT '2147483647.4'::double::int4;
                    int4
                ------------
                 2147483647
                (1 row)


                SELECT '2147483647.6'::double::int4;
                    int4
                ------------
                 2147483647
                (1 row)

                SELECT '-2147483648.4'::double::int4;
                    int4
                -------------
                 -2147483648
                (1 row)

                -- check edge cases for exp
                SELECT exp('inf'::float8), exp('-inf'::float8), exp('nan'::float8);
                   exp    | exp | exp
                ----------+-----+-----
                 Infinity |   0 | NaN
                (1 row)

                select exp(123.456::float8);
                          exp
                -----------------------
                 4.132944352778106e+53
                (1 row)

                -- the values have been slightly changed to account for fp errors
                -- take exp of ln(f.f1)
                SELECT f.f1, exp(ln(f.f1)) AS exp_ln_f1
                   FROM FLOAT8_TBL f
                   WHERE f.f1 > '0.0';
                          f1          |       exp_ln_f1
                ----------------------+-----------------------
                               1004.3 |      1004.3000000000004
                 1.2345678901234e+200 |  1.234567890123379e+200
                 1.2345678901234e-200 | 1.2345678901233948e-200
                (3 rows)
                
                -- adapted for BROUND; not a Postgres function
                SELECT f.f1, bround(f.f1, 1) AS round_f1
                   FROM FLOAT8_TBL f
                   WHERE abs(f1) < 10000;
                          f1          |       round_f1
                ----------------------+----------------------
                                    0 |                    0
                               1004.3 |               1004.3
                 1.2345678901234e-200 |                    0
                               -34.84 |                -34.8
                (4 rows)"""
        );
    }

/*

-- error functions -> not supported by calcite
                SELECT x,
       erf(x),
       erfc(x)
FROM (VALUES (float8 '-infinity'),
      (-28), (-6), (-3.4), (-2.1), (-1.1), (-0.45),
      (-1.2e-9), (-2.3e-13), (-1.2e-17), (0),
      (1.2e-17), (2.3e-13), (1.2e-9),
      (0.45), (1.1), (2.1), (3.4), (6), (28),
      (float8 'infinity'), (float8 'nan')) AS t(x);
     x     |         erf          |        erfc
-----------+----------------------+---------------------
 -Infinity |                   -1 |                   2
       -28 |                   -1 |                   2
        -6 |                   -1 |                   2
      -3.4 |    -0.99999847800664 |     1.9999984780066
      -2.1 |    -0.99702053334367 |     1.9970205333437
      -1.1 |    -0.88020506957408 |     1.8802050695741
     -0.45 |    -0.47548171978692 |     1.4754817197869
  -1.2e-09 | -1.3540550005146e-09 |     1.0000000013541
  -2.3e-13 | -2.5952720843197e-13 |     1.0000000000003
  -1.2e-17 | -1.3540550005146e-17 |                   1
         0 |                    0 |                   1
   1.2e-17 |  1.3540550005146e-17 |                   1
   2.3e-13 |  2.5952720843197e-13 |    0.99999999999974
   1.2e-09 |  1.3540550005146e-09 |    0.99999999864595
      0.45 |     0.47548171978692 |    0.52451828021308
       1.1 |     0.88020506957408 |    0.11979493042592
       2.1 |     0.99702053334367 |   0.002979466656333
       3.4 |     0.99999847800664 | 1.5219933628623e-06
         6 |                    1 | 2.1519736712499e-17
        28 |                    1 |                   0
  Infinity |                    1 |                   0
       NaN |                  NaN |                 NaN
(22 rows)

-- ignored some of the tests after the update query --> trigonometric functions for degrees not supported
SELECT x,
       sind(x),
       sind(x) IN (-1,-0.5,0,0.5,1) AS sind_exact
FROM (VALUES (0), (30), (90), (150), (180),
      (210), (270), (330), (360)) AS t(x);
  x  | sind | sind_exact
-----+------+------------
   0 |    0 | t
  30 |  0.5 | t
  90 |    1 | t
 150 |  0.5 | t
 180 |    0 | t
 210 | -0.5 | t
 270 |   -1 | t
 330 | -0.5 | t
 360 |    0 | t
(9 rows)

SELECT x,
       cosd(x),
       cosd(x) IN (-1,-0.5,0,0.5,1) AS cosd_exact
FROM (VALUES (0), (60), (90), (120), (180),
      (240), (270), (300), (360)) AS t(x);
  x  | cosd | cosd_exact
-----+------+------------
   0 |    1 | t
  60 |  0.5 | t
  90 |    0 | t
 120 | -0.5 | t
 180 |   -1 | t
 240 | -0.5 | t
 270 |    0 | t
 300 |  0.5 | t
 360 |    1 | t
(9 rows)

SELECT x,
       tand(x),
       tand(x) IN ('-Infinity'::float8,-1,0,
                   1,'Infinity'::float8) AS tand_exact,
       cotd(x),
       cotd(x) IN ('-Infinity'::float8,-1,0,
                   1,'Infinity'::float8) AS cotd_exact
FROM (VALUES (0), (45), (90), (135), (180),
      (225), (270), (315), (360)) AS t(x);
  x  |   tand    | tand_exact |   cotd    | cotd_exact
-----+-----------+------------+-----------+------------
   0 |         0 | t          |  Infinity | t
  45 |         1 | t          |         1 | t
  90 |  Infinity | t          |         0 | t
 135 |        -1 | t          |        -1 | t
 180 |         0 | t          | -Infinity | t
 225 |         1 | t          |         1 | t
 270 | -Infinity | t          |         0 | t
 315 |        -1 | t          |        -1 | t
 360 |         0 | t          |  Infinity | t
(9 rows)

SELECT x,
       asind(x),
       asind(x) IN (-90,-30,0,30,90) AS asind_exact,
       acosd(x),
       acosd(x) IN (0,60,90,120,180) AS acosd_exact
FROM (VALUES (-1), (-0.5), (0), (0.5), (1)) AS t(x);
  x   | asind | asind_exact | acosd | acosd_exact
------+-------+-------------+-------+-------------
   -1 |   -90 | t           |   180 | t
 -0.5 |   -30 | t           |   120 | t
    0 |     0 | t           |    90 | t
  0.5 |    30 | t           |    60 | t
    1 |    90 | t           |     0 | t
(5 rows)

SELECT x,
       atand(x),
       atand(x) IN (-90,-45,0,45,90) AS atand_exact
FROM (VALUES ('-Infinity'::float8), (-1), (0), (1),
      ('Infinity'::float8)) AS t(x);
     x     | atand | atand_exact
-----------+-------+-------------
 -Infinity |   -90 | t
        -1 |   -45 | t
         0 |     0 | t
         1 |    45 | t
  Infinity |    90 | t
(5 rows)

SELECT x, y,
       atan2d(y, x),
       atan2d(y, x) IN (-90,0,90,180) AS atan2d_exact
FROM (SELECT 10*cosd(a), 10*sind(a)
      FROM generate_series(0, 360, 90) AS t(a)) AS t(x,y);
  x  |  y  | atan2d | atan2d_exact
-----+-----+--------+--------------
  10 |   0 |      0 | t
   0 |  10 |     90 | t
 -10 |   0 |    180 | t
   0 | -10 |    -90 | t
  10 |   0 |      0 | t
(5 rows)
*/
    @Test
    public void testPower() {
        this.q("""
                SELECT power(f.f1, '2.0'::DOUBLE) AS square_f1
                   FROM FLOAT8_TBL f where f.f1 = '1004.3';
                     square_f1
                --------------------
                 1008618.4899999999"""
        );
    }

    // Postgres gives a runtime error, but rust returns inf
    @Test
    public void testPower2() {
        this.qs("""
                SELECT power('0'::DOUBLE, '-inf'::DOUBLE);
                 power
                -------
                 Infinity
                (1 row)

                SELECT power('-inf'::double, '3.5'::double);
                 power
                -------
                 Infinity
                (1 row)
                """
        );
    }

    @Test
    public void testFails() {
        this.qf("SELECT acosh('-infinity'::double)", "out of range");
        this.qf("SELECT atanh('infinity'::double)", "out of range");
        this.qf("SELECT atanh('-infinity'::double)", "out of range");
    }

    @Test
    public void castToInt() {
        this.qf("SELECT '32768.6'::double::int2", "Cannot convert");
        this.qf("SELECT '-9223372036854780000'::float8::int8", "Cannot convert");
    }

    @Test
    public void nonPostgresTests() {
        this.qs("""
                SELECT ln(1);
                 ln
                ----
                 0
                (1 row)

                SELECT ln(1e0::float8);
                 ln
                ----
                 0
                (1 row)

                SELECT log10(1);
                 log10
                -------
                 0
                (1 row)

                SELECT log10(1e0::float4);
                 log10
                -------
                 0
                (1 row)

                SELECT round(log(2), 10);
                 log
                -----
                 0.6931471806
                (1 row)

                SELECT log(1.0);
                 log
                -----
                 0
                (1 row)

                SELECT log(1, 2);
                 log
                -----
                 0
                (1 row)

                SELECT log(1.0, 2.0);
                 log
                -----
                 0
                (1 row)

                SELECT ROUND(log(3.0, 4.0), 10);
                 log
                -----
                 0.7924812504
                (1 row)

                SELECT ROUND(log(7.7, PI), 10);
                 log
                -----
                 1.7831458356
                (1 row)

                SELECT log(64.0, 2.0);
                 log
                -----
                 6.0
                (1 row)

                select log(2, 0);
                 log
                -----
                 0
                (1 row)

                select log(1, 0);
                 log
                -----
                 0
                (1 row)
                """
        );
    }

    @Test
    public void testLogFail() {
        this.runtimeFail("select ln(-1)", "Unable to calculate ln for -1", this.streamWithEmptyChanges());
        this.runtimeFail("select log10(-1)", "Unable to calculate log10 for -1", this.streamWithEmptyChanges());
        this.runtimeFail("select log(-1)", "Unable to calculate ln for -1", this.streamWithEmptyChanges());
        this.runtimeFail("select log(0, -1)", "Unable to calculate log(0, -1)", this.streamWithEmptyChanges());
        this.runtimeFail("select log(-1, 0)", "Unable to calculate log(-1, 0)", this.streamWithEmptyChanges());
        this.runtimeFail("select log(-1, -1)", "Unable to calculate log(-1, -1)", this.streamWithEmptyChanges());
    }

    // Moved here from `PostgresNumericTests`
    @Test
    public void testFpDiv() {
        // no div or mod defined for fp, so I removed these
        this.q("""
                WITH v(x) AS
                  (VALUES(0E0),(1E0),(-1E0),(4.2E0),(CAST ('Infinity' AS DOUBLE)),(CAST ('-Infinity' AS DOUBLE)),\
                (CAST ('nan' AS DOUBLE)))
                SELECT x1, x2,
                  x1 / x2 AS quot
                --  x1 % x2 AS m,
                --  div(x1, x2) AS div
                FROM v AS v1(x1), v AS v2(x2) WHERE x2 != 0E0;
                    x1     |    x2     |          quot
                -----------+-----------+------------------------
                         0 |         1 |  0.00000000000000000000
                         1 |         1 |  1.00000000000000000000
                        -1 |         1 | -1.00000000000000000000
                       4.2 |         1 |      4.2000000000000000
                  Infinity |         1 |                Infinity
                 -Infinity |         1 |               -Infinity
                       NaN |         1 |                     NaN
                         0 |        -1 | -0.00000000000000000000
                         1 |        -1 | -1.00000000000000000000
                        -1 |        -1 |  1.00000000000000000000
                       4.2 |        -1 |     -4.2000000000000000
                  Infinity |        -1 |               -Infinity
                 -Infinity |        -1 |                Infinity
                       NaN |        -1 |                     NaN
                         0 |       4.2 |  0.00000000000000000000
                         1 |       4.2 |  0.23809523809523809524
                        -1 |       4.2 | -0.23809523809523809524
                       4.2 |       4.2 |  1.00000000000000000000
                  Infinity |       4.2 |                Infinity
                 -Infinity |       4.2 |               -Infinity
                       NaN |       4.2 |                     NaN
                         0 |  Infinity |                       0
                         1 |  Infinity |                       0
                        -1 |  Infinity |                      -0
                       4.2 |  Infinity |                       0
                  Infinity |  Infinity |                     NaN
                 -Infinity |  Infinity |                     NaN
                       NaN |  Infinity |                     NaN
                         0 | -Infinity |                      -0
                         1 | -Infinity |                      -0
                        -1 | -Infinity |                       0
                       4.2 | -Infinity |                      -0
                  Infinity | -Infinity |                     NaN
                 -Infinity | -Infinity |                     NaN
                       NaN | -Infinity |                     NaN
                         0 |       NaN |                     NaN
                         1 |       NaN |                     NaN
                        -1 |       NaN |                     NaN
                       4.2 |       NaN |                     NaN
                  Infinity |       NaN |                     NaN
                 -Infinity |       NaN |                     NaN
                       NaN |       NaN |                     NaN""");
    }

    // Moved here from `PostgresNumericTests`
    @Test
    public void testSpecialValues() {
        // This test was written with NUMERIC values, but was converted to FP
        this.q(
                """
                        WITH v(x) AS (VALUES(0E0),(1E0),(-1E0),(4.2E0),(CAST ('Infinity' AS DOUBLE)),(CAST ('-Infinity' AS DOUBLE)),(CAST ('nan' AS DOUBLE)))
                        SELECT x1, x2,
                          x1 + x2 AS s,
                          x1 - x2 AS diff,
                          x1 * x2 AS prod
                        FROM v AS v1(x1), v AS v2(x2);
                            x1     |    x2     |    sum    |   diff    |   prod   \s
                        -----------+-----------+-----------+-----------+-----------
                                 0 |         0 |         0 |         0 |         0
                                 0 |         1 |         1 |        -1 |         0
                                 0 |        -1 |        -1 |         1 |        -0
                                 0 |       4.2 |       4.2 |      -4.2 |       0.0
                                 0 |  Infinity |  Infinity | -Infinity |       NaN
                                 0 | -Infinity | -Infinity |  Infinity |       NaN
                                 0 |       NaN |       NaN |       NaN |       NaN
                                 1 |         0 |         1 |         1 |         0
                                 1 |         1 |         2 |         0 |         1
                                 1 |        -1 |         0 |         2 |        -1
                                 1 |       4.2 |       5.2 |      -3.2 |       4.2
                                 1 |  Infinity |  Infinity | -Infinity |  Infinity
                                 1 | -Infinity | -Infinity |  Infinity | -Infinity
                                 1 |       NaN |       NaN |       NaN |       NaN
                                -1 |         0 |        -1 |        -1 |        -0
                                -1 |         1 |         0 |        -2 |        -1
                                -1 |        -1 |        -2 |         0 |         1
                                -1 |       4.2 |       3.2 |      -5.2 |      -4.2
                                -1 |  Infinity |  Infinity | -Infinity | -Infinity
                                -1 | -Infinity | -Infinity |  Infinity |  Infinity
                                -1 |       NaN |       NaN |       NaN |       NaN
                               4.2 |         0 |       4.2 |       4.2 |       0.0
                               4.2 |         1 |       5.2 |       3.2 |       4.2
                               4.2 |        -1 |       3.2 |       5.2 |      -4.2
                               4.2 |       4.2 |       8.4 |       0.0 |     17.64
                               4.2 |  Infinity |  Infinity | -Infinity |  Infinity
                               4.2 | -Infinity | -Infinity |  Infinity | -Infinity
                               4.2 |       NaN |       NaN |       NaN |       NaN
                          Infinity |         0 |  Infinity |  Infinity |       NaN
                          Infinity |         1 |  Infinity |  Infinity |  Infinity
                          Infinity |        -1 |  Infinity |  Infinity | -Infinity
                          Infinity |       4.2 |  Infinity |  Infinity |  Infinity
                          Infinity |  Infinity |  Infinity |       NaN |  Infinity
                          Infinity | -Infinity |       NaN |  Infinity | -Infinity
                          Infinity |       NaN |       NaN |       NaN |       NaN
                         -Infinity |         0 | -Infinity | -Infinity |       NaN
                         -Infinity |         1 | -Infinity | -Infinity | -Infinity
                         -Infinity |        -1 | -Infinity | -Infinity |  Infinity
                         -Infinity |       4.2 | -Infinity | -Infinity | -Infinity
                         -Infinity |  Infinity |       NaN | -Infinity | -Infinity
                         -Infinity | -Infinity | -Infinity |       NaN |  Infinity
                         -Infinity |       NaN |       NaN |       NaN |       NaN
                               NaN |         0 |       NaN |       NaN |       NaN
                               NaN |         1 |       NaN |       NaN |       NaN
                               NaN |        -1 |       NaN |       NaN |       NaN
                               NaN |       4.2 |       NaN |       NaN |       NaN
                               NaN |  Infinity |       NaN |       NaN |       NaN
                               NaN | -Infinity |       NaN |       NaN |       NaN
                               NaN |       NaN |       NaN |       NaN |       NaN""");
    }

    @Test
    public void testModulo() {
        this.qs("""
                select 1.12::DOUBLE % 0.3::DOUBLE;
                 ?column?
                ----------
                     0.22
                (1 row)

                select 1.12::DOUBLE % -0.3::DOUBLE;
                 ?column?
                ----------
                     0.22
                (1 row)

                select -1.12::DOUBLE % 0.3::DOUBLE;
                 ?column?
                ----------
                    -0.22
                (1 row)

                select -1.12::DOUBLE % -0.3::DOUBLE;
                 ?column?
                ----------
                    -0.22
                (1 row)
                """
        );
    }
}
