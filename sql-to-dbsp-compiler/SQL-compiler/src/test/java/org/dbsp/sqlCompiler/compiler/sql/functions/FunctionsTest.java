package org.dbsp.sqlCompiler.compiler.sql.functions;

import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.sql.tools.SqlIoTest;
import org.junit.Test;

public class FunctionsTest extends SqlIoTest {
    @Override
    public void prepareInputs(DBSPCompiler compiler) {
        String setup = """
                CREATE TABLE ARR_TABLE (VALS INTEGER ARRAY NOT NULL,ID INTEGER NOT NULL);
                INSERT INTO ARR_TABLE VALUES(ARRAY [1, 2, 3], 6);
                INSERT INTO ARR_TABLE VALUES(ARRAY [1, 2, 3], 7);""";
        compiler.submitStatementsForCompilation(setup);
    }

    @Test
    public void testSqlFunction() {
        this.getCCS("""
                CREATE FUNCTION displayIPv4(ip BINARY(4)) RETURNS VARCHAR(15)
                AS CAST( TO_INT( SUBSTRING( ip, 1, 1)) AS VARCHAR(3) ) || '.'
                    || CAST( TO_INT( SUBSTRING( ip, 2, 1)) AS VARCHAR(3) ) || '.'
                    || CAST( TO_INT( SUBSTRING( ip, 3, 1)) AS VARCHAR(3) ) || '.'
                    || CAST( TO_INT( SUBSTRING( ip, 4, 1)) AS VARCHAR(3) );""");
    }

    @Test
    public void testTypeError() {
        this.statementsFailingInCompilation("CREATE FUNCTION error(x INTEGER) RETURNS INTEGER AS ''",
                "should return");
    }

    @Test
    public void testBround() {
        this.qs("""
                SELECT BROUND(1.123, 0), BROUND(1.123, 1), BROUND(1.123, 2), BROUND(1.123, 3);
                 a |   b |    c |     d
                ------------------------
                 1 | 1.1 | 1.12 | 1.123
                (1 row)
                
                SELECT BROUND(1.15, 1), BROUND(1.25, 1), BROUND(1.55, 1);
                 a   |   b |   c
                -----------------
                 1.2 | 1.2 | 1.6
                (1 row)
                
                SELECT BROUND(11.15, -1), BROUND(15.25, -1), BROUND(25.55, -1);
                 a  |  b |  c
                --------------
                 10 | 20 | 30
                (1 row)
                
                SELECT BROUND(11, -4), BROUND(11.25e0, 1), BROUND(0, CAST(1 AS TINYINT));
                 a   |    b | c
                ----------------
                 0   | 11.2 | 0
                (1 row)""");
    }

    @Test
    public void testBetween() {
        this.qs("""
                SELECT TIMESTAMP '2020-02-02 10:00:00' between TIMESTAMP '2020-02-02 09:00:00' and TIMESTAMP '2020-02-02 11:00:00';
                 result
                --------
                 true
                (1 row)

                SELECT INTERVAL 5 MONTHS between INTERVAL 4 MONTHS and INTERVAL 1 YEAR;
                 result
                --------
                 true
                (1 row)

                SELECT INTERVAL '1:01' HOURS TO MINUTES between INTERVAL 1 HOURS and INTERVAL 1 DAYS;
                 result
                --------
                 true
                (1 row)

                SELECT DATE '2020-02-02' between DATE '2020-01-01' and DATE '2020-03-03';
                 result
                --------
                 true
                (1 row)

                SELECT TIME '10:00:00' between TIME '09:00:00' and TIME '11:00:00';
                 result
                --------
                 true
                (1 row)""");
    }

    @Test
    public void testSqlFunc() {
        var ccs = this.getCCS("""
                CREATE FUNCTION dbl(x INTEGER) RETURNS INTEGER AS x * 2;
                CREATE VIEW V AS SELECT dbl(3);""");
        ccs.step("", """
                 r | weight
                ------------
                 6 | 1""");
        ccs = this.getCCS("""
                CREATE FUNCTION contains_number(str VARCHAR NOT NULL, value INTEGER)
                RETURNS BOOLEAN NOT NULL
                AS (str LIKE ('%' || COALESCE(CAST(value AS VARCHAR), 'NULL') || '%'));
                CREATE VIEW V AS SELECT contains_number(CAST('YES: 10 NO:5' AS VARCHAR), 5)""");
        ccs.step("", """
                 result | weight
                -----------------
                 t      | 1""");
    }

    @Test
    public void testIssue1450() {
        this.q("""
                SELECT 0.809 * 100;
                 result
                ---------
                80.9""");
    }

    @Test
    public void issue1520() {
        this.qs("""
                select 3.456::decimal(25, 3) + 1.123::decimal(4, 0);
                 ?column?
                ----------
                 4.456
                (1 row)

                select 3.456::decimal(25, 3) - 1.123::decimal(4, 0);
                 ?column?
                ----------
                 2.456
                (1 row)

                select 3.456::decimal(25, 3) * 1.123::decimal(4, 0);
                 ?column?
                ----------
                 3.456
                (1 row)

                select 3.456::decimal(25, 3) / 1.234::decimal(4, 1);
                 ?column?
                ----------
                 2.88
                (1 row)"""
        );
    }

    @Test
    public void testUnnest() {
        this.q("""
                select * from arr_table;
                  vals   | id
                ---------+----
                 {1,2,3} |  6
                 {1,2,3} |  7"""
        );
        this.q("""
                SELECT VAL FROM ARR_TABLE, UNNEST(VALS) AS VAL;
                 val
                -----
                 1
                 2
                 3
                 1
                 2
                 3"""
        );
    }

    @Test
    public void testLeft() {
        this.q("""
                SELECT LEFT('string', 1);
                result
                ---------
                 s""");
        this.q("""
                SELECT LEFT('string', 0);
                result
                ---------
                \s""");
        this.q("""
                SELECT LEFT('string', 100);
                result
                ---------
                 string""");
        this.q("""
                SELECT LEFT('string', -2);
                result
                ---------
                \s""");
    }


    @Test
    public void testBinaryCast() {
        this.qs("""
                SELECT CAST('1234567890' AS VARBINARY) as val;
                 val
                -----
                 31323334353637383930
                (1 row)
                
                SELECT '1234567890'::VARBINARY as val;
                 val
                -----
                 31323334353637383930
                (1 row)
                """);
    }

    @Test
    public void issue1180() {
        this.runtimeConstantFail("SELECT '1_000'::INT4",
                "invalid digit found in string");
    }

    @Test
    public void issue1192() {
        this.runtimeConstantFail("select '-9223372036854775809'::int64", "number too small to fit in target type");
        this.runtimeConstantFail("select '9223372036854775808'::int64", "number too large to fit in target type");
    }

    // this is an edge case for negative integer modulo
    // Rust's default modulo operator currently panics on
    // INT::MIN % -1 instead of just returning 0
    @Test
    public void issue1187modulo() {
        this.qs(
                """
                SELECT (-128)::tinyint % (-1)::tinyint as x;
                 x
                ---
                 0
                (1 row)

                SELECT (-32768)::int2 % (-1)::int2 as x;
                x
                ---
                 0
                (1 row)

                SELECT (-2147483648)::int4 % (-1)::int4 as x;
                x
                ---
                 0
                (1 row)

                SELECT (-9223372036854775808)::int64 % (-1)::int64 as x;
                x
                ---
                 0
                (1 row)

                SELECT (8)::tinyint % (3)::tinyint as x;
                x
                ---
                 2
                (1 row)

                SELECT (8)::int2 % (3)::int2 as x;
                x
                ---
                 2
                (1 row)

                SELECT (8)::int4 % (3)::int4 as x;
                x
                ---
                 2
                (1 row)

                SELECT (8)::int64 % (3)::int64 as x;
                x
                ---
                 2
                (1 row)

                SELECT (8)::tinyint % (-3)::tinyint as x;
                x
                ---
                 2
                (1 row)

                SELECT (8)::int2 % (-3)::int2 as x;
                x
                ---
                 2
                (1 row)

                SELECT (8)::int4 % (-3)::int4 as x;
                x
                ---
                 2
                (1 row)

                SELECT (8)::int64 % (-3)::int64 as x;
                x
                ---
                 2
                (1 row)

                SELECT (-8)::tinyint % (3)::tinyint as x;
                x
                ---
                 -2
                (1 row)

                SELECT (-8)::int2 % (3)::int2 as x;
                x
                ---
                 -2
                (1 row)

                SELECT (-8)::int4 % (3)::int4 as x;
                x
                ---
                 -2
                (1 row)

                SELECT (-8)::int64 % (3)::int64 as x;
                x
                ---
                 -2
                (1 row)

                SELECT (-8)::tinyint % (-3)::tinyint as x;
                x
                ---
                 -2
                (1 row)

                SELECT (-8)::int2 % (-3)::int2 as x;
                x
                ---
                 -2
                (1 row)

                SELECT (-8)::int4 % (-3)::int4 as x;
                x
                ---
                 -2
                (1 row)

                SELECT (-8)::int64 % (-3)::int64 as x;
                x
                ---
                 -2
                (1 row)
                """
        );
    }

    @Test
    public void issue1209() {
        this.qf("SELECT '-32768'::int2 % 0::int2", "attempt to calculate the remainder with a divisor of zero");

        this.qf("SELECT '-32768'::int2 / 0::int2", "attempt to divide by zero", true);

        this.qf("SELECT '-32768'::int2 / 0::int2", "attempt to divide by zero", false);
    }

    @Test
    public void issue1187divisorZero() {
        this.runtimeConstantFail("SELECT 8 % 0", "attempt to calculate the remainder with a divisor of zero");
        this.shouldWarn("CREATE VIEW V AS SELECT 8 % 0", "Modulus by constant zero value as divisor.");
    }

    @Test
    public void testSqrtNull() {
        this.q("""
                SELECT sqrt(null);
                 sqrt
                ------
                null"""
        );
    }

    @Test
    public void testLeftNull() {
        this.q("""
                SELECT LEFT(NULL, 100);
                result
                ---------
                NULL""");
    }

    @Test
    public void testConcat() {
        this.qs("""
                SELECT CONCAT('string', 1);
                result
                ---------
                 string1
                (1 row)

                SELECT CONCAT('string', 1, true);
                result
                ---------
                 string1TRUE
                (1 row)""");
    }

    @Test
    public void testCoalesce() {
        this.q("""
                SELECT COALESCE(NULL, 5);
                result
                ------
                5""");
        this.q("""
                SELECT COALESCE(5, NULL);
                result
                ------
                5""");
        this.q("""
                SELECT COALESCE(NULL, 5, NULL);
                result
                ------
                5""");
        this.q("""
                SELECT COALESCE(2, 3, 4);
                result
                ------
                2""");
        this.q("""
                SELECT COALESCE(NULL, NULL);
                result
                ------
                NULL""");
    }

    // Calcite rounds differently from Postgres and other databases
    @Test
    public void testRounding() {
        this.qs("""
                select CAST((CAST('1234.1264' AS DECIMAL(8, 4))) AS DECIMAL(6, 2));
                 cast
                ------
                 1234.12
                (1 row)

                select CAST((CAST('1234.1234' AS DECIMAL(8, 4))) AS DECIMAL(6, 2));
                 cast
                ------
                 1234.12
                (1 row)

                select CAST((CAST('-1234.1264' AS DECIMAL(8, 4))) AS DECIMAL(6, 2));
                 cast
                ------
                 -1234.12
                (1 row)

                select CAST((CAST('-1234.1234' AS DECIMAL(8, 4))) AS DECIMAL(6, 2));
                 cast
                ------
                 -1234.12
                (1 row)

                select CAST((CAST('1234.1250' AS DECIMAL(8, 4))) AS DECIMAL(6, 2));
                 cast
                ------
                 1234.12
                (1 row)

                select CAST((CAST('-1234.1250' AS DECIMAL(8, 4))) AS DECIMAL(6, 2));
                 cast
                ------
                 -1234.12
                (1 row)

                select CAST((CAST('.1264' AS DECIMAL(4, 4))) AS DECIMAL(2, 2));
                 cast
                ------
                 0.12
                (1 row)

                select CAST((CAST('.1234' AS DECIMAL(4, 4))) AS DECIMAL(2, 2));
                 cast
                ------
                 0.12
                (1 row)

                select CAST((CAST('-.1264' AS DECIMAL(4, 4))) AS DECIMAL(2, 2));
                 cast
                ------
                 -0.12
                (1 row)

                select CAST((CAST('-.1234' AS DECIMAL(4, 4))) AS DECIMAL(2, 2));
                 cast
                ------
                 -0.12
                (1 row)

                select CAST((CAST('00.1264' AS DECIMAL(4, 4))) AS DECIMAL(2, 2));
                 cast
                ------
                 0.12
                (1 row)

                select CAST((CAST('00.1234' AS DECIMAL(4, 4))) AS DECIMAL(2, 2));
                 cast
                ------
                 0.12
                (1 row)

                select CAST((CAST('-00.1264' AS DECIMAL(4, 4))) AS DECIMAL(2, 2));
                 cast
                ------
                 -0.12
                (1 row)

                select CAST((CAST('-00.1234' AS DECIMAL(4, 4))) AS DECIMAL(2, 2));
                 cast
                ------
                 -0.12
                (1 row)

                -- the following tests are from mysql
                select cast('1.00000001335143196001808973960578441619873046875E-10' as decimal(28,15));
                     decimal
                -------------------
                 0.000000000100000
                (1 row)

                select ln(14000) as c1, cast(ln(14000) as decimal(5,3)) as c2, cast(ln(14000) as decimal(5,3)) as c3;
                        c1         |  c2   |  c3
                -------------------+-------+-------
                 9.546812608597396 | 9.546 | 9.546
                (1 row)

                select cast(143.481 as decimal(4,1));
                 cast(143.481 as decimal(4,1))
                -------------------------------
                143.4
                (1 row)

                select cast(143.481 as decimal(4,0));
                 cast(143.481 as decimal(4,0))
                -------------------------------
                143
                (1 row)

                select cast(-3.4 as decimal(2,1));
                 cast(-3.4 as decimal(2,1))
                -------------------------------
                -3.4
                (1 row)

                select cast(98.6 as decimal(2,0));
                 cast(98.6 as decimal(2,0))
                -------------------------------
                98
                (1 row)
                """
        );
    }

    @Test
    public void testDecimalErrors() {
        this.runtimeConstantFail("select cast(1234.1234 AS DECIMAL(6, 3))",
                "Cannot represent 1234.123 as DECIMAL(6, 3): precision of DECIMAL type too small to represent value");
        this.runtimeConstantFail("select cast(1234.1236 AS DECIMAL(6, 3))",
                "Cannot represent 1234.123 as DECIMAL(6, 3): precision of DECIMAL type too small to represent value");
        this.runtimeConstantFail("select cast(143.481 as decimal(2, 1))",
                "Cannot represent 143.4 as DECIMAL(2, 1): precision of DECIMAL type too small to represent value");
        this.q("""
                select cast(99.6 as decimal(2, 0));
                 result
                --------
                 99""");
        this.runtimeConstantFail("select cast(-13.4 as decimal(2,1))",
                "Cannot represent -13.4 as DECIMAL(2, 1)");
    }

    @Test
    public void testLn() {
        this.qs("""
                SELECT ROUND(ln(2e0), 12);
                 ln
                ----
                 0.69314718056
                (1 row)

                SELECT ROUND(ln(2.0), 12);
                 ln
                ----
                 0.69314718056
                (1 row)
                """
        );
    }

    @Test
    public void testIsInf() {
        this.qs(
                """
                        SELECT IS_INF(null);
                        is_inf
                        -------
                        null
                        (1 row)

                        SELECT IS_INF(1);
                        is_inf
                        -------
                         f
                        (1 row)

                        SELECT IS_INF('INF'::DOUBLE);
                        is_inf
                        -------
                         t
                        (1 row)

                        SELECT IS_INF('-INF'::DOUBLE);
                        is_inf
                        -------
                         t
                        (1 row)

                        SELECT IS_INF('Infinity'::DOUBLE);
                        is_inf
                        -------
                         t
                        (1 row)

                        SELECT IS_INF('-Infinity'::DOUBLE);
                        is_inf
                        -------
                         t
                        (1 row)

                        -- f64::MAX
                        SELECT IS_INF(1.7976931348623157e308);
                        is_inf
                        -------
                         f
                        (1 row)

                        -- f64::MIN
                        SELECT IS_INF(-1.7976931348623157e308);
                        is_inf
                        -------
                         f
                        (1 row)

                        SELECT IS_INF(1e0 / 0e0);
                         IS_INF
                        --------
                         t
                        (1 row)

                        SELECT IS_INF(-1e0 / 0e0);
                         IS_INF
                        --------
                         t
                        (1 row)
                        """
        );
    }

    @Test
    public void testIsInfReal() {
        this.qs("""
                -- f64::MAX
                SELECT is_inf(1.7976931348623157e308::real);
                real
                -------
                 t
                (1 row)

                -- f64::MIN
                SELECT is_inf(-1.7976931348623157e308::real);
                real
                -------
                 t
                (1 row)
                """
        );
    }

    @Test
    public void testIsNan() {
        this.qs("""
                SELECT is_nan(null);
                 is_nan
                --------
                 null
                (1 row)

                SELECT is_nan('nan'::double);
                 is_nan
                --------
                 t
                (1 row)

                SELECT is_nan(0e0 / 0e0);
                 is_nan
                --------
                 t
                (1 row)

                SELECT is_nan(-0e0 / 0e0);
                 is_nan
                --------
                 t
                (1 row)

                SELECT is_nan(0.0);
                 is_nan
                --------
                 f
                (1 row)

                SELECT is_nan(1.7976931348623157e308);
                 is_nan
                --------
                 f
                (1 row)

                SELECT is_nan(-1.7976931348623157e308);
                 is_nan
                --------
                 f
                (1 row)
                """
        );
    }

    @Test
    public void testPower() {
        this.qs("""
                SELECT power(2e0, 2);
                 power
                -------
                 4.0000000000000000
                (1 row)

                SELECT power(2e0::real, 2);
                 power
                -------
                 4.0000000000000000
                (1 row)

                SELECT power(2, 2e0);
                 power
                -------
                 4.0000000000000000
                (1 row)

                SELECT power(2e0, 2.0);
                 power
                -------
                 4.0000000000000000
                (1 row)

                SELECT power(2.0, 2e0);
                 power
                -------
                 4.0000000000000000
                (1 row)

                SELECT power(2e0, 2e0);
                 power
                -------
                 4.0000000000000000
                (1 row)

                SELECT power(2.0, 2.0);
                 power
                -------
                 4.0000000000000000
                (1 row)

                SELECT power(2.0, 2);
                 power
                -------
                 4.0000000000000000
                (1 row)


                SELECT power(2, 2.0);
                 power
                -------
                 4.0000000000000000
                (1 row)

                SELECT POWER(2, 2);
                 power
                -------
                 4
                (1 row)
                """
        );
    }

    @Test
    public void testSqrtDouble() {
        this.qs("""
                SELECT sqrt(9e0);
                 sqrt
                ------
                 3
                (1 row)

                SELECT sqrt(null::double);
                 sqrt
                ------
                null
                (1 row)
                """
        );
    }

    @Test
    public void testRoundDecimalDecimal() {
        this.queryFailingInCompilation("SELECT round(15.1, 1.0)",
                "Cannot apply 'ROUND' to arguments of type 'ROUND(<DECIMAL(3, 1)>, <DECIMAL(2, 1)>)'.");
    }

    @Test
    public void testRound() {
        this.qs("""
                select round(15.1);
                 round(15.1)
                ------------
                 15
                (1 row)

                select round(15.4);
                 round(15.4)
                ------------
                 15
                (1 row)

                select round(15.5);
                 round(15.5)
                ------------
                 16
                (1 row)

                select round(15.6);
                 round(15.6)
                ------------
                 16
                (1 row)

                select round(15.9);
                 round(15.9)
                ------------
                 16
                (1 row)

                select round(-15.1);
                 round(-15.1)
                ------------
                 -15
                (1 row)

                select round(-15.4);
                 round(-15.4)
                ------------
                 -15
                (1 row)

                select round(-15.5);
                 round(-15.5)
                ------------
                 -16
                (1 row)

                select round(-15.6);
                 round(-15.6)
                ------------
                 -16
                (1 row)

                select round(-15.9);
                 round(-15.9)
                ------------
                 -16
                (1 row)

                select round(15.1,1);
                 round(15.1,1)
                ------------
                 15.1
                (1 row)

                select round(15.4,1);
                 round(15.4,1)
                ------------
                 15.4
                (1 row)

                select round(15.5,1);
                 round(15.5,1)
                ------------
                 15.5
                (1 row)

                select round(15.6,1);
                 round(15.6,1)
                ------------
                 15.6
                (1 row)

                select round(15.9,1);
                 round(15.9,1)
                ------------
                 15.9
                (1 row)

                select round(-15.1,1);
                round(-15.1,1)
                ------------
                 -15.1
                (1 row)

                select round(-15.4,1);
                round(-15.4,1)
                ------------
                 -15.4
                (1 row)

                select round(-15.5,1);
                round(-15.5,1)
                ------------
                 -15.5
                (1 row)

                select round(-15.6,1);
                round(-15.6,1)
                ------------
                 -15.6
                (1 row)

                select round(-15.9,1);
                round(-15.9,1)
                ------------
                 -15.9
                (1 row)

                select round(15.1,0);
                round(15.1,0)
                ------------
                 15
                (1 row)

                select round(15.4,0);
                 round(15.4,0)
                ------------
                 15
                (1 row)

                select round(15.5,0);
                round(15.5,0)
                ------------
                 16
                (1 row)

                select round(15.6,0);
                round(15.6,0)
                ------------
                 16
                (1 row)

                select round(15.9,0);
                round(15.9,0)
                ------------
                 16
                (1 row)

                select round(-15.1,0);
                round(-15.1,0)
                ------------
                 -15
                (1 row)

                select round(-15.4,0);
                round(-15.4,0)
                ------------
                 -15
                (1 row)

                select round(-15.5,0);
                round(-15.5,0)
                ------------
                 -16
                (1 row)

                select round(-15.6,0);
                round(-15.6,0)
                ------------
                 -16
                (1 row)

                select round(-15.9,0);
                round(-15.9,0)
                ------------
                 -16
                (1 row)

                select round(15.1,-1);
                round(15.1,-1)
                ------------
                 20
                (1 row)

                select round(15.4,-1);
                round(15.4,-1)
                ------------
                 20
                (1 row)

                select round(15.5,-1);
                round(15.5,-1)
                ------------
                 20
                (1 row)

                select round(15.6,-1);
                round(15.6,-1)
                ------------
                 20
                (1 row)

                select round(15.9,-1);
                round(15.9,-1)
                ------------
                 20
                (1 row)

                select round(-15.1,-1);
                round(-15.1,-1)
                ------------
                 -20
                (1 row)

                select round(-15.4,-1);
                round(-15.4,-1)
                ------------
                 -20
                (1 row)

                select round(-15.5,-1);
                round(-15.5,-1)
                ------------
                 -20
                (1 row)

                select round(-15.6,-1);
                round(-15.6,-1)
                ------------
                 -20
                (1 row)

                select round(-15.91,-1);
                round(-15.91,-1)
                ------------
                 -20
                (1 row)

                select round(-15.91,-1::int);
                round(-15.91,-1)
                ------------
                 -20
                (1 row)


                select round(15.1);
                 round(15.1)
                ------------
                 15
                (1 row)

                select round(15.4);
                 round(15.4)
                ------------
                 15
                (1 row)

                select round(15.5);
                 round(15.5)
                ------------
                 16
                (1 row)

                select round(15.6);
                 round(15.6)
                ------------
                 16
                (1 row)

                select round(15.9);
                 round(15.9)
                ------------
                 16
                (1 row)

                select round(-15.1);
                 round(-15.1)
                ------------
                 -15
                (1 row)

                select round(-15.4);
                 round(-15.4)
                ------------
                 -15
                (1 row)

                select round(-15.5);
                 round(-15.5)
                ------------
                 -16
                (1 row)

                select round(-15.6);
                 round(-15.6)
                ------------
                 -16
                (1 row)

                select round(-15.9);
                 round(-15.9)
                ------------
                 -16
                (1 row)

                select round(15.1::DOUBLE,1);
                 round(15.1::DOUBLE,1)
                ------------
                 15.1
                (1 row)

                select round(15.4::DOUBLE,1);
                 round(15.4::DOUBLE,1)
                ------------
                 15.4
                (1 row)

                select round(15.5::DOUBLE,1);
                 round(15.5::DOUBLE,1)
                ------------
                 15.5
                (1 row)

                select round(15.6::DOUBLE,1);
                 round(15.6::DOUBLE,1)
                ------------
                 15.6
                (1 row)

                select round(15.9::DOUBLE,1);
                 round(15.9::DOUBLE,1)
                ------------
                 15.9
                (1 row)

                select round(-15.1::DOUBLE,1);
                round(-15.1::DOUBLE,1)
                ------------
                 -15.1
                (1 row)

                select round(-15.4::DOUBLE,1);
                round(-15.4::DOUBLE,1)
                ------------
                 -15.4
                (1 row)

                select round(-15.5::DOUBLE,1);
                round(-15.5::DOUBLE,1)
                ------------
                 -15.5
                (1 row)

                select round(-15.6::DOUBLE,1);
                round(-15.6::DOUBLE,1)
                ------------
                 -15.6
                (1 row)

                select round(-15.9::DOUBLE,1);
                round(-15.9::DOUBLE,1)
                ------------
                 -15.9
                (1 row)

                select round(15.1::DOUBLE,0);
                round(15.1::DOUBLE,0)
                ------------
                 15
                (1 row)

                select round(15.4::DOUBLE,0);
                 round(15.4::DOUBLE,0)
                ------------
                 15
                (1 row)

                select round(15.5::DOUBLE,0);
                round(15.5::DOUBLE,0)
                ------------
                 16
                (1 row)

                select round(15.6::DOUBLE,0);
                round(15.6::DOUBLE,0)
                ------------
                 16
                (1 row)

                select round(15.9::DOUBLE,0);
                round(15.9::DOUBLE,0)
                ------------
                 16
                (1 row)

                select round(-15.1::DOUBLE,0);
                round(-15.1::DOUBLE,0)
                ------------
                 -15
                (1 row)

                select round(-15.4::DOUBLE,0);
                round(-15.4::DOUBLE,0)
                ------------
                 -15
                (1 row)

                select round(-15.5::DOUBLE,0);
                round(-15.5::DOUBLE,0)
                ------------
                 -16
                (1 row)

                select round(-15.6::DOUBLE,0);
                round(-15.6::DOUBLE,0)
                ------------
                 -16
                (1 row)

                select round(-15.9::DOUBLE,0);
                round(-15.9::DOUBLE,0)
                ------------
                 -16
                (1 row)

                select round(15.1::DOUBLE,-1);
                round(15.1::DOUBLE,-1)
                ------------
                 20
                (1 row)

                select round(15.4::DOUBLE,-1);
                round(15.4::DOUBLE,-1)
                ------------
                 20
                (1 row)

                select round(15.5::DOUBLE,-1);
                round(15.5::DOUBLE,-1)
                ------------
                 20
                (1 row)

                select round(15.6::DOUBLE,-1);
                round(15.6::DOUBLE,-1)
                ------------
                 20
                (1 row)

                select round(15.9::DOUBLE,-1);
                round(15.9::DOUBLE,-1)
                ------------
                 20
                (1 row)

                select round(-15.1::DOUBLE,-1);
                round(-15.1::DOUBLE,-1)
                ------------
                 -20
                (1 row)

                select round(-15.4::DOUBLE,-1);
                round(-15.4::DOUBLE,-1)
                ------------
                 -20
                (1 row)

                select round(-15.5::DOUBLE,-1);
                round(-15.5::DOUBLE,-1)
                ------------
                 -20
                (1 row)

                select round(-15.6::DOUBLE,-1);
                round(-15.6::DOUBLE,-1)
                ------------
                 -20
                (1 row)

                select round(-15.91::DOUBLE,-1);
                round(-15.91::DOUBLE,-1)
                ------------
                 -20
                (1 row)

                select round(-15.91::DOUBLE,-1::int);
                round(-15.91::DOUBLE,-1)
                ------------
                 -20
                (1 row)

                select round(null::double);
                 round
                -------
                 NULL
                (1 row)

                select round(null::double, null);
                 round
                -------
                 NULL
                (1 row)

                select round(null::DECIMAL);
                 round
                -------
                 NULL
                (1 row)

                select round(null::DECIMAL, null);
                 round
                -------
                 NULL
                (1 row)
                """
        );
    }

    @Test
    public void testTruncate() {
        this.qs("""
                select truncate(5678.123451);
                truncate(5678.123451)
                -----
                5678
                (1 row)

                select truncate(5678.123451,0);
                truncate(5678.123451,0)
                -----
                5678
                (1 row)

                select truncate(5678.123451,1);
                truncate(5678.123451,1)
                -----
                5678.1
                (1 row)

                select truncate(5678.123451,2);
                truncate(5678.123451,2)
                -----
                5678.12
                (1 row)

                select truncate(5678.123451,3);
                truncate(5678.123451,3)
                -----
                5678.123
                (1 row)

                select truncate(5678.123451,4);
                truncate(5678.123451,4)
                -----
                5678.1234
                (1 row)

                select truncate(5678.123451,5);
                truncate(5678.123451,5)
                -----
                5678.12345
                (1 row)

                select truncate(5678.123451,6);
                truncate(5678.123451,6)
                -----
                5678.123451
                (1 row)

                select truncate(5678.123451,-1);
                truncate(5678.123451,-1)
                -----
                5670
                (1 row)

                select truncate(5678.123451,-2);
                truncate(5678.123451,-2)
                -----
                5600
                (1 row)

                select truncate(5678.123451,-3);
                truncate(5678.123451,-3)
                -----
                5000
                (1 row)

                select truncate(5678.123451,-4);
                truncate(5678.123451,-4)
                -----
                0
                (1 row)

                select truncate(-5678.123451,0);
                truncate(-5678.123451,0)
                -----
                -5678
                (1 row)

                select truncate(-5678.123451,1);
                truncate(-5678.123451,1)
                -----
                -5678.1
                (1 row)

                select truncate(-5678.123451,2);
                truncate(-5678.123451,2)
                -----
                -5678.12
                (1 row)

                select truncate(-5678.123451,3);
                truncate(-5678.123451,3)
                -----
                -5678.123
                (1 row)

                select truncate(-5678.123451,4);
                truncate(-5678.123451,4)
                -----
                -5678.1234
                (1 row)

                select truncate(-5678.123451,5);
                truncate(-5678.123451,5)
                -----
                -5678.12345
                (1 row)

                select truncate(-5678.123451,6);
                truncate(-5678.123451,6)
                -----
                -5678.123451
                (1 row)

                select truncate(-5678.123451,-1);
                truncate(-5678.123451,-1)
                -----
                -5670
                (1 row)

                select truncate(-5678.123451,-2);
                truncate(-5678.123451,-2)
                -----
                -5600
                (1 row)

                select truncate(-5678.123451,-3);
                truncate(-5678.123451,-3)
                -----
                -5000
                (1 row)

                select truncate(-5678.123451,-4);
                truncate(-5678.123451,-4)
                -----
                0
                (1 row)

                select truncate(5678.123451,1::int);
                truncate(5678.123451,1)
                -----
                5678.1
                (1 row)


                select truncate(5678.123451);
                truncate(5678.123451)
                -----
                5678
                (1 row)

                select truncate(5678.123451::DOUBLE,0);
                truncate(5678.123451::DOUBLE,0)
                -----
                5678
                (1 row)

                select truncate(5678.123451::DOUBLE,1);
                truncate(5678.123451::DOUBLE,1)
                -----
                5678.1
                (1 row)

                select truncate(5678.123451::DOUBLE,2);
                truncate(5678.123451::DOUBLE,2)
                -----
                5678.12
                (1 row)

                select truncate(5678.123451::DOUBLE,3);
                truncate(5678.123451::DOUBLE,3)
                -----
                5678.123
                (1 row)

                select truncate(5678.123451::DOUBLE,4);
                truncate(5678.123451::DOUBLE,4)
                -----
                5678.1234
                (1 row)

                select truncate(5678.123451::DOUBLE,5);
                truncate(5678.123451::DOUBLE,5)
                -----
                5678.12345
                (1 row)

                select truncate(5678.123451::DOUBLE,6);
                truncate(5678.123451::DOUBLE,6)
                -----
                5678.123451
                (1 row)

                select truncate(5678.123451::DOUBLE,-1);
                truncate(5678.123451::DOUBLE,-1)
                -----
                5670
                (1 row)

                select truncate(5678.123451::DOUBLE,-2);
                truncate(5678.123451::DOUBLE,-2)
                -----
                5600
                (1 row)

                select truncate(5678.123451::DOUBLE,-3);
                truncate(5678.123451::DOUBLE,-3)
                -----
                5000
                (1 row)

                select truncate(5678.123451::DOUBLE,-4);
                truncate(5678.123451::DOUBLE,-4)
                -----
                0
                (1 row)

                select truncate(-5678.123451::DOUBLE,0);
                truncate(-5678.123451::DOUBLE,0)
                -----
                -5678
                (1 row)

                select truncate(-5678.123451::DOUBLE,1);
                truncate(-5678.123451::DOUBLE,1)
                -----
                -5678.1
                (1 row)

                select truncate(-5678.123451::DOUBLE,2);
                truncate(-5678.123451::DOUBLE,2)
                -----
                -5678.12
                (1 row)

                select truncate(-5678.123451::DOUBLE,3);
                truncate(-5678.123451::DOUBLE,3)
                -----
                -5678.123
                (1 row)

                select truncate(-5678.123451::DOUBLE,4);
                truncate(-5678.123451::DOUBLE,4)
                -----
                -5678.1234
                (1 row)

                select truncate(-5678.123451::DOUBLE,5);
                truncate(-5678.123451::DOUBLE,5)
                -----
                -5678.12345
                (1 row)

                select truncate(-5678.123451::DOUBLE,6);
                truncate(-5678.123451::DOUBLE,6)
                -----
                -5678.123451
                (1 row)

                select truncate(-5678.123451::DOUBLE,-1);
                truncate(-5678.123451::DOUBLE,-1)
                -----
                -5670
                (1 row)

                select truncate(-5678.123451::DOUBLE,-2);
                truncate(-5678.123451::DOUBLE,-2)
                -----
                -5600
                (1 row)

                select truncate(-5678.123451::DOUBLE,-3);
                truncate(-5678.123451::DOUBLE,-3)
                -----
                -5000
                (1 row)

                select truncate(-5678.123451::DOUBLE,-4);
                truncate(-5678.123451::DOUBLE,-4)
                -----
                0
                (1 row)

                select truncate(5678.123451::DOUBLE,1::int);
                truncate(5678.123451::DOUBLE,1)
                -----
                5678.1
                (1 row)

                """
        );
    }

    @Test
    public void testRoundTruncateEdgeCases() {
        // Calcite normalizes the sign here
        this.qs("""
                select truncate(-0.00123::DOUBLE, 2);
                 truncate
                ----------
                 0.00
                (1 row)

                select truncate(-0.00123::DECIMAL(10, 5), 2);
                 truncate
                ----------
                 0.00
                (1 row)

                select round(-0.00123::DOUBLE, 2);
                 round
                ----------
                 0.00
                (1 row)

                select round(-0.00123::DECIMAL(10, 5), 2);
                 round
                ----------
                 0.00
                (1 row)

                select round(-0.00623::DOUBLE, 2);
                 round
                ----------
                 -0.01
                (1 row)

                select round(-0.00623::DECIMAL(10, 5), 2);
                 round
                ----------
                 -0.01
                (1 row)
                """
        );
    }

    @Test
    public void testRoundBigInt() {
        this.queryFailingInCompilation("SELECT round(123.123, 2::bigint)", "Cannot apply 'ROUND' to arguments of type");
        this.queryFailingInCompilation("select truncate(5678.123451,1::bigint)", "Cannot apply 'TRUNCATE' to arguments of type");
    }

    @Test
    public void testExp() {
        this.qs("""
                SELECT EXP(0.0);
                 exp
                -----
                 1.0
                (1 row)

                SELECT EXP(0e0);
                 exp
                -----
                 1.0
                (1 row)

                SELECT EXP(null);
                 exp
                -----
                null
                (1 row)

                SELECT EXP(null::double);
                 exp
                -----
                null
                (1 row)

                SELECT EXP(0);
                 exp
                -----
                 1
                (1 row)
                """
        );
    }

    @Test
    public void testExpEdgeCase() {
        this.qs("""
                -- changed the type from numeric to double
                -- therefore the value has also slightly changed
                -- cases that used to generate inaccurate results in Postgres
                select exp(32.999::double);
                         exp
                ---------------------
                 214429043492155.56
                (1 row)
                """
        );
    }

    @Test
    public void testGunzip() {
        this.qs("""
                SELECT GUNZIP(x'1f8b08000000000000ff4b4bcd49492d4a0400218115ac07000000'::bytea);
                 gunzip
                ------------
                 feldera
                (1 row)

                SELECT GUNZIP(x'1f8b08000000000000ff734bcd49492d4a0400bdb8a86307000000'::bytea);
                 gunzip
                ------------
                 Feldera
                (1 row)

                SELECT GUNZIP(x'1f8b08000000000000ffcb48cdc9c9070086a6103605000000'::bytea);
                 gunzip
                ------------
                 hello
                (1 row)

                SELECT GUNZIP(x'1f8b08000000000000132bc9c82c5600a2dc4a851282ccd48a12002e7a22ff30000000'::bytea);
                 gunzip
                --------------------------------------------------------------------------
                 this is my this is my this is my this is my text
                (1 row)

                SELECT GUNZIP(null);
                 gunzip
                --------
                NULL
                (1 row)
                """
        );
    }

    @Test
    public void testGunzipRuntimeFail() {
        this.runtimeConstantFail("SELECT GUNZIP(x'1100'::bytea)", "failed to decompress gzipped data");
    }

    @Test
    public void testIssue1505() {
        this.qs("""
                SELECT ROUND(123.1234::DOUBLE, 2);
                 round
                -------
                 123.12
                (1 row)

                SELECT ROUND(123.1266::DOUBLE, 2);
                 round
                -------
                 123.13
                (1 row)

                SELECT ROUND(123.1266::DOUBLE, -2);
                 round
                -------
                 100
                (1 row)

                SELECT ROUND(123.1266::DOUBLE);
                 round
                -------
                 123
                (1 row)
                """
        );
    }

    @Test
    public void testRlike() {
        this.qs("""
                SELECT RLIKE('string', 's..i.*');
                 rlike
                -------
                 true
                (1 row)

                SELECT RLIKE(null, 's..i.*');
                 rlike
                -------
                 NULL
                (1 row)

                SELECT RLIKE('string', null);
                 rlike
                -------
                 NULL
                (1 row)

                SELECT RLIKE(null, null);
                 rlike
                -------
                 NULL
                (1 row)
                """
        );
    }

    @Test
    public void testSequence() {
        this.qs("""
                SELECT SEQUENCE(1, 10);
                 sequence
                ----------
                 {1, 2, 3, 4, 5, 6, 7, 8, 9, 10}
                (1 row)

                SELECT SEQUENCE(10, 1);
                 sequence
                ----------
                 {}
                (1 row)

                SELECT SEQUENCE(1, 1);
                 sequence
                ----------
                 {1}
                (1 row)

                SELECT SEQUENCE(-6, 1);
                 sequence
                ----------
                 {-6, -5, -4, -3, -2, -1, 0, 1}
                (1 row)

                SELECT SEQUENCE(-6, -9);
                 sequence
                ----------
                 {}
                (1 row)

                SELECT SEQUENCE(0, -2);
                 sequence
                ----------
                 {}
                (1 row)

                SELECT SEQUENCE(null::int, 1);
                 sequence
                ----------
                 NULL
                (1 row)

                SELECT SEQUENCE(null, 1);
                 sequence
                ----------
                 NULL
                (1 row)

                SELECT SEQUENCE(1, null);
                 sequence
                ----------
                 NULL
                (1 row)

                SELECT SEQUENCE(null, null);
                 sequence
                ----------
                 NULL
                (1 row)

                SELECT SEQUENCE(null::tinyint, null::tinyint);
                 sequence
                ----------
                 NULL
                (1 row)

                SELECT SEQUENCE(1::tinyint, 10::tinyint);
                 sequence
                ----------
                 {1, 2, 3, 4, 5, 6, 7, 8, 9, 10}
                (1 row)

                SELECT SEQUENCE(1::tinyint, 3::bigint);
                 sequence
                ----------
                 {1, 2, 3}
                (1 row)
                """
        );
    }

    @Test
    public void testCeilInt() {
        this.qs("""
                SELECT ceil(2::tinyint);
                 ceil
                ------
                     2
                (1 row)

                SELECT ceil(2::smallint);
                 ceil
                ------
                     2
                (1 row)

                SELECT ceil(2::int);
                 ceil
                ------
                     2
                (1 row)

                SELECT ceil(2::bigint);
                 ceil
                ------
                     2
                (1 row)

                SELECT ceil(null::tinyint);
                 ceil
                ------
                 NULL
                (1 row)

                SELECT ceil(null::smallint);
                 ceil
                ------
                 NULL
                (1 row)

                SELECT ceil(null::int);
                 ceil
                ------
                 NULL
                (1 row)

                SELECT ceil(null::bigint);
                 ceil
                ------
                 NULL
                (1 row)

                SELECT ceil(null);
                 ceil
                ------
                 NULL
                (1 row)
                """
        );
    }

    @Test
    public void testFloorInt() {
        this.qs("""
                SELECT floor(2::tinyint);
                 floor
                ------
                     2
                (1 row)

                SELECT floor(2::smallint);
                 floor
                ------
                     2
                (1 row)

                SELECT floor(2::int);
                 floor
                ------
                     2
                (1 row)

                SELECT floor(2::bigint);
                 floor
                ------
                     2
                (1 row)

                SELECT floor(null::tinyint);
                 floor
                ------
                 NULL
                (1 row)

                SELECT floor(null::smallint);
                 floor
                ------
                 NULL
                (1 row)

                SELECT floor(null::int);
                 floor
                ------
                 NULL
                (1 row)

                SELECT floor(null::bigint);
                 floor
                ------
                 NULL
                (1 row)

                SELECT floor(null);
                 floor
                ------
                 NULL
                (1 row)
                """
        );
    }

    @Test
    public void testBlackboxFunction() {
        this.qs("""
                SELECT blackbox(1);
                 blackbox
                ----------
                 1
                (1 row)
                
                SELECT blackbox('blah');
                 blackbox
                ----------
                 blah
                (1 row)
                
                SELECT blackbox(true);
                 blackbox
                ----------
                 true
                (1 row)
                
                SELECT blackbox(1e0);
                 blackbox
                ----------
                 1e0
                (1 row)
                
                SELECT blackbox(1.0);
                 blackbox
                ----------
                 1.0
                (1 row)
                
                SELECT blackbox(vals) from arr_table;
                 blackbox
                ----------
                 {1,2,3}
                 {1,2,3}
                (2 rows)
                
                SELECT blackbox(1.0) as c0, blackbox(true) as c1;
                  c0 | c1
                -----+-----
                 1.0 | true
                (1 row)
                """
        );
    }
}
