package org.dbsp.sqlCompiler.compiler.sql.functions;

import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.sql.SqlIoTest;
import org.junit.Ignore;
import org.junit.Test;

public class FunctionsTest extends SqlIoTest {
    @Override
    public void prepareInputs(DBSPCompiler compiler) {
        String ddl = "CREATE TABLE ARR_TABLE (VALS INTEGER ARRAY NOT NULL,ID INTEGER NOT NULL)";
        String insert = """
                INSERT INTO ARR_TABLE VALUES(ARRAY [1, 2, 3], 6);
                INSERT INTO ARR_TABLE VALUES(ARRAY [1, 2, 3], 7);
                """;
        compiler.compileStatement(ddl);
        compiler.compileStatements(insert);
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

    @Test @Ignore("https://issues.apache.org/jira/projects/CALCITE/issues/CALCITE-6210")
    public void testSubstring2() {
        this.qs(
                """
                        SELECT CAST('1234567890' AS VARBINARY) as val;
                         val
                        -----
                         34567890
                        (1 row)
                        
                        SELECT '1234567890'::VARBINARY as val;
                         val
                        -----
                         34567890
                        (1 row)
                        """
        );
    }

    @Test
    public void issue1180() {
        this.runtimeConstantFail("SELECT '1_000'::INT4", "ParseIntError");
    }

    @Test
    public void issue1192() {
        this.runtimeConstantFail("select '-9223372036854775809'::int64", "ParseIntError");
        this.runtimeConstantFail("select '9223372036854775808'::int64", "ParseIntError");
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

        this.shouldWarn("SELECT 8 % 0", "Modulus by constant zero value as divisor.");
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
        this.q("""
                SELECT CONCAT('string', 1);
                result
                ---------
                 string1""");
        this.q("""
                SELECT CONCAT('string', 1, true);
                result
                ---------
                 string1TRUE""");
    }

    @Test
    public void testCoalesce() {
        this.q("""
                SELECT COALESCE(NULL, 5);
                result
                ------
                5""");
    }

    // Tested on Postgres and some taken from MySQL
    @Test
    public void testRounding() {
        this.qs("""
                select CAST((CAST('1234.1264' AS DECIMAL(8, 4))) AS DECIMAL(6, 2));
                 cast
                ------
                 1234.13
                (1 row)
                
                select CAST((CAST('1234.1234' AS DECIMAL(8, 4))) AS DECIMAL(6, 2));
                 cast
                ------
                 1234.12
                (1 row)
                
                select CAST((CAST('-1234.1264' AS DECIMAL(8, 4))) AS DECIMAL(6, 2));
                 cast
                ------
                 -1234.13
                (1 row)
                
                select CAST((CAST('-1234.1234' AS DECIMAL(8, 4))) AS DECIMAL(6, 2));
                 cast
                ------
                 -1234.12
                (1 row)
                
                select CAST((CAST('1234.1250' AS DECIMAL(8, 4))) AS DECIMAL(6, 2));
                 cast
                ------
                 1234.13
                (1 row)
                
                select CAST((CAST('-1234.1250' AS DECIMAL(8, 4))) AS DECIMAL(6, 2));
                 cast
                ------
                 -1234.13
                (1 row)
                
                select CAST((CAST('.1264' AS DECIMAL(4, 4))) AS DECIMAL(2, 2));
                 cast
                ------
                 0.13
                (1 row)
                
                select CAST((CAST('.1234' AS DECIMAL(4, 4))) AS DECIMAL(2, 2));
                 cast
                ------
                 0.12
                (1 row)
                
                select CAST((CAST('-.1264' AS DECIMAL(4, 4))) AS DECIMAL(2, 2));
                 cast
                ------
                 -0.13
                (1 row)
                
                select CAST((CAST('-.1234' AS DECIMAL(4, 4))) AS DECIMAL(2, 2));
                 cast
                ------
                 -0.12
                (1 row)
                
                select CAST((CAST('00.1264' AS DECIMAL(4, 4))) AS DECIMAL(2, 2));
                 cast
                ------
                 0.13
                (1 row)
                
                select CAST((CAST('00.1234' AS DECIMAL(4, 4))) AS DECIMAL(2, 2));
                 cast
                ------
                 0.12
                (1 row)
                
                select CAST((CAST('-00.1264' AS DECIMAL(4, 4))) AS DECIMAL(2, 2));
                 cast
                ------
                 -0.13
                (1 row)
                
                select CAST((CAST('-00.1234' AS DECIMAL(4, 4))) AS DECIMAL(2, 2));
                 cast
                ------
                 -0.12
                (1 row)
                
                -- the following tests are from mysql
                select cast('1.00000001335143196001808973960578441619873046875E-10' as decimal(30,15));
                     decimal
                -------------------
                 0.000000000100000
                (1 row)
                
                select ln(14000) as c1, cast(ln(14000) as decimal(5,3)) as c2, cast(ln(14000) as decimal(5,3)) as c3;
                        c1         |  c2   |  c3
                -------------------+-------+-------
                 9.546812608597396 | 9.547 | 9.547
                (1 row)
                
                select cast(143.481 as decimal(4,1));
                 cast(143.481 as decimal(4,1))
                -------------------------------
                143.5
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
                99
                (1 row)
                """
        );
    }

    @Test
    public void testDecimalErrors() {
        this.qf("select cast('1234.1234' AS DECIMAL(6, 3))",
                // The compiler rounds `1234.1234` to `1234.123` before calling the rust code
                "cannot represent 1234.123 as DECIMAL(6, 3)"
        );

        this.qf("select cast('1234.1236' AS DECIMAL(6, 3))",
                // The compiler rounds `1234.1236` to `1234.124` before calling the rust code
                "cannot represent 1234.124 as DECIMAL(6, 3)"
        );

        this.shouldFail("select cast(1234.1234 AS DECIMAL(6, 3))",
                "cannot represent 1234.1234 as DECIMAL(6, 3)"
        );

        this.shouldFail("select cast(1234.1236 AS DECIMAL(6, 3))",
                "cannot represent 1234.1236 as DECIMAL(6, 3)"
        );

        this.shouldFail("select cast(143.481 as decimal(2, 1))", "cannot represent 143.481 as DECIMAL(2, 1)");

        // this only fails in runtime
        this.runtimeConstantFail("select cast(99.6 as decimal(2, 0))", "cannot represent 99.6 as DECIMAL(2, 0)");
        this.shouldFail("select cast(-13.4 as decimal(2,1))", "cannot represent -13.4 as DECIMAL(2, 1)");
    }

    @Test
    public void testLn() {
        this.qs("""
                SELECT ln(2e0);
                 ln
                ----
                 0.693147180559945
                (1 row)
                
                SELECT ln(2.0);
                 ln
                ----
                 0.693147180559945
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

    @Test @Ignore("Calcite bug: https://github.com/feldera/feldera/issues/1345")
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
        this.shouldFail("SELECT round(15.1, 1.0)", "Error in SQL statement: Cannot apply 'ROUND' to arguments of type 'ROUND(<DECIMAL(3, 1)>, <DECIMAL(2, 1)>)'. Supported form(s): 'ROUND(<NUMERIC>, <INTEGER>)'");
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
                
                select round(-15.91,-1::tinyint);
                round(-15.91,-1)
                ------------
                 -20
                (1 row)
                
                select round(-15.91,-1::smallint);
                round(-15.91,-1)
                ------------
                 -20
                (1 row)
                
                select round(-15.91,-1::int);
                round(-15.91,-1)
                ------------
                 -20
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
                
                select truncate(5678.123451,1::tinyint);
                truncate(5678.123451,1)
                -----
                5678.1
                (1 row)
                
                select truncate(5678.123451,1::smallint);
                truncate(5678.123451,1)
                -----
                5678.1
                (1 row)
                
                select truncate(5678.123451,1::int);
                truncate(5678.123451,1)
                -----
                5678.1
                (1 row)
                """
        );
    }

    @Test @Ignore("https://github.com/feldera/feldera/issues/1379")
    public void testRoundBigInt() {
        this.q("""
                SELECT round(123.123, 2::bigint);
                 round
                -------
                 123.12"""
        );
    }

    @Test @Ignore("https://github.com/feldera/feldera/issues/1379")
    public void testTruncateBigInt() {
        this.q("""
                select truncate(5678.123451,1::bigint);
                truncate(5678.123451,1)
                -----
                5678.1"""
        );
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

    @Test @Ignore("FP comparison error for Calcite optimized version")
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
}
