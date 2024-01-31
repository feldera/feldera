package org.dbsp.sqlCompiler.compiler.sql.functions;

import org.dbsp.sqlCompiler.compiler.sql.SqlIoTest;
import org.junit.Ignore;
import org.junit.Test;

public class FunctionsTest extends SqlIoTest {
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

    // Tested on Postgres
    @Test
    public void testDecimalRounding() {
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
}
