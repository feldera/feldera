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
        this.runtimeFail("SELECT '1_000'::INT4", "ParseIntError", this.getEmptyIOPair());
    }

    @Test
    public void issue1192() {
        this.runtimeFail("select '-9223372036854775809'::int64", "ParseIntError", this.getEmptyIOPair());
        this.runtimeFail("select '9223372036854775808'::int64", "ParseIntError", this.getEmptyIOPair());
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
        this.runtimeFail("SELECT 8 % 0", "attempt to calculate the remainder with a divisor of zero", this.getEmptyIOPair());

        this.shouldWarn("SELECT 8 % 0", "Modulus by constant zero value as divisor.");
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
}
