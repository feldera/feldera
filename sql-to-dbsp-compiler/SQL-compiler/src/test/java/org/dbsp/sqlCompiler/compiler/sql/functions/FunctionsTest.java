package org.dbsp.sqlCompiler.compiler.sql.functions;

import org.dbsp.sqlCompiler.compiler.sql.SqlIoTest;
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

    @Test
    public void issue1180() {
        this.runtimeFail("SELECT '1_000'::INT4", "ParseIntError", this.getEmptyIOPair());
    }

    @Test
    public void issue1192() {
        this.runtimeFail("select '-9223372036854775809'::int64", "ParseIntError", this.getEmptyIOPair());
        this.runtimeFail("select '9223372036854775808'::int64", "ParseIntError", this.getEmptyIOPair());
    }

    @Test
    public void issue1209() {
        this.qf("SELECT '-32768'::int2 % 0::int2", "attempt to calculate the remainder with a divisor of zero");

        this.qf("SELECT '-32768'::int2 / 0::int2", "attempt to divide by zero", true);

        this.qf("SELECT '-32768'::int2 / 0::int2", "attempt to divide by zero", false);
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
}
