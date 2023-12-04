package org.dbsp.sqlCompiler.compiler.sql.simple;
import org.dbsp.sqlCompiler.compiler.sql.SqlIoTest;
import org.junit.Ignore;
import org.junit.Test;

public class SafeAddTests extends SqlIoTest {
    @Test
    public void testSafeAdd() {
        this.qs(
                "SELECT SAFE_ADD(2, 3) as 'result';\n" +
                " result \n" +
                "--------\n" +
                "5\n" +
                "(1 row)\n" +
                "\n" +
                "SELECT SAFE_ADD(9223372036854775807, 2) as 'result';\n" +
                " result \n" +
                "--------\n" +
                " \n" +
                "(1 row)\n" +
                "\n" +
                "SELECT SAFE_ADD(-9223372036854775807, -2) as 'result';\n" +
                " result \n" +
                "--------\n" +
                " \n" +
                "(1 row)\n" +
                "\n" +
                "SELECT SAFE_ADD(9.9, 1.0) as 'result';\n" +
                " result \n" +
                "--------\n" +
                "10.9\n" +
                "(1 row)\n" +
                "\n" +
                "SELECT SAFE_ADD(9.9, 1) as 'result';\n" +
                " result \n" +
                "--------\n" +
                "10.9\n" +
                "(1 row)\n" +
                "\n" +
                "SELECT SAFE_ADD(CAST(125 AS TINYINT), CAST(2 AS TINYINT)) as 'result';\n" +
                " result \n" +
                "--------\n" +
                " 127\n" +
                "(1 row)\n" +
                "\n"
        );
    }

    @Test
    @Ignore
    public void testSafeAddWithCasts() {
        this.q(
        "SELECT SAFE_ADD(CAST(32767 AS SMALLINT), CAST(2 AS SMALLINT)) as 'result';\n" +
                " result \n" +
                "--------\n" +
                " \n"
        );
    }

    @Test
    @Ignore
    public void testSafeAddWithNestedSelectCast() {
        this.q("SELECT SAFE_ADD((SELECT CAST(2 AS SMALLINT)), (SELECT CAST(4 AS SMALLINT))) as 'result';\n" +
                " result \n" +
                "--------\n" +
                " 6\n"
        );
    }

    @Test
    @Ignore
    public void testSafeAddDecimalOverflow() {
        this.q("SELECT SAFE_ADD(9, cast(9.999999999999999999e75 as DECIMAL(38, 19))) as 'result';\n" +
                " result \n" +
                "--------\n" +
                " \n"
        );
    }
}
