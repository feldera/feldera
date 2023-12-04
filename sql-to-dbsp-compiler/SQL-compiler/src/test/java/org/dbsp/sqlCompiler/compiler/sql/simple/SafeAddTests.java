package org.dbsp.sqlCompiler.compiler.sql.simple;
import org.dbsp.sqlCompiler.compiler.sql.SqlIoTest;
import org.junit.Test;

public class SafeAddTests extends SqlIoTest {
    @Test
    public void testSimpleInt() {
        this.q("SELECT SAFE_ADD(2, 3) as 'result';\n" +
                " result \n" +
                "--------\n" +
                "5"
        );
    }

    @Test
    public void testOverflowInt() {
        this.q("SELECT SAFE_ADD(9223372036854775807, 2) as 'result';\n" +
                " result \n" +
                "--------\n" +
                " "
        );
    }

    @Test
    public void testUnderflowInt() {
        this.q("SELECT SAFE_ADD(-9223372036854775807, -2) as 'result';\n" +
                " result \n" +
                "--------\n" +
                " "
        );
    }

    @Test
    public void testDecimal() {
        this.q("SELECT SAFE_ADD(9.9, 1.0) as 'result';\n" +
                " result \n" +
                "--------\n" +
                "10.9"
        );
    }

    @Test
    public void testDecimalInt() {
        this.q("SELECT SAFE_ADD(9.9, 1) as 'result';\n" +
                " result \n" +
                "--------\n" +
                "10.9"
        );
    }
}
