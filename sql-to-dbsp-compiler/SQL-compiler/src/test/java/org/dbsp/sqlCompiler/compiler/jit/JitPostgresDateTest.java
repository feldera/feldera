package org.dbsp.sqlCompiler.compiler.jit;

import org.dbsp.sqlCompiler.compiler.CompilerOptions;
import org.dbsp.sqlCompiler.compiler.postgres.PostgresDateTests;
import org.junit.Ignore;
import org.junit.Test;

public class JitPostgresDateTest extends PostgresDateTests {
    public CompilerOptions getOptions(boolean optimize) {
        CompilerOptions options = super.getOptions(optimize);
        options.ioOptions.jit = true;
        return options;
    }

    // TODO: all ignored tests below are JIT bugs

    @Test @Ignore("No support for intervals https://github.com/feldera/feldera/issues/309")
    public void testDiff() {
        this.q("SELECT (f1 - date '2000-01-01') day AS \"Days From 2K\" FROM DATE_TBL;\n" +
                " Days From 2K \n" +
                "--------------\n" +
                "       -15607\n" +
                "       -15542\n" +
                "        -1403\n" +
                "        -1402\n" +
                "        -1401\n" +
                "        -1400\n" +
                "        -1037\n" +
                "        -1036\n" +
                "        -1035\n" +
                "           91\n" +
                "           92\n" +
                "           93\n" +
                "        13977\n" +
                "        14343\n" +
                "        14710\n" +
                "null"); // Added manually
    }

    @Test @Ignore("Incorrect result for dates before 1970 https://github.com/feldera/feldera/issues/400")
    public void testLt() {
        this.q("SELECT f1 FROM DATE_TBL WHERE f1 < '2000-01-01';\n" +
                " f1       \n" +
                "---------------\n" +
                " 04-09-1957\n" +
                " 06-13-1957\n" +
                " 02-28-1996\n" +
                " 02-29-1996\n" +
                " 03-01-1996\n" +
                " 03-02-1996\n" +
                " 02-28-1997\n" +
                " 03-01-1997\n" +
                " 03-02-1997");
        //" 04-10-2040 BC";
    }
}
