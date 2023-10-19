package org.dbsp.sqlCompiler.compiler.jit;

import org.dbsp.sqlCompiler.compiler.CompilerOptions;
import org.dbsp.sqlCompiler.compiler.postgres.PostgresBaseTest;
import org.junit.Ignore;
import org.junit.Test;

public class JitFpTests extends PostgresBaseTest {
    @Override
    public CompilerOptions getOptions(boolean optimize) {
        CompilerOptions options = super.getOptions(optimize);
        options.ioOptions.jit = true;
        return options;
    }

    @Test
    public void testInfinity() {
        this.q("SELECT 1E0 / CAST('Infinity' AS DOUBLE), CAST('Infinity' AS DOUBLE) / 1E0;\n" +
                "result | result\n" +
                "-------|-------\n" +
                "  0    | Infinity");
    }

    @Test @Ignore("https://github.com/feldera/feldera/issues/902")
    public void testSimpleFp() {
        this.q("WITH v(x) AS\n" +
                "  (VALUES(1E0),(CAST ('Infinity' AS DOUBLE)))\n" +
                "SELECT x1, x2,\n" +
                "  x1 / x2 AS quot\n" +
                "FROM v AS v1(x1), v AS v2(x2);\n" +
                "    x1     |    x2     |          quot            \n" +
                "-----------+-----------+--------------------------\n" +
                "  Infinity |  Infinity |                     NaN \n" +
                "         1 |  Infinity |                       0 \n" +
                "  Infinity |         1 |                Infinity \n" +
                "         1 |         1 |  1.00000000000000000000 ");
    }

    @Test
    public void testSimpleFp1() {
        this.q("WITH v(x) AS\n" +
                "  (VALUES(1E0),(CAST ('0' AS DOUBLE)))\n" +
                "SELECT x1, x2,\n" +
                "  x1 / x2 AS quot\n" +
                "FROM v AS v1(x1), v AS v2(x2);\n" +
                "    x1     |    x2     |          quot            \n" +
                "-----------+-----------+--------------------------\n" +
                "         0 |         0 |                     NaN \n" +
                "         1 |         0 |                Infinity \n" +
                "         0 |         1 |                       0 \n" +
                "         1 |         1 |  1.00000000000000000000 ");
    }


    @Test @Ignore("https://github.com/feldera/feldera/issues/903")
    public void testDiff() {
        this.q("SELECT (1.2 - 1.2) = 0;\n" +
                "r \n" +
                "----\n" +
                "  t");
    }
}
