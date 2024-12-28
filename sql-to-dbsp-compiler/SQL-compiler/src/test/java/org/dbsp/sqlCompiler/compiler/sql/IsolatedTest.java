package org.dbsp.sqlCompiler.compiler.sql;

import org.dbsp.sqlCompiler.compiler.CompilerOptions;
import org.dbsp.sqlCompiler.compiler.sql.tools.SqlIoTest;
import org.junit.Test;

/** Used for interactive debugging: create here temporary tests. */
@SuppressWarnings("unused")
public class IsolatedTest extends SqlIoTest {
    @Override
    public CompilerOptions getOptions(boolean optimize) {
        CompilerOptions options = super.getOptions(optimize);
        options.ioOptions.raw = true;
        return options;
    }

    @Test
    public void test() {
        this.qs("""
                SELECT CAST('1' AS INTERVAL HOURS);
                 i
                ---
                 1 hours
                (1 row)""");
    }
}
