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
                SELECT CAST(INTERVAL 22 MONTHS AS INTERVAL YEARS);
                 x
                ---
                 22 months
                (1 row)""");
    }
}
