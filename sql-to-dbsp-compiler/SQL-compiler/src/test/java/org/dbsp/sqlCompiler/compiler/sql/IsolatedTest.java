package org.dbsp.sqlCompiler.compiler.sql;

import org.dbsp.sqlCompiler.compiler.CompilerOptions;
import org.dbsp.sqlCompiler.compiler.sql.tools.SqlIoTest;

/** Used for interactive debugging: create here temporary tests. */
@SuppressWarnings("unused")
public class IsolatedTest extends SqlIoTest {
    @Override
    public CompilerOptions testOptions() {
        CompilerOptions options = super.testOptions();
        options.ioOptions.raw = true;
        options.languageOptions.throwOnError = true;
        options.ioOptions.quiet = false;
        options.languageOptions.incrementalize = true;
        options.languageOptions.optimizationLevel = 2;
        return options;
    }
}
