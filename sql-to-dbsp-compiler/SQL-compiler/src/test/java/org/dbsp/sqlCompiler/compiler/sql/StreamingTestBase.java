package org.dbsp.sqlCompiler.compiler.sql;

import org.dbsp.sqlCompiler.compiler.CompilerOptions;
import org.dbsp.sqlCompiler.compiler.sql.tools.SqlIoTest;

/** Base class for tests that use incremental circuits */
public class StreamingTestBase extends SqlIoTest {
    @Override
    public CompilerOptions testOptions(boolean incremental, boolean optimize) {
        CompilerOptions options = super.testOptions(incremental, optimize);
        options.languageOptions.incrementalize = true;
        options.languageOptions.throwOnError = true;
        options.languageOptions.streaming = true;
        return options;
    }
}
