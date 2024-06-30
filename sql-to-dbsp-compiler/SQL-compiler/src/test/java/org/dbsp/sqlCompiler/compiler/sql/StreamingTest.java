package org.dbsp.sqlCompiler.compiler.sql;

import org.dbsp.sqlCompiler.compiler.CompilerOptions;

/** Base class for tests that use incremental circuits */
public class StreamingTest extends SqlIoTest {
    @Override
    public CompilerOptions testOptions(boolean incremental, boolean optimize) {
        CompilerOptions options = super.testOptions(incremental, optimize);
        options.languageOptions.incrementalize = true;
        options.languageOptions.throwOnError = true;
        return options;
    }
}
