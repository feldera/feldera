package org.dbsp.sqlCompiler.compiler.sql.jit;

import org.dbsp.sqlCompiler.compiler.CompilerOptions;
import org.dbsp.sqlCompiler.compiler.sql.postgres.PostgresBoolTests;

public class JitBoolTests extends PostgresBoolTests {
    @Override
    public CompilerOptions getOptions(boolean optimize) {
        CompilerOptions options = super.getOptions(optimize);
        options.ioOptions.jit = true;
        return options;
    }
}
