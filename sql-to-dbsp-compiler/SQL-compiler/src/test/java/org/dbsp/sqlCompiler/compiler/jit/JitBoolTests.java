package org.dbsp.sqlCompiler.compiler.jit;

import org.dbsp.sqlCompiler.compiler.CompilerOptions;
import org.dbsp.sqlCompiler.compiler.postgres.PostgresBoolTests;

public class JitBoolTests extends PostgresBoolTests {
    @Override
    public CompilerOptions getOptions(boolean optimize) {
        CompilerOptions options = super.getOptions(optimize);
        options.ioOptions.jit = true;
        return options;
    }
}
